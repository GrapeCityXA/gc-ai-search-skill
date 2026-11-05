#!/usr/bin/env python3
"""GCID-authenticated client for the GC AI streaming search endpoint."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
import webbrowser
import uuid
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen


DEFAULT_ENDPOINT = (
    "https://ai-assist.grapecity.com.cn/api/rag/deep_research_streaming/"
)
# Skill authorization is intentionally attributed to one external client
# class. The GCID user identity, rather than the host application, is the
# authorization boundary.
DEFAULT_SOURCE = "external_skill"
STREAM_SCHEMA = "gc-ai-search.stream.v1"
TOKEN_CACHE_VERSION = 2
TOKEN_LOCK_TIMEOUT = 600.0
TOKEN_LOCK_STALE_AFTER = 900.0
PAIRING_STATUS_REPORT_INTERVAL = 10.0


def normalize_source(_value: str = "") -> str:
    """Return the single supported Skill authorization source."""
    return DEFAULT_SOURCE


def _api_origin(endpoint: str) -> str:
    parsed = urlsplit(endpoint)
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", "")).rstrip("/")


def _auth_endpoint(endpoint: str, path: str) -> str:
    return f"{_api_origin(endpoint)}/api{path}"


def _json_request(
    url: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: float,
    token: str = "",
) -> dict[str, Any]:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(url, data=body, headers=headers, method="POST" if body else "GET")
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        detail = exc.read(500).decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"auth server returned HTTP {exc.code}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"auth server is unreachable: {exc.reason}") from exc
    except TimeoutError as exc:
        raise RuntimeError(f"auth request timed out after {timeout:g}s") from exc
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("auth server returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise RuntimeError("auth server returned an invalid response")
    return value


def _token_file() -> Path:
    configured = os.getenv("GC_AI_SEARCH_TOKEN_FILE", "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path.home() / ".gc-ai-search" / "token.json"


def _cache_identity(endpoint: str, source: str) -> str:
    return f"{_api_origin(endpoint)}|{normalize_source(source)}"


def _cache_key(endpoint: str, source: str) -> str:
    return hashlib.sha256(_cache_identity(endpoint, source).encode("utf-8")).hexdigest()[:24]


def _load_cached_token(endpoint: str = DEFAULT_ENDPOINT, source: str = DEFAULT_SOURCE) -> str:
    try:
        value = json.loads(_token_file().read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            return ""
        tokens = value.get("tokens")
        if isinstance(tokens, dict):
            entry = tokens.get(_cache_key(endpoint, source))
            if isinstance(entry, dict):
                return str(entry.get("access_token") or "").strip()

            # Older versions stored one entry per host (workbuddy/codex).
            # Reuse a matching endpoint entry once so upgrading does not
            # force an unnecessary second GCID pairing.
            origin = _api_origin(endpoint)
            legacy_entries = [
                item
                for item in tokens.values()
                if isinstance(item, dict)
                and item.get("endpoint") == origin
                and str(item.get("access_token") or "").strip()
            ]
            if legacy_entries:
                legacy_entries.sort(key=lambda item: int(item.get("saved_at") or 0), reverse=True)
                return str(legacy_entries[0].get("access_token") or "").strip()
            return ""
        # Migrate the old single-token cache only for the current default
        # endpoint. The server-side validation below still enforces the source.
        # Other endpoints must never inherit this legacy token.
        if _api_origin(endpoint) == _api_origin(DEFAULT_ENDPOINT):
            return str(value.get("access_token") or "").strip()
        # Do not reuse the pre-v2 single-token cache across environments.
        return ""
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return ""


def _save_cached_token(
    token: str,
    endpoint: str = DEFAULT_ENDPOINT,
    source: str = DEFAULT_SOURCE,
) -> None:
    path = _token_file()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            existing = {}
        tokens = existing.get("tokens") if isinstance(existing, dict) else None
        if not isinstance(tokens, dict):
            tokens = {}
        tokens[_cache_key(endpoint, source)] = {
            "access_token": token,
            "endpoint": _api_origin(endpoint),
            "source": normalize_source(source),
            "saved_at": int(time.time()),
        }
        payload = {"version": TOKEN_CACHE_VERSION, "tokens": tokens}
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        temporary.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(temporary, path)
        try:
            path.chmod(0o600)
        except OSError:
            pass
    except OSError as exc:
        print(f"warning: unable to cache GCID access token: {exc}", file=sys.stderr)


def _clear_cached_token(
    endpoint: str = DEFAULT_ENDPOINT,
    source: str = DEFAULT_SOURCE,
    expected_token: str = "",
) -> None:
    """Remove only the rejected endpoint/source entry from the token cache."""
    path = _token_file()
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            return
        tokens = value.get("tokens")
        if isinstance(tokens, dict):
            key = _cache_key(endpoint, source)
            entry = tokens.get(key)
            if expected_token and isinstance(entry, dict) and entry.get("access_token") != expected_token:
                return
            tokens.pop(key, None)
            path.write_text(json.dumps({"version": TOKEN_CACHE_VERSION, "tokens": tokens}), encoding="utf-8")
            return
        if value.get("endpoint") == _api_origin(endpoint) and normalize_source(str(value.get("source") or "")) == normalize_source(source):
            if not expected_token or value.get("access_token") == expected_token:
                path.unlink(missing_ok=True)
    except OSError as exc:
        print(f"warning: unable to clear cached GCID access token: {exc}", file=sys.stderr)
    except (ValueError, TypeError, json.JSONDecodeError):
        return


def _token_lock_path(endpoint: str, source: str) -> Path:
    path = _token_file()
    return path.with_name(f"{path.name}.{_cache_key(endpoint, source)}.lock")


def _lock_owner_is_alive(lock_path: Path) -> bool | None:
    """Return whether the process recorded in a lock file still exists."""
    try:
        owner_pid = int(lock_path.read_text(encoding="ascii").strip())
    except (OSError, TypeError, ValueError):
        return None
    if owner_pid <= 0:
        return False
    if os.name == "nt":
        try:
            import ctypes

            kernel32 = ctypes.windll.kernel32
            kernel32.OpenProcess.restype = ctypes.c_void_p
            handle = kernel32.OpenProcess(0x00100000, False, owner_pid)
            if not handle:
                return False
            kernel32.CloseHandle(handle)
            return True
        except (AttributeError, OSError):
            return None
    try:
        os.kill(owner_pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return None
    return True


@contextmanager
def _token_lock(
    endpoint: str,
    source: str,
    timeout: float = TOKEN_LOCK_TIMEOUT,
    on_wait: Callable[[float, float], None] | None = None,
) -> Iterator[None]:
    """Serialize pairing and cache refresh across Skill processes."""
    lock_path = _token_lock_path(endpoint, source)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout
    wait_started = time.monotonic()
    last_report = 0.0
    descriptor: int | None = None
    while descriptor is None:
        try:
            descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(descriptor, str(os.getpid()).encode("ascii", errors="ignore"))
        except FileExistsError:
            elapsed = time.monotonic() - wait_started
            if on_wait and (
                last_report == 0.0
                or elapsed - last_report >= PAIRING_STATUS_REPORT_INTERVAL
            ):
                try:
                    lock_age = max(0.0, time.time() - lock_path.stat().st_mtime)
                except OSError:
                    lock_age = 0.0
                on_wait(elapsed, lock_age)
                last_report = elapsed
            if _lock_owner_is_alive(lock_path) is False:
                try:
                    lock_path.unlink(missing_ok=True)
                    continue
                except OSError:
                    pass
            try:
                if time.time() - lock_path.stat().st_mtime > TOKEN_LOCK_STALE_AFTER:
                    lock_path.unlink(missing_ok=True)
                    continue
            except OSError:
                pass
            if time.monotonic() >= deadline:
                raise RuntimeError("等待 Skill 授权锁超时，请稍后重试")
            time.sleep(0.1)
    try:
        yield
    finally:
        try:
            os.close(descriptor)
        except OSError:
            pass
        try:
            lock_path.unlink(missing_ok=True)
        except OSError:
            pass


def _validate_token(
    endpoint: str,
    token: str,
    timeout: float,
    expected_source: str = "",
) -> bool:
    try:
        result = _json_request(
            _auth_endpoint(endpoint, "/auth/me"),
            timeout=timeout,
            token=token,
        )
        if not result.get("authenticated"):
            return False
        if expected_source:
            user = result.get("user")
            actual_source = normalize_source(
                user.get("auth_source") if isinstance(user, dict) else ""
            )
            return actual_source == normalize_source(expected_source)
        return True
    except RuntimeError:
        return False


def _emit_pairing_progress(
    on_event: Callable[[dict[str, Any]], None] | None,
    message: str,
    *,
    status: str = "active",
    **details: Any,
) -> None:
    event = {
        "schema": STREAM_SCHEMA,
        "type": "progress",
        "event": "pairing",
        "sequence": 0,
        "data": {
            "kind": "pairing",
            "stage": "authorization",
            "status": status,
            "message": message,
            **details,
        },
    }
    if on_event:
        on_event(event)
    else:
        print(f"[配对状态] {message}", file=sys.stderr, flush=True)


def _pair_for_token(
    endpoint: str,
    source: str,
    timeout: float,
    reason: str = "首次配对",
    on_event: Callable[[dict[str, Any]], None] | None = None,
) -> str:
    start = _json_request(
        _auth_endpoint(endpoint, "/auth/pairing/start"),
        {},
        timeout=timeout,
    )
    verification_uri = str(start.get("verification_uri") or "")
    user_code = str(start.get("user_code") or "")
    pairing_id = str(start.get("pairing_id") or "")
    pairing_secret = str(start.get("pairing_secret") or "")
    if not verification_uri or not pairing_id or not pairing_secret:
        raise RuntimeError("auth server did not return a valid pairing request")
    print(
        f"需要 GCID 配对（{reason}）：{verification_uri}（配对码 {user_code}）",
        file=sys.stderr,
    )
    pairing_timeout = float(os.getenv("GC_AI_SEARCH_PAIRING_TIMEOUT", "600"))
    try:
        webbrowser.open(verification_uri)
    except Exception:
        pass
    _emit_pairing_progress(
        on_event,
        "已打开 GCID 配对页面，请在页面中确认授权",
        verification_uri=verification_uri,
        user_code=user_code,
        timeout_seconds=int(pairing_timeout),
    )

    deadline = time.monotonic() + pairing_timeout
    interval = max(2.0, float(start.get("poll_interval") or 2))
    wait_started = time.monotonic()
    next_status_report = 0.0
    while time.monotonic() < deadline:
        result = _json_request(
            _auth_endpoint(endpoint, "/auth/pairing/token"),
            {"pairing_id": pairing_id, "pairing_secret": pairing_secret},
            timeout=timeout,
        )
        status = str(result.get("status") or "")
        if status == "approved" and result.get("access_token"):
            _emit_pairing_progress(
                on_event,
                "GCID 授权已完成，正在接收查询结果",
                status="completed",
                elapsed_seconds=round(time.monotonic() - wait_started, 1),
            )
            token = str(result["access_token"])
            _save_cached_token(token, endpoint, source)
            return token
        if status != "pending":
            _emit_pairing_progress(
                on_event,
                f"GCID 配对失败：{status or '未知状态'}",
                status="failed",
            )
            raise RuntimeError(f"GCID pairing failed: {status or 'unknown status'}")
        elapsed = time.monotonic() - wait_started
        if next_status_report == 0.0 or elapsed >= next_status_report:
            _emit_pairing_progress(
                on_event,
                f"正在等待 GCID 授权（已等待 {int(elapsed)} 秒）",
                elapsed_seconds=round(elapsed, 1),
                timeout_seconds=int(pairing_timeout),
            )
            next_status_report = elapsed + PAIRING_STATUS_REPORT_INTERVAL
        time.sleep(interval)
    _emit_pairing_progress(
        on_event,
        "配对等待超时，请返回 Skill 重新发起配对",
        status="failed",
        elapsed_seconds=round(time.monotonic() - wait_started, 1),
    )
    raise RuntimeError("GCID pairing timed out; please try again")


def _obtain_token(
    endpoint: str,
    source: str,
    timeout: float,
    auth_token: str = "",
    on_event: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[str, bool]:
    """Load a valid token, single-flight pairing when needed, and report whether paired."""
    if auth_token:
        _emit_pairing_progress(on_event, "正在校验提供的 GCID 授权 Token")
        if _validate_token(endpoint, auth_token, min(timeout, 15), expected_source=source):
            return auth_token, False
        _emit_pairing_progress(on_event, "提供的 GCID Token 已失效，需要重新配对", status="failed")
        raise RuntimeError("提供的 GCID Skill Token 无效或已撤销")

    cached = _load_cached_token(endpoint, source)
    if cached:
        _emit_pairing_progress(on_event, "正在校验已缓存的 GCID 授权")
        if _validate_token(endpoint, cached, min(timeout, 15), expected_source=source):
            return cached, False
        _emit_pairing_progress(on_event, "缓存的 GCID 授权已失效，准备重新配对")

    def report_lock_wait(elapsed: float, lock_age: float) -> None:
        _emit_pairing_progress(
            on_event,
            f"正在等待其他 Skill 进程完成授权（已等待 {int(elapsed)} 秒）",
            lock_wait=True,
            lock_age_seconds=round(lock_age, 1),
            elapsed_seconds=round(elapsed, 1),
        )

    with _token_lock(endpoint, source, on_wait=report_lock_wait):
        # Another process may have completed pairing while this process waited.
        cached = _load_cached_token(endpoint, source)
        if cached:
            _emit_pairing_progress(on_event, "正在再次校验其他进程刚更新的 GCID 授权")
            if _validate_token(endpoint, cached, min(timeout, 15), expected_source=source):
                return cached, False
        if cached:
            _clear_cached_token(endpoint, source, expected_token=cached)
        return _pair_for_token(endpoint, source, timeout, on_event=on_event), True


def _refresh_token_after_401(
    endpoint: str,
    source: str,
    timeout: float,
    rejected_token: str,
    on_event: Callable[[dict[str, Any]], None] | None = None,
) -> str:
    """Reuse another process's refreshed token before creating a new pairing."""
    with _token_lock(endpoint, source):
        cached = _load_cached_token(endpoint, source)
        if cached and cached != rejected_token and _validate_token(
            endpoint, cached, min(timeout, 15), expected_source=source
        ):
            return cached
        if cached:
            _clear_cached_token(endpoint, source, expected_token=cached)
        return _pair_for_token(
            endpoint,
            source,
            timeout,
            reason="查询返回 401 后重试",
            on_event=on_event,
        )


def _json_object(value: str, fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON argument: {exc}") from exc


def build_payload(
    question: str,
    *,
    conversation_id: str = "",
    session_index: int = 0,
    messages: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    question = question.strip()
    if not question:
        raise ValueError("question must not be empty")
    if len(question) > 1000:
        raise ValueError("question must be 1000 characters or fewer")

    conversation_id = conversation_id.strip() or str(uuid.uuid4())
    history = list(messages or [])
    history.append({"role": "user", "content": question})

    return {
        "keyword": question,
        "messages": history,
        "product": "",
        "extra_instruction": "",
        "search_hits": [],
        "search_completed": False,
        "session_id": str(uuid.uuid4()),
        "conversation_id": conversation_id,
        "session_index": int(session_index),
        "use_retrieval_tools": True,
        "source_channel": "",
        "source_product": "",
        "product_selection_source": "none",
        "enable_thinking": False,
    }


def _source_items(value: Any) -> list[dict[str, str]]:
    if isinstance(value, dict):
        value = value.get("sources") or value.get("references") or []
    if not isinstance(value, list):
        return []
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in value:
        if isinstance(item, str):
            url = item.strip()
            title = ""
        elif isinstance(item, dict):
            url = str(
                item.get("url") or item.get("source_url") or item.get("link") or ""
            ).strip()
            title = str(item.get("title") or item.get("name") or "").strip()
        else:
            continue
        if not url or url in seen:
            continue
        seen.add(url)
        result.append({"title": title, "url": url})
    return result


def _answer_sources(answer: str) -> list[dict[str, str]]:
    """Recover explicit links when the server puts them in answer Markdown."""
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    markdown_pattern = re.compile(r"\[([^\]]+)\]\((https?://[^)]+)\)")
    for match in markdown_pattern.finditer(answer):
        title, url = match.group(1).strip(), match.group(2).strip()
        if url and url not in seen:
            seen.add(url)
            result.append({"title": title, "url": url})
    url_pattern = re.compile(r"https?://[^\s<>\]\)\"']+")
    for match in url_pattern.finditer(answer):
        url = match.group(0).rstrip(".,;:!?，。；：！？、")
        if url and url not in seen:
            seen.add(url)
            result.append({"title": "来源", "url": url})
    return result


def _iter_sse_events(lines: Iterable[bytes | str]) -> Iterator[tuple[str, str]]:
    """Yield complete SSE events without buffering the whole answer."""
    event_name = "message"
    data_lines: list[str] = []

    def flush_event() -> tuple[str, str] | None:
        if not data_lines:
            return None
        return event_name, "\n".join(data_lines)

    for raw_line in lines:
        line = raw_line.decode("utf-8", errors="replace") if isinstance(raw_line, bytes) else str(raw_line)
        line = line.rstrip("\r\n")
        if not line:
            event = flush_event()
            if event is not None:
                yield event
            event_name = "message"
            data_lines = []
            continue
        if line.startswith(":"):
            continue
        field, separator, value = line.partition(":")
        if not separator:
            continue
        if value.startswith(" "):
            value = value[1:]
        if field == "event":
            event_name = value.strip() or "message"
        elif field == "data":
            data_lines.append(value)

    event = flush_event()
    if event is not None:
        yield event


def _consume_sse_events(
    events: Iterable[tuple[str, str]],
    on_event: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    answer = ""
    sources: list[dict[str, str]] = []
    progress: list[dict[str, Any]] = []
    status = "completed"
    reason = ""
    sequence = 0

    def emit(kind: str, event_name: str, data: Any) -> None:
        nonlocal sequence
        sequence += 1
        envelope = {
            "schema": STREAM_SCHEMA,
            "type": kind,
            "event": event_name,
            "sequence": sequence,
            "data": data,
        }
        if on_event is not None:
            on_event(envelope)

    for event_name, raw_data in events:
        try:
            data: Any = json.loads(raw_data)
        except json.JSONDecodeError:
            data = raw_data

        if event_name == "sources":
            event_sources = _source_items(data)
            sources.extend(event_sources)
            emit("sources", event_name, {"sources": event_sources})
            continue
        if event_name in {"tool_status", "reasoning_summary", "research_summary"}:
            if isinstance(data, dict):
                progress_item = {"kind": event_name, **data}
                progress.append(progress_item)
                emit("progress", event_name, progress_item)
            continue
        if event_name == "answer_replace":
            if isinstance(data, dict):
                answer = str(
                    data.get("text") or data.get("content") or data.get("answer") or ""
                )
                emit("answer_replace", event_name, {"text": answer})
            continue
        if event_name == "finish":
            if isinstance(data, dict):
                status = str(data.get("status") or status)
                reason = str(data.get("reason") or reason)
                emit("finish", event_name, {"status": status, "reason": reason})
            continue
        if event_name == "usage":
            continue
        if isinstance(data, dict):
            text = data.get("text") or data.get("content") or data.get("answer")
            if text:
                text_value = str(text)
                answer += text_value
                emit("answer_delta", event_name, {"text": text_value})
        elif isinstance(data, str) and data != "[DONE]":
            answer += data
            emit("answer_delta", event_name, {"text": data})

    return {
        "answer": answer,
        "sources": _source_items(sources),
        "status": status,
        "reason": reason,
        "progress": progress,
    }


def parse_sse(body: str) -> tuple[str, list[dict[str, str]], str, str]:
    """Parse a complete SSE payload for compatibility with simple callers."""
    parsed = _consume_sse_events(_iter_sse_events(body.splitlines(keepends=True)))
    return parsed["answer"], parsed["sources"], parsed["status"], parsed["reason"]


def request_search(
    endpoint: str,
    payload: dict[str, Any],
    timeout: float,
    token: str,
    on_event: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    request = Request(
        endpoint,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "Accept": "text/event-stream",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            def forward_event(event: dict[str, Any]) -> None:
                if on_event is None:
                    return
                on_event(
                    {
                        **event,
                        "conversation_id": payload["conversation_id"],
                        "session_id": payload["session_id"],
                        "session_index": payload["session_index"],
                    }
                )

            parsed = _consume_sse_events(
                _iter_sse_events(response),
                on_event=forward_event if on_event is not None else None,
            )
    except HTTPError as exc:
        detail = exc.read(500).decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"search server returned HTTP {exc.code}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"search server is unreachable: {exc.reason}") from exc
    except TimeoutError as exc:
        raise RuntimeError(f"search server timed out after {timeout:g}s") from exc

    answer = str(parsed["answer"] or "")
    sources = _source_items([*parsed["sources"], *_answer_sources(answer)])
    status = str(parsed["status"] or "")
    reason = str(parsed["reason"] or "")
    if status not in {"", "completed"}:
        raise RuntimeError(f"search stream finished with status={status}: {reason}")
    return {
        "success": True,
        "answer": answer.strip(),
        "sources": sources,
        "conversation_id": payload["conversation_id"],
        "session_id": payload["session_id"],
        "session_index": payload["session_index"],
        "status": status or "completed",
        "progress": parsed["progress"],
    }


def _text_result(result: dict[str, Any]) -> str:
    lines = ["回答：", str(result.get("answer") or "").strip()]
    sources = result.get("sources") or []
    lines.extend(["", "参考链接："])
    if not sources:
        lines.append("未返回参考链接")
    else:
        for source in sources:
            title = source.get("title") or "来源"
            lines.append(f"- {title}: {source['url']}")
    return "\n".join(lines)


def _human_stream_event(event: dict[str, Any]) -> None:
    event_type = event.get("type")
    data = event.get("data") or {}
    if event_type == "progress":
        kind = data.get("kind")
        prefix = (
            "研究摘要"
            if kind in {"reasoning_summary", "research_summary"}
            else "处理状态"
        )
        message = data.get("text") or data.get("message") or ""
        if message:
            print(f"[{prefix}] {message}", file=sys.stderr, flush=True)
    elif event_type == "sources":
        print(
            f"[参考资料] 已收到 {len(data.get('sources') or [])} 条来源",
            file=sys.stderr,
            flush=True,
        )
    elif event_type == "finish":
        status = str(data.get("status") or "completed")
        status_label = {
            "completed": "已完成",
            "failed": "失败",
            "cancelled": "已取消",
        }.get(status, status)
        print(f"[处理完成] {status_label}", file=sys.stderr, flush=True)


class _StreamOutput:
    def __init__(self, output_format: str):
        self.output_format = output_format
        self.sequence = 0

    def emit(self, event: dict[str, Any]) -> None:
        self.sequence += 1
        event = {**event, "sequence": self.sequence}
        if self.output_format == "jsonl":
            print(json.dumps(event, ensure_ascii=False), flush=True)
        elif self.output_format in {"json", "text"}:
            _human_stream_event(event)

    def result(self, result: dict[str, Any]) -> None:
        event = {
            "schema": STREAM_SCHEMA,
            "type": "result",
            "event": "result",
            "sequence": self.sequence + 1,
            "data": result,
        }
        if self.output_format == "jsonl":
            print(json.dumps(event, ensure_ascii=False), flush=True)

    def error(self, message: str) -> None:
        event = {
            "schema": STREAM_SCHEMA,
            "type": "error",
            "event": "error",
            "sequence": self.sequence + 1,
            "data": {"success": False, "error": message},
        }
        if self.output_format == "jsonl":
            print(json.dumps(event, ensure_ascii=False), flush=True)
        else:
            print(json.dumps(event["data"], ensure_ascii=False))


def main() -> int:
    parser = argparse.ArgumentParser(description="使用 GCID 授权访问 GC AI 知识库")
    parser.add_argument("--question", required=True)
    parser.add_argument("--conversation-id", default="")
    parser.add_argument("--session-index", type=int, default=0)
    parser.add_argument("--messages-json", default="")
    parser.add_argument(
        "--format",
        choices=("jsonl", "json", "text"),
        default="jsonl",
        help="jsonl 实时输出进度和回答事件；json 输出一个完整结果对象",
    )
    parser.add_argument("--auth-token", default=os.getenv("GC_AI_SEARCH_TOKEN", ""))
    parser.add_argument("--endpoint", default=os.getenv("GC_AI_SEARCH_ENDPOINT", DEFAULT_ENDPOINT))
    parser.add_argument("--timeout", type=float, default=float(os.getenv("GC_AI_SEARCH_TIMEOUT", "120")))
    args = parser.parse_args()

    output = _StreamOutput(args.format)
    try:
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except (AttributeError, OSError):
            pass
        source = DEFAULT_SOURCE
        token, paired_this_run = _obtain_token(
            args.endpoint,
            source,
            args.timeout,
            args.auth_token.strip(),
            on_event=output.emit,
        )
        messages = _json_object(args.messages_json, [])
        if not isinstance(messages, list) or any(not isinstance(item, dict) for item in messages):
            raise ValueError("messages-json must be a JSON array of objects")
        payload = build_payload(
            args.question,
            conversation_id=args.conversation_id,
            session_index=args.session_index,
            messages=messages,
        )
        try:
            result = request_search(
                args.endpoint,
                payload,
                args.timeout,
                token,
                on_event=output.emit,
            )
        except RuntimeError as exc:
            # A cached token can be revoked between preflight and search. If
            # this invocation already paired, stop instead of opening a second
            # browser window for the same question.
            if "HTTP 401" not in str(exc):
                raise
            if paired_this_run:
                raise RuntimeError(
                    "刚完成 GCID 配对后查询仍返回 HTTP 401，已停止重复配对；请检查登录状态或稍后重试"
                ) from exc
            token = _refresh_token_after_401(
                args.endpoint,
                source,
                args.timeout,
                token,
                on_event=output.emit,
            )
            result = request_search(
                args.endpoint,
                payload,
                args.timeout,
                token,
                on_event=output.emit,
            )
    except (ValueError, RuntimeError) as exc:
        output.error(str(exc))
        return 1

    if args.format == "text":
        print(_text_result(result))
    elif args.format == "json":
        print(json.dumps(result, ensure_ascii=False))
    else:
        output.result(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())

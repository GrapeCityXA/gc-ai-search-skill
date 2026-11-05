---
name: gc-ai-search
description: 通过 GCID 登录授权访问 GC AI 企业知识库，实时返回检索进度、状态摘要、完整回答和参考链接。适用于 WorkBuddy、Codex 等 Skill 宿主进行知识库问答，并支持在同一会话中继续追问。
---

# GC AI 搜索

使用 Skill 内置的 `scripts/search.py` 客户端执行只读知识库查询。客户端必须先完成 GCID 用户授权，严禁回退到匿名访问。

## 请求流程

使用最新的问题调用脚本。首次请求前，脚本会启动 AI 搜索配对流程并打开配对页面，用户在页面中完成正式环境 GCID 登录和授权。默认通过正式环境发送短期 Bearer Token：

`https://ai-assist.grapecity.com.cn/api/rag/deep_research_streaming/`

如需切换到其他环境，再通过 `GC_AI_SEARCH_ENDPOINT` 覆盖默认地址。

如果用户尚未登录，配对页面会跳转到 GCID 登录页。不得删除认证步骤，也不得在没有 Bearer Token 的情况下调用搜索接口。缓存 Token 最长有效期为 8 小时，但 GCID 退出或 AI 搜索退出可以提前撤销它。

如果未提供 `conversation_id` 和 `session_id`，脚本会自动生成 UUID。继续追问时，复用返回的 `conversation_id`，递增 `session_index`，并将包含历史助手回答的完整 `messages` 列表传入。

Token 缓存按 endpoint 统一保存，并通过跨进程锁保护。同一个请求在刚完成配对的 Token 被拒绝后不会再次打开配对窗口，而是直接返回认证错误。

示例：

```powershell
python scripts/search.py --question "如何配置某功能？" --format jsonl
```

继续追问示例：

```powershell
python scripts/search.py `
  --question "这个配置在哪个页面？" `
  --conversation-id "<previous conversation_id>" `
  --session-index 1 `
  --format jsonl `
  --messages-json '[{"role":"user","content":"如何配置某功能？"},{"role":"assistant","content":"上一轮回答"}]'
```

默认输出格式为 NDJSON（`--format jsonl`）。应立即读取 stdout 的每一行，并按照 `type` 字段分发：

- 将 `progress` 事件显示在宿主的状态或活动区域。`tool_status` 表示运行状态；`reasoning_summary` 和 `research_summary` 是服务端提供的安全摘要，不是隐藏思维链。
- 配对阶段也会发送 `pairing` 进度事件，说明授权页是否已打开、是否正在等待其他 Skill 进程、已等待时长和最终结果；这些是用户可见的运行状态。
- 将 `answer_delta` 持续写入回答区域；收到 `answer_replace` 时替换已累积的回答。
- 将 `sources` 与回答正文分开展示。
- 以最终的 `result` 事件为权威结果，其中包含完整回答、来源、进度、会话标识和终止状态。
- 将 `error` 显示为失败的工具结果。

使用 `--format json` 获取一个完整的 JSON 结果；实时进度会通过 stderr 以简洁中文状态显示，JSON 结果中包含完整的 `progress` 数组。使用 `--format text` 获取适合人类阅读的回答和状态。

## 返回规则

- 返回回答文本，但不得改写其含义。
- 返回 `sources` SSE 事件中的完整 URL。
- 如果服务将 Markdown 链接或普通 URL 放在回答中而没有发送 `sources` 事件，应提取这些明确链接，去重后作为参考链接返回。
- 按 URL 对来源去重。
- 服务未返回来源时，不得自行编造 URL，应明确说明未返回参考链接。
- 保存返回的 `conversation_id`，供后续问题继续使用。
- 脚本运行期间持续转发进度，不要等到 `result` 事件后才首次说明任务已开始。
- 不得将 `reasoning_summary` 或 `research_summary` 当作原始模型思维链，应标记为状态或研究摘要。
- HTTP 错误、超时和失败的流结束都应视为失败，并返回简明错误。
- Token 缺失、过期、撤销或被拒绝时，应视为认证失败，清理本地缓存并重新配对，严禁匿名重试。

## 配置

可选环境变量：

- `GC_AI_SEARCH_ENDPOINT`：完整 API 地址，默认使用上面的正式环境地址。
- `GC_AI_SEARCH_TIMEOUT`：请求超时时间，单位为秒，默认 `120`。
- `GC_AI_SEARCH_TOKEN`：可选的短期 Token。脚本将 Token 缓存在 `GC_AI_SEARCH_TOKEN_FILE` 或 `~/.gc-ai-search/token.json`，服务端可以在过期前撤销缓存 Token。

Skill 的授权来源固定为 `external_skill`，不按 WorkBuddy、Codex 或其他宿主区分。客户端不接受 `--source` 或 `GC_AI_SEARCH_SOURCE` 配置；授权主体始终是 GCID 用户身份。

此 Skill 不直接使用 Qdrant、LLM、Embedding、ETL、Cookie 或静态 API Key，而是使用绑定到授权用户的短期 AI Search Bearer Token。

请求和 SSE 协议详见 [references/api-contract.md](references/api-contract.md)。

# GC AI 搜索 API 协议

接口地址：

```text
POST https://ai-assist.grapecity.com.cn/api/rag/deep_research_streaming/
Content-Type: application/json
Accept: text/event-stream
```

Skill 授权来源固定为 `external_skill`。配对和 Token 校验以 GCID 用户身份为准，不接受调用宿主传入的客户端来源名称。

如需切换到其他环境，可通过 `GC_AI_SEARCH_ENDPOINT` 覆盖接口地址。

服务端接受 `ChatModel` 结构，最小兼容请求体如下：

```json
{
  "keyword": "用户问题",
  "messages": [
    {"role": "user", "content": "用户问题"}
  ],
  "product": "",
  "extra_instruction": "",
  "search_hits": [],
  "search_completed": false,
  "session_id": "uuid-v4",
  "conversation_id": "uuid-v4",
  "session_index": 0,
  "use_retrieval_tools": true,
  "source_channel": "",
  "source_product": "",
  "product_selection_source": "none",
  "enable_thinking": false
}
```

继续追问时，保持 `conversation_id` 不变，递增 `session_index`，并将之前的用户和助手消息放入 `messages`。每次请求都生成新的 `session_id`。

响应采用 Server-Sent Events，相关事件如下：

```text
event: tool_status
data: {"stage":"retrieval","status":"active","message":"正在查询相关产品资料"}

event: reasoning_summary
data: {"text":"正在比较检索到的资料","sequence":1}

event: research_summary
data: {"text":"已扩大检索范围","sequence":2}

event: sources
data: {"sources":[{"title":"...","url":"https://..."}]}

data: {"text":"回答片段"}

event: answer_replace
data: {"text":"替换后的完整回答"}

event: finish
data: {"status":"completed","reason":"stop"}
```

为保持兼容，客户端也接受 `content` 和 `answer` 作为文本字段。Usage 事件仅供内部使用，不会对外暴露。

## Skill NDJSON 输出

默认的 `--format jsonl` 使用 `gc-ai-search.stream.v1` 协议，每行输出一个 JSON 对象，并立即刷新：

```json
{"schema":"gc-ai-search.stream.v1","type":"progress","event":"tool_status","sequence":1,"data":{"kind":"tool_status","stage":"retrieval","status":"active","message":"正在查询相关产品资料"}}
{"schema":"gc-ai-search.stream.v1","type":"progress","event":"reasoning_summary","sequence":2,"data":{"kind":"reasoning_summary","text":"正在比较检索到的资料","sequence":1}}
{"schema":"gc-ai-search.stream.v1","type":"answer_delta","event":"message","sequence":3,"data":{"text":"回答片段"}}
{"schema":"gc-ai-search.stream.v1","type":"result","event":"result","sequence":4,"data":{"success":true,"answer":"完整回答","sources":[],"progress":[],"conversation_id":"...","session_id":"...","session_index":0,"status":"completed"}}
```

所有流事件还包含 `conversation_id`、`session_id` 和 `session_index`。事件类型如下：

- `progress`：工具状态、安全的思考/研究摘要，或 GCID 配对状态。配对状态使用 `kind: pairing`，可包含授权页地址、配对等待时长和锁等待信息。
- `answer_delta`：追加可见回答文本。
- `answer_replace`：替换已累积的全部回答文本。
- `sources`：更新参考链接。
- `finish`：服务端终止状态。
- `result`：权威的完整结果和进度历史。
- `error`：客户端或服务端终止错误。

宿主应将 `progress` 与回答正文分开展示。这些摘要是有边界的状态信息，不得描述为隐藏思维链。

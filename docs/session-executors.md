# Session drivers and client tools

The gateway is completely driver-agnostic: it dispatches all providers to isolated external driver binaries over stdio (`drivers/http`, `drivers/acp`, `drivers/web`, or any custom `<name>-driver` / `command` executable) using a single unified NDJSON wire protocol and client engine in `internal/executor/external`.
`driver` specifies the driver binary name (defaults to `"http"` when both `driver` and `command` are omitted, resolved to `<driver>-driver` via `external.ResolveDriverBinary`), or can be omitted when an explicit `command` executable is provided.

Concrete driver documentation lives alongside each driver's source code:
- [HTTP Provider Driver (`drivers/http/README.md`)](../drivers/http/README.md) — stateless multiplexed HTTP/API driver (`http-driver`) powered by Bifrost Core.
- [ACP Agent Driver (`drivers/acp/README.md`)](../drivers/acp/README.md) — stateful ACP agent session driver (`acp-driver`) powered by `agentrun`.
- [Web CDP Driver (`drivers/web/README.md`)](../drivers/web/README.md) — stateful Rust CDP browser automation driver (`web-driver`) with YAML site profiles.

Driver-specific diagrams: [HTTP request flow](../drivers/http/README.md#architecture-and-request-flow),
[ACP sessions and MCP tools](../drivers/acp/README.md#architecture-and-tool-turns),
and [Web sessions and prompt tools](../drivers/web/README.md#architecture-and-tool-turns).

Driver-specific settings (such as `base_provider`, `inference_url`, `api_key`, `strip_params`, `set_params`, `profile`, `cdp_url`, `account`, `state_dir`, `heal`, etc.) are stored opaquely by the gateway in `Provider.Config` (with recursive `env.VAR` secret expansion) and validated by the driver itself during the `{"type":"init",...}` handshake, which also returns the provider's `Capabilities` (`chat`, `responses`, `streaming`, `tools`, `stateful`, `resume`, `safe_replay`).

When using a stateful session driver (`capabilities.stateful == true`), configure:
- `driver`: driver name (e.g. `"acp"`, `"web"`) or explicit `command`.
- `tool_strategy`: `"auto"` (default), `"mcp"` (session-scoped MCP bridge), `"prompt"` (text prompt injection + JSON response codec), `"wire"` (structured `tool_call` and `tool_result` events without prompt injection), or `"both"` (prompt + wire).

The `"acp"` driver supports only `"auto"` and `"mcp"`; both deliver tools through MCP.
The `"web"` and `"custom"` drivers support all listed strategies; `"auto"` selects `"both"`.

Start with [config.sessions.example.json](../configs/config.sessions.example.json).
Replace the example commands and paths before serving. Relative
`session.default_cwd`, `session.allowed_roots`, and top-level `session_file` paths
resolve against the gateway working directory at startup. `check` validates
configuration without launching agents, browsers or MCP processes. Commands are
argument arrays passed directly to exec; no shell interpretation is performed.

## Responsibilities

- `internal/executor`: provider registry, capabilities and attempt middleware.
- `internal/session`: engine-independent conversations, history checkpoints,
  native-session restore, pending tool calls, cancellation and turn deadlines.
- `internal/executor/session`: OpenAI request/result and SSE adaptation.
- `internal/executor/external`: unified NDJSON driver client supporting both stateless multiplexed HTTP drivers (`http-driver`) and stateful session drivers (`acp-driver`, `web-driver`, custom drivers) with configurable tool delivery strategies (MCP, prompt codec, wire).
- `drivers/http`, `drivers/acp`, `drivers/web`: isolated external driver implementations (each with its own dependency tree and README; `drivers/acp` owns the private stdio MCP bridge and session facade).

All session drivers implement `session.Engine` / `session.Session` via `external.DriverEngine`. The core gateway binary is pure Go standard library with zero external SDK dependencies (`bifrost`, `agentrun`, or `modelcontextprotocol`).

Session and tool lifecycle code is adapted from agentrun-openai. Its second HTTP
server, authentication and public CLI have not been copied. The source project
is unchanged.

## Pi and other OpenAI clients

Send `tools` on every request. They describe the client's own functions. The
client remains responsible for execution. For a consistent conversation send
`X-Session-Affinity` (or `Session-ID`, `session_id`, `X-Client-Request-ID`, or JSON
`session_id`, in that precedence order). Use a stable conversation ID rather than
a unique ID for every request. When omitted, the gateway returns a newly generated
`X-Session-ID`; copy it into the next request's affinity header or JSON session_id.

Optional `X-Agent-CWD` overrides `session.default_cwd`. The path must exist and
be absolute. With `allowed_roots` configured, symlinks are resolved before checking
containment. Session identity is scoped by authenticated client key ID, logical
model, provider runtime, native model and reasoning effort. Keyless requests share
one anonymous principal.

The exchange is:

1. Client sends `messages` and `tools`.
2. The session driver receives tools according to `tool_strategy` (a session-scoped MCP server, prompt-injected tool definitions, or structured `tools` in `start`).
3. An executor's function request becomes OpenAI `tool_calls` with gateway call IDs
   and `finish_reason: "tool_calls"`. A streaming exchange ends with `[DONE]`.
4. The native turn remains alive, waiting for results.
5. Client appends the exact assistant message and a `role: "tool"` message for
   each returned call, using its `tool_call_id`, and sends the full history with
   the same conversation ID, model, working directory and tool definitions.
6. Results release the waiting calls. The same native turn continues and may
   request more tools before returning `finish_reason: "stop"`.

Unknown, duplicate, stale or incomplete tool results are rejected before any
results in that batch are delivered. Correctable errors leave the pending turn
available. Concurrent calls can be returned together or in separate exchanges;
return every call delivered in an assistant response. Gateway-generated IDs are
independent of native driver IDs.

`tool_choice` accepts `auto`, `none`, `required`, and named function objects.
Forbidden function calls are rejected. A backend completing without a required
call fails the exchange. The gateway validates the tool envelope and JSON object
arguments; schema-constrained generation remains the engine's responsibility.

## Lifecycle and routing

The runtime compares canonical history prefixes and sends only new messages to
an existing native session. Editing history, changing tools or changing CWD
recreates the session. Full history supplied to a fresh session includes prior
tool calls/results as historical context; they do not resolve live pending calls.

`session.ttl` defaults to 10 minutes. Idle eviction releases completed native
sessions; pending tool turns live until `session.turn_timeout` (30 minutes by
default). That deadline includes time spent waiting for pi. Disconnection during
an attached exchange cancels its turn; completing an exchange with tool_calls
**detaches** it from that HTTP request. Shutdown cancels all native work.

`session_file` (configured at the top level of the gateway config, alongside `affinity_file`) enables atomic checkpoint persistence across all stateful providers. The file contains
native resume IDs, CWD, history count/hash and toolset hash, not prompts or tool
results. Pending work is never
persisted. This checkpoint format is version 2; use a new file path when moving
from agentrun-openai, whose version 1 keys do not include gateway client/model
identity. Legacy files are rejected rather than silently reusing another scope. Completed sessions can resume after eviction/restart; a missing native
session falls back once to the supplied conversation history.

Stateful routes currently require exactly one target and explicit `race count: 1`.
Retry, fallback, hedge, continue and repetition recovery are rejected by config
validation, including incoming transitions to stateful routes. They cannot be
made safe by waiting for a first token: an agent/browser may already have changed
its native state. Forking, reconciliation and idempotency guarantees would be
needed to relax these restrictions. State-uncertain failures are not advertised
as safely retryable.

Session drivers (`acp`, `web`, `custom`) accept text Chat Completions, including text-part
arrays. They reject Responses requests and non-text content. Stateless HTTP drivers (`http`) preserve full Chat Completions and Responses API support.

Attempt middleware can prepare an isolated request body, observe stream events
and receive completion callbacks. Completion runs when the upstream stream ends
or cancels, rather than when a channel is returned. `session.heartbeat` defaults to 20 seconds (a negative duration disables it).
Heartbeats remain
non-meaningful for selection/TTFT and do not enter stored conversation history.
Existing compiled rules retain their behavior.

## Session NDJSON driver protocol

Stateful session drivers (`acp-driver`, `web-driver`, and `"driver": "custom"` executables) communicate with the gateway over `stdin` and `stdout` using one JSON object per line (maximum inbound line size 16 MiB). `stdout` is reserved for protocol messages; `stderr` is discarded so credentials and page contents are not copied into gateway logs.

### Model discovery (`models`)

Before or independent of starting a session, the gateway can probe available native models:

```json
{"type":"models"}
```

The driver replies:

```json
{"type":"models","models":[{"id":"default"}]}
```

### Session handshake (`start`) and turns (`prompt`)

Gateway starts a process with the resolved working directory and sends:

```json
{"type":"start","model":"default","effort":"","system":"...","resume_id":"","tools":[]}
```

The driver replies `{"type":"ready","resume_id":"optional-native-id"}`.
If a requested native session is missing, reply `{"type":"session_not_found"}`.
The driver should make its restored conversation ready before emitting `ready`.
If the backend assigns its ID only after the first prompt, emit
`{"type":"session_updated","resume_id":"..."}` before
`message`/`done`. The gateway records it using the existing session checkpoint.
For strict restore, return an `error` event instead of `session_not_found`;
`session_not_found` explicitly allows the runtime to create a replacement session.
Gateway then sends `{"type":"prompt","text":"..."}` for each native turn.

Driver output events:

| Type | Fields | Meaning |
| --- | --- | --- |
| `text` | `text` | Visible text delta |
| `reasoning` | `text` | Reasoning delta |
| `heartbeat` | — | Liveness without model progress |
| `usage` | `input_tokens`, `output_tokens` | Usage snapshot for the current exchange |
| `tool_call` | `id`, `name`, `arguments` object | Structured function request |
| `message` | `id`, `text` | Complete text-tool JSON response |
| `session_updated` | `resume_id` | Update the native session checkpoint after the backend assigns a session/thread ID |
| `done` | — | Native turn has completed |
| `error` | `error` | Native failure |

### Prompt-based and wire-based tool calls

When `tool_strategy` includes prompt injection (`"prompt"` or `"both"`), the gateway appends function definitions and instructions to the initial system prompt. A `message.text` must contain exactly this JSON shape, without Markdown fences:

```json
{"content":"optional text","tool_calls":[{"name":"lookup","arguments":{"id":42}}]}
```

A final answer uses an empty `tool_calls` array. Structured `tool_call` events avoid
text parsing when the driver already offers native function calling (`"wire"` or `"both"`). Do not emit requested
tool calls as ordinary `text` events: that would display them as answer text.

When the client supplies a tool result for a wire or prompt tool call, the driver receives:

```json
{"type":"tool_result","id":"native-call-id","content":"result from client"}
```

For calls parsed from `message`, IDs are `<message.id>/<zero-based-call-index>`.
The driver feeds that result back to the backend and continues generating events
for the same turn. Emit `done` after the final answer, not while waiting for a
tool result. `text`/`reasoning` events are deltas; do not repeat a final snapshot
of already streamed text. Native processes are terminated on cancellation or
session eviction, and each fresh process receives a new `start` handshake.

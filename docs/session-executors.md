# Session executors and client tools

The gateway accepts OpenAI Chat Completions and executes providers in-process.
`executor` selects `bifrost` (the default), `acp`, or `web`. Existing HTTP
providers, including a separately running agentrun-openai, continue to use the
ordinary Bifrost/OpenAI configuration. There is no special HTTP connection to
agentrun-openai in the native executor path.

Start with [config.sessions.example.json](../configs/config.sessions.example.json).
Replace the example commands and absolute paths before serving. `check` validates
configuration without launching agents, browsers or MCP processes. Commands are
argument arrays passed directly to exec; no shell interpretation is performed.

## Responsibilities

- `internal/executor`: provider registry, capabilities and attempt middleware.
- `internal/session`: engine-independent conversations, history checkpoints,
  native-session restore, pending tool calls, cancellation and turn deadlines.
- `internal/executor/session`: OpenAI request/result and SSE adaptation.
- `internal/executor/acp`: agentrun engine and session-scoped MCP facade.
- `internal/executor/web`: direct browser-driver adapter and text tool codec.
- `internal/mcpbridge`: private stdio MCP subprocess mode of llm-gateway.

Both native engines implement `session.Engine` / `session.Session`. A web engine
does not implement ACP or depend on agentrun. Bifrost retains native API function
calling: there is no reason to route API tool calls through MCP internally.

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
2. ACP receives tools through a private MCP server. A browser driver receives
   definitions and tool-format instructions.
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

ACP agents may also expose and execute their own tools. Publishing pi's tools
through MCP does not disable an agent's native tools or its agent loop. The ACP
adapter preserves agentrun-openai's noninteractive HITL-off behavior. Use a model
or browser executor when pi must own all tool execution.

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

`session.store_path` enables atomic checkpoint persistence. The file contains
native resume IDs, CWD, history count/hash and toolset hash, not prompts or tool
results. Use a different store path for every provider. Pending work is never
persisted. This checkpoint format is version 2; use a new store path when moving
from agentrun-openai, whose version 1 keys do not include gateway client/model
identity. Legacy files are rejected rather than silently reusing another scope. Completed sessions can resume after eviction/restart; a missing native
session falls back once to the supplied conversation history. Browser drivers
must implement restore to retain their native conversation.

Stateful routes currently require exactly one target and explicit `race count: 1`.
Retry, fallback, hedge, continue and repetition recovery are rejected by config
validation, including incoming transitions to stateful routes. They cannot be
made safe by waiting for a first token: an agent/browser may already have changed
its native state. Forking, reconciliation and idempotency guarantees would be
needed to relax these restrictions. State-uncertain failures are not advertised
as safely retryable.

ACP and web executors currently accept text Chat Completions, including text-part
arrays. They reject Responses requests and non-text content. Bifrost's existing
Chat/Responses behavior is preserved. Use native `default` to leave ACP model
selection to the agent. Concrete ACP models can map discovered bracketed effort
variants to `reasoning_effort`; single-target logical models advertise those
efforts through `/v1/models` when discovery succeeds.

Attempt middleware can prepare an isolated request body, observe stream events
and receive completion callbacks. Completion runs when the upstream stream ends
or cancels, rather than when a channel is returned. `session.heartbeat` defaults to 20 seconds (a negative duration disables it).
Heartbeats remain
non-meaningful for selection/TTFT and do not enter stored conversation history.
Existing compiled rules retain their behavior.

## Direct browser-driver protocol

The gateway includes a driver adapter, not a site-specific browser automation
implementation. There is no bundled Muse driver. A driver runs the browser,
authenticates to its site, selects the model and maps these messages to that
site's conversation UI or native transport. Its lifecycle is owned by the gateway.

The executable's stdin and stdout carry one JSON object per line (maximum inbound
line size 16 MiB). Stdout is reserved for protocol messages. Stderr is discarded
so credentials and page contents are not copied into gateway logs.

Gateway starts a process with the resolved working directory and sends:

```json
{"type":"start","model":"default","system":"...","resume_id":"","tools":[]}
```

The driver replies `{"type":"ready","resume_id":"optional-native-id"}`.
If a requested native session is missing, reply `{"type":"session_not_found"}`.
The driver should make its restored conversation ready before emitting `ready`.
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
| `done` | — | Native turn has completed |
| `error` | `error` | Native failure |

For a text-only model, the gateway appends function definitions and instructions
to the initial system prompt. A `message.text` must contain exactly this JSON
shape, without Markdown fences:

```json
{"content":"optional text","tool_calls":[{"name":"lookup","arguments":{"id":42}}]}
```

A final answer uses an empty `tool_calls` array. Structured tool requests avoid
text parsing when the site already offers function calling. Do not emit requested
tool calls as ordinary `text` events: that would display them as answer text.

When pi supplies a result, the driver receives:

```json
{"type":"tool_result","id":"native-call-id","content":"result from pi"}
```

For calls parsed from `message`, IDs are `<message.id>/<zero-based-call-index>`.
The driver feeds that result back to the website and continues generating events
for the same turn. Emit `done` after the final answer, not while waiting for a
tool result. `text`/`reasoning` events are deltas; do not repeat a final snapshot
of already streamed text. Native processes are terminated on cancellation or
session eviction, and each fresh process receives a new `start` handshake.

# ACP Agent Driver (`acp-driver`)

Standalone Go module implementing the stateful Agent Client Protocol (ACP) session driver for `llm-gateway`, powered by [`github.com/dmora/agentrun`](https://github.com/dmora/agentrun).

## Source layout

```text
drivers/acp/
  go.mod            Standalone Go module (isolates agentrun ACP backend dependencies)
  main.go           NDJSON stdio loop for `models` discovery and `start`/`prompt` sessions
  engine.go         agentrun ACP process lifecycle, MCP server injection, model[effort] resolution
  engine_test.go    Unit and subprocess integration tests (models, effort, restore, effective model)
```

## Build and test

From the repository root:

```sh
# Build into bin/acp-driver (included in `make build`)
make build-acp

# Run standalone module tests with race detector
go test -race -C drivers/acp ./...
```

When `llm-gateway` resolves a provider with `"driver": "acp"`, [`external.ResolveDriverBinary`](../../internal/executor/external/resolve.go) locates `acp-driver` next to the gateway executable, in `./bin/acp-driver`, on `$PATH`, or builds `./drivers/acp` on demand in a source checkout, and passes the configured ACP agent `command` to `acp-driver -- <command...>`.

## Gateway configuration

```json
{
  "id": "acp-codex",
  "driver": "acp",
  "tool_strategy": "mcp",
  "command": ["codex-acp"],
  "env": {
    "OPENAI_API_KEY": "env.OPENAI_API_KEY"
  },
  "session": {
    "default_cwd": ".",
    "allowed_roots": ["."],
    "ttl": "10m",
    "turn_timeout": "30m",
    "heartbeat": "20s",
    "store_path": "./state/acp-codex-sessions.json"
  }
}
```

- `driver`: `"acp"`.
- `tool_strategy`: `"auto"` (default) or `"mcp"`. Both deliver client tools to the ACP agent through the gateway's session-scoped stdio MCP bridge (`mcp_servers` in the `start` handshake).
- `command`: Executable and arguments of the target ACP agent binary (passed directly via `exec`, no shell interpretation).

## Behavior and ACP specifics

### Non-interactive HITL-off mode

`acp-driver` spawns the underlying ACP agent with `agentrun.WithHITL(agentrun.HITLOff)`. Publishing a client's (e.g., Pi's) tools through MCP does not disable the ACP agent's native built-in tools or its internal agent loop. When the client must exclusively own all tool execution, use an HTTP model or browser driver instead.

### Model discovery and bracketed `model[effort]` resolution

- Before starting a conversation, the gateway can query available models by sending `{"type":"models"}` to `acp-driver`.
- `acp-driver` probes the ACP agent with an initial session, reads `EventInit.AvailableModels`, and replies with `{"type":"models","models":[...]}`.
- Use native model `"default"` to leave model selection to the ACP agent.
- Concrete ACP models can expose reasoning effort variants using bracket syntax (e.g. `gpt-5[low]`, `gpt-5[high]`). When a request specifies a base model and `reasoning_effort`, the gateway and `acp-driver` resolve the corresponding `model[effort]` variant and verify that the ACP agent actually switches to the requested effective model. Single-target logical models advertise discovered reasoning efforts through `/v1/models`.

### Native session restore (`resume_id`)

- When `resume_id` is supplied in `{"type":"start"}`, `acp-driver` sets `agentrun.OptionResumeID`.
- If the ACP agent reports that the session does not exist (`session not found`, `unknown session`, `invalid session`, etc.), `acp-driver` emits `{"type":"session_not_found"}` so the gateway's session runtime can automatically replay the conversation history into a fresh native session.
- If the agent assigns or updates its session ID via `EventInit.SessionID`, `acp-driver` emits `{"type":"session_updated","resume_id":"..."}` before completing the turn.

### Process lifecycle

- Each active conversation session owns one `acp-driver` process and its child ACP agent process.
- When `stdin` closes (EOF) or the process receives `SIGINT`/`SIGTERM`, `acp-driver` cancels the active turn and terminates the child ACP agent process cleanly.

## NDJSON wire protocol

See the shared [Session NDJSON Driver Protocol](../../docs/session-executors.md#session-ndjson-driver-protocol) for the full message reference (`start`, `prompt`, `models`, `ready`, `session_not_found`, `session_updated`, `text`, `reasoning`, `usage`, `done`, `error`).

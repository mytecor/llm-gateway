# LLM Gateway

An OpenAI-compatible gateway in Go featuring native ACP/web executors and API providers powered by [Bifrost Core](https://github.com/maximhq/bifrost).

The gateway encapsulates upstream LLM providers: clients interact with virtual logical models (e.g., `standard`, `fast`) without needing to know physical model identifiers, provider endpoints, internal URLs, or API keys.

## Features

- **OpenAI Compatibility:** Full support for `/v1/chat/completions` and `/v1/responses` in both standard and streaming (SSE) modes.
- **Logical Models:** Decouples client requests from physical provider models, endpoints, and credentials.
- **Declarative Routing:** Composable pipeline stages (`filter`, `map`, `admission`, `balance`, `affinity`, `race`, `retry`, `hedge`, `timeout`).
- **Load Balancing & Resilience:** Dynamic balancing via expected TTFT (EWMA), automatic cooldown on upstream failures (429, 5xx), multi-tier fallbacks, and request hedging.
- **Stream Lifecycle Management:** Mid-stream break detection and continuation, loop guard (repetition detection), and chunk idle timeouts.
- **Observability:** Prometheus metrics served on a dedicated loopback listener and structured JSON logging (`log/slog`) with end-to-end `request_id` tracing.
- **Security:** Client authorization via Bearer tokens and safe secret ingestion using environment variables (`env.VARIABLE_NAME`).

---

## Native ACP and Web Executors

In addition to standard API providers, the gateway executes ACP agents directly via `agentrun` and connects to browser drivers without an intermediate HTTP service. A unified runtime manages conversations, history checkpoints, session resume, and pending tool calls. Tools provided in OpenAI `tools` are executed by the client (e.g., Pi); the gateway returns `tool_calls` and forwards the received tool results to the waiting engine. For ACP, this operates through a session-scoped MCP bridge.

Existing providers use `executor: "bifrost"` by default. Native executors are selected via `executor: "acp"` or `"web"` with `command` and `session`. They support the text Chat Completions API; stateful routes require a single target, `race count: 1`, and disabled retry/fallback/hedge/continue/repetition.

- [Session Configuration Example](configs/config.sessions.example.json)
- [Sessions, Tools, and Browser Driver Protocol Documentation](docs/session-executors.md)
- [Architecture Deep-Dive](ARCHITECTURE.md)

*Note: Drivers for specific websites must be implemented separately; a Muse driver is not included.*

---

## API Endpoints

| Method | Path | Description | Authorization |
| --- | --- | --- | --- |
| `POST` | `/v1/chat/completions` | OpenAI Chat Completions (streaming / non-streaming) | Bearer Token |
| `POST` | `/v1/responses` | OpenAI Responses API (streaming / non-streaming) | Bearer Token |
| `GET` | `/v1/models` | List available logical models | Bearer Token |
| `GET` | `/healthz` | Health check (`{"status":"ok"}`) | None |
| `POST` | `/admin/models/refresh` | Force refresh model catalogs | Bearer Token |
| `GET` | `/metrics` | Prometheus metrics (default `127.0.0.1:9209`) | Loopback, None |

When `client_api_keys` are configured, `/v1/*` and `/admin/*` endpoints require the header:

```text
Authorization: Bearer <client_api_key>
```

---

## Quick Start

### Building

```sh
# Build binary to bin/llm-gateway
make build

# Or directly with Go:
go build -o bin/llm-gateway ./cmd/llm-gateway
```

### Validating Configuration

```sh
./bin/llm-gateway --config configs/config.example.json check
# or using make:
make check-config
```

### Running the Gateway

```sh
./bin/llm-gateway --config configs/config.example.json serve
```

---

## Configuration

Configuration is specified in JSON format (see [configs/config.example.json](configs/config.example.json) for a comprehensive example). Secret values support loading from environment variables using the `env.` prefix (e.g., `"api_key": "env.OPENAI_API_KEY"`).

### Example Configuration

```json
{
  "host": "127.0.0.1",
  "port": 9208,
  "metrics_host": "127.0.0.1",
  "metrics_port": 9209,
  "log_level": "info",
  "catalog_refresh_interval": "10m",
  "client_api_keys": [
    {
      "id": "app-client",
      "key": "env.CLIENT_API_KEY"
    }
  ],
  "providers": [
    {
      "id": "upstream-primary",
      "base_provider": "openai",
      "inference_url": "https://api.openai.com/v1",
      "api_key": "env.OPENAI_API_KEY",
      "priority": 10,
      "cooldown": "15s",
      "request_timeout": "60s"
    },
    {
      "id": "upstream-backup",
      "base_provider": "openai",
      "inference_url": "https://api.backup-provider.com/v1",
      "api_key": "env.BACKUP_API_KEY",
      "priority": 5,
      "cooldown": "15s",
      "request_timeout": "60s"
    }
  ],
  "rules": [
    { "route": "standard", "action": "filter", "where": { "model": { "eq": "standard" } } },
    { "route": "standard", "action": "filter", "where": { "provider": { "in": ["upstream-primary", "upstream-backup"] } } },
    { "route": "standard", "action": "map", "native": "gpt-4o" },
    { "route": "standard", "action": "rank", "strategy": "priority" },
    { "route": "standard", "action": "race", "count": 1 },
    {
      "route": "standard",
      "action": "retry",
      "target": "standard.retry",
      "attempts": 2,
      "backoff": { "type": "exponential", "initial": "200ms", "max": "1s" }
    },
    { "route": "standard", "action": "timeout", "duration": "60s" },

    { "route": "standard.retry", "action": "filter", "where": { "error": { "in": ["429", "5xx", "timeout", "connection_error"] } } },
    { "route": "standard.retry", "action": "filter", "where": { "provider": { "in": ["upstream-primary", "upstream-backup"], "unused": true } } },
    { "route": "standard.retry", "action": "map", "native": "gpt-4o" },
    { "route": "standard.retry", "action": "rank", "strategy": "priority" },
    { "route": "standard.retry", "action": "race", "count": 1 }
  ]
}
```

---

## Rules

Routing is configured as a flat list of rules (`rules`). Each rule targets a specific route (`route`) and executes a concrete action (`action`):

```text
filter → map → admission → balance → affinity → race → retry → hedge → timeout
```

- **`filter`**: Filters requests by logical model name (`where.model.eq`), provider list (`where.provider.in`), error class (`where.error.in`), or usage status (`unused: true`).
- **`map`**: Maps a virtual model to a provider's native model name (`native`), with optional tier assignment (`tier`).
- **`admission`**: Enforces concurrency and queue bounds (`maxInFlight`, `maxPending`, `waitTimeout`).
- **`balance`**: Selects targets dynamically (e.g., predicted Time to First Token: `expected-ttft` with $\epsilon$-greedy exploration).
- **`affinity`**: Binds Responses API sequences to the winning provider using a session identifier.
- **`race`**: Executes parallel requests (`count`) across top-ranked providers (first successful response wins).
- **`retry`**: Reroutes on errors (429, 5xx, timeouts) to fallback routes with configurable exponential backoff.
- **`hedge`**: Dispatches a parallel backup request if the primary target has not returned a response within the specified threshold (`after`).
- **`timeout`**: Enforces request-wide deadlines (`duration`) and chunk idle timeouts (`idle`).

---

## Observability

### Prometheus Metrics

Metrics are exposed in Prometheus format on a dedicated loopback listener (`127.0.0.1:9209/metrics`).

Verify metrics collection:

```sh
curl -s localhost:9209/metrics
```

Key metrics include:
- `llm_requests_total` — Counter of completed requests partitioned by status, route, and provider.
- `llm_request_duration_seconds` — Histogram of end-to-end request latency.
- `llm_ttft_seconds` — Histogram of Time to First Token (TTFT).
- `llm_input_tokens_total` / `llm_output_tokens_total` — Accurate token accounting.
- `llm_attempts_total` / `llm_stream_breaks_total` — Upstream attempts and mid-stream disconnections.
- `llm_requests_in_flight` — Active concurrent requests per provider.
- `llm_cooldown_until_seconds` — Timestamp indicating provider cooldown expiration on failure.

### Structured Logging

Logging uses the standard Go `log/slog` package formatted as JSON to `stdout`. Every request is annotated with a unique `request_id` linking operational events (`request_received`, `llm_attempt`, `llm_retry`, `request_completed`, `request_failed`). Request bodies, prompts, and authorization tokens are strictly never logged.

---

## Testing & Linting

```sh
make test       # Run unit and integration tests
make test-race  # Run tests with the Go race detector enabled
make lint       # Run linter (golangci-lint or go vet)
```

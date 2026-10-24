# HTTP Provider Driver (`http-driver`)

Standalone Go module implementing the stateless multiplexed NDJSON HTTP/API provider driver for `llm-gateway`, powered by [Bifrost Core](https://github.com/maximhq/bifrost).

## Source layout

```text
drivers/http/
  go.mod              Standalone Go module (isolates Bifrost & provider SDK dependencies)
  main.go             Long-lived multiplexed NDJSON stdio loop (start, request, cancel)
  executor.go         Bifrost Core account adapter, request/response translation, error mapping
  executor_test.go    Unit tests for Bifrost payload translation, Vertex config, and error classes
```

## Build and test

From the repository root:

```sh
# Build into bin/http-driver (included in `make build`)
make build-http

# Run standalone module tests with race detector
go test -race -C drivers/http ./...
```

When `llm-gateway` resolves a provider with `"driver": "http"` (the default when `"driver"` is omitted), [`external.ResolveDriverBinary`](../../internal/executor/external/resolve.go) locates `http-driver` next to the gateway executable, in `./bin/http-driver`, on `$PATH`, or builds `./drivers/http` on demand in a source checkout.

## Gateway configuration

Providers default to `"driver": "http"` when `"driver"` is omitted:

```json
{
  "id": "openai-primary",
  "driver": "http",
  "base_provider": "openai",
  "inference_url": "https://api.openai.com/v1",
  "api_key": "env.OPENAI_API_KEY",
  "strip_params": ["service_tier"],
  "set_params": {
    "store": false
  }
}
```

### Supported `base_provider` options

- **OpenAI & OpenAI-compatible (`"openai"`):**
  - Custom `chat_completions_path` and `responses_path` overrides.
  - Automatic normalization of trailing `/v1` in `inference_url` when custom paths are configured.
- **Google Vertex AI (`"vertex"`):**
  - Supports Vertex OAuth bearer tokens (`api_key` or `vertex.auth_credentials`) as well as service-account JSON credentials.
  - Supports resource-scoped publisher model URLs (`projects/.../locations/.../publishers/.../models/...`) and custom `inference_url` routing via an internal round-tripper rewrite.
- **All other Bifrost providers (`"anthropic"`, `"bedrock"`, `"cohere"`, `"gemini"`, etc.):**
  - Configured via `base_provider`, `api_key`, `inference_url`, and `extra_headers`.

### Payload normalization & parameter rewriting

- **`strip_params` / `set_params`:** Applied to the outbound JSON payload via `sjson` before passing to Bifrost (`model` is protected from accidental overwrite).
- **Empty stream chunk filtering:** Blank initialization chunks (no content, reasoning, tool calls, `finish_reason`, or usage) are filtered out until a meaningful chunk arrives, so empty warm-up frames do not prematurely count as Time to First Token (TTFT).
- **Reasoning backfill:** When a non-streaming response includes `reasoning_details` array items without a top-level `reasoning_content` string, the driver backfills `reasoning_content` for client compatibility.

## Multiplexed NDJSON wire protocol

Unlike stateful session drivers (which run one process per conversation session), `http-driver` runs as a single long-lived subprocess per configured HTTP provider and multiplexes concurrent in-flight requests by `id` over `stdin`/`stdout`.

### Inbound commands (`stdin`)

1. **Handshake (`start`):**
   ```json
   {"type":"start","config":{...}}
   ```
   Replies with `{"type":"ready"}` once the Bifrost client and provider configuration are initialized.
2. **Execute request (`request`):**
   ```json
   {"type":"request","id":"1","endpoint":"/v1/chat/completions","model":"gpt-4o","stream":true,"body":"<base64>","headers":{"X-Request-Id":"..."}}
   ```
3. **Cancel in-flight request (`cancel`):**
   ```json
   {"type":"cancel","id":"1"}
   ```
   Cancels the context associated with request `id` immediately without affecting other concurrent requests on the same driver process.

### Outbound events (`stdout`)

| Type | Fields | Meaning |
| --- | --- | --- |
| `ready` | — | Handshake succeeded |
| `response` | `id`, `status_code`, `body` | Non-streaming response completed |
| `stream_opened` | `id` | Upstream streaming connection established |
| `chunk` | `id`, `data` | Raw SSE frame (`data: ...\n\n`) |
| `done` | `id` | Stream finished cleanly |
| `error` | `id`, `class`, `status_code`, `message`, `retry_after_ms` | Classified upstream error (`429`, `5xx`, `timeout`, `connection_error`, `4xx`, `cancelled`) |

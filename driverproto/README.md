# Driver protocol

`protocol.go` is the authoritative Go contract for the gateway and the HTTP/ACP
drivers. It has no SDK dependencies. The Web driver implements the session subset
in Rust and checks the same fixtures under `testdata/` in its unit tests.

The transport is one JSON object per line over stdin/stdout. Diagnostics belong
on stderr; stdout carries protocol messages only. Unknown optional JSON fields
may be ignored. Request `body`, event `data` and tool `arguments` are embedded
JSON values, never JSON encoded strings.

## Initialization and versions

The gateway sends `init` with `protocol_version: 1`, provider identity, opaque
`config`, optional `models_url`, tool strategy and timeout. `describe` is the
legacy alias for `init`. The driver validates configuration and returns `ready`
with its explicit `capabilities` and normalized `config`, then exits.

Bundled drivers return `protocol_version: 1`. Unversioned custom drivers are
accepted as v0 when they advertise capabilities explicitly. Unsupported versions
and missing capabilities fail initialization; no capabilities are inferred.
An empty capabilities object advertises no operations. Stateless startup checks
that capabilities still agree with the prepared description.

## Operations

| Command | Driver lifetime | Events |
| --- | --- | --- |
| `init` / `describe` | Short-lived validation process | `ready` or `error` |
| `models` | Short-lived discovery process | `models` with native `ModelInfo` objects, or `error` |
| `start` | Opens a stateless worker or one native session | `ready`, optional `resume_id` |
| `request` | Stateless worker; concurrent requests use unique `id` | `response`, or `chunk` then `done`; `error` on failure |
| `cancel` | Cancels a stateless request by `id` | No acknowledgement required |
| `prompt` | Runs a stateful session turn | `text`, `reasoning`, `usage`, `tool_call`, `session_updated`, then `done` |
| `tool_result` | Returns a session tool result by call `id` | Turn continues |

Stateless response/chunk/error/done events echo the request `id`. Session
`tool_call`/`tool_result` IDs identify tool calls. The legacy HTTP `start.providers`
array remains accepted, while gateway startup uses one provider per worker.

A `models` result contains objects with `id`, optional `name` and
`reasoning_efforts`. An empty result means discovery is unavailable. The driver
fetches current data; the gateway owns refresh scheduling, last-known-good
snapshots, model validation and metadata caching. HTTP URL construction,
credentials and response decoding stay inside the HTTP driver.

`call_error` carries stable `class`, `status`, `scope`, `replay` and optional public
`code`. `replay: unknown` forbids further speculative attempts or route
transitions. Stateful routes retain the existing restrictions against retry,
fallback, hedge, continuation and racing multiple targets.

The fixture files cover the session subset, stateless operations and output
events. Changes to the wire must update the shared fixtures and keep Go and Rust
contract tests passing.

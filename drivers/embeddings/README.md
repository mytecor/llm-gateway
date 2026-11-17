# Embeddings driver

Standalone standard-library Go adapter between gateway NDJSON and a local
embedding runtime. Build using `make build-embeddings`; test in this directory
with `go test -race ./...`. No Bifrost conversion or implicit HTTP retries occur.

Configuration contains only `inference_url`, an HTTP(S) base URL. Redirects and
environment HTTP proxies are disabled. The runtime exposes:

* `GET /models`: `{ "data": [{ "id": "native-model", "input_modalities": ["text"], "embedding_dimensions": [768] }] }`.
* `POST /embed`: the shared [embeddings contract](./../../driverproto/embeddings/contract.go).

An input contains `model` and `items`. Each item contains ordered `parts` or
`token_ids`, `dimensions`, optional `task_type` and `title`. The response is
`embeddings`, an ordered array of `{ "values": [float32...], "tokens": integer }`.
The gateway and driver validate count, dimensions, finite values, normalization
and usage. Backend errors use `{ "error": { "code": "queue_full" } }` with an
appropriate HTTP status and optional `Retry-After`.

`describe` is local and does not contact or warm the runtime. `models` reads the
lightweight catalog. The stateless `start` worker multiplexes `request`/`cancel`
commands; cancellation aborts its HTTP request, allowing the runtime to terminate
active inference. Only `kind: embeddings`, without streaming, is accepted.

Public API and model behavior are documented in [embeddings](./../../docs/embeddings.md).

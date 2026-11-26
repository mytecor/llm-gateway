# Embeddings driver

Standalone Go adapter between gateway NDJSON and OpenAI-compatible
embeddings providers. Build using `make build-embeddings` and test with
`go test -race ./...` in this directory.

Configuration uses `inference_url`, an HTTP(S) base URL without a version
suffix, and optional `image_align` (`""` or `"embeddinggemma2"`). When
`image_align` is `"embeddinggemma2"`, inline JPEG/PNG/WebP images are decoded,
EXIF-transposed, bicubic-resampled onto EmbeddingGemma 2's 280-soft-token
48 px grid, and forwarded as RGB PNG (rejecting images over 16 megapixels or
invalid payloads with `400 unsupported_media`). This keeps raw `llama-server`
instances aligned with official EmbeddingGemma 2 / CPU-Max image vectors without
a per-host proxy.

The driver calls `GET /v1/models` and `POST /v1/embeddings`.
CPU-Max and llama-server accept the same typed inline content for images,
audio and video. Task prefixes on native token IDs use `POST /tokenize`.
Redirects, HTTP proxies and implicit HTTP retries are disabled.

The gateway validates requests against the [internal embeddings contract](./../../driverproto/embeddings/contract.go).
The driver adds task prompts for text and token inputs, restores input order
from response indices, truncates vectors to the requested dimensions and
L2-normalizes them.

The stateless `start` worker multiplexes request and cancel commands.
Cancellation aborts the upstream HTTP request. Public client APIs are
documented in [embeddings](./../../docs/embeddings.md).

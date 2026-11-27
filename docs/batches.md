# Batch API

An opt-in batch route uses named client API keys and a logical model alias.
A local batch ID identifies one upstream job. Batch submission runs outside
the online scheduler's racing, fallback and retry policies.

```json
{
  "batch": {
    "clients": ["apartments-worker"],
    "model": "smart",
    "routes": {
      "smart": {"provider": "vertex", "native": "gemini-3.8-flash", "backend": "vertex"},
      "deepseek": {
        "provider": "openrouter",
        "native": "deepseek/deepseek-v4.1-flash-1017",
        "backend": "openrouter",
        "image_providers": ["sail-research", "together"]
      }
    },
    "storage_provider": "vertex",
    "bucket": "your-private-bucket",
    "prefix": "catalog",
    "state_dir": "/var/lib/llm-gateway/batches"
  }
}
```

The named provider and native model must be a target of the corresponding
logical model route. An OpenRouter HTTP provider uses
`https://openrouter.ai/api/v1` and its own provider credential. Vertex Batch
uses an existing private GCS bucket, its configured provider credential, and
the Vertex service agent's access to batch objects. OpenRouter text or
public HTTPS image requests can run with `storage_provider` and `bucket`
omitted. Private media on OpenRouter uses Vertex storage, an RSA service
account key, and image providers that can fetch signed GCS URLs. Signed
links expire after 48 hours. Configure bucket lifecycle expiration for
retaining batch inputs and outputs.

## OpenAI-compatible Chat Completions subset

1. Upload a multipart JSONL file to `POST /v1/files` with `purpose=batch`.
   Each line has `custom_id`, `method=POST`, `url=/v1/chat/completions` and a
   `body`. The body selects a logical model and contains one user message
   with optional system instructions. Use a string or a text part followed
   by an image part. Public HTTPS image URLs can go to OpenRouter when the
   selected provider supports them. The body accepts token limits and JSON
   object output. For schema-constrained output, set `response_format.type`
   to `json_schema` and supply `json_schema.name` and `json_schema.schema`.
   The gateway compiles the schema before accepting the file. A completion
   that violates its row’s schema yields `error.code=invalid_json_schema_output`.
2. Create a batch at `POST /v1/batches` with the returned `input_file_id`,
   `endpoint=/v1/chat/completions` and `completion_window=24h`.
3. Poll `GET /v1/batches/{id}` and download JSONL from
   `GET /v1/files/{output_file_id}/content`. The output preserves input
   `custom_id` values. Each entry contains a Chat Completions response or an
   item error.

## Private photos in Chat Completions batches

1. Send `{"model":"smart"}` to `POST /v1/batches/prepare`. Keep the returned `id`.
2. Upload each JPEG or PNG with `PUT /v1/batches/{id}/files/photos/{sha256}.{jpg|png}`.
   The gateway checks the SHA-256 digest of the uploaded bytes.
3. In a user message, follow a text part with
   `{"type":"image_url","image_url":{"url":"batch-file://photos/{sha256}.jpg"}}`.
   Upload the JSONL through `POST /v1/files` with multipart fields `purpose=batch`
   and `workspace_id={id}`. Resending the identical file reuses its job identity.
4. Submit and poll through the standard endpoints above. Read the output as a
   JSONL stream. Each row is adapted as it is read from the provider spool. A
   transport error after the first row indicates an incomplete download.

`batch.clients` and the legacy `batch.client` grant access to the union of their
named keys. Each key owns its own job and photo namespace across restarts. Retain
the original key ID in the allowlist while draining its jobs.

## Legacy managed media extension

Generate a 32-character lowercase hexadecimal ID. Use
`GET /v1/batches/{id}/input?model=LOGICAL_MODEL` for the storage prefix and
provider model. Upload images as `photos/{sha256}.jpg` or `photos/{sha256}.png`
with `PUT /v1/batches/{id}/files/{path}?model=LOGICAL_MODEL`. Upload
`requests.jsonl` under the same ID with Vertex GenerateContent-shaped
rows. The HTTP driver translates these rows to Chat Completions for
OpenRouter. Submit using `POST /v1/batches` with `id` and the logical
`model`, and download provider JSONL through
`GET /v1/batches/{id}/results`.

Uploads are private and immutable per job. A repeated submission checks the
existing record. An ambiguous Vertex response is recovered by the persisted
job name. When OpenRouter's response is lost before its ID arrives, the
record stays in `SUBMITTING` and prevents another paid create. Inspect the
OpenRouter workspace before attempting a new job. Known job IDs survive
gateway restarts.

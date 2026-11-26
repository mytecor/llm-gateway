# Embeddings

The non-streaming embedding operation shares gateway authorization, routing,
deadlines and observability with other operations. An embedding-only provider
cannot serve Chat/Responses. Modality compatibility is checked before balancing,
cooldown fail-open and recovery routing. Existing drivers need no new fields.

## OpenAI endpoint

`POST /v1/embeddings` accepts `model`, `input`, optional `dimensions`,
`encoding_format` (`float`, default, or `base64`), `user`, `task_type`, and `title`.
The standard input forms are string, string array, native token ID array, and
array of native token ID arrays. Token IDs are exact IDs of the served model's
tokenizer, including any desired special tokens; they are never decoded and
re-tokenized or interpreted as OpenAI's tokenizer IDs.

Multimodal input is an explicit gateway extension: a Content object, or an array
of Content objects. One Content produces one vector, preserving part order;
an array produces separate vectors in input order. Do not mix strings, token
arrays and Content objects in the same batch.

When `default_embedding_model` is configured, OpenAI requests can omit
`model`. When the client supplies a model, the gateway uses that logical model.

```json
{
  "model": "embedding",
  "input": {
    "parts": [
      {"text": "Find this product: "},
      {"inlineData": {"mimeType": "image/jpeg", "data": "<base64 bytes>"}}
    ]
  },
  "dimensions": 768
}
```

Responses retain OpenAI's `object: list`, `data[].object: embedding`, `index`,
`embedding`, logical `model`, and `usage.prompt_tokens/total_tokens` fields.
Base64 represents little-endian IEEE754 float32 vectors, as expected by OpenAI
SDKs. Use ordinary SDK embedding calls for text. For the extension, use the SDK's
custom JSON/body facility or direct HTTP; the object input is not an OpenAI API
standard. Streaming and chat-generation parameters are rejected.

## Google endpoints

* `POST /v1beta/models/{logical_model}:embedContent`
* `POST /v1beta/models/{logical_model}:batchEmbedContents`

Protobuf field-name aliases `inline_data`/`inlineData` and `mime_type`/`mimeType`
are accepted and normalized; conflicting aliases and unknown fields are rejected.
This includes the nested Content serialization used by Google GenAI SDKs.
Media bytes accept standard and URL-safe base64, with or without padding, as
specified by [ProtoJSON](https://protobuf.dev/programming-guides/json/). They
normalize to padded standard base64 before the provider request is formed.

The body accepts `content.parts`, optional `model: models/{logical_model}`,
`taskType`, `title`, and `outputDimensionality`. Batch bodies use `requests`, an
array of these request objects; every model must match the path. Task and
dimension may differ per batch item. A Content yields one joint embedding;
batching yields separate embeddings. Responses use `embedding.values` and
`embeddings[].values`, respectively.

```json
{
  "content": {
    "parts": [
      {"text": "A bell rings: "},
      {"inlineData": {"mimeType": "audio/wav", "data": "<base64 bytes>"}}
    ]
  },
  "outputDimensionality": 512
}
```

Use Bearer authentication or `x-goog-api-key` with the same configured gateway
client keys. Different simultaneous credentials are rejected. Google endpoints
return Google error envelopes (`error.code/status/message`). Google Files,
remote URLs, async batch jobs and upload APIs are outside this contract.

## Model semantics and limits

Supported dimensions are 768 (default), 512, 256, 128. Truncated vectors are
L2-normalized. Context is 8192 model tokens per input, including instruction and
media tokens; overflow is rejected without silently truncating text.
HTTP JSON is limited to 32 MiB and batches to 64 inputs. Embedding NDJSON commands
allow 40 MiB including the envelope, while Chat/Responses framing stays at 16 MiB.

The gateway accepts inline JPEG/PNG/WebP, WAV/MP3/FLAC/OGG and MP4/WebM. Actual
decoding and preprocessing depend on the provider. Each part contains exactly `text` or `inlineData`. Images are limited to 16 megapixels; audio/video
clips to 330 seconds independently of context/byte limits. Audio becomes mono
16 kHz. Video uses 1 fps, at most 32 uniformly sampled frames, and no implicit
audio track. PDF, file paths and URI references are rejected. Literal media
placeholder tokens in text are rejected to prevent ambiguous placement.

No task prefix is added by default. Text-only tasks map to the checkpoint's
documented instructions: `RETRIEVAL_QUERY`, `RETRIEVAL_DOCUMENT`,
`SEMANTIC_SIMILARITY`, `CLASSIFICATION`, `CLUSTERING`, `QUESTION_ANSWERING`,
`FACT_VERIFICATION`, `CODE_RETRIEVAL_QUERY`. `title` requires
`RETRIEVAL_DOCUMENT`. Instructions/titles on media inputs are rejected. For
retrieval, encode queries and documents with their respective task types.

`GET /v1/models` adds `capabilities`, `input_modalities`, and
`embedding_dimensions` when the single-target provider supplies embedding
metadata. Discovery does not load model weights.

## Local provider

Install `embeddings-driver` alongside the core. See the
[driver contract](./../drivers/embeddings/README.md). Example provider:

```json
{
  "id": "local-embedding",
  "driver": "embeddings",
  "request_timeout": "5m",
  "config": {"inference_url": "http://127.0.0.1:9226"}
}
```

Use one local target with `budget.max_calls: 1`, `max_calls_per_provider: 1`,
`max_in_flight: 1`, `max_hedges: 0`, and a five-minute route timeout. Do not add
fallback to a different embedding space. Changing model weights, preprocessing
or task instructions requires an explicit index compatibility/migration decision.

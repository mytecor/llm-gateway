# Native System One decisions

`POST /v1/systemone` and its alias `POST /v1/decisions` accept the native
[TypeSafe System One contract](https://docs.typesafe.ai/api/system-one).
Both use the same gateway bearer authentication as Chat Completions. Configure a
logical model such as `jev`; the scheduler balances its compatible targets using
normal admission, health, latency, request budgets, retry and hedge policies.

```json
{
  "model": "jev",
  "state": {"ticket": "My card was charged twice."},
  "questions": {
    "team": {
      "type": "choice",
      "instructions": "Which team should handle this?",
      "criteria": {"billing": "Payments and refunds", "technical": "Bugs"}
    },
    "urgency": {
      "type": "score",
      "instructions": "How urgent is the ticket?",
      "criteria": ["Can wait", "Needs attention today"]
    },
    "refund": {"type": "noul", "instructions": "Is a refund requested?"}
  }
}
```

State accepts strings, objects and arrays. Instructions and criteria retain their
JSON structure. Score criteria use an ordered array, with levels starting at zero;
choice criteria use an object with 1–255 options. Score supports 1–10 levels.
The response contains `model`, an `answers` map with the same IDs and question
types, and optional upstream `usage`. Unlike the OpenAI endpoints, `model` reports
the actual upstream version rather than the logical alias. Probabilities,
confidence and numbers retain their original precision. Missing usage is unknown,
not an invented zero-token result. Low confidence is a successful decision.

The driver adapts the documented [Gonka dialect](https://proxy.gonka.gg/docs/jev):
score arrays become numerically keyed criteria objects upstream, and score legend
arrays become objects in the public response. Answer IDs, types, choices, legends
and numeric ranges are validated before a branch can win. Invalid answers can
trigger normal provider recovery; there are no corrective generations.

## Images, Box and long context

To use assets or Box questions, explicitly send
`TypeSafe-Danger-Zone-Images: true`. For long context, send
`TypeSafe-Danger-Zone-Long-Context: true`. Values other than `true` are rejected;
no arbitrary client headers or client authorization are forwarded to providers.

Both JSON/base64 and `multipart/form-data` follow the
[TypeSafe image contract](https://docs.typesafe.ai/images). For multipart, the first
part is `request` containing JSON; each image part is referenced by exactly one
asset's `part`. For JSON, each asset has `format` and `data_base64`. Every asset
reference must occur in a value in state, instructions or criteria. Images are
PNG/JPEG, at most 16 MiB each, with dimensions from 280 to 1568 pixels per side.
No resizing or re-encoding occurs. The upstream retains responsibility for its
expanded-image/context limits and full image decoding.

```json
{
  "model": "jev",
  "state": "photo-reference",
  "assets": {"photo-reference": {"part": "photo"}},
  "questions": {"location": {"type": "box", "instructions": "Locate the door."}}
}
```

This JSON is the `request` part; send the PNG/JPEG as the `photo` part. Box answers
contain normalized `top`, `bottom`, `left`, `right` edges and nullable confidence.
Box is not routed to the older `jev-1.13.0` model. These experimental features remain
explicitly enabled per request, as in TypeSafe's API.

The gateway derives requirements before balancing and filters every candidate
pool, including retry, fallback, named hedges and internal hedges. Gonka declares
only text decisions; TypeSafe additionally declares images, Box and long context.
Compatibility is never bypassed by cooling fail-open. If the routing graph has no
compatible provider, the response is `503 no_compatible_provider`.

## Errors and limits

Gonka eligibility `403` becomes a provider failure, allowing another member of the
same pool to answer. Eligibility is evaluated by Gonka on each call; the gateway
does not infer eligibility from token metrics. Upstream authentication failures
also remain provider failures rather than gateway-client authentication errors.
Rate limits carry `Retry-After` through the driver and scheduler; this deadline is
not bypassed by cooling fail-open. Temporary failures including TypeSafe `529`
participate in normal recovery. Invalid client requests (`400`, `413`, `415`,
`422`) are terminal and are not retried against other providers.

System One accepts at most 32 MiB over HTTP. Multipart is normalized to JSON with
base64 assets, bounded at 48 MiB, and sent through a JEV-only 64 MiB NDJSON command
limit. The driver sends images upstream as multipart to avoid base64 overhead.
Responses remain bounded at 16 MiB. Chat/Responses retain their existing 16 MiB
body and command limits. Compressed HTTP request bodies are not accepted.

This endpoint is non-streaming. It does not translate Chat Completions or perform
tools/session orchestration. See the [JEV driver](./../drivers/jev/README.md) and
[example pool](./../configs/config.jev.json) for configuration.

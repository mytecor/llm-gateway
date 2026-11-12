# Response transformations

Response transformations are explicit, opt-in rules on a logical entry route.
They are independent of Strict Structured Outputs and apply to ordinary and
streaming Chat Completions responses. No provider or model name enables them
implicitly.

```json
{
  "rules": [
    {"route": "small", "action": "filter", "where": {"model": {"eq": "small"}}},
    {"route": "small", "action": "map", "providers": ["provider-a"], "native": "native-model"},
    {"route": "small", "action": "race", "count": 1},
    {"route": "small", "action": "transform", "type": "split_thinking"}
  ]
}
```

Declare `transform` after `race` on the entry route. The logical model's
transformations apply to all its physical branches, including retry, fallback,
hedge, stream continuation, and strict corrective generations. They run before
stream winner selection, repetition detection, continuation accumulation, and
JSON Schema validation. A different logical model sharing the same provider is
unaffected. Duplicate or unknown transformation types, unknown fields, and
subroute declarations are rejected during configuration checks.

The first version supports stateless Chat Completions routes. Responses API
payloads pass through unchanged. A stateful route cannot declare an output
transformation: rewriting native session output also requires coordinating its
public history/checkpoint contract, which is owned by the session runtime.

## `split_thinking`

This transformation separates one leading `<think>...</think>` block into
`reasoning_content` and its equivalent `reasoning` alias, leaving the final
answer in `content`. It handles providers that emit a model's raw reasoning
template inside the answer instead of returning separate fields.

```json
{"content":"<think>reasoning</think>{\"ok\":true}"}
```

becomes:

```json
{"content":"{\"ok\":true}","reasoning_content":"reasoning","reasoning":"reasoning"}
```

Leading JSON whitespace is preserved. Only the leading reasoning block is
recognized. Literal tags in JSON strings and tags after ordinary content remain
unchanged. The transformation preserves native reasoning fields, opaque
`reasoning_details` and signatures, usage, tool calls, refusal, choice indexes,
and unknown metadata. Consumers should read one reasoning alias, not concatenate
both copies.

For SSE, each choice has its own incremental parser. Delimiters can cross event
boundaries; partial delimiters never become meaningful race-winning output.
Parser memory is bounded by delimiter lookahead, and Unicode text is preserved.
A new physical stream always starts with fresh parser state. Retained strict
output remains bounded by the strict generation buffer.

An unclosed block followed by `stop` or an abrupt end produces the provider
error `incomplete_thinking`; normal routing/recovery rules handle it. If generation
explicitly ends with `length`, observed partial reasoning is preserved with empty
final content and the original finish reason. No final JSON is manufactured.

## Strict output integration

Strict validation applies only to final `content`. Both content and reasoning
from an unaccepted generation remain private; events from the accepted generation
are replayed together after validation. Native signatures and details pass through
without JSON Schema validation or decoding. Usage is accounted from provider
usage reports, independently of text separation.

Strict responses ending with `length` fail with
`502 structured_output_incomplete` rather than retrying the unchanged generation
budget. The gateway does not silently increase the client's token limit.
See [Strict Structured Outputs](./structured-outputs.md).

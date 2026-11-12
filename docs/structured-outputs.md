# Strict Structured Outputs

The gateway validates and corrects Chat Completions responses when the client
sends `response_format.type: "json_schema"` and `json_schema.strict: true`.
No gateway-specific client option or routing policy is required.

```json
{
  "model": "standard",
  "messages": [{"role": "user", "content": "Return the delivery confidence."}],
  "response_format": {
    "type": "json_schema",
    "json_schema": {
      "name": "confidence",
      "strict": true,
      "schema": {
        "type": "object",
        "properties": {"confidence": {"type": "number", "minimum": 0, "maximum": 1}},
        "required": ["confidence"],
        "additionalProperties": false
      }
    }
  }
}
```

## Validation and correction

The schema is compiled once before execution with
[jsonschema/v6](https://pkg.go.dev/github.com/santhosh-tekuri/jsonschema/v6).
Draft 2020-12 is the default; supported explicit dialects use the library's
standard semantics, including format annotations. References within the
supplied schema are supported. External resource loading, including network
and filesystem access, is disabled.

After each complete routing run, the gateway checks exactly one JSON document
from `choices[0].message.content`. Numeric values retain their precision.
Markdown fences, additional prose, and trailing JSON documents fail validation.
The original response text is preserved when accepted.

There are at most two corrective generations after the initial response.
Feedback contains JSON Pointer locations, violated keywords, and descriptions,
bounded to ten errors and 8 KiB. Feedback asks the model to preserve valid
values and correct only the response without repeating the original task.

| Executor | Initial request | Correction |
| --- | --- | --- |
| HTTP | Forward the original `response_format` | Append the previous assistant response and user feedback; rerun the same virtual model through its routing pipeline |
| ACP / Web | Append the original schema to the current turn prompt | Send feedback as a new turn in the same native session |

HTTP corrections can select another physical provider. Unsupported upstream
formats remain ordinary provider errors; the gateway does not detect native
Structured Outputs capabilities or convert schemas to tool calls.

## Streaming, deadlines, and usage

For strict `stream: true` requests, upstream streaming and the existing race,
retry/fallback, continuation, repetition, admission, capacity, and idle-timeout
machinery remain active. Selected events are buffered until the complete
response passes validation. Only events from the accepted generation are
released. Non-strict streaming keeps its existing behavior.

The buffer is limited to 16 MiB per generation, including retained events and
content. Overflow fails closed. Waiting connections may receive SSE comments
and empty deltas; rejected text and reasoning are never sent to the client.
After SSE has begun, a terminal error has `partial: false` and no `[DONE]`.
Before SSE begins, failures use ordinary HTTP error responses.

A strict request has one deadline across all generations. It uses the earlier
of the client deadline and the entry route timeout, measured from request
start. If neither exists, it defaults to 30 minutes. Existing provider and
native turn deadlines may stop individual generations earlier.

Reported usage from all corrective generations is accumulated. Missing usage
is left unknown. Token metrics retain each actual provider's attribution,
including usage reported before a stream fails. Non-stream responses contain
aggregate usage; SSE emits one aggregate usage event when
`stream_options.include_usage` is true and any generation reported usage.
The client request is observed once, independently of upstream attempts.

## Native sessions

One conversation lock covers the entire validation/correction exchange.
The native chat retains schema instructions, invalid answers, and feedback.
The public transcript contains the original client messages and accepted
assistant answer. Prefix matching and checkpoints use that public transcript,
so the next client turn and restart resume do not replay hidden turns.

A checkpoint contains only public history count/hash and native resume metadata;
it never stores a hidden transcript. Intermediate generations do not create
checkpoints. Cancellation, failure, and correction exhaustion close the local
native session and delete its checkpoint. A session lost during correction
fails the request instead of replaying the original task. Initial session
restoration retains the normal missing-session recovery behavior.

Client function tools are unavailable in this first version. ACP agents can
still have their own built-in tools: correction instructions request formatting
only, but cannot guarantee that a native agent performs no other actions.
See [session lifecycle](./session-executors.md) for the underlying runtime.

## Errors and compatibility

| HTTP status | Code | Meaning |
| --- | --- | --- |
| 400 | `invalid_json_schema` | The schema cannot be compiled or requires an external resource |
| 400 | `structured_output_tools_unsupported` | Non-empty client tools were supplied |
| 400 | `structured_output_n_unsupported` | `n` differs from one |
| 502 | `structured_output_validation_failed` | All three generations failed JSON Schema validation |
| 502 | `structured_output_buffer_exceeded` | A generation exceeded its bounded buffer |
| 502 | `structured_output_unexpected_tool_calls` | The provider unexpectedly returned a function call |
| 502 | `structured_output_content_filtered` | Generation was filtered without an explicit refusal |
| 504 | `structured_output_timeout` | The shared request deadline expired |

Public errors contain no schema, feedback, or rejected response. Explicit
`refusal` responses are terminal and returned without correction; see the
[OpenAI Structured Outputs contract](https://developers.openai.com/api/docs/guides/structured-outputs?api-mode=chat).
Transport failures retain their normal error classes.

Requests without strict JSON Schema, `strict: false`, `json_object`, and
`/v1/responses` retain their existing behavior. Strict requests support one
completion and reject non-empty client `tools`.

The additional counters are `llm_structured_validations_total` (logical model,
provider, and bounded validation status) and `llm_structured_corrections_total`
(logical model). Schemas, response text, feedback, and session identifiers never
become log fields or metric labels.

// Package structured validates strict Chat Completions responses and owns the
// bounded correction loop outside the routing pipeline.
package structured

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

const MaxCorrections = 2
const MaxBufferBytes = 16 << 20
const maxFeedbackBytes = 8 << 10
const maxFeedbackErrors = 10

// Contract is immutable and compiled once per incoming strict request.
type Contract struct {
	Schema    string
	validator *jsonschema.Schema
	rootProps map[string]struct{}
}

type noExternalResources struct{}

func (noExternalResources) Load(string) (any, error) {
	return nil, errors.New("external schema resources are disabled")
}

func Error(status int, code string) *model.CallError {
	return &model.CallError{Class: model.ErrorInvalid, Status: status, Code: code, Scope: model.FailureScopeRequest, Replay: model.ReplayNotSent}
}

// Parse returns nil for requests outside the strict JSON Schema contract.
func Parse(body []byte) (*Contract, *model.CallError) {
	var envelope struct {
		ResponseFormat json.RawMessage   `json:"response_format"`
		Tools          []json.RawMessage `json:"tools"`
		N              *int              `json:"n"`
	}
	// Only inspect format first: non-strict requests remain opaque pass-through.
	var root map[string]json.RawMessage
	if json.Unmarshal(body, &root) != nil {
		return nil, Error(400, "invalid_request")
	}
	var activation struct {
		Type       string `json:"type"`
		JSONSchema struct {
			Strict bool `json:"strict"`
		} `json:"json_schema"`
	}
	if json.Unmarshal(root["response_format"], &activation) != nil || activation.Type != "json_schema" || !activation.JSONSchema.Strict {
		return nil, nil
	}
	if json.Unmarshal(body, &envelope) != nil {
		return nil, Error(400, "invalid_request")
	}
	if len(envelope.Tools) > 0 {
		return nil, Error(400, "structured_output_tools_unsupported")
	}
	if envelope.N != nil && *envelope.N != 1 {
		return nil, Error(400, "structured_output_n_unsupported")
	}
	var format struct {
		JSONSchema struct {
			Schema json.RawMessage `json:"schema"`
		} `json:"json_schema"`
	}
	if json.Unmarshal(envelope.ResponseFormat, &format) != nil || len(format.JSONSchema.Schema) == 0 {
		return nil, Error(400, "invalid_json_schema")
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(format.JSONSchema.Schema))
	if err != nil {
		return nil, Error(400, "invalid_json_schema")
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.UseLoader(noExternalResources{})
	const resource = "https://llm-gateway.invalid/response.json"
	if err = compiler.AddResource(resource, doc); err != nil {
		return nil, Error(400, "invalid_json_schema")
	}
	schema, err := compiler.Compile(resource)
	if err != nil {
		return nil, Error(400, "invalid_json_schema")
	}
	return &Contract{
		Schema:    string(format.JSONSchema.Schema),
		validator: schema,
		rootProps: extractRootProps(doc),
	}, nil
}

var (
	errMalformedJSON = errors.New("response must contain exactly one JSON document")
	errUnrelatedJSON = errors.New("response JSON shares no properties with target schema")
)

func extractRootProps(doc any) map[string]struct{} {
	obj, ok := doc.(map[string]any)
	if !ok {
		return nil
	}
	rawProps, ok := obj["properties"].(map[string]any)
	if !ok || len(rawProps) == 0 {
		return nil
	}
	props := make(map[string]struct{}, len(rawProps))
	for k := range rawProps {
		props[k] = struct{}{}
	}
	return props
}

func (c *Contract) sharesRootProperty(value any) bool {
	if len(c.rootProps) == 0 {
		return true
	}
	obj, ok := value.(map[string]any)
	if !ok {
		return false
	}
	for k := range obj {
		if _, ok := c.rootProps[k]; ok {
			return true
		}
	}
	return false
}

func (c *Contract) Validate(content string) error {
	value, err := jsonschema.UnmarshalJSON(strings.NewReader(content))
	if err != nil {
		return errMalformedJSON
	}
	if err := c.validator.Validate(value); err != nil {
		if !c.sharesRootProperty(value) {
			return fmt.Errorf("%w: %w", errUnrelatedJSON, err)
		}
		return err
	}
	return nil
}

// Feedback is bounded and sent only to the model, never to logs or the client.
func Feedback(err error) string {
	var lines []string
	var validation *jsonschema.ValidationError
	if errors.As(err, &validation) {
		printer := message.NewPrinter(language.English)
		var visit func(*jsonschema.ValidationError)
		visit = func(e *jsonschema.ValidationError) {
			if len(lines) >= maxFeedbackErrors {
				return
			}
			if len(e.Causes) > 0 {
				for _, child := range e.Causes {
					visit(child)
				}
				return
			}
			var pointer strings.Builder
			for _, part := range e.InstanceLocation {
				pointer.WriteByte('/')
				pointer.WriteString(strings.ReplaceAll(strings.ReplaceAll(part, "~", "~0"), "/", "~1"))
			}
			loc := pointer.String()
			if loc == "" {
				loc = "/"
			}
			lines = append(lines, fmt.Sprintf("- %s [%s]: %s", loc, strings.Join(e.ErrorKind.KeywordPath(), "/"), e.ErrorKind.LocalizedString(printer)))
		}
		visit(validation)
	} else {
		lines = append(lines, "- / [json]: response must contain exactly one JSON document")
	}
	prefix := "Your previous response failed JSON Schema validation:\n"
	suffix := "\n\nCorrect only the previous response to satisfy the original JSON Schema. Preserve valid values. Do not repeat the original task or call tools. Return only the corrected JSON."
	detail := strings.Join(lines, "\n")
	limit := maxFeedbackBytes - len(prefix) - len(suffix)
	if len(detail) > limit {
		detail = detail[:limit]
		for !utf8.ValidString(detail) {
			detail = detail[:len(detail)-1]
		}
	}
	return prefix + detail + suffix
}

// Answer extracts only public assistant content; refusal and tool calls are
// distinct terminal outcomes and never become schema-correction instructions.
func Answer(body []byte) (content string, refusal bool, callErr *model.CallError) {
	var envelope struct {
		Choices []struct {
			Index   int `json:"index"`
			Message struct {
				Content  *string           `json:"content"`
				Refusal  *string           `json:"refusal"`
				Tools    []json.RawMessage `json:"tool_calls"`
				Function json.RawMessage   `json:"function_call"`
			} `json:"message"`
			Finish string `json:"finish_reason"`
		} `json:"choices"`
	}
	if json.Unmarshal(body, &envelope) != nil || len(envelope.Choices) != 1 || envelope.Choices[0].Index != 0 {
		return "", false, Error(502, "invalid_upstream_response")
	}
	choice := envelope.Choices[0]
	if len(choice.Message.Tools) > 0 || (len(choice.Message.Function) > 0 && string(choice.Message.Function) != "null") || choice.Finish == "tool_calls" || choice.Finish == "function_call" {
		return "", false, Error(502, "structured_output_unexpected_tool_calls")
	}
	if choice.Message.Content != nil {
		content = *choice.Message.Content
	}
	if choice.Message.Refusal != nil && *choice.Message.Refusal != "" {
		return content, true, nil
	}
	if choice.Finish == "length" {
		return "", false, Error(502, "structured_output_incomplete")
	}
	if choice.Finish == "content_filter" {
		return "", false, Error(502, "structured_output_content_filtered")
	}
	return content, false, nil
}

func appendFeedback(body []byte, content, feedback string) ([]byte, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(body, &root); err != nil {
		return nil, err
	}
	var messages []json.RawMessage
	if err := json.Unmarshal(root["messages"], &messages); err != nil {
		return nil, err
	}
	for _, m := range []map[string]string{{"role": "assistant", "content": content}, {"role": "user", "content": feedback}} {
		data, _ := json.Marshal(m)
		messages = append(messages, data)
	}
	root["messages"], _ = json.Marshal(messages)
	return json.Marshal(root)
}

// WithUsage changes only usage on the accepted upstream envelope/chunk.
func WithUsage(body []byte, usage model.UsageSummary, hasUsage bool) []byte {
	if !hasUsage {
		return body
	}
	var root map[string]json.RawMessage
	if json.Unmarshal(body, &root) != nil {
		return body
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(root["usage"], &fields)
	if fields == nil {
		fields = map[string]json.RawMessage{}
	}
	number := func(n int64) json.RawMessage { b, _ := json.Marshal(n); return b }
	fields["prompt_tokens"], fields["completion_tokens"], fields["total_tokens"] = number(usage.Input), number(usage.Output), number(usage.Input+usage.Output)
	// Preserve unrelated detail fields while replacing the accumulated cache count.
	var details map[string]json.RawMessage
	_ = json.Unmarshal(fields["prompt_tokens_details"], &details)
	if details == nil {
		details = map[string]json.RawMessage{}
	}
	details["cached_tokens"] = number(usage.CachedTokens)
	fields["prompt_tokens_details"], _ = json.Marshal(details)
	root["usage"], _ = json.Marshal(fields)
	data, _ := json.Marshal(root)
	return data
}

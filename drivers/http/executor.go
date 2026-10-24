// Package main implements the standalone HTTP/Bifrost provider driver over the gateway NDJSON wire protocol.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/tidwall/sjson"
)

type ProviderConfig struct {
	ID                    string                     `json:"id"`
	BaseProvider          string                     `json:"base_provider"`
	InferenceURL          string                     `json:"inference_url"`
	VertexProjectID       string                     `json:"vertex_project_id,omitempty"`
	VertexProjectNumber   string                     `json:"vertex_project_number,omitempty"`
	VertexRegion          string                     `json:"vertex_region,omitempty"`
	VertexAuthCredentials string                     `json:"vertex_auth_credentials,omitempty"`
	APIKey                string                     `json:"api_key,omitempty"`
	RequestTimeoutSeconds int                        `json:"request_timeout_seconds,omitempty"`
	BifrostMaxRetries     int                        `json:"bifrost_max_retries,omitempty"`
	AllowPrivateNetwork   bool                       `json:"allow_private_network,omitempty"`
	Headers               map[string]string          `json:"headers,omitempty"`
	StripParams           []string                   `json:"strip_params,omitempty"`
	SetParams             map[string]json.RawMessage `json:"set_params,omitempty"`
}

type CallError struct {
	Code   string `json:"code,omitempty"`
	Class  string `json:"class"`
	Status int    `json:"status"`
	Scope  string `json:"scope,omitempty"`
	Replay string `json:"replay,omitempty"`
	Detail string `json:"detail,omitempty"`
	Log    string `json:"log,omitempty"`
}

type StreamChunk struct {
	Data       json.RawMessage
	Event      string
	Meaningful bool
	Err        *CallError
}

type bifrostAccount struct {
	providers []schemas.ModelProvider
	configs   map[schemas.ModelProvider]*schemas.ProviderConfig
	keys      map[schemas.ModelProvider][]schemas.Key
}

func (a *bifrostAccount) GetConfiguredProviders() ([]schemas.ModelProvider, error) {
	return append([]schemas.ModelProvider(nil), a.providers...), nil
}

func (a *bifrostAccount) GetConfigForProvider(provider schemas.ModelProvider) (*schemas.ProviderConfig, error) {
	config, ok := a.configs[provider]
	if !ok {
		return nil, fmt.Errorf("provider is not configured")
	}
	cp := *config
	return &cp, nil
}

func (a *bifrostAccount) GetKeysForProvider(_ context.Context, provider schemas.ModelProvider) ([]schemas.Key, error) {
	keys, ok := a.keys[provider]
	if !ok {
		return nil, fmt.Errorf("provider has no key configuration")
	}
	return append([]schemas.Key(nil), keys...), nil
}

type silentLogger struct{}

func (silentLogger) Debug(string, ...any)                   {}
func (silentLogger) Info(string, ...any)                    {}
func (silentLogger) Warn(string, ...any)                    {}
func (silentLogger) Error(string, ...any)                   {}
func (silentLogger) Fatal(string, ...any)                   { panic("bifrost fatal error") }
func (silentLogger) SetLevel(schemas.LogLevel)              {}
func (silentLogger) SetOutputType(schemas.LoggerOutputType) {}
func (silentLogger) LogHTTPRequest(schemas.LogLevel, string) schemas.LogEventBuilder {
	return schemas.NoopLogEvent
}

type Executor struct {
	client           *bifrost.Bifrost
	providers        map[string]ProviderConfig
	bifrostProviders map[string]schemas.ModelProvider
}

func BifrostProviderKey(provider ProviderConfig) (schemas.ModelProvider, bool, error) {
	baseProvider := schemas.ModelProvider(provider.BaseProvider)
	if bifrost.IsSupportedBaseProvider(baseProvider) {
		return schemas.ModelProvider(provider.ID), true, nil
	}
	if baseProvider == schemas.Vertex {
		return schemas.Vertex, false, nil
	}
	return "", false, fmt.Errorf("provider %q has unsupported Bifrost base_provider %q", provider.ID, provider.BaseProvider)
}

func NewExecutor(ctx context.Context, list []ProviderConfig) (*Executor, error) {
	providers := make(map[string]ProviderConfig, len(list))
	for _, p := range list {
		providers[p.ID] = p
	}
	account := &bifrostAccount{
		configs: make(map[schemas.ModelProvider]*schemas.ProviderConfig, len(providers)),
		keys:    make(map[schemas.ModelProvider][]schemas.Key, len(providers)),
	}
	bifrostProviders := make(map[string]schemas.ModelProvider, len(providers))
	for _, id := range sortedProviderIDs(providers) {
		provider := providers[id]
		baseProvider := schemas.ModelProvider(provider.BaseProvider)
		providerKey, custom, err := BifrostProviderKey(provider)
		if err != nil {
			return nil, err
		}
		bifrostProviders[provider.ID] = providerKey
		account.providers = append(account.providers, providerKey)
		var customConfig *schemas.CustomProviderConfig
		if custom {
			customConfig = &schemas.CustomProviderConfig{
				BaseProviderType: baseProvider,
				IsKeyLess:        provider.APIKey == "" && provider.VertexAuthCredentials == "",
			}
		}
		if custom && baseProvider == schemas.OpenAI {
			customConfig.RequestPathOverrides = map[schemas.RequestType]string{
				schemas.ChatCompletionRequest:       "/chat/completions",
				schemas.ChatCompletionStreamRequest: "/chat/completions",
				schemas.ResponsesRequest:            "/responses",
				schemas.ResponsesStreamRequest:      "/responses",
			}
		}
		timeoutSec := max(1, provider.RequestTimeoutSeconds)
		account.configs[providerKey] = &schemas.ProviderConfig{
			NetworkConfig: schemas.NetworkConfig{
				BaseURL:                        provider.InferenceURL,
				ExtraHeaders:                   provider.Headers,
				DefaultRequestTimeoutInSeconds: timeoutSec,
				MaxRetries:                     provider.BifrostMaxRetries,
				AllowPrivateNetwork:            provider.AllowPrivateNetwork,
			},
			CustomProviderConfig:    customConfig,
			SendBackRawRequest:      false,
			SendBackRawResponse:     false,
			StoreRawRequestResponse: false,
		}
		key := bifrostKeyForProvider(provider, baseProvider)
		account.keys[providerKey] = []schemas.Key{key}
	}
	client, err := bifrost.Init(ctx, schemas.BifrostConfig{
		Account:         account,
		Logger:          silentLogger{},
		InitialPoolSize: 64,
	})
	if err != nil {
		return nil, fmt.Errorf("initialize Bifrost: %w", err)
	}
	for providerID, providerKey := range bifrostProviders {
		if client.GetProviderByKey(providerKey) == nil {
			client.Shutdown()
			return nil, fmt.Errorf("initialize Bifrost provider %q as %q: provider was not prepared", providerID, providerKey)
		}
	}
	return &Executor{
		client:           client,
		providers:        providers,
		bifrostProviders: bifrostProviders,
	}, nil
}

func bifrostKeyForProvider(provider ProviderConfig, baseProvider schemas.ModelProvider) schemas.Key {
	key := schemas.Key{
		ID:      "key-" + provider.ID,
		Name:    "key-" + provider.ID,
		Models:  schemas.WhiteList{"*"},
		Weight:  1,
		Enabled: schemas.Ptr(true),
	}
	if provider.APIKey != "" {
		key.Value = *schemas.NewSecretVar(provider.APIKey)
	}
	if baseProvider == schemas.Vertex {
		key.VertexKeyConfig = &schemas.VertexKeyConfig{
			ProjectID:       *schemas.NewSecretVar(provider.VertexProjectID),
			ProjectNumber:   *schemas.NewSecretVar(provider.VertexProjectNumber),
			Region:          *schemas.NewSecretVar(provider.VertexRegion),
			AuthCredentials: *schemas.NewSecretVar(provider.VertexAuthCredentials),
		}
	}
	return key
}

func sortedProviderIDs(providers map[string]ProviderConfig) []string {
	ids := make([]string, 0, len(providers))
	for id := range providers {
		ids = append(ids, id)
	}
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j] < ids[j-1]; j-- {
			ids[j], ids[j-1] = ids[j-1], ids[j]
		}
	}
	return ids
}

func (e *Executor) Do(ctx context.Context, providerID, nativeModel, kind string, body []byte) ([]byte, *CallError) {
	bfContext := schemas.NewBifrostContext(ctx, schemas.NoDeadline)
	switch kind {
	case "chat":
		chatRequest, callErr := e.chatRequest(bfContext, providerID, nativeModel, body)
		if callErr != nil {
			callErr.Log = "provider_request_rejected"
			return nil, callErr
		}
		response, bfErr := e.client.ChatCompletionRequest(bfContext, chatRequest)
		if bfErr != nil {
			callErr := classifyBifrostError(ctx, bfErr)
			callErr.Log = "upstream_request_failed"
			return nil, callErr
		}
		response.Model = nativeModel
		populateChatResponseReasoning(response)
		return marshalSanitized(response, "")
	case "responses":
		responsesRequest, callErr := e.responsesRequest(bfContext, providerID, nativeModel, body)
		if callErr != nil {
			callErr.Log = "provider_request_rejected"
			return nil, callErr
		}
		response, bfErr := e.client.ResponsesRequest(bfContext, responsesRequest)
		if bfErr != nil {
			callErr := classifyBifrostError(ctx, bfErr)
			callErr.Log = "upstream_request_failed"
			return nil, callErr
		}
		response.Model = nativeModel
		return marshalSanitized(response, "")
	default:
		return nil, &CallError{Class: "invalid_response", Status: 400, Log: "provider_request_rejected"}
	}
}

func (e *Executor) Stream(ctx context.Context, providerID, nativeModel, kind string, body []byte, streamIdleTimeout time.Duration) (<-chan StreamChunk, *CallError) {
	bfContext := schemas.NewBifrostContext(ctx, schemas.NoDeadline)
	if streamIdleTimeout > 0 {
		bfContext.SetValue(schemas.BifrostContextKeyStreamIdleTimeout, streamIdleTimeout)
	}
	var source <-chan *schemas.BifrostStreamChunk
	switch kind {
	case "chat":
		chatRequest, callErr := e.chatRequest(bfContext, providerID, nativeModel, body)
		if callErr != nil {
			callErr.Log = "provider_request_rejected"
			return nil, callErr
		}
		stream, bfErr := e.client.ChatCompletionStreamRequest(bfContext, chatRequest)
		if bfErr != nil {
			callErr := classifyBifrostError(ctx, bfErr)
			callErr.Log = "upstream_request_failed"
			return nil, callErr
		}
		source = stream
	case "responses":
		responsesRequest, callErr := e.responsesRequest(bfContext, providerID, nativeModel, body)
		if callErr != nil {
			callErr.Log = "provider_request_rejected"
			return nil, callErr
		}
		stream, bfErr := e.client.ResponsesStreamRequest(bfContext, responsesRequest)
		if bfErr != nil {
			callErr := classifyBifrostError(ctx, bfErr)
			callErr.Log = "upstream_request_failed"
			return nil, callErr
		}
		source = stream
	default:
		return nil, &CallError{Class: "invalid_response", Status: 400, Log: "provider_request_rejected"}
	}

	output := make(chan StreamChunk, 8)
	go func() {
		defer close(output)
		for {
			select {
			case <-ctx.Done():
				return
			case chunk, ok := <-source:
				if !ok {
					return
				}
				if chunk == nil {
					continue
				}
				if chunk.BifrostError != nil {
					callErr := classifyBifrostError(ctx, chunk.BifrostError)
					callErr.Log = "upstream_request_failed"
					select {
					case output <- StreamChunk{Err: callErr}:
					case <-ctx.Done():
					}
					return
				}
				data, event, meaningful, err := sanitizeStreamChunk(chunk, nativeModel)
				if err != nil {
					select {
					case output <- StreamChunk{Err: &CallError{Class: "invalid_response", Status: 502, Detail: err.Error(), Scope: "provider"}}:
					case <-ctx.Done():
					}
					return
				}
				select {
				case output <- StreamChunk{Data: data, Event: event, Meaningful: meaningful}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output, nil
}

func (e *Executor) prepareRequestBody(ctx *schemas.BifrostContext, providerID, nativeModel string, body []byte) ([]byte, *CallError) {
	provider := e.providers[providerID]
	rewritten, err := rewriteRequestBody(body, nativeModel, provider.StripParams, provider.SetParams)
	if err != nil {
		return nil, &CallError{Class: "invalid_response", Status: 400, Detail: err.Error()}
	}
	if provider.BaseProvider == string(schemas.OpenAI) {
		ctx.SetValue(schemas.BifrostContextKeyUseRawRequestBody, true)
	}
	return rewritten, nil
}

func (e *Executor) chatRequest(ctx *schemas.BifrostContext, providerID, nativeModel string, body []byte) (*schemas.BifrostChatRequest, *CallError) {
	rewritten, callErr := e.prepareRequestBody(ctx, providerID, nativeModel, body)
	if callErr != nil {
		return nil, callErr
	}
	var wire struct {
		Messages  []schemas.ChatMessage `json:"messages"`
		MaxTokens *int                  `json:"max_tokens"`
	}
	if err := json.Unmarshal(rewritten, &wire); err != nil || len(wire.Messages) == 0 {
		detail := "messages are required"
		if err != nil {
			detail = err.Error()
		}
		return nil, &CallError{Class: "invalid_response", Status: 400, Detail: detail}
	}
	params := &schemas.ChatParameters{}
	if err := json.Unmarshal(rewritten, params); err != nil {
		return nil, &CallError{Class: "invalid_response", Status: 400, Detail: err.Error()}
	}
	if params.MaxCompletionTokens == nil && wire.MaxTokens != nil {
		params.MaxCompletionTokens = wire.MaxTokens
	}
	request := &schemas.BifrostChatRequest{
		Provider: e.bifrostProviders[providerID],
		Model:    nativeModel,
		Input:    wire.Messages,
		Params:   params,
	}
	if e.providers[providerID].BaseProvider == string(schemas.OpenAI) {
		request.RawRequestBody = rewritten
	}
	return request, nil
}

func (e *Executor) responsesRequest(ctx *schemas.BifrostContext, providerID, nativeModel string, body []byte) (*schemas.BifrostResponsesRequest, *CallError) {
	rewritten, callErr := e.prepareRequestBody(ctx, providerID, nativeModel, body)
	if callErr != nil {
		return nil, callErr
	}
	var wire struct {
		Input json.RawMessage `json:"input"`
	}
	if err := json.Unmarshal(rewritten, &wire); err != nil {
		return nil, &CallError{Class: "invalid_response", Status: 400, Detail: err.Error()}
	}
	input, err := parseResponsesInput(wire.Input)
	if err != nil {
		return nil, &CallError{Class: "invalid_response", Status: 400, Detail: err.Error()}
	}
	params := &schemas.ResponsesParameters{}
	if err := json.Unmarshal(rewritten, params); err != nil {
		return nil, &CallError{Class: "invalid_response", Status: 400, Detail: err.Error()}
	}
	request := &schemas.BifrostResponsesRequest{
		Provider: e.bifrostProviders[providerID],
		Model:    nativeModel,
		Input:    input,
		Params:   params,
	}
	if e.providers[providerID].BaseProvider == string(schemas.OpenAI) {
		request.RawRequestBody = rewritten
	}
	return request, nil
}

func rewriteRequestBody(body []byte, nativeModel string, strip []string, set map[string]json.RawMessage) ([]byte, error) {
	rewritten, err := sjson.SetBytes(body, "model", nativeModel)
	if err != nil {
		return nil, err
	}
	for _, key := range strip {
		rewritten, err = sjson.DeleteBytes(rewritten, key)
		if err != nil {
			return nil, err
		}
	}
	for key, value := range set {
		rewritten, err = sjson.SetRawBytes(rewritten, key, value)
		if err != nil {
			return nil, err
		}
	}
	return rewritten, nil
}

func parseResponsesInput(raw json.RawMessage) ([]schemas.ResponsesMessage, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, errors.New("input is required")
	}
	if trimmed[0] == '"' {
		var text string
		if err := json.Unmarshal(trimmed, &text); err != nil {
			return nil, err
		}
		messageType := schemas.ResponsesMessageTypeMessage
		role := schemas.ResponsesInputMessageRoleUser
		return []schemas.ResponsesMessage{{
			Type:    &messageType,
			Role:    &role,
			Content: &schemas.ResponsesMessageContent{ContentStr: &text},
		}}, nil
	}
	var messages []schemas.ResponsesMessage
	if err := json.Unmarshal(trimmed, &messages); err != nil {
		return nil, fmt.Errorf("input must be a string or array: %w", err)
	}
	if len(messages) == 0 {
		return nil, errors.New("input must not be empty")
	}
	return messages, nil
}

func classifyBifrostError(ctx context.Context, err *schemas.BifrostError) *CallError {
	detail := ""
	if err != nil {
		detail = err.GetErrorString()
	}
	if ctx.Err() != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return &CallError{Class: "timeout", Status: 504, Detail: ctx.Err().Error()}
		}
		return &CallError{Class: "cancelled", Status: 499, Detail: ctx.Err().Error()}
	}
	status := 502
	if err != nil && err.StatusCode != nil {
		status = *err.StatusCode
	}
	switch {
	case status == 429:
		return &CallError{Class: "429", Status: status, Scope: "provider", Detail: detail}
	case status == 401 || status == 402 || status == 403:
		return &CallError{Class: "5xx", Status: status, Scope: "provider", Detail: detail}
	case status == 404 || status == 410:
		return &CallError{Class: "404", Status: status, Scope: "target", Detail: detail}
	case status == 408 || status == 504:
		return &CallError{Class: "timeout", Status: status, Scope: "provider", Detail: detail}
	case status >= 500:
		return &CallError{Class: "5xx", Status: status, Scope: "provider", Detail: detail}
	}
	message := ""
	if err != nil && err.Error != nil {
		message = strings.ToLower(err.Error.Message)
	}
	if strings.Contains(message, "timeout") || strings.Contains(message, "deadline") {
		return &CallError{Class: "timeout", Status: 504, Scope: "provider", Detail: detail}
	}
	if strings.Contains(message, "connection") || strings.Contains(message, "network") || strings.Contains(message, "dns") {
		return &CallError{Class: "connection_error", Status: 502, Scope: "provider", Detail: detail}
	}
	return &CallError{Class: "invalid_response", Status: status, Scope: "target", Detail: detail}
}

func marshalSanitized(value any, logicalModel string) ([]byte, *CallError) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, &CallError{Class: "invalid_response", Status: 502, Detail: err.Error(), Scope: "provider"}
	}
	data, err = sanitizeJSON(data, logicalModel)
	if err != nil {
		return nil, &CallError{Class: "invalid_response", Status: 502, Detail: err.Error(), Scope: "provider"}
	}
	return data, nil
}

func sanitizeStreamChunk(chunk *schemas.BifrostStreamChunk, nativeModel string) ([]byte, string, bool, error) {
	event := ""
	if chunk.BifrostResponsesStreamResponse != nil {
		event = string(chunk.BifrostResponsesStreamResponse.Type)
	}
	populateChatResponseReasoning(chunk.BifrostChatResponse)
	data, err := json.Marshal(chunk)
	if err != nil {
		return nil, "", false, err
	}
	data, err = sanitizeJSON(data, nativeModel)
	if err != nil {
		return nil, "", false, err
	}
	return data, event, meaningfulPayload(data, event), nil
}

func sanitizeJSON(data []byte, modelName string) ([]byte, error) {
	var object map[string]any
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, err
	}
	delete(object, "extra_fields")
	delete(object, "provider_extra_fields")
	if modelName != "" {
		if _, exists := object["model"]; exists {
			object["model"] = modelName
		}
		if response, ok := object["response"].(map[string]any); ok {
			response["model"] = modelName
			delete(response, "extra_fields")
			delete(response, "provider_extra_fields")
		}
	}
	return json.Marshal(object)
}

func populateChatResponseReasoning(response *schemas.BifrostChatResponse) {
	if response == nil {
		return
	}
	for i := range response.Choices {
		choice := &response.Choices[i]
		if choice.ChatNonStreamResponseChoice != nil &&
			choice.ChatNonStreamResponseChoice.Message != nil &&
			choice.ChatNonStreamResponseChoice.Message.ChatAssistantMessage != nil {
			msg := choice.ChatNonStreamResponseChoice.Message.ChatAssistantMessage
			if msg.Reasoning == nil || *msg.Reasoning == "" {
				if text := reasoningTextFromDetails(msg.ReasoningDetails); text != "" {
					msg.Reasoning = &text
				}
			}
		}
		if choice.ChatStreamResponseChoice != nil && choice.ChatStreamResponseChoice.Delta != nil {
			delta := choice.ChatStreamResponseChoice.Delta
			if delta.Reasoning == nil || *delta.Reasoning == "" {
				if text := reasoningTextFromDetails(delta.ReasoningDetails); text != "" {
					delta.Reasoning = &text
				}
			}
		}
	}
}

func reasoningTextFromDetails(details []schemas.ChatReasoningDetails) string {
	var builder strings.Builder
	for _, detail := range details {
		if detail.Type == schemas.BifrostReasoningDetailsTypeText && detail.Text != nil {
			builder.WriteString(*detail.Text)
		}
	}
	return builder.String()
}

func meaningfulPayload(data []byte, event string) bool {
	if event != "" {
		switch event {
		case "response.output_text.delta", "response.reasoning_summary_text.delta",
			"response.function_call_arguments.delta", "response.custom_tool_call_input.delta":
			var object map[string]any
			if json.Unmarshal(data, &object) == nil {
				if delta, ok := object["delta"].(string); ok {
					return delta != ""
				}
			}
		}
		return false
	}
	var object struct {
		Choices []struct {
			Delta map[string]json.RawMessage `json:"delta"`
		} `json:"choices"`
	}
	if json.Unmarshal(data, &object) != nil {
		return false
	}
	for _, choice := range object.Choices {
		for _, key := range []string{"content", "reasoning", "reasoning_content", "tool_calls"} {
			raw := bytes.TrimSpace(choice.Delta[key])
			if key != "tool_calls" {
				var value string
				if json.Unmarshal(raw, &value) == nil && strings.TrimSpace(strings.ReplaceAll(value, "\u200b", "")) == "" {
					continue
				}
			}
			if len(raw) > 0 && !bytes.Equal(raw, []byte("null")) && !bytes.Equal(raw, []byte(`""`)) && !bytes.Equal(raw, []byte("[]")) {
				return true
			}
		}
	}
	return false
}

func (e *Executor) Close() error {
	if e.client != nil {
		e.client.Shutdown()
	}
	return nil
}

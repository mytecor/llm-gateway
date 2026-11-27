package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCredentialsStayOnGatewayAndErrorsExposeLimits(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("redirect was followed") }))
	defer target.Close()
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-LLM-Priority") != "background" || r.Header.Get("Authorization") != "Bearer example" {
			t.Error("missing identity or priority")
		}
		switch r.URL.Path {
		case "/v1/chat/completions":
			http.Redirect(w, r, target.URL, 307)
		case "/v1/embeddings":
			w.Header().Set("X-LLM-Route-Limit", "3")
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(429)
			_, _ = w.Write([]byte(`{"error":{"type":"route_queue_full"}}`))
		}
	}))
	defer gateway.Close()
	c, err := New(gateway.URL+"/v1/", "example", WithHTTPClient(gateway.Client()), WithPriority(PriorityBackground))
	if err != nil {
		t.Fatal(err)
	}
	var failure *Error
	_, err = c.Chat(t.Context(), ChatRequest{Model: "smart", User: "hello"})
	if !errors.As(err, &failure) || failure.Status != 307 {
		t.Fatal(err)
	}
	_, err = c.Embed(t.Context(), EmbedRequest{Text: "abc", Truncate: true})
	if !errors.As(err, &failure) || !failure.IsRateLimit() || failure.RouteLimit != 3 || failure.RetryAfter != 7*time.Second || failure.Code != "route_queue_full" {
		t.Fatalf("wrong admission error %+v", failure)
	}
}
func TestTypedOperations(t *testing.T) {
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/chat/completions":
			var body struct {
				ResponseFormat struct {
					JSONSchema struct {
						Strict bool
						Name   string
						Schema json.RawMessage
					} `json:"json_schema"`
				} `json:"response_format"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || !body.ResponseFormat.JSONSchema.Strict || body.ResponseFormat.JSONSchema.Name != "count" {
				t.Error("strict format missing")
			}
			_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"17"},"finish_reason":"stop"}]}`))
		case "/v1/embeddings":
			var body struct {
				Truncate bool
				Input    []struct{ Parts []struct{ Text string } }
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || !body.Truncate || len(body.Input) != 2 || body.Input[1].Parts[0].Text != "second" {
				t.Error("unexpected embedding payload")
			}
			first, second := make([]float32, 128), make([]float32, 128)
			first[0] = 1
			second[1] = 1
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{map[string]any{"index": 1, "embedding": second}, map[string]any{"index": 0, "embedding": first}}})
		case "/v1/systemone":
			if r.Header.Get("TypeSafe-Danger-Zone-Long-Context") != "true" {
				t.Error("missing decision header")
			}
			var req struct{ Model string }
			_ = json.NewDecoder(r.Body).Decode(&req)
			if req.Model == "invalid" {
				_, _ = w.Write([]byte(`{"model":"native","answers":{"q":{"type":"noul","noul":1.2}}}`))
				return
			}
			_, _ = w.Write([]byte(`{"model":"native","answers":{"q":{"type":"noul","noul":0.2}}}`))
		default:
			t.Error(r.URL.Path)
		}
	}))
	defer gateway.Close()
	c, err := New(gateway.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	content, err := c.ChatJSON(t.Context(), ChatJSONRequest{Model: "smart", User: "count", SchemaName: "count", StrictSchema: json.RawMessage(`{"type":"integer"}`)})
	if err != nil || string(content) != "17" {
		t.Fatal(string(content), err)
	}
	vectors, err := c.EmbedBatch(t.Context(), EmbedBatchRequest{Inputs: []EmbeddingInput{{Text: "first"}, {Text: "second"}}, Dimensions: 128, Truncate: true})
	if err != nil || vectors[0][0] != 1 || vectors[1][1] != 1 {
		t.Fatal("vector ordering", err)
	}
	r := DecisionRequest{Model: "jev", State: "state", Questions: map[string]Question{"q": {Type: "noul"}}, LongContext: true}
	decision, err := c.Decide(context.Background(), r)
	if err != nil || decision.Answers["q"].Noul == nil || *decision.Answers["q"].Noul != 0.2 {
		t.Fatal(decision, err)
	}
	r.Model = "invalid"
	if _, err := c.Decide(t.Context(), r); err == nil {
		t.Fatal("accepted out-of-range answer")
	}
}

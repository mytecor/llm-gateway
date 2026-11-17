package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mytecor/llm-gateway/driverproto"
)

func TestUpstreamErrorsAndInvalidVectors(t *testing.T) {
	for _, scenario := range []string{"queue", "invalid", "redirect"} {
		t.Run(scenario, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch scenario {
				case "queue":
					w.Header().Set("Retry-After", "2")
					w.WriteHeader(429)
					_, _ = w.Write([]byte(`{"error":{"code":"queue_full"}}`))
				case "redirect":
					http.Redirect(w, r, "/other", 302)
				default:
					_, _ = w.Write([]byte(`{"embeddings":[{"values":[0],"tokens":1}]}`))
				}
			}))
			defer server.Close()
			cfg, _ := json.Marshal(config{InferenceURL: server.URL})
			exec, err := newExecutor(cfg)
			if err != nil {
				t.Fatal(err)
			}
			_, failure := exec.do(context.Background(), driverproto.Command[json.RawMessage]{Kind: "embeddings", Model: "native", Body: json.RawMessage(`{"model":"logical","items":[{"parts":[{"text":"hello"}],"dimensions":128}]}`)})
			if failure == nil {
				t.Fatal("invalid upstream succeeded")
			}
			if scenario == "queue" && (failure.Status != 429 || failure.RetryAfterMs != 2000 || failure.Code != "queue_full") {
				t.Fatal(failure)
			}
		})
	}
}

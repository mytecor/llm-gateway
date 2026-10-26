package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mytecor/llm-gateway/driverproto"
)

func TestModelsCommandUsesProviderDiscoveryAndCredentials(t *testing.T) {
	for _, base := range []string{"/v1", "/functions/v1/gonka"} {
		t.Run(base, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != base+"/models" {
					t.Errorf("path=%q", r.URL.Path)
				}
				if r.Header.Get("Authorization") != "Bearer provider-key" {
					t.Error("discovery credential missing")
				}
				_, _ = w.Write([]byte(`{"data":[{"id":"zeta"},{"id":"alpha"},{"id":"alpha"}]}`))
			}))
			defer server.Close()
			config, _ := json.Marshal(map[string]any{"base_provider": "openai", "inference_url": server.URL + base, "api_key": "provider-key"})
			input, _ := json.Marshal(inboundMessage{Type: "models", ID: "provider", ProtocolVersion: driverproto.Version, Config: config})
			var output bytes.Buffer
			if err := serve(context.Background(), bytes.NewReader(append(input, '\n')), &output); err != nil {
				t.Fatal(err)
			}
			var result outboundMessage
			if err := json.Unmarshal(output.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if result.Type != "models" || len(result.Models) != 2 || result.Models[0].ID != "alpha" || result.Models[1].ID != "zeta" {
				t.Fatalf("unexpected models event: %+v", result)
			}
		})
	}
}

func TestExplicitCatalogAndFailures(t *testing.T) {
	for _, tc := range []struct {
		body   string
		status int
		valid  bool
	}{
		{`{"data":[{"id":"model"}]}`, 200, true},
		{`{"data":[]}`, 200, false},
		{`not-json`, 200, false},
		{`{"error":"unavailable"}`, 503, false},
	} {
		t.Run(tc.body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/custom/catalog" {
					t.Errorf("explicit URL ignored: %s", r.URL.Path)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			raw, _ := json.Marshal(map[string]any{"inference_url": server.URL + "/unused", "base_provider": "openai"})
			_, err := listModels(context.Background(), inboundMessage{ID: "provider", Config: raw, ModelsURL: server.URL + "/custom/catalog"})
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v error=%v", tc.valid, err)
			}
		})
	}
}

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"image"
	"image/png"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/driverproto/systemone"
	"github.com/mytecor/llm-gateway/internal/config"
	"github.com/mytecor/llm-gateway/internal/executor"
	"github.com/mytecor/llm-gateway/internal/policy"
	"github.com/mytecor/llm-gateway/internal/provider"
	"github.com/mytecor/llm-gateway/internal/scheduler"
	"github.com/mytecor/llm-gateway/internal/subsystem"
)

const nativeDecision = `{"model":"jev","state":"fixture","questions":{"q":{"type":"noul","instructions":"Yes?"}}}`
const nativeAnswer = `{"model":"jev-1.14.0","answers":{"q":{"type":"noul","noul":0.92}},"usage":{"input_tokens":8,"output_tokens":1}}`

func nativeGateway(t *testing.T, handlers ...http.HandlerFunc) *httptest.Server {
	t.Helper()
	var providers []provider.Provider
	var targets []policy.TargetSpec
	for i, handler := range handlers {
		upstream := httptest.NewServer(handler)
		t.Cleanup(upstream.Close)
		id, dialect, native := "typesafe", "typesafe", "jev-latest"
		if i == 0 {
			id, dialect, native = "gonka", "gonka", "jev"
		}
		providers = append(providers, provider.Provider{ID: id, Driver: "jev", Config: map[string]any{"dialect": dialect, "inference_url": upstream.URL + "/v1", "api_key": "upstream-fixture"}})
		targets = append(targets, policy.TargetSpec{Provider: id, Native: native})
	}
	noHedges := 0
	cfg := config.Config{LogLevel: "silent", Providers: providers, Models: map[string]policy.ModelSpec{"jev": {Profile: "balanced", Timeout: config.Duration{Duration: 5 * time.Second}, Budget: &policy.BudgetSpec{MaxHedges: &noHedges}, Tiers: []policy.TierSpec{{Targets: targets}}}}}
	compiled, err := config.PrepareAndCompile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Use the existing deterministic balancing strategy to exercise both members
	// without timing/exploration assumptions. Production keeps balanced defaults.
	compiled.Models["jev"].Balance.Strategy = "round_robin"
	compiled.ClientKeys = map[string]string{"fixture": "client-fixture"}
	registry, err := executor.New(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	catalog := subsystem.NewCatalog(compiled)
	runner := scheduler.NewRunner(compiled.RoutingPlan(), catalog, registry)
	t.Cleanup(func() { _ = runner.Close() })
	api := httptest.NewServer(NewServer(compiled, catalog, runner))
	t.Cleanup(api.Close)
	return api
}
func nativePost(t *testing.T, api, path, body string, headers map[string]string) (int, []byte, http.Header) {
	t.Helper()
	request, _ := http.NewRequest(http.MethodPost, api+path, strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer client-fixture")
	request.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		request.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	data, _ := io.ReadAll(res.Body)
	return res.StatusCode, data, res.Header
}
func TestSystemOneRealDriverAuthAndPool(t *testing.T) {
	var gonka, typesafe atomic.Int32
	api := nativeGateway(t, func(w http.ResponseWriter, r *http.Request) { gonka.Add(1); _, _ = io.WriteString(w, nativeAnswer) }, func(w http.ResponseWriter, r *http.Request) { typesafe.Add(1); _, _ = io.WriteString(w, nativeAnswer) })
	for _, path := range []string{"/v1/systemone", "/v1/decisions"} {
		request, _ := http.NewRequest(http.MethodPost, api.URL+path, strings.NewReader(nativeDecision))
		res, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 401 {
			t.Fatal("decision endpoint bypasses auth")
		}
		status, body, _ := nativePost(t, api.URL, path, nativeDecision, nil)
		if status != 200 || !bytes.Contains(body, []byte(`"model":"jev-1.14.0"`)) {
			t.Fatalf("%d %s", status, body)
		}
	}
	if gonka.Load() == 0 || typesafe.Load() == 0 {
		t.Fatalf("pool did not balance: gonka=%d typesafe=%d", gonka.Load(), typesafe.Load())
	}
	before := gonka.Load()
	status, body, _ := nativePost(t, api.URL, "/v1/systemone", nativeDecision, map[string]string{systemone.LongContextHeader: "true"})
	if status != 200 || gonka.Load() != before {
		t.Fatalf("long-context reached Gonka: %d %s", status, body)
	}
	for _, path := range []string{"/v1/chat/completions", "/v1/responses"} {
		status, body, _ := nativePost(t, api.URL, path, `{"model":"jev","messages":[]}`, nil)
		if status != 503 || !bytes.Contains(body, []byte("no_compatible_provider")) {
			t.Fatalf("JEV accepted wrong kind: %d %s", status, body)
		}
	}
}
func TestSystemOneProviderRecoveryAndInvalidAnswer(t *testing.T) {
	for _, failure := range []string{"forbidden", "invalid", "bad-request"} {
		t.Run(failure, func(t *testing.T) {
			var bad, good atomic.Int32
			api := nativeGateway(t, func(w http.ResponseWriter, r *http.Request) {
				bad.Add(1)
				switch failure {
				case "forbidden":
					w.WriteHeader(403)
				case "bad-request":
					w.WriteHeader(422)
				default:
					_, _ = io.WriteString(w, `{"model":"jev","answers":{"q":{"type":"noul","noul":4}}}`)
				}
			}, func(w http.ResponseWriter, r *http.Request) { good.Add(1); _, _ = io.WriteString(w, nativeAnswer) })
			// Round-robin starts on the first target, ensuring this call exercises recovery.
			status, body, _ := nativePost(t, api.URL, "/v1/systemone", nativeDecision, nil)
			if failure == "bad-request" {
				if status != 422 || good.Load() != 0 {
					t.Fatalf("request error retried: %d %s good=%d", status, body, good.Load())
				}
			} else if status != 200 || good.Load() == 0 {
				t.Fatalf("failed to recover: %d %s bad=%d good=%d", status, body, bad.Load(), good.Load())
			}
			if bad.Load() == 0 {
				t.Fatal("failure fixture was never exercised")
			}
		})
	}
}
func TestSystemOneMultipartImagesAndLimits(t *testing.T) {
	var gonka, official atomic.Int32
	var imageData bytes.Buffer
	_ = png.Encode(&imageData, image.NewRGBA(image.Rect(0, 0, 280, 280)))
	api := nativeGateway(t, func(w http.ResponseWriter, r *http.Request) { gonka.Add(1); w.WriteHeader(500) }, func(w http.ResponseWriter, r *http.Request) {
		official.Add(1)
		if r.Header.Get(systemone.ImagesHeader) != "true" {
			t.Error("lost image opt-in")
		}
		reader, err := r.MultipartReader()
		if err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		part, err := reader.NextPart()
		if err != nil {
			t.Error(err)
			return
		}
		if part.FormName() != "request" {
			t.Error("JSON not first")
		}
		var req systemone.Request
		_ = json.NewDecoder(part).Decode(&req)
		if req.Model != "jev-latest" || len(req.Assets) != 1 {
			t.Error("invalid native request")
		}
		part, err = reader.NextPart()
		if err != nil {
			t.Error(err)
			return
		}
		data, _ := io.ReadAll(part)
		if !bytes.Equal(data, imageData.Bytes()) {
			t.Error("image bytes changed")
		}
		_, _ = io.WriteString(w, `{"model":"jev-1.14.0","answers":{"q":{"type":"box","box":{"top":0,"bottom":1,"left":0,"right":1},"confidence":null}}}`)
	})
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, _ := writer.CreateFormField("request")
	_, _ = io.WriteString(part, `{"model":"jev","state":"photo-ref","assets":{"photo-ref":{"part":"photo"}},"questions":{"q":{"type":"box","instructions":"Locate it"}}}`)
	part, _ = writer.CreateFormFile("photo", "photo.png")
	_, _ = part.Write(imageData.Bytes())
	_ = writer.Close()
	status, data, _ := nativePost(t, api.URL, "/v1/systemone", body.String(), map[string]string{"Content-Type": writer.FormDataContentType(), systemone.ImagesHeader: "true"})
	if status != 200 || gonka.Load() != 0 || official.Load() != 1 {
		t.Fatalf("multipart routing: %d %s gonka=%d official=%d", status, data, gonka.Load(), official.Load())
	}
	status, _, _ = nativePost(t, api.URL, "/v1/systemone", body.String(), map[string]string{"Content-Type": writer.FormDataContentType()})
	if status != 400 || official.Load() != 1 {
		t.Fatal("images accepted without explicit opt in")
	}
	status, _, _ = nativePost(t, api.URL, "/v1/systemone", strings.Repeat(" ", systemone.MaxHTTPBytes+1), nil)
	if status != 413 {
		t.Fatalf("oversize status %d", status)
	}
}
func TestSystemOneRetryAfterCannotFailOpen(t *testing.T) {
	var calls atomic.Int32
	api := nativeGateway(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(429)
	})
	for range 2 {
		status, body, h := nativePost(t, api.URL, "/v1/systemone", nativeDecision, nil)
		if status != 429 || h.Get("Retry-After") == "" {
			t.Fatalf("retry after not relayed: %d %s %v", status, body, h)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("Retry-After bypassed by fail-open: %d calls", calls.Load())
	}
}
func TestSystemOneLargeCommandPreservesOtherLimits(t *testing.T) {
	api := nativeGateway(t, func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		if len(data) < 17<<20 {
			t.Error("large body truncated")
		}
		_, _ = io.WriteString(w, nativeAnswer)
	})
	large := strings.Replace(nativeDecision, `"fixture"`, `"`+strings.Repeat("a", 17<<20)+`"`, 1)
	status, body, _ := nativePost(t, api.URL, "/v1/systemone", large, nil)
	if status != 200 {
		t.Fatalf("large native command failed: %d %s", status, body)
	}
	status, _, _ = nativePost(t, api.URL, "/v1/chat/completions", large, nil)
	if status != 400 {
		t.Fatalf("Chat body limit changed: %d", status)
	}
}

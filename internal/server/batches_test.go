package server

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mytecor/llm-gateway/driverproto/batch"
	"github.com/mytecor/llm-gateway/internal/config"
	"github.com/mytecor/llm-gateway/internal/model"
)

type batchTestExecutor struct {
	submissions int
	uploaded    map[string]string
	openRouter  bool
}

func (e *batchTestExecutor) Do(_ context.Context, _ model.Target, req model.ExecuteRequest) ([]byte, *model.CallError) {
	var cmd batch.Command
	if req.Kind != model.RequestBatch || !req.SingleAttempt || json.Unmarshal(req.Body, &cmd) != nil {
		return nil, &model.CallError{Status: 500}
	}
	result := batch.Result{Job: "projects/test/locations/global/batchPredictionJobs/123", State: "JOB_STATE_SUCCEEDED"}
	switch cmd.Operation {
	case batch.Upload:
		body, err := os.ReadFile(cmd.File)
		if err != nil {
			return nil, &model.CallError{Status: 500}
		}
		e.uploaded[cmd.Object] = string(body)
		return []byte("{}"), nil
	case batch.Submit:
		e.submissions++
		if e.openRouter {
			break
		}
		// Simulate a Vertex job being accepted before its response was lost.
		return nil, &model.CallError{Status: 502, Code: "batch_upstream_unavailable"}
	case batch.Find, batch.Status:
	case batch.Prepare:
		body, err := os.ReadFile(cmd.Input)
		if err != nil || !strings.Contains(string(body), "Batch item 1") {
			return nil, &model.CallError{Status: 500}
		}
		break
	case batch.Download:
		data := "{\"status\":\"\"}\n"
		if e.openRouter {
			data = `{"custom_id":"2","response":{"status_code":200,"request_id":"req-2","body":{"choices":[]}},"error":null}
{"custom_id":"1","response":null,"error":{"message":"provider rejected"}}
`
		}
		if err := os.WriteFile(cmd.File, []byte(data), 0o600); err != nil {
			return nil, &model.CallError{Status: 500}
		}
	default:
		return nil, &model.CallError{Status: 500}
	}
	return marshalBatch(result), nil
}

func TestOpenAIFileBatchLifecycleWithoutStorage(t *testing.T) {
	executor := &batchTestExecutor{openRouter: true}
	cfg := &config.CompiledConfig{Raw: config.Config{Batch: &config.BatchConfig{
		Client: "catalog", Model: "deepseek", Prefix: "catalog", StateDir: filepath.Join(t.TempDir(), "state"),
		Routes: map[string]config.BatchRoute{"deepseek": {Backend: "openrouter", Provider: "or", Native: "deepseek/model"}},
	}}, ClientKeys: map[string]string{"catalog": "key"}}
	gateway := &Server{config: cfg, mux: http.NewServeMux()}
	if err := gateway.EnableBatch(executor); err != nil {
		t.Fatal(err)
	}
	do := func(method, uri string, body *bytes.Buffer, mime string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, uri, body)
		r.Header.Set("Authorization", "Bearer key")
		r.Header.Set("Content-Type", mime)
		w := httptest.NewRecorder()
		gateway.ServeHTTP(w, r)
		return w
	}
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	if err := form.WriteField("purpose", "batch"); err != nil {
		t.Fatal(err)
	}
	input, err := form.CreateFormFile("file", "batch.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"one", "two"} {
		_, _ = input.Write([]byte(`{"custom_id":"` + id + `","method":"POST","url":"/v1/chat/completions","body":{"model":"deepseek","messages":[{"role":"user","content":"hello"}]}}` + "\n"))
	}
	if err := form.Close(); err != nil {
		t.Fatal(err)
	}
	out := do("POST", "/v1/files", &body, form.FormDataContentType())
	var file struct{ ID string }
	if out.Code != 200 || json.Unmarshal(out.Body.Bytes(), &file) != nil || file.ID == "" {
		t.Fatalf("create file: %d %s", out.Code, out.Body.String())
	}
	makePayload := func() *bytes.Buffer {
		return bytes.NewBuffer(marshalBatch(map[string]string{"input_file_id": file.ID, "endpoint": "/v1/chat/completions", "completion_window": "24h"}))
	}
	out = do("POST", "/v1/batches", makePayload(), "application/json")
	var job struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		File   string `json:"output_file_id"`
	}
	if out.Code != 202 || json.Unmarshal(out.Body.Bytes(), &job) != nil || job.File == "" || job.Status != "completed" {
		t.Fatalf("create job: %d %s", out.Code, out.Body.String())
	}
	out = do("POST", "/v1/batches", makePayload(), "application/json")
	if out.Code != 200 || executor.submissions != 1 {
		t.Fatalf("create called %d times, status %d", executor.submissions, out.Code)
	}
	out = do("GET", "/v1/files/"+job.File+"/content", &bytes.Buffer{}, "")
	lines := strings.Split(strings.TrimSpace(out.Body.String()), "\n")
	if out.Code != 200 || len(lines) != 2 || !strings.Contains(lines[0], `"custom_id":"two"`) ||
		!strings.Contains(lines[1], `"custom_id":"one"`) || strings.Contains(out.Body.String(), "provider rejected") {
		t.Fatalf("results: %d %s", out.Code, out.Body.String())
	}
}

func (*batchTestExecutor) Stream(context.Context, model.Target, model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
	return nil, &model.CallError{Status: 500}
}

func (*batchTestExecutor) Close() error { return nil }

func TestBatchRoutesKeepTransfersScopedAndReconcileSubmission(t *testing.T) {
	executor := &batchTestExecutor{uploaded: make(map[string]string)}
	cfg := &config.CompiledConfig{
		Raw: config.Config{Batch: &config.BatchConfig{
			Client: "catalog", Model: "smart", StorageProvider: "vertex",
			Routes: map[string]config.BatchRoute{"smart": {Backend: "vertex", Provider: "vertex", Native: "gemini-3.8-flash"}},
			Bucket: "private-bucket", Prefix: "catalog", StateDir: filepath.Join(t.TempDir(), "state"),
		}},
		ClientKeys: map[string]string{"catalog": "key", "other": "another"},
	}
	newGateway := func() *Server {
		handler := &Server{config: cfg, mux: http.NewServeMux()}
		if err := handler.EnableBatch(executor); err != nil {
			t.Fatal(err)
		}
		return handler
	}
	gateway := newGateway()
	do := func(method, path, key, body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		result := httptest.NewRecorder()
		gateway.ServeHTTP(result, req)
		return result
	}
	id := strings.Repeat("a", 32)
	endpoint := "/v1/batches/" + id
	for key, want := range map[string]int{"": 401, "another": 403, "key": 200} {
		if got := do("GET", endpoint+"/input", key, ""); got.Code != want {
			t.Fatalf("input: got %d, want %d", got.Code, want)
		}
	}
	var input struct {
		StoragePrefix string `json:"storage_prefix"`
	}
	if err := json.Unmarshal(do("GET", endpoint+"/input", "key", "").Body.Bytes(), &input); err != nil {
		t.Fatal(err)
	}
	name := "photos/" + strings.Repeat("b", 64) + ".jpg"
	if out := do("PUT", endpoint+"/files/"+name, "key", "image"); out.Code != 204 {
		t.Fatalf("photo upload: %d %s", out.Code, out.Body.String())
	}
	request := func(uri string) string {
		return `{"request":{"contents":[{"role":"user","parts":[{"text":"image"},{"fileData":{"fileUri":"` + uri +
			`","mimeType":"image/jpeg"}}]}],"systemInstruction":{"parts":[{"text":"extract facts"}]},"generationConfig":{"responseMimeType":"application/json","maxOutputTokens":900,"thinkingConfig":{"thinkingLevel":"LOW"}}}}`
	}
	if out := do("PUT", endpoint+"/files/requests.jsonl", "key", request("gs://unrelated-bucket/"+name)); out.Code != 400 {
		t.Fatalf("outside file accepted: %d", out.Code)
	}
	if out := do("PUT", endpoint+"/files/requests.jsonl", "key", request(input.StoragePrefix+"/"+name)); out.Code != 204 {
		t.Fatalf("JSONL upload: %d %s", out.Code, out.Body.String())
	}
	submission := `{"model":"smart","id":"` + id + `"}`
	if out := do("POST", "/v1/batches", "key", submission); out.Code != 502 {
		t.Fatalf("first submit: %d %s", out.Code, out.Body.String())
	}
	gateway = newGateway()
	if out := do("POST", "/v1/batches", "key", submission); out.Code != 200 || executor.submissions != 1 {
		t.Fatalf("resubmit: %d, upstream creates=%d, %s", out.Code, executor.submissions, out.Body.String())
	}
	if out := do("PUT", endpoint+"/files/"+name, "key", "new"); out.Code != 409 {
		t.Fatalf("submitted inputs reopened: %d", out.Code)
	}
	if out := do("GET", endpoint+"/results", "key", ""); out.Code != 200 || out.Body.String() != "{\"status\":\"\"}\n" {
		t.Fatalf("results: %d %s", out.Code, out.Body.String())
	}
}

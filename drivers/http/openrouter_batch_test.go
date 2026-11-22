package main

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/driverproto/batch"
)

func TestOpenRouterBatchPreparationSignsOnlyManagedImage(t *testing.T) {
	dir := t.TempDir()
	const image = "gs://bucket/example/photos/file.jpg"
	requests := filepath.Join(dir, "requests.jsonl")
	payload := `{"request":{"contents":[{"parts":[{"text":"describe"},{"fileData":{"fileUri":"` + image + `"}}]}],"systemInstruction":{"parts":[{"text":"instruction"}]},"generationConfig":{"maxOutputTokens":900,"responseMimeType":"application/json"}}}` + "\n"
	if err := os.WriteFile(requests, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "output.json")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	const email = "test@example.test"
	service, _ := json.Marshal(map[string]string{"client_email": email,
		"private_key": string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))})
	cmd := batch.Command{Bucket: "bucket", Prefix: "example", Input: requests, File: target, Model: "deepseek/model",
		ImageProviders: []string{"together"}}
	if err := prepareOpenRouterBatch(t.Context(), ProviderConfig{BaseProvider: "vertex", VertexAuthCredentials: string(service)}, cmd); err != nil {
		t.Fatalf("prepare: %+v", err)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(data), `{"endpoint":"/v1/chat/completions","model":"deepseek/model","provider":`) {
		t.Fatal("OpenRouter routing fields must precede requests")
	}
	var batch struct {
		Requests []struct {
			Body struct {
				Messages []json.RawMessage `json:"messages"`
			} `json:"body"`
		} `json:"requests"`
	}
	var user struct {
		Content []struct {
			Image struct{ URL string } `json:"image_url"`
		} `json:"content"`
	}
	if json.Unmarshal(data, &batch) != nil || len(batch.Requests) != 1 ||
		len(batch.Requests[0].Body.Messages) != 2 || json.Unmarshal(batch.Requests[0].Body.Messages[1], &user) != nil ||
		len(user.Content) != 2 {
		t.Fatal("unexpected OpenRouter request")
	}
	signed, err := url.Parse(user.Content[1].Image.URL)
	if err != nil || signed.Host != "storage.googleapis.com" || signed.Path != "/bucket/example/photos/file.jpg" {
		t.Fatal("signed URL references a different object")
	}
	q := signed.Query()
	stamp := q.Get("X-Goog-Date")
	if _, err := time.Parse("20060102T150405Z", stamp); err != nil || q.Get("X-Goog-Expires") != "172800" {
		t.Fatal("unexpected signed URL lifetime")
	}
	sig, err := hex.DecodeString(q.Get("X-Goog-Signature"))
	q.Del("X-Goog-Signature")
	hash := sha256.Sum256([]byte("GET\n" + signed.EscapedPath() + "\n" + q.Encode() + "\nhost:storage.googleapis.com\n\nhost\nUNSIGNED-PAYLOAD"))
	_, scope, _ := strings.Cut(q.Get("X-Goog-Credential"), "/")
	digest := sha256.Sum256([]byte("GOOG4-RSA-SHA256\n" + stamp + "\n" + scope + "\n" + hex.EncodeToString(hash[:])))
	if err != nil || rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, digest[:], sig) != nil {
		t.Fatal("invalid signed URL signature")
	}
}

func TestOpenRouterBatchResponseStream(t *testing.T) {
	input := `{"id":"batch_id","status":"completed","request_counts":{"completed":1,"failed":1},"results":[
      {"custom_id":"2",
       "response":{"status_code":200}},
      {"custom_id":"1","error":{"code":400}}]}`
	var lines bytes.Buffer
	batch, rows, err := readOpenRouterBatch(strings.NewReader(input), &lines)
	if err != nil || batch.State != "JOB_STATE_SUCCEEDED" || batch.Successful != "1" ||
		batch.Failed != "1" || rows != 2 || strings.Count(lines.String(), "\n") != rows {
		t.Fatalf("result %+v, rows %d, error %v", batch, rows, err)
	}
}

func TestOpenRouterBatchHyphenIDAndNullResults(t *testing.T) {
	input := `{"id":"batch-1795378140-Uibpm0S6dx6pLtOajyhP","status":"in_progress","request_counts":{"total":1860,"completed":0,"failed":0},"results":null}`
	batch, rows, err := readOpenRouterBatch(strings.NewReader(input), nil)
	if err != nil || !routerBatchID.MatchString(batch.Job) || batch.State != "JOB_STATE_RUNNING" || rows != 0 {
		t.Fatalf("result %+v, rows %d, error %v", batch, rows, err)
	}
}

package main

import (
	"crypto/md5"
	"encoding/base64"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mytecor/llm-gateway/driverproto/batch"
)

type batchUploadTransport func(*http.Request) (*http.Response, error)

func (f batchUploadTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestVertexBatchUploadAcceptsIdenticalExistingObject(t *testing.T) {
	data := []byte("synthetic image data")
	path := filepath.Join(t.TempDir(), "photo.jpg")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	md5sum := md5.Sum(data)
	calls := 0
	c := vertexBatchClient{client: &http.Client{Transport: batchUploadTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		code, payload := http.StatusPreconditionFailed, ""
		if r.Method == http.MethodPost {
			got, err := io.ReadAll(r.Body)
			_ = r.Body.Close()
			if err != nil || string(got) != string(data) || r.URL.Query().Get("ifGenerationMatch") != "0" {
				t.Fatalf("invalid upload: %v", err)
			}
		} else if r.Method == http.MethodGet {
			code = http.StatusOK
			payload = `{"md5Hash":"` + base64.StdEncoding.EncodeToString(md5sum[:]) + `"}`
		} else {
			t.Fatalf("unexpected call %s", r.Method)
		}
		return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(payload))}, nil
	})}}
	if err := c.upload(t.Context(), batch.Command{Bucket: "test", Prefix: "run", Object: "photos/photo.jpg", File: path}); err != nil || calls != 2 {
		t.Fatalf("replayed upload = %+v, calls %d", err, calls)
	}
}

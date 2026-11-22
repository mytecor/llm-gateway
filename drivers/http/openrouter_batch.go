package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mytecor/llm-gateway/driverproto/batch"
	"golang.org/x/oauth2"
)

const openRouterBatchURL = "https://openrouter.ai/api/v1/batches"

func isOpenRouter(p ProviderConfig) bool {
	return p.BaseProvider == "openai" && strings.TrimRight(p.InferenceURL, "/") == "https://openrouter.ai/api/v1"
}

func supportsProviderBatch(p ProviderConfig) bool {
	return p.BaseProvider == "vertex" || isOpenRouter(p)
}

type gcsURLSigner struct {
	Email string
	Key   *rsa.PrivateKey
}

func newGCSURLSigner(raw string) (gcsURLSigner, error) {
	var config struct {
		Email      string `json:"client_email"`
		PrivateKey string `json:"private_key"`
	}
	data := []byte(raw)
	if !strings.HasPrefix(strings.TrimSpace(raw), "{") {
		var err error
		data, err = os.ReadFile(raw)
		if err != nil {
			return gcsURLSigner{}, err
		}
	}
	if json.Unmarshal(data, &config) != nil || config.Email == "" {
		return gcsURLSigner{}, errors.New("missing GCS signing identity")
	}
	block, _ := pem.Decode([]byte(config.PrivateKey))
	if block == nil {
		return gcsURLSigner{}, errors.New("GCS signing key unavailable")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	rsaKey, ok := key.(*rsa.PrivateKey)
	if err != nil || !ok {
		return gcsURLSigner{}, errors.New("GCS signing requires an RSA service account key")
	}
	return gcsURLSigner{Email: config.Email, Key: rsaKey}, nil
}

func (s gcsURLSigner) sign(bucket, object string, now time.Time) (string, error) {
	now = now.UTC()
	pathParts := strings.Split(object, "/")
	for i, part := range pathParts {
		pathParts[i] = url.PathEscape(part)
	}
	path := "/" + url.PathEscape(bucket) + "/" + strings.Join(pathParts, "/")
	scope := now.Format("20060102") + "/auto/storage/goog4_request"
	stamp := now.Format("20060102T150405Z")
	query := url.Values{
		"X-Goog-Algorithm": {"GOOG4-RSA-SHA256"}, "X-Goog-Credential": {s.Email + "/" + scope},
		"X-Goog-Date": {stamp}, "X-Goog-Expires": {"172800"}, "X-Goog-SignedHeaders": {"host"},
	}
	canonical := "GET\n" + path + "\n" + query.Encode() + "\nhost:storage.googleapis.com\n\nhost\nUNSIGNED-PAYLOAD"
	digest := sha256.Sum256([]byte(canonical))
	toSign := "GOOG4-RSA-SHA256\n" + stamp + "\n" + scope + "\n" + hex.EncodeToString(digest[:])
	signHash := sha256.Sum256([]byte(toSign))
	signature, err := rsa.SignPKCS1v15(rand.Reader, s.Key, crypto.SHA256, signHash[:])
	if err != nil {
		return "", err
	}
	query.Set("X-Goog-Signature", hex.EncodeToString(signature))
	return "https://storage.googleapis.com" + path + "?" + query.Encode(), nil
}

type openRouterInputRow struct {
	Request struct {
		Contents []struct {
			Parts []struct {
				Text string `json:"text"`
				File *struct {
					URI string `json:"fileUri"`
				} `json:"fileData"`
			} `json:"parts"`
		} `json:"contents"`
		System struct {
			Parts []struct{ Text string } `json:"parts"`
		} `json:"systemInstruction"`
		Config struct {
			Max  int    `json:"maxOutputTokens"`
			MIME string `json:"responseMimeType"`
		} `json:"generationConfig"`
	} `json:"request"`
}

func prepareOpenRouterBatch(ctx context.Context, provider ProviderConfig, command batch.Command) *CallError {
	in, err := os.Open(command.Input)
	if err != nil {
		return vertexBatchError(500, "batch_input_unavailable")
	}
	defer in.Close()
	out, err := os.OpenFile(command.File, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return vertexBatchError(500, "batch_spool_unavailable")
	}
	defer out.Close()
	writer := bufio.NewWriter(out)
	header := struct {
		Endpoint string         `json:"endpoint"`
		Model    string         `json:"model"`
		Provider map[string]any `json:"provider,omitempty"`
	}{Endpoint: "/v1/chat/completions", Model: command.Model}
	if len(command.ImageProviders) != 0 {
		header.Provider = map[string]any{"only": command.ImageProviders, "allow_fallbacks": true}
	}
	rawHeader, _ := json.Marshal(header)
	if _, err := writer.Write(append(rawHeader[:len(rawHeader)-1], []byte(`,"requests":[`)...)); err != nil {
		return vertexBatchError(500, "batch_spool_write_failed")
	}
	encoder := json.NewEncoder(writer)
	var signer *gcsURLSigner
	scan := bufio.NewScanner(in)
	scan.Buffer(make([]byte, 64<<10), 16<<20)
	line := 0
	for scan.Scan() {
		if ctx.Err() != nil {
			return vertexBatchError(499, "batch_preparation_cancelled")
		}
		var row openRouterInputRow
		if json.Unmarshal(scan.Bytes(), &row) != nil || len(row.Request.Contents) != 1 ||
			len(row.Request.System.Parts) != 1 {
			return vertexBatchError(400, "invalid_batch_requests")
		}
		var parts []map[string]any
		for _, part := range row.Request.Contents[0].Parts {
			if part.File == nil {
				parts = append(parts, map[string]any{"type": "text", "text": part.Text})
				continue
			}
			prefix := "gs://" + command.Bucket + "/" + command.Prefix + "/"
			object, managed := strings.CutPrefix(part.File.URI, prefix)
			imageURL := part.File.URI
			if managed {
				if provider.BaseProvider != "vertex" || len(command.ImageProviders) == 0 {
					return vertexBatchError(400, "batch_private_images_unavailable")
				}
				if signer == nil {
					s, err := newGCSURLSigner(provider.VertexAuthCredentials)
					if err != nil {
						return vertexBatchError(502, "batch_signer_unavailable")
					}
					signer = &s
				}
				imageURL, err = signer.sign(command.Bucket, command.Prefix+"/"+object, time.Now())
				if err != nil {
					return vertexBatchError(502, "batch_image_signing_failed")
				}
			}
			parts = append(parts, map[string]any{"type": "image_url", "image_url": map[string]string{"url": imageURL}})
		}
		if line > 0 {
			if _, err := writer.WriteString(","); err != nil {
				return vertexBatchError(500, "batch_spool_write_failed")
			}
		}
		line++
		body := map[string]any{
			"messages": []map[string]any{
				{"role": "system", "content": row.Request.System.Parts[0].Text},
				{"role": "user", "content": parts},
			},
			"max_tokens": row.Request.Config.Max,
		}
		if row.Request.Config.MIME == "application/json" {
			body["response_format"] = map[string]string{"type": "json_object"}
		}
		if err := encoder.Encode(map[string]any{
			"custom_id": strconv.Itoa(line),
			"body":      body,
		}); err != nil {
			return vertexBatchError(500, "batch_spool_write_failed")
		}
	}
	if scan.Err() != nil || line == 0 {
		return vertexBatchError(400, "invalid_batch_requests")
	}
	if _, err := writer.WriteString("]}"); err != nil || writer.Flush() != nil {
		return vertexBatchError(500, "batch_spool_write_failed")
	}
	if err := out.Close(); err != nil {
		return vertexBatchError(500, "batch_spool_write_failed")
	}
	return nil
}

var routerBatchID = regexp.MustCompile(`^batch[-_][a-zA-Z0-9_-]+$`)

func runOpenRouterBatch(ctx context.Context, provider ProviderConfig, command batch.Command) (batch.Result, *CallError) {
	if provider.APIKey == "" {
		return batch.Result{}, vertexBatchError(502, "batch_credentials_unavailable")
	}
	client := oauth2.NewClient(ctx, oauth2.StaticTokenSource(&oauth2.Token{AccessToken: provider.APIKey}))
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c := vertexBatchClient{client: client}
	switch command.Operation {
	case batch.Submit:
		file, err := os.Open(command.File)
		if err != nil {
			return batch.Result{}, vertexBatchError(500, "batch_input_unavailable")
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return batch.Result{}, vertexBatchError(500, "batch_input_unavailable")
		}
		resp, callErr := c.request(ctx, http.MethodPost, openRouterBatchURL, "application/json", file, info.Size())
		if callErr != nil {
			return batch.Result{}, callErr
		}
		defer resp.Body.Close()
		result, _, err := readOpenRouterBatch(resp.Body, nil)
		if err != nil || !routerBatchID.MatchString(result.Job) {
			return batch.Result{}, vertexBatchError(502, "batch_job_name_missing")
		}
		return result, nil
	case batch.Status, batch.Download:
		if !routerBatchID.MatchString(command.Job) {
			return batch.Result{}, vertexBatchError(400, "invalid_batch_job")
		}
		resp, callErr := c.request(ctx, http.MethodGet, openRouterBatchURL+"/"+command.Job, "", nil, 0)
		if callErr != nil {
			return batch.Result{}, callErr
		}
		defer resp.Body.Close()
		if command.Operation == batch.Status {
			result, _, err := readOpenRouterBatch(resp.Body, nil)
			if err != nil {
				return batch.Result{}, vertexBatchError(502, "invalid_batch_upstream_response")
			}
			return result, nil
		}
		file, err := os.OpenFile(command.File, os.O_WRONLY|os.O_TRUNC, 0)
		if err != nil {
			return batch.Result{}, vertexBatchError(500, "batch_spool_unavailable")
		}
		result, rows, err := readOpenRouterBatch(resp.Body, file)
		err = errors.Join(err, file.Close())
		if err != nil {
			return batch.Result{}, vertexBatchError(502, "batch_result_download_failed")
		}
		switch result.State {
		case "JOB_STATE_SUCCEEDED", "JOB_STATE_FAILED", "JOB_STATE_CANCELLED", "JOB_STATE_EXPIRED":
		default:
			return batch.Result{}, vertexBatchError(409, "batch_results_pending")
		}
		if rows == 0 {
			return batch.Result{}, vertexBatchError(404, "batch_results_unavailable")
		}
		return result, nil
	default:
		return batch.Result{}, vertexBatchError(400, "invalid_batch_operation")
	}
}

func openRouterJobState(status string) string {
	switch status {
	case "completed":
		return "JOB_STATE_SUCCEEDED"
	case "failed":
		return "JOB_STATE_FAILED"
	case "expired":
		return "JOB_STATE_EXPIRED"
	case "cancelled":
		return "JOB_STATE_CANCELLED"
	case "in_progress", "finalizing", "cancelling":
		return "JOB_STATE_RUNNING"
	default:
		return "JOB_STATE_PENDING"
	}
}

func readOpenRouterBatch(in io.Reader, dst io.Writer) (out batch.Result, rows int, err error) {
	d := json.NewDecoder(io.LimitReader(in, 2_000_000_001))
	if token, err := d.Token(); err != nil || token != json.Delim('{') {
		return out, 0, errors.New("invalid OpenRouter batch response")
	}
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return out, rows, err
		}
		switch key {
		case "id":
			err = d.Decode(&out.Job)
		case "status":
			var state string
			err = d.Decode(&state)
			out.State = openRouterJobState(state)
		case "request_counts":
			var counts struct{ Completed, Failed int }
			err = d.Decode(&counts)
			out.Successful, out.Failed = strconv.Itoa(counts.Completed), strconv.Itoa(counts.Failed)
		case "results":
			var token json.Token
			token, err = d.Token()
			if err == nil && token == json.Delim('[') {
				for d.More() {
					var row json.RawMessage
					if err := d.Decode(&row); err != nil {
						return out, rows, err
					}
					if dst != nil {
						var compact bytes.Buffer
						if err := json.Compact(&compact, row); err != nil {
							return out, rows, err
						}
						if _, err := dst.Write(append(compact.Bytes(), '\n')); err != nil {
							return out, rows, err
						}
					}
					rows++
				}
				_, err = d.Token()
			} else if err == nil && token != nil {
				err = errors.New("invalid OpenRouter batch results")
			}
		default:
			err = d.Decode(new(json.RawMessage))
		}
		if err != nil {
			return out, rows, err
		}
	}
	if _, err := d.Token(); err != nil {
		return out, rows, err
	}
	if out.Job == "" || out.State == "" {
		return out, rows, fmt.Errorf("OpenRouter batch response lacks job state")
	}
	return out, rows, nil
}

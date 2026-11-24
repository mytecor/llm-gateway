package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	batchproto "github.com/mytecor/llm-gateway/driverproto/batch"
	"github.com/mytecor/llm-gateway/internal/config"
	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/telemetry"
)

var (
	batchIDPattern    = regexp.MustCompile(`^[a-f0-9]{32}$`)
	batchPhotoPattern = regexp.MustCompile(`^photos/[a-f0-9]{64}\.(jpg|png)$`)
)

const batchInputFile = "requests.jsonl"

type batchServer struct {
	settings  config.BatchConfig
	executor  model.Executor
	transfers chan struct{}
	large     chan struct{}
}

type batchRecord struct {
	Bucket          string            `json:"bucket"`
	Prefix          string            `json:"prefix"`
	Model           string            `json:"model"`
	Native          string            `json:"native"`
	Provider        string            `json:"provider"`
	Backend         string            `json:"backend"`
	StorageProvider string            `json:"storage_provider,omitempty"`
	ImageProviders  []string          `json:"image_providers,omitempty"`
	ReasoningEffort string            `json:"reasoning_effort,omitempty"`
	Job             string            `json:"job,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

// EnableBatch mounts the batch API only when the operator supplies its scope.
func (s *Server) EnableBatch(executor model.Executor) error {
	if s.config.Raw.Batch == nil {
		return nil
	}
	b := &batchServer{settings: *s.config.Raw.Batch, executor: executor, transfers: make(chan struct{}, 16), large: make(chan struct{}, 2)}
	if err := os.MkdirAll(b.settings.StateDir, 0o700); err != nil {
		return err
	}
	state, err := os.Stat(b.settings.StateDir)
	if err != nil {
		return err
	}
	if !state.IsDir() || state.Mode().Perm()&0o077 != 0 {
		return errors.New("batch state directory must be private")
	}
	mount := func(pattern string, next http.HandlerFunc) {
		s.mux.HandleFunc(pattern, s.auth(func(w http.ResponseWriter, r *http.Request) {
			if telemetry.ClientKeyFrom(r.Context()) != b.settings.Client {
				writeAPIError(w, http.StatusForbidden, "batch_client_forbidden")
				return
			}
			if id := r.PathValue("id"); id != "" && !batchIDPattern.MatchString(id) {
				writeAPIError(w, http.StatusBadRequest, "invalid_batch_id")
				return
			}
			next(w, r)
		}))
	}
	mount("GET /v1/batches/{id}/input", b.input)
	mount("PUT /v1/batches/{id}/files/{file...}", b.upload)
	mount("POST /v1/batches", b.submit)
	mount("GET /v1/batches/{id}", b.status)
	mount("GET /v1/batches/{id}/results", b.results)
	mount("POST /v1/files", b.openAIUpload)
	mount("GET /v1/files/{file_id}", b.openAIFileInfo)
	mount("GET /v1/files/{file_id}/content", b.openAIContent)
	return nil
}

func (b *batchServer) newRecord(id, logical string) (batchRecord, bool) {
	if logical == "" {
		logical = b.settings.Model
	}
	route, ok := b.settings.Routes[logical]
	return batchRecord{Bucket: b.settings.Bucket, Prefix: b.settings.Prefix + "/" + b.jobKey(id),
		Model: logical, Native: route.Native, Provider: route.Provider, Backend: route.Backend,
		StorageProvider: b.settings.StorageProvider, ImageProviders: route.ImageProviders,
		ReasoningEffort: route.ReasoningEffort}, ok
}

func (r batchRecord) storagePrefix() string {
	if r.Bucket == "" {
		return "batch://local/" + r.Prefix
	}
	return "gs://" + r.Bucket + "/" + r.Prefix
}

func (b *batchServer) inputPath(id, logical string) string {
	hash := sha256.Sum256([]byte(logical))
	return filepath.Join(b.settings.StateDir, fmt.Sprintf("input-%s-%x.jsonl", b.jobKey(id), hash[:]))
}

func (b *batchServer) recordPath(id string) string {
	return filepath.Join(b.settings.StateDir, b.jobKey(id)+".json")
}

func (b *batchServer) jobKey(id string) string {
	digest := sha256.Sum256([]byte(b.settings.Client + "\x00" + id))
	return fmt.Sprintf("%x", digest[:])
}

func (b *batchServer) readRecord(id string) (batchRecord, error) {
	var record batchRecord
	body, err := os.ReadFile(b.recordPath(id))
	if err != nil {
		return record, err
	}
	err = json.Unmarshal(body, &record)
	return record, err
}

func (b *batchServer) saveRecord(id string, record batchRecord, reserve bool) error {
	file, err := os.CreateTemp(b.settings.StateDir, ".job-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, err = io.Copy(file, bytes.NewReader(marshalBatch(record)))
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err = errors.Join(err, closeErr); err != nil {
		return err
	}
	if reserve {
		err = os.Link(file.Name(), b.recordPath(id))
	} else {
		err = os.Rename(file.Name(), b.recordPath(id))
	}
	if err != nil {
		return err
	}
	dir, err := os.Open(b.settings.StateDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func marshalBatch(value any) []byte {
	body, _ := json.Marshal(value)
	return body
}

func (b *batchServer) call(ctx context.Context, record batchRecord, operation batchproto.Command) (batchproto.Result, *model.CallError) {
	operation.Bucket, operation.Prefix = record.Bucket, record.Prefix
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	data, err := b.executor.Do(ctx, model.Target{Provider: record.Provider, Model: record.Native}, model.ExecuteRequest{
		Kind: model.RequestBatch, Body: marshalBatch(operation), SingleAttempt: true, AttemptTimeout: 10 * time.Minute,
	})
	var result batchproto.Result
	if err == nil && json.Unmarshal(data, &result) != nil {
		err = &model.CallError{Status: 502, Code: "invalid_batch_driver_response"}
	}
	return result, err
}

func batchCallError(w http.ResponseWriter, err *model.CallError) {
	status := err.Status
	if status < 400 || status > 599 {
		status = 502
	}
	code := err.Code
	if code == "" {
		code = "batch_operation_failed"
	}
	writeAPIError(w, status, code)
}

func (b *batchServer) input(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	record, err := b.readRecord(id)
	if errors.Is(err, os.ErrNotExist) {
		var ok bool
		record, ok = b.newRecord(id, r.URL.Query().Get("model"))
		if !ok {
			writeAPIError(w, 400, "unknown_batch_model")
			return
		}
	} else if err != nil {
		writeAPIError(w, 500, "batch_state_unavailable")
		return
	}
	writeJSON(w, 200, map[string]any{
		"id": id, "model": record.Model, "native_model": record.Native, "storage_prefix": record.storagePrefix(),
		"backend": record.Backend, "accepts_private_images": record.Bucket != "" &&
			(record.Backend == "vertex" || len(record.ImageProviders) != 0),
	})
}

func acquireBatchTransfer(w http.ResponseWriter, ctx context.Context, slots chan struct{}) bool {
	select {
	case slots <- struct{}{}:
		return true
	case <-ctx.Done():
		writeAPIError(w, 408, "batch_transfer_cancelled")
		return false
	}
}

func (b *batchServer) upload(w http.ResponseWriter, r *http.Request) {
	id, name := r.PathValue("id"), r.PathValue("file")
	if name != batchInputFile && !batchPhotoPattern.MatchString(name) {
		writeAPIError(w, 400, "invalid_batch_file")
		return
	}
	if _, err := b.readRecord(id); !errors.Is(err, os.ErrNotExist) {
		writeAPIError(w, 409, "batch_input_closed")
		return
	}
	record, ok := b.newRecord(id, r.URL.Query().Get("model"))
	if !ok {
		writeAPIError(w, 400, "unknown_batch_model")
		return
	}
	if native := r.URL.Query().Get("native_model"); native != "" && native != record.Native {
		writeAPIError(w, 409, "batch_native_model_changed")
		return
	}
	if name != batchInputFile && (record.Bucket == "" || (record.Backend == "openrouter" && len(record.ImageProviders) == 0)) {
		writeAPIError(w, 400, "batch_private_images_unavailable")
		return
	}
	slots := b.transfers
	if name == batchInputFile {
		slots = b.large
	}
	if !acquireBatchTransfer(w, r.Context(), slots) {
		return
	}
	defer func() { <-slots }()
	file, err := os.CreateTemp("", "llm-batch-")
	if err != nil {
		writeAPIError(w, 500, "batch_spool_unavailable")
		return
	}
	defer os.Remove(file.Name())
	limit := int64(20 << 20)
	if name == batchInputFile {
		limit = 1_000_000_000
	}
	n, copyErr := io.Copy(file, http.MaxBytesReader(w, r.Body, limit))
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil || n == 0 {
		var tooLarge *http.MaxBytesError
		if errors.As(copyErr, &tooLarge) {
			writeAPIError(w, 413, "batch_file_too_large")
		} else {
			writeAPIError(w, 400, "invalid_batch_file")
		}
		return
	}
	spool := file.Name()
	if name == batchInputFile {
		normalized, err := os.CreateTemp(b.settings.StateDir, ".batch-jsonl-")
		if err != nil {
			writeAPIError(w, 500, "batch_spool_unavailable")
			return
		}
		defer os.Remove(normalized.Name())
		ok := normalizeBatchJSONL(file.Name(), normalized, record)
		closeErr := normalized.Close()
		if !ok || closeErr != nil {
			writeAPIError(w, 400, "invalid_batch_requests")
			return
		}
		spool = normalized.Name()
		if record.Backend == "openrouter" {
			if err := installBatchInput(spool, b.inputPath(id, record.Model)); errors.Is(err, os.ErrExist) {
				writeAPIError(w, 409, "batch_file_already_exists")
				return
			} else if err != nil {
				writeAPIError(w, 500, "batch_state_unavailable")
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	record.Provider = record.StorageProvider
	if _, err := b.call(r.Context(), record, batchproto.Command{Operation: batchproto.Upload, Object: name, File: spool}); err != nil {
		batchCallError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (b *batchServer) submit(w http.ResponseWriter, r *http.Request) {
	var input struct {
		ID       string            `json:"id"`
		Model    string            `json:"model"`
		Native   string            `json:"native_model"`
		FileID   string            `json:"input_file_id"`
		Endpoint string            `json:"endpoint"`
		Window   string            `json:"completion_window"`
		Metadata map[string]string `json:"metadata"`
	}
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	d.DisallowUnknownFields()
	if d.Decode(&input) != nil || d.Decode(new(any)) != io.EOF {
		writeAPIError(w, 400, "invalid_batch_submission")
		return
	}
	if input.FileID != "" {
		id, ok := strings.CutPrefix(input.FileID, "file-")
		if !ok || !batchIDPattern.MatchString(id) || input.ID != "" || input.Model != "" || input.Native != "" ||
			input.Endpoint != "/v1/chat/completions" || input.Window != "24h" {
			writeAPIError(w, 400, "invalid_batch_submission")
			return
		}
		file, err := b.readOpenAIFile(id)
		if err != nil {
			batchReadError(w, err)
			return
		}
		input.ID, input.Model, input.Native = id, file.Model, file.Native
	} else if input.Endpoint != "" || input.Window != "" {
		writeAPIError(w, 400, "invalid_batch_submission")
		return
	}
	if !batchIDPattern.MatchString(input.ID) || input.Model == "" {
		writeAPIError(w, 400, "invalid_batch_submission")
		return
	}
	id := input.ID
	if previous, err := b.readRecord(id); err == nil {
		if previous.Model != input.Model || (input.Native != "" && previous.Native != input.Native) {
			writeAPIError(w, 409, "batch_model_already_selected")
			return
		}
		r.SetPathValue("id", id)
		b.status(w, r)
		return
	} else if !errors.Is(err, os.ErrNotExist) {
		batchReadError(w, err)
		return
	}
	record, ok := b.newRecord(id, input.Model)
	if !ok {
		writeAPIError(w, 400, "unknown_batch_model")
		return
	}
	if input.Native != "" && record.Native != input.Native {
		writeAPIError(w, 409, "batch_native_model_changed")
		return
	}
	record.Metadata = input.Metadata
	submit := batchproto.Command{Operation: batchproto.Submit, Name: "gateway-" + b.jobKey(id)}
	if record.Backend == "openrouter" {
		inputFile := b.inputPath(id, record.Model)
		if _, err := os.Stat(inputFile); err != nil {
			batchReadError(w, err)
			return
		}
		if !acquireBatchTransfer(w, r.Context(), b.large) {
			return
		}
		defer func() { <-b.large }()
		prepared, err := os.CreateTemp("", "llm-batch-openrouter-")
		if err != nil {
			writeAPIError(w, 500, "batch_spool_unavailable")
			return
		}
		prepared.Close()
		defer os.Remove(prepared.Name())
		prepare := record
		if record.StorageProvider != "" {
			prepare.Provider = record.StorageProvider
		}
		_, callErr := b.call(r.Context(), prepare, batchproto.Command{Operation: batchproto.Prepare, Input: inputFile,
			File: prepared.Name(), Model: record.Native, ImageProviders: record.ImageProviders,
			ReasoningEffort: record.ReasoningEffort})
		if callErr != nil {
			batchCallError(w, callErr)
			return
		}
		submit.File = prepared.Name()
	}
	if err := b.saveRecord(id, record, true); errors.Is(err, os.ErrExist) {
		if previous, readErr := b.readRecord(id); readErr != nil || previous.Model != input.Model ||
			(input.Native != "" && previous.Native != input.Native) {
			writeAPIError(w, 409, "batch_model_already_selected")
			return
		}
		r.SetPathValue("id", id)
		b.status(w, r)
		return
	} else if err != nil {
		writeAPIError(w, 500, "batch_state_unavailable")
		return
	}
	result, callErr := b.call(r.Context(), record, submit)
	if callErr != nil {
		// A definitive Vertex rejection permits retry. An interrupted or ambiguous
		// submission is reconciled by name, without another paid create call.
		if callErr.Code == "batch_upstream_rejected" && (callErr.Status == 400 || callErr.Status == 401 ||
			callErr.Status == 402 || callErr.Status == 403 || callErr.Status == 404 || callErr.Status == 429) {
			if err := os.Remove(b.recordPath(id)); err != nil {
				writeAPIError(w, 500, "batch_state_unavailable")
				return
			}
		}
		batchCallError(w, callErr)
		return
	}
	record.Job = result.Job
	if err := b.saveRecord(id, record, false); err != nil {
		writeAPIError(w, 500, "batch_state_unavailable")
		return
	}
	if record.Backend == "openrouter" {
		_ = os.Remove(b.inputPath(id, record.Model))
	}
	b.writeBatchStatus(w, http.StatusAccepted, id, record, result)
}

func (b *batchServer) writeBatchStatus(w http.ResponseWriter, status int, id string, record batchRecord, result batchproto.Result) {
	if file, err := b.readOpenAIFile(id); err == nil {
		reply := openAIBatchStatus(id, file, result)
		if len(record.Metadata) != 0 {
			reply["metadata"] = record.Metadata
		}
		writeJSON(w, status, reply)
		return
	}
	state := result.State
	if state == "" {
		state = "SUBMITTING"
	}
	reply := map[string]string{
		"id": id, "model": record.Model, "status": state, "successful": result.Successful, "failed": result.Failed,
	}
	if result.Provider != "" {
		reply["provider"] = result.Provider
	}
	if result.Error != "" {
		reply["error"] = result.Error
	}
	writeJSON(w, status, reply)
}

func (b *batchServer) status(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	record, err := b.readRecord(id)
	if err != nil {
		batchReadError(w, err)
		return
	}
	command := batchproto.Command{Operation: batchproto.Status, Job: record.Job}
	if record.Job == "" {
		if record.Backend == "openrouter" {
			b.writeBatchStatus(w, http.StatusOK, id, record, batchproto.Result{})
			return
		}
		command = batchproto.Command{Operation: batchproto.Find, Name: "gateway-" + b.jobKey(id)}
	}
	result, callErr := b.call(r.Context(), record, command)
	if callErr != nil {
		batchCallError(w, callErr)
		return
	}
	if record.Job == "" && result.Job != "" {
		record.Job = result.Job
		if err = b.saveRecord(id, record, false); err != nil {
			writeAPIError(w, 500, "batch_state_unavailable")
			return
		}
	}
	b.writeBatchStatus(w, http.StatusOK, id, record, result)
}

func batchReadError(w http.ResponseWriter, err error) {
	if errors.Is(err, os.ErrNotExist) {
		writeAPIError(w, 404, "batch_not_found")
	} else {
		writeAPIError(w, 500, "batch_state_unavailable")
	}
}

func (b *batchServer) results(w http.ResponseWriter, r *http.Request) {
	b.serveResults(w, r, r.PathValue("id"), nil)
}

func (b *batchServer) serveResults(w http.ResponseWriter, r *http.Request, id string, openAI *openAIInputFile) {
	record, err := b.readRecord(id)
	if err != nil {
		batchReadError(w, err)
		return
	}
	if record.Job == "" {
		writeAPIError(w, 409, "batch_results_pending")
		return
	}
	if !acquireBatchTransfer(w, r.Context(), b.large) {
		return
	}
	defer func() { <-b.large }()
	file, err := os.CreateTemp("", "llm-batch-output-")
	if err != nil {
		writeAPIError(w, 500, "batch_spool_unavailable")
		return
	}
	defer file.Close()
	defer os.Remove(file.Name())
	if _, err := b.call(r.Context(), record, batchproto.Command{Operation: batchproto.Download, Job: record.Job, File: file.Name()}); err != nil {
		batchCallError(w, err)
		return
	}
	if openAI != nil {
		converted, err := os.CreateTemp("", "llm-batch-openai-")
		if err != nil {
			writeAPIError(w, 500, "batch_spool_unavailable")
			return
		}
		defer os.Remove(converted.Name())
		defer converted.Close()
		if adaptOpenAIResults(file, converted, *openAI, record.Backend) != nil {
			writeAPIError(w, 502, "invalid_batch_output")
			return
		}
		file = converted
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	http.ServeContent(w, r, "predictions.jsonl", time.Time{}, file)
}

type vertexBatchPart struct {
	Text string `json:"text,omitempty"`
	File *struct {
		URI  string `json:"fileUri"`
		MIME string `json:"mimeType"`
	} `json:"fileData,omitempty"`
}

type vertexBatchContent struct {
	Role  string            `json:"role,omitempty"`
	Parts []vertexBatchPart `json:"parts"`
}

func normalizeBatchJSONL(path string, dst io.Writer, record batchRecord) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), maxRequestBody)
	count := 0
	written := 0
	for scanner.Scan() {
		row := normalizeBatchRow(scanner.Bytes(), record)
		written += len(row) + 1
		if count == 200_000 || row == nil || written > 1_000_000_000 {
			return false
		}
		if _, err := dst.Write(append(row, '\n')); err != nil {
			return false
		}
		count++
	}
	return count > 0 && scanner.Err() == nil
}

func normalizeBatchRow(raw []byte, record batchRecord) []byte {
	var row struct {
		Request struct {
			Contents []vertexBatchContent `json:"contents"`
			System   *vertexBatchContent  `json:"systemInstruction"`
			Config   struct {
				MIME      string `json:"responseMimeType"`
				MaxTokens int    `json:"maxOutputTokens"`
				Thinking  struct {
					Level string `json:"thinkingLevel"`
				} `json:"thinkingConfig"`
			} `json:"generationConfig"`
		} `json:"request"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&row) != nil || decoder.Decode(new(any)) != io.EOF {
		return nil
	}
	r := row.Request
	if len(r.Contents) != 1 || r.Contents[0].Role != "user" || len(r.Contents[0].Parts) < 1 || len(r.Contents[0].Parts) > 2 ||
		r.System == nil || len(r.System.Parts) != 1 || r.System.Parts[0].File != nil || r.System.Parts[0].Text == "" ||
		(r.Config.MIME != "application/json" && r.Config.MIME != "text/plain") || r.Config.MaxTokens < 1 || r.Config.MaxTokens > 8192 {
		return nil
	}
	if level := r.Config.Thinking.Level; level != "LOW" && level != "MEDIUM" && level != "HIGH" {
		return nil
	}
	parts := r.Contents[0].Parts
	if parts[0].File != nil || parts[0].Text == "" {
		return nil
	}
	if len(parts) == 1 {
		return marshalBatch(row)
	}
	if parts[1].Text != "" || parts[1].File == nil {
		return nil
	}
	file := parts[1].File
	if record.Backend == "openrouter" && publicBatchHTTPS(file.URI) {
		return marshalBatch(row)
	}
	name, ok := strings.CutPrefix(file.URI, record.storagePrefix()+"/")
	if record.Bucket == "" || !ok || !batchPhotoPattern.MatchString(name) ||
		(record.Backend == "openrouter" && len(record.ImageProviders) == 0) {
		return nil
	}
	if (strings.HasSuffix(name, ".png") && file.MIME != "image/png") ||
		(strings.HasSuffix(name, ".jpg") && file.MIME != "image/jpeg") {
		return nil
	}
	return marshalBatch(row)
}

func publicBatchHTTPS(raw string) bool {
	parsed, err := url.Parse(raw)
	return err == nil && parsed.Scheme == "https" && parsed.User == nil && parsed.Hostname() != "" &&
		parsed.Fragment == "" && (parsed.Port() == "" || parsed.Port() == "443")
}

func installBatchInput(src, dst string) error {
	err := os.Link(src, dst)
	if !errors.Is(err, os.ErrExist) {
		return err
	}
	digest := func(name string) ([]byte, error) {
		file, err := os.Open(name)
		if err != nil {
			return nil, err
		}
		defer file.Close()
		h := sha256.New()
		_, err = io.Copy(h, file)
		return h.Sum(nil), err
	}
	left, leftErr := digest(src)
	right, rightErr := digest(dst)
	if err := errors.Join(leftErr, rightErr); err != nil {
		return err
	}
	if !bytes.Equal(left, right) {
		return os.ErrExist
	}
	return nil
}

package server

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	batchproto "github.com/mytecor/llm-gateway/driverproto/batch"
	"github.com/mytecor/llm-gateway/internal/structured"
)

type openAIInputFile struct {
	Schemas map[string]string `json:"schemas,omitempty"`
	Model   string            `json:"model"`
	Native  string            `json:"native_model"`
	Name    string            `json:"name"`
	Bytes   int64             `json:"bytes"`
	At      int64             `json:"created_at"`
	IDs     []string          `json:"ids"`
}

func (b *batchServer) openAIFilePath(id string) string {
	return filepath.Join(b.settings.StateDir, "openai-"+b.jobKey(id)+".json")
}

func (b *batchServer) readOpenAIFile(id string) (meta openAIInputFile, err error) {
	data, err := os.ReadFile(b.openAIFilePath(id))
	if err == nil {
		err = json.Unmarshal(data, &meta)
	}
	return meta, err
}

func openAIFileResponse(fileID string, meta openAIInputFile) map[string]any {
	return map[string]any{
		"id": fileID, "object": "file", "purpose": "batch", "filename": meta.Name,
		"bytes": meta.Bytes, "created_at": meta.At, "status": "processed",
	}
}

func (b *batchServer) openAIUpload(w http.ResponseWriter, r *http.Request) {
	if !acquireBatchTransfer(w, r.Context(), b.large) {
		return
	}
	defer func() { <-b.large }()
	r.Body = http.MaxBytesReader(w, r.Body, 1_001_000_000)
	if err := r.ParseMultipartForm(1 << 20); err != nil {
		writeAPIError(w, 400, "invalid_batch_file")
		return
	}
	defer r.MultipartForm.RemoveAll()
	input, info, err := r.FormFile("file")
	if err != nil {
		writeAPIError(w, 400, "invalid_batch_file")
		return
	}
	defer input.Close()
	if r.FormValue("purpose") != "batch" || info.Size > 1_000_000_000 {
		writeAPIError(w, 400, "invalid_batch_file")
		return
	}
	id, err := newBatchID()
	if err != nil {
		writeAPIError(w, 500, "batch_state_unavailable")
		return
	}
	var workspace batchRecord
	if workspaceID := r.FormValue("workspace_id"); workspaceID != "" {
		if !batchIDPattern.MatchString(workspaceID) {
			writeAPIError(w, 400, "invalid_batch_id")
			return
		}
		id = workspaceID
		workspace, err = b.workspace(workspaceID)
		if err != nil {
			batchReadError(w, err)
			return
		}
	}
	file, err := os.CreateTemp(b.settings.StateDir, ".openai-input-")
	if err != nil {
		writeAPIError(w, 500, "batch_spool_unavailable")
		return
	}
	defer os.Remove(file.Name())
	metadata := openAIInputFile{Name: filepath.Base(info.Filename), At: time.Now().Unix(), Bytes: info.Size}
	digest := sha256.New()
	record, err := b.convertOpenAIInput(id, io.TeeReader(input, digest), file, &metadata, workspace)
	err = errors.Join(err, file.Close())
	if err != nil {
		writeAPIError(w, 400, "invalid_batch_requests")
		return
	}
	if workspace.Model != "" && !b.claimWorkspaceInput(w, id, digest.Sum(nil)) {
		return
	}
	if record.Backend == "openrouter" {
		if err := installBatchInput(file.Name(), b.inputPath(id, record.Model)); err != nil {
			writeAPIError(w, 500, "batch_state_unavailable")
			return
		}
	} else if _, err := b.call(r.Context(), record, batchproto.Command{
		Operation: batchproto.Upload, Object: batchInputFile, File: file.Name(),
	}); err != nil {
		batchCallError(w, err)
		return
	}
	if err := writeImmutable(b.openAIFilePath(id), marshalBatch(metadata)); err != nil && !errors.Is(err, os.ErrExist) {
		writeAPIError(w, 500, "batch_state_unavailable")
		return
	}
	writeJSON(w, 200, openAIFileResponse("file-"+id, metadata))
}

type openAIChatBatchRow struct {
	ID     string `json:"custom_id"`
	Method string `json:"method"`
	URL    string `json:"url"`
	Body   struct {
		Model    string `json:"model"`
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
		MaxTokens     int      `json:"max_tokens"`
		Temperature   *float64 `json:"temperature,omitempty"`
		MaxCompletion int      `json:"max_completion_tokens"`
		Reasoning     string   `json:"reasoning_effort"`
		Format        *struct {
			Type       string `json:"type"`
			JSONSchema *struct {
				Name        string          `json:"name"`
				Description string          `json:"description,omitempty"`
				Schema      json.RawMessage `json:"schema"`
				Strict      bool            `json:"strict,omitempty"`
			} `json:"json_schema,omitempty"`
		} `json:"response_format"`
	} `json:"body"`
}

func (b *batchServer) convertOpenAIInput(id string, input io.Reader, output io.Writer, file *openAIInputFile, workspace batchRecord) (batchRecord, error) {
	var record batchRecord
	buffered := bufio.NewWriter(output)
	lastSchema := ""
	seen := make(map[string]bool)
	scan := bufio.NewScanner(input)
	scan.Buffer(make([]byte, 64<<10), maxRequestBody)
	for scan.Scan() {
		var row openAIChatBatchRow
		parser := json.NewDecoder(bytes.NewReader(scan.Bytes()))
		parser.DisallowUnknownFields()
		if parser.Decode(&row) != nil || parser.Decode(new(any)) != io.EOF ||
			row.ID == "" || len(row.ID) > 256 || seen[row.ID] || row.Method != "POST" ||
			row.URL != "/v1/chat/completions" || row.Body.Model == "" || len(file.IDs) == 200_000 {
			return record, errors.New("invalid chat batch row")
		}
		if file.Model == "" {
			file.Model = row.Body.Model
			var ok bool
			record, ok = b.newRecord(id, file.Model)
			if !ok {
				return record, errors.New("unknown batch model")
			}
			file.Native = record.Native
			if workspace.Model != "" {
				if workspace.Model != record.Model || workspace.Bucket != record.Bucket || workspace.StorageProvider != record.StorageProvider || workspace.Prefix != record.Prefix || workspace.Native != record.Native || workspace.Backend != record.Backend {
					return record, errors.New("batch workspace model or storage changed")
				}
			}
		}
		if row.Body.Model != file.Model {
			return record, errors.New("batch contains more than one model")
		}
		native, err := row.native(len(file.IDs)+1, workspace)
		if err != nil {
			return record, err
		}
		normalized := normalizeBatchRow(native, record)
		if normalized == nil {
			return record, errors.New("unsupported chat batch row")
		}
		if f := row.Body.Format; f != nil && f.Type == "json_schema" {
			raw := f.JSONSchema.Schema
			hash := fmt.Sprintf("%x", sha256.Sum256(raw))
			if hash != lastSchema {
				if _, err := structured.Compile(raw); err != nil {
					return record, errors.New("invalid batch schema")
				}
				if err := b.storeSchema(hash, raw); err != nil {
					return record, err
				}
				lastSchema = hash
			}
			if file.Schemas == nil {
				file.Schemas = make(map[string]string)
			}
			file.Schemas[row.ID] = hash
			if record.Backend == "openrouter" {
				var envelope map[string]json.RawMessage
				if err := json.Unmarshal(normalized, &envelope); err != nil {
					return record, err
				}
				envelope["response_format"] = marshalBatch(f)
				normalized = marshalBatch(envelope)
			}
		}
		if _, err := buffered.Write(append(normalized, '\n')); err != nil {
			return record, err
		}
		seen[row.ID] = true
		file.IDs = append(file.IDs, row.ID)
	}
	if len(file.IDs) == 0 || scan.Err() != nil {
		return record, errors.New("empty or invalid chat batch file")
	}
	return record, buffered.Flush()
}

const openAIItemMarker = "[Batch item "

func (r openAIChatBatchRow) native(line int, workspace batchRecord) ([]byte, error) {
	system := "Answer the user's request."
	var parts []map[string]any
	for _, msg := range r.Body.Messages {
		var text string
		switch msg.Role {
		case "system", "developer":
			if json.Unmarshal(msg.Content, &text) != nil {
				return nil, errors.New("invalid batch system message")
			}
			system = text
		case "user":
			if parts != nil {
				return nil, errors.New("only one user message is supported")
			}
			if json.Unmarshal(msg.Content, &text) == nil {
				parts = []map[string]any{{"text": text}}
				continue
			}
			var content []struct {
				Type  string `json:"type"`
				Text  string `json:"text"`
				Image *struct {
					URL string `json:"url"`
				} `json:"image_url"`
			}
			if json.Unmarshal(msg.Content, &content) != nil {
				return nil, errors.New("invalid batch content")
			}
			for _, part := range content {
				switch {
				case part.Type == "text":
					parts = append(parts, map[string]any{"text": part.Text})
				case part.Type == "image_url" && part.Image != nil:
					if name, managed := strings.CutPrefix(part.Image.URL, "batch-file://"); managed {
						if !batchPhotoPattern.MatchString(name) || workspace.Bucket == "" {
							return nil, errors.New("invalid workspace photo")
						}
						part.Image.URL = workspace.storagePrefix() + "/" + name
					}
					mime := "image/jpeg"
					if strings.HasSuffix(strings.SplitN(part.Image.URL, "?", 2)[0], ".png") {
						mime = "image/png"
					}
					parts = append(parts, map[string]any{"fileData": map[string]string{"fileUri": part.Image.URL, "mimeType": mime}})
				default:
					return nil, errors.New("unsupported chat batch content")
				}
			}
		default:
			return nil, errors.New("unsupported chat batch role")
		}
	}
	if len(parts) == 0 {
		return nil, errors.New("missing chat batch content")
	}
	first, ok := parts[0]["text"].(string)
	if !ok {
		return nil, errors.New("chat batch starts with a text part")
	}
	parts[0]["text"] = fmt.Sprintf("%s%d]\n%s", openAIItemMarker, line, first)
	maxTokens := r.Body.MaxCompletion
	if maxTokens == 0 {
		maxTokens = r.Body.MaxTokens
	}
	if maxTokens == 0 {
		maxTokens = 8192
	}
	level := strings.ToUpper(r.Body.Reasoning)
	if level == "" {
		level = "LOW"
	}
	mime := "text/plain"
	if r.Body.Format != nil {
		switch r.Body.Format.Type {
		case "json_schema":
			if r.Body.Format.JSONSchema == nil || r.Body.Format.JSONSchema.Name == "" || len(r.Body.Format.JSONSchema.Schema) == 0 {
				return nil, errors.New("missing batch JSON schema")
			}
			mime = "application/json"
		case "json_object":
			mime = "application/json"
		case "text":
		default:
			return nil, errors.New("unsupported chat batch response format")
		}
	}
	generation := map[string]any{"maxOutputTokens": maxTokens, "responseMimeType": mime, "thinkingConfig": map[string]string{"thinkingLevel": level}}
	if r.Body.Temperature != nil {
		generation["temperature"] = *r.Body.Temperature
	}
	if f := r.Body.Format; f != nil && f.Type == "json_schema" {
		generation["responseJsonSchema"] = f.JSONSchema.Schema
	}
	return marshalBatch(map[string]any{"request": map[string]any{
		"contents":          []any{map[string]any{"role": "user", "parts": parts}},
		"systemInstruction": map[string]any{"parts": []any{map[string]string{"text": system}}},
		"generationConfig":  generation,
	}}), nil
}

func (b *batchServer) openAIFileInfo(w http.ResponseWriter, r *http.Request) {
	fileID := r.PathValue("file_id")
	id, ok := strings.CutPrefix(fileID, "file-")
	if !ok || !batchIDPattern.MatchString(id) {
		writeAPIError(w, 404, "batch_file_not_found")
		return
	}
	file, err := b.readOpenAIFile(id)
	if err != nil {
		batchReadError(w, err)
		return
	}
	writeJSON(w, 200, openAIFileResponse(fileID, file))
}

func (b *batchServer) openAIContent(w http.ResponseWriter, r *http.Request) {
	id, ok := strings.CutPrefix(r.PathValue("file_id"), "file-out-")
	if !ok || !batchIDPattern.MatchString(id) {
		writeAPIError(w, 404, "batch_file_not_found")
		return
	}
	file, err := b.readOpenAIFile(id)
	if err != nil {
		batchReadError(w, err)
		return
	}
	b.serveResults(w, r, id, &file)
}

func openAIBatchStatus(id string, file openAIInputFile, result batchproto.Result) map[string]any {
	status := "validating"
	switch result.State {
	case "JOB_STATE_RUNNING", "JOB_STATE_UPDATING":
		status = "in_progress"
	case "JOB_STATE_SUCCEEDED", "JOB_STATE_PARTIALLY_SUCCEEDED":
		status = "completed"
	case "JOB_STATE_FAILED":
		status = "failed"
	case "JOB_STATE_CANCELLED":
		status = "cancelled"
	case "JOB_STATE_EXPIRED":
		status = "expired"
	case "JOB_STATE_CANCELLING":
		status = "cancelling"
	}
	completed, _ := strconv.Atoi(result.Successful)
	failed, _ := strconv.Atoi(result.Failed)
	var output any
	if status == "completed" || ((status == "failed" || status == "expired" || status == "cancelled") && completed > 0) {
		output = "file-out-" + id
	}
	return map[string]any{
		"id": id, "object": "batch", "endpoint": "/v1/chat/completions", "status": status,
		"completion_window": "24h", "input_file_id": "file-" + id, "output_file_id": output,
		"created_at": file.At, "request_counts": map[string]int{"total": len(file.IDs), "completed": completed, "failed": failed},
	}
}

func (b *batchServer) adaptOpenAIResults(input io.Reader, output io.Writer, file openAIInputFile, backend string) error {
	scan := bufio.NewScanner(input)
	scan.Buffer(make([]byte, 64<<10), 32<<20)
	writer := json.NewEncoder(output)
	var lastHash string
	var contract *structured.Contract
	emit := func(id string, status int, requestID string, body map[string]any, failed bool) error {
		result := openAIResult(id, status, requestID, body, failed)
		if hash := file.Schemas[id]; hash != "" && result["error"] == nil {
			if hash != lastHash {
				raw, err := os.ReadFile(b.schemaPath(hash))
				if err != nil {
					return err
				}
				compiled, failure := structured.Compile(raw)
				if failure != nil {
					return errors.New("invalid stored schema")
				}
				contract, lastHash = compiled, hash
			}
			var completion struct {
				Choices []struct{ Message struct{ Content string } }
			}
			if json.Unmarshal(marshalBatch(body), &completion) != nil || len(completion.Choices) != 1 || contract.Validate(completion.Choices[0].Message.Content) != nil {
				result["response"] = nil
				result["error"] = map[string]string{"code": "invalid_json_schema_output", "message": "Completion does not satisfy the requested JSON schema."}
			}
		}
		if err := writer.Encode(result); err != nil {
			return err
		}
		if f, ok := output.(http.Flusher); ok {
			f.Flush()
		}
		return nil
	}
	for scan.Scan() {
		if len(bytes.TrimSpace(scan.Bytes())) == 0 {
			continue
		}
		if backend == "openrouter" {
			var item struct {
				ID       string          `json:"custom_id"`
				Error    json.RawMessage `json:"error"`
				Response struct {
					Status int            `json:"status_code"`
					ID     string         `json:"request_id"`
					Body   map[string]any `json:"body"`
				} `json:"response"`
			}
			reader := json.NewDecoder(bytes.NewReader(scan.Bytes()))
			reader.UseNumber()
			if err := reader.Decode(&item); err != nil {
				return err
			}
			if reader.Decode(new(any)) != io.EOF {
				return errors.New("invalid batch result row")
			}
			id, err := batchCustomID(file, item.ID)
			if err != nil {
				return err
			}
			if err := emit(id, item.Response.Status, item.Response.ID, item.Response.Body, len(item.Error) > 0 && string(item.Error) != "null"); err != nil {
				return err
			}
			continue
		}
		var item struct {
			Status  string `json:"status"`
			Request struct {
				Contents []vertexBatchContent `json:"contents"`
			} `json:"request"`
			Response struct {
				ID         string `json:"responseId"`
				Candidates []struct {
					Finish  string `json:"finishReason"`
					Content struct {
						Parts []struct {
							Text    string `json:"text"`
							Thought bool   `json:"thought"`
						} `json:"parts"`
					} `json:"content"`
				} `json:"candidates"`
			} `json:"response"`
		}
		if err := json.Unmarshal(scan.Bytes(), &item); err != nil {
			return err
		}
		if len(item.Request.Contents) != 1 || len(item.Request.Contents[0].Parts) == 0 {
			return errors.New("missing batch result identity")
		}
		rawID, ok := strings.CutPrefix(item.Request.Contents[0].Parts[0].Text, openAIItemMarker)
		if !ok {
			return errors.New("missing batch result identity")
		}
		rawID, _, _ = strings.Cut(rawID, "]")
		id, err := batchCustomID(file, rawID)
		if err != nil {
			return err
		}
		var data map[string]any
		if item.Status == "" && len(item.Response.Candidates) == 1 {
			candidate := item.Response.Candidates[0]
			var text strings.Builder
			for _, part := range candidate.Content.Parts {
				if !part.Thought {
					text.WriteString(part.Text)
				}
			}
			finish := "content_filter"
			switch candidate.Finish {
			case "STOP":
				finish = "stop"
			case "MAX_TOKENS":
				finish = "length"
			}
			data = map[string]any{"id": item.Response.ID, "object": "chat.completion", "model": file.Model,
				"choices": []any{map[string]any{"index": 0, "finish_reason": finish,
					"message": map[string]string{"role": "assistant", "content": text.String()}}},
			}
		}
		if err := emit(id, 200, item.Response.ID, data, data == nil); err != nil {
			return err
		}
	}
	return scan.Err()
}

func batchCustomID(file openAIInputFile, raw string) (string, error) {
	line, err := strconv.Atoi(raw)
	if err != nil || line < 1 || line > len(file.IDs) {
		return "", errors.New("result references an unknown batch item")
	}
	return file.IDs[line-1], nil
}

func openAIResult(id string, status int, requestID string, body map[string]any, failed bool) map[string]any {
	var response, failure any
	if failed || body == nil || status < 200 || status >= 300 {
		failure = map[string]string{"code": "batch_item_failed", "message": "The upstream batch did not return a completion for this item."}
	} else {
		response = map[string]any{"status_code": status, "request_id": requestID, "body": body}
	}
	return map[string]any{"id": id, "custom_id": id, "response": response, "error": failure}
}

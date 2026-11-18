package server

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/hex"
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
)

type openAIInputFile struct {
	Model  string   `json:"model"`
	Native string   `json:"native_model"`
	Name   string   `json:"name"`
	Bytes  int64    `json:"bytes"`
	At     int64    `json:"created_at"`
	IDs    []string `json:"ids"`
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
	var randomness [16]byte
	if _, err := rand.Read(randomness[:]); err != nil {
		writeAPIError(w, 500, "batch_state_unavailable")
		return
	}
	id := hex.EncodeToString(randomness[:])
	file, err := os.CreateTemp(b.settings.StateDir, ".openai-input-")
	if err != nil {
		writeAPIError(w, 500, "batch_spool_unavailable")
		return
	}
	defer os.Remove(file.Name())
	metadata := openAIInputFile{Name: filepath.Base(info.Filename), At: time.Now().Unix(), Bytes: info.Size}
	record, err := b.convertOpenAIInput(id, input, file, &metadata)
	err = errors.Join(err, file.Close())
	if err != nil {
		writeAPIError(w, 400, "invalid_batch_requests")
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
	if err := os.WriteFile(b.openAIFilePath(id), marshalBatch(metadata), 0o600); err != nil {
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
		MaxTokens     int    `json:"max_tokens"`
		MaxCompletion int    `json:"max_completion_tokens"`
		Reasoning     string `json:"reasoning_effort"`
		Format        *struct {
			Type string `json:"type"`
		} `json:"response_format"`
	} `json:"body"`
}

func (b *batchServer) convertOpenAIInput(id string, input io.Reader, output io.Writer, file *openAIInputFile) (batchRecord, error) {
	var record batchRecord
	buffered := bufio.NewWriter(output)
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
		}
		if row.Body.Model != file.Model {
			return record, errors.New("batch contains more than one model")
		}
		native, err := row.native(len(file.IDs) + 1)
		if err != nil {
			return record, err
		}
		normalized := normalizeBatchRow(native, record)
		if normalized == nil {
			return record, errors.New("unsupported chat batch row")
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

func (r openAIChatBatchRow) native(line int) ([]byte, error) {
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
		case "json_object":
			mime = "application/json"
		case "text":
		default:
			return nil, errors.New("unsupported chat batch response format")
		}
	}
	return marshalBatch(map[string]any{"request": map[string]any{
		"contents":          []any{map[string]any{"role": "user", "parts": parts}},
		"systemInstruction": map[string]any{"parts": []any{map[string]string{"text": system}}},
		"generationConfig": map[string]any{"maxOutputTokens": maxTokens, "responseMimeType": mime,
			"thinkingConfig": map[string]string{"thinkingLevel": level}},
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

func adaptOpenAIResults(input, output *os.File, file openAIInputFile, backend string) error {
	if _, err := input.Seek(0, io.SeekStart); err != nil {
		return err
	}
	reader := json.NewDecoder(input)
	writer := json.NewEncoder(output)
	for {
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
			err := reader.Decode(&item)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			id, err := batchCustomID(file, item.ID)
			if err != nil {
				return err
			}
			result := openAIResult(id, item.Response.Status, item.Response.ID, item.Response.Body, len(item.Error) > 0 && string(item.Error) != "null")
			if err := writer.Encode(result); err != nil {
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
		err := reader.Decode(&item)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
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
		if err := writer.Encode(openAIResult(id, 200, item.Response.ID, data, data == nil)); err != nil {
			return err
		}
	}
	_, err := output.Seek(0, io.SeekStart)
	return err
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

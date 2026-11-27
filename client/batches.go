package client

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"mime/multipart"
	"regexp"
	"strings"
)

var batchID = regexp.MustCompile("^[a-f0-9]{32}$")

type BatchWorkspace struct{ ID, Model string }
type Batch struct {
	ID, Status   string
	InputFileID  string `json:"input_file_id"`
	OutputFileID string `json:"output_file_id"`
}
type BatchItem struct {
	CustomID     string
	Request      ChatRequest
	SchemaName   string
	StrictSchema json.RawMessage
}
type BatchItemError struct{ Code, Message string }
type BatchResult struct {
	CustomID, Content, FinishReason string
	Error                           *BatchItemError
}

func (c *Client) PrepareBatch(ctx context.Context, model string) (BatchWorkspace, error) {
	var workspace BatchWorkspace
	err := c.json(ctx, "POST", "/batches/prepare", map[string]string{"model": model}, &workspace, nil)
	if err == nil && (!batchID.MatchString(workspace.ID) || workspace.Model == "") {
		err = errors.New("invalid batch workspace")
	}
	return workspace, err
}

// UploadBatchPhoto returns the batch-file URL for a chat image part.
func (c *Client) UploadBatchPhoto(ctx context.Context, id, mime string, data []byte) (string, error) {
	if !batchID.MatchString(id) {
		return "", errors.New("invalid batch ID")
	}
	ext := map[string]string{"image/jpeg": "jpg", "image/png": "png"}[mime]
	if ext == "" || len(data) == 0 {
		return "", errors.New("expected a JPEG or PNG photo")
	}
	name := fmt.Sprintf("photos/%x.%s", sha256.Sum256(data), ext)
	resp, err := c.request(ctx, "PUT", "/batches/"+id+"/files/"+name, mime, bytes.NewReader(data), nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	return "batch-file://" + name, nil
}

func writeBatchInput(w *multipart.Writer, id string, items iter.Seq[BatchItem]) error {
	if err := w.WriteField("purpose", "batch"); err != nil {
		return err
	}
	if err := w.WriteField("workspace_id", id); err != nil {
		return err
	}
	f, err := w.CreateFormFile("file", "requests.jsonl")
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for item := range items {
		body, err := item.Request.body()
		if err != nil {
			return err
		}
		if len(item.StrictSchema) > 0 || item.SchemaName != "" {
			format, err := strictFormat(item.SchemaName, item.StrictSchema)
			if err != nil {
				return err
			}
			body["response_format"] = format
		}
		if err := enc.Encode(map[string]any{"method": "POST", "url": "/v1/chat/completions", "custom_id": item.CustomID, "body": body}); err != nil {
			return err
		}
	}
	return w.Close()
}

// SubmitChatBatch uploads JSONL incrementally. Replaying the same ordered
// items against the same workspace recovers the existing file and job.
func (c *Client) SubmitChatBatch(ctx context.Context, id string, items iter.Seq[BatchItem]) (Batch, error) {
	var batch Batch
	if !batchID.MatchString(id) || items == nil {
		return batch, errors.New("invalid batch input")
	}
	reader, writer := io.Pipe()
	multipartWriter := multipart.NewWriter(writer)
	contentType := multipartWriter.FormDataContentType()
	go func() { err := writeBatchInput(multipartWriter, id, items); writer.CloseWithError(err) }()
	resp, err := c.request(ctx, "POST", "/files", contentType, reader, nil)
	reader.Close()
	if err != nil {
		return batch, err
	}
	defer resp.Body.Close()
	var file struct{ ID string }
	if err := decodeResponse(resp.Body, &file); err != nil {
		return batch, err
	}
	if file.ID != "file-"+id {
		return batch, errors.New("mismatched batch input file ID")
	}
	err = c.json(ctx, "POST", "/batches", map[string]string{"input_file_id": file.ID, "endpoint": "/v1/chat/completions", "completion_window": "24h"}, &batch, nil)
	return batch, err
}
func (c *Client) GetBatch(ctx context.Context, id string) (Batch, error) {
	var batch Batch
	if !batchID.MatchString(id) {
		return batch, errors.New("invalid batch ID")
	}
	err := c.json(ctx, "GET", "/batches/"+id, nil, &batch, nil)
	return batch, err
}

// StreamBatchResults distinguishes per-item errors from transport/JSONL errors.
// Breaking out of the loop closes the HTTP response immediately.
func (c *Client) StreamBatchResults(ctx context.Context, outputFileID string) iter.Seq2[BatchResult, error] {
	return func(yield func(BatchResult, error) bool) {
		id, ok := strings.CutPrefix(outputFileID, "file-out-")
		if !ok || !batchID.MatchString(id) {
			yield(BatchResult{}, errors.New("invalid batch output ID"))
			return
		}
		resp, err := c.request(ctx, "GET", "/files/"+outputFileID+"/content", "", nil, nil)
		if err != nil {
			yield(BatchResult{}, err)
			return
		}
		defer resp.Body.Close()
		scan := bufio.NewScanner(resp.Body)
		scan.Buffer(make([]byte, 64<<10), 32<<20)
		for scan.Scan() {
			var row struct {
				CustomID string `json:"custom_id"`
				Response *struct{ Body chatResponse }
				Error    *BatchItemError
			}
			err := json.Unmarshal(scan.Bytes(), &row)
			result := BatchResult{CustomID: row.CustomID, Error: row.Error}
			if err == nil && (row.CustomID == "" || (row.Error == nil) == (row.Response == nil)) {
				err = errors.New("invalid batch result envelope")
			}
			if err == nil && row.Response != nil {
				var choice Completion
				choice, err = row.Response.Body.completion()
				result.Content, result.FinishReason = choice.Content, choice.FinishReason
			}
			if !yield(result, err) || err != nil {
				return
			}
		}
		if err := scan.Err(); err != nil {
			yield(BatchResult{}, err)
		}
	}
}

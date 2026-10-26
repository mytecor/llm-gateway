package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/mytecor/llm-gateway/internal/model"
	"github.com/mytecor/llm-gateway/internal/scheduler"
	"github.com/mytecor/llm-gateway/internal/telemetry"
)

func (s *Server) stream(writer http.ResponseWriter, request *http.Request, logical string, executeRequest model.ExecuteRequest, started time.Time) {
	flusher, ok := writer.(http.Flusher)
	if !ok {
		writeAPIError(writer, http.StatusInternalServerError, "streaming_not_supported")
		return
	}
	sink := &sseStreamSink{writer: writer, flusher: flusher, logical: logical,
		kind: executeRequest.Kind, requestID: telemetry.RequestIDFrom(request.Context())}
	s.runner.RunStream(request.Context(), logical, executeRequest, started, sink)
}

type sseStreamSink struct {
	writer             http.ResponseWriter
	flusher            http.Flusher
	logical, requestID string
	kind               model.RequestKind
	begun              bool
}

func (s *sseStreamSink) Begin() {
	if s.begun {
		return
	}
	s.begun = true
	s.writer.Header().Set("Content-Type", "text/event-stream")
	s.writer.Header().Set("Cache-Control", "no-cache")
	s.writer.Header().Set("Connection", "keep-alive")
	s.writer.WriteHeader(http.StatusOK)
	s.flusher.Flush()
}

func (s *sseStreamSink) Write(event model.StreamEvent) bool {
	return writeStreamEvent(s.writer, s.flusher, s.logical, s.kind, event)
}

// KeepAlive includes an empty chat delta while thinking or emitting tool calls:
// OpenAI SDKs discard SSE comments before client stall watchdogs observe them.
func (s *sseStreamSink) KeepAlive(heartbeat scheduler.StreamHeartbeat) bool {
	s.Begin()
	if _, err := io.WriteString(s.writer, ": ping\n\n"); err != nil {
		return false
	}
	if s.kind == model.RequestChat {
		chunkID := heartbeat.ChunkID
		if chunkID == "" {
			chunkID = "chatcmpl-" + s.requestID
		}
		var delta map[string]any
		if heartbeat.ToolCallIndex != nil {
			idx := *heartbeat.ToolCallIndex
			delta = map[string]any{
				"tool_calls": []map[string]any{{
					"index":    idx,
					"function": map[string]any{"arguments": ""},
				}},
			}
		} else if !heartbeat.TextStarted {
			delta = map[string]any{"reasoning_content": " "}
		}
		if delta != nil {
			keepAliveChunk, _ := json.Marshal(map[string]any{
				"id":     chunkID,
				"object": "chat.completion.chunk",
				"model":  s.logical,
				"choices": []map[string]any{{
					"index": 0,
					"delta": delta,
				}},
			})
			if _, err := fmt.Fprintf(s.writer, "data: %s\n\n", keepAliveChunk); err != nil {
				return false
			}
		}
	}
	s.flusher.Flush()
	return true
}

func (s *sseStreamSink) Complete() {
	if s.kind == model.RequestChat {
		_, _ = io.WriteString(s.writer, "data: [DONE]\n\n")
		s.flusher.Flush()
	}
}

func (s *sseStreamSink) Fail(err *model.CallError, retryable bool) {
	if s.begun {
		writeStreamError(s.writer, s.flusher, err, s.requestID, retryable)
	} else {
		writeCallError(s.writer, err)
	}
}

func writeStreamEvent(writer http.ResponseWriter, flusher http.Flusher, logical string, kind model.RequestKind, event model.StreamEvent) bool {
	data, err := model.SanitizeJSON(event.Data, logical)
	if err != nil {
		return false
	}
	if kind == model.RequestResponses && event.Event != "" {
		if _, err := fmt.Fprintf(writer, "event: %s\n", event.Event); err != nil {
			return false
		}
	}
	if _, err := fmt.Fprintf(writer, "data: %s\n\n", data); err != nil {
		return false
	}
	flusher.Flush()
	return true
}

// writeStreamError emits the terminal SSE error event of a relayed winner
// stream. Headers and the buffered prelude are already flushed at this point,
// so the SSE body is the only channel left: the payload is machine-readable
// (status_code/retryable/partial/request_id) while the message text stays the
// stable "upstream stream failed" string that text-matching clients rely on.
// Deliberately no trailing "data: [DONE]" follows: the stream did not
// complete, and a terminating marker would let clients treat the turn as
// successfully finished.
func writeStreamError(writer http.ResponseWriter, flusher http.Flusher, callErr *model.CallError, requestID string, retryable bool) {
	payload, _ := json.Marshal(map[string]any{
		"error": streamErrorPayload(callErr, requestID, retryable),
	})
	_, _ = fmt.Fprintf(writer, "event: error\ndata: %s\n\n", payload)
	flusher.Flush()
}

// streamErrorPayload builds the machine-readable error body of the SSE
// "event: error" frame. partial is always true: the winner was selected on a
// meaningful event, so content has already been relayed to the client before
// the failure arrived — an in-gateway retry could only duplicate it, which is
// why retrying is left to the client.
func streamErrorPayload(callErr *model.CallError, requestID string, retryable bool) map[string]any {
	type_ := "upstream_error"
	if callErr != nil {
		type_ = string(callErr.Class)
	}
	payload := map[string]any{
		"message":     "upstream stream failed",
		"type":        type_,
		"status_code": model.CallErrorStatus(callErr),
		"retryable":   retryable,
		"partial":     true,
	}
	if callErr != nil && callErr.Code != "" {
		payload["code"] = callErr.Code
	}
	if requestID != "" {
		payload["request_id"] = requestID
	}
	return payload
}

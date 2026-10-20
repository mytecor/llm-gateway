package model

import (
	"context"
	"errors"
	"time"
)

type ErrorClass string

type FailureScope string

const (
	FailureScopeProvider FailureScope = "provider"
	FailureScopeTarget   FailureScope = "target"
	FailureScopeRequest  FailureScope = "request"
)

const (
	ErrorTimeout       ErrorClass = "timeout"
	ErrorConnection    ErrorClass = "connection_error"
	ErrorRateLimit     ErrorClass = "429"
	ErrorNotFound      ErrorClass = "404"
	ErrorModelNotFound ErrorClass = "model_not_found"
	ErrorUpstream      ErrorClass = "5xx"
	ErrorInvalid       ErrorClass = "invalid_response"
	ErrorCancelled     ErrorClass = "cancelled"
)

type CallError struct {
	Class  ErrorClass
	Status int
	Cause  error
	Scope  FailureScope
}

func (e *CallError) Error() string {
	if e == nil {
		return ""
	}
	return string(e.Class)
}

func RouteTimeoutError() *CallError {
	return &CallError{Class: ErrorTimeout, Status: 504, Cause: errors.New("route timeout exceeded")}
}

func (e *CallError) HTTPStatus() int {
	status := 502 // http.StatusBadGateway
	if e != nil {
		switch e.Status {
		case 400, 401, 403, 404, 408, 429, 499, 503, 504:
			status = e.Status
		}
	}
	if status == 499 {
		return 408 // http.StatusRequestTimeout
	}
	return status
}

func CallErrorStatus(callErr *CallError) int {
	return callErr.HTTPStatus()
}

type Target struct {
	Provider string
	Model    string
	Tier     int
}

type RequestKind string

const (
	RequestChat      RequestKind = "chat"
	RequestResponses RequestKind = "responses"
)

type ExecuteRequest struct {
	Kind              RequestKind
	Body              []byte
	StreamIdleTimeout time.Duration
}

type StreamEvent struct {
	Data       []byte
	Event      string
	Meaningful bool
	Done       bool
	Err        *CallError
}

type Executor interface {
	Do(context.Context, Target, ExecuteRequest) ([]byte, *CallError)
	Stream(context.Context, Target, ExecuteRequest) (<-chan StreamEvent, *CallError)
	Close() error
}

// RunOutcome carries the winner of a non-streaming request.
type RunOutcome struct {
	Body     []byte
	Provider string
	// Model is the winner's native provider model ID (empty when no branch
	// ever succeeded).
	Model string
	// Attempts is the number of route executions dispatched for this request
	// (races plus retry/fallback rounds), used by request-level events.
	Attempts int
	// TTFT is the time to first meaningful event; for a non-streaming request
	// this is the full response duration.
	TTFT time.Duration
}

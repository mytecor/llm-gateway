// Package session owns conversations and turns independently of HTTP and engine protocols.
package session

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/mytecor/llm-gateway/internal/model"
	"time"
)

var ErrSessionNotFound = errors.New("native session not found")

// Engine starts a native session. Start must honor ctx, but a successfully
// started Session lives until Close; it must not retain the startup deadline.
type Engine interface {
	Start(context.Context, Spec) (Session, error)
}
type Session interface {
	Run(context.Context, string, func(Event) error) error
	Close() error
}

// ModelLister is optional; failure must leave the last known catalog intact.
type ModelLister = model.ModelLister
type ModelInfo = model.ModelInfo

type Spec struct {
	CWD, Model, System, ResumeID string
	ReasoningEffort              string
	Tools                        []FunctionTool
	CallTool                     func(context.Context, string, json.RawMessage) (string, error)
}

type EventKind string

const (
	EventInit      EventKind = "init"
	EventText      EventKind = "text"
	EventReasoning EventKind = "reasoning"
	EventUsage     EventKind = "usage"
	EventHeartbeat EventKind = "heartbeat"
	EventToolCall  EventKind = "tool_call"
)

type Usage struct{ Input, Output int64 }
type Event struct {
	Kind     EventKind
	Text     string
	ResumeID string
	Usage    *Usage
	ToolCall *ToolCall
}

type Request struct {
	Principal, Conversation, LogicalModel, Model, CWD, ReasoningEffort string
	Messages                                                           []Message
	Tools                                                              []FunctionTool
	ToolChoice                                                         json.RawMessage
}
type Result struct {
	Text, Reasoning, FinishReason string
	ToolCalls                     []ToolCall
	Usage                         *Usage
}
type Config struct {
	Provider                string
	DefaultCWD              string
	AllowedRoots            []string
	TurnTimeout, SessionTTL time.Duration
	HeartbeatInterval       time.Duration
	StorePath               string
	Store                   *Store
}

type Error struct {
	Status int
	Code   string
	Cause  error
}

func (e *Error) Error() string             { return e.Code }
func (e *Error) Unwrap() error             { return e.Cause }
func invalid(code string, err error) error { return &Error{Status: 400, Code: code, Cause: err} }

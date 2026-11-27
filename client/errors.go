package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

type Error struct {
	Status     int
	Code       string
	RetryAfter time.Duration
	RouteLimit int
}

func (e *Error) Error() string       { return fmt.Sprintf("llm-gateway HTTP %d (%s)", e.Status, e.Code) }
func (e *Error) IsRateLimit() bool   { return e.Status == 429 }
func (e *Error) IsUnavailable() bool { return e.Status == 502 || e.Status == 503 || e.Status == 504 }
func responseError(resp *http.Response) *Error {
	var body struct{ Error struct{ Code, Type string } }
	_ = json.NewDecoder(io.LimitReader(resp.Body, 8192)).Decode(&body)
	e := &Error{Status: resp.StatusCode, Code: body.Error.Code}
	if e.Code == "" {
		e.Code = body.Error.Type
	}
	e.RouteLimit, _ = strconv.Atoi(resp.Header.Get("X-LLM-Route-Limit"))
	retry := resp.Header.Get("Retry-After")
	if sec, err := strconv.ParseUint(retry, 10, 32); err == nil {
		e.RetryAfter = time.Duration(sec) * time.Second
	} else if date, err := http.ParseTime(retry); err == nil {
		e.RetryAfter = max(0, time.Until(date))
	}
	return e
}

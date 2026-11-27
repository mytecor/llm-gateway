// Package client provides a typed HTTP client for llm-gateway.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type Priority string

const (
	PriorityInteractive Priority = "interactive"
	PriorityNormal      Priority = "normal"
	PriorityBackground  Priority = "background"
)

type Client struct {
	base, key string
	http      *http.Client
	priority  Priority
}
type Option func(*Client)

func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }
func WithPriority(p Priority) Option       { return func(c *Client) { c.priority = p } }

// New accepts a gateway root or its /v1 base URL. Redirects are disabled
// even when a caller supplies its own transport.
func New(baseURL, apiKey string, opts ...Option) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, errors.New("invalid gateway base URL")
	}
	base := strings.TrimRight(u.String(), "/")
	c := &Client{base: strings.TrimSuffix(base, "/v1") + "/v1", key: apiKey, http: http.DefaultClient}
	for _, opt := range opts {
		opt(c)
	}
	if c.http == nil {
		return nil, errors.New("missing HTTP client")
	}
	switch c.priority {
	case "", PriorityNormal, PriorityBackground, PriorityInteractive:
	default:
		return nil, errors.New("invalid priority")
	}
	h := *c.http
	h.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c.http = &h
	return c, nil
}

func (c *Client) request(ctx context.Context, method, path, mime string, body io.Reader, headers map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return nil, err
	}
	if c.key != "" {
		req.Header.Set("Authorization", "Bearer "+c.key)
	}
	if c.priority != "" {
		req.Header.Set("X-LLM-Priority", string(c.priority))
	}
	if mime != "" {
		req.Header.Set("Content-Type", mime)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		return nil, responseError(resp)
	}
	return resp, nil
}

func (c *Client) json(ctx context.Context, method, path string, in, out any, headers map[string]string) error {
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	resp, err := c.request(ctx, method, path, "application/json", body, headers)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return decodeResponse(resp.Body, out)
}

func decodeResponse(body io.Reader, out any) error {
	const limit = 16 << 20
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return err
	}
	if len(data) > limit {
		return errors.New("gateway response too large")
	}
	return json.Unmarshal(data, out)
}

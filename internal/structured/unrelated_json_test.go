package structured

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mytecor/llm-gateway/internal/model"
)

func TestContractFlagsUnrelatedJSONSeparateFromConstraintErrors(t *testing.T) {
	body := []byte(`{"model":"standard","messages":[{"role":"user","content":"hi"}],"response_format":{"type":"json_schema","json_schema":{"name":"answer","strict":true,"schema":{"type":"object","properties":{"value":{"type":"integer","minimum":0}},"required":["value"],"additionalProperties":false}}}}`)
	c, callErr := Parse(body)
	if callErr != nil {
		t.Fatalf("Parse error: %+v", callErr)
	}

	if err := c.Validate(`{"error":"bad shard"}`); !errors.Is(err, errUnrelatedJSON) {
		t.Fatalf("expected errUnrelatedJSON for unrelated object, got %v", err)
	}
	if err := c.Validate(`[1,2,3]`); !errors.Is(err, errUnrelatedJSON) {
		t.Fatalf("expected errUnrelatedJSON for array, got %v", err)
	}
	err := c.Validate(`{"value":-1}`)
	if err == nil || errors.Is(err, errUnrelatedJSON) || errors.Is(err, errMalformedJSON) {
		t.Fatalf("expected normal validation error when key matches schema, got %v", err)
	}
}

func TestRunWithOptionsFailsOverCleanlyOnUnrelatedJSON(t *testing.T) {
	rawBody := []byte(`{"model":"standard","messages":[{"role":"user","content":"hi"}],"response_format":{"type":"json_schema","json_schema":{"name":"answer","strict":true,"schema":{"type":"object","properties":{"value":{"type":"integer","minimum":0}},"required":["value"],"additionalProperties":false}}}}`)
	c, callErr := Parse(rawBody)
	if callErr != nil {
		t.Fatalf("Parse error: %+v", callErr)
	}

	var calls int
	var failovers int
	gen, runErr := RunWithOptions(context.Background(), c, model.ExecuteRequest{Body: rawBody}, func(_ context.Context, req model.ExecuteRequest) (*Generation, *model.CallError) {
		calls++
		// Failover must keep messages unpolluted (no correction appended).
		if strings.Contains(string(req.Body), "previous response failed") {
			t.Fatalf("unexpected correction feedback in failover body: %s", string(req.Body))
		}
		if calls == 1 {
			return &Generation{
				Outcome: model.RunOutcome{
					Provider: "p1",
					Body:     []byte(`{"choices":[{"index":0,"finish_reason":"stop","message":{"content":"{\"status\":\"spoofed\"}"}}]}`),
				},
			}, nil
		}
		return &Generation{
			Outcome: model.RunOutcome{
				Provider: "p2",
				Body:     []byte(`{"choices":[{"index":0,"finish_reason":"stop","message":{"content":"{\"value\":7}"}}]}`),
			},
		}, nil
	}, RunOptions{
		Failover: func(_ context.Context, _ model.RunOutcome, _ string) bool {
			failovers++
			return true
		},
	})
	if runErr != nil {
		t.Fatalf("unexpected error: %+v", runErr)
	}
	if calls != 2 || failovers != 1 {
		t.Fatalf("calls=%d failovers=%d, want 2 and 1", calls, failovers)
	}
	if gen == nil || !strings.Contains(string(gen.Outcome.Body), `\"value\":7`) {
		t.Fatalf("unexpected generation result: %+v", gen)
	}
}

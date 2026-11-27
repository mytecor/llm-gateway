package client

import (
	"context"
	"encoding/json"

	"github.com/mytecor/llm-gateway/driverproto/systemone"
)

type Question = systemone.Question
type Asset = systemone.Asset
type DecisionRequest struct {
	Model       string
	State       any
	Questions   map[string]Question
	Assets      map[string]Asset
	LongContext bool
}
type Box struct{ Top, Bottom, Left, Right float64 }
type Answer struct {
	Type                    string
	Noul, Score, Confidence *float64
	Choice                  string
	Box                     *Box
	Probabilities           map[string]float64
	Legend                  map[string]json.RawMessage
}
type Decision struct {
	Model   string
	Answers map[string]Answer
	Usage   json.RawMessage
}

func (c *Client) Decide(ctx context.Context, r DecisionRequest) (Decision, error) {
	var decision Decision
	state, err := json.Marshal(r.State)
	if err != nil {
		return decision, err
	}
	req := systemone.Request{Model: r.Model, State: state, Questions: r.Questions, Assets: r.Assets}
	headers := map[string]string{}
	if r.LongContext {
		headers[systemone.LongContextHeader] = "true"
	}
	images := len(r.Assets) > 0
	for _, q := range r.Questions {
		images = images || q.Type == "box"
	}
	if images {
		headers[systemone.ImagesHeader] = "true"
	}
	var raw json.RawMessage
	if err := c.json(ctx, "POST", "/systemone", req, &raw, headers); err != nil {
		return decision, err
	}
	validated, err := req.ValidateResponse(raw, "")
	if err != nil {
		return decision, err
	}
	err = json.Unmarshal(validated, &decision)
	return decision, err
}

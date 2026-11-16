package systemone

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
)

// ValidateResponse normalizes only the documented dialect difference in score
// legends. RawMessage preserves every upstream number and extension field.
func (r *Request) ValidateResponse(data []byte, dialect string) ([]byte, error) {
	if len(data) > MaxResponseBytes {
		return nil, errors.New("decision response exceeds limit")
	}
	var root map[string]json.RawMessage
	if json.Unmarshal(data, &root) != nil || root == nil {
		return nil, errors.New("invalid decision response")
	}
	var model string
	if json.Unmarshal(root["model"], &model) != nil || model == "" {
		return nil, errors.New("missing decision model")
	}
	var answers map[string]map[string]json.RawMessage
	if json.Unmarshal(root["answers"], &answers) != nil || len(answers) != len(r.Questions) {
		return nil, errors.New("answer IDs do not match questions")
	}
	if raw, ok := root["usage"]; ok {
		var usage map[string]json.RawMessage
		if json.Unmarshal(raw, &usage) != nil || usage == nil {
			return nil, errors.New("invalid usage")
		}
		for _, key := range []string{"input_tokens", "output_tokens"} {
			var n int64
			if json.Unmarshal(usage[key], &n) != nil || n < 0 {
				return nil, errors.New("invalid usage tokens")
			}
		}
	}
	for id, q := range r.Questions {
		a, ok := answers[id]
		if !ok {
			return nil, errors.New("missing answer")
		}
		var typ string
		if json.Unmarshal(a["type"], &typ) != nil || typ != q.Type {
			return nil, errors.New("answer type mismatch")
		}
		switch typ {
		case "noul":
			if !inRange(a["noul"], 0, 1) {
				return nil, errors.New("invalid noul")
			}
		case "choice":
			var opts map[string]json.RawMessage
			_ = json.Unmarshal(q.Criteria, &opts)
			var choice string
			if json.Unmarshal(a["choice"], &choice) != nil {
				return nil, errors.New("invalid choice")
			}
			if _, ok := opts[choice]; !ok {
				return nil, errors.New("unknown choice")
			}
			if !inRange(a["confidence"], 0, 1) || !validProbabilities(a["probabilities"], opts) {
				return nil, errors.New("invalid choice probabilities or confidence")
			}
		case "score":
			var levels []json.RawMessage
			_ = json.Unmarshal(q.Criteria, &levels)
			legend := make(map[string]json.RawMessage, len(levels))
			for i, v := range levels {
				legend[fmt.Sprint(i)] = v
			}
			if dialect == "gonka" {
				var list []json.RawMessage
				if json.Unmarshal(a["legend"], &list) == nil && list != nil {
					obj := make(map[string]json.RawMessage, len(list))
					for i, v := range list {
						obj[fmt.Sprint(i)] = v
					}
					a["legend"], _ = json.Marshal(obj)
				}
			}
			var got map[string]json.RawMessage
			if json.Unmarshal(a["legend"], &got) != nil || len(got) != len(legend) {
				return nil, errors.New("invalid score legend")
			}
			for k, v := range legend {
				if !sameJSON(v, got[k]) {
					return nil, errors.New("score legend mismatch")
				}
			}
			if !inRange(a["score"], 0, float64(len(levels)-1)) || !inRange(a["confidence"], 0, 1) || !validProbabilities(a["probabilities"], legend) {
				return nil, errors.New("invalid score probabilities or confidence")
			}
		case "box":
			var box map[string]json.RawMessage
			if json.Unmarshal(a["box"], &box) != nil {
				return nil, errors.New("invalid box")
			}
			for _, edge := range []string{"top", "bottom", "left", "right"} {
				if !inRange(box[edge], 0, 1) {
					return nil, errors.New("invalid box edge")
				}
			}
			if greater(box["top"], box["bottom"]) || greater(box["left"], box["right"]) {
				return nil, errors.New("inverted box")
			}
			if string(a["confidence"]) != "null" && !inRange(a["confidence"], 0, 1) {
				return nil, errors.New("invalid box confidence")
			}
		}
	}
	root["answers"], _ = json.Marshal(answers)
	return json.Marshal(root)
}

// Compare bounds without rounding a value just above 1 down to float64(1).
// Precision scales with the decimal mantissa length; exponent overflow is rejected.
func decimal(raw json.RawMessage, precision uint) (*big.Float, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] == '"' {
		return nil, false
	}
	var n json.Number
	if json.Unmarshal(raw, &n) != nil {
		return nil, false
	}
	value, _, err := big.ParseFloat(string(n), 10, precision, big.ToNearestEven)
	return value, err == nil
}
func inRange(raw json.RawMessage, lo, hi float64) bool {
	n, ok := decimal(raw, uint(4*len(raw)+64))
	return ok && n.Cmp(big.NewFloat(lo)) >= 0 && n.Cmp(big.NewFloat(hi)) <= 0
}
func greater(a, b json.RawMessage) bool {
	precision := uint(4*(len(a)+len(b)) + 64)
	x, xok := decimal(a, precision)
	y, yok := decimal(b, precision)
	return xok && yok && x.Cmp(y) > 0
}
func validProbabilities(raw json.RawMessage, expected map[string]json.RawMessage) bool {
	var probabilities map[string]json.RawMessage
	if json.Unmarshal(raw, &probabilities) != nil || len(probabilities) != len(expected) {
		return false
	}
	for key := range expected {
		if !inRange(probabilities[key], 0, 1) {
			return false
		}
	}
	return true
}
func sameJSON(a, b []byte) bool {
	var x, y any
	if Decode(a, &x) != nil || Decode(b, &y) != nil {
		return false
	}
	xa, _ := json.Marshal(x)
	ya, _ := json.Marshal(y)
	return string(xa) == string(ya)
}

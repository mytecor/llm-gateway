// Package systemone defines the native decision contract shared by the gateway
// and JEV driver. It depends only on the standard library.
package systemone

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"strings"
)

const (
	MaxHTTPBytes      = 32 << 20
	MaxCanonicalBytes = 48 << 20
	MaxCommandBytes   = 64 << 20
	MaxResponseBytes  = 16 << 20
	ImagesHeader      = "TypeSafe-Danger-Zone-Images"
	LongContextHeader = "TypeSafe-Danger-Zone-Long-Context"
)

type Features struct{ Images, Box, LongContext bool }
type Request struct {
	Model     string              `json:"model"`
	State     json.RawMessage     `json:"state"`
	Questions map[string]Question `json:"questions"`
	Assets    map[string]Asset    `json:"assets,omitempty"`
}
type Question struct {
	Type         string          `json:"type"`
	Instructions json.RawMessage `json:"instructions,omitempty"`
	Criteria     json.RawMessage `json:"criteria,omitempty"`
}
type Asset struct {
	Type   string `json:"type,omitempty"`
	Format string `json:"format,omitempty"`
	Data   string `json:"data_base64,omitempty"`
	Part   string `json:"part,omitempty"`
}

// Decode rejects additional documents and unknown contract fields. Raw JSON
// values preserve structured instructions and number precision.
func Decode(data []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		return errors.New("invalid System One JSON")
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return errors.New("expected one JSON document")
	}
	return nil
}

func Parse(data []byte, headers map[string]string) (*Request, Features, error) {
	var req Request
	var features Features
	if len(data) > MaxCanonicalBytes {
		return nil, features, errors.New("System One canonical body exceeds 48 MiB")
	}
	if err := Decode(data, &req); err != nil {
		return nil, features, err
	}
	for k, v := range headers {
		if k != ImagesHeader && k != LongContextHeader {
			return nil, features, errors.New("unsupported System One header")
		}
		if v != "true" {
			return nil, features, errors.New("feature headers must be true when present")
		}
	}
	features.Images = headers[ImagesHeader] == "true"
	features.LongContext = headers[LongContextHeader] == "true"
	if strings.TrimSpace(req.Model) == "" || len(req.Questions) == 0 || !structured(req.State, false) {
		return nil, features, errors.New("model, state and non-empty questions are required")
	}
	for _, q := range req.Questions {
		if len(q.Instructions) > 0 && !structured(q.Instructions, true) {
			return nil, features, errors.New("invalid question instructions")
		}
		switch q.Type {
		case "choice":
			var options map[string]json.RawMessage
			if json.Unmarshal(q.Criteria, &options) != nil || len(options) < 1 || len(options) > 255 {
				return nil, features, errors.New("choice criteria require 1 to 255 options")
			}
			for _, v := range options {
				if !structured(v, true) {
					return nil, features, errors.New("invalid choice criterion")
				}
			}
		case "score":
			var levels []json.RawMessage
			if json.Unmarshal(q.Criteria, &levels) != nil || len(levels) < 1 || len(levels) > 10 {
				return nil, features, errors.New("score criteria require an ordered array of 1 to 10 levels")
			}
			for _, v := range levels {
				if !structured(v, false) {
					return nil, features, errors.New("invalid score level")
				}
			}
		case "noul":
			if len(q.Criteria) > 0 && string(q.Criteria) != "null" {
				var criteria map[string]json.RawMessage
				if json.Unmarshal(q.Criteria, &criteria) != nil || criteria == nil {
					return nil, features, errors.New("invalid noul criteria")
				}
				for k, v := range criteria {
					if (k != "true" && k != "false") || !structured(v, true) {
						return nil, features, errors.New("invalid noul criterion")
					}
				}
			}
		case "box":
			features.Box = true
			if len(q.Criteria) > 0 {
				return nil, features, errors.New("box does not accept criteria")
			}
		default:
			return nil, features, errors.New("unsupported question type")
		}
	}
	if (len(req.Assets) > 0 || features.Box) && !features.Images {
		return nil, features, errors.New("images and box require the images feature header")
	}
	// Bound JSON nesting without converting numbers to float64.
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil || !depthOK(value, 0) {
		return nil, features, errors.New("JSON nesting exceeds 64 levels")
	}
	var references []string
	collectStrings(req.State, &references)
	for _, q := range req.Questions {
		collectStrings(q.Instructions, &references)
		collectStrings(q.Criteria, &references)
	}
	totalRefBytes, totalImageBytes := 0, 0
	for ref, a := range req.Assets {
		totalRefBytes += len(ref)
		if strings.TrimSpace(ref) == "" || len(ref) > 1024 || totalRefBytes > 65536 {
			return nil, features, errors.New("invalid asset reference")
		}
		used := false
		for _, s := range references {
			if strings.Contains(s, ref) {
				used = true
				break
			}
		}
		if !used {
			return nil, features, errors.New("unreferenced asset")
		}
		b, err := a.Bytes()
		if err != nil {
			return nil, features, err
		}
		totalImageBytes += len(b)
		if totalImageBytes > MaxHTTPBytes {
			return nil, features, errors.New("images exceed 32 MiB")
		}
	}
	return &req, features, nil
}
func structured(raw json.RawMessage, nullable bool) bool {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return false
	}
	return raw[0] == '"' || raw[0] == '{' || raw[0] == '[' || (nullable && string(raw) == "null")
}
func depthOK(v any, depth int) bool {
	if depth > 64 {
		return false
	}
	switch x := v.(type) {
	case json.Number:
		_, err := x.Float64()
		return err == nil
	case map[string]any:
		for _, c := range x {
			if !depthOK(c, depth+1) {
				return false
			}
		}
	case []any:
		for _, c := range x {
			if !depthOK(c, depth+1) {
				return false
			}
		}
	}
	return true
}
func collectStrings(raw []byte, into *[]string) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if dec.Decode(&v) != nil {
		return
	}
	var visit func(any)
	visit = func(v any) {
		switch x := v.(type) {
		case string:
			*into = append(*into, x)
		case map[string]any:
			for _, c := range x {
				visit(c)
			}
		case []any:
			for _, c := range x {
				visit(c)
			}
		}
	}
	visit(v)
}

func (a Asset) Bytes() ([]byte, error) {
	if (a.Type != "" && a.Type != "image") || a.Part != "" || (a.Format != "png" && a.Format != "jpg" && a.Format != "jpeg") || a.Data == "" || strings.ContainsAny(a.Data, "\r\n\t ") {
		return nil, errors.New("invalid image asset")
	}
	if base64.StdEncoding.DecodedLen(len(a.Data)) > (16<<20)+2 {
		return nil, errors.New("image exceeds 16 MiB")
	}
	b, err := base64.StdEncoding.Strict().DecodeString(a.Data)
	if err != nil || len(b) > 16<<20 {
		return nil, errors.New("invalid or oversized base64 image")
	}
	format, err := ImageFormat(b)
	if err != nil {
		return nil, err
	}
	if (a.Format == "png") != (format == "png") {
		return nil, errors.New("image format mismatch")
	}
	return b, nil
}

// ImageFormat checks format/dimensions without resizing or decoding pixels.
func ImageFormat(b []byte) (string, error) {
	cfg, format, err := image.DecodeConfig(bytes.NewReader(b))
	if err != nil || (format != "png" && format != "jpeg") {
		return "", errors.New("expected PNG or JPEG image")
	}
	if cfg.Width < 280 || cfg.Height < 280 || cfg.Width > 1568 || cfg.Height > 1568 {
		return "", errors.New("image dimensions must be between 280 and 1568")
	}
	if format == "png" {
		for pos := 8; pos+12 <= len(b); {
			size := int(binary.BigEndian.Uint32(b[pos : pos+4]))
			if size > len(b)-pos-12 {
				return "", errors.New("invalid PNG chunk")
			}
			if string(b[pos+4:pos+8]) == "acTL" {
				return "", errors.New("animated PNG is unsupported")
			}
			pos += size + 12
		}
	}
	return format, nil
}

func (r *Request) Body(model, dialect string) ([]byte, error) {
	copyReq := *r
	copyReq.Model = model
	if dialect != "gonka" {
		return json.Marshal(copyReq)
	}
	copyReq.Questions = make(map[string]Question, len(r.Questions))
	for id, q := range r.Questions {
		if q.Type == "score" {
			var levels []json.RawMessage
			if err := json.Unmarshal(q.Criteria, &levels); err != nil {
				return nil, err
			}
			criteria := make(map[string]json.RawMessage, len(levels))
			for i, v := range levels {
				criteria[fmt.Sprint(i)] = v
			}
			q.Criteria, _ = json.Marshal(criteria)
		}
		copyReq.Questions[id] = q
	}
	return json.Marshal(copyReq)
}

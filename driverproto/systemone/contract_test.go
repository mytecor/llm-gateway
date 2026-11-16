package systemone

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/png"
	"strings"
	"testing"
)

const mixed = `{"model":"jev","state":{"amount":9007199254740993},"questions":{"team":{"type":"choice","instructions":{"task":["Select a team"]},"criteria":{"a":null,"b":{"description":"Other"}}},"severity":{"type":"score","criteria":["Low",{"description":"High"}]},"urgent":{"type":"noul","instructions":["Urgent?"],"criteria":{"true":"Today","false":"Later"}}}}`
const mixedAnswer = `{"model":"jev-1.14.0","answers":{"team":{"type":"choice","choice":"a","confidence":0.1,"probabilities":{"a":0.50000000000000001,"b":0.49999999999999999}},"severity":{"type":"score","score":0.7,"confidence":0.3,"legend":{"0":"Low","1":{"description":"High"}},"probabilities":{"0":0.3,"1":0.7}},"urgent":{"type":"noul","noul":0.92}},"usage":{"input_tokens":10,"output_tokens":3}}`

func TestNativeContractPreservesStructureAndPrecision(t *testing.T) {
	req, _, err := Parse([]byte(mixed), nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := req.Body("jev", "gonka")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`9007199254740993`)) || !bytes.Contains(body, []byte(`"criteria":{"0":"Low","1":{"description":"High"}}`)) {
		t.Fatalf("lost precision or score adaptation: %s", body)
	}
	result, err := req.ValidateResponse([]byte(mixedAnswer), "typesafe")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(result, []byte(`0.50000000000000001`)) || !bytes.Contains(result, []byte(`"model":"jev-1.14.0"`)) {
		t.Fatalf("response changed: %s", result)
	}
	gonka := strings.Replace(mixedAnswer, `"legend":{"0":"Low","1":{"description":"High"}}`, `"legend":["Low",{"description":"High"}]`, 1)
	if _, err = req.ValidateResponse([]byte(gonka), "gonka"); err != nil {
		t.Fatal(err)
	}
}
func TestRejectInvalidRequestAndAnswers(t *testing.T) {
	for _, input := range []string{
		strings.Replace(mixed, `"type":"choice"`, `"type":"unknown"`, 1),
		strings.Replace(mixed, `"state":{"amount":9007199254740993}`, `"state":null`, 1),
		strings.Replace(mixed, `["Low",{"description":"High"}]`, `{"0":"Low"}`, 1),
		mixed + ` {}`, strings.Replace(mixed, `"questions":`, `"stream":true,"questions":`, 1),
	} {
		if _, _, err := Parse([]byte(input), nil); err == nil {
			t.Fatalf("accepted invalid request: %s", input)
		}
	}
	req, _, _ := Parse([]byte(mixed), nil)
	for _, input := range []string{
		strings.Replace(mixedAnswer, `"choice":"a"`, `"choice":"missing"`, 1),
		strings.Replace(mixedAnswer, `"noul":0.92`, `"noul":1.1`, 1),
		strings.Replace(mixedAnswer, `"noul":0.92`, `"noul":1.00000000000000000001`, 1),
		strings.Replace(mixedAnswer, `"noul":0.92`, `"noul":-1e-400`, 1),
		strings.Replace(mixedAnswer, `"type":"noul"`, `"type":"score"`, 1),
		strings.Replace(mixedAnswer, `"confidence":0.1`, `"confidence":null`, 1),
		strings.Replace(mixedAnswer, `"score":0.7`, `"score":2`, 1),
		strings.Replace(mixedAnswer, `"urgent":`, `"other":`, 1),
		strings.Replace(mixedAnswer, `"input_tokens":10`, `"input_tokens":-1`, 1),
	} {
		if _, err := req.ValidateResponse([]byte(input), "typesafe"); err == nil {
			t.Fatalf("accepted invalid response: %s", input)
		}
	}
	withoutUsage := strings.Replace(mixedAnswer, `,"usage":{"input_tokens":10,"output_tokens":3}`, ``, 1)
	if _, err := req.ValidateResponse([]byte(withoutUsage), "typesafe"); err != nil {
		t.Fatalf("optional usage: %v", err)
	}
}
func TestImagesAndBox(t *testing.T) {
	var imageData bytes.Buffer
	if err := png.Encode(&imageData, image.NewRGBA(image.Rect(0, 0, 280, 280))); err != nil {
		t.Fatal(err)
	}
	req := Request{Model: "jev", State: json.RawMessage(`{"photo":"image-ref"}`), Questions: map[string]Question{"where": {Type: "box", Instructions: json.RawMessage(`"Find object"`)}}, Assets: map[string]Asset{"image-ref": {Type: "image", Format: "png", Data: base64.StdEncoding.EncodeToString(imageData.Bytes())}}}
	body, _ := json.Marshal(req)
	if _, _, err := Parse(body, nil); err == nil {
		t.Fatal("accepted images without opt in")
	}
	headers := map[string]string{ImagesHeader: "true", LongContextHeader: "true"}
	parsed, features, err := Parse(body, headers)
	if err != nil {
		t.Fatal(err)
	}
	if !features.Images || !features.Box || !features.LongContext {
		t.Fatal("lost required features")
	}
	answer := []byte(`{"model":"jev-1.14.0","answers":{"where":{"type":"box","box":{"top":0.1,"bottom":0.8,"left":0.2,"right":0.9},"confidence":null}}}`)
	if _, err := parsed.ValidateResponse(answer, "typesafe"); err != nil {
		t.Fatal(err)
	}
	if _, err := parsed.ValidateResponse(bytes.Replace(answer, []byte(`"top":0.1`), []byte(`"top":0.95`), 1), "typesafe"); err == nil {
		t.Fatal("accepted inverted box")
	}
	req.State = json.RawMessage(`"unrelated"`)
	body, _ = json.Marshal(req)
	if _, _, err := Parse(body, headers); err == nil {
		t.Fatal("accepted unreferenced asset")
	}
	req.State = json.RawMessage(`"image-ref"`)
	a := req.Assets["image-ref"]
	a.Data = "data:image/png;base64," + a.Data
	req.Assets["image-ref"] = a
	body, _ = json.Marshal(req)
	if _, _, err := Parse(body, headers); err == nil {
		t.Fatal("accepted data URL")
	}
}

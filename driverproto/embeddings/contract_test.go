package embeddings

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestInputShapesAndValidation(t *testing.T) {
	for _, input := range []string{`"hello"`, `["one","two"]`, `[2,3,1]`, `[[2,3],[2,4]]`, `{"parts":[{"text":"one"}]}`, `[{"parts":[{"text":"one"}]},{"parts":[{"text":"two"}]}]`} {
		r, encoding, err := ParseOpenAI([]byte(`{"model":"embedding","input":` + input + `}`))
		if err != nil || encoding != "float" || len(r.Items) == 0 {
			t.Fatalf("%s: %v", input, err)
		}
	}
	for _, input := range []string{`[]`, `null`, `""`, `["text",{"parts":[{"text":"text"}]}]`, `[true]`, `[-1]`, `[[1.2]]`, `{"parts":[{"fileData":{"fileUri":"file:///etc/passwd"}}]}`, `{"parts":[{"inlineData":{"mimeType":"image/png","data":"%%%"}}]}`, `{"parts":[{"text":"text","inlineData":{"mimeType":"image/png","data":"YQ=="}}]}`} {
		if _, _, err := ParseOpenAI([]byte(`{"model":"embedding","input":` + input + `}`)); err == nil {
			t.Fatalf("accepted %s", input)
		}
	}
	for _, extra := range []string{`,"dimensions":13`, `,"encoding_format":"int8"`, `,"task_type":"unknown"`, `,"stream":true`, `,"title":"title"`} {
		if _, _, err := ParseOpenAI([]byte(`{"model":"embedding","input":"text"` + extra + `}`)); err == nil {
			t.Fatalf("accepted %s", extra)
		}
	}
}

func TestGoogleAndOpenAIHaveIdenticalCanonicalInputs(t *testing.T) {
	parts := `{"parts":[{"text":"before"},{"inlineData":{"mimeType":"video/mp4","data":"YQ=="}},{"text":"after"}]}`
	a, _, err := ParseOpenAI([]byte(`{"model":"embedding","input":` + parts + `,"dimensions":256}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := ParseGoogle([]byte(`{"model":"models/embedding","content":`+parts+`,"outputDimensionality":256}`), "embedding", false)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatal("API dialects differ")
	}
	f, _ := a.Validate()
	if !f.Video || f.Images || f.Audio {
		t.Fatal(f)
	}
	if _, err := ParseGoogle([]byte(`{"model":"models/other","content":{"parts":[{"text":"x"}]}}`), "embedding", false); err == nil {
		t.Fatal("accepted mismatched model")
	}
	if _, _, err := ParseOpenAI([]byte(`{"model":"embedding","input":` + parts + `,"task_type":"RETRIEVAL_QUERY"}`)); err == nil {
		t.Fatal("accepted task prefix for media")
	}
	batch, err := ParseGoogle([]byte(`{"requests":[{"content":{"parts":[{"text":"one"}]},"outputDimensionality":128},{"content":{"parts":[{"text":"two"}]},"outputDimensionality":512}]}`), "embedding", true)
	if err != nil || batch.Items[0].Dimensions != 128 || batch.Items[1].Dimensions != 512 {
		t.Fatal(batch, err)
	}
}

func TestGenAISDKProtobufFieldNames(t *testing.T) {
	camel, err := ParseGoogle([]byte(`{"requests":[{"content":{"parts":[{"text":"caption"},{"inlineData":{"data":"YQ==","mimeType":"image/png"}}]},"outputDimensionality":128,"model":"models/embedding"}]}`), "embedding", true)
	if err != nil {
		t.Fatal(err)
	}
	snake, err := ParseGoogle([]byte(`{"requests":[{"content":{"parts":[{"text":"caption"},{"inline_data":{"data":"YQ==","mime_type":"image/png"}}]},"outputDimensionality":128,"model":"models/embedding"}]}`), "embedding", true)
	if err != nil || !reflect.DeepEqual(camel, snake) {
		t.Fatalf("GenAI SDK body did not normalize: %v", err)
	}
	for _, input := range []string{
		`{"parts":[{"inlineData":{"mimeType":"image/png","mime_type":"image/png","data":"YQ=="}}]}`,
		`{"parts":[{"inlineData":{"mimeType":"image/png","data":"YQ=="},"inline_data":{"mime_type":"image/png","data":"YQ=="}}]}`,
		`{"parts":[{"inline_data":{"mime_type":"image/png","data":"YQ==","unknown":true}}]}`,
	} {
		if _, _, err := ParseOpenAI([]byte(`{"model":"embedding","input":` + input + `}`)); err == nil {
			t.Fatal("accepted ambiguous or unknown media field")
		}
	}
}

func TestResponseContractAndBase64(t *testing.T) {
	r, _, _ := ParseOpenAI([]byte(`{"model":"embedding","input":"hello","dimensions":128}`))
	values := make([]float32, 128)
	values[0] = 1
	b, _ := json.Marshal(Response{Embeddings: []Vector{{Values: values, Tokens: 7}}})
	response, err := r.ValidateResponse(b)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(response.OpenAI("embedding", "base64"))
	var out struct {
		Data []struct {
			Embedding string `json:"embedding"`
		} `json:"data"`
	}
	_ = json.Unmarshal(encoded, &out)
	decoded, _ := base64.StdEncoding.DecodeString(out.Data[0].Embedding)
	if len(decoded) != 128*4 || math.Float32frombits(binary.LittleEndian.Uint32(decoded)) != 1 || !strings.Contains(string(encoded), `"prompt_tokens":7`) {
		t.Fatal("invalid OpenAI base64 response")
	}
	for _, body := range []string{`{"embeddings":[]}`, strings.Replace(string(b), `"tokens":7`, `"tokens":-1`, 1), strings.Replace(string(b), `[1,`, `[2,`, 1)} {
		if _, err := r.ValidateResponse([]byte(body)); err == nil {
			t.Fatal("accepted invalid response")
		}
	}
}

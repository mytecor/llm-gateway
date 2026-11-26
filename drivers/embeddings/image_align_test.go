package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mytecor/llm-gateway/driverproto"
)

func makeJPEGWithEXIF(t *testing.T, img image.Image, orientation uint16) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: 95}); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	if orientation == 0 {
		return raw
	}
	// Minimal little-endian TIFF with a single IFD0 Orientation tag (0x0112).
	var tiff bytes.Buffer
	tiff.WriteString("II")
	_ = binary.Write(&tiff, binary.LittleEndian, uint16(42))
	_ = binary.Write(&tiff, binary.LittleEndian, uint32(8))
	_ = binary.Write(&tiff, binary.LittleEndian, uint16(1))
	_ = binary.Write(&tiff, binary.LittleEndian, uint16(0x0112))
	_ = binary.Write(&tiff, binary.LittleEndian, uint16(3))
	_ = binary.Write(&tiff, binary.LittleEndian, uint32(1))
	_ = binary.Write(&tiff, binary.LittleEndian, orientation)
	_ = binary.Write(&tiff, binary.LittleEndian, uint16(0))
	_ = binary.Write(&tiff, binary.LittleEndian, uint32(0))

	payload := append([]byte("Exif\x00\x00"), tiff.Bytes()...)
	segLen := uint16(len(payload) + 2)
	var out bytes.Buffer
	out.Write(raw[:2]) // SOI
	out.Write([]byte{0xFF, 0xE1})
	_ = binary.Write(&out, binary.BigEndian, segLen)
	out.Write(payload)
	out.Write(raw[2:])
	return out.Bytes()
}

func TestConfigRejectsUnknownImageAlign(t *testing.T) {
	cfg, _ := json.Marshal(config{InferenceURL: "http://127.0.0.1:9226", ImageAlign: "unknown"})
	if _, err := newExecutor(cfg); err == nil {
		t.Fatal("accepted unknown image_align value")
	}
	cfg, _ = json.Marshal(config{InferenceURL: "http://127.0.0.1:9226", ImageAlign: "embeddinggemma2"})
	if _, err := newExecutor(cfg); err != nil {
		t.Fatalf("rejected valid image_align: %v", err)
	}
}

func TestAlignEmbeddingGemma2DimensionsAndEXIF(t *testing.T) {
	// 1080x1004 with EXIF orientation 6 transposes to 1004x1080 -> 768x816 (16*48 x 17*48 = 272 <= 280).
	src := image.NewRGBA(image.Rect(0, 0, 1080, 1004))
	// Paint the top-left corner red and bottom-right corner blue to verify 90 CW rotation.
	for y := 0; y < 200; y++ {
		for x := 0; x < 200; x++ {
			src.SetRGBA(x, y, color.RGBA{R: 240, G: 10, B: 10, A: 255})
		}
	}
	for y := 804; y < 1004; y++ {
		for x := 880; x < 1080; x++ {
			src.SetRGBA(x, y, color.RGBA{R: 10, G: 10, B: 240, A: 255})
		}
	}
	raw := makeJPEGWithEXIF(t, src, 6)
	alignedB64, err := alignEmbeddingGemma2Image(base64.StdEncoding.EncodeToString(raw), "image/jpeg")
	if err != nil {
		t.Fatal(err)
	}
	alignedBytes, err := base64.StdEncoding.DecodeString(alignedB64)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := png.Decode(bytes.NewReader(alignedBytes))
	if err != nil {
		t.Fatalf("expected PNG output: %v", err)
	}
	if decoded.Bounds().Dx() != 768 || decoded.Bounds().Dy() != 816 {
		t.Fatalf("unexpected aligned dimensions: got %dx%d, want 768x816", decoded.Bounds().Dx(), decoded.Bounds().Dy())
	}
	// After 90 deg CW rotation, top-left (red) moves to top-right.
	tr := color.RGBAModel.Convert(decoded.At(720, 40)).(color.RGBA)
	if tr.R < 200 || tr.B > 50 {
		t.Fatalf("expected red at top-right after EXIF 6 rotation, got %+v", tr)
	}
	// Bottom-right (blue) moves to bottom-left.
	bl := color.RGBAModel.Convert(decoded.At(40, 770)).(color.RGBA)
	if bl.B < 200 || bl.R > 50 {
		t.Fatalf("expected blue at bottom-left after EXIF 6 rotation, got %+v", bl)
	}
}

func TestAlignEmbeddingGemma2AllOrientations(t *testing.T) {
	// Asymmetric 2x3 RGB image with unique pixels 0..5.
	// (0,0)=0 (1,0)=1
	// (0,1)=2 (1,1)=3
	// (0,2)=4 (1,2)=5
	src := []byte{
		0, 0, 0, 1, 0, 0,
		2, 0, 0, 3, 0, 0,
		4, 0, 0, 5, 0, 0,
	}
	want := map[int]struct {
		w, h int
		r    []byte
	}{
		2: {2, 3, []byte{1, 0, 3, 2, 5, 4}},
		3: {2, 3, []byte{5, 4, 3, 2, 1, 0}},
		4: {2, 3, []byte{4, 5, 2, 3, 0, 1}},
		5: {3, 2, []byte{0, 2, 4, 1, 3, 5}},
		6: {3, 2, []byte{4, 2, 0, 5, 3, 1}},
		7: {3, 2, []byte{5, 3, 1, 4, 2, 0}},
		8: {3, 2, []byte{1, 3, 5, 0, 2, 4}},
	}
	for orient, exp := range want {
		got, w, h := applyEXIFOrientation(src, 2, 3, orient)
		if w != exp.w || h != exp.h {
			t.Fatalf("orientation %d: got size %dx%d, want %dx%d", orient, w, h, exp.w, exp.h)
		}
		for i, wantR := range exp.r {
			if got[i*3] != wantR {
				t.Fatalf("orientation %d pixel %d: got %d, want %d", orient, i, got[i*3], wantR)
			}
		}
	}
}

func TestAlignEmbeddingGemma2RejectsPixelLimitAndCorrupt(t *testing.T) {
	// Synthesize a valid PNG header claiming 5000x5000 = 25,000,000 > 16,777,216 pixels.
	var hdr bytes.Buffer
	hdr.Write([]byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1A, '\n'})
	_ = binary.Write(&hdr, binary.BigEndian, uint32(13))
	hdr.WriteString("IHDR")
	_ = binary.Write(&hdr, binary.BigEndian, uint32(5000))
	_ = binary.Write(&hdr, binary.BigEndian, uint32(5000))
	hdr.Write([]byte{8, 2, 0, 0, 0})
	_ = binary.Write(&hdr, binary.BigEndian, uint32(0)) // CRC not verified by DecodeConfig
	if _, err := alignEmbeddingGemma2Image(base64.StdEncoding.EncodeToString(hdr.Bytes()), "image/png"); err == nil {
		t.Fatal("accepted image exceeding pixel limit")
	}

	// Corrupt JPEG returns HTTP 400 unsupported_media without hitting upstream.
	hits := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		http.Error(w, "should not be called", 500)
	}))
	defer server.Close()
	cfg, _ := json.Marshal(config{InferenceURL: server.URL, ImageAlign: "embeddinggemma2"})
	exec, err := newExecutor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	body := json.RawMessage(`{"model":"logical","items":[{"dimensions":768,"parts":[{"inlineData":{"mimeType":"image/jpeg","data":"bm90YW5pbWFnZQ=="}}]}]}`)
	_, failure := exec.do(context.Background(), driverproto.Command[json.RawMessage]{Kind: "embeddings", Model: "google/embeddinggemma-2", Body: body})
	if failure == nil || failure.Status != 400 || failure.Code != "unsupported_media" || failure.Scope != "request" {
		t.Fatalf("unexpected failure for corrupt image: %+v", failure)
	}
	if hits != 0 {
		t.Fatalf("upstream was called %d times on corrupt image", hits)
	}
}

func TestOpenAIImageAlignForwardsAlignedPNG(t *testing.T) {
	src := image.NewRGBA(image.Rect(0, 0, 640, 595))
	raw := makeJPEGWithEXIF(t, src, 0)
	var gotURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Input []struct {
				Content []openAIContent `json:"content"`
			} `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode upstream request: %v", err)
		}
		gotURL = req.Input[0].Content[0].Image["url"]
		vals := make([]float32, 768)
		vals[0] = 1
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data":  []map[string]any{{"index": 0, "embedding": vals}},
			"usage": map[string]int{"prompt_tokens": 276},
		})
	}))
	defer server.Close()

	cfg, _ := json.Marshal(config{InferenceURL: server.URL, ImageAlign: "embeddinggemma2"})
	exec, err := newExecutor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(map[string]any{
		"model": "logical",
		"items": []map[string]any{{
			"dimensions": 768,
			"parts": []map[string]any{{
				"inlineData": map[string]any{
					"mimeType": "image/jpeg",
					"data":     base64.StdEncoding.EncodeToString(raw),
				},
			}},
		}},
	})
	if _, failure := exec.do(context.Background(), driverproto.Command[json.RawMessage]{Kind: "embeddings", Model: "google/embeddinggemma-2", Body: body}); failure != nil {
		t.Fatalf("unexpected failure: %+v", failure)
	}
	if !strings.HasPrefix(gotURL, "data:image/png;base64,") {
		t.Fatalf("expected aligned PNG data URI, got prefix %q", gotURL[:min(32, len(gotURL))])
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(gotURL, "data:image/png;base64,"))
	if err != nil {
		t.Fatal(err)
	}
	cfgImg, err := png.DecodeConfig(bytes.NewReader(decoded))
	if err != nil {
		t.Fatal(err)
	}
	if cfgImg.Width != 816 || cfgImg.Height != 768 {
		t.Fatalf("got aligned PNG %dx%d, want 816x768", cfgImg.Width, cfgImg.Height)
	}
}

package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"math"

	"golang.org/x/image/webp"
)

const (
	imageAlignEmbeddingGemma2 = "embeddinggemma2"
	gemma2PatchPixels         = 48
	gemma2ImageTokenBudget    = 280
	maxAlignedImagePixels     = 16_777_216
)

// alignEmbeddingGemma2Image decodes an inline JPEG/PNG/WebP image, applies EXIF
// orientation, scales it with Pillow-compatible bicubic resampling onto
// EmbeddingGemma 2's 280-soft-token 48px grid, and re-encodes it as RGB PNG.
func alignEmbeddingGemma2Image(base64Data, mimeType string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(base64Data)
	if err != nil {
		return "", err
	}
	cfg, err := decodeImageConfig(raw, mimeType)
	if err != nil {
		return "", err
	}
	if cfg.Width <= 0 || cfg.Height <= 0 || int64(cfg.Width)*int64(cfg.Height) > maxAlignedImagePixels {
		return "", errors.New("image exceeds the pixel limit")
	}
	img, err := decodeImage(raw, mimeType)
	if err != nil {
		return "", err
	}
	orientation := readEXIFOrientation(raw, mimeType)
	rgb, w, h := imageToRGB(img)
	if orientation >= 2 && orientation <= 8 {
		rgb, w, h = applyEXIFOrientation(rgb, w, h, orientation)
	}
	dstW, dstH := gemma2TargetSize(w, h, gemma2ImageTokenBudget)
	resized := resizeBicubicRGB(rgb, w, h, dstW, dstH)
	rgba := rgbToOpaqueRGBA(resized, dstW, dstH)

	var out bytes.Buffer
	enc := png.Encoder{CompressionLevel: png.BestSpeed}
	if err := enc.Encode(&out, rgba); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(out.Bytes()), nil
}

func decodeImageConfig(raw []byte, mimeType string) (image.Config, error) {
	r := bytes.NewReader(raw)
	switch mimeType {
	case "image/jpeg":
		return jpeg.DecodeConfig(r)
	case "image/png":
		return png.DecodeConfig(r)
	case "image/webp":
		return webp.DecodeConfig(r)
	default:
		return image.Config{}, errors.New("unsupported image MIME type")
	}
}

func decodeImage(raw []byte, mimeType string) (image.Image, error) {
	r := bytes.NewReader(raw)
	switch mimeType {
	case "image/jpeg":
		return jpeg.Decode(r)
	case "image/png":
		return png.Decode(r)
	case "image/webp":
		return webp.Decode(r)
	default:
		return nil, errors.New("unsupported image MIME type")
	}
}

func gemma2TargetSize(width, height, budget int) (int, int) {
	scale := math.Sqrt(float64(budget*gemma2PatchPixels*gemma2PatchPixels) / float64(width*height))
	tw := max(1, int(math.Floor(float64(width)*scale/gemma2PatchPixels)))
	th := max(1, int(math.Floor(float64(height)*scale/gemma2PatchPixels)))
	return min(tw, budget) * gemma2PatchPixels, min(th, budget) * gemma2PatchPixels
}

func imageToRGB(img image.Image) ([]byte, int, int) {
	b := img.Bounds()
	w, h := b.Dx(), b.Dy()
	out := make([]byte, w*h*3)
	switch src := img.(type) {
	case *image.NRGBA:
		for y := 0; y < h; y++ {
			row := src.Pix[(y+b.Min.Y-src.Rect.Min.Y)*src.Stride:]
			xOff := (b.Min.X - src.Rect.Min.X) * 4
			dstOff := y * w * 3
			for x := 0; x < w; x++ {
				s := xOff + x*4
				d := dstOff + x*3
				out[d] = row[s]
				out[d+1] = row[s+1]
				out[d+2] = row[s+2]
			}
		}
	case *image.RGBA:
		for y := 0; y < h; y++ {
			row := src.Pix[(y+b.Min.Y-src.Rect.Min.Y)*src.Stride:]
			xOff := (b.Min.X - src.Rect.Min.X) * 4
			dstOff := y * w * 3
			for x := 0; x < w; x++ {
				s := xOff + x*4
				d := dstOff + x*3
				a := row[s+3]
				if a == 255 || a == 0 {
					out[d] = row[s]
					out[d+1] = row[s+1]
					out[d+2] = row[s+2]
				} else {
					out[d] = uint8(min(255, int(row[s])*255/int(a)))
					out[d+1] = uint8(min(255, int(row[s+1])*255/int(a)))
					out[d+2] = uint8(min(255, int(row[s+2])*255/int(a)))
				}
			}
		}
	case *image.YCbCr:
		ycbcrToRGB(src, b, out)
	case *image.Gray:
		for y := 0; y < h; y++ {
			row := src.Pix[(y+b.Min.Y-src.Rect.Min.Y)*src.Stride:]
			xOff := b.Min.X - src.Rect.Min.X
			dstOff := y * w * 3
			for x := 0; x < w; x++ {
				v := row[xOff+x]
				d := dstOff + x*3
				out[d] = v
				out[d+1] = v
				out[d+2] = v
			}
		}
	default:
		for y := 0; y < h; y++ {
			dstOff := y * w * 3
			for x := 0; x < w; x++ {
				c := color.NRGBAModel.Convert(img.At(b.Min.X+x, b.Min.Y+y)).(color.NRGBA)
				d := dstOff + x*3
				out[d] = c.R
				out[d+1] = c.G
				out[d+2] = c.B
			}
		}
	}
	return out, w, h
}

// ycbcrToRGB converts YCbCr to RGB using libjpeg-compatible triangle (3/4, 1/4)
// chroma upsampling for subsampled JPEGs (4:2:0, 4:2:2, 4:4:0).
func ycbcrToRGB(src *image.YCbCr, b image.Rectangle, out []byte) {
	w, h := b.Dx(), b.Dy()
	hSub := src.SubsampleRatio == image.YCbCrSubsampleRatio420 || src.SubsampleRatio == image.YCbCrSubsampleRatio422
	vSub := src.SubsampleRatio == image.YCbCrSubsampleRatio420 || src.SubsampleRatio == image.YCbCrSubsampleRatio440
	if !hSub && !vSub {
		for y := 0; y < h; y++ {
			sy := y + b.Min.Y
			dstOff := y * w * 3
			for x := 0; x < w; x++ {
				sx := x + b.Min.X
				yi := src.YOffset(sx, sy)
				ci := src.COffset(sx, sy)
				r, g, bl := color.YCbCrToRGB(src.Y[yi], src.Cb[ci], src.Cr[ci])
				d := dstOff + x*3
				out[d], out[d+1], out[d+2] = r, g, bl
			}
		}
		return
	}
	minX, maxX := src.Rect.Min.X, src.Rect.Max.X-1
	minY, maxY := src.Rect.Min.Y, src.Rect.Max.Y-1
	for y := 0; y < h; y++ {
		sy := y + b.Min.Y
		cy0 := sy
		cy1 := sy
		if vSub {
			if (sy-minY)&1 == 0 {
				cy1 = max(minY, sy-1)
			} else {
				cy1 = min(maxY, sy+1)
			}
		}
		dstOff := y * w * 3
		for x := 0; x < w; x++ {
			sx := x + b.Min.X
			cx0 := sx
			cx1 := sx
			if hSub {
				if (sx-minX)&1 == 0 {
					cx1 = max(minX, sx-1)
				} else {
					cx1 = min(maxX, sx+1)
				}
			}
			var cb, cr uint8
			switch {
			case hSub && vSub:
				i00 := src.COffset(cx0, cy0)
				i10 := src.COffset(cx1, cy0)
				i01 := src.COffset(cx0, cy1)
				i11 := src.COffset(cx1, cy1)
				cb = uint8((9*int(src.Cb[i00]) + 3*int(src.Cb[i10]) + 3*int(src.Cb[i01]) + int(src.Cb[i11]) + 8) >> 4)
				cr = uint8((9*int(src.Cr[i00]) + 3*int(src.Cr[i10]) + 3*int(src.Cr[i01]) + int(src.Cr[i11]) + 8) >> 4)
			case hSub:
				i0 := src.COffset(cx0, cy0)
				i1 := src.COffset(cx1, cy0)
				cb = uint8((3*int(src.Cb[i0]) + int(src.Cb[i1]) + 2) >> 2)
				cr = uint8((3*int(src.Cr[i0]) + int(src.Cr[i1]) + 2) >> 2)
			default: // vSub
				i0 := src.COffset(cx0, cy0)
				i1 := src.COffset(cx0, cy1)
				cb = uint8((3*int(src.Cb[i0]) + int(src.Cb[i1]) + 2) >> 2)
				cr = uint8((3*int(src.Cr[i0]) + int(src.Cr[i1]) + 2) >> 2)
			}
			yi := src.YOffset(sx, sy)
			r, g, bl := color.YCbCrToRGB(src.Y[yi], cb, cr)
			d := dstOff + x*3
			out[d], out[d+1], out[d+2] = r, g, bl
		}
	}
}

func rgbToOpaqueRGBA(rgb []byte, w, h int) *image.RGBA {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for i, j := 0, 0; i < len(rgb); i, j = i+3, j+4 {
		img.Pix[j] = rgb[i]
		img.Pix[j+1] = rgb[i+1]
		img.Pix[j+2] = rgb[i+2]
		img.Pix[j+3] = 255
	}
	return img
}

// applyEXIFOrientation matches PIL.ImageOps.exif_transpose for EXIF orientations 2..8.
func applyEXIFOrientation(src []byte, w, h, orientation int) ([]byte, int, int) {
	dstW, dstH := w, h
	if orientation >= 5 {
		dstW, dstH = h, w
	}
	dst := make([]byte, dstW*dstH*3)
	for y := 0; y < dstH; y++ {
		for x := 0; x < dstW; x++ {
			var sx, sy int
			switch orientation {
			case 2: // flip horizontal
				sx, sy = w-1-x, y
			case 3: // rotate 180
				sx, sy = w-1-x, h-1-y
			case 4: // flip vertical
				sx, sy = x, h-1-y
			case 5: // transpose
				sx, sy = y, x
			case 6: // rotate 90 CW (PIL ROTATE_270)
				sx, sy = y, h-1-x
			case 7: // transverse
				sx, sy = w-1-y, h-1-x
			case 8: // rotate 270 CW / 90 CCW (PIL ROTATE_90)
				sx, sy = w-1-y, x
			}
			si := (sy*w + sx) * 3
			di := (y*dstW + x) * 3
			dst[di] = src[si]
			dst[di+1] = src[si+1]
			dst[di+2] = src[si+2]
		}
	}
	return dst, dstW, dstH
}

func readEXIFOrientation(raw []byte, mimeType string) int {
	switch mimeType {
	case "image/jpeg":
		return readJPEGEXIFOrientation(raw)
	case "image/png":
		return readPNGEXIFOrientation(raw)
	case "image/webp":
		return readWebPEXIFOrientation(raw)
	default:
		return 1
	}
}

func readJPEGEXIFOrientation(raw []byte) int {
	if len(raw) < 4 || raw[0] != 0xFF || raw[1] != 0xD8 {
		return 1
	}
	i := 2
	for i+4 <= len(raw) {
		for i < len(raw) && raw[i] != 0xFF {
			i++
		}
		for i < len(raw) && raw[i] == 0xFF {
			i++
		}
		if i >= len(raw) {
			return 1
		}
		marker := raw[i]
		i++
		// SOS (0xDA) starts entropy-coded scan data; EOI (0xD9) ends the image.
		if marker == 0xDA || marker == 0xD9 {
			return 1
		}
		if marker == 0x01 || (marker >= 0xD0 && marker <= 0xD7) {
			continue
		}
		if i+2 > len(raw) {
			return 1
		}
		segLen := int(binary.BigEndian.Uint16(raw[i : i+2]))
		if segLen < 2 || i+segLen > len(raw) {
			return 1
		}
		if marker == 0xE1 && segLen >= 8 && bytes.Equal(raw[i+2:i+8], []byte("Exif\x00\x00")) {
			if o := parseTIFFOrientation(raw[i+8 : i+segLen]); o >= 1 && o <= 8 {
				return o
			}
		}
		i += segLen
	}
	return 1
}

func readPNGEXIFOrientation(raw []byte) int {
	if len(raw) < 8 || !bytes.Equal(raw[:8], []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1A, '\n'}) {
		return 1
	}
	i := 8
	for i+12 <= len(raw) {
		chunkLen := int(binary.BigEndian.Uint32(raw[i : i+4]))
		chunkType := raw[i+4 : i+8]
		dataStart := i + 8
		dataEnd := dataStart + chunkLen
		if chunkLen < 0 || dataEnd+4 > len(raw) {
			return 1
		}
		if bytes.Equal(chunkType, []byte("eXIf")) {
			return parseTIFFOrientation(raw[dataStart:dataEnd])
		}
		if bytes.Equal(chunkType, []byte("IEND")) {
			return 1
		}
		i = dataEnd + 4
	}
	return 1
}

func readWebPEXIFOrientation(raw []byte) int {
	if len(raw) < 12 || !bytes.Equal(raw[0:4], []byte("RIFF")) || !bytes.Equal(raw[8:12], []byte("WEBP")) {
		return 1
	}
	i := 12
	for i+8 <= len(raw) {
		fourCC := raw[i : i+4]
		chunkLen := int(binary.LittleEndian.Uint32(raw[i+4 : i+8]))
		dataStart := i + 8
		dataEnd := dataStart + chunkLen
		if chunkLen < 0 || dataEnd > len(raw) {
			return 1
		}
		if bytes.Equal(fourCC, []byte("EXIF")) {
			payload := raw[dataStart:dataEnd]
			if len(payload) >= 6 && bytes.Equal(payload[:6], []byte("Exif\x00\x00")) {
				payload = payload[6:]
			}
			return parseTIFFOrientation(payload)
		}
		// RIFF chunks are padded to even byte offsets.
		i = dataEnd + (chunkLen & 1)
	}
	return 1
}

func parseTIFFOrientation(tiff []byte) int {
	if len(tiff) < 8 {
		return 1
	}
	var order binary.ByteOrder
	switch string(tiff[:2]) {
	case "II":
		order = binary.LittleEndian
	case "MM":
		order = binary.BigEndian
	default:
		return 1
	}
	if order.Uint16(tiff[2:4]) != 42 {
		return 1
	}
	ifdOff := int(order.Uint32(tiff[4:8]))
	if ifdOff < 8 || ifdOff+2 > len(tiff) {
		return 1
	}
	entries := int(order.Uint16(tiff[ifdOff : ifdOff+2]))
	pos := ifdOff + 2
	for idx := 0; idx < entries && pos+12 <= len(tiff); idx, pos = idx+1, pos+12 {
		tag := order.Uint16(tiff[pos : pos+2])
		typ := order.Uint16(tiff[pos+2 : pos+4])
		count := order.Uint32(tiff[pos+4 : pos+8])
		if tag == 0x0112 && typ == 3 && count >= 1 {
			val := int(order.Uint16(tiff[pos+8 : pos+10]))
			if val >= 1 && val <= 8 {
				return val
			}
			return 1
		}
	}
	return 1
}

// Pillow's bicubic filter kernel (Catmull-Rom, a = -0.5, support = 2.0).
func bicubicWeight(x float64) float64 {
	if x < 0 {
		x = -x
	}
	if x < 1.0 {
		return ((1.5*x-2.5)*x)*x + 1.0
	}
	if x < 2.0 {
		return ((-0.5*x+2.5)*x-4.0)*x + 2.0
	}
	return 0.0
}

type resampleTap struct {
	start   int
	weights []float64
}

func buildBicubicTaps(srcSize, dstSize int) []resampleTap {
	scale := float64(srcSize) / float64(dstSize)
	filterScale := max(1.0, scale)
	support := 2.0 * filterScale
	taps := make([]resampleTap, dstSize)
	for i := 0; i < dstSize; i++ {
		center := (float64(i) + 0.5) * scale
		xmin := max(0, int(math.Floor(center-support+0.5)))
		xmax := min(srcSize, int(math.Floor(center+support+0.5)))
		weights := make([]float64, max(1, xmax-xmin))
		var total float64
		for k := xmin; k < xmax; k++ {
			w := bicubicWeight((float64(k) - center + 0.5) / filterScale)
			weights[k-xmin] = w
			total += w
		}
		if total != 0 {
			inv := 1.0 / total
			for j := range weights {
				weights[j] *= inv
			}
		} else {
			xmin = min(srcSize-1, max(0, int(center)))
			weights = []float64{1.0}
		}
		taps[i] = resampleTap{start: xmin, weights: weights}
	}
	return taps
}

func clampByte(v float64) byte {
	r := math.Round(v)
	if r <= 0 {
		return 0
	}
	if r >= 255 {
		return 255
	}
	return byte(r)
}

// resizeBicubicRGB performs separable horizontal + vertical bicubic resampling
// matching PIL.Image.Resampling.BICUBIC (with anti-aliasing on downscale).
func resizeBicubicRGB(src []byte, srcW, srcH, dstW, dstH int) []byte {
	if srcW == dstW && srcH == dstH {
		out := make([]byte, len(src))
		copy(out, src)
		return out
	}
	hTaps := buildBicubicTaps(srcW, dstW)
	tmp := make([]float64, dstW*srcH*3)
	for y := 0; y < srcH; y++ {
		srcRow := y * srcW * 3
		dstRow := y * dstW * 3
		for x, tap := range hTaps {
			var r, g, b float64
			for j, w := range tap.weights {
				si := srcRow + (tap.start+j)*3
				r += float64(src[si]) * w
				g += float64(src[si+1]) * w
				b += float64(src[si+2]) * w
			}
			di := dstRow + x*3
			tmp[di] = r
			tmp[di+1] = g
			tmp[di+2] = b
		}
	}
	vTaps := buildBicubicTaps(srcH, dstH)
	dst := make([]byte, dstW*dstH*3)
	for y, tap := range vTaps {
		dstRow := y * dstW * 3
		for x := 0; x < dstW; x++ {
			var r, g, b float64
			for j, w := range tap.weights {
				si := ((tap.start+j)*dstW + x) * 3
				r += tmp[si] * w
				g += tmp[si+1] * w
				b += tmp[si+2] * w
			}
			di := dstRow + x*3
			dst[di] = clampByte(r)
			dst[di+1] = clampByte(g)
			dst[di+2] = clampByte(b)
		}
	}
	return dst
}

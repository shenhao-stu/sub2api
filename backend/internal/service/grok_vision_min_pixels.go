package service

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"strings"

	"github.com/tidwall/gjson"
	"golang.org/x/image/draw"
)

// Grok 4.7 rejects vision inputs under 512 total pixels. Replicate each pixel
// by an integer factor; keep the caller's original request for accounting.
func fitGrokTinyVisionInputs(body []byte) []byte {
	if !bytes.Contains(body, []byte("input_image")) {
		return body
	}
	var out bytes.Buffer
	last := 0
	// Bound transcoding work per request even when many distinct tiny images arrive.
	fitted := 0
	const maxFitted = 64
	gjson.GetBytes(body, "input").ForEach(func(_, item gjson.Result) bool {
		parts := item.Get("content")
		if item.Get("type").Str == "function_call_output" {
			parts = item.Get("output")
		}
		if !parts.IsArray() {
			return true
		}
		parts.ForEach(func(_, part gjson.Result) bool {
			if part.Get("type").Str != "input_image" {
				return true
			}
			url := part.Get("image_url")
			if fitted >= maxFitted || url.Type != gjson.String || url.Index <= last || url.Index+len(url.Raw) > len(body) || len(url.Raw) > 128<<10 {
				return true
			}
			updated := fitGrokTinyImageURL(url.Str)
			if updated == "" {
				return true
			}
			quoted, _ := json.Marshal(updated)
			out.Write(body[last:url.Index])
			out.Write(quoted)
			fitted++
			last = url.Index + len(url.Raw)
			return true
		})
		return true
	})
	if last == 0 {
		return body
	}
	out.Write(body[last:])
	return out.Bytes()
}

func fitGrokTinyImageURL(url string) string {
	header, data, ok := strings.Cut(url, ",")
	if !ok {
		return ""
	}
	var config func(io.Reader) (image.Config, error)
	var decode func(io.Reader) (image.Image, error)
	switch strings.ToLower(header) {
	case "data:image/png;base64":
		config = png.DecodeConfig
		decode = png.Decode
	case "data:image/jpeg;base64", "data:image/jpg;base64":
		config = jpeg.DecodeConfig
		decode = jpeg.Decode
	default:
		return ""
	}
	raw, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return ""
	}
	cfg, err := config(bytes.NewReader(raw))
	if err != nil || cfg.Width <= 0 || cfg.Height <= 0 || cfg.Width >= 512 || cfg.Height >= 512 || cfg.Width*cfg.Height >= 512 {
		return ""
	}
	// Conversion to PNG cannot preserve animation, EXIF orientation or custom
	// color profiles. Leave these images with their original renderer.
	for _, marker := range []string{"acTL", "eXIf", "iCCP", "gAMA", "cHRM", "Exif\x00\x00", "ICC_PROFILE\x00"} {
		if bytes.Contains(raw, []byte(marker)) {
			return ""
		}
	}
	img, err := decode(bytes.NewReader(raw))
	if err != nil {
		return ""
	}
	scale := 2
	for cfg.Width*cfg.Height*scale*scale < 512 {
		scale++
	}
	dst := image.NewNRGBA64(image.Rect(0, 0, cfg.Width*scale, cfg.Height*scale))
	draw.NearestNeighbor.Scale(dst, dst.Bounds(), img, img.Bounds(), draw.Src, nil)
	var encoded bytes.Buffer
	if err := png.Encode(&encoded, dst); err != nil {
		return ""
	}
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(encoded.Bytes())
}

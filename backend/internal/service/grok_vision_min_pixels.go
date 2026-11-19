package service

import (
	"bytes"
	"encoding/base64"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"strings"

	"golang.org/x/image/draw"
)

// Grok 4.7 rejects vision inputs under 512 total pixels. Replicate each pixel
// by an integer factor; keep the caller's original request for accounting.
func fitGrokTinyVisionInputs(items []any) {
	// Bound work on untrusted data URLs, including malformed images.
	bytesLeft, attemptsLeft := 2<<20, 64
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		parts, _ := item["content"].([]any)
		for _, rawPart := range parts {
			part, _ := rawPart.(map[string]any)
			if part["type"] != "input_image" && part["type"] != "image_url" {
				continue
			}
			holder, field := part, "image_url"
			if nested, ok := holder[field].(map[string]any); ok {
				holder, field = nested, "url"
			}
			url, ok := holder[field].(string)
			if !ok || len(url) > 128<<10 || len(url) > bytesLeft || len(url) < 5 || !strings.EqualFold(url[:5], "data:") {
				continue
			}
			bytesLeft -= len(url)
			if updated := fitGrokTinyImageURL(url); updated != "" {
				holder[field] = updated
			}
			attemptsLeft--
			if attemptsLeft == 0 {
				return
			}
		}
	}
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

package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"mime"
	"net/http"
	"strings"
)

const (
	publicModel = "nano-banana-2"
	flashModel  = "gemini-3.1-flash-image"
	maxImage    = 10 << 20
	maxRequest  = 20 << 20
	maxResponse = 24 << 20
)

type obj = map[string]any
type uploaded struct {
	MIME string
	Raw  []byte
}

// Validate both the header dimensions and the full image before accepting a source or result.
func verifyImage(mimeType string, raw []byte) (string, error) {
	if len(raw) == 0 || len(raw) > maxImage {
		return "", errors.New("image exceeds byte limit or is empty")
	}
	config, format, err := image.DecodeConfig(bytes.NewReader(raw))
	if err != nil || (format != "jpeg" && format != "png") || mimeType != "image/"+format {
		return "", errors.New("expected a PNG or JPEG matching its MIME type")
	}
	if config.Width <= 0 || config.Height <= 0 || config.Width > 8192 || config.Height > 8192 || int64(config.Width)*int64(config.Height) > 17_000_000 {
		return "", errors.New("image dimensions exceed limit")
	}
	if _, _, err = image.Decode(bytes.NewReader(raw)); err != nil {
		return "", errors.New("invalid image data")
	}
	return fmt.Sprintf("%dx%d", config.Width, config.Height), nil
}

func readLimited(reader io.Reader, max int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(reader, max+1))
	if err != nil || int64(len(b)) > max {
		return nil, errors.New("body incomplete or exceeds size limit")
	}
	return b, nil
}

func readRequest(r *http.Request, edit bool) (map[string]string, []uploaded, error) {
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return nil, nil, errors.New("invalid content type")
	}
	fields := map[string]string{}
	if !edit && mt == "application/json" {
		// Read tokens individually so duplicate fields cannot override material parameters.
		d := json.NewDecoder(r.Body)
		d.UseNumber()
		t, e := d.Token()
		if e != nil || t != json.Delim('{') {
			return nil, nil, errors.New("expected a JSON object")
		}
		for d.More() {
			k, e := d.Token()
			if e != nil {
				return nil, nil, errors.New("invalid JSON")
			}
			name := k.(string)
			if _, ok := fields[name]; ok {
				return nil, nil, errors.New("duplicate parameter")
			}
			var v any
			if err = d.Decode(&v); err != nil {
				return nil, nil, errors.New("invalid field")
			}
			switch v := v.(type) {
			case string:
				if name == "n" || name == "stream" {
					return nil, nil, errors.New("invalid JSON field type")
				}
				fields[name] = v
			case json.Number:
				if name != "n" {
					return nil, nil, errors.New("invalid JSON field type")
				}
				fields[name] = v.String()
			case bool:
				if name != "stream" {
					return nil, nil, errors.New("invalid JSON field type")
				}
				fields[name] = fmt.Sprint(v)
			default:
				return nil, nil, errors.New("unsupported parameter value")
			}
		}
		if t, e = d.Token(); e != nil || t != json.Delim('}') {
			return nil, nil, errors.New("incomplete JSON")
		}
		if _, e = d.Token(); e != io.EOF {
			return nil, nil, errors.New("trailing JSON")
		}
		return fields, nil, nil
	}
	if !edit || mt != "multipart/form-data" {
		return nil, nil, errors.New("use JSON for generation and multipart for edits")
	}
	reader, err := r.MultipartReader()
	if err != nil {
		return nil, nil, errors.New("invalid multipart boundary")
	}
	var images []uploaded
	for count := 0; ; count++ {
		part, e := reader.NextPart()
		if e == io.EOF {
			break
		}
		if e != nil || count >= 16 {
			return nil, nil, errors.New("invalid multipart")
		}
		name := part.FormName()
		if name == "image" || name == "image[]" {
			if len(images) >= 3 {
				return nil, nil, errors.New("at most three input images")
			}
			raw, e := readLimited(part, maxImage)
			if e != nil {
				return nil, nil, e
			}
			mimeType := part.Header.Get("Content-Type")
			// Some valid multipart clients use generic binary MIME. Still validate content bytes.
			if mimeType == "" || mimeType == "application/octet-stream" {
				mimeType = http.DetectContentType(raw)
			}
			if _, e = verifyImage(mimeType, raw); e != nil {
				return nil, nil, e
			}
			images = append(images, uploaded{mimeType, raw})
		} else {
			if part.FileName() != "" {
				return nil, nil, errors.New("only image files are accepted")
			}
			if _, ok := fields[name]; ok {
				return nil, nil, errors.New("duplicate parameter")
			}
			raw, e := readLimited(part, 16<<10)
			if e != nil {
				return nil, nil, e
			}
			fields[name] = string(raw)
		}
	}
	if len(images) == 0 {
		return nil, nil, errors.New("edits require an uploaded image")
	}
	return fields, images, nil
}

func buildNative(f map[string]string, images []uploaded) (obj, error) {
	for k := range f {
		switch k {
		case "model", "prompt", "n", "size", "image_size", "aspect_ratio", "response_format", "quality", "stream":
		default:
			return nil, errors.New("unsupported Images parameter")
		}
	}
	if f["model"] != publicModel {
		return nil, errors.New("only nano-banana-2 is available")
	}
	prompt := f["prompt"]
	if strings.TrimSpace(prompt) == "" || len(prompt) > 16<<10 {
		return nil, errors.New("a prompt of at most 16 KiB is required")
	}
	for k, accepted := range map[string]string{"n": "1", "response_format": "b64_json", "quality": "auto", "stream": "false"} {
		if v, exists := f[k]; exists && v != accepted {
			return nil, fmt.Errorf("only %s=%s is supported", k, accepted)
		}
	}
	ratio, tier := f["aspect_ratio"], f["image_size"]
	switch ratio {
	case "", "1:1", "2:3", "3:2", "3:4", "4:3", "9:16", "16:9", "4:5", "5:4", "21:9":
	default:
		return nil, errors.New("unsupported aspect_ratio")
	}
	switch tier {
	case "", "1K", "2K", "4K":
	default:
		return nil, errors.New("unsupported image_size")
	}
	if size, exists := f["size"]; exists && size != "auto" {
		sz, ok := map[string]string{"1024x1024": "1K", "2048x2048": "2K", "4096x4096": "4K"}[size]
		if !ok || (ratio != "" && ratio != "1:1") || (tier != "" && tier != sz) {
			return nil, errors.New("unsupported or conflicting size; use aspect_ratio plus image_size for rectangular output")
		}
		ratio, tier = "1:1", sz
	}
	if ratio == "" {
		ratio = "1:1"
	}
	if tier == "" {
		tier = "1K"
	}
	parts := make([]obj, 0, len(images)+1)
	for _, img := range images {
		parts = append(parts, obj{"inlineData": obj{"mimeType": img.MIME, "data": base64.StdEncoding.EncodeToString(img.Raw)}})
	}
	parts = append(parts, obj{"text": prompt})
	return obj{"contents": []obj{{"role": "user", "parts": parts}}, "generationConfig": obj{"candidateCount": 1, "responseModalities": []string{"TEXT", "IMAGE"}, "imageConfig": obj{"aspectRatio": ratio, "imageSize": tier}}}, nil
}

type part struct {
	Thought    bool
	InlineData *struct {
		MIME string `json:"mimeType"`
		Data string `json:"data"`
	} `json:"inlineData"`
}
type nativeResult struct {
	Candidates []struct {
		Finish  string `json:"finishReason"`
		Content struct {
			Role  string
			Parts []part
		}
	}
	Error          json.RawMessage
	PromptFeedback struct{ BlockReason string }
	UsageMetadata  map[string]json.RawMessage
}

func imageResult(raw []byte) (obj, error) {
	var n nativeResult
	if err := json.Unmarshal(raw, &n); err != nil || len(n.Error) > 0 || n.PromptFeedback.BlockReason != "" || len(n.Candidates) != 1 {
		return nil, errors.New("no unblocked final image candidate")
	}
	c := n.Candidates[0]
	if c.Finish != "STOP" || (c.Content.Role != "" && c.Content.Role != "model") {
		return nil, errors.New("provider did not return a completed model candidate")
	}
	var img obj
	for _, p := range c.Content.Parts {
		if p.Thought || p.InlineData == nil {
			continue
		}
		if img != nil {
			return nil, errors.New("provider returned multiple images for n=1")
		}
		data := p.InlineData.Data
		if len(data) > base64.StdEncoding.EncodedLen(maxImage) {
			return nil, errors.New("image exceeds size limit")
		}
		blob, e := base64.StdEncoding.Strict().DecodeString(data)
		if e != nil {
			return nil, errors.New("invalid provider base64")
		}
		size, e := verifyImage(p.InlineData.MIME, blob)
		if e != nil {
			return nil, e
		}
		img = obj{"b64_json": data, "size": size, "mime_type": p.InlineData.MIME}
	}
	if img == nil {
		return nil, errors.New("provider returned no final image bytes")
	}
	out := usageFields(n.UsageMetadata)
	out["data"] = []obj{img}
	return out, nil
}

func usageFields(native map[string]json.RawMessage) obj {
	out := obj{}
	valid := true
	// Preserve native counts even when no unambiguous OpenAI usage mapping exists.
	usage := map[string]int64{}
	for _, k := range []string{"promptTokenCount", "candidatesTokenCount", "thoughtsTokenCount", "cachedContentTokenCount", "toolUsePromptTokenCount", "totalTokenCount"} {
		if v, ok := native[k]; ok {
			var value int64
			if string(v) != "null" && json.Unmarshal(v, &value) == nil && value >= 0 {
				usage[k] = value
			} else {
				valid = false
			}
		}
	}
	if len(usage) > 0 {
		out["gemini_usage_metadata"] = usage
		p, hasPrompt := usage["promptTokenCount"]
		c, hasCandidate := usage["candidatesTokenCount"]
		total, hasTotal := usage["totalTokenCount"]
		thought := usage["thoughtsTokenCount"]
		cached := usage["cachedContentTokenCount"]
		if valid && hasPrompt && hasCandidate && hasTotal && p <= total && c <= total-p && thought == total-p-c && cached <= p && usage["toolUsePromptTokenCount"] == 0 {
			standard := obj{"input_tokens": p, "output_tokens": total - p, "total_tokens": total}
			if _, exists := usage["cachedContentTokenCount"]; exists {
				standard["input_tokens_details"] = obj{"cached_tokens": cached}
			}
			if _, exists := usage["thoughtsTokenCount"]; exists {
				standard["output_tokens_details"] = obj{"reasoning_tokens": thought}
			}
			out["usage"] = standard
		}
	}
	return out
}

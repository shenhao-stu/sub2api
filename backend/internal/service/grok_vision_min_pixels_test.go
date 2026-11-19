package service

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func pixelTestPNG(t *testing.T, width, height int) string {
	t.Helper()
	img := image.NewNRGBA(image.Rect(0, 0, width, height))
	img.SetNRGBA(0, 0, color.NRGBA{R: 177, G: 55, B: 66, A: 255})
	var b bytes.Buffer
	if err := png.Encode(&b, img); err != nil {
		t.Fatal(err)
	}
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(b.Bytes())
}

func TestGrokVisionMinPixels(t *testing.T) {
	small := pixelTestPNG(t, 16, 16)
	edge := pixelTestPNG(t, 16, 32)
	url := "https://example.invalid/private-image"
	content := []map[string]string{{"type": "input_image", "image_url": small, "detail": "high"}, {"type": "input_image", "image_url": edge}, {"type": "input_image", "image_url": url}, {"type": "input_text", "text": small}}
	for _, model := range []string{"grok-4.7", "grok-4.6"} {
		raw, _ := json.Marshal(map[string]any{"input": []any{map[string]any{"role": "user", "content": content}, map[string]any{"type": "function_call_output", "call_id": "call_test", "output": content}}})
		saved := bytes.Clone(raw)
		patched, err := patchGrokResponsesBody(raw, model)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(raw, saved) {
			t.Fatal("mutated original request")
		}
		for _, path := range []string{"input.0.content", "input.1.output"} {
			val := gjson.GetBytes(patched, path+".0.image_url").String()
			if model == "grok-4.7" {
				data, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(val, "data:image/png;base64,"))
				if err != nil {
					t.Fatal(err)
				}
				img, err := png.Decode(bytes.NewReader(data))
				if err != nil {
					t.Fatal(err)
				}
				if img.Bounds().Dx() != 32 || img.Bounds().Dy() != 32 {
					t.Fatalf("16x16 input reaches Grok as %v", img.Bounds())
				}
				if img.At(0, 0) != img.At(1, 1) || color.NRGBAModel.Convert(img.At(0, 0)).(color.NRGBA) != (color.NRGBA{177, 55, 66, 255}) {
					t.Fatal("source pixel not replicated")
				}
			} else if val != small {
				t.Fatal("changed another model")
			}
			if gjson.GetBytes(patched, path+".0.detail").String() != "high" || gjson.GetBytes(patched, path+".1.image_url").String() != edge || gjson.GetBytes(patched, path+".2.image_url").String() != url || gjson.GetBytes(patched, path+".3.text").String() != small {
				t.Fatal("changed unrelated content")
			}
		}
	}
}

func TestGrokVisionTinyImageFormats(t *testing.T) {
	img := image.NewNRGBA(image.Rect(0, 0, 1, 1))
	var jpegData bytes.Buffer
	if err := jpeg.Encode(&jpegData, img, nil); err != nil {
		t.Fatal(err)
	}
	tiny := "data:image/jpeg;base64," + base64.StdEncoding.EncodeToString(jpegData.Bytes())
	url := fitGrokTinyImageURL(tiny)
	b, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(url, "data:image/png;base64,"))
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := png.DecodeConfig(bytes.NewReader(b))
	if err != nil || cfg.Width != 23 || cfg.Height != 23 {
		t.Fatalf("1x1 JPEG output: %+v %v", cfg, err)
	}
	for _, s := range []string{"data:image/png;base64,broken!", "data:image/png;base64,AA==", "data:image/gif;base64,R0lG", "https://example.invalid/image", pixelTestPNG(t, 32, 16)} {
		if fitGrokTinyImageURL(s) != "" {
			t.Fatal("unexpected rewrite")
		}
	}
}

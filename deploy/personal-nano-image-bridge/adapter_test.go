package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

func sampleImage(t *testing.T, format string) []byte {
	t.Helper()
	var b bytes.Buffer
	img := image.NewRGBA(image.Rect(0, 0, 24, 12))
	img.Set(7, 4, color.RGBA{10, 20, 50, 255})
	var err error
	if format == "jpeg" {
		err = jpeg.Encode(&b, img, nil)
	} else {
		err = png.Encode(&b, img)
	}
	if err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func resultBytes(t *testing.T, img []byte, mt string) []byte {
	t.Helper()
	v := obj{"candidates": []obj{{"finishReason": "STOP", "content": obj{"role": "model", "parts": []obj{
		{"thought": true, "inlineData": obj{"mimeType": "image/png", "data": "this must be skipped"}},
		{"text": "description"}, {"inlineData": obj{"mimeType": mt, "data": base64.StdEncoding.EncodeToString(img)}},
	}}}}, "usageMetadata": obj{"promptTokenCount": 17, "totalTokenCount": 33, "irrelevant": "secret"}}
	b, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func mockBridge(t *testing.T, handler http.HandlerFunc) *bridge {
	t.Helper()
	s := httptest.NewServer(handler)
	t.Cleanup(s.Close)
	return &bridge{upstream: s.URL, stateDir: t.TempDir(), slots: make(chan struct{}, 2), client: oneShotClient(), routes: []route{{"pro-key", "cp", "gemini-3.1-flash-image"}, {"ultra-key", "cu", "ultra/gemini-3.1-flash-image"}}}
}
func call(b *bridge, method, path, key, mt, id string, body []byte) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, bytes.NewReader(body))
	if key != "" {
		r.Header.Set("Authorization", "Bearer "+key)
	}
	if mt != "" {
		r.Header.Set("Content-Type", mt)
	}
	if id != "" {
		r.Header.Set("X-Request-ID", id)
	}
	w := httptest.NewRecorder()
	b.ServeHTTP(w, r)
	return w
}

const gen = "/v1/images/generations"
const edit = "/v1/images/edits"
const minimal = `{"model":"nano-banana-2","prompt":"draw a dot"}`

func TestProviderAndActualOutput(t *testing.T) {
	for _, tc := range []struct{ key, target, auth, format string }{{"pro-key", flashModel, "cp", "jpeg"}, {"ultra-key", "ultra/" + flashModel, "cu", "png"}} {
		t.Run(tc.key, func(t *testing.T) {
			img := sampleImage(t, tc.format)
			wire := resultBytes(t, img, "image/"+tc.format)
			var calls atomic.Int32
			b := mockBridge(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != "POST" || r.URL.Path != "/v1beta/models/"+tc.target+":generateContent" || r.Header.Get("Authorization") != "Bearer "+tc.auth {
					t.Error("wrong native route")
				}
				if r.Header.Get("Idempotency-Key") != "" || r.Header.Get("X-Request-ID") == "" {
					t.Error("unexpected outbound headers")
				}
				var q struct {
					Contents         []struct{ Parts []struct{ Text string } }
					GenerationConfig struct {
						ImageConfig        struct{ AspectRatio, ImageSize string }
						ResponseModalities []string
					}
				}
				if json.NewDecoder(r.Body).Decode(&q) != nil || len(q.Contents) != 1 || q.Contents[0].Parts[0].Text != "draw a dot" || q.GenerationConfig.ImageConfig.ImageSize != "1K" || q.GenerationConfig.ImageConfig.AspectRatio != "1:1" || strings.Join(q.GenerationConfig.ResponseModalities, ",") != "TEXT,IMAGE" {
					t.Error("wrong payload")
				}
				w.Header().Set("X-Cpa-Trace-Id", "trace-abc")
				w.Write(wire)
			})
			w := call(b, "POST", gen, tc.key, "application/json", "new-id", []byte(minimal))
			if w.Code != 200 || w.Header().Get("X-Cpa-Trace-Id") != "trace-abc" {
				t.Fatalf("%d %s", w.Code, w.Body)
			}
			var out struct {
				Data []struct {
					B64  string `json:"b64_json"`
					Size string
				}
				Usage map[string]int `json:"gemini_usage_metadata"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
				t.Fatal(err)
			}
			got, e := base64.StdEncoding.DecodeString(out.Data[0].B64)
			if e != nil || sha256.Sum256(got) != sha256.Sum256(img) || out.Data[0].Size != "24x12" || len(out.Usage) != 2 || out.Usage["promptTokenCount"] != 17 {
				t.Fatal("image/size or native-count evidence changed")
			}
			again := call(b, "POST", gen, tc.key, "application/json", "new-id", []byte(minimal))
			if again.Code != 409 || calls.Load() != 1 {
				t.Fatal("replayed")
			}
			clone := *b
			afterRestart := call(&clone, "POST", gen, "ultra-key", "application/json", "new-id", []byte(minimal))
			if afterRestart.Code != 409 || calls.Load() != 1 {
				t.Fatal("restart/cross-pool replay")
			}
		})
	}
}
func TestModelsAndPredispatchErrors(t *testing.T) {
	var calls atomic.Int32
	b := mockBridge(t, func(http.ResponseWriter, *http.Request) { calls.Add(1) })
	for _, key := range []string{"", "wrong", "pro-key", "ultra-key"} {
		w := call(b, "GET", "/v1/models", key, "", "", nil)
		if key == "" || key == "wrong" {
			if w.Code != 401 {
				t.Fatal(w.Code)
			}
		} else if w.Code != 200 || !strings.Contains(w.Body.String(), publicModel) || strings.Contains(w.Body.String(), "nano-banana-pro") {
			t.Fatal(w.Body.String())
		}
	}
	fields := []string{`,"image_url":"http://127.0.0.1"`, `,"mask":"file:/tmp/mask"`, `,"n":2`, `,"response_format":"url"`, `,"quality":"high"`, `,"size":"1536x1024"`, `,"aspect_ratio":"2:1"`, `,"image_size":"8K"`, `,"size":"1024x1024","aspect_ratio":"16:9"`, `,"size":"2048x2048","image_size":"1K"`, `,"stream":true`, `,"background":"transparent"`, `,"output_format":"png"`, `,"n":"1"`, `,"n":1,"n":1`, `,"seed":0`, `,"prompt":""`, `,"model":"nano-banana-pro"`}
	for _, f := range fields {
		w := call(b, "POST", gen, "pro-key", "application/json", "", []byte(strings.TrimSuffix(minimal, "}")+f+"}"))
		if w.Code != 400 {
			t.Errorf("%s status=%d", f, w.Code)
		}
	}
	for _, raw := range []string{"null", "[]", "{}", "{", minimal + " 1", strings.Replace(minimal, publicModel, "nano-banana-pro", 1)} {
		w := call(b, "POST", gen, "pro-key", "application/json", "", []byte(raw))
		if w.Code != 400 {
			t.Errorf("%q=%d", raw, w.Code)
		}
	}
	if w := call(b, "POST", edit, "pro-key", "application/json", "", []byte(minimal)); w.Code != 400 {
		t.Fatal("edits accepted JSON")
	}
	if w := call(b, "POST", gen+"?upstream=https://example.org", "pro-key", "application/json", "", []byte(minimal)); w.Code != 400 {
		t.Fatal("accepted URL override")
	}
	b.slots <- struct{}{}
	b.slots <- struct{}{}
	if w := call(b, "POST", gen, "pro-key", "application/json", "", []byte(minimal)); w.Code != 429 {
		t.Fatal(w.Code)
	}
	if calls.Load() != 0 {
		t.Fatal("invalid client calls reached upstream")
	}
}
func editBody(t *testing.T, images [][]byte, additional map[string]string) ([]byte, string) {
	t.Helper()
	var body bytes.Buffer
	m := multipart.NewWriter(&body)
	for k, v := range map[string]string{"model": publicModel, "prompt": "preserve both reference images", "aspect_ratio": "16:9", "image_size": "2K"} {
		if e := m.WriteField(k, v); e != nil {
			t.Fatal(e)
		}
	}
	for k, v := range additional {
		if e := m.WriteField(k, v); e != nil {
			t.Fatal(e)
		}
	}
	for i, img := range images {
		part, e := m.CreateFormFile("image[]", fmt.Sprintf("ref%d", i))
		if e != nil {
			t.Fatal(e)
		}
		part.Write(img)
	}
	if e := m.Close(); e != nil {
		t.Fatal(e)
	}
	return body.Bytes(), m.FormDataContentType()
}
func TestEditPreservesOrderAndConfig(t *testing.T) {
	jpg, png := sampleImage(t, "jpeg"), sampleImage(t, "png")
	wire := resultBytes(t, jpg, "image/jpeg")
	var calls atomic.Int32
	b := mockBridge(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var doc struct {
			Contents         []struct{ Parts []part }
			GenerationConfig struct {
				ImageConfig struct{ AspectRatio, ImageSize string }
			}
		}
		if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
			t.Error(err)
			return
		}
		p := doc.Contents[0].Parts
		if len(p) != 3 || p[0].InlineData.MIME != "image/jpeg" || p[1].InlineData.MIME != "image/png" || p[0].InlineData.Data != base64.StdEncoding.EncodeToString(jpg) || p[1].InlineData.Data != base64.StdEncoding.EncodeToString(png) || doc.GenerationConfig.ImageConfig.AspectRatio != "16:9" || doc.GenerationConfig.ImageConfig.ImageSize != "2K" {
			t.Error("wrong edit inputs")
		}
		w.Write(wire)
	})
	body, mt := editBody(t, [][]byte{jpg, png}, nil)
	if w := call(b, "POST", edit, "pro-key", mt, "", body); w.Code != 200 {
		t.Fatal(w.Code, w.Body)
	}
	for _, imgs := range [][][]byte{nil, {[]byte("not png")}, {jpg, jpg, jpg, jpg}} {
		body, mt = editBody(t, imgs, nil)
		if w := call(b, "POST", edit, "pro-key", mt, "", body); w.Code != 400 {
			t.Error("accepted unsupported input")
		}
	}
	body, mt = editBody(t, [][]byte{jpg}, map[string]string{"mask": "http://untrusted"})
	if w := call(b, "POST", edit, "pro-key", mt, "", body); w.Code != 400 {
		t.Error("accepted mask")
	}
	if calls.Load() != 1 {
		t.Fatal(calls.Load())
	}
}
func TestFinalImageEvidence(t *testing.T) {
	raw := resultBytes(t, sampleImage(t, "png"), "image/png")
	cases := []string{"{}", strings.Replace(string(raw), "\"STOP\"", "\"MAX_TOKENS\"", 1), strings.Replace(string(raw), "\"role\":\"model\"", "\"role\":\"user\"", 1), strings.Replace(string(raw), "\"image/png\"", "\"image/jpeg\"", 2), strings.Replace(string(raw), "iVBORw", "AAAAAA", 1)}
	for _, v := range cases {
		if _, e := imageResult([]byte(v)); e == nil {
			t.Errorf("accepted invalid image: %.180s", v)
		}
	}
	for _, v := range []string{`{"candidates":[{"finishReason":"STOP","content":{"role":"model","parts":[{"text":"placeholder"}]}}]}`, `{"candidates":[{"finishReason":"STOP","content":{"parts":[{"thought":true,"inlineData":{"mimeType":"image/png","data":"AAAA"}}]}}]}`} {
		if _, e := imageResult([]byte(v)); e == nil {
			t.Errorf("accepted non-image")
		}
	}
	if _, e := verifyImage("image/jpeg", sampleImage(t, "jpeg")[:25]); e == nil {
		t.Fatal("accepted truncated JPEG")
	}
}
func TestRedirectAndErrorsNeverReplay(t *testing.T) {
	var calls atomic.Int32
	for _, status := range []int{307, 429, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			b := mockBridge(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Location", "/elsewhere")
				w.WriteHeader(status)
				io.WriteString(w, "SECRET_PROVIDER_ERROR")
			})
			before := calls.Load()
			w := call(b, "POST", gen, "pro-key", "application/json", "claimed-id", []byte(minimal))
			if w.Code < 400 || strings.Contains(w.Body.String(), "SECRET_PROVIDER_ERROR") || calls.Load() != before+1 {
				t.Fatal(w.Code, calls.Load())
			}
			if r := call(b, "POST", gen, "pro-key", "application/json", "claimed-id", []byte(minimal)); r.Code != 409 || calls.Load() != before+1 {
				t.Fatal("failed intent replayed")
			}
		})
	}
}
func TestOnlyPrivateNumericAddressesAndDurableClaim(t *testing.T) {
	for _, s := range []string{"localhost:80", "0.0.0.0:81", "8.8.8.8:80", "http://127.0.0.1:80", "user@127.0.0.1:80", "[::1]:80"} {
		if internalAddress(s) {
			t.Error(s)
		}
	}
	if !internalAddress("172.17.0.1:57242") || !internalAddress("127.0.0.1:57240") {
		t.Fatal("valid private addresses rejected")
	}
	dir := t.TempDir()
	if e := claim(dir, "abc"); e != nil {
		t.Fatal(e)
	}
	files, _ := os.ReadDir(dir)
	if len(files) != 1 || len(files[0].Name()) != 64 {
		t.Fatal("bad replay marker")
	}
}

func TestOnlyReconciledUsageIsMapped(t *testing.T) {
	wire := resultBytes(t, sampleImage(t, "jpeg"), "image/jpeg")
	for _, tc := range []struct {
		counts string
		want   bool
	}{{`{"promptTokenCount":17,"candidatesTokenCount":16,"totalTokenCount":33}`, true}, {`{"promptTokenCount":17,"candidatesTokenCount":14,"thoughtsTokenCount":2,"cachedContentTokenCount":2,"totalTokenCount":33}`, true}, {`{"promptTokenCount":17,"candidatesTokenCount":15,"totalTokenCount":33}`, false}, {`{"promptTokenCount":17,"totalTokenCount":33}`, false}, {`{"promptTokenCount":17,"candidatesTokenCount":16,"cachedContentTokenCount":18,"totalTokenCount":33}`, false}} {
		var doc map[string]json.RawMessage
		json.Unmarshal(wire, &doc)
		doc["usageMetadata"] = json.RawMessage(tc.counts)
		b, _ := json.Marshal(doc)
		out, e := imageResult(b)
		if e != nil {
			t.Fatal(e)
		}
		v, ok := out["usage"]
		if ok != tc.want {
			t.Fatal(tc.counts, ok)
		}
		if ok && (v.(obj)["total_tokens"] != int64(33) || v.(obj)["output_tokens"] != int64(16)) {
			t.Fatal(v)
		}
	}
}

func TestPreserveMeasuredUsageWhenNoImageDelivered(t *testing.T) {
	for _, status := range []int{200, 502} {
		b := mockBridge(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			io.WriteString(w, `{"usageMetadata":{"promptTokenCount":15,"candidatesTokenCount":5,"totalTokenCount":20},"error":{"message":"SECRET_DETAIL"},"candidates":[]}`)
		})
		w := call(b, "POST", gen, "pro-key", "application/json", "", []byte(minimal))
		var out struct {
			Data  json.RawMessage
			Usage struct {
				Total int `json:"total_tokens"`
			}
			Error json.RawMessage
		}
		if json.Unmarshal(w.Body.Bytes(), &out) != nil || w.Code != 502 || out.Usage.Total != 20 || out.Data != nil || out.Error == nil || strings.Contains(w.Body.String(), "SECRET_DETAIL") {
			t.Fatal("lost native failed-response metering or leaked body", w.Body.String())
		}
	}
}

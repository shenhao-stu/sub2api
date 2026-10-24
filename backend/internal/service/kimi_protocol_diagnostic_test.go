package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

type kimiDiagnosticUpstream struct {
	calls             int
	body              []byte
	path              string
	rejectTemperature bool
	headers           http.Header
}

func (u *kimiDiagnosticUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	u.calls++
	u.body, _ = io.ReadAll(req.Body)
	u.path = req.URL.Path
	status := 200
	body := `{"id":"synthetic","object":"chat.completion","choices":[{"message":{"role":"assistant","content":"synthetic"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1000,"completion_tokens":100,"total_tokens":1100,"prompt_tokens_details":{"cached_tokens":300,"cache_write_tokens":200},"completion_tokens_details":{"reasoning_tokens":80}}}`
	if raw := u.headers.Get("X-Fixture-Usage"); raw != "" {
		body = `{"id":"synthetic","object":"chat.completion","choices":[{"message":{"role":"assistant","content":"synthetic"},"finish_reason":"stop"}],"usage":` + raw + `}`
	}
	if temp := gjson.GetBytes(u.body, "temperature"); u.rejectTemperature && temp.Exists() && temp.Float() != 1 {
		status = 400
		body = `{"error":{"type":"invalid_request_error","message":"temperature must be 1 when thinking is enabled","param":"temperature"}}`
	}
	headers := u.headers.Clone()
	if headers == nil {
		headers = make(http.Header)
	}
	headers.Set("Content-Type", "application/json")
	if status == 200 && gjson.GetBytes(u.body, "stream").Bool() {
		headers.Set("Content-Type", "text/event-stream")
		usage := gjson.Get(body, "usage").Raw
		body = "data: {\"id\":\"synthetic\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"synthetic\"},\"finish_reason\":null}]}\n\n" +
			"data: {\"id\":\"synthetic\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
			"data: {\"id\":\"synthetic\",\"choices\":[],\"usage\":" + usage + "}\n\n" +
			"data: {\"id\":\"synthetic\",\"choices\":[],\"usage\":" + usage + "}\n\ndata: [DONE]\n\n"
	}
	return &http.Response{StatusCode: status, Header: headers, Body: io.NopCloser(strings.NewReader(body))}, nil
}
func (u *kimiDiagnosticUpstream) DoWithTLS(r *http.Request, p string, a int64, n int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(r, p, a, n)
}

func kimiDiagnosticForward(t *testing.T, model, endpoint, extra string, reject bool, headers http.Header, receipt ...bool) (*OpenAIForwardResult, error, *httptest.ResponseRecorder, *kimiDiagnosticUpstream) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	message := `[{"role":"user","content":"synthetic"}]`
	body := []byte(fmt.Sprintf(`{"model":%q,"max_tokens":512,"messages":%s%s}`, model, message, extra))
	if endpoint == "/v1/responses" {
		body = []byte(fmt.Sprintf(`{"model":%q,"input":"synthetic"%s}`, model, extra))
	}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if len(receipt) > 0 && receipt[0] {
		c.Request.Header.Set(ChatReceiptVersionHeader, "v2")
		c.Request.Header.Set(ChatReceiptIDHeader, "2c4e888b-836b-4992-9ff1-dc1699488902")
	}
	upstream := &kimiDiagnosticUpstream{rejectTemperature: reject, headers: headers}
	cfg := &config.Config{}
	billing := NewBillingService(cfg, nil)
	svc := &OpenAIGatewayService{cfg: cfg, billingService: billing, resolver: NewModelPricingResolver(nil, billing), httpUpstream: upstream}
	price := 0.000001
	gid := int64(41)
	c.Set("api_key", &APIKey{ID: 254, GroupID: &gid, Group: &Group{ID: gid, RateMultiplier: 1, ModelPricing: []ChannelModelPricing{{Models: []string{model}, BillingMode: BillingModeToken, InputPrice: &price, OutputPrice: &price}}}})
	account := &Account{ID: 425, Platform: PlatformKimi, Type: AccountTypeAPIKey, Concurrency: 1, Credentials: map[string]any{"api_key": "fixture-not-secret", "base_url": "https://synthetic.invalid", "api_protocol": "chat_completions"}}
	var result *OpenAIForwardResult
	var err error
	if endpoint == "/v1/messages" {
		result, err = svc.ForwardAsAnthropic(context.Background(), c, account, body, "", "")
	} else if endpoint == "/v1/responses" {
		result, err = svc.Forward(context.Background(), c, account, body)
	} else {
		result, err = svc.ForwardAsChatCompletions(context.Background(), c, account, body, "", "")
	}
	return result, err, rec, upstream
}

func TestKimiDiagnosticFixedSamplingForward(t *testing.T) {
	for _, model := range []string{"kimi-k3", "kimi-k3.1-pro", "kimi-k3.1-pro-highspeed"} {
		for _, endpoint := range []string{"/v1/chat/completions", "/v1/messages"} {
			for _, tc := range []struct {
				name, extra string
				bad         bool
			}{{"omitted", "", false}, {"one", `,"temperature":1`, false}, {"non_one", `,"temperature":0.2`, true}} {
				t.Run(model+endpoint+"/"+tc.name, func(t *testing.T) {
					result, err, rec, up := kimiDiagnosticForward(t, model, endpoint, tc.extra, true, nil)
					if up.calls != 1 || up.path != "/v1/chat/completions" {
						t.Fatalf("dispatch mismatch calls=%d path=%s err=%v", up.calls, up.path, err)
					}
					temp := gjson.GetBytes(up.body, "temperature")
					if tc.name == "omitted" && temp.Exists() {
						t.Fatal("adapter invented sampling parameter")
					}
					if tc.name != "omitted" && (!temp.Exists() || temp.Float() != map[bool]float64{true: 0.2, false: 1}[tc.bad]) {
						t.Fatal("adapter changed explicit sampling parameter")
					}
					if tc.bad {
						if err == nil || rec.Code != 400 || result != nil {
							t.Fatalf("synthetic upstream rejection not preserved: status=%d result=%v err=%v", rec.Code, result, err)
						}
						return
					}
					if err != nil || rec.Code != 200 || result == nil {
						t.Fatalf("valid fixture failed: status=%d err=%v", rec.Code, err)
					}
					u := result.Usage
					if u.InputTokens != 1000 || u.CacheReadInputTokens != 300 || u.CacheCreationInputTokens != 200 || u.OutputTokens != 100 {
						t.Fatalf("metering mismatch %+v", u)
					}
					if endpoint == "/v1/messages" {
						if gjson.Get(rec.Body.String(), "usage.input_tokens").Int() != 500 || gjson.Get(rec.Body.String(), "usage.output_tokens").Int() != 100 {
							t.Fatal("Messages inclusive conversion broke token conservation")
						}
					}
					t.Logf("public adapter dispatch=%d; sampling_preserved=true; input=1000 cache_read=300 write=200 ordinary=500 output_including_reasoning=100", up.calls)
				})
			}
		}
	}
}

func TestKimiDiagnosticCacheTTLRequestPreservation(t *testing.T) {
	for _, endpoint := range []string{"/v1/chat/completions", "/v1/messages"} {
		t.Run(endpoint, func(t *testing.T) {
			_, err, _, up := kimiDiagnosticForward(t, "kimi-k3", endpoint, map[string]string{"/v1/chat/completions": `,"prompt_cache_options":{"mode":"implicit","ttl":"1h"}`, "/v1/messages": `,"cache_control":{"type":"ephemeral","ttl":"1h"}`}[endpoint], false, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !gjson.GetBytes(up.body, "prompt_cache_options").Exists() {
				t.Fatal("explicit cache_control ttl=1h disappeared before upstream dispatch")
			}
			if gjson.GetBytes(up.body, "prompt_cache_options.ttl").String() != "1h" {
				t.Fatal("explicit TTL changed")
			}
		})
	}
}

func TestKimiDiagnosticHeaderTTLRetention(t *testing.T) {
	for _, endpoint := range []string{"/v1/chat/completions", "/v1/messages"} {
		t.Run(endpoint, func(t *testing.T) {
			headers := http.Header{"Msh-Usage-Cache-Write-Tokens-5m": []string{"75"}, "Msh-Usage-Cache-Write-Tokens-1h": []string{"125"}}
			result, err, rec, up := kimiDiagnosticForward(t, "kimi-k3", endpoint, "", false, headers)
			if err != nil {
				t.Fatal(err)
			}
			if up.calls != 1 {
				t.Fatal("duplicate dispatch")
			}
			encoded, _ := json.Marshal(result.Usage)
			if gjson.GetBytes(encoded, "cache_creation_5m_tokens").Int() != 75 || gjson.GetBytes(encoded, "cache_creation_1h_tokens").Int() != 125 {
				t.Errorf("authoritative header TTL split missing from forwarding usage")
			}
			if gjson.Get(rec.Body.String(), "usage.cache_creation.ephemeral_5m_input_tokens").Int() != 75 || gjson.Get(rec.Body.String(), "usage.cache_creation.ephemeral_1h_input_tokens").Int() != 125 {
				t.Error("authoritative header TTL split missing from downstream usage")
			}
		})
	}
}

func TestKimiCacheWriteHeaderBoundary(t *testing.T) {
	for _, tc := range []struct {
		name    string
		headers http.Header
		known   bool
	}{
		{"complete", http.Header{"Msh-Usage-Cache-Write-Tokens-5m": {"75"}, "Msh-Usage-Cache-Write-Tokens-1h": {"125"}}, true},
		{"missing", nil, false},
		{"partial", http.Header{"Msh-Usage-Cache-Write-Tokens-5m": {"200"}}, false},
		{"conflicting_sum", http.Header{"Msh-Usage-Cache-Write-Tokens-5m": {"75"}, "Msh-Usage-Cache-Write-Tokens-1h": {"126"}}, false},
		{"negative", http.Header{"Msh-Usage-Cache-Write-Tokens-5m": {"-1"}, "Msh-Usage-Cache-Write-Tokens-1h": {"201"}}, false},
		{"overflow", http.Header{"Msh-Usage-Cache-Write-Tokens-5m": {"9223372036854775808"}, "Msh-Usage-Cache-Write-Tokens-1h": {"0"}}, false},
		{"duplicate", http.Header{"Msh-Usage-Cache-Write-Tokens-5m": {"75", "75"}, "Msh-Usage-Cache-Write-Tokens-1h": {"125"}}, false},
		{"duplicate_case", http.Header{"Msh-Usage-Cache-Write-Tokens-5m": {"75"}, "msh-usage-cache-write-tokens-5m": {"75"}, "Msh-Usage-Cache-Write-Tokens-1h": {"125"}}, false},
		{"decimal", http.Header{"Msh-Usage-Cache-Write-Tokens-5m": {"75.0"}, "Msh-Usage-Cache-Write-Tokens-1h": {"125"}}, false},
	} {
		for _, endpoint := range []string{"/v1/chat/completions", "/v1/messages", "/v1/responses"} {
			for _, stream := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s%s/stream=%t", tc.name, endpoint, stream), func(t *testing.T) {
					extra := ""
					if stream {
						extra = `,"stream":true`
					}
					result, err, rec, up := kimiDiagnosticForward(t, "kimi-k3", endpoint, extra, false, tc.headers)
					if err != nil || result == nil {
						t.Fatalf("forward: status=%d err=%v", rec.Code, err)
					}
					if up.calls != 1 {
						t.Fatal("repeated source dispatch")
					}
					u := result.Usage
					if u.InputTokens != 1000 || u.CacheCreationInputTokens != 200 || u.CacheReadInputTokens != 300 || u.OutputTokens != 100 {
						t.Fatalf("real usage changed: %+v", u)
					}
					if u.CacheCreationTTLKnown != tc.known {
						t.Fatalf("TTL known=%t want %t", u.CacheCreationTTLKnown, tc.known)
					}
					if tc.known {
						if u.CacheCreation5mTokens != 75 || u.CacheCreation1hTokens != 125 || !strings.Contains(rec.Body.String(), `"ephemeral_1h_input_tokens":125`) {
							t.Fatalf("split missing; counts=%d/%d", u.CacheCreation5mTokens, u.CacheCreation1hTokens)
						}
					} else {
						if u.CacheCreation5mTokens != 0 || u.CacheCreation1hTokens != 0 || strings.Contains(rec.Body.String(), `ephemeral_1h_input_tokens`) {
							t.Fatal("unknown TTL invented")
						}
					}
					if stream && !gjson.GetBytes(up.body, "stream_options.include_usage").Bool() {
						t.Fatal("stream usage not requested")
					}
				})
			}
		}
	}
}

func TestKimiCacheWriteUsageAmbiguity(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Set(kimiCacheMeteringContextKey, true)
	headers := http.Header{"Msh-Usage-Cache-Write-Tokens-5m": {"75"}, "Msh-Usage-Cache-Write-Tokens-1h": {"125"}}
	for _, raw := range []string{
		`{"cache_creation_input_tokens":200,"cache_creation_input_tokens":201}`,
		`{"cache_creation_input_tokens":200,"prompt_tokens_details":{"cache_write_tokens":201}}`,
		`{"prompt_tokens_details":{"cache_write_tokens":"200"}}`,
		`{"prompt_tokens_details":{"cache_write_tokens":-200}}`,
		`{"prompt_tokens_details":null}`,
	} {
		u := OpenAIUsage{InputTokens: 1000, OutputTokens: 100, CacheReadInputTokens: 300, CacheCreationInputTokens: 200}
		got := applyKimiCacheWriteHeaders(c, headers, u, []byte(`{"usage":`+raw+`}`))
		if got.CacheCreationTTLKnown {
			t.Fatalf("ambiguous metering was trusted: %s", raw)
		}
		if got.CacheCreationInputTokens != 200 {
			t.Fatal("aggregate discarded")
		}
	}
}

func TestKimiMessagesInvalidCacheTTLRejectedBeforeDispatch(t *testing.T) {
	for _, extra := range []string{`,"cache_control":null`, `,"cache_control":{"type":"ephemeral","ttl":"2h"}`, `,"cache_control":{"type":"other","ttl":"1h"}`, `,"cache_control":{"type":"ephemeral","ttl":"1h","ttl":"5m"}`} {
		result, err, rec, up := kimiDiagnosticForward(t, "kimi-k3", "/v1/messages", extra, false, nil)
		if err == nil || rec.Code != 400 || up.calls != 0 || result != nil {
			t.Fatalf("invalid TTL reached source: status=%d calls=%d err=%v", rec.Code, up.calls, err)
		}
	}
}

func TestKimiReceiptV2CannotLoseTTL(t *testing.T) {
	result, err, rec, up := kimiDiagnosticForward(t, "kimi-k3", "/v1/chat/completions", "", false, nil, true)
	if err == nil || rec.Code != 400 || result != nil || up.calls != 0 || !strings.Contains(rec.Body.String(), "unsupported_metering_protocol") {
		t.Fatalf("unsupported metering reached paid forward: status=%d calls=%d err=%v", rec.Code, up.calls, err)
	}
	svc := &OpenAIGatewayService{}
	body := []byte(`{"model":"gemini-4-argon","messages":[{"role":"user","content":"synthetic"}]}`)
	if svc.SupportsChatUsageReceipt(context.Background(), &Account{Platform: PlatformKimi, Type: AccountTypeAPIKey}, body) {
		t.Fatal("Kimi v2 would be claimed before dispatch")
	}
	if !svc.SupportsChatUsageReceipt(context.Background(), &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey}, body) {
		t.Fatal("ordinary Responses/Gemini receipt capability regressed")
	}
}

func TestKimiCacheWriteConflictingSourcesRemainUnknown(t *testing.T) {
	const prefix = `"prompt_tokens":1000,"completion_tokens":100,"total_tokens":1100,"prompt_tokens_details":{"cached_tokens":300,"cache_write_tokens":200}`
	for _, tc := range []struct {
		name, extra string
		headers     bool
		known       bool
	}{
		{"matching", `,"cache_creation":{"ephemeral_5m_input_tokens":75,"ephemeral_1h_input_tokens":125}`, true, true},
		{"body_only", `,"cache_creation":{"ephemeral_5m_input_tokens":75,"ephemeral_1h_input_tokens":125}`, false, true},
		{"conflicting", `,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":200}`, true, false},
		{"partial", `,"cache_creation":{"ephemeral_1h_input_tokens":200}`, true, false},
		{"duplicate_bucket", `,"cache_creation":{"ephemeral_5m_input_tokens":75,"ephemeral_5m_input_tokens":75,"ephemeral_1h_input_tokens":125}`, true, false},
		{"duplicate_object", `,"cache_creation":{"ephemeral_5m_input_tokens":75,"ephemeral_1h_input_tokens":125},"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":200}`, true, false},
		{"case_collision", `,"cache_creation":{"ephemeral_5m_input_tokens":75,"ephemeral_1h_input_tokens":125},"Cache_Creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":200}`, true, false},
		{"incorrect_case", `,"Cache_Creation":{"ephemeral_5m_input_tokens":75,"ephemeral_1h_input_tokens":125}`, true, false},
	} {
		for _, endpoint := range []string{"/v1/chat/completions", "/v1/messages", "/v1/responses"} {
			for _, stream := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s%s/stream=%t", tc.name, endpoint, stream), func(t *testing.T) {
					headers := http.Header{"X-Fixture-Usage": []string{"{" + prefix + tc.extra + "}"}}
					if tc.headers {
						headers.Set("Msh-Usage-Cache-Write-Tokens-5m", "75")
						headers.Set("Msh-Usage-Cache-Write-Tokens-1h", "125")
					}
					extra := ""
					if stream {
						extra = `,"stream":true`
					}
					result, err, rec, up := kimiDiagnosticForward(t, "kimi-k3", endpoint, extra, false, headers)
					if err != nil || result == nil {
						t.Fatalf("forward error status=%d err=%v", rec.Code, err)
					}
					if up.calls != 1 || result.Usage.InputTokens != 1000 || result.Usage.OutputTokens != 100 || result.Usage.CacheCreationInputTokens != 200 {
						t.Fatal("conflict changed dispatch or aggregate quantities")
					}
					if result.Usage.CacheCreationTTLKnown != tc.known {
						t.Fatal("conflicting source trusted")
					}
					wire := strings.ToLower(rec.Body.String())
					if tc.known {
						if !strings.Contains(wire, `"ephemeral_1h_input_tokens":125`) {
							t.Fatal("known split lost")
						}
					} else {
						if strings.Contains(wire, "ephemeral_1h_input_tokens") || result.Usage.CacheCreation1hTokens != 0 {
							t.Fatal("conflicting TTL reached one ledger")
						}
					}
				})
			}
		}
	}
}

func TestKimiCacheWriteRawEnvelopeAndTailEvidence(t *testing.T) {
	const good = `{"cache_creation_input_tokens":200,"cache_creation":{"ephemeral_5m_input_tokens":75,"ephemeral_1h_input_tokens":125}}`
	const bad = `{"cache_creation_input_tokens":200,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":200}}`
	headers := http.Header{"Msh-Usage-Cache-Write-Tokens-5m": {"75"}, "Msh-Usage-Cache-Write-Tokens-1h": {"125"}}
	base := OpenAIUsage{InputTokens: 1000, OutputTokens: 100, CacheReadInputTokens: 300, CacheCreationInputTokens: 200}
	for _, raw := range []string{
		`{"usage":` + good + `,"usage":` + bad + `}`,
		`{"usage":` + good + `,"Usage":` + bad + `}`,
		`{"Usage":` + good + `}`,
	} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Set(kimiCacheMeteringContextKey, true)
		got := applyKimiCacheWriteHeaders(c, headers, base, []byte(raw))
		if got.CacheCreationTTLKnown {
			t.Fatal("ambiguous raw envelope accepted")
		}
		// A typed conversion may have normalized the body. It must not re-establish
		// trust from the headers after raw evidence was rejected.
		converted := injectKimiCacheWriteUsage(c, []byte(`{"usage":`+good+`}`))
		if strings.Contains(string(converted), "ephemeral_") {
			t.Fatal("normalized envelope invented TTL")
		}
		original := injectKimiCacheWriteUsage(c, []byte(raw))
		if strings.Contains(string(original), "ephemeral_") {
			t.Fatal("a duplicate usage occurrence leaked disputed TTL")
		}
	}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Set(kimiCacheMeteringContextKey, true)
	got := applyKimiCacheWriteHeaders(c, headers, base, []byte(`{"usage":`+good+`}`))
	if !got.CacheCreationTTLKnown {
		t.Fatal("initial complete evidence lost")
	}
	got = applyKimiCacheWriteHeaders(c, headers, base, []byte(`{"usage":`+bad+`}`))
	if !got.CacheCreationTTLKnown || got.CacheCreation1hTokens != 125 {
		t.Fatal("bad same-aggregate tail erased known TTL")
	}
	changed := base
	changed.CacheCreationInputTokens = 201
	got = applyKimiCacheWriteHeaders(c, headers, changed, []byte(`{"usage":{"cache_creation_input_tokens":201}}`))
	if got.CacheCreationTTLKnown || got.CacheCreation1hTokens != 0 || got.CacheCreationInputTokens != 201 {
		t.Fatal("old TTL attached to changed aggregate")
	}
}

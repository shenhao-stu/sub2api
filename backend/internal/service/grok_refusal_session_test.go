//go:build unit

package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestGrokRefusalContinuationAtHTTPBoundary(t *testing.T) {
	for _, protocol := range []string{"raw_chat", "chat_bridge", "responses", "messages"} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", protocol, stream), func(t *testing.T) {
				var sends atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					sends.Add(1)
					io.Copy(io.Discard, r.Body)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(403)
					io.WriteString(w, `{"code":"permission-denied","error":"I'm sorry, I can't help with that request.","usage":{"input_tokens":17,"output_tokens":2}}`)
				}))
				defer server.Close()
				destination, err := url.Parse(server.URL)
				require.NoError(t, err)
				svc := openAIClientToolsTestService(nil)
				svc.billingService = newLocalFixtureBilling()
				svc.resolver = NewModelPricingResolver(nil, svc.billingService)
				svc.cfg.Gateway.Grok.RefusalSessionIsolationEnabled = true
				svc.cache = &comboCacheAndStore{}
				svc.accountRepo = &grokQuotaAccountRepo{}
				svc.httpUpstream = &grokRefusalHTTPUpstream{client: server.Client(), url: destination}
				account := &Account{ID: 81, Platform: PlatformGrok, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "offline", "base_url": "https://api.x.ai/v1"}, Extra: map[string]any{"openai_responses_supported": true}}
				path := "/v1/chat/completions"
				if protocol == "responses" || protocol == "messages" {
					path = "/v1/" + protocol
				}
				call := func(session string, key int64, text string) (*OpenAIForwardResult, string) {
					field := "messages"
					if protocol == "responses" {
						field = "input"
					}
					body := []byte(fmt.Sprintf(`{"model":"grok-4.6","%s":[{"role":"user","content":%q}],"stream":%t,"max_tokens":32}`, field, text, stream))
					rec := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(rec)
					c.Request = httptest.NewRequest("POST", path, strings.NewReader(string(body)))
					c.Request.Header.Set("X-Grok-Conv-Id", session)
					c.Set("api_key", &APIKey{ID: key, UserID: key})
					var result *OpenAIForwardResult
					switch protocol {
					case "raw_chat":
						result, err = svc.forwardAsRawChatCompletions(c.Request.Context(), c, account, body, "")
					case "chat_bridge":
						result, err = svc.forwardGrokChatCompletionsViaResponses(c.Request.Context(), c, account, body, "", "")
					case "responses":
						result, err = svc.forwardGrokResponses(c.Request.Context(), c, account, body, "grok-4.6", stream, time.Now())
					case "messages":
						result, err = svc.ForwardAsAnthropic(c.Request.Context(), c, account, body, "", "")
					}
					require.Error(t, err)
					return result, rec.Body.String()
				}
				r, _ := call("one", 10, "synthetic")
				require.NotNil(t, r, "%v", err)
				require.Equal(t, 17, r.Usage.InputTokens)
				r, out := call("one", 10, "continue")
				require.Nil(t, r)
				require.Contains(t, out, "grok_session_isolated")
				require.Contains(t, out, "content_policy_violation")
				require.EqualValues(t, 1, sends.Load())
				call("two", 10, "new conversation")
				call("one", 11, "synthetic")
				require.EqualValues(t, 3, sends.Load())
				svc.cfg.Gateway.Grok.RefusalSessionIsolationEnabled = false
				call("one", 10, "continue")
				require.EqualValues(t, 4, sends.Load())
			})
		}
	}
}

func TestGrokRefusalAnonymousHistory(t *testing.T) {
	store := &comboCacheAndStore{}
	g := grokRefusalSession{store: store, scope: "test-key", body: []byte(`{"messages":[{"role":"user","content":"common"},{"role":"assistant","content":"ready"},{"role":"user","content":"branch-A","opaque":9007199254740993}]}`)}
	g.mark()
	g.body = []byte(`{"messages":[{"role":"user","content":"common"},{"role":"assistant","content":"ready"},{"opaque":9007199254740993,"content":"branch-A","role":"user"},{"role":"user","content":"Continue"}]}`)
	hit, err := g.blocked(context.Background())
	require.NoError(t, err)
	require.True(t, hit)
	for _, diff := range []string{"branch-B", "9007199254740992"} {
		b := string(g.body)
		if diff == "branch-B" {
			b = strings.Replace(b, "branch-A", diff, 1)
		} else {
			b = strings.Replace(b, "9007199254740993", diff, 1)
		}
		other := g
		other.body = []byte(b)
		hit, err = other.blocked(context.Background())
		require.NoError(t, err)
		require.False(t, hit)
	}
}
func TestGrokRefusalOfficialEvents(t *testing.T) {
	for _, f := range []struct {
		event, body string
		mark        bool
	}{
		{"response.refusal.done", `{"refusal":"I'm sorry, I can't help with that request."}`, true},
		{"error", `{"error":{"code":"permission-denied","message":"I'm sorry, I can't help with that request."}}`, true},
		{"error", `{"error":{"code":"subscription_required","message":"I'm sorry, I can't help with that request."}}`, false},
		{"response.output_text.delta", `{"delta":"I'm sorry, I can't help with that request."}`, false},
		{"", `{"choices":[{"message":{"content":"I'm sorry, I can't help with that request."}}]}`, false},
		{"", `{"choices":[{"message":{"refusal":"I'm sorry, I can't help with that request."}}]}`, true},
	} {
		g := &grokRefusalSession{store: &comboCacheAndStore{}, scope: "private", explicit: "exact"}
		g.observeEvent([]byte(f.body), f.event)
		require.Equal(t, f.mark, g.marked, f.event)
	}
}
func TestGrokChatNameRewritesOnlyForbiddenSpeakerLabels(t *testing.T) {
	body := []byte(`{ "messages" : [ {"role":"system","name":"sys","content":"A"},{"role":"user","name":"guest","content":[{"type":"text","text":"test"}]}, {"role":"assistant","name":"agent","tool_calls":[{"id":"call_1","function":{"name":"search","arguments":"{}"}}]},{"role":"tool","name":"tool-label","tool_call_id":"call_1","content":"result"},{"role":"function","name":"legacy","content":"result"} ], "other":9007199254740993 }`)
	initial := string(body)
	out, err := normalizeGrokChatMessageNames(body)
	require.NoError(t, err)
	require.Equal(t, initial, string(body))
	require.NotEqual(t, initial, string(out))
	for _, path := range []string{"messages.0.name", "messages.2.name", "messages.3.name"} {
		require.False(t, gjson.GetBytes(out, path).Exists(), path)
	}
	for _, path := range []string{"messages.1", "messages.2.tool_calls", "messages.3.tool_call_id", "messages.4", "other"} {
		require.Equal(t, gjson.GetBytes(body, path).Raw, gjson.GetBytes(out, path).Raw, path)
	}
	second, err := normalizeGrokChatMessageNames(out)
	require.NoError(t, err)
	require.Equal(t, out, second)
}
func TestGrokSharedRelayTenantAndHistoryIsolation(t *testing.T) {
	s := openAIClientToolsTestService(nil)
	s.cache = &comboCacheAndStore{}
	s.cfg.Gateway.Grok.RefusalSessionIsolationEnabled = true
	s.cfg.Gateway.Grok.RefusalSessionRelayKeyIDs = []int64{216}
	account := &Account{ID: 4, Platform: PlatformGrok, Type: AccountTypeOAuth}
	const turn = `{"role":"user","content":"private synthetic turn"}`
	first := []byte(`{"messages":[` + turn + `]}`)
	next := []byte(`{"messages":[` + turn + `,{"role":"user","content":"continue"}]}`)
	check := func(tenant, sid string, body []byte, blocked bool) *grokRefusalSession {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)
		c.Request.Header.Set(AccountAffinityHeader, tenant)
		c.Request.Header.Set("X-Grok-Conv-Id", sid)
		c.Set("api_key", &APIKey{ID: 216, UserID: 1})
		err := s.CheckGrokRefusalSession(c.Request.Context(), c, account, body)
		require.Equal(t, blocked, err != nil)
		return grokRefusalGuardFromContext(c)
	}
	check("123", "default", first, false).mark()
	check("123", "default", next, true)
	check("123", "", next, true)
	check("123", "changed", next, true)
	check("456", "default", next, false)
	check("123", "new", []byte(`{"messages":[{"role":"user","content":"fresh"}]}`), false)
	require.Nil(t, check("", "default", next, false))
	require.Nil(t, check("spoof", "default", next, false))
}

func TestGrokChatNamesOnWire(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, official := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/official=%t", stream, official), func(t *testing.T) {
				var calls atomic.Int32
				up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					b, _ := io.ReadAll(r.Body)
					for _, i := range []int{0, 2, 3} {
						require.Equal(t, !official, gjson.GetBytes(b, fmt.Sprintf("messages.%d.name", i)).Exists())
					}
					require.Equal(t, "speaker", gjson.GetBytes(b, "messages.1.name").Str)
					require.Equal(t, "lookup", gjson.GetBytes(b, "messages.2.tool_calls.0.function.name").Str)
					require.Equal(t, "call_1", gjson.GetBytes(b, "messages.3.tool_call_id").Str)
					if stream {
						w.Header().Set("Content-Type", "text/event-stream")
						io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"OK\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":7}}\n\ndata: [DONE]\n\n")
						return
					}
					io.WriteString(w, `{"choices":[{"message":{"content":"OK"},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":7}}`)
				}))
				defer up.Close()
				u, _ := url.Parse(up.URL)
				s := openAIClientToolsTestService(nil)
				s.httpUpstream = &grokRefusalHTTPUpstream{client: up.Client(), url: u}
				base := "https://relay.example.com/v1"
				if official {
					base = "https://api.x.ai/v1"
				}
				a := &Account{ID: 4, Platform: PlatformGrok, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "offline", "base_url": base}}
				body := []byte(fmt.Sprintf(`{"model":"grok-4.7","messages":[{"role":"system","name":"sys","content":"test"},{"role":"user","name":"speaker","content":"test"},{"role":"assistant","name":"bot","tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}}]},{"role":"tool","name":"lookup","tool_call_id":"call_1","content":"{}"}],"stream":%t}`, stream))
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)
				r, err := s.forwardAsRawChatCompletions(c.Request.Context(), c, a, body, "")
				require.NoError(t, err)
				require.Equal(t, 12, r.Usage.InputTokens)
				require.Equal(t, 7, r.Usage.OutputTokens)
				require.EqualValues(t, 1, calls.Load())
			})
		}
	}
}

func TestGrokRefusalContinuationByProviderResponseID(t *testing.T) {
	for _, stream := range []bool{true, false} {
		store := &comboCacheAndStore{}
		g := grokRefusalSession{store: store, scope: "tenant123", body: []byte(`{"input":"first"}`)}
		o := &upstreamResponseModelObserver{grokRefusal: &g}
		const id = "resp_12345678901234567890"
		if stream {
			o.ObserveOpenAI([]byte(`{"type":"response.created","response":{"id":"`+id+`"}}`), "response.created")
			o.ObserveOpenAI([]byte(`{"type":"response.refusal.done","refusal":"I'm sorry, I can't help with that request."}`), "response.refusal.done")
		} else {
			o.ObserveOpenAI([]byte(`{"id":"`+id+`","object":"response","status":"completed","output":[{"content":[{"type":"refusal","refusal":"I'm sorry, I can't help with that request."}]}]}`), "")
		}
		require.True(t, g.marked)
		for _, tenant := range []string{"tenant123", "tenant456"} {
			next := grokRefusalSession{store: store, scope: tenant, body: []byte(`{"previous_response_id":"` + id + `","input":"continue"}`)}
			next.previous = next.sessionKey(grokPreviousResponseSessionSeed(next.body))
			hit, err := next.blocked(context.Background())
			require.NoError(t, err)
			require.Equal(t, tenant == "tenant123", hit)
		}
	}
}

func TestGrokRefusalWebSocketConsecutiveTurns(t *testing.T) {
	var upstreamCalls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		if r.Header.Get("X-Request-Id") == "" {
			w.Header().Set("X-Request-Id", "synthetic-refusal")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(403)
		io.WriteString(w, `{"code":"permission-denied","error":"I'm sorry, I can't help with that request.","usage":{"input_tokens":17,"output_tokens":2}}`)
	}))
	defer up.Close()
	s := openAIClientToolsTestService(nil)
	s.cfg.Gateway.Grok.RefusalSessionIsolationEnabled = true
	s.cache = &comboCacheAndStore{}
	u, _ := url.Parse(up.URL)
	s.httpUpstream = &grokRefusalHTTPUpstream{client: up.Client(), url: u}
	a := &Account{ID: 99, Platform: PlatformGrok, Type: AccountTypeAPIKey, Credentials: map[string]any{"base_url": "https://api.x.ai", "api_key": "offline"}}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("GET", "/v1/responses", nil)
	c.Set("api_key", &APIKey{ID: 18, UserID: 4})
	// The HTTP bridge strips previous_response_id before dispatch. Check the
	// original turn, including when the client sends only a fresh input.
	const id = "resp_12345678901234567890"
	call := func(n int, input string) (*OpenAIForwardResult, error, []byte) {
		body := []byte(fmt.Sprintf(`{"type":"response.create","previous_response_id":%q,"input":%q,"model":"grok-4.7"}`, id, input))
		var frame []byte
		result, err := s.proxyOpenAIWSHTTPBridgeTurn(c.Request.Context(), c, a, "offline", body, len(body), "grok-4.7", "", "", "", "", n, func(b []byte) error { frame = append([]byte(nil), b...); return nil })
		return result, err, frame
	}
	r, err, frame := call(1, "synthetic")
	require.ErrorIs(t, err, ErrGrokWSContentRefusal)
	require.NotNil(t, r)
	require.Equal(t, 17, r.Usage.InputTokens)
	require.Equal(t, "content_policy_violation", gjson.GetBytes(frame, "error.code").Str)
	r, err, frame = call(2, "continue")
	require.Error(t, err)
	require.Nil(t, r)
	require.Equal(t, "grok_session_isolated", gjson.GetBytes(frame, "error.type").Str)
	require.EqualValues(t, 1, upstreamCalls.Load())
}

package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func relayScopeAccount(enabled bool) *Account {
	return &Account{ID: 81, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Credentials: map[string]any{"base_url": "https://relay.example/v1"},
		Extra:       map[string]any{"openai_relay_session_scope": enabled}}
}

func relayScopeContext(userID, keyID int64, tag, session string) *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set("session_id", session)
	c.Request.Header.Set("session-id", session)
	c.Request.Header.Set("X-GeToken-User-Id", "untrusted-input")
	if tag != "" {
		c.Request = c.Request.WithContext(WithAccountAffinityTags(c.Request.Context(), []string{tag}))
	}
	groupID := int64(90)
	c.Set("api_key", &APIKey{ID: keyID, UserID: userID, GroupID: &groupID})
	SetOpenAIHTTPResponseOwner(c, userID, keyID)
	return c
}

func relayScopeBuild(s *OpenAIGatewayService, c *gin.Context, a *Account, body []byte, passthrough bool) (*http.Request, error) {
	if passthrough {
		return s.buildUpstreamRequestOpenAIPassthrough(c.Request.Context(), c, a, body, "upstream-key")
	}
	return s.buildUpstreamRequest(c.Request.Context(), c, a, body, "upstream-key", true, "same-session", false)
}

func TestRelaySessionScopeBuildersSeparateTrustedUsers(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := []byte(`{"model":"gpt-6.1-terra","stream":true,"input":[{"role":"user","content":"hello"}],"prompt_cache_key":"same-session","client_metadata":{"session_id":"same-session","thread_id":"thread","counter":9007199254740993},"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]}`)
	for _, passthrough := range []bool{false, true} {
		s := openCodeSessionTestService()
		a := relayScopeAccount(true)
		first, err := relayScopeBuild(s, relayScopeContext(1, 101, "getoken:134", "same-session"), a, body, passthrough)
		require.NoError(t, err)
		second, err := relayScopeBuild(s, relayScopeContext(1, 101, "getoken:135", "same-session"), a, body, passthrough)
		require.NoError(t, err)
		again, err := relayScopeBuild(s, relayScopeContext(1, 101, "getoken:134", "same-session"), a, body, passthrough)
		require.NoError(t, err)
		require.NotEmpty(t, first.Header.Get("session-id"))
		require.NotEqual(t, first.Header.Get("session-id"), second.Header.Get("session-id"))
		require.Equal(t, first.Header.Get("session-id"), again.Header.Get("session-id"))
		require.Equal(t, first.Header.Get("session-id"), first.Header.Get("session_id"))
		firstBody, err := io.ReadAll(first.Body)
		require.NoError(t, err)
		require.Equal(t, first.Header.Get("session-id"), gjson.GetBytes(firstBody, "prompt_cache_key").String())
		require.Equal(t, first.Header.Get("session-id"), gjson.GetBytes(firstBody, "client_metadata.session_id").String())
		for _, path := range []string{"input", "tools", "client_metadata.counter", "model", "stream"} {
			require.Equal(t, gjson.GetBytes(body, path).Raw, gjson.GetBytes(firstBody, path).Raw)
		}
		require.Equal(t, int64(len(firstBody)), first.ContentLength)
		replayBody, err := first.GetBody()
		require.NoError(t, err)
		replayed, err := io.ReadAll(replayBody)
		require.NoError(t, err)
		require.Equal(t, firstBody, replayed)
		require.Equal(t, "Bearer upstream-key", first.Header.Get("Authorization"))
		require.Empty(t, first.Header.Get("X-GeToken-User-Id"))
	}
}

func TestRelaySessionScopeDirectKeysAndSpoofedHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s, a := openCodeSessionTestService(), relayScopeAccount(true)
	body := []byte(`{"model":"gpt-6.1-terra","input":"hello"}`)
	aCtx := relayScopeContext(1, 101, "", "session")
	bCtx := relayScopeContext(2, 102, "", "session")
	aReq, err := relayScopeBuild(s, aCtx, a, body, true)
	require.NoError(t, err)
	bReq, err := relayScopeBuild(s, bCtx, a, body, true)
	require.NoError(t, err)
	require.NotEqual(t, aReq.Header.Get("session-id"), bReq.Header.Get("session-id"))
	aCtx.Request.Header.Set("X-GeToken-User-Id", "135")
	again, err := relayScopeBuild(s, aCtx, a, body, true)
	require.NoError(t, err)
	require.Equal(t, aReq.Header.Get("session-id"), again.Header.Get("session-id"))
}

func TestRelaySessionScopeDefaultOffAndMissingIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s := openCodeSessionTestService()
	body := []byte(`{"model":"gpt-6.1-terra","input":"hello","prompt_cache_key":"same-session"}`)
	for _, passthrough := range []bool{false, true} {
		c := relayScopeContext(1, 101, "getoken:134", "same-session")
		req, err := relayScopeBuild(s, c, relayScopeAccount(false), body, passthrough)
		require.NoError(t, err)
		got, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		require.True(t, bytes.Equal(body, got))
		require.Equal(t, "same-session", req.Header.Get("session_id"))
		c.Set("api_key", &APIKey{})
		_, err = relayScopeBuild(s, c, relayScopeAccount(true), body, passthrough)
		require.Error(t, err)
	}
}

func TestRelaySessionScopeContinuationOwnerAndTools(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s, a := openCodeSessionTestService(), relayScopeAccount(true)
	owner := relayScopeContext(1, 101, "getoken:134", "same-session")
	stranger := relayScopeContext(1, 101, "getoken:135", "same-session")
	s.bindHTTPResponseAccount(owner.Request.Context(), owner, a, "resp_relay_owned")
	allowed, err := s.ValidateOpenAIHTTPResponseOwner(owner.Request.Context(), 90, "resp_relay_owned", 1, 101)
	require.NoError(t, err)
	require.True(t, allowed)
	allowed, err = s.ValidateOpenAIHTTPResponseOwner(stranger.Request.Context(), 90, "resp_relay_owned", 1, 101)
	require.NoError(t, err)
	require.False(t, allowed)
	allowed, err = s.ValidateOpenAIHTTPResponseOwner(context.Background(), 90, "resp_relay_owned", 1, 101)
	require.NoError(t, err)
	require.False(t, allowed)
	body := []byte(`{"model":"gpt-6.1-terra","previous_response_id":"resp_relay_owned","input":[{"type":"function_call_output","call_id":"call_1","output":"result"}]}`)
	req, err := relayScopeBuild(s, owner, a, body, true)
	require.NoError(t, err)
	got, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	require.Equal(t, "resp_relay_owned", gjson.GetBytes(got, "previous_response_id").String())
	require.Equal(t, gjson.GetBytes(body, "input").Raw, gjson.GetBytes(got, "input").Raw)
	_, err = relayScopeBuild(s, stranger, a, body, true)
	require.Error(t, err)
}

func TestRelaySessionScopeParallelRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s, a := openCodeSessionTestService(), relayScopeAccount(true)
	var wg sync.WaitGroup
	results := make(chan string, 10)
	for userID := int64(1); userID <= 10; userID++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := relayScopeContext(userID, userID, "", "same-session")
			req, err := relayScopeBuild(s, c, a, []byte(`{"model":"gpt-6.1-terra","input":"hello"}`), true)
			if err != nil {
				results <- ""
				return
			}
			results <- req.Header.Get("session-id")
		}()
	}
	wg.Wait()
	close(results)
	seen := map[string]bool{}
	for value := range results {
		require.NotEmpty(t, value)
		require.False(t, seen[value])
		seen[value] = true
	}
}

func TestRelaySessionScopeNoHintIsStableOnlyWithinRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s, a := openCodeSessionTestService(), relayScopeAccount(true)
	body := []byte(`{"model":"gpt-6.1-terra","input":"same first prompt"}`)
	c := relayScopeContext(1, 101, "", "")
	first, err := relayScopeBuild(s, c, a, body, true)
	require.NoError(t, err)
	again, err := relayScopeBuild(s, c, a, body, true)
	require.NoError(t, err)
	another, err := relayScopeBuild(s, relayScopeContext(1, 101, "", ""), a, body, true)
	require.NoError(t, err)
	require.Equal(t, first.Header.Get("session-id"), again.Header.Get("session-id"))
	require.NotEqual(t, first.Header.Get("session-id"), another.Header.Get("session-id"))
}

func TestRelaySessionScopeOwnerSurvivesStoreRestartAndCancellation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cache := &relayScopeCache{}
	s := &OpenAIGatewayService{cache: cache}
	a := relayScopeAccount(true)
	c := relayScopeContext(1, 101, "getoken:134", "same-session")
	ctx, cancel := context.WithCancel(c.Request.Context())
	cancel()
	s.bindHTTPResponseAccount(ctx, c, a, "resp_persisted_scope")
	restarted := &OpenAIGatewayService{cache: cache}
	for _, tag := range []string{"getoken:134", "getoken:135", ""} {
		reader := relayScopeContext(1, 101, tag, "same-session")
		allowed, err := restarted.ValidateOpenAIHTTPResponseOwner(reader.Request.Context(), 90, "resp_persisted_scope", 1, 101)
		require.NoError(t, err)
		require.Equal(t, tag == "getoken:134", allowed)
	}
	_, _, legacyExists, err := restarted.getOpenAIWSStateStore().GetHTTPResponseOwner(context.Background(), 90, "resp_persisted_scope")
	require.ErrorIs(t, err, ErrStickySessionNotFound)
	require.False(t, legacyExists, "scoped continuations must never have an unscoped fallback")
}

type relayScopeCache struct{ stubGatewayCache }

func (c *relayScopeCache) GetSessionAccountID(ctx context.Context, groupID int64, key string) (int64, error) {
	if id, ok := c.sessionBindings[key]; ok {
		return id, nil
	}
	return 0, ErrStickySessionNotFound
}

func TestRelaySessionScopeDoesNotChangeOAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s, a := openCodeSessionTestService(), relayScopeAccount(true)
	a.Type = AccountTypeOAuth
	a.Credentials = map[string]any{"chatgpt_account_id": "oauth-account"}
	c := relayScopeContext(1, 101, "getoken:134", "same-session")
	req, err := relayScopeBuild(s, c, a, []byte(`{"model":"gpt-6.1-terra","input":"hello"}`), true)
	require.NoError(t, err)
	require.Equal(t, isolateOpenAIUpstreamSessionID(101, a, "same-session"), req.Header.Get("session_id"))
	require.Empty(t, req.Header.Get("session-id"))
	require.Equal(t, "chatgpt.com", req.Host)
}

func TestRelaySessionScopeCannotBypassHTTPThroughWebSockets(t *testing.T) {
	a := relayScopeAccount(true)
	a.Extra["openai_apikey_responses_websockets_v2_enabled"] = true
	a.Extra["openai_apikey_responses_websockets_v2_mode"] = "passthrough"
	require.False(t, a.IsOpenAIResponsesWebSocketV2Enabled())
	require.Equal(t, OpenAIWSIngressModeOff, a.ResolveOpenAIResponsesWebSocketV2Mode("passthrough"))
	a.Extra["openai_relay_session_scope"] = false
	require.True(t, a.IsOpenAIResponsesWebSocketV2Enabled())
	require.Equal(t, "passthrough", a.ResolveOpenAIResponsesWebSocketV2Mode("passthrough"))
}

func TestRelaySessionScopeSSEForwardToolsAndForeignContinuationNoDispatch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	response := func(id, output string) *http.Response {
		data := `event: response.completed` + "\n" + `data: {"type":"response.completed","response":{"id":"` + id + `","status":"completed","model":"gpt-6.1-terra","output":` + output + `,"usage":{"input_tokens":37,"output_tokens":2}}}` + "\n\n"
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(bytes.NewBufferString(data))}
	}
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		response("resp_tenant_a", `[{"type":"function_call","id":"fc_1","call_id":"call_1","name":"lookup","arguments":"{\"token\":\"alpha\"}"}]`),
		response("resp_tenant_b", `[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"B"}]}]`),
		response("resp_tenant_a_next", `[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"alpha"}]}]`),
	}}
	s := openCodeSessionTestService()
	s.httpUpstream = upstream
	a := relayScopeAccount(true)
	a.Credentials["api_key"] = "test-only-key"
	forward := func(tag, raw string) (*OpenAIForwardResult, *gin.Context, error) {
		c := relayScopeContext(1, 101, tag, "same-session")
		body := []byte(raw)
		result, err := s.forwardOpenAIPassthrough(c.Request.Context(), c, a, body, body, "gpt-6.1-terra", false, nil, true, time.Now())
		return result, c, err
	}
	firstBody := `{"model":"gpt-6.1-terra","stream":true,"input":"hello","tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]}`
	for _, tag := range []string{"getoken:134", "getoken:135"} {
		result, c, err := forward(tag, firstBody)
		require.NoError(t, err)
		require.Equal(t, 37, result.Usage.InputTokens)
		require.Equal(t, 2, result.Usage.OutputTokens)
		require.Equal(t, http.StatusOK, c.Writer.Status())
	}
	continuation := `{"model":"gpt-6.1-terra","stream":true,"previous_response_id":"resp_tenant_a","input":[{"type":"function_call_output","call_id":"call_1","output":"alpha"}]}`
	result, _, err := forward("getoken:134", continuation)
	require.NoError(t, err)
	require.Equal(t, 37, result.Usage.InputTokens)
	require.Equal(t, "resp_tenant_a", gjson.GetBytes(upstream.lastBody, "previous_response_id").String())
	require.Equal(t, "alpha", gjson.GetBytes(upstream.lastBody, "input.0.output").String())
	require.Len(t, upstream.requests, 3)
	require.NotEqual(t, upstream.requests[0].Header.Get("session-id"), upstream.requests[1].Header.Get("session-id"))
	require.Equal(t, upstream.requests[0].Header.Get("session-id"), upstream.requests[2].Header.Get("session-id"))
	_, rejected, err := forward("getoken:135", continuation)
	require.Error(t, err)
	var retry *UpstreamFailoverError
	require.False(t, errors.As(err, &retry), "an ownership rejection is terminal")
	require.Equal(t, http.StatusBadRequest, rejected.Writer.Status())
	require.Len(t, upstream.requests, 3, "a foreign continuation must not reach an upstream or incur usage")
}

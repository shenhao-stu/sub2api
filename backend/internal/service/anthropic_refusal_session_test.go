package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

const syntheticPolicyRefusal = `{"type":"error","error":{"type":"policy_blocked","code":"refusal_guard","details":{"blocked_by":"upstream","retryable":false}}}`

func TestAnthropicSessionRefusalAcceptsOnlyTypedUpstreamDecision(t *testing.T) {
	for _, payload := range []string{
		syntheticPolicyRefusal,
		`{"type":"message_delta","delta":{"stop_reason":"refusal"}}`,
		`{"type":"message","role":"assistant","stop_reason":"refusal"}`,
	} {
		require.True(t, isOfficialAnthropicSessionRefusal([]byte(payload)))
	}
	for _, payload := range []string{
		strings.Replace(syntheticPolicyRefusal, `"upstream"`, `"device"`, 1),
		strings.Replace(syntheticPolicyRefusal, `"retryable":false`, `"retryable":"false"`, 1),
		strings.Replace(syntheticPolicyRefusal, `"retryable":false`, `"retryable":true,"retryable":false`, 1),
		`{"type":"error","error":{"type":"overloaded_error","message":"refusal_guard"}}`,
		`{"type":"content_block_delta","delta":{"text":"Anthropic refused this request under its usage policy"}}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn"}}`,
	} {
		require.False(t, isOfficialAnthropicSessionRefusal([]byte(payload)))
	}
}

func TestAnthropicSessionRefusalKeepsMeterAndBlocksQueuedFailover(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const session = "d35e3672-a59f-4764-ac54-413a3155bc3b"
	const otherSession = "ae7a94b9-222e-4700-adc3-b239e9d07f11"
	key := &APIKey{ID: 214, UserID: 1}
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	svc := &GatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{
		MaxLineSize: defaultMaxLineSize, AnthropicRefusalRelayKeyIDs: []int64{214}, AnthropicRefusalSourceAccountIDs: []int64{41}, AnthropicRefusalJournalDir: dir,
	}}, cache: &comboCacheAndStore{}, rateLimitService: &RateLimitService{}}
	account := &Account{ID: 41, Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Extra: map[string]any{"anthropic_passthrough": true}}
	otherAccount := &Account{ID: 42, Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Extra: account.Extra}
	request := func(tags []string, sessionID string) (*gin.Context, *httptest.ResponseRecorder, *ParsedRequest) {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		// The raw header is deliberately forged. Only middleware tags count.
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
		c.Request.Header.Set(AccountAffinityHeader, "53")
		c.Request = c.Request.WithContext(WithAccountAffinityTags(c.Request.Context(), tags))
		parsed := &ParsedRequest{MetadataUserID: FormatMetadataUserID(strings.Repeat("a", 64), "", sessionID, "2.1.78")}
		return c, rec, parsed
	}
	first, _, parsed := request([]string{"getoken:53"}, session)
	queued, queuedRec, queuedParsed := request([]string{"getoken:53"}, session)
	require.NoError(t, svc.AdmitAnthropicRefusalSession(first.Request.Context(), first, key, parsed))
	require.NoError(t, svc.AdmitAnthropicRefusalSession(queued.Request.Context(), queued, key, queuedParsed))
	require.Nil(t, svc.trustedAnthropicRefusal(first, otherAccount))

	body := "data: " + `{"type":"message_start","message":{"usage":{"input_tokens":7,"cache_creation_input_tokens":99}}}` + "\n\n" +
		"event: error\ndata: " + syntheticPolicyRefusal + "\n\n"
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(body))}
	result, _ := svc.handleStreamingResponseAnthropicAPIKeyPassthrough(context.Background(), resp, first, account, time.Now(), "claude-opus")
	require.NotNil(t, result)
	require.Equal(t, 7, result.usage.InputTokens)
	require.Equal(t, 99, result.usage.CacheCreationInputTokens)

	// This request was admitted before the refusal but has not reached another account yet.
	forward, err := svc.Forward(queued.Request.Context(), queued, otherAccount, queuedParsed)
	require.ErrorIs(t, err, errAnthropicSessionAdmission)
	require.Nil(t, forward)
	require.Equal(t, http.StatusBadRequest, queuedRec.Code)
	require.Contains(t, queuedRec.Body.String(), `"code":"session_quarantined"`)

	// A replacement process with a fresh Redis store still sees the marker.
	scope, ok := anthropicRefusalSessionKey(first.Request.Context(), key, parsed.MetadataUserID)
	require.True(t, ok)
	pendingAnthropicRefusalMarks.Delete(scope)
	svc.cache = &comboCacheAndStore{}
	restarted, restartedRec, restartedParsed := request([]string{"getoken:53"}, session)
	require.ErrorIs(t, svc.AdmitAnthropicRefusalSession(restarted.Request.Context(), restarted, key, restartedParsed), errAnthropicSessionAdmission)
	require.Contains(t, restartedRec.Body.String(), `"code":"session_quarantined"`)

	for _, test := range []struct{ tag, session string }{{"getoken:54", session}, {"getoken:53", otherSession}} {
		c, _, p := request([]string{test.tag}, test.session)
		require.NoError(t, svc.AdmitAnthropicRefusalSession(c.Request.Context(), c, key, p))
	}
	forged, _, forgedParsed := request(nil, session)
	require.NoError(t, svc.AdmitAnthropicRefusalSession(forged.Request.Context(), forged, key, forgedParsed))
	require.Nil(t, anthropicRefusalFromContext(forged))

	sdk, _, sdkParsed := request([]string{"getoken:53"}, session)
	sdkParsed.MetadataUserID = ""
	require.NoError(t, svc.AdmitAnthropicRefusalSession(sdk.Request.Context(), sdk, key, sdkParsed))
	require.Nil(t, anthropicRefusalFromContext(sdk))
}

func TestAnthropicSessionRefusalHTTPErrorPreservesUsageWithoutFailover(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	c.Set(anthropicRefusalSessionContextKey, &anthropicRefusalSession{
		store: &comboCacheAndStore{}, journalDir: dir, key: t.Name(), ctx: c.Request.Context(),
	})
	svc := newForwardPartialUsageServiceForTest(nil)
	account := newAnthropicAPIKeyAccountForTest()
	account.Credentials["pool_mode"] = true
	account.Extra["anthropic_passthrough"] = true
	svc.cfg.Gateway.AnthropicRefusalSourceAccountIDs = []int64{account.ID}
	body := strings.TrimSuffix(syntheticPolicyRefusal, "}") + `,"usage":{"input_tokens":12,"output_tokens":2,"cache_creation":{"ephemeral_1h_input_tokens":3}}}`
	upstream := &queuedHTTPUpstreamStub{responses: []*http.Response{{
		StatusCode: http.StatusBadRequest,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}}}
	svc.httpUpstream = upstream
	parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(`{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"synthetic"}]}`)), PlatformAnthropic)
	require.NoError(t, err)
	result, err := svc.Forward(c.Request.Context(), c, account, parsed)
	require.Error(t, err)
	require.NotErrorAs(t, err, new(*UpstreamFailoverError))
	require.Equal(t, 1, upstream.callCount)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), `"code":"refusal_guard"`)
	require.NotNil(t, result)
	require.Equal(t, 12, result.Usage.InputTokens)
	require.Equal(t, 2, result.Usage.OutputTokens)
	require.Equal(t, 3, result.Usage.CacheCreation1hTokens)
	require.ErrorIs(t, checkAnthropicRefusalSession(c.Request.Context(), c), errAnthropicSessionAdmission)
}

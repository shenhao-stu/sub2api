//go:build unit

package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

const geminiVertexAPIKeyTestResponse = `{"candidates":[{"content":{"parts":[{"text":"hello"}],"role":"model"},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":5,"totalTokenCount":15}}`

func newGeminiVertexAPIKeyTestService() (*GeminiMessagesCompatService, *geminiCompatHTTPUpstreamStub) {
	httpStub := &geminiCompatHTTPUpstreamStub{
		response: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}, "X-Request-Id": []string{"vertex-req-1"}},
			Body:       io.NopCloser(strings.NewReader(geminiVertexAPIKeyTestResponse)),
		},
	}
	// tokenProvider 留空：API Key 认证不依赖 access token。
	return &GeminiMessagesCompatService{httpUpstream: httpStub, cfg: &config.Config{}}, httpStub
}

func geminiVertexAPIKeyTestAccount(projectID string) *Account {
	credentials := map[string]any{
		"auth_mode": "apikey",
		"api_key":   "vertex-test-key",
		"location":  "us-central1",
		"tier_id":   "vertex",
	}
	if projectID != "" {
		credentials["project_id"] = projectID
	}
	return &Account{
		ID:          811,
		Platform:    PlatformGemini,
		Type:        AccountTypeServiceAccount,
		Credentials: credentials,
		Concurrency: 1,
	}
}

func requireVertexAPIKeyAuth(t *testing.T, req *http.Request) {
	t.Helper()
	require.NotNil(t, req)
	require.Equal(t, "vertex-test-key", req.Header.Get("x-goog-api-key"))
	require.Empty(t, req.Header.Get("Authorization"))
}

func TestGeminiForwardNative_VertexAPIKeyWithoutProjectUsesExpressEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc, httpStub := newGeminiVertexAPIKeyTestService()
	c, _ := newGeminiNativeTestContext(t)

	result, err := svc.ForwardNative(context.Background(), c, geminiVertexAPIKeyTestAccount(""),
		"gemini-3-flash-preview", "generateContent", false, []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`))
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, httpStub.calls)
	require.Equal(t, "https://aiplatform.googleapis.com/v1/publishers/google/models/gemini-3-flash-preview:generateContent", httpStub.lastReq.URL.String())
	requireVertexAPIKeyAuth(t, httpStub.lastReq)
}

func TestGeminiMessagesCompatServiceForward_VertexAPIKeyWithProjectUsesProjectEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc, httpStub := newGeminiVertexAPIKeyTestService()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	body := []byte(`{"model":"gemini-3-flash-preview","max_tokens":16,"messages":[{"role":"user","content":"hello"}]}`)

	result, err := svc.Forward(context.Background(), c, geminiVertexAPIKeyTestAccount("my-project"), body)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "https://us-central1-aiplatform.googleapis.com/v1/projects/my-project/locations/us-central1/publishers/google/models/gemini-3-flash-preview:generateContent", httpStub.lastReq.URL.String())
	requireVertexAPIKeyAuth(t, httpStub.lastReq)
}

func TestGeminiForwardAsChatCompletions_VertexAPIKeyUsesAPIKeyHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc, httpStub := newGeminiVertexAPIKeyTestService()
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gemini-3-flash-preview","messages":[{"role":"user","content":"hi"}]}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

	result, err := svc.ForwardAsChatCompletions(context.Background(), c, geminiVertexAPIKeyTestAccount(""), body)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "https://aiplatform.googleapis.com/v1/publishers/google/models/gemini-3-flash-preview:generateContent", httpStub.lastReq.URL.String())
	requireVertexAPIKeyAuth(t, httpStub.lastReq)
}

type geminiVertex429SequenceUpstream struct {
	responses []*http.Response
	calls     int
}

func (s *geminiVertex429SequenceUpstream) Do(req *http.Request, proxyURL string, accountID int64, accountConcurrency int) (*http.Response, error) {
	resp := s.responses[min(s.calls, len(s.responses)-1)]
	s.calls++
	return resp, nil
}

func (s *geminiVertex429SequenceUpstream) DoWithTLS(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return s.Do(req, proxyURL, accountID, accountConcurrency)
}

// 明确容量拒绝不在同一账号重试；原生缓存绑定仍由 handler 保持，不换用另一账号的缓存。
func TestGeminiForwardNative_VertexSharedCapacity429StopsRetryWithModelCooldown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	sharedCapacity := `{"error":{"code":429,"message":"Resource exhausted. Please try again later.","status":"RESOURCE_EXHAUSTED"}}`
	tests := []struct {
		name string
		ctx  context.Context
	}{
		{name: "unbound", ctx: context.Background()},
		{name: "bound_cached_content", ctx: WithGeminiCachedContentBound(context.Background())},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := &geminiVertex429SequenceUpstream{responses: []*http.Response{
				{
					StatusCode: http.StatusTooManyRequests,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(sharedCapacity)),
				},
				{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(geminiVertexAPIKeyTestResponse)),
				},
			}}
			repo, account := vertexCooldownFixture()
			svc := &GeminiMessagesCompatService{
				httpUpstream:     upstream,
				cfg:              &config.Config{Gateway: config.GatewayConfig{MaxSameAccountRetriesGemini: 1}},
				accountRepo:      repo,
				rateLimitService: NewRateLimitService(repo, nil, &config.Config{}, nil, nil),
			}
			c, _ := newGeminiNativeTestContext(t)

			result, err := svc.ForwardNative(tt.ctx, c, account,
				"gemini-3.8-flash", "generateContent", false, []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`))
			require.Error(t, err)
			require.Nil(t, result)
			require.Equal(t, 1, upstream.calls)
			require.Zero(t, repo.accountWrites)
			require.Equal(t, 1, repo.writes)
			require.NotNil(t, repo.accountsByID[account.ID].modelRateLimitResetAt("gemini-3.8-flash"))
		})
	}
}

//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

const codexClientRestrictionFixture = `{"error":{"type":"upstream_error","message":"client 'claude_code' is not allowed on the Codex hop"}}`

func TestCodexClientRestrictionNeverPoisonsAccount(t *testing.T) {
	repo := &rateLimitAccountRepoStub{}
	limits := NewRateLimitService(repo, nil, nil, nil, nil)
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	body := []byte(codexClientRestrictionFixture)
	require.False(t, limits.HandleUpstreamError(context.Background(), account, 403, nil, body))
	require.Zero(t, repo.tempCalls)
	require.Zero(t, repo.setErrorCalls)
	require.Zero(t, repo.rateLimitedCalls)
	svc := &OpenAIGatewayService{rateLimitService: limits}
	require.False(t, svc.shouldFailoverOpenAIUpstreamResponse(account, 403, "", body))
	require.False(t, shouldFailoverOpenAIPassthroughResponse(account, 403, body))
	require.False(t, svc.handleOpenAIAccountUpstreamError(context.Background(), account, 403, nil, body))
}
func TestCodexClientRestrictionReturnsActionable403(t *testing.T) {
	for _, mode := range []string{"responses", "passthrough", "compat"} {
		t.Run(mode, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
			svc := &OpenAIGatewayService{}
			a := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
			resp := &http.Response{StatusCode: 403, Header: http.Header{"X-Request-Id": []string{"fixture-codex-client"}}, Body: io.NopCloser(strings.NewReader(codexClientRestrictionFixture))}
			var err error
			switch mode {
			case "responses":
				_, err = svc.handleErrorResponse(context.Background(), resp, c, a, nil)
			case "passthrough":
				err = svc.handleErrorResponsePassthrough(context.Background(), resp, c, a, nil, []byte(codexClientRestrictionFixture))
			case "compat":
				_, err = svc.handleCompatErrorResponse(resp, c, a, writeResponsesError)
			}
			require.Error(t, err)
			require.Equal(t, 403, rec.Code)
			require.Contains(t, rec.Body.String(), "Codex")
			require.NotContains(t, rec.Body.String(), "temporarily unavailable")
			var failover *UpstreamFailoverError
			require.NotErrorAs(t, err, &failover)
		})
	}
}

func TestCodexClientRestrictionDoesNotMaskOther403(t *testing.T) {
	for _, body := range []string{
		`{"error":{"message":"Access forbidden (403): client 'claude_code' is not allowed on the Codex hop"}}`,
		codexClientRestrictionFixture,
	} {
		require.True(t, isOpenAIClientRestriction(403, []byte(body)))
		require.False(t, isOpenAIClientRestriction(401, []byte(body)))
	}
	for _, body := range []string{
		`{"error":{"message":"Invalid API key"}}`, `{"error":{"message":"Forbidden"}}`,
		`{"error":{"message":"client 'claude_code' is not allowed on the Codex hop\ninvalid_api_key"}}`,
		`{"request":{"message":"client 'claude_code' is not allowed on the Codex hop"}}`,
		`{"error":{"message":"Your account has been disabled"}}`,
	} {
		require.False(t, isOpenAIClientRestriction(403, []byte(body)))
	}
	repo := &rateLimitAccountRepoStub{}
	svc := NewRateLimitService(repo, nil, nil, nil, nil)
	a := &Account{ID: 7, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	require.True(t, svc.HandleUpstreamError(context.Background(), a, 403, nil, []byte(`{"error":{"message":"Forbidden"}}`)))
	require.Equal(t, 1, repo.setErrorCalls, "ordinary API-key authentication protection remains enabled")
}

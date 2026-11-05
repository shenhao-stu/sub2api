//go:build unit

package service

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestSessionAdmissionDoesNotChangeAccountOrStickyEligibility(t *testing.T) {
	repo := &modelCooldownRepo{}
	s := newForwardPartialUsageServiceForTest(nil)
	s.rateLimitService.accountRepo = repo
	account := newAnthropicAPIKeyAccountForTest()
	before, err := json.Marshal(account)
	require.NoError(t, err)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	resp := &http.Response{StatusCode: 429, Body: io.NopCloser(strings.NewReader(sessionAdmissionTestBody))}
	_, err, handled := s.handleSessionAdmissionResponse(c, account, resp, "claude-opus-5-5", "claude-opus-5-5", false, time.Now())
	require.True(t, handled)
	require.Error(t, err)
	after, marshalErr := json.Marshal(account)
	require.NoError(t, marshalErr)
	require.Equal(t, before, after)
	require.Zero(t, repo.rateLimitCalls)
	require.Zero(t, repo.tempUnschedCalls)
	require.Zero(t, repo.modelRateLimitCalls)
	account.Type = AccountTypeOAuth
	resp.Body = io.NopCloser(strings.NewReader(sessionAdmissionTestBody))
	_, err, handled = s.handleSessionAdmissionResponse(c, account, resp, "claude-opus-5-5", "claude-opus-5-5", false, time.Now())
	require.False(t, handled)
	require.NoError(t, err)
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, sessionAdmissionTestBody, string(got))
}

//go:build unit

package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestGrokCredentialBudgetExhaustionHasAccurateClientDiagnostic(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, protocol := range []string{"responses", "chat"} {
		t.Run(protocol, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			failoverErr := &service.UpstreamFailoverError{
				Stage:             service.GatewayFailureStageAccountAuth,
				Scope:             service.GatewayFailureScopeRequest,
				Reason:            service.GrokCredentialReasonFailoverTimeout,
				NextAccountAction: service.NextAccountStop,
				ClientMessage:     "access_token=must-not-leak",
			}
			if protocol == "responses" {
				(&OpenAIGatewayHandler{}).handleFailoverExhausted(c, failoverErr, false)
			} else {
				(&GatewayHandler{}).handleCCFailoverExhausted(c, failoverErr, false)
			}
			require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
			require.Contains(t, recorder.Body.String(), "credential failover budget exhausted")
			require.NotContains(t, recorder.Body.String(), "No healthy")
			require.NotContains(t, recorder.Body.String(), "must-not-leak")
		})
	}
}

func TestGrokCredentialBudgetDiagnosticDoesNotOverrideAccountFailures(t *testing.T) {
	for _, reason := range []service.GatewayFailureReason{
		service.GrokCredentialReasonRevoked,
		service.GrokCredentialReasonMissing,
		service.GrokCredentialReasonRefreshTransient,
	} {
		status, message := credentialFailoverClientResponse(&service.UpstreamFailoverError{
			Stage:  service.GatewayFailureStageAccountAuth,
			Scope:  service.GatewayFailureScopeAccount,
			Reason: reason,
		})
		require.Equal(t, http.StatusServiceUnavailable, status)
		require.Equal(t, service.GrokCredentialUnavailableClientMessage, message)
	}
}

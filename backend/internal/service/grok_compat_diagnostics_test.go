//go:build unit

package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/model"
	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestGrokCompatErrorRetainsDiagnosticAndClientPolicy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, protocol := range []struct {
		path  string
		write compatErrorWriter
	}{
		{"/v1/chat/completions", writeChatCompletionsError},
		{"/v1/messages", writeAnthropicError},
	} {
		for _, shape := range []struct{ name, body, client string }{
			{"string", `{"error":"invalid tool definition; Bearer bearer-secret","access_token":"body-secret"}`, "Upstream error: 400"},
			{"nested error", `{"error":{"error":"invalid tool definition; Bearer bearer-secret"},"access_token":"body-secret"}`, "Upstream error: 400"},
			{"nested message", `{"error":{"message":"invalid tool definition"},"access_token":"body-secret"}`, "invalid tool definition"},
		} {
			for _, logBody := range []bool{false, true} {
				for _, ruleMode := range []string{"none", "custom", "passthrough"} {
					t.Run(fmt.Sprintf("%s/%s/log=%t/%s", protocol.path, shape.name, logBody, ruleMode), func(t *testing.T) {
						repo := &grokQuotaAccountRepo{}
						svc := &OpenAIGatewayService{accountRepo: repo, cfg: &config.Config{
							Gateway: config.GatewayConfig{LogUpstreamErrorBody: logBody, LogUpstreamErrorBodyMaxBytes: 128},
						}}
						account := &Account{ID: 227, Platform: PlatformGrok, Type: AccountTypeOAuth}
						recorder := httptest.NewRecorder()
						c, _ := gin.CreateTestContext(recorder)
						c.Request = httptest.NewRequest(http.MethodPost, protocol.path, nil)
						if ruleMode != "none" {
							rule := newNonFailoverPassthroughRule(http.StatusBadRequest, "invalid tool definition", http.StatusTeapot, "oneOf/image template")
							rule.PassthroughBody = ruleMode == "passthrough"
							rules := &ErrorPassthroughService{}
							rules.setLocalCache([]*model.ErrorPassthroughRule{rule})
							BindErrorPassthroughService(c, rules)
						}
						resp := &http.Response{StatusCode: http.StatusBadRequest, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(shape.body))}
						result, err := svc.handleCompatErrorResponse(resp, c, account, protocol.write, "grok-4.7-fast")
						require.Nil(t, result)
						require.ErrorContains(t, err, "invalid tool definition")
						require.Contains(t, c.GetString(OpsUpstreamErrorMessageKey), "invalid tool definition")
						require.NotContains(t, err.Error(), "bearer-secret")
						require.NotContains(t, c.GetString(OpsUpstreamErrorMessageKey), "bearer-secret")
						detail := c.GetString(OpsUpstreamErrorDetailKey)
						if logBody {
							require.Contains(t, detail, "invalid tool definition")
							require.LessOrEqual(t, len(detail), 128)
						} else {
							require.Empty(t, detail)
						}
						require.NotContains(t, detail, "bearer-secret")
						require.NotContains(t, detail, "body-secret")
						message := gjson.GetBytes(recorder.Body.Bytes(), "error.message").String()
						if ruleMode == "none" {
							require.Equal(t, http.StatusBadRequest, recorder.Code)
							require.Equal(t, shape.client, message)
							raw, ok := c.Get(OpsUpstreamErrorsKey)
							require.True(t, ok)
							events := raw.([]*OpsUpstreamErrorEvent)
							require.Len(t, events, 1)
							require.Equal(t, c.GetString(OpsUpstreamErrorMessageKey), events[0].Message)
							require.Equal(t, detail, events[0].Detail)
						} else {
							require.Equal(t, http.StatusTeapot, recorder.Code)
							require.Equal(t, "upstream_error", gjson.GetBytes(recorder.Body.Bytes(), "error.type").String())
							if ruleMode == "custom" {
								require.Equal(t, "oneOf/image template", message)
								require.NotContains(t, err.Error(), "oneOf/image template")
							} else {
								require.Contains(t, message, "invalid tool definition")
								require.NotContains(t, message, "bearer-secret")
							}
						}
						require.True(t, IsResponseCommitted(c))
						require.Zero(t, repo.tempUnschedCalls)
						require.Zero(t, repo.rateLimitedCalls)
						require.Zero(t, repo.updateCalls)
						require.False(t, svc.isOpenAIAccountRuntimeBlocked(account))
					})
				}
			}
		}
	}
}

func TestCompatDiagnosticFallbackAndNonGrokUnchanged(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tt := range []struct{ name, platform, body, want string }{
		{"unknown Grok shape", PlatformGrok, `{"code":"invalid-argument"}`, "Upstream error: 400"},
		{"OpenAI string stays generic", PlatformOpenAI, `{"error":"invalid tool definition"}`, "Upstream error: 400"},
		{"OpenAI message unchanged", PlatformOpenAI, `{"error":{"message":"invalid tool definition"}}`, "invalid tool definition"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			svc := &OpenAIGatewayService{}
			resp := &http.Response{StatusCode: http.StatusBadRequest, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tt.body))}
			_, err := svc.handleCompatErrorResponse(resp, c, &Account{ID: 1, Platform: tt.platform}, writeChatCompletionsError)
			require.ErrorContains(t, err, tt.want)
			require.Equal(t, http.StatusBadRequest, recorder.Code)
			require.Equal(t, tt.want, gjson.GetBytes(recorder.Body.Bytes(), "error.message").String())
			require.Equal(t, tt.want, c.GetString(OpsUpstreamErrorMessageKey))
			require.Empty(t, c.GetString(OpsUpstreamErrorDetailKey))
		})
	}
}

func TestErrorPassthroughNonGrokExtractionUnchanged(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, platform := range []string{PlatformOpenAI, PlatformAnthropic} {
		for _, tt := range []struct{ body, want string }{
			{`{"error":"invalid tool definition"}`, ""},
			{`{"error":{"message":"invalid tool definition"}}`, "invalid tool definition"},
		} {
			t.Run(platform+tt.body, func(t *testing.T) {
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				rule := newNonFailoverPassthroughRule(http.StatusBadRequest, "invalid tool definition", http.StatusTeapot, "unused template")
				rule.PassthroughBody = true
				rules := &ErrorPassthroughService{}
				rules.setLocalCache([]*model.ErrorPassthroughRule{rule})
				BindErrorPassthroughService(c, rules)
				status, errType, message, matched := applyErrorPassthroughRule(c, platform, http.StatusBadRequest, []byte(tt.body), http.StatusBadGateway, "api_error", "fallback")
				require.True(t, matched)
				require.Equal(t, http.StatusTeapot, status)
				require.Equal(t, "upstream_error", errType)
				require.Equal(t, tt.want, message)
			})
		}
	}
}

func TestGrokCompatFailoverKeepsDiagnosticWithoutReclassifying(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, logBody := range []bool{false, true} {
		t.Run(fmt.Sprintf("log=%t", logBody), func(t *testing.T) {
			body := []byte(`{"error":"input[2]: could not decode ModelInput; server is overloaded; Bearer bearer-secret","access_token":"body-secret"}`)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			repo := &grokQuotaAccountRepo{}
			svc := &OpenAIGatewayService{accountRepo: repo, cfg: &config.Config{Gateway: config.GatewayConfig{LogUpstreamErrorBody: logBody}}}
			account := &Account{ID: 227, Platform: PlatformGrok, Type: AccountTypeOAuth}
			resp := &http.Response{StatusCode: http.StatusUnprocessableEntity, Header: make(http.Header)}
			err := svc.failoverOpenAIUpstreamHTTPError(context.Background(), c, account, resp, body, "", "grok-4.7-fast")
			require.NotNil(t, err)
			require.Equal(t, http.StatusUnprocessableEntity, err.StatusCode)
			require.Equal(t, body, err.ResponseBody)
			require.False(t, err.RequestScopedTransient)
			require.False(t, err.RetryableOnSameAccount)
			require.False(t, c.Writer.Written())
			raw, ok := c.Get(OpsUpstreamErrorsKey)
			require.True(t, ok)
			events := raw.([]*OpsUpstreamErrorEvent)
			require.Len(t, events, 1)
			require.Equal(t, "failover", events[0].Kind)
			require.Contains(t, events[0].Message, "could not decode ModelInput")
			require.Equal(t, c.GetString(OpsUpstreamErrorMessageKey), events[0].Message)
			require.Equal(t, c.GetString(OpsUpstreamErrorDetailKey), events[0].Detail)
			require.Equal(t, logBody, events[0].Detail != "")
			require.NotContains(t, events[0].Message+events[0].Detail, "bearer-secret")
			require.NotContains(t, events[0].Detail, "body-secret")
			require.Zero(t, repo.tempUnschedCalls)
			require.Zero(t, repo.rateLimitedCalls)
			require.Zero(t, repo.updateCalls)
		})
	}
}

func TestGrokChatResponsesBridgeRetainsConfiguredDiagnostic(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, status := range []int{http.StatusBadRequest, http.StatusUnprocessableEntity} {
		for _, logBody := range []bool{false, true} {
			t.Run(fmt.Sprintf("status=%d/log=%t", status, logBody), func(t *testing.T) {
				body := []byte(`{"model":"grok","messages":[{"role":"user","content":"hi"}],"stream":false,"prompt_cache_key":"test-session"}`)
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPost, grokChatRawEndpoint, bytes.NewReader(body))
				c.Set("api_key", &APIKey{ID: 7601})
				account := grokChatBridgeTestAccount(76)
				repo := &grokQuotaAccountRepo{mockAccountRepoForPlatform: &mockAccountRepoForPlatform{accountsByID: map[int64]*Account{account.ID: account}}}
				responseBody := `{"error":"input[2]: could not decode ModelInput; Bearer bearer-secret","access_token":"body-secret"}`
				upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(responseBody))}}
				svc := &OpenAIGatewayService{
					billingService: newLocalFixtureBilling(), httpUpstream: upstream,
					grokTokenProvider: NewGrokTokenProvider(repo, nil), accountRepo: repo,
					cfg: &config.Config{Gateway: config.GatewayConfig{LogUpstreamErrorBody: logBody}},
				}
				result, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, "", "")
				require.Nil(t, result)
				require.Error(t, err)
				require.Equal(t, xai.DefaultCLIBaseURL+"/responses", upstream.lastReq.URL.String())
				raw, ok := c.Get(OpsUpstreamErrorsKey)
				require.True(t, ok)
				events := raw.([]*OpsUpstreamErrorEvent)
				require.NotEmpty(t, events)
				require.Contains(t, events[0].Message, "could not decode ModelInput")
				require.Equal(t, c.GetString(OpsUpstreamErrorMessageKey), events[0].Message)
				require.Equal(t, c.GetString(OpsUpstreamErrorDetailKey), events[0].Detail)
				require.Equal(t, logBody, events[0].Detail != "")
				require.NotContains(t, events[0].Message+events[0].Detail, "bearer-secret")
				require.NotContains(t, events[0].Detail, "body-secret")
				if status == http.StatusUnprocessableEntity {
					var failover *UpstreamFailoverError
					require.ErrorAs(t, err, &failover)
					require.Equal(t, responseBody, string(failover.ResponseBody))
					require.Equal(t, "failover", events[0].Kind)
					require.False(t, c.Writer.Written())
				} else {
					require.Equal(t, status, recorder.Code)
					require.Equal(t, "http_error", events[0].Kind)
				}
				require.Zero(t, repo.tempUnschedCalls)
				require.Zero(t, repo.rateLimitedCalls)
				require.Zero(t, repo.updateCalls)
			})
		}
	}
}

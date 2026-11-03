//go:build unit

package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Exercise the real receipt negotiation, account mapping, forwarding, buffering
// and billing path. Only persistence and the provider transport are doubles.
func TestChatReceiptResponsesShapeUsesUpstreamStream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name                                                string
		stream, normalChat, tools, nullMessages, omitStream bool
	}{
		{name: "null_messages_reject_receipt", nullMessages: true},
		{name: "responses_buffered"},
		{name: "responses_default_buffered", omitStream: true},
		{name: "responses_buffered_tool_history", tools: true},
		{name: "responses_streamed", stream: true},
		{name: "normal_chat_buffered", normalChat: true},
		{name: "normal_chat_streamed", normalChat: true, stream: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const model = "gemini-4-argon"
			const upstreamModel = "ultra/gemini-4-argon-high"
			accounts := []service.Account{{ID: 19930, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Credentials: map[string]any{"api_key": "synthetic", "model_mapping": map[string]any{model: upstreamModel}}, Extra: map[string]any{"openai_responses_supported": true}}}
			accountRepo := &grokPartialUsageAccountRepo{grokStreamFailoverAccountRepo: grokStreamFailoverAccountRepo{openAIWSFailoverHandlerAccountRepoStub: openAIWSFailoverHandlerAccountRepoStub{accounts: accounts}}}
			repo := &chatHandlerReceiptRepo{openAIWSUsageHandlerUsageLogRepoStub: openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 3)}}
			billing := &monitorCyberBillingRepo{commands: make(chan *service.UsageBillingCommand, 3)}
			users := &monitorCyberUserRepo{}
			cfg := &config.Config{}
			cfg.Default.RateMultiplier = 1
			cfg.Gateway.MaxAccountSwitches = 0
			cache := service.NewBillingCacheService(nil, users, nil, nil, nil, nil, cfg, nil)
			t.Cleanup(cache.Stop)
			input := `[{"role":"user","content":[{"type":"input_text","text":"Return exactly synthetic."}]}]`
			tools := `[{"type":"function","name":"lookup","description":"Synthetic lookup","parameters":{"type":"object","properties":{"label":{"type":"string"}}}}]`
			if tc.tools {
				input = `[{"role":"user","content":[{"type":"input_text","text":"Look up the label."}]},{"type":"function_call","call_id":"call_synthetic","name":"lookup","arguments":"{\"label\":\"synthetic\"}"},{"type":"function_call_output","call_id":"call_synthetic","output":"{\"value\":7}"},{"role":"user","content":[{"type":"input_text","text":"Return exactly synthetic."}]}]`
			}
			final := `{"id":"resp_synthetic","object":"response","status":"completed","model":"` + upstreamModel + `","output":[{"id":"msg_synthetic","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"synthetic"}]}],"usage":{"input_tokens":100,"output_tokens":7,"input_tokens_details":{"cached_tokens":90},"output_tokens_details":{"reasoning_tokens":0}}}`
			var sent []byte
			upstream := &grokMediaSlotUpstream{call: func(req *http.Request, _ int64) (*http.Response, error) {
				require.Equal(t, "/v1/responses", req.URL.Path)
				var err error
				sent, err = io.ReadAll(req.Body)
				require.NoError(t, err)
				require.Equal(t, upstreamModel, gjson.GetBytes(sent, "model").String())
				require.False(t, gjson.GetBytes(sent, "messages").Exists())
				require.Equal(t, int64(128), gjson.GetBytes(sent, "max_output_tokens").Int())
				if tc.normalChat {
					require.Contains(t, string(sent), "Return exactly synthetic.")
				} else {
					require.JSONEq(t, input, gjson.GetBytes(sent, "input").Raw)
				}
				if tc.tools {
					require.JSONEq(t, tools, gjson.GetBytes(sent, "tools").Raw)
					require.Equal(t, "none", gjson.GetBytes(sent, "tool_choice").String())
				}
				// Behave like a real Responses endpoint: stream:false returns ordinary JSON.
				responseBody := final
				kind := "application/json"
				if gjson.GetBytes(sent, "stream").Bool() {
					responseBody = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"synthetic\"}\n\ndata: {\"type\":\"response.completed\",\"response\":" + final + "}\n\n"
					kind = "text/event-stream"
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{kind}}, Body: io.NopCloser(strings.NewReader(responseBody))}, nil
			}}
			gateway := service.NewOpenAIGatewayService(accountRepo, repo, billing, users, nil, nil, nil, cfg, nil, nil, service.NewBillingService(cfg, nil), nil, cache, upstream, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil)
			h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), cache, service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
			router := gin.New()
			router.Use(middleware.ClientRequestID())
			router.Use(func(c *gin.Context) {
				gid := int64(14230)
				c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{ID: 11830, UserID: 11730, GroupID: &gid, User: &service.User{ID: 11730, Status: service.StatusActive, Balance: 100}, Group: &service.Group{ID: gid, Hydrated: true, Platform: service.PlatformOpenAI, Status: service.StatusActive, RateMultiplier: 1}})
				c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 11730})
				c.Next()
			})
			router.POST(service.ChatReceiptEndpoint, h.ChatCompletions)
			messages := "[]"
			if tc.nullMessages {
				messages = "null"
			}
			body := fmt.Sprintf(`{"model":%q,"messages":%s,"input":%s,"max_output_tokens":128,"stream":%t}`, model, messages, input, tc.stream)
			if tc.normalChat {
				body = fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"Return exactly synthetic."}],"max_tokens":128,"stream":%t}`, model, tc.stream)
			}
			if tc.tools {
				body = strings.TrimSuffix(body, "}") + `,"tools":` + tools + `,"tool_choice":"none"}`
			}
			if tc.omitStream {
				body = strings.Replace(body, `,"stream":false`, "", 1)
			}
			id, clientID := uuid.NewString(), uuid.NewString()
			request := httptest.NewRequest("POST", service.ChatReceiptEndpoint, strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set(service.ChatReceiptVersionHeader, "v2")
			request.Header.Set(service.ChatReceiptIDHeader, id)
			request.Header.Set("X-Client-Request-ID", clientID)
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if tc.nullMessages {
				require.Equal(t, 400, response.Code)
				require.Contains(t, response.Body.String(), "Unsupported usage receipt negotiation")
				require.Zero(t, upstream.calls)
				require.Nil(t, repo.claim)
				require.Zero(t, len(billing.commands))
				require.Zero(t, len(repo.created))
				return
			}
			require.Equal(t, 1, upstream.calls)
			require.Equal(t, 200, response.Code, response.Body.String())
			require.True(t, gjson.GetBytes(sent, "stream").Bool())
			require.Equal(t, "v2", response.Header().Get(service.ChatReceiptVersionHeader))
			require.Equal(t, id, response.Header().Get(service.ChatReceiptIDHeader))
			require.NotNil(t, repo.receipt)
			require.Equal(t, "wire_final", repo.receipt.Source)
			require.Equal(t, "completed", repo.receipt.TerminalState)
			require.Equal(t, model, repo.receipt.Model)
			require.Equal(t, id, repo.receipt.RequestID)
			require.Equal(t, 7, repo.receipt.OutputTokens)
			require.Equal(t, 10, repo.receipt.InputTokens)
			require.Equal(t, 90, repo.receipt.CacheReadTokens)
			require.Equal(t, "client:"+response.Header().Get("X-Client-Request-ID"), repo.billingID)
			require.NotEqual(t, "client:"+clientID, repo.billingID, "caller cannot dictate billing identity")
			if tc.stream {
				require.Contains(t, response.Header().Get("Content-Type"), "text/event-stream")
				require.Contains(t, response.Body.String(), "synthetic")
				require.Equal(t, 1, strings.Count(response.Body.String(), "data: [DONE]"))
			} else {
				require.Contains(t, response.Header().Get("Content-Type"), "application/json")
				var decoded struct {
					Choices []struct {
						Message struct {
							Content string `json:"content"`
						} `json:"message"`
						FinishReason string `json:"finish_reason"`
					} `json:"choices"`
				}
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded))
				require.Len(t, decoded.Choices, 1)
				require.Equal(t, "synthetic", decoded.Choices[0].Message.Content)
				require.Equal(t, "stop", decoded.Choices[0].FinishReason)
			}
			require.Len(t, billing.commands, 1)
			charge := <-billing.commands
			require.Equal(t, model, charge.Model)
			require.Equal(t, 10, charge.InputTokens)
			require.Equal(t, 7, charge.OutputTokens)
			require.Equal(t, 90, charge.CacheReadTokens)
			require.Greater(t, charge.BalanceCost, float64(0))
			require.Len(t, repo.created, 1)
			usage := <-repo.created
			require.Equal(t, model, usage.Model)
			require.Equal(t, model, usage.RequestedModel)
			require.NotNil(t, usage.UpstreamModel)
			require.Equal(t, upstreamModel, *usage.UpstreamModel)
			require.Equal(t, charge.RequestID, usage.RequestID)
			require.InDelta(t, charge.BalanceCost, usage.ActualCost, 1e-12)
		})
	}
}

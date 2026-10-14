//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const grokBodyLimitHTML = `<html><head><title>413 Request Entity Too Large</title></head><body><h1>413 Request Entity Too Large</h1><hr>nginx</body></html>`

func TestGrokRequestBodyLimitHTTPProtocols(t *testing.T) {
	for _, body := range []string{grokBodyLimitHTML, `{"error":{"message":"Request payload is too large"}}`} {
		for _, protocol := range []string{"responses", "chat_bridge", "raw_chat", "messages"} {
			for _, committed := range []string{"", "json", "sse"} {
				t.Run(protocol+"/"+committed+"/"+strconv.Itoa(len(body)), func(t *testing.T) {
					result, err, rec, repo := runGrokRefusalHTTP(t, protocol, committed, http.StatusRequestEntityTooLarge, body)
					require.Error(t, err)
					require.Nil(t, result, "unmetered rejection must not create usage")
					var failover *UpstreamFailoverError
					require.False(t, errors.As(err, &failover), "must not select another account")
					require.Zero(t, repo.tempUnschedCalls)
					require.Zero(t, repo.rateLimitedCalls)
					require.Zero(t, repo.updateCalls)
					wantStatus := http.StatusRequestEntityTooLarge
					if committed != "" {
						wantStatus = http.StatusOK
					}
					require.Equal(t, wantStatus, rec.Code)
					payload := strings.TrimSpace(rec.Body.String())
					errorPath := "error"
					if committed == "sse" {
						require.Contains(t, rec.Header().Get("Content-Type"), "text/event-stream")
						require.Equal(t, 1, strings.Count(payload, "data: "))
						_, payload, _ = strings.Cut(payload, "data: ")
						if protocol == "responses" {
							require.Equal(t, "response.failed", gjson.Get(payload, "type").String())
							errorPath = "response.error"
						} else if protocol == "messages" {
							require.Contains(t, rec.Body.String(), "event: error\n")
						}
					}
					require.True(t, gjson.Valid(payload), "one complete protocol error")
					typeField, wantType := ".type", "invalid_request_error"
					if protocol == "responses" && committed == "sse" {
						typeField, wantType = ".code", "request_too_large"
					}
					require.Equal(t, wantType, gjson.Get(payload, errorPath+typeField).String())
					require.Equal(t, OpenAIRequestBodyTooLargeClientMessage, gjson.Get(payload, errorPath+".message").String())
					require.False(t, gjson.Get(payload, "usage").Exists())
					require.NotContains(t, payload, "response.completed")
				})
			}
		}
	}
}

func TestGrokRequestBodyLimitCannotChangeAccountState(t *testing.T) {
	repo := &grokQuotaAccountRepo{}
	svc := &OpenAIGatewayService{accountRepo: repo}
	account := grokProtocolAPIKeyAccount(29601)
	until := time.Now().Add(time.Hour)
	account.RateLimitResetAt = &until
	body := []byte(`{"error":{"message":"Request payload is too large; free usage exhausted"}}`)
	headers := http.Header{"Retry-After": {"600"}, "X-Ratelimit-Remaining": {"0"}}
	require.False(t, svc.shouldFailoverGrokUpstreamError(http.StatusRequestEntityTooLarge, body))
	svc.handleGrokAccountUpstreamError(context.Background(), account, http.StatusRequestEntityTooLarge, headers, body)
	require.Zero(t, repo.tempUnschedCalls)
	require.Zero(t, repo.rateLimitedCalls)
	require.Zero(t, repo.updateCalls)
	require.Equal(t, until, *account.RateLimitResetAt, "an existing real limit is preserved")
	for _, status := range []int{http.StatusUnauthorized, http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable} {
		require.True(t, svc.shouldFailoverGrokUpstreamError(status, []byte(`{"error":"upstream unavailable"}`)), "status %d retains failover", status)
	}
}

func TestGrokRequestBodyLimitRetainsMeasuredUsage(t *testing.T) {
	for _, protocol := range []string{"responses", "chat_bridge", "raw_chat", "messages"} {
		t.Run(protocol, func(t *testing.T) {
			body := `{"error":{"type":"invalid_request_error","message":"Request payload is too large"},"usage":{"input_tokens":37,"output_tokens":2}}`
			result, err, rec, repo := runGrokRefusalHTTP(t, protocol, "", http.StatusRequestEntityTooLarge, body)
			require.Error(t, err)
			require.NotNil(t, result)
			require.Equal(t, 37, result.Usage.InputTokens)
			require.Equal(t, 2, result.Usage.OutputTokens)
			require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
			require.True(t, gjson.GetBytes(rec.Body.Bytes(), "usage").IsObject())
			require.Zero(t, repo.tempUnschedCalls)
			require.Zero(t, repo.rateLimitedCalls)
			var failover *UpstreamFailoverError
			require.False(t, errors.As(err, &failover))
		})
	}
}

func TestGrokRequestBodyLimitWSHTTPBridge(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, turn := range []int{1, 2} {
		t.Run(strconv.Itoa(turn), func(t *testing.T) {
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: http.StatusRequestEntityTooLarge, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(grokBodyLimitHTML))}}
			repo := &grokQuotaAccountRepo{}
			svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream, accountRepo: repo}
			account := grokProtocolAPIKeyAccount(29602)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
			payload := []byte(`{"type":"response.create","model":"grok-4.6","input":"offline fixture"}`)
			var writes [][]byte
			result, err := svc.proxyOpenAIWSHTTPBridgeTurn(context.Background(), c, account, "offline-only", payload, len(payload), "grok-4.6", "", "", "", "", turn, func(message []byte) error {
				writes = append(writes, append([]byte(nil), message...))
				return nil
			})
			require.Error(t, err)
			require.Nil(t, result)
			var failover *UpstreamFailoverError
			require.False(t, errors.As(err, &failover))
			require.Len(t, upstream.requests, 1)
			require.Len(t, writes, 1)
			require.EqualValues(t, http.StatusRequestEntityTooLarge, gjson.GetBytes(writes[0], "status").Int())
			require.Zero(t, repo.tempUnschedCalls)
			require.Zero(t, repo.rateLimitedCalls)
			require.Zero(t, repo.updateCalls)
		})
	}
}

func TestGrokRequestBodyLimitMediaKeepsProtocol(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, heartbeat := range []bool{false, true} {
		t.Run(strconv.FormatBool(heartbeat), func(t *testing.T) {
			repo := &grokQuotaAccountRepo{}
			svc := &OpenAIGatewayService{accountRepo: repo}
			account := grokProtocolAPIKeyAccount(29603)
			account.Credentials["custom_error_codes_enabled"] = true
			account.Credentials["custom_error_codes"] = []any{float64(http.StatusTooManyRequests)}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/edits", nil)
			if heartbeat {
				stop := StartOpenAIJSONKeepalive(c, time.Hour)
				defer stop()
				require.True(t, openAIImagesJSONKeepaliveFromContext(c).beat())
			}
			resp := &http.Response{StatusCode: http.StatusRequestEntityTooLarge, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(grokBodyLimitHTML))}
			result, err := svc.handleGrokMediaErrorResponse(context.Background(), resp, c, account, "offline-request", "grok-imagine-image-2.0")
			require.Error(t, err)
			require.Nil(t, result)
			var failover *UpstreamFailoverError
			require.False(t, errors.As(err, &failover))
			wantStatus := http.StatusRequestEntityTooLarge
			if heartbeat {
				wantStatus = http.StatusOK
			}
			require.Equal(t, wantStatus, recorder.Code)
			require.True(t, gjson.ValidBytes(recorder.Body.Bytes()))
			require.Equal(t, OpenAIRequestBodyTooLargeClientMessage, gjson.GetBytes(recorder.Body.Bytes(), "error.message").String())
			require.Equal(t, http.StatusRequestEntityTooLarge, c.GetInt(OpsUpstreamStatusCodeKey))
			require.Zero(t, repo.tempUnschedCalls)
			require.Zero(t, repo.rateLimitedCalls)
			require.Zero(t, repo.updateCalls)
		})
	}
}

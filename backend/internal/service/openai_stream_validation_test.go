package service

import (
	"context"
	"errors"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const invalidGrokFrame = `{"sequence_number":0,"type":"error","code":null,"message":"Invalid arguments passed to the model.","param":null}`

func TestGrokBareValidationIsClientError(t *testing.T) {
	payload := []byte(invalidGrokFrame)
	require.Equal(t, 400, openAIStreamFailureStatus(payload, extractOpenAISSEErrorMessage(payload)))
	require.False(t, openAIStreamFailedEventShouldFailover(payload, extractOpenAISSEErrorMessage(payload)))
	require.False(t, openAIStreamErrorEventShouldFailover(payload, extractOpenAISSEErrorMessage(payload)))
	for _, message := range []string{"Invalid argument while processing server state, try again", "Internal server error", "Concurrency limit exceeded for account, please retry later"} {
		require.False(t, isOpenAIStreamInvalidArguments([]byte(`{"type":"error"}`), message))
	}
}
func TestGrokBareValidationDeliveryAndUsage(t *testing.T) {
	for _, committed := range []bool{false, true} {
		for _, nonstream := range []bool{false, true} {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			if committed {
				c.Header("Content-Type", "text/event-stream")
				_, _ = c.Writer.WriteString(": ping\n\n")
				c.Writer.Flush()
			}
			svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}
			account := &Account{ID: 922801, Platform: PlatformGrok, Type: AccountTypeAPIKey}
			payload := `{"type":"response.failed","response":{"status":"failed","error":{"code":"invalid_argument","message":"Invalid arguments passed to the model.","param":"tools"},"usage":{"input_tokens":17,"output_tokens":3}}}`
			body := "event: response.failed\ndata: " + payload + "\n\n"
			resp := &http.Response{StatusCode: 200, Header: http.Header{"X-Request-Id": []string{"grok-test"}}, Body: io.NopCloser(strings.NewReader(body))}
			var err error
			if nonstream {
				r, e := svc.handleSSEToJSON(resp, c, account, []byte(body), "grok-4.7", "grok-4.7")
				err = e
				require.NotNil(t, r)
				require.Equal(t, 17, r.usage.InputTokens)
			} else {
				r, e := svc.handleStreamingResponse(context.Background(), resp, c, account, time.Now(), "grok-4.7", "grok-4.7")
				err = e
				require.NotNil(t, r)
				require.Equal(t, 17, r.usage.InputTokens)
			}
			require.Error(t, err)
			var retry *UpstreamFailoverError
			require.False(t, errors.As(err, &retry))
			if committed {
				require.Equal(t, 200, recorder.Code)
				require.Contains(t, recorder.Body.String(), "response.failed")
			} else {
				require.Equal(t, 400, recorder.Code)
			}
			require.Contains(t, recorder.Body.String(), "invalid_argument")
			require.Contains(t, recorder.Body.String(), `"input_tokens":17`)
			events, _ := c.Get(OpsUpstreamErrorsKey)
			require.Len(t, events, 1)
			upstreamErrors, ok := events.([]*OpsUpstreamErrorEvent)
			require.True(t, ok)
			require.Equal(t, 400, upstreamErrors[0].UpstreamStatusCode)
		}
	}
}
func TestGrokBareValidationStreamingBeforeOutput(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}
	resp := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("data: " + invalidGrokFrame + "\n\n"))}
	_, err := svc.handleStreamingResponse(context.Background(), resp, c, &Account{ID: 7, Platform: PlatformGrok}, time.Now(), "grok-4.7", "grok-4.7")
	require.Error(t, err)
	require.Equal(t, 400, recorder.Code)
	require.Equal(t, "invalid_request_error", gjson.Get(recorder.Body.String(), "error.type").String())
}

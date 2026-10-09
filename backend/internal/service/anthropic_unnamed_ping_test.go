package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestAnthropicPassthroughNamesOnlyUnambiguousPings(t *testing.T) {
	const ping = "data: {\"type\":\"ping\"}\n\n"
	const finish = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":13}}}\n\n" +
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":7}}\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	for _, tc := range []struct{ name, input, want string }{
		{"unnamed", ping, "event: ping\n" + ping},
		{"after comment", ": waiting\n" + ping, ": waiting\nevent: ping\n" + ping},
		{"already named", "event: ping\n" + ping, "event: ping\n" + ping},
		{"explicit event", "event: custom\n" + ping, "event: custom\n" + ping},
		{"frame reset", "event: ping\n" + ping + ping, "event: ping\n" + ping + "event: ping\n" + ping},
		{"openai", "data: {\"object\":\"chat.completion.chunk\",\"choices\":[]}\n\n", "data: {\"object\":\"chat.completion.chunk\",\"choices\":[]}\n\n"},
		{"ambiguous", "data: {\"type\":\"ping\",\"choices\":[]}\n\n", "data: {\"type\":\"ping\",\"choices\":[]}\n\n"},
		{"unknown", "data: {\"type\":\"custom\"}\n\n", "data: {\"type\":\"custom\"}\n\n"},
		{"multiline", "data: {\n data: \"type\":\"ping\"}\n\n", "data: {\n data: \"type\":\"ping\"}\n\n"},
		{"extra data", "data: {\"type\":\"ping\"}\ndata: other\n\n", "data: {\"type\":\"ping\"}\ndata: other\n\n"},
		{"trailing name", "data: {\"type\":\"ping\"}\nevent: custom\n\n", "data: {\"type\":\"ping\"}\nevent: custom\n\n"},
		{"prior data", "data: other\ndata: {\"type\":\"ping\"}\n\n", "data: other\ndata: {\"type\":\"ping\"}\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			resp := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tc.input + finish))}
			svc := &GatewayService{}
			result, err := svc.handleStreamingResponseAnthropicAPIKeyPassthrough(context.Background(), resp, c, &Account{ID: 1}, time.Now(), "claude-opus-5-5")
			require.NoError(t, err)
			require.Equal(t, tc.want+finish, rec.Body.String())
			require.Equal(t, 13, result.usage.InputTokens)
			require.Equal(t, 7, result.usage.OutputTokens)
		})
	}
}

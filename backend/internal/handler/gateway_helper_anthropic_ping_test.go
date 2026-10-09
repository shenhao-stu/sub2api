package handler

import (
	"bufio"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestConcurrencyWaitEmitsNamedAnthropicPing(t *testing.T) {
	for _, slot := range []string{"user", "account"} {
		t.Run(slot, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
			helper := NewConcurrencyHelper(service.NewConcurrencyService(&concurrencyCacheMock{}), SSEPingFormatClaude, time.Millisecond)
			started := false
			release, err := helper.waitForSlotWithPingTimeout(c, slot, 1, 1, 100*time.Millisecond, true, &started, false)
			require.Error(t, err)
			require.Nil(t, release)
			require.True(t, started)
			require.True(t, rec.Flushed)

			// Anthropic clients and downstream protocol guards classify the event
			// before any content arrives; a data-only heartbeat changes its type.
			scanner := bufio.NewScanner(strings.NewReader(rec.Body.String()))
			require.True(t, scanner.Scan())
			require.Equal(t, "event: ping", scanner.Text())
			require.True(t, scanner.Scan())
			var payload map[string]string
			require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(scanner.Text(), "data: ")), &payload))
			require.Equal(t, "ping", payload["type"])
			require.True(t, scanner.Scan())
			require.Empty(t, scanner.Text())
		})
	}
}

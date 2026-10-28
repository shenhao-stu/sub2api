//go:build unit

package service

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestAntigravityMalformedToolPreservesObservedUsage(t *testing.T) {
	failure := "data: {\"response\":{\"candidates\":[{\"finishReason\":\"MALFORMED_FUNCTION_CALL\"}]}}\n\n"
	meter := "data: {\"response\":{\"usageMetadata\":{\"promptTokenCount\":13,\"candidatesTokenCount\":7,\"cachedContentTokenCount\":3,\"thoughtsTokenCount\":2}}}\n\n"
	for _, order := range []string{"before", "after", "missing"} {
		for _, protocol := range []string{"chat", "responses", "claude"} {
			t.Run(order+"/"+protocol, func(t *testing.T) {
				frames := failure
				if order == "before" {
					frames = meter + failure
				}
				if order == "after" {
					frames += meter
				}
				s := newAntigravityCompatService(config.GatewayConfig{MaxLineSize: defaultMaxLineSize}, nil)
				c, rec := newAntigravityCompatContext(http.MethodPost, "/v1/messages", nil)
				handle := s.handleChatCompletionsNonStreamingFromAntigravity
				if protocol == "responses" {
					handle = s.handleResponsesNonStreamingFromAntigravity
				}
				if protocol == "claude" {
					handle = s.handleClaudeStreamToNonStreaming
				}
				result, err := handle(c, &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(frames))}, time.Now(), "fixture")
				require.Error(t, err)
				require.Equal(t, 502, rec.Code)
				require.Contains(t, rec.Body.String(), "malformed_function_call")
				if order == "missing" {
					require.Nil(t, result)
					return
				}
				require.NotNil(t, result)
				require.Equal(t, 10, result.usage.InputTokens)
				require.Equal(t, 9, result.usage.OutputTokens)
				require.Equal(t, 3, result.usage.CacheReadInputTokens)
				require.NotContains(t, rec.Body.String(), `"status":"completed"`)
			})
		}
	}
}

func TestAntigravityMalformedUsagePartialAndExplicitZero(t *testing.T) {
	for _, tc := range []struct {
		name, late    string
		input, output int
	}{
		{"partial", `{"promptTokenCount":15}`, 12, 9},
		{"explicit zero", `{"candidatesTokenCount":0,"thoughtsTokenCount":0}`, 10, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frames := "data: {\"response\":{\"usageMetadata\":{\"promptTokenCount\":13,\"cachedContentTokenCount\":3,\"candidatesTokenCount\":7,\"thoughtsTokenCount\":2}}}\n\n" +
				"data: {\"response\":{\"candidates\":[{\"finishReason\":\"MALFORMED_FUNCTION_CALL\"}]}}\n\n" +
				"data: {\"response\":{\"usageMetadata\":" + tc.late + "}}\n\n"
			svc := newAntigravityCompatService(config.GatewayConfig{}, nil)
			c, _ := newAntigravityCompatContext(http.MethodPost, "/v1/messages", nil)
			_, res, err := svc.collectClaudeStreamResponse(c, &http.Response{Body: io.NopCloser(strings.NewReader(frames))}, time.Now(), "fixture")
			require.Error(t, err)
			require.NotNil(t, res)
			require.Equal(t, tc.input, res.usage.InputTokens)
			require.Equal(t, tc.output, res.usage.OutputTokens)
			require.Equal(t, 3, res.usage.CacheReadInputTokens)
		})
	}
}

func TestAntigravityMalformedUsageDrainBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reader, writer := io.Pipe()
		defer writer.Close()
		go func() {
			_, _ = io.WriteString(writer, "data: {\"response\":{\"candidates\":[{\"finishReason\":\"MALFORMED_FUNCTION_CALL\"}],\"usageMetadata\":{\"promptTokenCount\":2}}}\n\n")
		}()
		svc := newAntigravityCompatService(config.GatewayConfig{}, nil)
		c, _ := newAntigravityCompatContext(http.MethodPost, "/v1/messages", nil)
		start := time.Now()
		_, res, err := svc.collectClaudeStreamResponse(c, &http.Response{Body: reader}, start, "fixture")
		require.ErrorContains(t, err, "usage drain timeout")
		require.Equal(t, disconnectedResponseDrainTimeout, time.Since(start))
		require.NotNil(t, res)
		require.Equal(t, 2, res.usage.InputTokens)
		_, err = writer.Write([]byte("late"))
		require.ErrorIs(t, err, io.ErrClosedPipe)
	})
}

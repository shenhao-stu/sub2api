//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type completionFailureUpstream struct {
	HTTPUpstream
	do func(*http.Request) (*http.Response, error)
}

func (u completionFailureUpstream) Do(r *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	return u.do(r)
}

type completionFailureReader struct{}

func (completionFailureReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

type completionFailureWriter struct{ gin.ResponseWriter }

func (completionFailureWriter) Write([]byte) (int, error)       { return 0, io.ErrClosedPipe }
func (completionFailureWriter) WriteString(string) (int, error) { return 0, io.ErrClosedPipe }

func TestGrokCompletedVideoRetainsBillingOnContentFailure(t *testing.T) {
	for _, scenario := range []string{"transport", "http error", "redirect", "read", "write", "unsafe url", "pending"} {
		t.Run(scenario, func(t *testing.T) {
			c, _ := grokMediaContentTestContext(http.MethodGet, "/v1/videos/task/content", nil)
			if scenario == "write" {
				c.Writer = completionFailureWriter{c.Writer}
			}
			calls := 0
			upstream := completionFailureUpstream{do: func(*http.Request) (*http.Response, error) {
				calls++
				if calls == 1 {
					body := `{"status":"done","model":"grok-imagine-video","video":{"url":"https://vidgen.x.ai/test.mp4","duration":9}}`
					if scenario == "unsafe url" {
						body = strings.ReplaceAll(body, "vidgen.x.ai", "untrusted.example")
					}
					if scenario == "pending" {
						body = `{"status":"pending"}`
					}
					return grokMediaContentStatusResponse(body), nil
				}
				if scenario == "transport" || scenario == "pending" {
					return nil, errors.New("fixture content transport failed")
				}
				resp := &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("fake video"))}
				switch scenario {
				case "http error":
					resp.StatusCode = 404
				case "redirect":
					resp.StatusCode = 302
				case "read":
					resp.Body = io.NopCloser(completionFailureReader{})
				}
				return resp, nil
			}}
			svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
			result, err := svc.ForwardGrokMedia(context.Background(), c, grokMediaContentTestAccount(), GrokMediaEndpointVideoContent, "task", nil, "")
			require.Error(t, err)
			if scenario == "pending" {
				require.Nil(t, result, "pending is not a completed video")
				return
			}
			require.NotNil(t, result, "completed generation must survive content delivery errors")
			require.Equal(t, "task", result.ResponseID)
			require.Equal(t, 1, result.VideoCount)
			require.Equal(t, 9, result.VideoDurationSeconds)
			if scenario == "unsafe url" {
				require.Equal(t, 1, calls, "billing must not weaken content URL validation")
			} else {
				require.Equal(t, 2, calls)
			}
		})
	}
}

func TestNativeAnthropicStreamPreservesReportedStartingOutput(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "truncated", true: "cumulative delta"}[terminal], func(t *testing.T) {
			body := "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"output_tokens\":1}}}\n\n"
			if terminal {
				body += "data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":6}}\n\ndata: {\"type\":\"message_stop\"}\n\n"
			}
			c, _ := grokMediaContentTestContext(http.MethodPost, "/v1/messages", nil)
			svc := newNativeAnthropicHangTestService(1)
			result, err := svc.handleNativeAnthropicStreamingResponse(context.Background(), grokMediaContentStatusResponse(body), c,
				&Account{ID: 1, Platform: PlatformZhipu}, "glm-4.7", "glm-4.7", "glm-4.7", nil, time.Now())
			require.NotNil(t, result)
			require.Equal(t, 10, result.Usage.InputTokens)
			if terminal {
				require.NoError(t, err)
				require.Equal(t, 6, result.Usage.OutputTokens, "delta is cumulative, not additive")
			} else {
				require.Error(t, err)
				require.Equal(t, 1, result.Usage.OutputTokens)
			}
		})
	}
}

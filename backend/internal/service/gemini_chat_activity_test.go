//go:build unit

package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const activityCreated = `data: {"type":"response.created","response":{"id":"resp_synthetic","status":"in_progress"}}` + "\n\n"
const activityProgress = `data: {"type":"response.in_progress","response":{"id":"resp_synthetic","status":"in_progress"}}` + "\n\n"
const activityOpaque = `data: {"type":"response.output_item.added","item":{"id":"rs_synthetic","type":"reasoning","encrypted_content":"PRIVATE_OPAQUE","summary":[]}}` + "\n\n"
const activityTerminal = `data: {"type":"response.completed","response":{"id":"resp_synthetic","status":"completed","output":[],"usage":{"input_tokens":1000,"output_tokens":7,"input_tokens_details":{"cached_tokens":900}}}}` + "\n\n"
const activitySourceComment = ": private-upstream-content https://private.invalid/secret\n\n"
const activityPublicComment = ": upstream-activity\n\n"

// Both scanner paths must treat transport activity separately from semantic
// output, success, retry safety, and the final measured usage.
func TestGeminiChatTransportActivityBoundaries(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, async := range []bool{false, true} {
		for _, tc := range []struct {
			name, prefix, tail, model string
			comments                  int
			noUsage                   bool
		}{
			{name: "comment", prefix: activityCreated + activitySourceComment, comments: 1},
			{name: "in_progress", prefix: activityCreated + activityProgress, comments: 1},
			{name: "opaque", prefix: activityCreated + activityOpaque, comments: 1},
			{name: "message_role", prefix: activityCreated + `data: {"type":"response.output_item.added","item":{"id":"msg_synthetic","type":"message","role":"assistant","content":[]}}` + "\n\n", comments: 1},
			{name: "reasoning_part", prefix: activityCreated + `data: {"type":"response.reasoning_summary_part.added","item_id":"rs_synthetic","part":{"type":"summary_text","text":""}}` + "\n\n", comments: 1},
			{name: "output_part", prefix: activityCreated + `data: {"type":"response.content_part.added","item_id":"msg_synthetic","part":{"type":"output_text","text":""}}` + "\n\n", comments: 1},
			{name: "bounded_burst", prefix: activityCreated + strings.Repeat(activitySourceComment, 100), comments: 1},
			{name: "created_only", prefix: activityCreated},
			{name: "prebootstrap_comment", prefix: activitySourceComment},
			{name: "prebootstrap_opaque", prefix: activityOpaque},
			{name: "unknown_event", prefix: activityCreated + `data: {"type":"response.private_extension","delta":"PRIVATE"}` + "\n\n"},
			{name: "invalid_lifecycle", prefix: `data: {"type":"response.created","response":{"id":"r","status":"failed"}}` + "\n\n" + activitySourceComment},
			{name: "invalid_item", prefix: activityCreated + `data: {"type":"response.output_item.added","item":{"type":"reasoning"}}` + "\n\n"},
			{name: "after_terminal", prefix: activityCreated, tail: activitySourceComment},
			{name: "other_model", prefix: activityCreated + activitySourceComment, model: "gpt-5.5"},
			{name: "comments_without_terminal", prefix: activitySourceComment, noUsage: true},
		} {
			t.Run(fmt.Sprintf("async_%t/%s", async, tc.name), func(t *testing.T) {
				cfg := structuredInputRetryTestConfig()
				if async {
					cfg.Gateway.StreamDataIntervalTimeout = 1
				}
				model := tc.model
				if model == "" {
					model = "gemini-4-argon"
				}
				terminal := activityTerminal
				if tc.noUsage {
					terminal = ""
				}
				upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(tc.prefix + terminal + tc.tail))}}
				svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream}
				body := []byte(fmt.Sprintf(`{"model":%q,"stream":true,"messages":[{"role":"user","content":%q}]}`, model, strings.Repeat("synthetic", 9000)))
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(body)))
				result, err := svc.ForwardAsChatCompletions(context.Background(), c, structuredInputRetryTestAccount(), body, "", "")
				require.Len(t, upstream.requests, 1)
				wire := rec.Body.String()
				require.Equal(t, tc.comments, strings.Count(wire, activityPublicComment))
				require.NotContains(t, wire, "private.invalid")
				require.NotContains(t, wire, "PRIVATE_OPAQUE")
				require.NotContains(t, wire, "private-upstream-content")
				if tc.noUsage {
					require.Error(t, err)
					require.Zero(t, result.Usage.InputTokens)
					require.Nil(t, result.FirstTokenMs)
					return
				}
				require.NotNil(t, result)
				require.Equal(t, 1000, result.Usage.InputTokens)
				require.Equal(t, 900, result.Usage.CacheReadInputTokens)
				require.Equal(t, 7, result.Usage.OutputTokens)
				if tc.model != "" {
					require.NoError(t, err)
					return
				}
				require.Error(t, err)
				var failover *UpstreamFailoverError
				require.NotErrorAs(t, err, &failover)
				status := http.StatusBadGateway
				if tc.comments > 0 {
					status = 200
					require.Contains(t, wire, `"status_code":502`)
				}
				require.Equal(t, status, rec.Code)
				require.Contains(t, wire, "upstream_empty_completion")
				require.NotContains(t, wire, "data: [DONE]")
				require.Contains(t, wire, `"input_tokens":1000`)
				require.Contains(t, wire, `"cache_read_input_tokens":900`)
			})
		}
	}
}

func TestGeminiChatActivityCommittedErrorRetainsStatusAndUsage(t *testing.T) {
	for _, async := range []bool{false, true} {
		for _, status := range []int{400, 503} {
			t.Run(fmt.Sprintf("async_%t/status_%d", async, status), func(t *testing.T) {
				cfg := structuredInputRetryTestConfig()
				if async {
					cfg.Gateway.StreamDataIntervalTimeout = 1
				}
				payload := activityCreated + activitySourceComment + fmt.Sprintf(`data: {"type":"response.failed","response":{"status":"failed","error":{"type":"upstream_error","status_code":%d,"code":"synthetic_rejection","message":"synthetic rejected request via https://derouter.invalid/private?api_key=PRIVATEKEY Bearer PRIVATEBEARER"},"usage":{"input_tokens":1000,"output_tokens":7,"input_tokens_details":{"cached_tokens":900}}}}`, status) + "\n\n"
				upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(payload))}}
				svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream}
				body := []byte(`{"model":"gemini-4-argon","stream":true,"messages":[{"role":"user","content":"synthetic"}]}`)
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(body)))
				result, err := svc.ForwardAsChatCompletions(context.Background(), c, structuredInputRetryTestAccount(), body, "", "")
				require.Error(t, err)
				var failover *UpstreamFailoverError
				require.NotErrorAs(t, err, &failover)
				require.Len(t, upstream.requests, 1)
				require.Equal(t, 200, rec.Code)
				require.Contains(t, rec.Body.String(), activityPublicComment)
				require.NotContains(t, rec.Body.String(), "[DONE]")
				require.NotContains(t, rec.Body.String(), "derouter.invalid")
				require.NotContains(t, rec.Body.String(), "PRIVATEKEY")
				require.NotContains(t, rec.Body.String(), "PRIVATEBEARER")
				errorsFound := 0
				for _, line := range strings.Split(rec.Body.String(), "\n") {
					if !strings.HasPrefix(line, "data:") {
						continue
					}
					obj := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
					if !gjson.Get(obj, "error").Exists() {
						continue
					}
					errorsFound++
					require.EqualValues(t, status, gjson.Get(obj, "error.status_code").Int())
					require.Equal(t, "synthetic_rejection", gjson.Get(obj, "error.code").String())
					require.EqualValues(t, 1000, gjson.Get(obj, "usage.input_tokens").Int())
					require.EqualValues(t, 900, gjson.Get(obj, "usage.cache_read_input_tokens").Int())
					require.EqualValues(t, 7, gjson.Get(obj, "usage.output_tokens").Int())
				}
				require.Equal(t, 1, errorsFound)
				require.Equal(t, 1000, result.Usage.InputTokens)
				require.Equal(t, 7, result.Usage.OutputTokens)
			})
		}
	}
}

type activityFailWriter struct {
	gin.ResponseWriter
	cancel context.CancelFunc
	writes int
}

func (w *activityFailWriter) Write(p []byte) (int, error) {
	w.writes++
	w.cancel()
	return 0, errors.New("synthetic disconnected")
}
func (w *activityFailWriter) WriteString(p string) (int, error) { return w.Write([]byte(p)) }

func TestGeminiChatActivityWriteFailureKeepsLateUsage(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async_%t", async), func(t *testing.T) {
			cfg := structuredInputRetryTestConfig()
			if async {
				cfg.Gateway.StreamDataIntervalTimeout = 1
			}
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(activityCreated + activitySourceComment + activityTerminal))}}
			svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream}
			body := []byte(`{"model":"gemini-4-argon","stream":true,"messages":[{"role":"user","content":"synthetic"}]}`)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(body))).WithContext(ctx)
			failed := &activityFailWriter{ResponseWriter: c.Writer, cancel: cancel}
			c.Writer = failed
			result, _ := svc.ForwardAsChatCompletions(ctx, c, structuredInputRetryTestAccount(), body, "", "")
			require.Len(t, upstream.requests, 1)
			require.Equal(t, 1, failed.writes, "disconnected client must never be written again")
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			require.NotNil(t, result)
			require.Equal(t, 1000, result.Usage.InputTokens)
			require.Equal(t, 900, result.Usage.CacheReadInputTokens)
			require.Equal(t, 7, result.Usage.OutputTokens)
		})
	}
}

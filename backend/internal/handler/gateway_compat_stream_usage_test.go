//go:build unit

package handler

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	middleware "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type compatStreamUsageUpstream struct {
	service.HTTPUpstream
	response *http.Response
	calls    int
}

func (u *compatStreamUsageUpstream) DoWithTLS(*http.Request, string, int64, int, *tlsfingerprint.Profile) (*http.Response, error) {
	u.calls++
	return u.response, nil
}

type compatStreamUsageBillingRepo struct {
	service.UsageBillingRepository
	mu       sync.Mutex
	commands []*service.UsageBillingCommand
}

func (r *compatStreamUsageBillingRepo) Apply(ctx context.Context, cmd *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands = append(r.commands, cmd)
	return &service.UsageBillingApplyResult{Applied: true}, nil
}

const compatStreamUsagePartialStream = "event: message_start\n" +
	`data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"claude-sonnet-4.5","usage":{"input_tokens":12,"output_tokens":1}}}` + "\n\n" +
	"event: content_block_start\n" +
	`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}` + "\n\n" +
	"event: content_block_delta\n" +
	`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"partial"}}` + "\n\n"

const compatStreamUsageCompletion = "event: content_block_stop\n" +
	`data: {"type":"content_block_stop","index":0}` + "\n\n" +
	"event: message_delta\n" +
	`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":7}}` + "\n\n" +
	"event: message_stop\n" +
	`data: {"type":"message_stop"}` + "\n\n"

type compatStreamUsageRun struct {
	rec         *httptest.ResponseRecorder
	upstream    *compatStreamUsageUpstream
	usageRepo   *openAIWSUsageHandlerUsageLogRepoStub
	billingRepo *compatStreamUsageBillingRepo
}

type compatStreamUsageOptions struct {
	accountCount int
	workerPool   *service.UsageRecordWorkerPool
}

// runCompatStreamUsageHandler 经 GatewayHandler 转发一次流式请求；上游写完 upstreamStream 后，
// closeUpstream 为 false 时保持连接不再发送数据。
func runCompatStreamUsageHandler(t *testing.T, endpoint, upstreamStream string, closeUpstream bool, options ...compatStreamUsageOptions) compatStreamUsageRun {
	t.Helper()
	cfg := &config.Config{Gateway: config.GatewayConfig{StreamDataIntervalTimeout: 1}}
	groupID := int64(9300)
	group := &service.Group{ID: groupID, Hydrated: true, Platform: service.PlatformAnthropic, Status: service.StatusActive, RateMultiplier: 1}
	account := &service.Account{
		ID: 9301, Platform: service.PlatformAnthropic, Type: service.AccountTypeAPIKey,
		Status: service.StatusActive, Schedulable: true, Concurrency: 1,
		Credentials:   map[string]any{"api_key": "test-key"},
		AccountGroups: []service.AccountGroup{{AccountID: 9301, GroupID: groupID}},
	}
	accounts := []*service.Account{account}
	if len(options) > 0 && options[0].accountCount > 1 {
		alternative := *account
		alternative.ID = 9401
		alternative.AccountGroups = []service.AccountGroup{{AccountID: 9401, GroupID: groupID}}
		accounts = append(accounts, &alternative)
	}
	snapshot := service.NewSchedulerSnapshotService(&fakeSchedulerCache{accounts: accounts}, nil, nil, nil, nil)
	run := compatStreamUsageRun{
		rec:         httptest.NewRecorder(),
		usageRepo:   &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 2)},
		billingRepo: &compatStreamUsageBillingRepo{},
	}
	billingCache := service.NewBillingCacheService(newHandlerInflightCache(100), nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billingCache.Stop)

	pr, pw := io.Pipe()
	t.Cleanup(func() { _ = pr.Close(); _ = pw.Close() })
	go func() {
		_, _ = io.WriteString(pw, upstreamStream)
		if closeUpstream {
			_ = pw.Close()
		}
	}()
	run.upstream = &compatStreamUsageUpstream{response: &http.Response{
		StatusCode: http.StatusOK, Body: pr,
		Header: http.Header{"X-Request-Id": []string{"compat-stream-usage"}},
	}}
	gw := service.NewGatewayService(
		nil, &fakeGroupRepo{group: group}, run.usageRepo, run.billingRepo, nil, nil, nil, nil, cfg,
		snapshot, nil, service.NewBillingService(cfg, nil), nil, nil, nil, run.upstream,
		&service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	h := &GatewayHandler{
		gatewayService: gw, billingCacheService: billingCache, cfg: cfg, maxAccountSwitches: 1,
		concurrencyHelper: NewConcurrencyHelper(service.NewConcurrencyService(&fakeConcurrencyCache{}), SSEPingFormatClaude, 0),
	}

	if len(options) > 0 {
		h.usageRecordWorkerPool = options[0].workerPool
	}
	body := `{"model":"claude-sonnet-4.5","stream":true,"input":"hello"}`
	if endpoint == "chat/completions" {
		body = `{"model":"claude-sonnet-4.5","stream":true,"messages":[{"role":"user","content":"hello"}]}`
	}
	c, _ := gin.CreateTestContext(run.rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/"+endpoint, bytes.NewBufferString(body))
	c.Request.Header.Set("Content-Type", "application/json")
	apiKey := &service.APIKey{ID: 9302, UserID: 9303, GroupID: &groupID, Group: group, User: &service.User{ID: 9303, Balance: 100}}
	c.Set(string(middleware.ContextKeyAPIKey), apiKey)
	c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 9303})

	if endpoint == "chat/completions" {
		h.ChatCompletions(c)
	} else {
		h.Responses(c)
	}
	return run
}

// compatStreamUsageMarkers 返回成功收尾与终止错误的标记。
func compatStreamUsageMarkers(endpoint string) (success, failure string) {
	if endpoint == "responses" {
		return "event: response.completed", "event: response.failed"
	}
	return "[DONE]", `data: {"error":`
}

func TestGatewayCompatibilityHandlers_StreamUsageRecordedOnceWithTerminalEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, endpoint := range []string{"chat/completions", "responses"} {
		for _, tc := range []struct {
			name       string
			completion string
			wantOutput int
		}{
			{name: "complete", completion: compatStreamUsageCompletion, wantOutput: 7},
			{name: "idle_before_message_stop", wantOutput: 1},
		} {
			t.Run(endpoint+"/"+tc.name, func(t *testing.T) {
				successMarker, failureMarker := compatStreamUsageMarkers(endpoint)
				wantMarker, denyMarker := successMarker, failureMarker
				if tc.completion == "" {
					wantMarker, denyMarker = failureMarker, successMarker
				}

				run := runCompatStreamUsageHandler(t, endpoint, compatStreamUsagePartialStream+tc.completion, tc.completion != "")

				require.Equal(t, 1, run.upstream.calls)
				out := run.rec.Body.String()
				require.Contains(t, out, "partial")
				require.Contains(t, out, wantMarker)
				require.NotContains(t, out, denyMarker)
				require.Equal(t, 1, strings.Count(out, wantMarker), "exactly one terminal event")
				require.NotContains(t, out, `data: {"type":"error"`, "the handler must not append a fallback frame")
				if tc.completion == "" {
					require.Contains(t, out, "stream_timeout")
				}

				run.billingRepo.mu.Lock()
				defer run.billingRepo.mu.Unlock()
				require.Len(t, run.billingRepo.commands, 1, "usage must be settled exactly once")
				require.Equal(t, 12, run.billingRepo.commands[0].InputTokens)
				require.Equal(t, tc.wantOutput, run.billingRepo.commands[0].OutputTokens)
				require.Len(t, run.usageRepo.created, 1)
			})
		}
	}
}

func TestGatewayCompatibilityHandlers_IdleBeforeAnyEventWritesStreamError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, endpoint := range []string{"chat/completions", "responses"} {
		t.Run(endpoint, func(t *testing.T) {
			successMarker, failureMarker := compatStreamUsageMarkers(endpoint)

			run := runCompatStreamUsageHandler(t, endpoint, "", false)

			require.Equal(t, 1, run.upstream.calls)
			require.Equal(t, http.StatusOK, run.rec.Code)
			require.Equal(t, "text/event-stream", run.rec.Header().Get("Content-Type"))
			out := run.rec.Body.String()
			require.Equal(t, 1, strings.Count(out, failureMarker), "exactly one terminal event")
			require.NotContains(t, out, `data: {"type":"error"`, "the handler must not append a fallback frame")
			require.Contains(t, out, "stream_timeout")
			require.NotContains(t, out, successMarker)

			run.billingRepo.mu.Lock()
			defer run.billingRepo.mu.Unlock()
			require.Empty(t, run.billingRepo.commands, "no usage was observed")
			require.Empty(t, run.usageRepo.created)
		})
	}
}

func TestGatewayCompatibilityHandlers_UpstreamErrorEventAfterOutputRecordsObservedUsageOnce(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstreamError := "event: error\n" + `data: {"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}` + "\n\n"
	for _, endpoint := range []string{"chat/completions", "responses"} {
		t.Run(endpoint, func(t *testing.T) {
			successMarker, failureMarker := compatStreamUsageMarkers(endpoint)

			run := runCompatStreamUsageHandler(t, endpoint, compatStreamUsagePartialStream+upstreamError, true)

			require.Equal(t, 1, run.upstream.calls)
			out := run.rec.Body.String()
			require.Contains(t, out, "partial")
			require.Equal(t, 1, strings.Count(out, failureMarker), "exactly one terminal event")
			require.NotContains(t, out, `data: {"type":"error"`, "the handler must not append a fallback frame")
			require.Contains(t, out, "Overloaded")
			require.NotContains(t, out, successMarker)

			run.billingRepo.mu.Lock()
			defer run.billingRepo.mu.Unlock()
			require.Len(t, run.billingRepo.commands, 1, "observed upstream usage must be settled exactly once")
			require.Equal(t, 12, run.billingRepo.commands[0].InputTokens)
			require.Equal(t, 1, run.billingRepo.commands[0].OutputTokens)
			require.Len(t, run.usageRepo.created, 1)
		})
	}
}

func TestGatewayCompatibilityHandlers_ErrorAfterProgressDoesNotReplayOrLoseWireUsage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, endpoint := range []string{"chat/completions", "responses"} {
		for _, tc := range []struct {
			name, usage  string
			input, cache int
			heartbeat    bool
		}{
			{name: "zero_usage", usage: `{"input_tokens":0,"output_tokens":0}`},
			{name: "exclusive_input_with_cache", usage: `{"input_tokens":10,"cache_read_input_tokens":90,"output_tokens":0}`, input: 10, cache: 90},
			{name: "heartbeat_then_cache", usage: `{"input_tokens":10,"cache_read_input_tokens":90,"output_tokens":0}`, input: 10, cache: 90, heartbeat: true},
		} {
			t.Run(endpoint+"/"+tc.name, func(t *testing.T) {
				stream := ""
				if tc.heartbeat {
					stream = "event: ping\ndata: {\"type\":\"ping\"}\n\n"
				}
				stream += "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_test\",\"model\":\"claude-sonnet-4.5\",\"role\":\"assistant\",\"usage\":" + tc.usage + "}}\n\n"
				stream += "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n"
				run := runCompatStreamUsageHandler(t, endpoint, stream, true, compatStreamUsageOptions{accountCount: 2})
				require.Equal(t, 1, run.upstream.calls, "a second eligible account must not replay accepted generation")
				_, failureMarker := compatStreamUsageMarkers(endpoint)
				require.Equal(t, 1, strings.Count(run.rec.Body.String(), failureMarker))
				var terminal gjson.Result
				for _, line := range strings.Split(run.rec.Body.String(), "\n") {
					if !strings.HasPrefix(line, "data: ") {
						continue
					}
					obj := gjson.Parse(strings.TrimPrefix(line, "data: "))
					if endpoint == "responses" {
						if obj.Get("type").String() == "response.failed" {
							terminal = obj.Get("response")
						}
					} else if obj.Get("error").Exists() {
						terminal = obj
					}
				}
				require.True(t, terminal.Exists())
				require.Equal(t, int64(529), terminal.Get("error.status_code").Int())
				run.billingRepo.mu.Lock()
				defer run.billingRepo.mu.Unlock()
				if tc.input+tc.cache == 0 {
					require.False(t, terminal.Get("usage").Exists())
					require.Empty(t, run.billingRepo.commands)
					require.Empty(t, run.usageRepo.created)
					return
				}
				require.Len(t, run.billingRepo.commands, 1)
				require.Equal(t, tc.input, run.billingRepo.commands[0].InputTokens)
				require.Equal(t, tc.cache, run.billingRepo.commands[0].CacheReadTokens)
				require.Zero(t, run.billingRepo.commands[0].OutputTokens)
				require.Len(t, run.usageRepo.created, 1)
				if endpoint == "responses" {
					require.Equal(t, int64(100), terminal.Get("usage.input_tokens").Int())
					require.Equal(t, int64(90), terminal.Get("usage.input_tokens_details.cached_tokens").Int())
				} else {
					require.Equal(t, int64(100), terminal.Get("usage.prompt_tokens").Int())
					require.Equal(t, int64(90), terminal.Get("usage.prompt_tokens_details.cached_tokens").Int())
				}
			})
		}
	}
}

func TestGatewayCompatibilityHandlers_MeteredFailureSurvivesFullDropQueue(t *testing.T) {
	for _, endpoint := range []string{"chat/completions", "responses"} {
		t.Run(endpoint, func(t *testing.T) {
			pool := service.NewUsageRecordWorkerPoolWithOptions(service.UsageRecordWorkerPoolOptions{WorkerCount: 1, QueueSize: 1, TaskTimeout: time.Minute, OverflowPolicy: config.UsageRecordOverflowPolicyDrop})
			started := make(chan struct{})
			release := make(chan struct{})
			t.Cleanup(func() { close(release); pool.Stop() })
			require.Equal(t, service.UsageRecordSubmitModeEnqueued, pool.Submit(func(context.Context) { close(started); <-release }))
			<-started
			require.Equal(t, service.UsageRecordSubmitModeEnqueued, pool.Submit(func(context.Context) {}))
			stream := compatStreamUsagePartialStream + "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n"
			run := runCompatStreamUsageHandler(t, endpoint, stream, true, compatStreamUsageOptions{accountCount: 2, workerPool: pool})
			require.Equal(t, 1, run.upstream.calls)
			run.billingRepo.mu.Lock()
			defer run.billingRepo.mu.Unlock()
			require.Len(t, run.billingRepo.commands, 1, "mandatory partial usage must run despite configured drop queue overflow")
			require.Equal(t, 12, run.billingRepo.commands[0].InputTokens)
			require.Equal(t, 1, run.billingRepo.commands[0].OutputTokens)
			require.Len(t, run.usageRepo.created, 1)
		})
	}
}

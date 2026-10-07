package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository" //nolint:depguard // Integration test exercises the production HTTP transport.
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestGrokJSONKeepaliveHTTP(t *testing.T) {
	gin.SetMode(gin.TestMode)
	old := grokJSONKeepaliveInterval
	grokJSONKeepaliveInterval = 20 * time.Millisecond
	t.Cleanup(func() { grokJSONKeepaliveInterval = old })
	for _, endpoint := range []string{"chat/completions", "responses"} {
		for _, scenario := range []string{"headers", "body", "http_error", "input_error", "protocol_error", "failover", "exhausted", "short_body"} {
			t.Run(endpoint+"/"+scenario, func(t *testing.T) {
				runGrokJSONKeepaliveHTTP(t, endpoint, scenario, service.PlatformGrok, false)
			})
		}
	}
}

func TestGrokJSONKeepaliveLeavesOtherProtocolsUnchanged(t *testing.T) {
	gin.SetMode(gin.TestMode)
	old := grokJSONKeepaliveInterval
	grokJSONKeepaliveInterval = 20 * time.Millisecond
	t.Cleanup(func() { grokJSONKeepaliveInterval = old })
	for _, endpoint := range []string{"chat/completions", "responses"} {
		t.Run(endpoint+"/non_grok", func(t *testing.T) {
			runGrokJSONKeepaliveHTTP(t, endpoint, "headers", service.PlatformOpenAI, false)
		})
		t.Run(endpoint+"/stream", func(t *testing.T) {
			runGrokJSONKeepaliveHTTP(t, endpoint, "headers", service.PlatformGrok, true)
		})
	}
}

func runGrokJSONKeepaliveHTTP(t *testing.T, endpoint, scenario, platform string, stream bool) {
	t.Helper()
	chat := endpoint == "chat/completions"
	wantPing := platform == service.PlatformGrok && !stream
	wantFailover := scenario == "failover" || scenario == "exhausted"
	wantSecondAttempt := wantFailover || (chat && scenario == "protocol_error")
	wantError := scenario != "headers" && scenario != "body" && scenario != "failover"
	model := "grok-4.6"
	if platform != service.PlatformGrok {
		model = "gpt-5.1"
	}
	gates := []chan struct{}{make(chan struct{}), make(chan struct{})}
	var releaseOnce [2]sync.Once
	release := func(i int) { releaseOnce[i].Do(func() { close(gates[i]) }) }
	defer func() { release(0); release(1) }()
	entered := make(chan int, 2)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		attempt := 0
		if r.Header.Get("Authorization") == "Bearer keepalive-9971" {
			attempt = 1
		}
		if scenario == "body" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			require.NoError(t, http.NewResponseController(w).Flush())
		}
		entered <- attempt
		select {
		case <-gates[attempt]:
		case <-r.Context().Done():
			return
		}
		if wantFailover && (attempt == 0 || scenario == "exhausted") {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":{"message":"synthetic unavailable"}}`)
			return
		}
		if scenario == "http_error" || scenario == "input_error" {
			status := http.StatusBadRequest
			message := "synthetic bad request"
			if scenario == "input_error" {
				status = http.StatusUnprocessableEntity
				message = "Failed to decode input[0]: unknown item type synthetic_test_item"
			}
			w.WriteHeader(status)
			_, _ = fmt.Fprintf(w, `{"error":{"message":%q}}`, message)
			return
		}
		if scenario == "short_body" {
			w.Header().Set("Content-Length", "100")
			_, _ = io.WriteString(w, `{"id":`)
			return
		}
		if scenario == "protocol_error" {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"invalid_request_error\",\"message\":\"synthetic terminal failure\"}}}\n\n")
			return
		}
		response := fmt.Sprintf(`{"id":"resp_keepalive","object":"response","status":"completed","model":%q,"output":[],"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}`, model)
		if chat {
			response = fmt.Sprintf(`{"id":"chatcmpl_keepalive","object":"chat.completion","model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":"5"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`, model)
		}
		if stream {
			w.Header().Set("Content-Type", "text/event-stream")
			if chat {
				_, _ = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", response)
			} else {
				_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":%s}\n\n", response)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, response)
	}))
	t.Cleanup(origin.Close)
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Default.RateMultiplier = 1
	cfg.Gateway.MaxAccountSwitches = 1
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Security.URLAllowlist.AllowPrivateHosts = true
	accounts := make([]service.Account, 2)
	for i := range accounts {
		id := int64(9970 + i)
		accounts[i] = service.Account{ID: id, Name: "keepalive-test", Platform: platform,
			Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: i + 1,
			Credentials: map[string]any{"api_key": fmt.Sprintf("keepalive-%d", id), "base_url": origin.URL}}
		if chat {
			accounts[i].Extra = map[string]any{"openai_responses_mode": "force_chat_completions"}
		}
	}
	accountRepo := &grokPartialUsageAccountRepo{grokStreamFailoverAccountRepo: grokStreamFailoverAccountRepo{
		openAIWSFailoverHandlerAccountRepoStub: openAIWSFailoverHandlerAccountRepoStub{accounts: accounts}}}
	usageRepo := &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 3)}
	billingCache := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billingCache.Stop)
	gateway := service.NewOpenAIGatewayService(accountRepo, usageRepo, nil, nil, nil, nil, nil, cfg, nil, nil,
		service.NewBillingService(cfg, nil), nil, billingCache, repository.NewHTTPUpstream(cfg),
		&service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil)
	h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), billingCache,
		service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
	done := make(chan bool, 1)
	router := gin.New()
	router.POST("/v1/"+endpoint, func(c *gin.Context) {
		groupID := int64(4270)
		c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{ID: 1870, GroupID: &groupID,
			User:  &service.User{ID: 1770, Status: service.StatusActive},
			Group: &service.Group{ID: groupID, Platform: platform, Status: service.StatusActive}})
		c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 1770})
		if chat {
			h.ChatCompletions(c)
		} else {
			h.Responses(c)
		}
		done <- service.OpenAIImagesJSONKeepalivePresent(c)
	})
	downstream := httptest.NewServer(router)
	t.Cleanup(downstream.Close)
	body := fmt.Sprintf(`{"model":%q,"input":"test","stream":%t}`, model, stream)
	if chat {
		body = fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"test"}],"stream":%t}`, model, stream)
	}
	type httpResult struct {
		resp *http.Response
		err  error
	}
	responses := make(chan httpResult, 1)
	go func() {
		client := &http.Client{Timeout: 4 * time.Second}
		resp, err := client.Post(downstream.URL+"/v1/"+endpoint, "application/json", strings.NewReader(body))
		responses <- httpResult{resp, err}
	}()
	awaitAttempt := func(want int) {
		t.Helper()
		select {
		case got := <-entered:
			require.Equal(t, want, got)
		case <-time.After(3 * time.Second):
			t.Fatal("upstream attempt did not start")
		}
	}
	awaitAttempt(0)
	var result httpResult
	var received []byte
	readPing := func() {
		t.Helper()
		ping := make([]byte, 2)
		_, err := io.ReadFull(result.resp.Body, ping)
		require.NoError(t, err)
		require.Equal(t, " \n", string(ping), "heartbeat must arrive before gated origin completes")
		received = append(received, ping...)
	}
	if wantPing {
		result = <-responses
		require.NoError(t, result.err)
		defer func() { _ = result.resp.Body.Close() }()
		require.Contains(t, result.resp.Header.Get("Content-Type"), "application/json")
		readPing()
	} else {
		select {
		case result = <-responses:
			t.Fatalf("non-JSON route committed before origin was released: %v", result.err)
		case <-time.After(3 * grokJSONKeepaliveInterval):
		}
	}
	release(0)
	if wantSecondAttempt {
		awaitAttempt(1)
		readPing()
		release(1)
	}
	if !wantPing {
		result = <-responses
		require.NoError(t, result.err)
		defer func() { _ = result.resp.Body.Close() }()
	}
	rest, err := io.ReadAll(result.resp.Body)
	require.NoError(t, err)
	received = append(received, rest...)
	require.Equal(t, wantPing, <-done)
	require.Equal(t, http.StatusOK, result.resp.StatusCode)
	if stream {
		require.Contains(t, result.resp.Header.Get("Content-Type"), "text/event-stream")
		require.False(t, strings.HasPrefix(string(received), " \n"))
	} else {
		var document map[string]any
		require.NoError(t, json.Unmarshal(received, &document), "exactly one JSON object, including after late errors: %s", received)
		require.Equal(t, wantError, document["error"] != nil, string(received))
		if !wantError {
			require.Equal(t, int64(3), gjson.GetBytes(received, "usage.total_tokens").Int())
		}
	}
	if wantError {
		require.Empty(t, usageRepo.created, "unmetered errors must not create usage")
	} else {
		require.Len(t, usageRepo.created, 1)
		usage := <-usageRepo.created
		wantAccount := int64(9970)
		if wantFailover {
			wantAccount++
		}
		require.Equal(t, wantAccount, usage.AccountID)
		require.Equal(t, 2, usage.InputTokens)
		require.Equal(t, 1, usage.OutputTokens)
	}
}

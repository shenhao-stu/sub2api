//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const grokFixedContentRefusal = `{"code":"permission-denied","error":"I can't help with that request."}`

// Exercise the real forwarding code and HTTP response lifecycle without any
// provider traffic. Only the transport destination is replaced with localhost.
type grokRefusalHTTPUpstream struct {
	HTTPUpstream
	client *http.Client
	url    *url.URL
}

func (u *grokRefusalHTTPUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.URL.Scheme, req.URL.Host = u.url.Scheme, u.url.Host
	req.Host = u.url.Host
	return u.client.Do(req)
}

func runGrokRefusalHTTP(t *testing.T, protocol, committed string, status int, body string) (*OpenAIForwardResult, error, *httptest.ResponseRecorder, *grokQuotaAccountRepo) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	defer upstream.Close()
	destination, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	repo := &grokQuotaAccountRepo{}
	svc := openAIClientToolsTestService(nil)
	svc.httpUpstream = &grokRefusalHTTPUpstream{client: upstream.Client(), url: destination}
	svc.accountRepo = repo
	account := &Account{ID: 25001, Platform: PlatformGrok, Type: AccountTypeAPIKey,
		Credentials: map[string]any{"api_key": "offline-only", "base_url": "https://api.x.ai/v1"},
		Extra:       map[string]any{"openai_responses_supported": true}}
	path := "/v1/" + protocol
	if protocol == "chat_bridge" || protocol == "raw_chat" {
		path = "/v1/chat/completions"
	}
	request := []byte(`{"model":"grok-4.6","messages":[{"role":"user","content":"offline fixture"}],"max_tokens":32,"stream":false}`)
	if protocol == "responses" {
		request = []byte(`{"model":"grok-4.6","input":"offline fixture","stream":false}`)
	}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(request)))
	switch committed {
	case "json":
		stop := StartOpenAIJSONKeepalive(c, time.Hour)
		defer stop()
		require.True(t, openAIImagesJSONKeepaliveFromContext(c).beat())
	case "sse":
		c.Header("Content-Type", "text/event-stream")
		_, _ = c.Writer.WriteString(": ping\n\n")
		c.Writer.Flush()
	}
	var result *OpenAIForwardResult
	switch protocol {
	case "responses":
		result, err = svc.forwardGrokResponses(context.Background(), c, account, request, "grok-4.6", false, time.Now())
	case "chat_bridge":
		result, err = svc.forwardGrokChatCompletionsViaResponses(context.Background(), c, account, request, "offline-session", "")
	case "raw_chat":
		result, err = svc.forwardAsRawChatCompletions(context.Background(), c, account, request, "")
	case "messages":
		result, err = svc.ForwardAsAnthropic(context.Background(), c, account, request, "offline-session", "")
	default:
		t.Fatalf("unknown protocol %q", protocol)
	}
	require.EqualValues(t, 1, requests.Load(), "no retry may dispatch a second HTTP request")
	if repo.tempUnschedCalls == 0 {
		require.False(t, svc.isOpenAIAccountRuntimeBlocked(account))
	}
	return result, err, rec, repo
}

func TestGrokFixedContentRefusalHTTPProtocols(t *testing.T) {
	for _, protocol := range []string{"responses", "chat_bridge", "raw_chat", "messages"} {
		for _, committed := range []string{"", "json", "sse"} {
			t.Run(protocol+"/"+committed, func(t *testing.T) {
				result, err, rec, repo := runGrokRefusalHTTP(t, protocol, committed, http.StatusForbidden, grokFixedContentRefusal)
				require.Error(t, err)
				require.Nil(t, result, "an unmetered refusal cannot become successful usage")
				var failover *UpstreamFailoverError
				require.False(t, errors.As(err, &failover))
				require.Zero(t, repo.tempUnschedCalls)
				require.Zero(t, repo.rateLimitedCalls)
				require.Zero(t, repo.updateCalls)
				wantStatus := http.StatusForbidden
				if committed != "" {
					wantStatus = http.StatusOK
				}
				require.Equal(t, wantStatus, rec.Code)
				body := strings.TrimSpace(rec.Body.String())
				errorPath := "error"
				if committed == "sse" {
					require.Contains(t, rec.Header().Get("Content-Type"), "text/event-stream")
					require.Equal(t, 1, strings.Count(body, "data: "))
					_, body, _ = strings.Cut(body, "data: ")
					if protocol == "responses" {
						require.Equal(t, "response.failed", gjson.Get(body, "type").String())
						errorPath = "response.error"
					} else if protocol == "messages" {
						require.Contains(t, rec.Body.String(), "event: error\n")
					}
				}
				var payload map[string]any
				require.NoError(t, json.Unmarshal([]byte(body), &payload), "exactly one complete error object")
				require.Equal(t, "content_policy_violation", gjson.Get(body, errorPath+".code").String())
				require.Equal(t, "I can't help with that request.", gjson.Get(body, errorPath+".message").String())
				require.False(t, gjson.Get(body, "usage").Exists())
				if protocol == "messages" {
					require.Equal(t, "error", gjson.Get(body, "type").String())
				}
			})
		}
	}
}

func TestGrokFixedContentRefusalHTTPRetainsMeasuredUsage(t *testing.T) {
	for _, protocol := range []string{"responses", "chat_bridge", "raw_chat", "messages"} {
		t.Run(protocol, func(t *testing.T) {
			body := `{"code":"permission-denied","error":"I can't help with that request.","usage":{"input_tokens":37,"output_tokens":2}}`
			result, err, rec, repo := runGrokRefusalHTTP(t, protocol, "", http.StatusForbidden, body)
			require.Error(t, err)
			require.NotNil(t, result)
			require.Equal(t, 37, result.Usage.InputTokens)
			require.Equal(t, 2, result.Usage.OutputTokens)
			require.True(t, gjson.GetBytes(rec.Body.Bytes(), "usage").IsObject())
			require.Zero(t, repo.tempUnschedCalls)
			require.Zero(t, repo.rateLimitedCalls)
			var failover *UpstreamFailoverError
			require.False(t, errors.As(err, &failover))
		})
	}
}

func TestGrokRefusalHTTPKeepsAccountFailureSemantics(t *testing.T) {
	for _, tt := range []struct {
		name, body string
		status     int
		cooldown   bool
	}{
		{"unknown permission", `{"code":"permission-denied","error":"Forbidden"}`, 403, false},
		{"plain forbidden", `{"error":{"message":"Forbidden"}}`, 403, false},
		{"entitlement", `{"error":{"code":"subscription_required","message":"I can't help with that request."}}`, 403, true},
		{"invalid key", `{"error":{"code":"invalid_api_key","message":"invalid api key"}}`, 401, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result, err, rec, repo := runGrokRefusalHTTP(t, "chat_bridge", "", tt.status, tt.body)
			require.Error(t, err)
			require.Nil(t, result)
			require.NotContains(t, rec.Body.String(), "content_policy_violation")
			if tt.cooldown {
				require.Positive(t, repo.tempUnschedCalls)
			} else {
				require.Zero(t, repo.tempUnschedCalls)
				require.Zero(t, repo.rateLimitedCalls)
			}
		})
	}
	for _, body := range []string{
		`{"error":{"message":"Quoted: I can't help with that request."}}`,
		`{"error":{"message":"I can't help with that request. account disabled"}}`,
		`{"content":"I can't help with that request."}`,
		`I can't help with that request.`,
	} {
		require.False(t, isGrokContentPolicyRejection(http.StatusForbidden, []byte(body)))
	}
}

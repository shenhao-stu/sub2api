package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestProtocolPolicyCacheWire(t *testing.T) {
	fixtures := []map[string]any{}
	defer func() {
		if path := os.Getenv("POLICY_CACHE_WIRE_FIXTURES"); path != "" {
			raw, err := json.Marshal(fixtures)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path, raw, 0600))
		}
	}()
	for _, tc := range []struct {
		name, split string
		five, hour  int
		known       bool
	}{
		{"one_hour", `,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":6}`, 0, 6, true},
		{"mixed", `,"cache_creation":{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":4}`, 2, 4, true},
		{"unknown", "", 0, 0, false},
		{"missing_bucket", `,"cache_creation":{"ephemeral_1h_input_tokens":6}`, 0, 0, false},
		{"inconsistent", `,"cache_creation":{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":3}`, 0, 0, false},
	} {
		for _, endpoint := range []string{"chat/completions", "responses"} {
			for _, upstreamStream := range []bool{false, true} {
				for _, clientStream := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/upstreamSSE=%t/clientSSE=%t", tc.name, endpoint, upstreamStream, clientStream), func(t *testing.T) {
						body := fmt.Sprintf(`{"model":"claude-sonnet-5-5","stream":%t,"messages":[{"role":"user","content":"synthetic"}]}`, clientStream)
						if endpoint == "responses" {
							body = fmt.Sprintf(`{"model":"claude-sonnet-5-5","stream":%t,"input":"synthetic"}`, clientStream)
						}
						rec := httptest.NewRecorder()
						c, _ := gin.CreateTestContext(rec)
						c.Request = httptest.NewRequest("POST", "/v1/"+endpoint, strings.NewReader(body))
						usage := `{"input_tokens":12,"output_tokens":7,"cache_read_input_tokens":5,"cache_creation_input_tokens":6` + tc.split + `}`
						upstreamBody := `{"error":{"type":"permission_error","code":"policy_blocked","message":"synthetic"},"usage":` + usage + `}`
						status := 403
						if upstreamStream {
							status = 200
							upstreamBody = "event: message_start\ndata: " + `{"type":"message_start","message":{"id":"synthetic","type":"message","role":"assistant","content":[],"model":"claude-sonnet-5-5","usage":` + usage + `}}` + "\n\nevent: error\ndata: " + `{"type":"error","error":{"type":"permission_error","code":"policy_blocked","message":"synthetic"},"usage":{"input_tokens":0,"output_tokens":7,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}}` + "\n\n"
						}
						up := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(upstreamBody))}}}
						svc := newForwardPartialUsageServiceForTest(nil)
						svc.httpUpstream = up
						var result *ForwardResult
						var err error
						if endpoint == "responses" {
							result, err = svc.ForwardAsResponses(context.Background(), c, newAnthropicAPIKeyAccountForTest(), []byte(body), nil)
						} else {
							result, err = svc.ForwardAsChatCompletions(context.Background(), c, newAnthropicAPIKeyAccountForTest(), []byte(body), nil)
						}
						require.Error(t, err)
						require.NotNil(t, result)
						require.Equal(t, 1, up.callCount)
						require.Equal(t, 12, result.Usage.InputTokens)
						require.Equal(t, 7, result.Usage.OutputTokens)
						require.Equal(t, 5, result.Usage.CacheReadInputTokens)
						require.Equal(t, 6, result.Usage.CacheCreationInputTokens)
						raw := rec.Body.String()
						if upstreamStream && clientStream {
							for _, line := range strings.Split(raw, "\n") {
								if value, ok := strings.CutPrefix(line, "data: "); ok && strings.Contains(value, `"policy_blocked"`) {
									raw = value
									break
								}
							}
						}
						require.True(t, json.Valid([]byte(raw)))
						u := gjson.Get(raw, "usage")
						if !u.Exists() {
							u = gjson.Get(raw, "response.usage")
						}
						require.True(t, u.Exists())
						fixtures = append(fixtures, map[string]any{"wire": rec.Body.String(), "stream": upstreamStream && clientStream, "name": t.Name(), "body": `{"usage":` + u.Raw + `}`, "five": tc.five, "hour": tc.hour, "known": tc.known})
						split := u.Get("cache_creation")
						if tc.known {
							require.True(t, split.Exists(), "complete known TTL must survive error conversion")
							require.Equal(t, tc.five, int(split.Get("ephemeral_5m_input_tokens").Int()))
							require.Equal(t, tc.hour, int(split.Get("ephemeral_1h_input_tokens").Int()))
							require.Equal(t, tc.five, result.Usage.CacheCreation5mTokens)
							require.Equal(t, tc.hour, result.Usage.CacheCreation1hTokens)
						} else {
							require.False(t, split.Exists(), "missing or inconsistent TTL must stay unknown")
						}
					})
				}
			}
		}
	}
}

func TestProtocolPolicyCacheEvidenceNeverInventsTTL(t *testing.T) {
	for _, invalid := range []string{
		``, `,"cache_creation":{"ephemeral_1h_input_tokens":6}`,
		`,"cache_creation":{"ephemeral_5m_input_tokens":null,"ephemeral_1h_input_tokens":6}`,
		`,"cache_creation":{"ephemeral_5m_input_tokens":-1,"ephemeral_1h_input_tokens":7}`,
		`,"cache_creation":{"ephemeral_5m_input_tokens":0.0,"ephemeral_1h_input_tokens":6}`,
		`,"cache_creation":{"ephemeral_5m_input_tokens":"0","ephemeral_1h_input_tokens":6}`,
		`,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":6}`,
		`,"cache_creation":{"ephemeral_5m_input_tokens":0,"EPHEMERAL_5M_INPUT_TOKENS":0,"ephemeral_1h_input_tokens":6}`,
		`,"cache_creation":{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":3}`,
		`,"cache_creation":{"ephemeral_5m_input_tokens":9223372036854775807,"ephemeral_1h_input_tokens":1}`,
	} {
		t.Run(invalid, func(t *testing.T) {
			u := parseClaudeUsageFromResponseBody([]byte(`{"usage":{"input_tokens":12,"cache_creation_input_tokens":6` + invalid + `}}`))
			converted := policyClaudeUsageToOpenAIUsage(u)
			require.Nil(t, converted.policyCacheCreation)
			require.False(t, converted.CacheCreationTTLKnown)
		})
	}
	for _, raw := range []string{
		`{"usage":{"cache_creation_input_tokens":6,"cache_creation_input_tokens":6,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":6}}}`,
		`{"usage":{"cache_creation_input_tokens":6,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":6}},"usage":{}}`,
	} {
		require.Nil(t, policyClaudeUsageToOpenAIUsage(parseClaudeUsageFromResponseBody([]byte(raw))).policyCacheCreation)
	}
}

func TestProtocolPolicyCacheEvidenceKeepsInputVector(t *testing.T) {
	start := `{"type":"message_start","message":{"usage":{"input_tokens":12,"cache_read_input_tokens":5,"cache_creation_input_tokens":6,"cache_creation":{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":4}}}}`
	svc := &GatewayService{}
	for _, tail := range []string{
		`{"input_tokens":0,"output_tokens":7,"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}`,
		`{"input_tokens":1,"output_tokens":3,"cache_creation_input_tokens":1,"cache_creation":{"ephemeral_5m_input_tokens":1,"ephemeral_1h_input_tokens":0}}`,
		`{"input_tokens":12,"output_tokens":7,"cache_creation_input_tokens":6,"cache_creation":{"ephemeral_5m_input_tokens":6,"ephemeral_1h_input_tokens":0}}`,
		`{"output_tokens":7}`,
	} {
		t.Run(tail, func(t *testing.T) {
			u := &ClaudeUsage{OutputTokens: 5}
			svc.parseSSEUsage(start, u)
			svc.mergeSSEErrorUsageJSON(u, `{"type":"error","error":{"type":"permission_error","code":"policy_blocked"},"usage":`+tail+`}`)
			require.Equal(t, 12, u.InputTokens)
			require.Equal(t, 6, u.CacheCreationInputTokens)
			require.Equal(t, 2, u.CacheCreation5mTokens)
			require.Equal(t, 4, u.CacheCreation1hTokens)
			require.GreaterOrEqual(t, u.OutputTokens, 5)
			require.Equal(t, 2, knownPolicyCacheCreation(u).Five)
			require.Equal(t, 4, knownPolicyCacheCreation(u).Hour)
		})
	}
	u := &ClaudeUsage{}
	svc.parseSSEUsage(start, u)
	svc.parseSSEUsage(`{"type":"message_delta","usage":{"output_tokens":9}}`, u)
	require.NotNil(t, knownPolicyCacheCreation(u))
	svc.parseSSEUsage(`{"type":"message_delta","usage":{"cache_creation_input_tokens":8}}`, u)
	require.Nil(t, knownPolicyCacheCreation(u), "old TTL cannot be moved to a new total")
	svc.parseSSEUsage(`{"type":"message_delta","usage":{"cache_creation_input_tokens":6}}`, u)
	require.Nil(t, knownPolicyCacheCreation(u), "returning to an old total cannot revive invalidated evidence")
	u = &ClaudeUsage{}
	svc.mergeSSEErrorUsageJSON(u, `{"type":"error","error":{"type":"permission_error","code":"policy_blocked"},"usage":{"input_tokens":12,"output_tokens":7,"cache_creation_input_tokens":6,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":6}}}`)
	require.Equal(t, 6, u.CacheCreation1hTokens)
	require.NotNil(t, knownPolicyCacheCreation(u))
}

func TestProtocolPolicyCacheDoesNotChangeSuccessfulConverter(t *testing.T) {
	u := parseClaudeUsageFromResponseBody([]byte(`{"usage":{"input_tokens":12,"cache_creation_input_tokens":6,"cache_creation":{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":4}}}`))
	known := policyClaudeUsageToOpenAIUsage(u)
	require.NotNil(t, known.policyCacheCreation)
	require.True(t, known.CacheCreationTTLKnown)
	ordinary := claudeUsageToOpenAIUsage(u)
	require.Nil(t, ordinary.policyCacheCreation)
	require.False(t, ordinary.CacheCreationTTLKnown)
	raw, err := json.Marshal(ordinary)
	require.NoError(t, err)
	require.NotContains(t, string(raw), `"cache_creation":`)
}

func TestProtocolPolicyCacheWireCannotBeActivatedByUnmarshal(t *testing.T) {
	var ordinary OpenAIUsage
	require.NoError(t, json.Unmarshal([]byte(`{"input_tokens":23,"cache_creation_input_tokens":6,"cache_creation":{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":4}}`), &ordinary))
	require.Nil(t, ordinary.policyCacheCreation)
	raw, err := json.Marshal(policyOpenAIWireUsage(&ordinary))
	require.NoError(t, err)
	require.NotContains(t, string(raw), `"cache_creation":`)
}

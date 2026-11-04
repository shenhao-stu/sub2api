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
	"testing/iotest"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/model"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type consumedModelCooldownRepo struct {
	gatewayForwardErrorPolicyRepoStub
	calls  int
	model  string
	until  time.Time
	ctxErr error
	err    error
}

type consumedSettingsRepo struct {
	SettingRepository
	value string
}

func (r *consumedSettingsRepo) GetValue(context.Context, string) (string, error) {
	if r.value != "" {
		return r.value, nil
	}
	return `{}`, nil
}
func (*consumedSettingsRepo) GetMultiple(context.Context, []string) (map[string]string, error) {
	return map[string]string{}, nil
}

func (r *consumedModelCooldownRepo) SetModelRateLimitIfLater(ctx context.Context, _ int64, model string, until time.Time, _ ...string) error {
	r.calls++
	r.model, r.until, r.ctxErr = model, until, ctx.Err()
	return r.err
}

func consumedQuotaBody(model, scope string, reset int64) string {
	return fmt.Sprintf(`{"type":"error","error":{"type":"api_error","code":"native_request_consumed_no_replay","message":"native_request_consumed_no_replay","details":{"no_replay":true,"upstream_type":"rate_limit_error","model":%q,"rate_limit_type":%q,"reset_at":%d}}}`, model, scope, reset)
}

func TestAnthropicConsumedModelCooldownForwardDoesNotReplay(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		for _, persistence := range []string{"success", "failure", "unsupported", "nil_service"} {
			for _, policy := range []string{"default", "broad400", "custom_excludes400", "failover400", "rewrite502"} {
				for _, mode := range []string{"uncommitted", "sse_ping", "json_space"} {
					t.Run(fmt.Sprintf("passthrough=%t/%s/%s/%s", passthrough, persistence, policy, mode), func(t *testing.T) {
						gin.SetMode(gin.TestMode)
						rec := httptest.NewRecorder()
						c, _ := gin.CreateTestContext(rec)
						c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
						reset := time.Now().Add(time.Hour).Truncate(time.Second)
						body := consumedQuotaBody("claude-opus-4-7", "seven_day", reset.Unix())
						if policy == "failover400" {
							body = strings.Replace(body, `"message":"native_request_consumed_no_replay"`, `"message":"requires beta"`, 1)
						}
						up := &anthropicNoReplayUpstream{anthropicHTTPUpstreamRecorder: anthropicHTTPUpstreamRecorder{resp: &http.Response{
							StatusCode: http.StatusBadRequest, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)),
						}}}
						svc := newForwardPartialUsageServiceForTest(nil)
						svc.settingService = &SettingService{settingRepo: &consumedSettingsRepo{}}
						svc.httpUpstream = up
						svc.cfg.Gateway.StreamKeepaliveInterval = 1
						svc.cfg.Gateway.AnthropicNonstreamKeepaliveInterval = 1
						repo := &consumedModelCooldownRepo{}
						if persistence == "failure" {
							repo.err = errors.New("synthetic persistence failure")
						}
						var storage AccountRepository = repo
						if persistence == "unsupported" {
							storage = &repo.gatewayForwardErrorPolicyRepoStub
						}
						svc.rateLimitService = NewRateLimitService(storage, nil, svc.cfg, nil, nil)
						if persistence == "nil_service" {
							svc.rateLimitService = nil
						}
						account := newAnthropicAPIKeyAccountForTest()
						account.Extra["anthropic_passthrough"] = passthrough
						account.Credentials["model_mapping"] = map[string]any{"friendly": "claude-opus-4-7", "claude-opus-4-7": "must-not-remap"}
						if policy == "broad400" {
							account.Credentials["temp_unschedulable_enabled"] = true
							account.Credentials["temp_unschedulable_rules"] = []any{map[string]any{"error_code": 400, "keywords": []any{"native_request_consumed_no_replay"}, "duration_minutes": 5}}
						}
						if policy == "custom_excludes400" {
							account.Credentials["custom_error_codes_enabled"] = true
							account.Credentials["custom_error_codes"] = []any{float64(429)}
						}
						svc.cfg.Gateway.FailoverOn400 = policy == "failover400"
						if policy == "rewrite502" {
							status, message := 502, "synthetic rewrite"
							rules := &ErrorPassthroughService{}
							rules.setLocalCache([]*model.ErrorPassthroughRule{{ID: 1, Enabled: true, ErrorCodes: []int{400}, Keywords: []string{"native_request_consumed_no_replay"}, MatchMode: "all", Platforms: []string{PlatformAnthropic}, ResponseCode: &status, CustomMessage: &message}})
							BindErrorPassthroughService(c, rules)
						}
						stream := mode == "sse_ping"
						if mode != "uncommitted" {
							require.True(t, svc.initAnthropicKeepalive(c, account, stream))
							frame, contentType := " \n", "application/json"
							if stream {
								frame, contentType = anthropicPingFrame, "text/event-stream"
							}
							c.Header("Content-Type", contentType)
							n, err := c.Writer.WriteString(frame)
							require.NoError(t, err)
							recordAnthropicKeepalive(c, n)
							c.Writer.Flush()
						}
						parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(fmt.Sprintf(`{"model":"friendly","stream":%t,"messages":[{"role":"user","content":"synthetic"}]}`, stream))), PlatformAnthropic)
						require.NoError(t, err)
						result, err := svc.Forward(context.Background(), c, account, parsed)
						require.Error(t, err)
						require.Nil(t, result)
						require.Equal(t, 1, up.calls)
						require.Equal(t, "claude-opus-4-7", gjson.GetBytes(up.lastBody, "model").String())
						var failover *UpstreamFailoverError
						require.NotErrorAs(t, err, &failover, "future cooldown cannot replay this consumed request")
						require.Zero(t, repo.tempCalls)
						require.Zero(t, repo.overloadCalls)
						require.Empty(t, repo.modelRateLimitCalls, "must use atomic extend-only model write")
						require.True(t, account.IsSchedulable(), "other models including Fable remain usable")
						if persistence == "success" || persistence == "failure" {
							require.Equal(t, 1, repo.calls)
							require.Equal(t, "claude-opus-4-7", repo.model)
							require.True(t, reset.Equal(repo.until))
							require.NoError(t, repo.ctxErr)
						}
						if mode == "uncommitted" {
							require.Equal(t, http.StatusBadRequest, rec.Code)
							require.JSONEq(t, body, rec.Body.String())
						} else {
							require.Equal(t, http.StatusOK, rec.Code)
							out := rec.Body.String()
							if stream {
								_, out, _ = strings.Cut(out, "event: error\ndata: ")
							}
							require.True(t, gjson.Valid(out))
							require.Equal(t, int64(400), gjson.Get(out, "error.status_code").Int())
							require.JSONEq(t, gjson.Get(body, "error.details").Raw, gjson.Get(out, "error.details").Raw)
						}
					})
				}
			}
		}
	}
}

func TestAnthropicConsumedModelCooldownRequiresFreshExactEvidence(t *testing.T) {
	reset := time.Now().Add(time.Hour).Truncate(time.Second)
	valid := consumedQuotaBody("claude-opus-4-7", "seven_day", reset.Unix())
	for _, tc := range []struct {
		name, path, raw string
		want            bool
	}{
		{"valid", "", "", true},
		{"five_hour", "error.details.rate_limit_type", `"five_hour"`, true},
		{"unknown_scope", "error.details.rate_limit_type", `"seven_day_unknown"`, false},
		{"model_mismatch", "error.details.model", `"claude-fable-5"`, false},
		{"model_number", "error.details.model", `123`, false},
		{"wrong_type", "error.details.upstream_type", `"api_error"`, false},
		{"missing_quota", "error.details", `{"no_replay":true}`, false},
		{"wrong_code", "error.code", `"native_rate_limit"`, false},
		{"false_no_replay", "error.details.no_replay", `false`, false},
		{"string_no_replay", "error.details.no_replay", `"true"`, false},
		{"expired", "error.details.reset_at", fmt.Sprint(time.Now().Unix()), false},
		{"too_far", "error.details.reset_at", fmt.Sprint(time.Now().Add(7*24*time.Hour + time.Minute).Unix()), false},
		{"string_reset", "error.details.reset_at", fmt.Sprintf(`"%d"`, reset.Unix()), false},
		{"float_reset", "error.details.reset_at", fmt.Sprintf(`%d.5`, reset.Unix()), false},
		{"bool_reset", "error.details.reset_at", `true`, false},
		{"overflow_reset", "error.details.reset_at", `9223372036854775808`, false},
		{"null_reset", "error.details.reset_at", `null`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := valid
			if tc.path != "" {
				var err error
				body, err = sjson.SetRaw(body, tc.path, tc.raw)
				require.NoError(t, err)
			}
			repo := &consumedModelCooldownRepo{}
			svc := &RateLimitService{accountRepo: repo}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			svc.persistAnthropicConsumedModelCooldown(ctx, newAnthropicAPIKeyAccountForTest(), []byte(body), "claude-opus-4-7")
			if tc.want {
				require.Equal(t, 1, repo.calls)
				require.Equal(t, "claude-opus-4-7", repo.model)
				require.True(t, reset.Equal(repo.until))
				require.NoError(t, repo.ctxErr)
			} else {
				require.Zero(t, repo.calls)
			}
			require.Zero(t, repo.tempCalls)
			require.Empty(t, repo.modelRateLimitCalls)
		})
	}
	for _, kind := range []string{"oauth", "other_platform", "nil_account", "invalid_model", "twice_mapped", "malformed"} {
		t.Run(kind, func(t *testing.T) {
			a := newAnthropicAPIKeyAccountForTest()
			body, model := valid, "claude-opus-4-7"
			switch kind {
			case "oauth":
				a.Type = AccountTypeOAuth
			case "other_platform":
				a.Platform = PlatformOpenAI
			case "nil_account":
				a = nil
			case "invalid_model":
				model = "claude-opus-4-7\n"
				body = consumedQuotaBody(model, "seven_day", reset.Unix())
			case "twice_mapped":
				model = "friendly"
			case "malformed":
				body += " tail"
			}
			repo := &consumedModelCooldownRepo{}
			(&RateLimitService{accountRepo: repo}).persistAnthropicConsumedModelCooldown(context.Background(), a, []byte(body), model)
			require.Zero(t, repo.calls)
		})
	}
}

type consumedPrefixTestBody struct {
	io.Reader
	closed int
}

func (b *consumedPrefixTestBody) Close() error { b.closed++; return nil }

func TestAnthropicConsumedResponsePrefixPreservesBodyAndClose(t *testing.T) {
	valid := consumedQuotaBody("claude-opus-4-7", "seven_day", time.Now().Add(time.Hour).Unix())
	for _, tc := range []struct {
		name, body string
		readErr    error
		want       bool
	}{
		{"consumed", valid, nil, true},
		{"unrelated", `{"error":{"message":"synthetic400"}}`, nil, false},
		{"oversize", valid + strings.Repeat(" ", anthropicConsumedErrorLimit), nil, false},
		{"partial_error", valid, io.ErrUnexpectedEOF, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reader io.Reader = strings.NewReader(tc.body)
			if tc.readErr != nil {
				reader = io.MultiReader(reader, iotest.ErrReader(tc.readErr))
			}
			source := &consumedPrefixTestBody{Reader: reader}
			resp := &http.Response{StatusCode: 400, Body: source}
			svc := &GatewayService{}
			require.Equal(t, tc.want, svc.isAnthropicConsumedResponse(nil, newAnthropicAPIKeyAccountForTest(), resp))
			body, err := io.ReadAll(resp.Body)
			require.Equal(t, tc.body, string(body))
			if tc.readErr != nil {
				require.ErrorIs(t, err, tc.readErr)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, resp.Body.Close())
			require.Equal(t, 1, source.closed)
		})
	}
}

func TestAnthropicConsumedResponseStopsSignatureRectification(t *testing.T) {
	for _, consumedAt := range []int{2, 3} {
		t.Run(fmt.Sprint(consumedAt), func(t *testing.T) {
			body := consumedQuotaBody("claude-sonnet-4-5", "seven_day", time.Now().Add(time.Hour).Unix())
			body = strings.Replace(body, `"message":"native_request_consumed_no_replay"`, `"message":"synthetic signature tool_use"`, 1)
			up := &consumedQueueUpstream{}
			for i := 1; i < consumedAt; i++ {
				up.bodies = append(up.bodies, `{"error":{"message":"synthetic signature tool_use"}}`)
			}
			up.bodies = append(up.bodies, body, `{"error":{"message":"must not dispatch"}}`)
			svc := newForwardPartialUsageServiceForTest(nil)
			svc.httpUpstream = up
			svc.settingService = &SettingService{settingRepo: &consumedSettingsRepo{value: `{"enabled":true,"apikey_signature_enabled":true,"apikey_signature_patterns":["synthetic signature"]}`}}
			repo := &consumedModelCooldownRepo{}
			svc.rateLimitService = NewRateLimitService(repo, nil, svc.cfg, nil, nil)
			a := newAnthropicAPIKeyAccountForTest()
			a.Extra["anthropic_passthrough"] = false
			a.Credentials["custom_error_codes_enabled"] = true
			a.Credentials["custom_error_codes"] = []any{float64(429)}
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
			parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(`{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"synthetic"}]}`)), PlatformAnthropic)
			require.NoError(t, err)
			result, err := svc.Forward(context.Background(), c, a, parsed)
			require.Error(t, err)
			require.Nil(t, result)
			var failover *UpstreamFailoverError
			require.NotErrorAs(t, err, &failover)
			require.Equal(t, consumedAt, up.calls, "must stop at the consumed result")
			require.Equal(t, 400, rec.Code)
			require.JSONEq(t, body, rec.Body.String())
			require.Equal(t, 1, repo.calls)
		})
	}
}

func TestAnthropicConsumedIncompleteErrorDoesNotPersist(t *testing.T) {
	body := consumedQuotaBody("claude-sonnet-4-5", "seven_day", time.Now().Add(time.Hour).Unix())
	source := &consumedPrefixTestBody{Reader: io.MultiReader(strings.NewReader(body), iotest.ErrReader(io.ErrUnexpectedEOF))}
	up := &anthropicNoReplayUpstream{anthropicHTTPUpstreamRecorder: anthropicHTTPUpstreamRecorder{resp: &http.Response{StatusCode: 400, Header: http.Header{"Content-Type": {"application/json"}}, Body: source}}}
	svc := newForwardPartialUsageServiceForTest(nil)
	svc.httpUpstream = up
	repo := &consumedModelCooldownRepo{}
	svc.rateLimitService = NewRateLimitService(repo, nil, svc.cfg, nil, nil)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(`{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"synthetic"}]}`)), PlatformAnthropic)
	require.NoError(t, err)
	result, err := svc.Forward(context.Background(), c, newAnthropicAPIKeyAccountForTest(), parsed)
	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, 1, up.calls)
	require.Zero(t, repo.calls, "a complete JSON prefix is not a complete response")
	require.Zero(t, repo.tempCalls)
	require.Equal(t, 1, source.closed)
}

type consumedQueueUpstream struct {
	anthropicHTTPUpstreamRecorder
	bodies []string
	calls  int
}

func (u *consumedQueueUpstream) DoWithTLS(req *http.Request, proxy string, accountID int64, concurrency int, profile *tlsfingerprint.Profile) (*http.Response, error) {
	i := u.calls
	u.calls++
	if i >= len(u.bodies) {
		return nil, errors.New("unexpected synthetic dispatch")
	}
	return &http.Response{StatusCode: 400, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(u.bodies[i]))}, nil
}

package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const imageTerminalMeter = `{"input_tokens":35,"output_tokens":196,"total_tokens":231,"input_tokens_details":{"text_tokens":31,"image_tokens":4,"cached_tokens":5,"cached_tokens_details":{"text_tokens":3,"image_tokens":2}}}`

func TestImagesAPIKeyDispatchedHTTPErrorNeverReplays(t *testing.T) {
	for _, status := range []int{307, 308, 400, 401, 403, 429, 500, 502, 503, 504} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			body := []byte(`{"model":"gpt-image-2.6-flare","prompt":"synthetic"}`)
			c, rec := newOpenAIImagesTestContext(t, body)
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: status, Header: http.Header{"Location": []string{"https://never-follow.invalid"}}, Body: io.NopCloser(strings.NewReader(`{"error":{"message":"Unavailable","code":"backend_failed"}}`))}}
			svc := newOpenAIImagesTestService(upstream)
			parsed, err := svc.ParseOpenAIImagesRequest(c, body)
			require.NoError(t, err)
			account := newOpenAIImagesAPIKeyAccount()
			account.Credentials["pool_mode"] = true
			result, err := svc.ForwardImages(context.Background(), c, account, body, parsed, "")
			require.Nil(t, result)
			var retry *UpstreamFailoverError
			require.NotErrorAs(t, err, &retry)
			var terminal *OpenAIImagesUpstreamError
			require.ErrorAs(t, err, &terminal)
			require.Len(t, upstream.requests, 1)
			want := status
			if status < 400 {
				want = 502
			}
			require.Equal(t, want, rec.Code)
			require.True(t, HTTPUpstreamRedirectsDisabled(upstream.lastReq.Context()))
			require.Nil(t, upstream.lastReq.GetBody)
		})
	}
}

func TestImagesAPIKeyTerminalUsageSurvivesFailure(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		payload string
		stream  bool
	}{
		{"http_failure", 503, `{"error":{"message":"failed","status_code":503},"usage":` + imageTerminalMeter + `}`, false},
		{"embedded_failure", 200, `{"error":{"message":"failed","status_code":503},"usage":` + imageTerminalMeter + `}`, false},
		{"output_missing", 200, `{"data":[],"usage":` + imageTerminalMeter + `}`, false},
		{"balance_after_meter", 502, `{"error":{"code":"insufficient_balance","message":"failed"},"usage":` + imageTerminalMeter + `}`, false},
		{"stream_error", 200, "data: {\"type\":\"error\",\"error\":{\"message\":\"failed\",\"status_code\":503},\"usage\":" + imageTerminalMeter + "}\n\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(fmt.Sprintf(`{"model":"gpt-image-2.6-flare","prompt":"synthetic","stream":%t}`, tc.stream))
			c, rec := newOpenAIImagesTestContext(t, body)
			contentType := "application/json"
			if tc.stream {
				contentType = "text/event-stream"
			}
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: tc.status, Header: http.Header{"Content-Type": []string{contentType}}, Body: io.NopCloser(strings.NewReader(tc.payload))}}
			svc := newOpenAIImagesTestService(upstream)
			parsed, err := svc.ParseOpenAIImagesRequest(c, body)
			require.NoError(t, err)
			result, err := svc.ForwardImages(context.Background(), c, newOpenAIImagesAPIKeyAccount(), body, parsed, "")
			require.Error(t, err)
			var retry *UpstreamFailoverError
			require.NotErrorAs(t, err, &retry)
			require.NotNil(t, result)
			require.True(t, OpenAIImageResultHasBillableUsage(result))
			require.Zero(t, result.ImageCount, "no invented delivered image")
			require.Equal(t, OpenAIUsage{InputTokens: 35, ImageInputTokens: 4, CacheReadInputTokens: 5, ImageCacheReadTokens: 2, OutputTokens: 196, ImageOutputTokens: 196}, result.Usage)
			require.Len(t, upstream.requests, 1)
			if !tc.stream {
				require.EqualValues(t, 196, gjson.Get(rec.Body.String(), "usage.output_tokens").Int())
			}
		})
	}
}

func TestImagesRequestCannotBecomeIdempotentViaHeaderOverride(t *testing.T) {
	body := []byte(`{"model":"gpt-image-2.6-flare","prompt":"synthetic"}`)
	c, _ := newOpenAIImagesTestContext(t, body)
	c.Request.Header.Set("Idempotency-Key", "synthetic")
	c.Request.Header.Set("X-Idempotency-Key", "synthetic")
	account := newOpenAIImagesAPIKeyAccount()
	account.Credentials["header_overrides"] = map[string]any{"Idempotency-Key": "synthetic-override", "X-Idempotency-Key": "override"}
	svc := newOpenAIImagesTestService(&httpUpstreamRecorder{})
	request, err := svc.buildOpenAIImagesRequest(context.Background(), c, account, body, "application/json", "synthetic-token", openAIImagesGenerationsEndpoint)
	require.NoError(t, err)
	require.Nil(t, request.GetBody)
	require.Empty(t, request.Header.Get("Idempotency-Key"))
	require.Empty(t, request.Header.Get("X-Idempotency-Key"))
	require.True(t, HTTPUpstreamRedirectsDisabled(request.Context()))
}

func TestImageTerminalMeterRejectsAmbiguousOrUntrustedFields(t *testing.T) {
	for _, raw := range []string{
		`{"usage":null}`, `{"usage":` + imageTerminalMeter + `,"usage":` + imageTerminalMeter + `}`,
		`{"usage":` + strings.Replace(imageTerminalMeter, `"output_tokens":196`, `"output_tokens":196.5`, 1) + `}`,
		`{"usage":` + strings.Replace(imageTerminalMeter, `"total_tokens":231`, `"total_tokens":230`, 1) + `}`,
		`{"usage":` + strings.Replace(imageTerminalMeter, `"image_tokens":4`, `"image_tokens":-4`, 1) + `}`,
		`{"usage":` + strings.Replace(imageTerminalMeter, `"cached_tokens":5`, `"cached_tokens":6`, 1) + `}`,
		`{"usage":` + imageTerminalMeter + `,"svg_usage":{}}`,
	} {
		_, rawMeter, ok := strictImageTerminalUsage([]byte(raw))
		require.False(t, ok, raw)
		require.Empty(t, rawMeter)
	}
	usage, raw, ok := strictImageTerminalUsage([]byte(`{"usage":` + strings.Replace(imageTerminalMeter, `"total_tokens":231`, `"total_tokens":231,"secret":"must-not-leak"`, 1) + `}`))
	require.True(t, ok)
	require.Equal(t, 196, usage.ImageOutputTokens)
	require.NotContains(t, string(raw), "secret")
	for _, field := range []string{"usage", "data", "output", "response", "svg_usage", "b64_json", "tool_usage"} {
		require.False(t, imageRejectionHasNoGenerationEvidence([]byte(`{"error":{"code":"insufficient_balance"},"`+field+`":null}`)))
	}
	require.True(t, imageRejectionHasNoGenerationEvidence([]byte(`{"error":{"code":"insufficient_balance"}}`)))
}

func TestImage26FallbackUsesCurrentOfficialRates(t *testing.T) {
	for _, model := range []string{"gpt-image-2.6-flare", "gpt-image-2.6-sunburst"} {
		svc := &PricingService{pricingData: map[string]*LiteLLMModelPricing{"gpt-image-1": {InputCostPerToken: 1}}}
		p := svc.GetModelPricing(model)
		require.NotNil(t, p)
		require.Equal(t, 5e-6, p.InputCostPerToken)
		require.Equal(t, 8e-6, p.InputCostPerImageToken)
		require.Equal(t, 30e-6, p.OutputCostPerImageToken)
		require.Equal(t, 1.25e-6, p.CacheReadInputTokenCost)
		require.Equal(t, 2e-6, p.CacheReadInputImageTokenCost)
	}
}

func TestImageBalanceNestedEvidencePreventsReplay(t *testing.T) {
	for _, payload := range []string{
		`{"error":{"code":"insufficient_balance","usage":{}}}`,
		`{"error":{"cause":{"code":"insufficient_balance","details":{"data":[{"b64_json":"synthetic"}]}}}}`,
		`{"error":{"code":"insufficient_balance","details":[{"output":[]}]}}`,
	} {
		require.False(t, imageRejectionHasNoGenerationEvidence([]byte(payload)), payload)
	}
}

func TestImagesStreamInvalidErrorMeterKeepsPriorDeliveredMeter(t *testing.T) {
	body := []byte(`{"model":"gpt-image-2.6-flare","prompt":"synthetic","stream":true}`)
	c, _ := newOpenAIImagesTestContext(t, body)
	frames := "data: {\"type\":\"image_generation.completed\",\"b64_json\":\"aW1hZ2U=\",\"usage\":" + imageTerminalMeter + "}\n\n" +
		"data: {\"type\":\"error\",\"error\":{\"message\":\"failed\",\"status_code\":503},\"usage\":{\"input_tokens\":99999,\"output_tokens\":99999.5}}\n\n"
	upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(frames))}}
	svc := newOpenAIImagesTestService(upstream)
	parsed, err := svc.ParseOpenAIImagesRequest(c, body)
	require.NoError(t, err)
	result, err := svc.ForwardImages(context.Background(), c, newOpenAIImagesAPIKeyAccount(), body, parsed, "")
	require.Error(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, result.ImageCount)
	require.Equal(t, 35, result.Usage.InputTokens)
	require.Equal(t, 196, result.Usage.OutputTokens)
	require.Equal(t, 2, result.Usage.ImageCacheReadTokens)
}

func TestImageFailureTerminalTokensUseLedgerWithoutInventedImage(t *testing.T) {
	logs := &openAIRecordUsageLogRepoStub{inserted: true}
	ledger := &openAIRecordUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
	users := &openAIRecordUsageUserRepoStub{}
	svc := newOpenAIRecordUsageServiceWithBillingRepoForTest(logs, ledger, users, &openAIRecordUsageSubRepoStub{}, nil)
	svc.billingService.pricingService = &PricingService{pricingData: map[string]*LiteLLMModelPricing{}}
	meter, _, known := strictImageTerminalUsage([]byte(`{"usage":` + imageTerminalMeter + `}`))
	require.True(t, known)
	result := &OpenAIForwardResult{RequestID: "synthetic-terminal-image", Usage: meter, Model: "gpt-image-2.6-flare", ImageEndpoint: true}
	groupID := int64(32)
	input := &OpenAIRecordUsageInput{Result: result, APIKey: &APIKey{ID: 235, GroupID: &groupID, Group: &Group{ID: 32, RateMultiplier: 1, ImageCountBilling: true}}, User: &User{ID: 1}, Account: &Account{ID: 353}}
	require.NoError(t, svc.RecordUsage(context.Background(), input))
	require.Zero(t, result.ImageCount)
	require.Zero(t, ledger.lastCmd.ImageCount)
	require.Zero(t, logs.lastLog.ImageCount)
	require.InDelta(t, 0.00604375, ledger.lastCmd.BalanceCost, 1e-10, "five disjoint actual token buckets, not flat image count")
	firstFingerprint := ledger.lastCmd.RequestFingerprint
	ledger.result = &UsageBillingApplyResult{Applied: false}
	require.NoError(t, svc.RecordUsage(context.Background(), input))
	require.Equal(t, firstFingerprint, ledger.lastCmd.RequestFingerprint)
	require.Zero(t, users.deductCalls, "all wallet writes stay behind the existing idempotent ledger")
}

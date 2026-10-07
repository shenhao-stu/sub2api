package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestWSPricingUsesActualIngressMapping(t *testing.T) {
	for _, tc := range []struct {
		name, mode         string
		bridge, grok, free bool
		wantWire           string
		wantMissing        bool
	}{
		{name: "passthrough_ignores_unused_mapping", mode: OpenAIWSIngressModePassthrough, wantWire: "local-wire", wantMissing: true},
		{name: "explicit_free_wire_price", mode: OpenAIWSIngressModePassthrough, free: true, wantWire: "local-wire"},
		{name: "native_ignores_http_passthrough_flag", mode: OpenAIWSIngressModeDedicated, wantWire: "gpt-5.1"},
		{name: "large_first_frame_bridge", mode: OpenAIWSIngressModePassthrough, bridge: true, wantWire: "gpt-5.1"},
		{name: "grok_forced_bridge", mode: OpenAIWSIngressModePassthrough, grok: true, wantWire: "gpt-5.1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			cfg := passthroughLifecycleConfig()
			cfg.Gateway.OpenAIWS.HTTPBridgeEnabled = tc.bridge
			cfg.Gateway.OpenAIWS.HTTPBridgeThresholdBytes = 1
			upstream := newStagedPassthroughConn()
			svc := newPassthroughLifecycleService(cfg, upstream)
			prices, key, _ := pricingAdmissionFixture()
			svc.billingService, svc.resolver = prices.billingService, prices.resolver
			account := passthroughLifecycleAccount()
			account.Extra["openai_apikey_responses_websockets_v2_mode"] = tc.mode
			// This HTTP setting has no authority over either native WS mapping or
			// WS passthrough, including its explicit HTTP bridge escape path.
			account.Extra["openai_passthrough"] = true
			account.Credentials["model_mapping"] = map[string]any{"local-wire": "gpt-5.1"}
			if tc.grok {
				account.Platform = PlatformGrok
			}
			if tc.free {
				price := 0.0
				key.Group.ModelPricing = []ChannelModelPricing{{Models: []string{"local-wire"}, BillingMode: BillingModeToken, InputPrice: &price, OutputPrice: &price}}
			}
			stop := errors.New("stop after real ingress price admission")
			var gotRequested, gotForwarded, gotWire string
			var pricingErr error
			server, done := startPassthroughHookRecordingServer(t, ctx, svc, account, &OpenAIWSIngressHooks{
				MapRequestModel: func(_ int, original string) (string, error) { return "local-wire", nil },
				ValidateModelPricing: func(requested, forwarded, wire string) error {
					gotRequested, gotForwarded, gotWire = requested, forwarded, wire
					pricingErr = svc.RequireOpenAIResolvedRequestPricing(ctx, key, account, requested, forwarded, wire, wire)
					// Every scenario stops before transport. Even allowed prices cost no tokens.
					return stop
				},
			})
			defer server.Close()
			conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
			require.NoError(t, err)
			defer func() { _ = conn.CloseNow() }()
			require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"local-client","input":"local"}`)))
			select {
			case err = <-done:
				require.ErrorIs(t, err, stop)
			case <-ctx.Done():
				t.Fatal("ingress did not reach price admission")
			}
			require.Equal(t, "local-client", gotRequested)
			require.Equal(t, "local-wire", gotForwarded)
			require.Equal(t, tc.wantWire, gotWire)
			if tc.wantMissing {
				require.ErrorIs(t, pricingErr, ErrModelPricingUnavailable)
			} else {
				require.NoError(t, pricingErr)
			}
			select {
			case payload := <-upstream.writes:
				t.Fatalf("pricing probe reached upstream: %s", payload)
			default:
			}
		})
	}
}

func TestWSPricingChecksSessionModelBeforeImplicitFollowup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := newStagedPassthroughConn()
	upstream.Send(`{"type":"response.completed","response":{"id":"resp_pricing_1","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`)
	svc := newPassthroughLifecycleService(passthroughLifecycleConfig(), upstream)
	prices, key, _ := pricingAdmissionFixture()
	account := passthroughLifecycleAccount()
	svc.billingService, svc.resolver = prices.billingService, prices.resolver
	checked := make(chan string, 2)
	hooks := &OpenAIWSIngressHooks{
		ValidateModelPricing: func(requested, forwarded, wire string) error {
			checked <- wire
			return svc.RequireOpenAIResolvedRequestPricing(ctx, key, account, requested, forwarded, wire, wire)
		},
	}
	server, done := startPassthroughHookRecordingServer(t, ctx, svc, account, hooks)
	defer server.Close()
	conn := dialPassthroughLifecycleClient(t, server)
	defer func() { _ = conn.CloseNow() }()
	requirePassthroughUpstreamWrite(t, upstream, time.Second)
	_, err := readPassthroughLifecycleFrame(t, conn, time.Second)
	require.NoError(t, err)
	require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"session.update","session":{"model":"zz-unpriced-session"}}`)))
	require.Equal(t, "session.update", gjson.GetBytes(requirePassthroughUpstreamWrite(t, upstream, time.Second), "type").String())
	require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","input":"local"}`)))
	select {
	case err = <-done:
		require.ErrorIs(t, err, ErrModelPricingUnavailable)
	case <-ctx.Done():
		t.Fatal("unpriced followup was not rejected")
	}
	require.Equal(t, "gpt-5.1", <-checked)
	require.Equal(t, "zz-unpriced-session", <-checked)
	select {
	case payload := <-upstream.writes:
		t.Fatalf("unpriced generation reached upstream: %s", payload)
	default:
	}
}

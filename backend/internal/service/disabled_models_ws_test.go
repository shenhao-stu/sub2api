package service

import (
	"context"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestDisabledModelsWSActualIngress(t *testing.T) {
	for _, mode := range []string{OpenAIWSIngressModePassthrough, OpenAIWSIngressModeDedicated, OpenAIWSIngressModeHTTPBridge} {
		for _, mapped := range []bool{false, true} {
			t.Run(mode+map[bool]string{true: "/mapped", false: "/public"}[mapped], func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				cfg := passthroughLifecycleConfig()
				cfg.Gateway.DisabledModels = disabledModelsFixture().Gateway.DisabledModels
				up := newStagedPassthroughConn()
				svc := newPassthroughLifecycleService(cfg, up)
				account := passthroughLifecycleAccount()
				account.Extra["openai_apikey_responses_websockets_v2_mode"] = mode
				model := "gpt-5.5"
				var hooks *OpenAIWSIngressHooks
				if mapped {
					model = "local-alias"
					hooks = &OpenAIWSIngressHooks{MapRequestModel: func(int, string) (string, error) { return "gpt-5.5", nil }}
				}
				server, done := startPassthroughHookRecordingServer(t, ctx, svc, account, hooks)
				defer server.Close()
				conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
				require.NoError(t, err)
				defer conn.CloseNow()
				require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"`+model+`","input":"local"}`)))
				select {
				case err = <-done:
					require.ErrorIs(t, err, ErrModelDisabled)
				case <-ctx.Done():
					t.Fatal("policy not checked")
				}
				select {
				case p := <-up.writes:
					t.Fatalf("disabled request reached upstream: %s", p)
				default:
				}
			})
		}
	}
}

func TestDisabledModelsWSPassthroughChecksImplicitFollowup(t *testing.T) {
	for _, sessionUpdate := range []bool{false, true} {
		t.Run(map[bool]string{false: "implicit_followup_mapping", true: "session_update"}[sessionUpdate], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			up := newStagedPassthroughConn()
			up.Send(`{"type":"response.completed","response":{"id":"resp_policy_1","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`)
			cfg := passthroughLifecycleConfig()
			cfg.Gateway.DisabledModels = disabledModelsFixture().Gateway.DisabledModels
			svc := newPassthroughLifecycleService(cfg, up)
			account := passthroughLifecycleAccount()
			var hooks *OpenAIWSIngressHooks
			if !sessionUpdate {
				hooks = &OpenAIWSIngressHooks{MapRequestModel: func(turn int, model string) (string, error) {
					if turn > 1 {
						return "gpt-5.5", nil
					}
					return model, nil
				}}
			}
			server, done := startPassthroughHookRecordingServer(t, ctx, svc, account, hooks)
			defer server.Close()
			conn := dialPassthroughLifecycleClient(t, server)
			defer conn.CloseNow()
			requirePassthroughUpstreamWrite(t, up, time.Second)
			_, err := readPassthroughLifecycleFrame(t, conn, time.Second)
			require.NoError(t, err)
			if sessionUpdate {
				require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"session.update","session":{"model":"gpt-5.5"}}`)))
			} else {
				require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","input":"local"}`)))
			}
			go func() { _, _, _ = conn.Read(ctx) }()
			select {
			case err = <-done:
				require.ErrorIs(t, err, ErrModelDisabled)
			case <-ctx.Done():
				t.Fatal("inherited disabled target not rejected")
			}
			select {
			case p := <-up.writes:
				t.Fatalf("disabled generation reached upstream: %s", p)
			default:
			}
		})
	}
}

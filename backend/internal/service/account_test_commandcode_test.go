//go:build unit

package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/openai_compat"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func newCommandCodeProbeAccount(provider, platform string) *Account {
	base := CommandCodeBaseURL
	if provider == CommandCodeGoProvider {
		base = CommandCodeGoBaseURL
	}
	return &Account{ID: 71, Platform: platform, Type: AccountTypeAPIKey, Credentials: map[string]any{
		"base_url": base, "api_key": "private-test-credential",
	}, Extra: map[string]any{"provider": provider}}
}

type commandCodeTestUpstream func(*http.Request, string, int64, int) (*http.Response, error)

func (fn commandCodeTestUpstream) Do(req *http.Request, proxy string, id int64, concurrency int) (*http.Response, error) {
	return fn(req, proxy, id, concurrency)
}
func (fn commandCodeTestUpstream) DoWithTLS(req *http.Request, proxy string, id int64, concurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return fn(req, proxy, id, concurrency)
}

func TestCommandCodeAccountTestUsesSelectedNativeProtocol(t *testing.T) {
	for _, tc := range []struct {
		provider, platform, endpoint, response string
		chat                                   bool
	}{
		{CommandCodeGoProvider, PlatformOpenAI, "/alpha/generate", "data: {\"type\":\"text-delta\",\"text\":\"ok\"}\n\ndata: {\"type\":\"finish\",\"finishReason\":\"stop\",\"totalUsage\":{\"inputTokens\":1,\"outputTokens\":1}}\n\ndata: [DONE]\n\n", false},
		{CommandCodeProvider, PlatformOpenAI, "/provider/v1/chat/completions", "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", true},
		{CommandCodeProvider, PlatformOpenAI, "/provider/v1/responses", "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\ndata: {\"type\":\"response.completed\"}\n\n", false},
		{CommandCodeProvider, PlatformAnthropic, "/provider/v1/messages", "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"ok\"}}\n\ndata: {\"type\":\"message_stop\"}\n\n", false},
	} {
		t.Run(tc.endpoint, func(t *testing.T) {
			account := newCommandCodeProbeAccount(tc.provider, tc.platform)
			account.Credentials["model_mapping"] = map[string]any{"public-model": "vendor/selected"}
			if tc.chat {
				account.Extra[openai_compat.ExtraKeyResponsesSupported] = false
			}
			proxyID := int64(9)
			account.ProxyID, account.Proxy = &proxyID, &Proxy{ID: proxyID, Protocol: "socks5", Host: "proxy.invalid", Port: 9000}
			called := 0
			upstream := commandCodeTestUpstream(func(req *http.Request, proxy string, id int64, _ int) (*http.Response, error) {
				called++
				require.Equal(t, "https://api.commandcode.ai"+tc.endpoint, req.URL.String())
				require.Equal(t, http.MethodPost, req.Method)
				require.Equal(t, "Bearer private-test-credential", req.Header.Get("Authorization"))
				require.Empty(t, req.Header.Get("X-API-Key"))
				require.True(t, HTTPUpstreamRedirectsDisabled(req.Context()))
				require.Equal(t, account.Proxy.URL(), proxy)
				require.Equal(t, account.ID, id)
				body, err := io.ReadAll(req.Body)
				require.NoError(t, err)
				if account.IsCommandCodeGo() {
					require.Equal(t, "vendor/selected", gjson.GetBytes(body, "params.model").String())
				} else {
					require.Equal(t, "vendor/selected", gjson.GetBytes(body, "model").String())
				}
				return newJSONResponse(http.StatusOK, tc.response), nil
			})
			service := &AccountTestService{httpUpstream: upstream, accountRepo: &mockAccountRepoForGemini{accountsByID: map[int64]*Account{account.ID: account}}}
			c, rec := newTestContext()
			err := service.TestAccountConnection(c, account.ID, "public-model", "ping", "")
			require.NoError(t, err)
			require.Equal(t, 1, called)
			require.Contains(t, rec.Body.String(), `"test_complete"`)
			require.Contains(t, rec.Body.String(), `"success":true`)
		})
	}
}

func TestCommandCodeAccountTestRejectsUnsupportedInputsBeforeNetwork(t *testing.T) {
	for _, tc := range []struct {
		name, model, mode string
		opts              AccountTestOptions
		badBase           bool
	}{
		{name: "model_required"},
		{name: "compact", model: "vendor/model", mode: AccountTestModeCompact},
		{name: "media", model: "vendor/model", opts: AccountTestOptions{ImageDataURL: "data:image/png;base64,AA"}},
		{name: "foreign_origin", model: "vendor/model", badBase: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account := newCommandCodeProbeAccount(CommandCodeGoProvider, PlatformOpenAI)
			if tc.badBase {
				account.Credentials["base_url"] = "https://attacker.invalid"
			}
			upstream := &queuedHTTPUpstream{}
			service := &AccountTestService{httpUpstream: upstream}
			c, rec := newTestContext()
			require.Error(t, service.testCommandCodeAccountConnection(c, account, tc.model, "", tc.mode, tc.opts))
			require.Empty(t, upstream.requests)
			require.NotContains(t, rec.Body.String(), `"success":true`)
		})
	}
}

func TestCommandCodeAccountTestFailureIsNotSuccessOrCredentialDisclosure(t *testing.T) {
	for _, tc := range []struct {
		name, platform, body string
		status               int
	}{
		{"provider_error", PlatformAnthropic, "private-test-credential", 403},
		{"incomplete_messages", PlatformAnthropic, "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"partial\"}}\n\n", 200},
		{"malformed_messages", PlatformAnthropic, "data: not-json\n\n", 200},
		{"empty_messages", PlatformAnthropic, "", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := &AccountTestService{httpUpstream: &queuedHTTPUpstream{responses: []*http.Response{newJSONResponse(tc.status, tc.body)}}}
			c, rec := newTestContext()
			require.Error(t, service.testCommandCodeAccountConnection(c, newCommandCodeProbeAccount(CommandCodeProvider, tc.platform), "vendor/model", "", "", AccountTestOptions{}))
			require.NotContains(t, rec.Body.String(), `"success":true`)
			require.NotContains(t, rec.Body.String(), "private-test-credential")
		})
	}
}

const commandCodeCatalogFixture = `{"data":[
 {"id":"vendor/chat","name":"Chat Model","context_length":123456,"supported_endpoints":["/chat/completions"]},
 {"id":"vendor/responses","name":"Responses Model","supported_endpoints":["/responses"]},
 {"id":"vendor/messages","name":"Messages Model","context_length":654321,"supported_endpoints":["/messages"]},
 {"id":"vendor/images","supported_endpoints":["/images/generations"]},
 {"id":"vendor/unknown"}
]}`

func TestCommandCodeModelCatalogIsPublicBoundedAndProtocolFiltered(t *testing.T) {
	for _, tc := range []struct{ provider, platform, expected string }{
		{CommandCodeGoProvider, PlatformOpenAI, "vendor/chat"},
		{CommandCodeProvider, PlatformOpenAI, "vendor/responses"},
		{CommandCodeProvider, PlatformAnthropic, "vendor/messages"},
	} {
		t.Run(tc.expected, func(t *testing.T) {
			account := newCommandCodeProbeAccount(tc.provider, tc.platform)
			delete(account.Credentials, "api_key") // unsaved preview needs no credential
			account.Credentials[credKeyHeaderOverrideEnabled] = true
			account.Credentials[credKeyHeaderOverrides] = map[string]any{"X-Private-Account": "do-not-send"}
			upstream := commandCodeTestUpstream(func(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
				require.Equal(t, CommandCodeBaseURL+"/v1/models", req.URL.String())
				require.Equal(t, http.MethodGet, req.Method)
				require.Empty(t, req.Header.Get("Authorization"))
				require.Empty(t, req.Header.Get("X-Private-Account"))
				require.Empty(t, req.Header.Get("Cookie"))
				require.True(t, HTTPUpstreamRedirectsDisabled(req.Context()))
				require.True(t, HTTPUpstreamPublicHostsOnly(req.Context()))
				_, bounded := req.Context().Deadline()
				require.True(t, bounded)
				return newJSONResponse(200, commandCodeCatalogFixture), nil
			})
			service := &AccountTestService{httpUpstream: upstream}
			models, err := service.FetchCommandCodeAccountModels(context.Background(), account)
			require.NoError(t, err)
			if tc.provider == CommandCodeGoProvider {
				require.Len(t, models, 3)
				require.Equal(t, []string{"vendor/chat", "vendor/messages", "vendor/responses"}, []string{models[0].ID, models[1].ID, models[2].ID})
			} else {
				require.Len(t, models, 1)
				require.Equal(t, tc.expected, models[0].ID)
			}
		})
	}
}

func TestCommandCodeModelSyncPreservesObservedContextWithoutRegistryGuessing(t *testing.T) {
	account := newCommandCodeProbeAccount(CommandCodeGoProvider, PlatformOpenAI)
	account.ID = 0
	delete(account.Credentials, "api_key")
	upstream := &queuedHTTPUpstream{responses: []*http.Response{newJSONResponse(200, commandCodeCatalogFixture)}}
	service := &AccountTestService{httpUpstream: upstream}
	catalog, err := service.SyncUpstreamModelCatalog(context.Background(), account)
	require.NoError(t, err)
	require.Equal(t, []string{"vendor/chat", "vendor/messages", "vendor/responses"}, catalog.Models)
	require.EqualValues(t, 123456, catalog.Metadata["vendor/chat"].ContextWindow)
	require.Nil(t, catalog.Metadata["vendor/chat"].Reasoning)
	require.Empty(t, catalog.Metadata["vendor/chat"].InputModalities)
	require.Len(t, catalog.Warnings, 2)
	require.Equal(t, "commandcode_public_catalog", catalog.Warnings[0].Code)
	require.Equal(t, UpstreamModelMetadataIncompleteCode, catalog.Warnings[1].Code)
	require.Len(t, upstream.requests, 1, "must not query models.dev or fall back to another endpoint")
}

func TestCommandCodeModelCatalogMappingAndFailure(t *testing.T) {
	account := newCommandCodeProbeAccount(CommandCodeGoProvider, PlatformOpenAI)
	account.Credentials["model_mapping"] = map[string]any{"public-alias": "vendor/chat"}
	service := &AccountTestService{httpUpstream: &queuedHTTPUpstream{responses: []*http.Response{newJSONResponse(200, commandCodeCatalogFixture)}}}
	models, err := service.FetchOpenAIAccountModels(context.Background(), account)
	require.NoError(t, err)
	require.Len(t, models, 1)
	require.Equal(t, "public-alias", models[0].ID)
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"error", "private-upstream-details", 503},
		{"redirect", "", 302},
		{"empty", `{"data":[]}`, 200},
		{"malformed", `{"data":[`, 200},
		{"oversized", strings.Repeat("x", commandCodeModelCatalogLimit+1), 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service.httpUpstream = &queuedHTTPUpstream{responses: []*http.Response{newJSONResponse(tc.status, tc.body)}}
			got, err := service.FetchCommandCodeAccountModels(context.Background(), account)
			require.Error(t, err)
			require.Nil(t, got)
			require.NotContains(t, err.Error(), "private-upstream-details")
		})
	}
	body, err := filterCommandCodeModelCatalog([]byte(commandCodeCatalogFixture), account)
	require.NoError(t, err)
	require.True(t, json.Valid(body))
}

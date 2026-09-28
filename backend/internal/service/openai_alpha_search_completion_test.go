package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestAlphaSearchFallbackRequiresSuccessfulCompletion(t *testing.T) {
	gin.SetMode(gin.TestMode)
	delta := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"unfinished answer\"}\n\n"
	cases := map[string]string{
		"empty":             "",
		"truncated":         delta,
		"done_only":         delta + "data: [DONE]\n\n",
		"failed":            delta + "data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\"}}\n\n",
		"incomplete":        delta + "data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\"}}\n\n",
		"error":             delta + "data: {\"type\":\"error\",\"error\":{\"message\":\"unavailable\"}}\n\n",
		"invalid_completed": delta + "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"failed\"}}\n\n",
		"missing_response":  delta + "data: {\"type\":\"response.completed\"}\n\n",
	}
	for name, wire := range cases {
		t.Run(name, func(t *testing.T) {
			body := []byte(`{"model":"gpt-5.5","commands":{"search_query":[{"q":"example"}]}}`)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/alpha/search", bytes.NewReader(body))
			upstream := &httpUpstreamRecorder{resp: &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(wire)),
			}}
			service := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
			account := &Account{ID: 43, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 1,
				Credentials: map[string]any{"access_token": "at-test-token", "auth_mode": OpenAIAuthModePersonalAccessToken, "chatgpt_account_id": "fixture-account"}}
			result, err := service.ForwardAlphaSearch(context.Background(), c, account, body)
			require.Error(t, err)
			require.Nil(t, result, "a failed search must not produce a billable WebSearchCalls result")
			require.False(t, c.Writer.Written(), "do not write partial content as a successful search")
		})
	}
}

func TestAlphaSearchFallbackCompletionPreservesOutputAndCitations(t *testing.T) {
	for _, wire := range []string{
		alphaSearchResponsesSSE("search result"),
		"data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"search result\",\"annotations\":[{\"type\":\"url_citation\",\"url\":\"https://example.com/news\",\"title\":\"Example News\"}]}]}]}}\n\n",
	} {
		body, err := openAIAlphaSearchResponseFromResponsesSSE([]byte(wire))
		require.NoError(t, err)
		require.JSONEq(t, `{"output":"search result","results":[{"type":"text_result","ref_id":"turn0search0","url":"https://example.com/news","title":"Example News"}]}`, string(body))
	}
}

type alphaSearchCanceledReader struct{}

func (alphaSearchCanceledReader) Read([]byte) (int, error) { return 0, context.Canceled }

func TestAlphaSearchFallbackPreservesCompletedSearchOnFailure(t *testing.T) {
	completed := "data: {\"type\":\"response.web_search_call.completed\",\"item_id\":\"search-1\"}\n\n"
	for name, fixture := range map[string]struct {
		wire     string
		readFail bool
		billable bool
	}{
		"completed_then_eof":              {wire: completed, billable: true},
		"completed_then_transport_cancel": {wire: completed, readFail: true, billable: true},
		"duplicate_completed_events":      {wire: completed + completed, billable: true},
		"completed_item_then_error":       {wire: "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"web_search_call\",\"status\":\"completed\"}}\n\ndata: {\"type\":\"error\"}\n\n", billable: true},
		"failed_with_completed_search":    {wire: "data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"output\":[{\"type\":\"web_search_call\",\"status\":\"completed\"}]}}\n\n", billable: true},
		"tokens_only":                     {wire: "data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"usage\":{\"input_tokens\":40,\"output_tokens\":10}}}\n\n"},
		"search_still_running":            {wire: "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"web_search_call\",\"status\":\"in_progress\"}}\n\n"},
	} {
		t.Run(name, func(t *testing.T) {
			body := []byte(`{"model":"gpt-5.5","commands":{"search_query":[{"q":"fixture"}]}}`)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/alpha/search", bytes.NewReader(body))
			var reader io.Reader = strings.NewReader(fixture.wire)
			if fixture.readFail {
				reader = io.MultiReader(reader, alphaSearchCanceledReader{})
			}
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(reader)}}
			svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
			account := &Account{ID: 43, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
				Credentials: map[string]any{"access_token": "at-fixture", "auth_mode": OpenAIAuthModePersonalAccessToken, "chatgpt_account_id": "fixture-account"}}
			result, err := svc.ForwardAlphaSearch(context.Background(), c, account, body)
			require.Error(t, err)
			require.False(t, c.Writer.Written(), "failed responses must not expose partial success")
			if fixture.billable {
				require.NotNil(t, result)
				require.Equal(t, 1, result.WebSearchCalls, "retain the original once-per-request contract")
				require.Zero(t, result.Usage.InputTokens, "do not introduce token pricing for this endpoint")
			} else {
				require.Nil(t, result)
			}
		})
	}
}

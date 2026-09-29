package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/Wei-Shaw/sub2api/internal/pkg/commandcode"
	"github.com/gin-gonic/gin"
)

type commandCodeHTTPDoer func(*http.Request) (*http.Response, error)

func (f commandCodeHTTPDoer) Do(req *http.Request) (*http.Response, error) { return f(req) }

func (s *OpenAIGatewayService) sendCommandCodeRequest(ctx context.Context, c *gin.Context, account *Account, body []byte, stream bool) (*http.Response, error) {
	if err := ValidateCommandCodeAccount(account); err != nil {
		return nil, err
	}
	policyRequest := &http.Request{Header: make(http.Header)}
	if err := applyCommandCodeClientPolicy(policyRequest, c, account); err != nil {
		writeChatCompletionsError(c, http.StatusBadRequest, "invalid_request_error", err.Error())
		return nil, err
	}
	proxyURL := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	client := commandcode.Client{
		UserAgent: commandCodeCLIUserAgent,
		Version:   commandCodeCLIVersion,
		HTTPClient: commandCodeHTTPDoer(func(req *http.Request) (*http.Response, error) {
			if err := applyCommandCodeClientPolicy(req, c, account); err != nil {
				return nil, err
			}
			req = req.WithContext(WithHTTPUpstreamProfile(req.Context(), HTTPUpstreamProfileOpenAI))
			return s.doOpenAIUpstream(req, proxyURL, account)
		}),
	}
	SetActualOpenAIUpstreamEndpoint(c, "/alpha/generate")
	upstreamCtx, release := detachUpstreamContext(ctx)
	defer release()
	resp, err := client.ChatCompletion(upstreamCtx, account.GetCredential("api_key"), body, stream)
	if err == nil {
		return resp, nil
	}
	var metered *commandcode.UsageError
	if errors.As(err, &metered) {
		// The existing metered HTTP-error path records observed usage once and
		// prevents replay. Streaming usage stays in the ordinary SSE pipeline.
		data, marshalErr := json.Marshal(map[string]any{
			"error": map[string]any{"type": "upstream_error", "message": err.Error()},
			"usage": metered.Usage,
		})
		if marshalErr != nil {
			return nil, marshalErr
		}
		return &http.Response{StatusCode: http.StatusBadGateway, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(data))}, nil
	}
	if errors.Is(err, commandcode.ErrRequest) {
		writeChatCompletionsError(c, http.StatusBadRequest, "invalid_request_error", err.Error())
		return nil, err
	}
	if !errors.Is(err, commandcode.ErrTransport) {
		// HTTP 200 already began generation. A protocol/usage failure must not
		// replay a potentially metered completion, even without final usage.
		setOpsUpstreamError(c, http.StatusBadGateway, err.Error(), "")
		writeChatCompletionsError(c, http.StatusBadGateway, "upstream_error", err.Error())
		return nil, err
	}
	return nil, s.handleOpenAIUpstreamTransportError(ctx, c, account, err, false)
}

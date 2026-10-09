package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/commandcode"
	"github.com/gin-gonic/gin"
)

type commandCodeAccountTestClient struct {
	service *AccountTestService
	account *Account
}

func (client commandCodeAccountTestClient) Do(req *http.Request) (*http.Response, error) {
	if err := prepareCommandCodeRequest(req, client.account); err != nil {
		return nil, err
	}
	if client.account.IsCommandCode() {
		req = req.WithContext(WithHTTPUpstreamProfile(req.Context(), HTTPUpstreamProfileOpenAI))
	}
	return client.service.commandCodeAccountTestUpstream(req, client.account)
}

func (s *AccountTestService) commandCodeAccountTestUpstream(req *http.Request, account *Account) (*http.Response, error) {
	if s == nil || s.httpUpstream == nil {
		return nil, errors.New("command code upstream transport is unavailable")
	}
	proxyURL := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	return s.httpUpstream.DoWithTLS(req, proxyURL, account.ID, account.Concurrency, s.tlsFPProfileService.ResolveTLSProfile(account))
}

func (s *AccountTestService) testCommandCodeAccountConnection(c *gin.Context, account *Account, modelID, prompt, mode string, opts AccountTestOptions) error {
	if err := ValidateCommandCodeAccount(account); err != nil {
		return s.sendErrorAndEnd(c, err.Error())
	}
	if strings.TrimSpace(modelID) == "" {
		return s.sendErrorAndEnd(c, "Select a Command Code model before testing the account")
	}
	if (mode != "" && mode != AccountTestModeDefault && mode != "text") || opts.ImageDataURL != "" || opts.AudioDataURL != "" {
		return s.sendErrorAndEnd(c, "Command Code account tests support text only")
	}
	if !account.IsModelSupported(modelID) {
		return s.sendErrorAndEnd(c, "The selected model is not allowed by this account")
	}
	modelID = account.GetMappedModel(modelID)
	if strings.TrimSpace(prompt) == "" {
		prompt = "hi"
	}
	payload := map[string]any{
		"model": modelID, "messages": []map[string]any{{"role": "user", "content": prompt}},
		"max_tokens": 512, "stream": true,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return s.sendErrorAndEnd(c, "Failed to create Command Code test request")
	}
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-store")
	c.Writer.Header().Set("X-Accel-Buffering", "no")
	c.Writer.Flush()
	s.sendEvent(c, TestEvent{Type: "test_start", Model: modelID})
	client := commandCodeAccountTestClient{service: s, account: account}
	native := commandcode.Client{HTTPClient: client, UserAgent: commandCodeCLIUserAgent, Version: commandCodeCLIVersion}
	resp, err := native.ChatCompletion(c.Request.Context(), account.GetCredential("api_key"), body, true)
	if err != nil {
		return s.sendErrorAndEnd(c, "Command Code account test request failed")
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return s.sendErrorAndEnd(c, fmt.Sprintf("Command Code account test returned HTTP %d", resp.StatusCode))
	}
	return s.processOpenAIChatCompletionsStream(c, resp.Body)
}

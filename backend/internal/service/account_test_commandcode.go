package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/commandcode"
	"github.com/gin-gonic/gin"
)

type commandCodeAccountTestClient struct {
	service *AccountTestService
	account *Account
}

func (s *AccountTestService) GetCommandCodeQuota(ctx context.Context, account *Account) (*commandcode.BillingCredits, error) {
	if !account.IsCommandCodeGo() {
		return nil, errors.New("command code quota is available for Go accounts only")
	}
	if err := ValidateCommandCodeAccount(account); err != nil {
		return nil, err
	}
	client := commandcode.Client{HTTPClient: commandCodeAccountTestClient{s, account}, UserAgent: commandCodeCLIUserAgent, Version: commandCodeCLIVersion}
	return client.BillingCredits(ctx, account.GetCredential("api_key"))
}

func (client commandCodeAccountTestClient) Do(req *http.Request) (*http.Response, error) {
	if !client.account.IsCommandCodeGo() {
		client.account.ApplyHeaderOverrides(req.Header)
	}
	if err := prepareCommandCodeRequest(req, client.account); err != nil {
		return nil, err
	}
	if client.account.Platform == PlatformOpenAI {
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
	endpoint := commandCodeAccountEndpoint(account)
	payload := map[string]any{
		"model": modelID, "messages": []map[string]any{{"role": "user", "content": prompt}},
		"max_tokens": 64, "stream": true,
	}
	if account.IsCommandCodeGo() {
		payload["max_tokens"] = 512
	}
	if endpoint == "/responses" {
		payload = map[string]any{
			"model": modelID, "input": prompt, "max_output_tokens": 64, "stream": true,
		}
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
	var resp *http.Response
	if account.IsCommandCodeGo() {
		native := commandcode.Client{HTTPClient: client, UserAgent: commandCodeCLIUserAgent, Version: commandCodeCLIVersion}
		resp, err = native.ChatCompletion(c.Request.Context(), account.GetCredential("api_key"), body, true)
	} else {
		req, requestErr := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, CommandCodeBaseURL+"/v1"+endpoint, bytes.NewReader(body))
		if requestErr != nil {
			return s.sendErrorAndEnd(c, "Failed to create Command Code test request")
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "text/event-stream")
		if endpoint == "/messages" {
			req.Header.Set("Anthropic-Version", "2023-06-01")
		}
		resp, err = client.Do(req)
	}
	if err != nil {
		return s.sendErrorAndEnd(c, "Command Code account test request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return s.sendErrorAndEnd(c, fmt.Sprintf("Command Code account test returned HTTP %d", resp.StatusCode))
	}
	switch endpoint {
	case "/messages":
		return s.processCommandCodeMessagesStream(c, resp.Body)
	case "/responses":
		return s.processOpenAIStream(c, resp.Body)
	default:
		return s.processOpenAIChatCompletionsStream(c, resp.Body)
	}
}

func (s *AccountTestService) processCommandCodeMessagesStream(c *gin.Context, body io.Reader) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !sseDataPrefix.MatchString(line) {
			continue
		}
		var event struct {
			Type  string `json:"type"`
			Delta struct {
				Text string `json:"text"`
			} `json:"delta"`
		}
		if json.Unmarshal([]byte(sseDataPrefix.ReplaceAllString(line, "")), &event) != nil {
			return s.sendErrorAndEnd(c, "Command Code Messages stream contains invalid JSON")
		}
		switch event.Type {
		case "content_block_delta":
			if event.Delta.Text != "" {
				s.sendEvent(c, TestEvent{Type: "content", Text: event.Delta.Text})
			}
		case "message_stop":
			s.sendEvent(c, TestEvent{Type: "test_complete", Success: true})
			return nil
		case "error":
			return s.sendErrorAndEnd(c, "Command Code Messages stream returned an error")
		}
	}
	return s.sendErrorAndEnd(c, "Command Code Messages stream ended before message_stop")
}

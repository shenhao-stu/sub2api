package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
)

func DeepSeekToolModel(path string) string {
	switch path {
	case "/v1/tools/search":
		return "deepseek-tools-search"
	case "/v1/tools/fetch":
		return "deepseek-tools-fetch"
	}
	return ""
}

// Only the upstream provider fetches a supplied page. Its URL is payload data,
// never a transport destination or a source of authentication headers.
func ValidateDeepSeekToolBody(model string, body []byte) error {
	var raw map[string]json.RawMessage
	if len(body) > 64<<10 || json.Unmarshal(body, &raw) != nil || raw == nil {
		return errors.New("invalid tool request")
	}
	delete(raw, "model")
	var text string
	switch model {
	case "deepseek-tools-search":
		if json.Unmarshal(raw["query"], &text) != nil || strings.TrimSpace(text) == "" || utf8.RuneCountInString(text) > 128 {
			return errors.New("query is required")
		}
		delete(raw, "query")
		if v, ok := raw["ori_question"]; ok {
			if string(bytes.TrimSpace(v)) != "null" && (json.Unmarshal(v, &text) != nil || len(text) == 0 || utf8.RuneCountInString(text) > 128) {
				return errors.New("invalid original question")
			}
			delete(raw, "ori_question")
		}
		if v, ok := raw["localization"]; ok {
			if json.Unmarshal(v, &text) != nil {
				return errors.New("invalid localization IP")
			}
			if text != "" {
				if _, err := netip.ParseAddr(text); err != nil {
					return errors.New("invalid localization IP")
				}
			}
			delete(raw, "localization")
		}
		if v, ok := raw["count"]; ok {
			var n int
			if json.Unmarshal(v, &n) != nil || n < 1 || n > 50 {
				return errors.New("count must be between 1 and 50")
			}
			delete(raw, "count")
		}
		if v, ok := raw["need_content"]; ok {
			if string(bytes.TrimSpace(v)) != "true" && string(bytes.TrimSpace(v)) != "false" {
				return errors.New("need_content must be a boolean")
			}
			delete(raw, "need_content")
		}
	case "deepseek-tools-fetch":
		if json.Unmarshal(raw["url"], &text) != nil || len(text) > 8192 {
			return errors.New("invalid page URL")
		}
		u, err := url.ParseRequestURI(text)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Hostname() == "" || u.User != nil {
			return errors.New("a public HTTP or HTTPS URL without user information is required")
		}
		delete(raw, "url")
	default:
		return errors.New("unknown tool")
	}
	if len(raw) != 0 {
		return errors.New("unsupported tool request field")
	}
	return nil
}

func (s *OpenAIGatewayService) ForwardDeepSeekTool(ctx context.Context, c *gin.Context, a *Account, model string, body []byte) (*OpenAIForwardResult, error) {
	if a.Platform != PlatformDeepseek || a.Type != AccountTypeAPIKey || DeepSeekToolModel(c.FullPath()) != model || !a.IsModelSupported(model) {
		return nil, errors.New("tool is unavailable for this account")
	}
	base, err := s.validateUpstreamBaseURL(a.GetCNProtocolBaseURL(APIProtocolChatCompletions))
	if err != nil || !strings.HasPrefix(base, "https://") {
		return nil, errors.New("tool account requires a verified HTTPS endpoint")
	}
	if err := rejectDisabledModels(c, s.cfg, model, model); err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil, err
	}
	delete(fields, "model")
	wire, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, buildOpenAIEndpointURL(base, c.FullPath()), bytes.NewReader(wire))
	if err != nil {
		return nil, err
	}
	token, _, err := s.GetAccessToken(ctx, a)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	proxy := ""
	if a.ProxyID != nil && a.Proxy != nil {
		proxy = a.Proxy.URL()
	}
	SetActualOpenAIUpstreamEndpoint(c, c.FullPath())
	SetOpsUpstreamModel(c, model)
	started := time.Now()
	res, err := s.doOpenAIUpstream(req, proxy, a)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	data, err := ReadUpstreamResponseBody(res.Body, s.cfg, c, openAITooLargeError)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		status := res.StatusCode
		if status < 400 || status > 599 {
			status = http.StatusBadGateway
		}
		if wait := res.Header.Get("Retry-After"); wait != "" {
			c.Header("Retry-After", wait)
		}
		c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"type": "upstream_error", "message": "DeepSeek tool request failed"}})
		return nil, fmt.Errorf("tool provider returned status %d", status)
	}
	billable, err := deepSeekToolResult(model, data)
	if err != nil {
		return nil, err
	}
	c.Data(http.StatusOK, "application/json", data)
	if !billable {
		return nil, nil
	}
	return &OpenAIForwardResult{RequestID: strings.TrimSpace(res.Header.Get("x-request-id")), UpstreamHeaders: res.Header, Model: model, UpstreamModel: model, Duration: time.Since(started), WebSearchCalls: 1}, nil
}

func deepSeekToolResult(model string, data []byte) (bool, error) {
	var obj struct {
		Results *[]struct{ URL string }
		Content *string
		Error   json.RawMessage
	}
	if json.Unmarshal(data, &obj) == nil && len(obj.Error) == 0 {
		switch model {
		case "deepseek-tools-search":
			if obj.Results != nil {
				for _, r := range *obj.Results {
					if strings.TrimSpace(r.URL) == "" {
						return false, errors.New("invalid search result")
					}
				}
				return len(*obj.Results) > 0, nil
			}
		case "deepseek-tools-fetch":
			if obj.Content != nil {
				return strings.TrimSpace(*obj.Content) != "", nil
			}
		}
	}
	return false, fmt.Errorf("invalid %s result from provider", model)
}

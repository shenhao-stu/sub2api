package service

import (
	"net/http"
	"strings"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/gin-gonic/gin"
)

const (
	CommandCodeProvider     = "commandcode"
	CommandCodeGoProvider   = "commandcode_go"
	CommandCodeBaseURL      = "https://api.commandcode.ai/provider"
	CommandCodeGoBaseURL    = "https://api.commandcode.ai"
	commandCodeCLIUserAgent = "command-code-cli/1.54.1"
	commandCodeCLIVersion   = "1.54.1"
)

func touchesCommandCodePolicy(extra map[string]any) bool {
	for _, key := range []string{"provider", "commandcode_zdr"} {
		if _, exists := extra[key]; exists {
			return true
		}
	}
	return false
}

func (a *Account) IsCommandCodeGo() bool {
	return a.IsCommandCode() && a.GetCredential("account_mode") == AccountModeGo
}

// ValidateCommandCodeAccount binds a credential to its protocol and official origin.
// The same validation runs at persistence and again at the network boundary.
func ValidateCommandCodeAccount(a *Account) error {
	if a != nil && !a.IsCommandCode() && (a.GetExtraString("provider") == CommandCodeProvider || a.GetExtraString("provider") == CommandCodeGoProvider) {
		return infraerrors.BadRequest("LEGACY_COMMANDCODE_ACCOUNT", "Select the Command Code platform and the Go or Provider mode")
	}
	if a == nil || !a.IsCommandCode() {
		return nil
	}
	invalid := func(message string) error {
		return infraerrors.BadRequest("INVALID_COMMANDCODE_ACCOUNT", message)
	}
	if a.Type != AccountTypeAPIKey {
		return invalid("Command Code requires an API key account")
	}
	expected := DefaultCommandCodeBaseURL
	if a.IsCommandCodeGo() {
		expected = CommandCodeGoBaseURL
	}
	base := strings.TrimRight(strings.TrimSpace(a.GetOpenAIBaseURL()), "/")
	if base != expected && (a.IsCommandCodeGo() || base != DefaultCommandCodeAnthropicBaseURL) {
		return invalid("Command Code requires its fixed official base URL")
	}
	key := a.GetCredential("api_key")
	if key == "" || len(key) > 8192 || strings.IndexFunc(key, func(r rune) bool { return r <= 32 || r > 126 }) >= 0 {
		return invalid("Command Code API key must contain only printable non-space ASCII characters")
	}
	if raw, exists := a.Extra["commandcode_zdr"]; exists {
		enabled, ok := raw.(bool)
		if !ok || (enabled && a.IsCommandCodeGo()) {
			return invalid("Command Code ZDR must be a boolean and is available only on the Provider API")
		}
	}
	if a.IsPoolMode() {
		return invalid("Command Code accounts cannot use gateway pool mode")
	}
	return nil
}

// prepareCommandCodeRequest is deliberately independent of mutable model catalogs.
// Credentials never follow redirects or leave the fixed protocol endpoint set.
func prepareCommandCodeRequest(req *http.Request, account *Account) error {
	if !account.IsCommandCode() {
		return nil
	}
	if err := ValidateCommandCodeAccount(account); err != nil {
		return err
	}
	if req == nil || req.URL == nil || req.URL.Scheme != "https" || req.URL.Host != "api.commandcode.ai" || req.URL.User != nil || req.URL.Fragment != "" {
		return infraerrors.BadRequest("INVALID_COMMANDCODE_ENDPOINT", "Command Code credentials require the official HTTPS endpoint")
	}
	allowed := false
	if account.IsCommandCodeGo() {
		allowed = (req.Method == http.MethodPost && req.URL.Path == "/alpha/generate") ||
			(req.Method == http.MethodGet && req.URL.Path == "/alpha/billing/credits")
	} else {
		switch req.URL.Path {
		case "/provider/v1/messages", "/provider/v1/chat/completions", "/provider/v1/responses":
			allowed = req.Method == http.MethodPost
		case "/provider/v1/models":
			allowed = req.Method == http.MethodGet
		}
	}
	if !allowed {
		return infraerrors.BadRequest("UNSUPPORTED_COMMANDCODE_ENDPOINT", "This endpoint is not supported by the configured Command Code protocol")
	}
	zdr, err := commandCodeZDRHeader(req.Header, account.IsCommandCodeGo())
	if err != nil {
		return err
	}
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	*req = *req.WithContext(WithHTTPUpstreamRedirectsDisabled(req.Context()))
	req.URL.RawQuery = ""
	req.Host = ""
	for key := range req.Header {
		switch strings.ToLower(key) {
		case "authorization", "x-api-key", "x-goog-api-key", "cookie", "host", "x-cmd-zdr":
			delete(req.Header, key)
		}
	}
	req.Header.Set("Authorization", "Bearer "+account.GetCredential("api_key"))
	if enabled, _ := account.Extra["commandcode_zdr"].(bool); enabled {
		zdr = "1"
	}
	if zdr != "" {
		req.Header.Set("X-Cmd-Zdr", zdr)
	}
	return nil
}

func applyCommandCodeClientPolicy(req *http.Request, c *gin.Context, account *Account) error {
	if !account.IsCommandCode() || c == nil || c.Request == nil {
		return nil
	}
	value, err := commandCodeZDRHeader(c.Request.Header, account.IsCommandCodeGo())
	if err != nil {
		return err
	}
	if value == "1" {
		req.Header.Set("X-Cmd-Zdr", value)
	}
	return nil
}

func commandCodeZDRHeader(header http.Header, goMode bool) (string, error) {
	value := ""
	seen := false
	for name, values := range header {
		if !strings.EqualFold(name, "x-cmd-zdr") {
			continue
		}
		if seen || len(values) != 1 || (values[0] != "0" && values[0] != "1") {
			return "", infraerrors.BadRequest("INVALID_COMMANDCODE_ZDR", "x-cmd-zdr must have exactly one value: 0 or 1")
		}
		value, seen = values[0], true
	}
	if goMode && value == "1" {
		return "", infraerrors.BadRequest("UNSUPPORTED_COMMANDCODE_ZDR", "Command Code Go does not support Provider API ZDR routing")
	}
	return value, nil
}

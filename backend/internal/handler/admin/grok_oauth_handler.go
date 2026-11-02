package admin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/mail"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/handler/dto"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

const grokSSOImportConcurrency = 3

type GrokOAuthHandler struct {
	grokOAuthService *service.GrokOAuthService
	adminService     service.AdminService
	quotaService     *service.GrokQuotaService
	importProber     grokImportProber
	reconciler       service.GrokOAuthReconciler
}

func NewGrokOAuthHandler(
	grokOAuthService *service.GrokOAuthService,
	adminService service.AdminService,
	quotaService *service.GrokQuotaService,
	reconciler service.GrokOAuthReconciler,
) *GrokOAuthHandler {
	return &GrokOAuthHandler{
		grokOAuthService: grokOAuthService,
		adminService:     adminService,
		quotaService:     quotaService,
		importProber:     quotaService,
		reconciler:       reconciler,
	}
}

type GrokGenerateAuthURLRequest struct {
	ProxyID     *int64 `json:"proxy_id"`
	RedirectURI string `json:"redirect_uri"`
}

func (h *GrokOAuthHandler) GetCapabilities(c *gin.Context) {
	response.Success(c, h.grokOAuthService.GetCapabilities())
}

func (h *GrokOAuthHandler) GenerateAuthURL(c *gin.Context) {
	var req GrokGenerateAuthURLRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		req = GrokGenerateAuthURLRequest{}
	}
	result, err := h.grokOAuthService.GenerateAuthURL(c.Request.Context(), req.ProxyID, req.RedirectURI)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

type GrokExchangeCodeRequest struct {
	SessionID   string `json:"session_id" binding:"required"`
	Code        string `json:"code" binding:"required"`
	State       string `json:"state"`
	RedirectURI string `json:"redirect_uri"`
	ProxyID     *int64 `json:"proxy_id"`
}

func (h *GrokOAuthHandler) ExchangeCode(c *gin.Context) {
	var req GrokExchangeCodeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	tokenInfo, err := h.grokOAuthService.ExchangeCode(c.Request.Context(), &service.GrokExchangeCodeInput{
		SessionID:   req.SessionID,
		Code:        req.Code,
		State:       req.State,
		RedirectURI: req.RedirectURI,
		ProxyID:     req.ProxyID,
	})
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, tokenInfo)
}

type GrokRefreshTokenRequest struct {
	RefreshToken string `json:"refresh_token"`
	RT           string `json:"rt"`
	ClientID     string `json:"client_id"`
	ProxyID      *int64 `json:"proxy_id"`
}

type GrokSSOTokenRequest struct {
	SSOToken string `json:"sso_token"`
	ProxyID  *int64 `json:"proxy_id"`
}

type GrokPasswordAuthorizeRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
	ProxyID  *int64 `json:"proxy_id"`
}

func (h *GrokOAuthHandler) RefreshToken(c *gin.Context) {
	var req GrokRefreshTokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	refreshToken := strings.TrimSpace(req.RefreshToken)
	if refreshToken == "" {
		refreshToken = strings.TrimSpace(req.RT)
	}
	if refreshToken == "" {
		response.BadRequest(c, "refresh_token is required")
		return
	}

	var proxyURL string
	if req.ProxyID != nil {
		proxy, err := h.adminService.GetProxy(c.Request.Context(), *req.ProxyID)
		if err != nil {
			response.ErrorFrom(c, err)
			return
		}
		if proxy == nil {
			response.BadRequest(c, "GROK_OAUTH_PROXY_NOT_FOUND: proxy not found")
			return
		}
		proxyURL = proxy.URL()
	}
	tokenInfo, err := h.grokOAuthService.RefreshToken(c.Request.Context(), refreshToken, proxyURL, req.ClientID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, tokenInfo)
}

// ValidateSSOToken converts a Web SSO cookie into Build OAuth tokens.
// Response contains OAuth token info only — never echoes sso_token.
func (h *GrokOAuthHandler) ValidateSSOToken(c *gin.Context) {
	var req GrokSSOTokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	tokenInfo, err := h.grokOAuthService.ValidateSSOToken(c.Request.Context(), req.SSOToken, req.ProxyID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, tokenInfo)
}

// AuthorizePassword exchanges email/password for Build OAuth tokens via SSO conversion.
// Response never includes password or raw sso_token.
func (h *GrokOAuthHandler) AuthorizePassword(c *gin.Context) {
	var req GrokPasswordAuthorizeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	tokenInfo, err := h.grokOAuthService.AuthorizePassword(c.Request.Context(), req.Email, req.Password, req.ProxyID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, tokenInfo)
}

func (h *GrokOAuthHandler) RefreshAccountToken(c *gin.Context) {
	accountID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.BadRequest(c, "Invalid account ID")
		return
	}
	account, err := h.adminService.GetAccount(c.Request.Context(), accountID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	if account.Platform != service.PlatformGrok {
		response.BadRequest(c, "Account platform does not match Grok OAuth endpoint")
		return
	}
	if !account.IsOAuth() {
		response.BadRequest(c, "Cannot refresh non-OAuth account credentials")
		return
	}
	tokenInfo, err := h.grokOAuthService.RefreshAccountToken(c.Request.Context(), account)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	newCredentials := h.grokOAuthService.BuildAccountCredentials(tokenInfo)
	newCredentials = service.MergeCredentials(account.Credentials, newCredentials)
	if baseURL := strings.TrimSpace(account.GetCredential("base_url")); baseURL != "" {
		newCredentials["base_url"] = baseURL
	}
	updatedAccount, err := h.adminService.UpdateAccount(c.Request.Context(), accountID, &service.UpdateAccountInput{
		Credentials: newCredentials,
	})
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, dto.AccountFromService(updatedAccount))
}

type GrokOAuthReconcileRequest struct {
	DryRun               *bool `json:"dry_run"`
	Apply                bool  `json:"apply"`
	AfterID              int64 `json:"after_id"`
	Limit                int   `json:"limit"`
	RefreshWindowSeconds int64 `json:"refresh_window_seconds"`
}

func (h *GrokOAuthHandler) ReconcileOAuthAccounts(c *gin.Context) {
	var req GrokOAuthReconcileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request")
		return
	}
	dryRun := true
	if req.DryRun != nil {
		dryRun = *req.DryRun
	}
	if req.Apply == dryRun {
		response.ErrorFrom(c, service.ErrGrokOAuthReconcileMode)
		return
	}
	if req.RefreshWindowSeconds < 0 || req.RefreshWindowSeconds > int64((24*time.Hour)/time.Second) {
		response.ErrorFrom(c, service.ErrGrokOAuthReconcileWindow)
		return
	}
	if h.reconciler == nil {
		response.InternalError(c, "Grok OAuth reconciliation service is unavailable")
		return
	}
	result, err := h.reconciler.ReconcileGrokOAuth(c.Request.Context(), service.GrokOAuthReconcileInput{
		DryRun:        dryRun,
		Apply:         req.Apply,
		AfterID:       req.AfterID,
		Limit:         req.Limit,
		RefreshWindow: time.Duration(req.RefreshWindowSeconds) * time.Second,
	})
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

func (h *GrokOAuthHandler) CreateAccountFromOAuth(c *gin.Context) {
	var req struct {
		SessionID   string  `json:"session_id" binding:"required"`
		Code        string  `json:"code" binding:"required"`
		State       string  `json:"state"`
		RedirectURI string  `json:"redirect_uri"`
		ProxyID     *int64  `json:"proxy_id"`
		Name        string  `json:"name"`
		Concurrency int     `json:"concurrency"`
		Priority    int     `json:"priority"`
		GroupIDs    []int64 `json:"group_ids"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	tokenInfo, err := h.grokOAuthService.ExchangeCode(c.Request.Context(), &service.GrokExchangeCodeInput{
		SessionID:   req.SessionID,
		Code:        req.Code,
		State:       req.State,
		RedirectURI: req.RedirectURI,
		ProxyID:     req.ProxyID,
	})
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	credentials := h.grokOAuthService.BuildAccountCredentials(tokenInfo)

	name := strings.TrimSpace(req.Name)
	if name == "" && tokenInfo.Email != "" {
		name = tokenInfo.Email
	}
	if name == "" {
		name = "Grok OAuth Account"
	}

	account, err := h.adminService.CreateAccount(c.Request.Context(), &service.CreateAccountInput{
		Name:        name,
		Platform:    service.PlatformGrok,
		Type:        service.AccountTypeOAuth,
		Credentials: credentials,
		ProxyID:     req.ProxyID,
		Concurrency: req.Concurrency,
		Priority:    req.Priority,
		GroupIDs:    req.GroupIDs,
	})
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	h.scheduleGrokImportProbe(account)
	response.Success(c, dto.AccountFromService(account))
}

type GrokSSOToOAuthRequest struct {
	SSOTokens          []string       `json:"sso_tokens"`
	SSOToken           string         `json:"sso_token"`
	Name               string         `json:"name"`
	Notes              *string        `json:"notes"`
	ProxyID            *int64         `json:"proxy_id"`
	GroupIDs           []int64        `json:"group_ids"`
	Credentials        map[string]any `json:"credentials"`
	Extra              map[string]any `json:"extra"`
	Concurrency        int            `json:"concurrency"`
	LoadFactor         *int           `json:"load_factor"`
	Priority           int            `json:"priority"`
	RateMultiplier     *float64       `json:"rate_multiplier"`
	ExpiresAt          *int64         `json:"expires_at"`
	AutoPauseOnExpired *bool          `json:"auto_pause_on_expired"`
}

type GrokSSOToOAuthItemResult struct {
	Index                     int          `json:"index"`
	Name                      string       `json:"name,omitempty"`
	Email                     string       `json:"email,omitempty"`
	Account                   *dto.Account `json:"account,omitempty"`
	Error                     string       `json:"error,omitempty"`
	Code                      string       `json:"code,omitempty"`
	NeedsBrowserAuthorization bool         `json:"needs_browser_authorization"`
}

type GrokSSOToOAuthResponse struct {
	Created                        []GrokSSOToOAuthItemResult `json:"created"`
	Failed                         []GrokSSOToOAuthItemResult `json:"failed"`
	Total                          int                        `json:"total"`
	CreatedCount                   int                        `json:"created_count"`
	FailedCount                    int                        `json:"failed_count"`
	NeedsBrowserAuthorizationCount int                        `json:"needs_browser_authorization_count"`
}

type grokSSOImportJob struct {
	index int
	token string
}

type grokSSOImportWorkerResult struct {
	created bool
	item    GrokSSOToOAuthItemResult
}

func (h *GrokOAuthHandler) CreateAccountsFromSSO(c *gin.Context) {
	var req GrokSSOToOAuthRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	tokens := normalizeSSOImportTokens(req.SSOTokens, req.SSOToken)
	if len(tokens) == 0 {
		response.BadRequest(c, "sso_tokens is required")
		return
	}

	ctx := c.Request.Context()
	workerCount := grokSSOImportConcurrency
	if len(tokens) < workerCount {
		workerCount = len(tokens)
	}
	jobs := make(chan grokSSOImportJob)
	items := make([]grokSSOImportWorkerResult, len(tokens))
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				items[job.index] = h.safeCreateAccountFromSSOToken(ctx, req, job.token, job.index+1, len(tokens))
			}
		}()
	}
	for i, token := range tokens {
		jobs <- grokSSOImportJob{index: i, token: token}
	}
	close(jobs)
	wg.Wait()

	result := GrokSSOToOAuthResponse{
		Total:   len(tokens),
		Created: make([]GrokSSOToOAuthItemResult, 0, len(tokens)),
		Failed:  make([]GrokSSOToOAuthItemResult, 0),
	}
	for _, item := range items {
		if item.created {
			result.Created = append(result.Created, item.item)
		} else {
			result.Failed = append(result.Failed, item.item)
			if item.item.NeedsBrowserAuthorization {
				result.NeedsBrowserAuthorizationCount++
			}
		}
	}
	result.CreatedCount = len(result.Created)
	result.FailedCount = len(result.Failed)
	response.Success(c, result)
}

func (h *GrokOAuthHandler) safeCreateAccountFromSSOToken(ctx context.Context, req GrokSSOToOAuthRequest, token string, index, total int) (result grokSSOImportWorkerResult) {
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Error("grok_sso_import_worker_panic", "index", index)
			result = grokSSOImportWorkerResult{
				item: GrokSSOToOAuthItemResult{
					Index: index,
					Error: "GROK_SSO_IMPORT_WORKER_PANIC: import worker failed",
					Code:  "GROK_SSO_IMPORT_WORKER_PANIC",
				},
			}
		}
	}()
	return h.createAccountFromSSOToken(ctx, req, token, index, total)
}

func (h *GrokOAuthHandler) createAccountFromSSOToken(ctx context.Context, req GrokSSOToOAuthRequest, token string, index, total int) grokSSOImportWorkerResult {
	tokenInfo, err := h.grokOAuthService.ConvertFromSSO(ctx, token, req.ProxyID)
	if err != nil {
		return grokSSOImportWorkerResult{item: grokSSOImportFailure(index, err)}
	}

	credentials := grokSSOImportCredentials(h.grokOAuthService.BuildAccountCredentials(tokenInfo), req.Credentials)
	name := grokSSOImportAccountName(req.Name, tokenInfo, index, total)
	expiresAt, autoPauseOnExpired := grokSSOImportExpiry(req.ExpiresAt, req.AutoPauseOnExpired, tokenInfo)
	account, err := h.adminService.CreateAccount(ctx, &service.CreateAccountInput{
		Name:               name,
		Notes:              req.Notes,
		Platform:           service.PlatformGrok,
		Type:               service.AccountTypeOAuth,
		Credentials:        credentials,
		Extra:              cloneGrokSSOMap(req.Extra),
		ProxyID:            req.ProxyID,
		Concurrency:        req.Concurrency,
		LoadFactor:         req.LoadFactor,
		Priority:           req.Priority,
		RateMultiplier:     req.RateMultiplier,
		GroupIDs:           append([]int64(nil), req.GroupIDs...),
		ExpiresAt:          expiresAt,
		AutoPauseOnExpired: autoPauseOnExpired,
	})
	if err != nil {
		item := grokSSOImportFailure(index, err)
		item.Name, item.Email = name, tokenInfo.Email
		return grokSSOImportWorkerResult{item: item}
	}
	h.scheduleGrokImportProbe(account)
	return grokSSOImportWorkerResult{
		created: true,
		item: GrokSSOToOAuthItemResult{
			Index:   index,
			Name:    name,
			Email:   tokenInfo.Email,
			Account: dto.AccountFromService(account),
		},
	}
}

// grokSSOImportCredentials 合并 SSO 兑换出的凭据与导入请求携带的运营侧配置。
// token 字段以 BuildAccountCredentials 为准（请求不可覆盖）；但 base_url 是运营侧
// 配置且 Build 恒写官方地址，会吞掉导入时指定的自定义转发地址——与
// RefreshAccountToken 的保留逻辑对齐，请求显式提供时以请求为准。
func grokSSOImportCredentials(built map[string]any, reqCredentials map[string]any) map[string]any {
	// Only merge operator config from the request — never free-form secrets
	// (password / sso_token / cookie / etc.) into stored credentials.
	allowedReqKeys := map[string]struct{}{
		"base_url": {}, "model_mapping": {},
		"header_override": {}, "header_overrides": {}, "header_override_enabled": {},
		"custom_headers": {},
	}
	ops := map[string]any{}
	for k, v := range reqCredentials {
		if _, ok := allowedReqKeys[k]; !ok {
			continue
		}
		if service.IsSensitiveCredentialKey(k) {
			continue
		}
		ops[k] = v
	}
	credentials := service.MergeCredentials(ops, built)
	// Strip any sensitive keys that might have slipped in via older callers.
	for k := range credentials {
		if service.IsSensitiveCredentialKey(k) {
			// Keep only keys produced by BuildAccountCredentials (tokens).
			if k == "access_token" || k == "refresh_token" || k == "id_token" {
				continue
			}
			delete(credentials, k)
		}
	}
	if reqBaseURL, ok := reqCredentials["base_url"].(string); ok && strings.TrimSpace(reqBaseURL) != "" {
		credentials["base_url"] = strings.TrimSpace(reqBaseURL)
	}
	return service.SanitizeStoredCredentials(service.PlatformGrok, credentials)
}

func grokSSOImportExpiry(requestExpiresAt *int64, requestAutoPause *bool, tokenInfo *service.GrokTokenInfo) (*int64, *bool) {
	if tokenInfo == nil || strings.TrimSpace(tokenInfo.RefreshToken) != "" || tokenInfo.ExpiresAt <= 0 {
		return requestExpiresAt, requestAutoPause
	}

	expiresAt := tokenInfo.ExpiresAt
	if requestExpiresAt != nil && *requestExpiresAt > 0 && *requestExpiresAt < expiresAt {
		expiresAt = *requestExpiresAt
	}
	autoPause := true
	return &expiresAt, &autoPause
}

func cloneGrokSSOMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	clone := make(map[string]any, len(source))
	for key, value := range source {
		clone[key] = cloneGrokSSOValue(value)
	}
	return clone
}

func cloneGrokSSOValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return cloneGrokSSOMap(v)
	case []any:
		clone := make([]any, len(v))
		for i, item := range v {
			clone[i] = cloneGrokSSOValue(item)
		}
		return clone
	default:
		return value
	}
}

func normalizeSSOImportTokens(tokens []string, single string) []string {
	items := make([]string, 0, len(tokens)+1)
	if strings.TrimSpace(single) != "" {
		items = append(items, single)
	}
	items = append(items, tokens...)
	seen := make(map[string]struct{}, len(items))
	result := make([]string, 0, len(items))
	appendToken := func(token string) {
		if token == "" {
			return
		}
		if _, ok := seen[token]; ok {
			return
		}
		seen[token] = struct{}{}
		result = append(result, token)
	}
	for _, item := range items {
		for _, line := range strings.FieldsFunc(item, func(r rune) bool { return r == '\r' || r == '\n' }) {
			line = strings.TrimSpace(line)
			if strings.Contains(line, "---") {
				if ssoImportRecordLabel(line) {
					appendToken(extractSSOImportRecordToken(line))
					continue
				}
				if !explicitSSOImportCookie(line) && !validSSOImportJWT(line) {
					continue
				}
			}
			for _, token := range strings.Split(line, ",") {
				appendToken(xai.NormalizeSSOToken(token))
			}
		}
	}
	return result
}

func explicitSSOImportCookie(value string) bool {
	if strings.HasPrefix(strings.ToLower(value), "cookie:") {
		value = strings.TrimSpace(value[len("cookie:"):])
	}
	for _, part := range strings.Split(value, ";") {
		name, _, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || strings.Contains(name, "---") {
			return false
		}
		if strings.EqualFold(name, "sso") || strings.EqualFold(name, "sso-rw") {
			return true
		}
	}
	return false
}

// Only the final JWT field is a credential. Email is an untrusted label and
// everything between the first and last delimiter is discarded as a password.
func ssoImportRecordLabel(value string) bool {
	label, _, _ := strings.Cut(value, "---")
	label = strings.TrimSpace(label)
	if fields := strings.Fields(label); len(fields) == 2 {
		if n, err := strconv.ParseUint(strings.TrimSuffix(fields[0], "."), 10, 64); err == nil && n > 0 {
			label = fields[1]
		}
	}
	address, err := mail.ParseAddress(label)
	return err == nil && address.Address == label
}

func extractSSOImportRecordToken(value string) string {
	first, last := strings.Index(value, "---"), strings.LastIndex(value, "---")
	if first == last {
		return ""
	}
	token := strings.TrimSpace(value[last+3:])
	if !validSSOImportJWT(token) {
		return ""
	}
	return xai.NormalizeSSOToken(token)
}

func validSSOImportJWT(value string) bool {
	if value == "" || xai.NormalizeSSOToken(value) != value {
		return false
	}
	parts := strings.Split(value, ".")
	if len(parts) != 3 {
		return false
	}
	for i, part := range parts {
		if part == "" {
			return false
		}
		for _, ch := range part {
			if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_') {
				return false
			}
		}
		decoded, err := base64.RawURLEncoding.DecodeString(part)
		if err != nil || len(decoded) == 0 {
			return false
		}
		if i == 2 {
			continue
		}
		var object map[string]any
		if json.Unmarshal(decoded, &object) != nil || object == nil {
			return false
		}
	}
	return true
}

func grokSSOImportAccountName(base string, tokenInfo *service.GrokTokenInfo, index, total int) string {
	base = strings.TrimSpace(base)
	if base == "" && tokenInfo != nil {
		base = strings.TrimSpace(tokenInfo.Email)
	}
	if base == "" {
		base = "Grok OAuth Account"
	}
	if total > 1 {
		return base + " #" + strconv.Itoa(index)
	}
	return base
}

func grokSSOImportErrorMessage(err error) string {
	status := infraerrors.FromError(err)
	if status == nil {
		return ""
	}
	if status.Reason != "" {
		return status.Reason + ": " + status.Message
	}
	return status.Message
}

func grokSSOImportFailure(index int, err error) GrokSSOToOAuthItemResult {
	code := infraerrors.Reason(err)
	if code == "" {
		code = "GROK_SSO_IMPORT_FAILED"
	}
	return GrokSSOToOAuthItemResult{Index: index, Error: grokSSOImportErrorMessage(err), Code: code, NeedsBrowserAuthorization: code == "GROK_SSO_BROWSER_VERIFICATION_REQUIRED"}
}

func (h *GrokOAuthHandler) QueryQuota(c *gin.Context) {
	accountID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.BadRequest(c, "Invalid account ID")
		return
	}
	if h.quotaService == nil {
		response.BadRequest(c, "grok quota service is not enabled")
		return
	}
	result, err := h.quotaService.QueryQuota(c.Request.Context(), accountID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

func (h *GrokOAuthHandler) ResetQuota(c *gin.Context) {
	accountID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.BadRequest(c, "Invalid account ID")
		return
	}
	if h.quotaService == nil {
		response.BadRequest(c, "grok quota service is not enabled")
		return
	}
	// ResetQuota 恒返回 GROK_QUOTA_RESET_UNSUPPORTED（xAI 无 OAuth 配额重置接口），err != nil 恒真为预期。
	//nolint:staticcheck // SA4023
	result, err := h.quotaService.ResetQuota(c.Request.Context(), accountID)
	if err != nil { //nolint:staticcheck // SA4023
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

func (h *GrokOAuthHandler) RuntimeSanity(c *gin.Context) {
	response.Success(c, xai.RuntimeSanity())
}

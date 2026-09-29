package service

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/util/responseheaders"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Error bodies are not allowed to introduce estimated or malformed quantities.
// Reuse the normal parser only after every recognized usage counter is an integer
// >= 0. An incomplete JSON body, string counter or negative bucket stays on the
// existing unmetered error path.
func openAIMeteredHTTPUsage(body []byte) *OpenAIUsage {
	if !gjson.ValidBytes(body) {
		return nil
	}
	for _, path := range []string{"usage", "response.usage", "data.usage", "data.response.usage"} {
		value := gjson.GetBytes(body, path)
		if !value.IsObject() {
			continue
		}
		if !openAIErrorUsageCountersValid(value) {
			return nil
		}
		usage, ok := openAIUsageFromGJSON(value)
		if ok && openAIUsageHasTokens(&usage) {
			return &usage
		}
		return nil
	}
	return nil
}

func openAIErrorUsageCountersValid(value gjson.Result) bool {
	// Validate precisely the fields consumed by openAIUsageFromGJSON. Unknown
	// provider metadata is not a token counter and cannot invalidate real usage.
	for _, path := range []string{
		"input_tokens_details", "prompt_tokens_details", "output_tokens_details", "completion_tokens_details",
	} {
		if nested := value.Get(path); nested.Exists() && !nested.IsObject() {
			return false
		}
	}
	for _, path := range []string{
		"input_tokens", "prompt_tokens", "output_tokens", "completion_tokens", "total_tokens",
		"cache_read_input_tokens", "cache_read_tokens", "cached_tokens", "cache_write_tokens",
		"cache_creation_input_tokens", "cache_write_input_tokens", "cache_creation_tokens",
		"input_tokens_details.cached_tokens", "prompt_tokens_details.cached_tokens",
		"input_tokens_details.cache_write_tokens", "prompt_tokens_details.cache_write_tokens",
		"input_tokens_details.cache_creation_tokens", "prompt_tokens_details.cache_creation_tokens",
		"input_tokens_details.image_tokens", "prompt_tokens_details.image_tokens",
		"output_tokens_details.image_tokens", "completion_tokens_details.image_tokens",
		"output_tokens_details.reasoning_tokens", "completion_tokens_details.reasoning_tokens",
	} {
		counter := value.Get(path)
		if !counter.Exists() {
			continue
		}
		n, err := strconv.ParseInt(counter.Raw, 10, 64)
		if counter.Type != gjson.Number || err != nil || n < 0 {
			return false
		}
	}
	return true
}

// The caller invokes this immediately after reading an HTTP error body, before
// any request rewrite or account failover can dispatch another paid attempt.
func (s *OpenAIGatewayService) handleMeteredOpenAIHTTPError(ctx context.Context, c *gin.Context, account *Account, resp *http.Response, responseBody, requestBody []byte, originalModel, billingModel, upstreamModel string, stream bool, startTime time.Time) (*OpenAIForwardResult, error) {
	if resp.StatusCode < http.StatusBadRequest {
		return nil, nil
	}
	usage := openAIMeteredHTTPUsage(responseBody)
	if usage == nil {
		return nil, nil
	}
	markOpenAICyberPolicyEvent(c, responseBody, resp.StatusCode, usage)
	defer func() { _ = resp.Body.Close() }()
	responseBody = s.redactAgentIdentitySensitiveBody(ctx, account, responseBody)
	observer := upstreamResponseModelObserverFromContext(c)
	if observer == nil {
		observer = beginUpstreamResponseModelObservation(c)
	}
	observer.ObserveOpenAI(responseBody, "")
	message := extractOpenAISSEErrorMessage(responseBody)
	if message == "" {
		message = fmt.Sprintf("Upstream returned HTTP %d", resp.StatusCode)
	}
	if account.IsGrok() {
		s.handleGrokAccountUpstreamError(withGrokTeamRateLimitModel(ctx, upstreamModel), account, resp.StatusCode, resp.Header, responseBody)
	} else {
		s.handleOpenAIAccountUpstreamError(ctx, account, resp.StatusCode, resp.Header, responseBody, upstreamModel)
	}
	setOpsUpstreamError(c, resp.StatusCode, message, "")
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
		Platform: account.Platform, AccountID: account.ID, AccountName: account.Name,
		ProxyID: opsUpstreamProxyID(account), ProxyName: opsUpstreamProxyName(account),
		UpstreamStatusCode: resp.StatusCode, UpstreamRequestID: firstNonEmpty(resp.Header.Get("x-request-id"), resp.Header.Get("xai-request-id")),
		Kind: "http_error", Message: message,
	})
	result := openAICompatMeteredResult(c, resp, *usage, originalModel, billingModel, upstreamModel, startTime)
	result.RequestID = firstNonEmpty(resp.Header.Get("x-request-id"), resp.Header.Get("xai-request-id"))
	result.Stream = stream
	result.ServiceTier = resolvedOpenAIUpstreamServiceTier(c, extractOpenAIServiceTierFromBody(requestBody))
	result.ReasoningEffort = extractOpenAIReasoningEffortFromBody(requestBody, upstreamModel, billingModel, originalModel)
	forwardErr := fmt.Errorf("upstream HTTP %d after reported usage: %s", resp.StatusCode, message)
	compactCommitted := StopOpenAICompactSSEKeepaliveCommitted(c)
	errorType := firstNonEmpty(gjson.GetBytes(responseBody, "error.type").String(), gjson.GetBytes(responseBody, "response.error.type").String(), "upstream_error")
	errorBody := gin.H{"type": errorType, "message": message}
	if code := extractUpstreamErrorCode(responseBody); code != "" {
		errorBody["code"] = code
	}
	clientBody := gin.H{"error": errorBody}
	path := strings.TrimRight(c.Request.URL.Path, "/")
	if strings.HasSuffix(path, "/messages") {
		clientBody["type"] = "error"
		clientBody = anthropicErrorBodyWithOpenAIUsage(clientBody, usage)
	} else {
		clientBody = openAIErrorBodyWithUsage(clientBody, usage)
	}
	MarkResponseCommitted(c)
	clientBody = openAIMeteredErrorMetadata(c, clientBody)
	if compactCommitted || (c.Writer.Written() && strings.HasPrefix(c.Writer.Header().Get("Content-Type"), "text/event-stream")) {
		// A queue or compact heartbeat has already committed HTTP 200. Finish
		// its existing SSE protocol, retaining the real upstream error and usage.
		event := ""
		switch {
		case strings.HasSuffix(path, "/messages"):
			event = "event: error\n"
		case strings.HasSuffix(path, "/chat/completions"):
		default:
			event = "event: response.failed\n"
			clientBody["id"] = "resp_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			clientBody["object"] = "response"
			clientBody["status"] = "failed"
			clientBody["output"] = []any{}
			clientBody = gin.H{"type": "response.failed", "response": clientBody}
		}
		payload, _ := marshalOpenAIUpstreamJSON(clientBody)
		_, _ = fmt.Fprintf(c.Writer, "%sdata: %s\n\n", event, payload)
		c.Writer.Flush()
		return result, forwardErr
	}
	responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	c.Header("Content-Type", "application/json; charset=utf-8")
	c.JSON(resp.StatusCode, clientBody)
	return result, forwardErr
}

// Optional usage extends the existing error envelope without changing its
// status or error fields. Only upstream-reported quantities are supplied here.
func openAIErrorBodyWithUsage(body gin.H, usage ...*OpenAIUsage) gin.H {
	if len(usage) > 0 && openAIUsageHasTokens(usage[0]) {
		body["usage"] = usage[0]
	}
	return body
}

func anthropicErrorBodyWithOpenAIUsage(body gin.H, usage ...*OpenAIUsage) gin.H {
	if len(usage) > 0 && openAIUsageHasTokens(usage[0]) {
		converted := *usage[0]
		// Messages input_tokens excludes both cache buckets; Responses includes them.
		converted.InputTokens = max(0, converted.InputTokens-converted.CacheReadInputTokens-converted.CacheCreationInputTokens)
		body["usage"] = &converted
	}
	return body
}

func openAIMeteredErrorMetadata(c *gin.Context, body gin.H) gin.H {
	if body["usage"] != nil {
		if tier := observedUpstreamResponseServiceTier(c); tier != "" {
			body["service_tier"] = tier
		}
		if model := observedUpstreamResponseModel(c); model != "" {
			body["model"] = model
		}
	}
	return body
}

func openAIErrorSSEWithUsage(payload []byte, usage ...*OpenAIUsage) []byte {
	if len(usage) == 0 || !openAIUsageHasTokens(usage[0]) {
		return payload
	}
	path := "usage"
	if gjson.GetBytes(payload, "response").Exists() {
		path = "response.usage"
	}
	if updated, err := sjson.SetBytes(payload, path, usage[0]); err == nil {
		return updated
	}
	return payload
}

// Preserve only observed usage on an unsuccessful buffered attempt. A nil
// result keeps the existing zero-usage error and failover behavior unchanged.
func openAICompatMeteredResult(c *gin.Context, resp *http.Response, usage OpenAIUsage, originalModel, billingModel, upstreamModel string, startTime time.Time) *OpenAIForwardResult {
	if !openAIUsageHasTokens(&usage) {
		return nil
	}
	return &OpenAIForwardResult{
		RequestID:                     resp.Header.Get("x-request-id"),
		UpstreamHeaders:               resp.Header,
		Usage:                         usage,
		Model:                         originalModel,
		BillingModel:                  billingModel,
		UpstreamModel:                 upstreamModel,
		UpstreamResponseModel:         observedUpstreamResponseModel(c),
		UpstreamResponseModelConflict: observedUpstreamResponseModelConflict(c),
		UpstreamResponseServiceTier:   observedUpstreamResponseServiceTier(c),
		Duration:                      time.Since(startTime),
	}
}

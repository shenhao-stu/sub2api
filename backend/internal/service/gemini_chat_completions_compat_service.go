package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/Wei-Shaw/sub2api/internal/pkg/geminicli"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/util/responseheaders"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ForwardAsChatCompletions serves OpenAI Chat Completions clients through
// Gemini accounts. It keeps the client-facing response in Chat Completions
// format while routing the upstream call through Gemini native endpoints.
func (s *GeminiMessagesCompatService) ForwardAsChatCompletions(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	body []byte,
) (*ForwardResult, error) {
	return s.forwardAsOpenAI(ctx, c, account, body, nil)
}

func (s *GeminiMessagesCompatService) forwardAsOpenAI(ctx context.Context, c *gin.Context, account *Account, body []byte, output *geminiResponsesOutput) (*ForwardResult, error) {
	startTime := time.Now()

	var ccReq apicompat.ChatCompletionsRequest
	if err := json.Unmarshal(body, &ccReq); err != nil {
		return nil, s.writeChatCompletionsError(c, http.StatusBadRequest, "invalid_request_error", "Failed to parse request body")
	}
	if strings.TrimSpace(ccReq.Model) == "" {
		return nil, s.writeChatCompletionsError(c, http.StatusBadRequest, "invalid_request_error", "model is required")
	}

	clientStream := ccReq.Stream
	includeUsage := ccReq.StreamOptions != nil && ccReq.StreamOptions.IncludeUsage

	// 历史推理内容不回传给 Gemini：Gemini 的思考连续性由 thoughtSignature 承载。
	for i := range ccReq.Messages {
		ccReq.Messages[i].ReasoningContent = ""
		ccReq.Messages[i].Reasoning = ""
		if ccReq.Messages[i].Role == "assistant" {
			ccReq.Messages[i].Content = dropChatReasoningContentParts(ccReq.Messages[i].Content)
		}
	}

	contents, fields := geminiChatContents(body)
	if fields != nil {
		writeOpenAIErrorFields(c, *fields, false)
		return nil, errors.New(fields.Message)
	}
	configReq := ccReq
	configReq.Messages = nil
	for _, message := range ccReq.Messages {
		if message.Role == "system" || message.Role == "developer" {
			configReq.Messages = append(configReq.Messages, message)
		}
	}
	responsesReq, err := chatCompletionsToResponses(s.cfg, &configReq)
	if err != nil {
		return nil, s.writeChatCompletionsError(c, http.StatusBadRequest, "invalid_request_error", err.Error())
	}

	anthropicReq, err := apicompat.ResponsesToAnthropicRequest(responsesReq)
	if err != nil {
		return nil, s.writeChatCompletionsError(c, http.StatusBadRequest, "invalid_request_error", err.Error())
	}
	anthropicReq.Stream = clientStream

	claudeBody, err := json.Marshal(anthropicReq)
	if err != nil {
		return nil, fmt.Errorf("marshal chat completions compat request: %w", err)
	}

	return s.forwardClaudeBodyAsChatCompletions(ctx, c, account, claudeBody, contents, &ccReq, clientStream, includeUsage, startTime, output)
}

func (s *GeminiMessagesCompatService) forwardClaudeBodyAsChatCompletions(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	claudeBody []byte,
	nativeContents []byte,
	chatReq *apicompat.ChatCompletionsRequest,
	clientStream bool,
	includeUsage bool,
	startTime time.Time,
	output *geminiResponsesOutput,
) (*ForwardResult, error) {
	originalModel := chatReq.Model
	var req struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
	}
	if err := json.Unmarshal(claudeBody, &req); err != nil {
		return nil, s.writeChatCompletionsError(c, http.StatusBadRequest, "invalid_request_error", "Failed to parse request body")
	}
	if strings.TrimSpace(req.Model) == "" {
		return nil, s.writeChatCompletionsError(c, http.StatusBadRequest, "invalid_request_error", "model is required")
	}

	mappedModel := req.Model
	if account.Type == AccountTypeAPIKey || account.Type == AccountTypeServiceAccount {
		mappedModel = account.GetMappedModel(req.Model)
	}
	if err := rejectDisabledModels(c, s.cfg, req.Model, mappedModel); err != nil {
		return nil, err
	}

	geminiReq, err := convertClaudeMessagesToGeminiGenerateContent(claudeBody)
	if err != nil {
		return nil, s.writeChatCompletionsError(c, http.StatusBadRequest, "invalid_request_error", err.Error())
	}
	geminiReq = applyGeminiCompatThinkingConfig(geminiReq, claudeBody, mappedModel, clientStream)
	geminiReq, err = sjson.SetRawBytes(geminiReq, "contents", nativeContents)
	if err != nil {
		return nil, err
	}
	toolConfig, fields := geminiChatToolConfig(chatReq)
	if fields != nil {
		writeOpenAIErrorFields(c, *fields, false)
		return nil, errors.New(fields.Message)
	}
	if len(toolConfig) > 0 {
		geminiReq, err = sjson.SetRawBytes(geminiReq, "toolConfig", toolConfig)
		if err != nil {
			return nil, err
		}
	}
	if fields := geminiChatFinalTurnError(mappedModel, chatReq.Messages, geminiReq); fields != nil {
		writeOpenAIErrorFields(c, *fields, false)
		return nil, errors.New(fields.Message)
	}
	if geminiImageGenerationDenied(c, geminiReq, originalModel, req.Model, mappedModel) {
		return nil, s.writeChatCompletionsError(c, http.StatusBadRequest, "invalid_request_error", ImageGenerationPermissionMessage())
	}

	proxyURL := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}

	useUpstreamStream := clientStream
	if account.Type == AccountTypeOAuth && !clientStream && strings.TrimSpace(account.GetCredential("project_id")) != "" {
		useUpstreamStream = true
	}

	buildReq, requestIDHeader := s.buildGeminiChatCompletionsUpstreamRequestFunc(
		account,
		mappedModel,
		geminiReq,
		clientStream,
		useUpstreamStream,
	)

	upstreamCtx, releaseUpstream := boundedGeminiUpstreamContext(ctx)
	defer releaseUpstream()
	var resp *http.Response
	// checkedPolicy 是循环内对当前 resp 的错误策略检查结果，未检查时为 nil。
	var checkedPolicy *ErrorPolicyResult
	maxAttempts := s.geminiMaxAttemptsPerAccount()
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		upstreamReq, idHeader, err := buildReq(upstreamCtx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			if failoverErr := s.handleVertexServiceAccountNotFound(ctx, c, account, err); failoverErr != nil {
				return nil, failoverErr
			}
			return nil, s.writeChatCompletionsError(c, http.StatusBadGateway, "upstream_error", err.Error())
		}
		requestIDHeader = idHeader
		// Authentication may have waited for token refresh. A canceled caller
		// must not start a new generation after that wait.
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		resp, err = s.httpUpstream.Do(upstreamReq, proxyURL, account.ID, account.Concurrency)
		if err != nil {
			return nil, s.handleUpstreamTransportError(ctx, c, account, err)
		}

		if paymentErr := s.geminiPaymentFailover(ctx, c, account, resp, mappedModel); paymentErr != nil {
			return nil, paymentErr
		}

		checked, rebuilt := s.checkErrorPolicyInLoop(ctx, account, resp, mappedModel)
		resp = rebuilt
		checkedPolicy = checked
		if checked != nil && *checked != ErrorPolicyNone {
			break
		}

		if resp.StatusCode >= 400 && s.shouldRetryGeminiUpstreamError(account, resp.StatusCode) {
			respBody := s.readUpstreamErrorBody(resp)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusForbidden && isGeminiInsufficientScope(resp.Header, respBody) {
				resp = &http.Response{
					StatusCode: resp.StatusCode,
					Header:     resp.Header.Clone(),
					Body:       io.NopCloser(bytes.NewReader(respBody)),
				}
				break
			}
			if delay, _ := vertexModelCooldown(account, resp.StatusCode, resp.Header, respBody, mappedModel, time.Now()); delay > 0 {
				// A definite capacity rejection can use the existing failover path;
				// retrying this same account immediately defeats the advertised backoff.
				resp.Body = io.NopCloser(bytes.NewReader(respBody))
				break
			}
			if resp.StatusCode == http.StatusTooManyRequests {
				s.handleGeminiUpstreamError(ctx, account, resp.StatusCode, resp.Header, respBody, mappedModel)
			}
			if attempt < maxAttempts {
				upstreamReqID := resp.Header.Get(requestIDHeader)
				if upstreamReqID == "" {
					upstreamReqID = resp.Header.Get("x-goog-request-id")
				}
				upstreamMsg := strings.TrimSpace(extractUpstreamErrorMessage(respBody))
				upstreamMsg = sanitizeUpstreamErrorMessage(upstreamMsg)
				appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
					ProxyID:            opsUpstreamProxyID(account),
					ProxyName:          opsUpstreamProxyName(account),
					Platform:           account.Platform,
					AccountID:          account.ID,
					AccountName:        account.Name,
					UpstreamStatusCode: resp.StatusCode,
					UpstreamRequestID:  upstreamReqID,
					Kind:               "retry",
					Message:            upstreamMsg,
				})
				logger.LegacyPrintf("service.gemini_chat_completions", "Gemini account %d: upstream status %d, retry %d/%d", account.ID, resp.StatusCode, attempt, maxAttempts)
				sleepGeminiBackoff(attempt)
				continue
			}
			resp = &http.Response{
				StatusCode: resp.StatusCode,
				Header:     resp.Header.Clone(),
				Body:       io.NopCloser(bytes.NewReader(respBody)),
			}
			break
		}

		break
	}
	defer func() { _ = resp.Body.Close() }()
	stopDrain := boundDisconnectedResponseDrain(ctx, resp.Body)
	defer stopDrain()

	requestID := resp.Header.Get(requestIDHeader)
	if requestID == "" {
		requestID = resp.Header.Get("x-goog-request-id")
	}
	if requestID != "" {
		c.Header("x-request-id", requestID)
	}

	reasoningEffort := extractGeminiReasoningEffortFromBody(geminiReq)

	if resp.StatusCode >= 400 {
		respBody := s.readUpstreamErrorBody(resp)
		policy := ErrorPolicyNone
		if s.rateLimitService != nil {
			policy = s.finalErrorPolicy(ctx, account, resp.StatusCode, respBody, mappedModel, checkedPolicy)
		}
		// 与 messages 兼容层一致：只有 None / Matched 才走账号状态处理。
		// Skipped（池模式、或自定义错误码未命中）与 TempUnscheduled 已由策略层裁决完毕。
		if policy == ErrorPolicyNone || policy == ErrorPolicyMatched {
			s.handleGeminiUpstreamError(ctx, account, resp.StatusCode, resp.Header, respBody, mappedModel)
		}
		evBody := unwrapIfNeeded(account.Type == AccountTypeOAuth, respBody)

		if s.shouldFailoverGeminiUpstreamError(resp.StatusCode) {
			upstreamMsg := sanitizeUpstreamErrorMessage(strings.TrimSpace(extractUpstreamErrorMessage(evBody)))
			appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
				ProxyID:            opsUpstreamProxyID(account),
				ProxyName:          opsUpstreamProxyName(account),
				Platform:           account.Platform,
				AccountID:          account.ID,
				AccountName:        account.Name,
				UpstreamStatusCode: resp.StatusCode,
				UpstreamRequestID:  requestID,
				Kind:               "failover",
				Message:            upstreamMsg,
			})
			return nil, &UpstreamFailoverError{
				StatusCode:             resp.StatusCode,
				ResponseBody:           evBody,
				RetryableOnSameAccount: account.IsPoolMode() && account.IsPoolModeRetryableStatus(resp.StatusCode),
			}
		}

		if policy == ErrorPolicySkipped && account.IsCustomErrorCodesEnabled() {
			return nil, s.writeGeminiCustomCodeSkippedError(c, account, resp.StatusCode, requestID, evBody, func() {
				_ = s.writeChatCompletionsError(c, http.StatusInternalServerError, "api_error", geminiCustomCodeSkippedClientMessage)
			})
		}
		return nil, s.writeGeminiChatCompletionsMappedError(c, account, resp.StatusCode, requestID, evBody)
	}

	var usage *ClaudeUsage
	var deliveryErr error
	var firstTokenMs *int
	clientDisconnect := false
	streamFinished := false
	if clientStream {
		streamRes, err := s.handleOpenAIStreamingResponseFromGemini(c, resp, startTime, originalModel, account.Type == AccountTypeOAuth, includeUsage, output)
		if streamRes == nil {
			return nil, err
		}
		deliveryErr = err
		usage = streamRes.usage
		firstTokenMs = streamRes.firstTokenMs
		clientDisconnect = streamRes.clientDisconnect
		streamFinished = streamRes.finished
	} else if useUpstreamStream {
		usage, deliveryErr = s.handleOpenAIBufferedStreamFromGemini(c, resp, originalModel, account.Type == AccountTypeOAuth, output)
	} else {
		usageResp, err := s.handleOpenAINonStreamingResponseFromGemini(c, resp, originalModel, account.Type == AccountTypeOAuth, output)
		if usageResp == nil {
			return nil, err
		}
		usage = usageResp
		deliveryErr = err
	}

	if usage == nil {
		usage = &ClaudeUsage{}
	}
	if deliveryErr != nil && !usage.hasObservedTokens() && usage.receiptMeasured == nil {
		return nil, deliveryErr
	}

	imageCount := 0
	imageInputSize := s.extractImageInputSize(claudeBody)
	imageSize := normalizeOpenAIImageSizeTier(imageInputSize)
	if isImageGenerationModel(originalModel) {
		imageCount = 1
	}
	// 客户端在收到结束分片前断开时只计已送达的图片：Chat 流不向客户端转发图片。
	if clientDisconnect && !streamFinished {
		imageCount = 0
	}

	clientDisconnect = clientDisconnect || ctx.Err() != nil
	return &ForwardResult{
		RequestID:        requestID,
		UpstreamHeaders:  resp.Header,
		Usage:            *usage,
		Model:            originalModel,
		UpstreamModel:    mappedModel,
		Stream:           clientStream,
		Duration:         time.Since(startTime),
		FirstTokenMs:     firstTokenMs,
		ReasoningEffort:  reasoningEffort,
		ImageEndpoint:    imageCount > 0,
		ImageCount:       imageCount,
		ImageSize:        imageSize,
		ImageInputSize:   imageInputSize,
		ClientDisconnect: clientDisconnect,
	}, deliveryErr
}

func (s *GeminiMessagesCompatService) buildGeminiChatCompletionsUpstreamRequestFunc(
	account *Account,
	mappedModel string,
	geminiReq []byte,
	clientStream bool,
	useUpstreamStream bool,
) (func(context.Context) (*http.Request, string, error), string) {
	switch account.Type {
	case AccountTypeAPIKey:
		return func(ctx context.Context) (*http.Request, string, error) {
			apiKey := account.GetCredential("api_key")
			if strings.TrimSpace(apiKey) == "" {
				return nil, "", errors.New("gemini api_key not configured")
			}

			baseURL := account.GetGeminiBaseURL(geminicli.AIStudioBaseURL)
			normalizedBaseURL, err := s.validateUpstreamBaseURL(baseURL)
			if err != nil {
				return nil, "", err
			}

			action := "generateContent"
			if clientStream {
				action = "streamGenerateContent"
			}
			fullURL, err := buildGeminiAIStudioModelActionURL(normalizedBaseURL, mappedModel, action, clientStream)
			if err != nil {
				return nil, "", err
			}

			restGeminiReq := normalizeGeminiChatRequest(geminiReq)
			upstreamReq, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, bytes.NewReader(restGeminiReq))
			if err != nil {
				return nil, "", err
			}
			upstreamReq.Header.Set("Content-Type", "application/json")
			upstreamReq.Header.Set("x-goog-api-key", apiKey)
			return upstreamReq, "x-request-id", nil
		}, "x-request-id"

	case AccountTypeOAuth:
		return func(ctx context.Context) (*http.Request, string, error) {
			if s.tokenProvider == nil {
				return nil, "", errors.New("gemini token provider not configured")
			}
			accessToken, err := s.tokenProvider.GetAccessToken(ctx, account)
			if err != nil {
				return nil, "", err
			}

			projectID := strings.TrimSpace(account.GetCredential("project_id"))
			action := "generateContent"
			if useUpstreamStream {
				action = "streamGenerateContent"
			}

			if projectID != "" {
				baseURL, err := s.validateUpstreamBaseURL(geminicli.GeminiCliBaseURL)
				if err != nil {
					return nil, "", err
				}
				fullURL := fmt.Sprintf("%s/v1internal:%s", strings.TrimRight(baseURL, "/"), action)
				if useUpstreamStream {
					fullURL += "?alt=sse"
				}

				inner := json.RawMessage(geminiReq)
				wrappedBytes, _ := json.Marshal(map[string]any{
					"model":   mappedModel,
					"project": projectID,
					"request": inner,
				})

				upstreamReq, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, bytes.NewReader(wrappedBytes))
				if err != nil {
					return nil, "", err
				}
				upstreamReq.Header.Set("Content-Type", "application/json")
				upstreamReq.Header.Set("Authorization", "Bearer "+accessToken)
				upstreamReq.Header.Set("User-Agent", geminicli.GeminiCLIUserAgent)
				return upstreamReq, "x-request-id", nil
			}

			baseURL := account.GetGeminiBaseURL(geminicli.AIStudioBaseURL)
			normalizedBaseURL, err := s.validateUpstreamBaseURL(baseURL)
			if err != nil {
				return nil, "", err
			}

			fullURL, err := buildGeminiAIStudioModelActionURL(normalizedBaseURL, mappedModel, action, useUpstreamStream)
			if err != nil {
				return nil, "", err
			}

			restGeminiReq := normalizeGeminiChatRequest(geminiReq)
			upstreamReq, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, bytes.NewReader(restGeminiReq))
			if err != nil {
				return nil, "", err
			}
			upstreamReq.Header.Set("Content-Type", "application/json")
			upstreamReq.Header.Set("Authorization", "Bearer "+accessToken)
			return upstreamReq, "x-request-id", nil
		}, "x-request-id"

	case AccountTypeServiceAccount:
		return func(ctx context.Context) (*http.Request, string, error) {
			action := "generateContent"
			if clientStream {
				action = "streamGenerateContent"
			}
			fullURL, err := buildVertexGeminiAccountURL(account, mappedModel, action, clientStream)
			if err != nil {
				return nil, "", err
			}

			restGeminiReq := normalizeGeminiChatRequest(geminiReq)
			upstreamReq, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, bytes.NewReader(restGeminiReq))
			if err != nil {
				return nil, "", err
			}
			upstreamReq.Header.Set("Content-Type", "application/json")
			if err := setVertexGeminiAuth(ctx, upstreamReq, account, s.tokenProvider); err != nil {
				return nil, "", err
			}
			return upstreamReq, "x-request-id", nil
		}, "x-request-id"

	default:
		return func(context.Context) (*http.Request, string, error) {
			return nil, "", fmt.Errorf("unsupported account type: %s", account.Type)
		}, "x-request-id"
	}
}

func (s *GeminiMessagesCompatService) handleChatCompletionsNonStreamingResponseFromGemini(
	c *gin.Context,
	resp *http.Response,
	originalModel string,
	isOAuth bool,
) (*ClaudeUsage, error) {
	return s.handleOpenAINonStreamingResponseFromGemini(c, resp, originalModel, isOAuth, nil)
}

func (s *GeminiMessagesCompatService) handleOpenAINonStreamingResponseFromGemini(c *gin.Context, resp *http.Response, originalModel string, isOAuth bool, output *geminiResponsesOutput) (*ClaudeUsage, error) {
	respBody, readErr := ReadUpstreamResponseBody(resp.Body, s.cfg, c, openAITooLargeError)
	if isOAuth {
		if unwrappedBody, uwErr := unwrapGeminiResponse(respBody); uwErr == nil {
			respBody = unwrappedBody
		}
	}

	var geminiResp map[string]any
	if err := decodeGeminiChatObject(respBody, &geminiResp); err != nil {
		return nil, s.writeChatCompletionsError(c, http.StatusBadGateway, "upstream_error", "Failed to parse upstream response")
	}

	if readErr != nil {
		meter := &geminiOpenAIMeterState{}
		meter.observe(respBody)
		setOpsUpstreamError(c, http.StatusBadGateway, "Upstream response delivery was incomplete", "")
		return &meter.usage, readErr
	}
	return s.writeOpenAINativeGeminiJSON(c, resp, geminiResp, respBody, originalModel, output, nil)
}

func geminiResponseToChatCompletions(
	geminiResp map[string]any,
	originalModel string,
	rawData []byte,
	usageOverride *ClaudeUsage,
) (*apicompat.ChatCompletionsResponse, *ClaudeUsage, error) {
	usage := usageOverride
	if usage == nil {
		usage = extractGeminiUsage(rawData)
	}
	if usage == nil {
		usage = &ClaudeUsage{}
	}
	stop := mapGeminiFinishReasonToClaudeStopReason(extractGeminiFinishReason(geminiResp))
	response := apicompat.AnthropicResponse{ID: generateAnthropicMsgID(), Type: "message", Role: "assistant", Model: originalModel, StopReason: &stop, Content: []apicompat.AnthropicContentBlock{}, Usage: apicompat.AnthropicUsage{InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens, CacheReadInputTokens: usage.CacheReadInputTokens}}
	for _, part := range extractGeminiParts(geminiResp) {
		if text, ok := part["text"].(string); ok && text != "" {
			block := apicompat.AnthropicContentBlock{Type: "text", Text: text}
			if isGeminiThoughtPart(part) {
				block = apicompat.AnthropicContentBlock{Type: "thinking", Thinking: text}
			}
			response.Content = append(response.Content, block)
		}
		if data, ok := part["inlineData"].(map[string]any); ok {
			mime, _ := data["mimeType"].(string)
			payload, _ := data["data"].(string)
			if isGeminiInlineImageMIMEType(mime) && isValidBase64(payload) {
				response.Content = append(response.Content, apicompat.AnthropicContentBlock{Type: "text", Text: fmt.Sprintf("![image](data:%s;base64,%s)", mime, payload)})
			}
		}
		if call, ok := part["functionCall"].(map[string]any); ok {
			if fields := validateGeminiOpenAIFunction(call); fields != nil {
				return nil, usage, errors.New(fields.Message)
			}
			args := json.RawMessage(`{}`)
			if value := call["args"]; value != nil {
				if text, ok := value.(string); ok {
					args = json.RawMessage(text)
				} else {
					args, _ = json.Marshal(value)
				}
			}
			response.Content = append(response.Content, apicompat.AnthropicContentBlock{Type: "tool_use", ID: "toolu_" + randomHex(8), Name: call["name"].(string), Input: args})
			if stop != "max_tokens" {
				stop = "tool_use"
			}
		}
	}
	responsesResp := apicompat.AnthropicToResponsesResponse(&response)
	return apicompat.ResponsesToChatCompletions(responsesResp, originalModel), usage, nil
}

func (s *GeminiMessagesCompatService) handleChatCompletionsStreamingResponseFromGemini(
	c *gin.Context,
	resp *http.Response,
	startTime time.Time,
	originalModel string,
	isOAuth bool,
	includeUsage bool,
) (*geminiStreamResult, error) {
	return s.handleOpenAIStreamingResponseFromGemini(c, resp, startTime, originalModel, isOAuth, includeUsage, nil)
}

func (s *GeminiMessagesCompatService) handleOpenAIStreamingResponseFromGemini(c *gin.Context, resp *http.Response, startTime time.Time, originalModel string, isOAuth, includeUsage bool, output *geminiResponsesOutput) (*geminiStreamResult, error) {
	if s.responseHeaderFilter != nil {
		responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	}
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("X-Accel-Buffering", "no")
	c.Writer.WriteHeader(http.StatusOK)

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		return nil, errors.New("streaming not supported")
	}

	anthState := apicompat.NewAnthropicEventToResponsesState()
	anthState.Model = originalModel
	ccState := apicompat.NewResponsesEventToChatState()
	ccState.Model = originalModel
	ccState.IncludeUsage = includeUsage

	var usage ClaudeUsage
	var firstTokenMs *int
	firstChunk := true
	finishReason := ""
	clientDisconnect := false
	drainCtx, disconnect := context.WithCancel(c.Request.Context())
	defer disconnect()
	stopDrain := boundDisconnectedResponseDrain(drainCtx, resp.Body)
	defer stopDrain()
	var nativeMeter geminiOpenAIMeterState
	finalMeterSeen := false
	doneSeen := false
	var upstreamFailure *openAIClientErrorFields
	markDisconnected := func() { clientDisconnect = true; disconnect() }
	streamResult := func() *geminiStreamResult {
		return &geminiStreamResult{usage: &usage, firstTokenMs: firstTokenMs, clientDisconnect: clientDisconnect || c.Request.Context().Err() != nil, finished: finishReason == "STOP" || finishReason == "MAX_TOKENS"}
	}

	toolSignatures := map[string]string{}
	var restorer *apicompat.ResponsesClientToolStreamRestorer
	if output != nil {
		restorer = apicompat.NewResponsesClientToolStreamRestorer(output.mapping)
	}
	writeResponsesEvent := func(evt apicompat.ResponsesStreamEvent) bool {
		if clientDisconnect || c.Request.Context().Err() != nil {
			markDisconnected()
			return false
		}
		raw, err := json.Marshal(evt)
		if err != nil {
			return true
		}
		events, _, err := restorer.RestoreEvent(raw)
		if err != nil {
			return true
		}
		for _, event := range events {
			event, err = geminiResponsesSignatures(event, toolSignatures)
			if err != nil {
				return true
			}
			kind := gjson.GetBytes(event, "type").String()
			if _, err := fmt.Fprintf(c.Writer, "event: %s\ndata: %s\n\n", kind, event); err != nil {
				markDisconnected()
				break
			}
		}
		return false
	}
	fail := func(code, message string, cause error) error {
		fields := openAIClientErrorFields{Type: "upstream_error", Code: code, Message: message, StatusCode: http.StatusBadGateway}
		if upstreamFailure != nil {
			fields = *upstreamFailure
			message = fields.Message
		}
		if output != nil {
			err := errors.New(message)
			if cause != nil {
				err = fmt.Errorf("%s: %w", message, cause)
			}
			setOpsUpstreamError(c, fields.StatusCode, message, "")
			if !clientDisconnect {
				writeAnthropicCompatResponsesStreamFailure(c, anthState, restorer, &anthropicCompatStreamFailure{err: err, errType: fields.Type, message: message, fields: &fields}, &usage)
			}
			return err
		}
		return geminiOpenAIStreamFailure(c, clientDisconnect, fields, cause, &usage)
	}
	writeChatChunk := func(chunk apicompat.ChatCompletionsChunk) bool {
		if clientDisconnect || c.Request.Context().Err() != nil {
			markDisconnected()
			return false
		}
		sse, err := geminiChatChunkSSE(chunk, toolSignatures)
		if err != nil {
			return false
		}
		if _, err := io.WriteString(c.Writer, sse); err != nil {
			markDisconnected()
		}
		return false
	}

	emitAnthropicEvent := func(evt *apicompat.AnthropicStreamEvent) bool {
		responsesEvents := apicompat.AnthropicEventToResponsesEvents(evt, anthState)
		for _, resEvt := range responsesEvents {
			if output != nil {
				if writeResponsesEvent(resEvt) {
					return true
				}
				continue
			}
			chunks := apicompat.ResponsesEventToChatChunks(&resEvt, ccState)
			for _, chunk := range chunks {
				if disconnected := writeChatChunk(chunk); disconnected {
					return true
				}
			}
		}
		if !clientDisconnect {
			flusher.Flush()
		}
		return false
	}

	messageID := generateAnthropicMsgID()
	if emitAnthropicEvent(&apicompat.AnthropicStreamEvent{
		Type: "message_start",
		Message: &apicompat.AnthropicResponse{
			ID:         messageID,
			Type:       "message",
			Role:       "assistant",
			Model:      originalModel,
			Content:    []apicompat.AnthropicContentBlock{},
			StopReason: nil, // JSON null
			Usage:      apicompat.AnthropicUsage{},
		},
	}) {
		return streamResult(), nil
	}

	sawToolUse := false
	nextBlockIndex := 0
	openBlockIndex := -1
	openBlockType := ""
	seenText := ""
	seenThought := ""
	openToolIndex := -1
	seenToolJSON := ""

	closeOpenBlock := func() bool {
		if openBlockIndex < 0 {
			return false
		}
		disconnected := emitAnthropicEvent(&apicompat.AnthropicStreamEvent{Type: "content_block_stop"})
		openBlockIndex = -1
		openBlockType = ""
		return disconnected
	}
	closeOpenTool := func() bool {
		if openToolIndex < 0 {
			return false
		}
		disconnected := emitAnthropicEvent(&apicompat.AnthropicStreamEvent{Type: "content_block_stop"})
		openToolIndex = -1
		seenToolJSON = ""
		return disconnected
	}

	stream := newAnthropicCompatStream(s.cfg, c, resp, "gemini-openai", "")
	defer stream.stop()
	for {
		line, ok := stream.nextLine()
		if !ok {
			break
		}
		if len(line) > 0 {
			trimmed := strings.TrimRight(line, "\r\n")
			if strings.HasPrefix(trimmed, "data:") {
				payload := strings.TrimSpace(strings.TrimPrefix(trimmed, "data:"))
				if payload == "[DONE]" {
					doneSeen = true
				}
				if payload != "" && payload != "[DONE]" {
					if doneSeen {
						upstreamFailure = &openAIClientErrorFields{Type: "upstream_error", Code: "invalid_upstream_terminal", Message: "Upstream sent data after its stream terminator", StatusCode: 502}
					}
					rawBytes := []byte(payload)
					if isOAuth {
						if innerBytes, uwErr := unwrapGeminiResponse(rawBytes); uwErr == nil {
							rawBytes = innerBytes
						}
					}

					var geminiResp map[string]any
					if err := decodeGeminiChatObject(rawBytes, &geminiResp); err != nil {
						upstreamFailure = &openAIClientErrorFields{Type: "upstream_error", Code: "malformed_upstream_stream", Message: "Upstream sent malformed stream data", StatusCode: 502}
					} else {
						nativeMeter.observe(rawBytes)
						usage = nativeMeter.usage
						if fields := geminiOpenAIErrorFields(rawBytes); fields != nil {
							upstreamFailure = fields
							continue
						}
						if finishReason != "" && len(extractGeminiParts(geminiResp)) > 0 {
							upstreamFailure = &openAIClientErrorFields{Type: "upstream_error", Code: "invalid_upstream_terminal", Message: "Upstream sent content after its terminal response", StatusCode: 502}
							continue
						}
						if firstChunk {
							firstChunk = false
							ms := int(time.Since(startTime).Milliseconds())
							firstTokenMs = &ms
						}
						if fr := extractGeminiFinishReason(geminiResp); fr != "" {
							finishReason = fr
						}
						if finishReason != "" && gjson.GetBytes(rawBytes, "usageMetadata").Exists() && nativeMeter.receipt != nil {
							finalMeterSeen = true
						}

						for _, part := range extractGeminiParts(geminiResp) {
							if data, ok := part["inlineData"].(map[string]any); ok {
								mime, _ := data["mimeType"].(string)
								payload, _ := data["data"].(string)
								if isGeminiInlineImageMIMEType(mime) && isValidBase64(payload) {
									part["text"] = fmt.Sprintf("![image](data:%s;base64,%s)", mime, payload)
								}
							}
							if text, ok := part["text"].(string); ok && isGeminiThoughtPart(part) {
								delta, newSeen := computeGeminiTextDelta(seenThought, text)
								seenThought = newSeen
								if delta == "" {
									continue
								}
								if openToolIndex >= 0 {
									if closeOpenTool() {
										return streamResult(), nil
									}
								}
								if openBlockType != "thinking" {
									if closeOpenBlock() {
										return streamResult(), nil
									}
									idx := nextBlockIndex
									nextBlockIndex++
									openBlockIndex = idx
									openBlockType = "thinking"
									if emitAnthropicEvent(&apicompat.AnthropicStreamEvent{
										Type:         "content_block_start",
										Index:        &idx,
										ContentBlock: &apicompat.AnthropicContentBlock{Type: "thinking"},
									}) {
										return streamResult(), nil
									}
								}
								if emitAnthropicEvent(&apicompat.AnthropicStreamEvent{
									Type:  "content_block_delta",
									Delta: &apicompat.AnthropicDelta{Type: "thinking_delta", Thinking: delta},
								}) {
									return streamResult(), nil
								}
								continue
							}
							if text, ok := part["text"].(string); ok && text != "" {
								if openToolIndex >= 0 {
									if closeOpenTool() {
										return streamResult(), nil
									}
								}
								delta, newSeen := computeGeminiTextDelta(seenText, text)
								seenText = newSeen
								if delta == "" {
									continue
								}
								if openBlockType != "text" {
									if closeOpenBlock() {
										return streamResult(), nil
									}
									idx := nextBlockIndex
									nextBlockIndex++
									openBlockIndex = idx
									openBlockType = "text"
									if emitAnthropicEvent(&apicompat.AnthropicStreamEvent{
										Type:  "content_block_start",
										Index: &idx,
										ContentBlock: &apicompat.AnthropicContentBlock{
											Type: "text",
											Text: "",
										},
									}) {
										return streamResult(), nil
									}
								}
								if emitAnthropicEvent(&apicompat.AnthropicStreamEvent{
									Type: "content_block_delta",
									Delta: &apicompat.AnthropicDelta{
										Type: "text_delta",
										Text: delta,
									},
								}) {
									return streamResult(), nil
								}
								continue
							}

							if fc, ok := part["functionCall"].(map[string]any); ok && fc != nil {
								name, _ := fc["name"].(string)
								if fields := validateGeminiOpenAIFunction(fc); fields != nil {
									upstreamFailure = fields
									continue
								}
								if closeOpenBlock() {
									return streamResult(), nil
								}
								if openToolIndex >= 0 {
									if closeOpenTool() {
										return streamResult(), nil
									}
								}
								if openToolIndex < 0 {
									idx := nextBlockIndex
									nextBlockIndex++
									openToolIndex = idx
									sawToolUse = true
									callID := "toolu_" + randomHex(8)
									if signature, _ := part["thoughtSignature"].(string); signature != "" {
										toolSignatures[callID] = signature
									}
									if emitAnthropicEvent(&apicompat.AnthropicStreamEvent{
										Type:  "content_block_start",
										Index: &idx,
										ContentBlock: &apicompat.AnthropicContentBlock{
											Type:  "tool_use",
											ID:    callID,
											Name:  name,
											Input: json.RawMessage(`{}`),
										},
									}) {
										return streamResult(), nil
									}
								}

								argsJSONText := "{}"
								switch v := fc["args"].(type) {
								case nil:
								case string:
									if strings.TrimSpace(v) != "" {
										argsJSONText = v
									}
								default:
									if b, err := json.Marshal(v); err == nil && len(b) > 0 {
										argsJSONText = string(b)
									}
								}
								delta, newSeen := computeGeminiTextDelta(seenToolJSON, argsJSONText)
								seenToolJSON = newSeen
								if delta != "" {
									if emitAnthropicEvent(&apicompat.AnthropicStreamEvent{
										Type: "content_block_delta",
										Delta: &apicompat.AnthropicDelta{
											Type:        "input_json_delta",
											PartialJSON: delta,
										},
									}) {
										return streamResult(), nil
									}
								}
							}
						}
					}
				}
			}
		}

	}
	if stream.readErr != nil && !errors.Is(stream.readErr, io.EOF) {
		return streamResult(), fail("upstream_stream_error", "Upstream stream ended before a complete response", stream.readErr)
	}

	if upstreamFailure != nil {
		return streamResult(), fail(upstreamFailure.Code, upstreamFailure.Message, nil)
	}
	if finishReason != "STOP" && finishReason != "MAX_TOKENS" {
		return streamResult(), fail("invalid_upstream_terminal", "Upstream did not return a supported successful terminal state", nil)
	}
	if !finalMeterSeen || nativeMeter.receipt == nil || nativeMeter.invalid {
		return streamResult(), fail("upstream_incomplete_response", "Upstream stream did not contain a valid terminal response and usage", nil)
	}
	usage.receiptMeasured = nativeMeter.receipt
	if seenText == "" && !sawToolUse {
		return streamResult(), fail("upstream_empty_completion", "Upstream returned no response content; reported usage is retained", nil)
	}

	if closeOpenBlock() {
		return streamResult(), nil
	}
	if closeOpenTool() {
		return streamResult(), nil
	}

	stopReason := mapGeminiFinishReasonToClaudeStopReason(finishReason)
	if sawToolUse && finishReason == "STOP" {
		stopReason = "tool_use"
	}
	anthState.InputTokens = usage.InputTokens
	anthState.CacheReadInputTokens = usage.CacheReadInputTokens
	if emitAnthropicEvent(&apicompat.AnthropicStreamEvent{
		Type: "message_delta",
		Delta: &apicompat.AnthropicDelta{
			Type:       "message_delta",
			StopReason: stopReason,
		},
		Usage: &apicompat.AnthropicUsage{
			InputTokens:          usage.InputTokens,
			OutputTokens:         usage.OutputTokens,
			CacheReadInputTokens: usage.CacheReadInputTokens,
		},
	}) {
		return streamResult(), nil
	}
	if emitAnthropicEvent(&apicompat.AnthropicStreamEvent{Type: "message_stop"}) {
		return streamResult(), nil
	}

	for _, resEvt := range apicompat.FinalizeAnthropicResponsesStream(anthState) {
		if output != nil {
			if writeResponsesEvent(resEvt) {
				return streamResult(), errors.New("cannot encode Responses event")
			}
			continue
		}
		chunks := apicompat.ResponsesEventToChatChunks(&resEvt, ccState)
		for _, chunk := range chunks {
			if disconnected := writeChatChunk(chunk); disconnected {
				return streamResult(), nil
			}
		}
	}
	if output != nil {
		if !clientDisconnect {
			flusher.Flush()
		}
		return streamResult(), nil
	}
	for _, chunk := range apicompat.FinalizeResponsesChatStream(ccState) {
		if disconnected := writeChatChunk(chunk); disconnected {
			return streamResult(), nil
		}
	}

	if !clientDisconnect {
		if _, err := io.WriteString(c.Writer, "data: [DONE]\n\n"); err != nil {
			markDisconnected()
		} else {
			flusher.Flush()
		}
	}

	return streamResult(), nil
}

func (s *GeminiMessagesCompatService) writeGeminiChatCompletionsMappedError(
	c *gin.Context,
	account *Account,
	upstreamStatus int,
	upstreamRequestID string,
	body []byte,
) error {
	upstreamMsg := sanitizeUpstreamErrorMessage(strings.TrimSpace(extractUpstreamErrorMessage(body)))
	setOpsUpstreamError(c, upstreamStatus, upstreamMsg, "")
	if account != nil {
		appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
			ProxyID:            opsUpstreamProxyID(account),
			ProxyName:          opsUpstreamProxyName(account),
			Platform:           account.Platform,
			AccountID:          account.ID,
			AccountName:        account.Name,
			UpstreamStatusCode: upstreamStatus,
			UpstreamRequestID:  upstreamRequestID,
			Kind:               "http_error",
			Message:            upstreamMsg,
		})
	}

	if status, errType, errMsg, matched := applyErrorPassthroughRule(
		c,
		PlatformGemini,
		upstreamStatus,
		body,
		http.StatusBadGateway,
		"upstream_error",
		"Upstream request failed",
	); matched {
		return s.writeChatCompletionsError(c, status, errType, errMsg)
	}

	statusCode := http.StatusBadGateway
	errType := "upstream_error"
	errMsg := "Upstream request failed"
	if mapped := mapGeminiErrorBodyToClaudeError(body); mapped != nil {
		if mapped.Type != "" {
			errType = mapped.Type
		}
		if mapped.Message != "" {
			errMsg = mapped.Message
		}
		if mapped.StatusCode > 0 {
			statusCode = mapped.StatusCode
		}
	}

	switch upstreamStatus {
	case http.StatusPaymentRequired:
		statusCode, errType, errMsg = http.StatusPaymentRequired, "billing_error", "Upstream payment required; please contact the administrator"
	case http.StatusBadRequest:
		if statusCode == http.StatusBadGateway {
			statusCode = http.StatusBadRequest
		}
		if errType == "upstream_error" {
			errType = "invalid_request_error"
		}
		// 400 是确定性的请求错误：回传上游 message（已脱敏），客户端据此定位非法字段。
		if errMsg == "Upstream request failed" {
			if upstreamMsg != "" {
				errMsg = upstreamMsg
			} else {
				errMsg = "Invalid request"
			}
		}
	case http.StatusNotFound:
		statusCode = http.StatusNotFound
		if errType == "upstream_error" {
			errType = "not_found_error"
		}
		if errMsg == "Upstream request failed" {
			errMsg = "Resource not found"
		}
	case http.StatusTooManyRequests:
		statusCode = http.StatusTooManyRequests
		if errType == "upstream_error" {
			errType = "rate_limit_error"
		}
		if errMsg == "Upstream request failed" {
			errMsg = "Upstream rate limit exceeded, please retry later"
		}
	case 529:
		statusCode = http.StatusServiceUnavailable
		if errType == "upstream_error" {
			errType = "overloaded_error"
		}
		if errMsg == "Upstream request failed" {
			errMsg = "Upstream service overloaded, please retry later"
		}
	}

	if upstreamMsg != "" && errMsg == "Upstream request failed" {
		errMsg = upstreamMsg
	}
	return s.writeChatCompletionsError(c, statusCode, errType, errMsg)
}

func (s *GeminiMessagesCompatService) writeChatCompletionsError(c *gin.Context, status int, errType, message string) error {
	writeOpenAIErrorFields(c, openAIClientErrorFields{StatusCode: status, Type: errType, Message: message}, false)
	return errors.New(message)
}

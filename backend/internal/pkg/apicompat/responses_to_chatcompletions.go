package apicompat

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Non-streaming: ResponsesResponse → ChatCompletionsResponse
// ---------------------------------------------------------------------------

// ResponsesToChatCompletions converts a Responses API response into a Chat
// Completions response. Text output items are concatenated into
// choices[0].message.content, refusal parts into choices[0].message.refusal;
// function_call items become tool_calls.
func ResponsesToChatCompletions(resp *ResponsesResponse, model string) *ChatCompletionsResponse {
	id := resp.ID
	if id == "" {
		id = generateChatCmplID()
	}

	out := &ChatCompletionsResponse{
		ID:          id,
		Object:      "chat.completion",
		Created:     time.Now().Unix(),
		Model:       model,
		ServiceTier: resp.ServiceTier,
	}

	var contentText string
	var refusalText string
	var reasoningText string
	var toolCalls []ChatToolCall

	for _, item := range resp.Output {
		switch item.Type {
		case "message":
			for _, part := range item.Content {
				switch {
				case part.Type == "output_text" && part.Text != "":
					contentText += part.Text
				case part.Type == "refusal" && part.Refusal != "":
					refusalText += part.Refusal
				}
			}
		case "function_call":
			toolCalls = append(toolCalls, ChatToolCall{
				ID:   item.CallID,
				Type: "function",
				Function: ChatFunctionCall{
					Name:      item.Name,
					Arguments: item.Arguments,
				},
			})
		case "reasoning":
			for _, s := range item.Summary {
				if s.Type == "summary_text" && s.Text != "" {
					reasoningText += s.Text
				}
			}
		case "web_search_call":
			// silently consumed — results already incorporated into text output
		}
	}

	msg := ChatMessage{Role: "assistant", Refusal: refusalText}
	if len(toolCalls) > 0 {
		msg.ToolCalls = toolCalls
	}
	if contentText != "" {
		raw, _ := json.Marshal(contentText)
		msg.Content = raw
	}
	if reasoningText != "" {
		msg.ReasoningContent = reasoningText
	}

	finishReason := responsesStatusToChatFinishReason(resp.Status, resp.IncompleteDetails, toolCalls)

	out.Choices = []ChatChoice{{
		Index:        0,
		Message:      msg,
		FinishReason: finishReason,
	}}

	out.Usage = chatUsageFromResponsesUsage(resp.Usage)
	if !chatUsageHasTokens(out.Usage) && (resp.ambiguousUsage || (resp.Status != "completed" && resp.Status != "incomplete" && resp.Status != "failed")) {
		out.Usage = nil
	}

	return out
}

func responsesStatusToChatFinishReason(status string, details *ResponsesIncompleteDetails, toolCalls []ChatToolCall) string {
	switch status {
	case "incomplete":
		if details != nil {
			switch details.Reason {
			case "max_output_tokens":
				return "length"
			case "content_filter":
				return "content_filter"
			}
		}
		return "stop"
	case "completed":
		if len(toolCalls) > 0 {
			return "tool_calls"
		}
		return "stop"
	default:
		return "stop"
	}
}

// ---------------------------------------------------------------------------
// Streaming: ResponsesStreamEvent → []ChatCompletionsChunk (stateful converter)
// ---------------------------------------------------------------------------

// ResponsesEventToChatState tracks state for converting a sequence of Responses
// SSE events into Chat Completions SSE chunks.
type ResponsesEventToChatState struct {
	ID                     string
	Model                  string
	Created                int64
	ServiceTier            string // upstream tier observed on response events; echoed on chunks
	SentRole               bool
	SawToolCall            bool
	SawText                bool
	Finalized              bool        // true after finish chunk has been emitted
	NextToolCallIndex      int         // next sequential tool_call index to assign
	OutputIndexToToolIndex map[int]int // Responses output_index → Chat tool_calls index
	OutputIndexToArguments map[int]string
	IncludeUsage           bool
	Usage                  *ChatUsage
}

// NewResponsesEventToChatState returns an initialised stream state.
func NewResponsesEventToChatState() *ResponsesEventToChatState {
	return &ResponsesEventToChatState{
		ID:                     generateChatCmplID(),
		Created:                time.Now().Unix(),
		OutputIndexToToolIndex: make(map[int]int),
		OutputIndexToArguments: make(map[int]string),
	}
}

// ResponsesEventToChatChunks converts a single Responses SSE event into zero
// or more Chat Completions chunks, updating state as it goes.
func ResponsesEventToChatChunks(evt *ResponsesStreamEvent, state *ResponsesEventToChatState) []ChatCompletionsChunk {
	// Progressive positive counters remain evidence even when the terminal receipt is empty.
	if u := chatUsageFromResponsesUsage(evt.Usage); chatUsageHasTokens(u) {
		state.Usage = retainChatUsage(state.Usage, u)
	}
	if evt.Response != nil {
		if u := chatUsageFromResponsesUsage(evt.Response.Usage); chatUsageHasTokens(u) {
			state.Usage = retainChatUsage(state.Usage, u)
		}
	}

	switch evt.Type {
	case "response.created":
		return resToChatHandleCreated(evt, state)
	case "response.output_text.delta":
		return resToChatHandleTextDelta(evt, state)
	case "response.refusal.delta":
		return resToChatHandleRefusalDelta(evt, state)
	case "response.output_item.added":
		return resToChatHandleOutputItemAdded(evt, state)
	case "response.function_call_arguments.delta",
		// custom/freeform 工具（如新版 apply_patch）的输入增量与 function_call 参数增量同形，
		// 均按 OutputIndex 累加到对应工具调用。
		"response.custom_tool_call_input.delta":
		return resToChatHandleFuncArgsDelta(evt, state)
	case "response.function_call_arguments.done", "response.custom_tool_call_input.done":
		return resToChatHandleFuncArgsDone(evt, state)
	case "response.reasoning_summary_text.delta",
		// 原始推理文本增量（真实 Codex 客户端消费的 reasoning_text.delta），
		// 与 reasoning summary 一样映射为 reasoning_content。
		"response.reasoning_text.delta":
		return resToChatHandleReasoningDelta(evt, state)
	case "response.reasoning_summary_text.done":
		return nil
	// response.done 是 Realtime/WS 与项目透传路径使用的终止别名；
	// 普通 Responses HTTP SSE 的公开终止事件仍以 response.completed 为主。
	case "response.completed", "response.done", "response.incomplete", "response.failed":
		return resToChatHandleCompleted(evt, state)
	default:
		return nil
	}
}

// FinalizeResponsesChatStream emits a final chunk with finish_reason if the
// stream ended without a proper completion event (e.g. upstream disconnect).
// It is idempotent: if a completion event already emitted the finish chunk,
// this returns nil.
func FinalizeResponsesChatStream(state *ResponsesEventToChatState) []ChatCompletionsChunk {
	if state.Finalized {
		return nil
	}
	state.Finalized = true

	finishReason := "stop"
	if state.SawToolCall {
		finishReason = "tool_calls"
	}

	chunks := []ChatCompletionsChunk{makeChatFinishChunk(state, finishReason)}

	if state.IncludeUsage && state.Usage != nil {
		chunks = append(chunks, ChatCompletionsChunk{
			ID:          state.ID,
			Object:      "chat.completion.chunk",
			Created:     state.Created,
			Model:       state.Model,
			ServiceTier: state.ServiceTier,
			Choices:     []ChatChunkChoice{},
			Usage:       state.Usage,
		})
	}

	return chunks
}

// ChatChunkToSSE formats a ChatCompletionsChunk as an SSE data line.
func ChatChunkToSSE(chunk ChatCompletionsChunk) (string, error) {
	data, err := json.Marshal(chunk)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("data: %s\n\n", data), nil
}

// --- internal handlers ---

func resToChatHandleCreated(evt *ResponsesStreamEvent, state *ResponsesEventToChatState) []ChatCompletionsChunk {
	if evt.Response != nil {
		if evt.Response.ID != "" {
			state.ID = evt.Response.ID
		}
		if state.Model == "" && evt.Response.Model != "" {
			state.Model = evt.Response.Model
		}
		if evt.Response.ServiceTier != "" {
			state.ServiceTier = evt.Response.ServiceTier
		}
	}
	// Emit the role chunk.
	if state.SentRole {
		return nil
	}
	state.SentRole = true

	role := "assistant"
	return []ChatCompletionsChunk{makeChatDeltaChunk(state, ChatDelta{Role: role})}
}

func resToChatHandleTextDelta(evt *ResponsesStreamEvent, state *ResponsesEventToChatState) []ChatCompletionsChunk {
	if evt.Delta == "" {
		return nil
	}
	state.SawText = true
	content := evt.Delta
	return []ChatCompletionsChunk{makeChatDeltaChunk(state, ChatDelta{Content: &content})}
}

func resToChatHandleRefusalDelta(evt *ResponsesStreamEvent, state *ResponsesEventToChatState) []ChatCompletionsChunk {
	if evt.Delta == "" {
		return nil
	}
	refusal := evt.Delta
	return []ChatCompletionsChunk{makeChatDeltaChunk(state, ChatDelta{Refusal: &refusal})}
}

func resToChatHandleOutputItemAdded(evt *ResponsesStreamEvent, state *ResponsesEventToChatState) []ChatCompletionsChunk {
	// function_call 与 custom_tool_call（custom/freeform 工具）均按工具调用注册，
	// 以便后续 *_input.delta / *_arguments.delta 能映射到正确的工具索引。
	if evt.Item == nil || (evt.Item.Type != "function_call" && evt.Item.Type != "custom_tool_call") {
		return nil
	}

	state.SawToolCall = true
	idx := state.NextToolCallIndex
	state.OutputIndexToToolIndex[evt.OutputIndex] = idx
	state.NextToolCallIndex++

	return []ChatCompletionsChunk{makeChatDeltaChunk(state, ChatDelta{
		ToolCalls: []ChatToolCall{{
			Index: &idx,
			ID:    evt.Item.CallID,
			Type:  "function",
			Function: ChatFunctionCall{
				Name: evt.Item.Name,
			},
		}},
	})}
}

func resToChatHandleFuncArgsDelta(evt *ResponsesStreamEvent, state *ResponsesEventToChatState) []ChatCompletionsChunk {
	if evt.Delta == "" {
		return nil
	}

	idx, ok := state.OutputIndexToToolIndex[evt.OutputIndex]
	if !ok {
		return nil
	}
	state.OutputIndexToArguments[evt.OutputIndex] += evt.Delta

	return []ChatCompletionsChunk{makeChatDeltaChunk(state, ChatDelta{
		ToolCalls: []ChatToolCall{{
			Index: &idx,
			Function: ChatFunctionCall{
				Arguments: evt.Delta,
			},
		}},
	})}
}

func resToChatHandleFuncArgsDone(evt *ResponsesStreamEvent, state *ResponsesEventToChatState) []ChatCompletionsChunk {
	idx, ok := state.OutputIndexToToolIndex[evt.OutputIndex]
	if !ok {
		return nil
	}

	completed := evt.Arguments
	if evt.Type == "response.custom_tool_call_input.done" {
		completed = evt.Input
	}
	current := state.OutputIndexToArguments[evt.OutputIndex]
	if completed == "" || !strings.HasPrefix(completed, current) || completed == current {
		return nil
	}

	remainder := completed[len(current):]
	state.OutputIndexToArguments[evt.OutputIndex] = completed
	return []ChatCompletionsChunk{makeChatDeltaChunk(state, ChatDelta{
		ToolCalls: []ChatToolCall{{
			Index: &idx,
			Function: ChatFunctionCall{
				Arguments: remainder,
			},
		}},
	})}
}

func resToChatHandleReasoningDelta(evt *ResponsesStreamEvent, state *ResponsesEventToChatState) []ChatCompletionsChunk {
	if evt.Delta == "" {
		return nil
	}
	reasoning := evt.Delta
	return []ChatCompletionsChunk{makeChatDeltaChunk(state, ChatDelta{ReasoningContent: &reasoning})}
}

func resToChatHandleCompleted(evt *ResponsesStreamEvent, state *ResponsesEventToChatState) []ChatCompletionsChunk {
	state.Finalized = true
	finishReason := "stop"

	if evt.Usage != nil {
		state.Usage = retainTerminalResponsesUsage(state.Usage, evt.Usage, !evt.ambiguousUsage)
	}
	if evt.Response != nil {
		if evt.Response.Usage != nil {
			state.Usage = retainTerminalResponsesUsage(state.Usage, evt.Response.Usage, !evt.ambiguousUsage && !evt.Response.ambiguousUsage)
		}
		if evt.Response.ServiceTier != "" {
			state.ServiceTier = evt.Response.ServiceTier
		}

		switch evt.Response.Status {
		case "incomplete":
			if evt.Response.IncompleteDetails != nil {
				switch evt.Response.IncompleteDetails.Reason {
				case "max_output_tokens":
					finishReason = "length"
				case "content_filter":
					finishReason = "content_filter"
				}
			}
		case "completed":
			if state.SawToolCall {
				finishReason = "tool_calls"
			}
		}
	} else if state.SawToolCall {
		finishReason = "tool_calls"
	}

	if !chatUsageHasTokens(state.Usage) && evt.Usage != nil && evt.Response != nil && evt.Response.Usage != nil && (evt.Usage.MeteringState() == "incomplete" || evt.Response.Usage.MeteringState() == "incomplete") {
		state.Usage = nil
	}

	if !chatUsageHasTokens(state.Usage) && (evt.ambiguousUsage || (evt.Response != nil && evt.Response.ambiguousUsage)) {
		state.Usage = nil
	}
	var chunks []ChatCompletionsChunk
	chunks = append(chunks, makeChatFinishChunk(state, finishReason))

	if state.IncludeUsage && state.Usage != nil {
		chunks = append(chunks, ChatCompletionsChunk{
			ID:          state.ID,
			Object:      "chat.completion.chunk",
			Created:     state.Created,
			Model:       state.Model,
			ServiceTier: state.ServiceTier,
			Choices:     []ChatChunkChoice{},
			Usage:       state.Usage,
		})
	}

	return chunks
}

// Complete terminal receipts may correct cache classification without erasing
// already observed inclusive input or output consumption.
func retainTerminalResponsesUsage(previous *ChatUsage, raw *ResponsesUsage, unambiguous bool) *ChatUsage {
	retained := retainChatUsage(previous, chatUsageFromResponsesUsage(raw))
	readZero, writeZero := raw.TerminalCacheCorrections()
	if retained == nil || !unambiguous || (!readZero && !writeZero) {
		return retained
	}
	if retained.PromptTokensDetails != nil {
		details := *retained.PromptTokensDetails
		if readZero {
			details.CachedTokens = 0
		}
		if writeZero {
			details.CacheCreationTokens = 0
			details.CacheWriteTokens = 0
		}
		retained.PromptTokensDetails = &details
	}
	return retained
}

// TerminalCacheCorrections identifies explicit cache reclassification in a
// complete positive-input receipt. Callers must separately require a terminal event.
func (u *ResponsesUsage) TerminalCacheCorrections() (readZero, writeZero bool) {
	if u == nil || !u.explicitTokenTotals || u.InputTokens <= 0 {
		return false, false
	}
	return u.cacheReadExplicitZero, u.cacheWriteExplicitZero
}

func retainChatUsage(previous, next *ChatUsage) *ChatUsage {
	if chatUsageHasTokens(previous) && !chatUsageHasTokens(next) {
		return previous
	}
	if next == nil {
		return nil
	}
	if previous == nil {
		return next
	}
	retained := *next
	if retained.PromptTokens == 0 {
		retained.PromptTokens = previous.PromptTokens
	}
	if retained.CompletionTokens == 0 {
		retained.CompletionTokens = previous.CompletionTokens
	}
	retained.PromptTokensDetails = retainChatTokenDetails(previous.PromptTokensDetails, retained.PromptTokensDetails)
	retained.CompletionTokensDetails = retainChatTokenDetails(previous.CompletionTokensDetails, retained.CompletionTokensDetails)
	retained.TotalTokens = chatTokenTotal(retained.PromptTokens, retained.CompletionTokens)
	return &retained
}

func retainChatTokenDetails(previous, next *ChatTokenDetails) *ChatTokenDetails {
	if previous == nil {
		return next
	}
	retained := *previous
	if next != nil {
		// These are cumulative raw counters, not already-priced billing buckets.
		if next.CachedTokens != 0 {
			retained.CachedTokens = next.CachedTokens
		}
		if next.CacheCreationTokens != 0 || next.CacheWriteTokens != 0 {
			// These names are aliases for one bucket, not independent consumption.
			retained.CacheCreationTokens = next.CacheCreationTokens
			retained.CacheWriteTokens = next.CacheWriteTokens
		}
		if next.ReasoningTokens != 0 {
			retained.ReasoningTokens = next.ReasoningTokens
		}
		if next.AudioTokens != 0 {
			retained.AudioTokens = next.AudioTokens
		}
		if next.AcceptedPredictionTokens != 0 {
			retained.AcceptedPredictionTokens = next.AcceptedPredictionTokens
		}
		if next.RejectedPredictionTokens != 0 {
			retained.RejectedPredictionTokens = next.RejectedPredictionTokens
		}
	}
	return &retained
}

func chatUsageHasTokens(u *ChatUsage) bool {
	if u == nil {
		return false
	}
	if u.PromptTokens > 0 || u.CompletionTokens > 0 {
		return true
	}
	return u.PromptTokensDetails != nil && (u.PromptTokensDetails.CachedTokens > 0 || u.PromptTokensDetails.CacheWriteTokens > 0 || u.PromptTokensDetails.CacheCreationTokens > 0)
}

func chatUsageFromResponsesUsage(u *ResponsesUsage) *ChatUsage {
	if u == nil {
		return nil
	}
	usage := &ChatUsage{
		PromptTokens:     u.InputTokens,
		CompletionTokens: u.OutputTokens,
		TotalTokens:      chatTokenTotal(u.InputTokens, u.OutputTokens),
	}
	usage.PromptTokensDetails = promptDetailsFromResponses(u.InputTokensDetails)
	if u.CacheCreationInputTokens > 0 {
		if usage.PromptTokensDetails == nil {
			usage.PromptTokensDetails = &ChatTokenDetails{}
		}
		if usage.PromptTokensDetails.CacheWriteTokens == 0 && usage.PromptTokensDetails.CacheCreationTokens == 0 {
			usage.PromptTokensDetails.CacheCreationTokens = u.CacheCreationInputTokens
		}
	}
	usage.CompletionTokensDetails = completionDetailsFromResponses(u.OutputTokensDetails)
	if !chatUsageHasTokens(usage) && !u.explicitZeroTokenTotals() {
		return nil
	}
	return usage
}

// A decoder's zero defaults must not become a downstream zero-cost receipt.
func (u *ResponsesUsage) explicitZeroTokenTotals() bool {
	return u.explicitTokenTotals && u.zeroTokenCountersValid && u.InputTokens == 0 && u.OutputTokens == 0 && u.TotalTokens == 0 && u.CacheCreationInputTokens == 0 &&
		(u.InputTokensDetails == nil || *u.InputTokensDetails == (ResponsesInputTokensDetails{})) &&
		(u.OutputTokensDetails == nil || *u.OutputTokensDetails == (ResponsesOutputTokensDetails{}))
}

func responsesZeroCountersValid(data []byte) bool {
	return responsesZeroCountersValidObject(data, false, true)
}

// Decode fields in wire order: map decoding would hide duplicate counters.
func responsesZeroCountersValidObject(data []byte, details, requireZero bool) bool {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return false
	}
	seen := make(map[string]bool)
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return false
		}
		name, ok := token.(string)
		if !ok {
			return false
		}
		name = strings.ToLower(name)
		if seen[name] {
			return false
		}
		seen[name] = true
		var raw json.RawMessage
		if decoder.Decode(&raw) != nil {
			return false
		}
		if strings.HasSuffix(name, "_tokens_details") {
			if !responsesZeroCountersValidObject(raw, true, requireZero) {
				return false
			}
		} else if details || strings.HasSuffix(name, "_tokens") {
			var count *int
			if json.Unmarshal(raw, &count) != nil || count == nil || *count < 0 || (requireZero && *count != 0) {
				return false
			}
		}
	}
	_, err = decoder.Token()
	return err == nil
}

// Validate only the cache bucket when accepting a complete terminal correction.
func responsesExplicitCacheZero(data []byte, write bool) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil {
		return false
	}
	selected := make(map[string]json.RawMessage)
	for name, raw := range fields {
		lower := strings.ToLower(name)
		if strings.HasSuffix(lower, "_tokens_details") && (strings.HasPrefix(lower, "input_") || strings.HasPrefix(lower, "prompt_")) {
			var nested map[string]json.RawMessage
			if json.Unmarshal(raw, &nested) != nil {
				return false
			}
			for key, value := range nested {
				if (!write && strings.EqualFold(key, "cached_tokens")) || (write && (strings.EqualFold(key, "cache_write_tokens") || strings.EqualFold(key, "cache_creation_tokens"))) {
					selected[lower+"."+strings.ToLower(key)] = value
				}
			}
		} else if responsesCacheField(lower, write) {
			selected[lower] = raw
		}
	}
	if len(selected) == 0 {
		return false
	}
	// Reject duplicate or invalid fields in this usage object before trusting a correction.
	if !responsesZeroCountersValidObject(data, false, false) {
		return false
	}
	for _, raw := range selected {
		var n *int
		if json.Unmarshal(raw, &n) != nil || n == nil || *n != 0 {
			return false
		}
	}
	return true
}
func responsesCacheField(name string, write bool) bool {
	name = strings.ToLower(name)
	if write {
		return name == "cache_write_tokens" || name == "cache_creation_tokens" || name == "cache_write_input_tokens" || name == "cache_creation_input_tokens"
	}
	return name == "cached_tokens" || name == "cache_read_tokens" || name == "cache_read_input_tokens" || name == "prompt_cache_hit_tokens"
}
func responsesUniqueUsageFields(data []byte) bool {
	return responsesZeroCountersValidObject(data, false, false)
}

// TerminalMeteringState includes both possible locations in one receipt.
func (evt *ResponsesStreamEvent) TerminalMeteringState() string {
	if evt == nil {
		return "missing"
	}
	outer := evt.Usage.MeteringState()
	inner := "missing"
	if evt.Response != nil {
		inner = evt.Response.Usage.MeteringState()
	}
	if outer == "positive" || inner == "positive" {
		return "positive"
	}
	if outer == "incomplete" || inner == "incomplete" {
		return "incomplete"
	}
	if evt.ambiguousUsage || (evt.Response != nil && evt.Response.ambiguousUsage) {
		return "incomplete"
	}
	if outer == "explicit_zero" || inner == "explicit_zero" {
		return "explicit_zero"
	}
	return "missing"
}

// MeteringState describes only counters and their presence, never response content.
func (u *ResponsesUsage) MeteringState() string {
	if u == nil {
		return "missing"
	}
	converted := chatUsageFromResponsesUsage(u)
	if converted == nil {
		return "incomplete"
	}
	if chatUsageHasTokens(converted) {
		return "positive"
	}
	return "explicit_zero"
}

// promptDetailsFromResponses maps Responses-API input_tokens_details into a
// Chat-Completions prompt_tokens_details. Returns nil when nothing would be
// emitted, so upstreams that do not break down prompt usage stay clean.
func promptDetailsFromResponses(src *ResponsesInputTokensDetails) *ChatTokenDetails {
	if src == nil {
		return nil
	}
	if src.CachedTokens == 0 && src.AudioTokens == 0 && src.CacheCreationTokens == 0 && src.CacheWriteTokens == 0 {
		return nil
	}
	return &ChatTokenDetails{
		CachedTokens:        src.CachedTokens,
		AudioTokens:         src.AudioTokens,
		CacheCreationTokens: src.CacheCreationTokens,
		CacheWriteTokens:    src.CacheWriteTokens,
	}
}

// completionDetailsFromResponses maps Responses-API output_tokens_details
// into a Chat-Completions completion_tokens_details. Mirrors the OpenAI
// official CompletionUsage schema: reasoning_tokens, audio_tokens, and
// the predicted-outputs accepted/rejected counts. Returns nil when nothing
// would be emitted so non-reasoning, non-audio responses stay clean.
func completionDetailsFromResponses(src *ResponsesOutputTokensDetails) *ChatTokenDetails {
	if src == nil {
		return nil
	}
	if src.ReasoningTokens == 0 && src.AudioTokens == 0 &&
		src.AcceptedPredictionTokens == 0 && src.RejectedPredictionTokens == 0 {
		return nil
	}
	return &ChatTokenDetails{
		ReasoningTokens:          src.ReasoningTokens,
		AudioTokens:              src.AudioTokens,
		AcceptedPredictionTokens: src.AcceptedPredictionTokens,
		RejectedPredictionTokens: src.RejectedPredictionTokens,
	}
}

func makeChatDeltaChunk(state *ResponsesEventToChatState, delta ChatDelta) ChatCompletionsChunk {
	return ChatCompletionsChunk{
		ID:          state.ID,
		Object:      "chat.completion.chunk",
		Created:     state.Created,
		Model:       state.Model,
		ServiceTier: state.ServiceTier,
		Choices: []ChatChunkChoice{{
			Index:        0,
			Delta:        delta,
			FinishReason: nil,
		}},
	}
}

func makeChatFinishChunk(state *ResponsesEventToChatState, finishReason string) ChatCompletionsChunk {
	empty := ""
	return ChatCompletionsChunk{
		ID:          state.ID,
		Object:      "chat.completion.chunk",
		Created:     state.Created,
		Model:       state.Model,
		ServiceTier: state.ServiceTier,
		Choices: []ChatChunkChoice{{
			Index:        0,
			Delta:        ChatDelta{Content: &empty},
			FinishReason: &finishReason,
		}},
	}
}

// generateChatCmplID returns a "chatcmpl-" prefixed random hex ID.
func generateChatCmplID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "chatcmpl-" + hex.EncodeToString(b)
}

// ---------------------------------------------------------------------------
// BufferedResponseAccumulator: accumulates SSE delta events for non-streaming
// paths where the terminal event may have empty output.
// ---------------------------------------------------------------------------

type bufferedFuncCall struct {
	OutputIndex int
	CallID      string
	Name        string
	Args        strings.Builder
}

// BufferedResponseAccumulator collects content from Responses SSE delta events
// so that non-streaming handlers can reconstruct output when the terminal event
// (response.completed / response.done) carries an empty output array.
type BufferedResponseAccumulator struct {
	text                 strings.Builder
	refusal              strings.Builder
	reasoning            strings.Builder
	funcCalls            []bufferedFuncCall
	outputIndexToFuncIdx map[int]int
}

// NewBufferedResponseAccumulator returns an initialised accumulator.
func NewBufferedResponseAccumulator() *BufferedResponseAccumulator {
	return &BufferedResponseAccumulator{
		outputIndexToFuncIdx: make(map[int]int),
	}
}

// ProcessEvent inspects a single Responses SSE event and accumulates any
// content it carries. Only delta events that contribute to the final output
// are handled; all other event types are silently ignored.
func (a *BufferedResponseAccumulator) ProcessEvent(event *ResponsesStreamEvent) {
	switch event.Type {
	case "response.output_text.delta":
		if event.Delta != "" {
			_, _ = a.text.WriteString(event.Delta)
		}
	case "response.refusal.delta":
		if event.Delta != "" {
			_, _ = a.refusal.WriteString(event.Delta)
		}
	case "response.output_item.added":
		if event.Item != nil && (event.Item.Type == "function_call" || event.Item.Type == "custom_tool_call") {
			idx := len(a.funcCalls)
			a.outputIndexToFuncIdx[event.OutputIndex] = idx
			a.funcCalls = append(a.funcCalls, bufferedFuncCall{
				OutputIndex: event.OutputIndex,
				CallID:      event.Item.CallID,
				Name:        event.Item.Name,
			})
		}
	case "response.function_call_arguments.delta", "response.custom_tool_call_input.delta":
		if event.Delta != "" {
			if idx, ok := a.outputIndexToFuncIdx[event.OutputIndex]; ok {
				_, _ = a.funcCalls[idx].Args.WriteString(event.Delta)
			}
		}
	case "response.function_call_arguments.done", "response.custom_tool_call_input.done":
		completed := event.Arguments
		if event.Type == "response.custom_tool_call_input.done" {
			completed = event.Input
		}
		if completed != "" {
			if idx, ok := a.outputIndexToFuncIdx[event.OutputIndex]; ok {
				a.funcCalls[idx].Args.Reset()
				_, _ = a.funcCalls[idx].Args.WriteString(completed)
			}
		}
	case "response.reasoning_summary_text.delta", "response.reasoning_text.delta":
		if event.Delta != "" {
			_, _ = a.reasoning.WriteString(event.Delta)
		}
	}
}

// HasContent reports whether any content has been accumulated.
func (a *BufferedResponseAccumulator) HasContent() bool {
	return a.text.Len() > 0 || a.refusal.Len() > 0 || len(a.funcCalls) > 0 || a.reasoning.Len() > 0
}

// BuildOutput constructs a []ResponsesOutput from the accumulated delta
// content. The order matches what ResponsesToChatCompletions expects:
// reasoning → message → function_calls.
func (a *BufferedResponseAccumulator) BuildOutput() []ResponsesOutput {
	var out []ResponsesOutput

	if a.reasoning.Len() > 0 {
		out = append(out, ResponsesOutput{
			Type: "reasoning",
			Summary: []ResponsesSummary{{
				Type: "summary_text",
				Text: a.reasoning.String(),
			}},
		})
	}

	if a.text.Len() > 0 || a.refusal.Len() > 0 {
		var content []ResponsesContentPart
		if a.text.Len() > 0 {
			content = append(content, ResponsesContentPart{Type: "output_text", Text: a.text.String()})
		}
		if a.refusal.Len() > 0 {
			content = append(content, ResponsesContentPart{Type: "refusal", Refusal: a.refusal.String()})
		}
		out = append(out, ResponsesOutput{
			Type:    "message",
			Role:    "assistant",
			Content: content,
		})
	}

	for i := range a.funcCalls {
		out = append(out, ResponsesOutput{
			Type:      "function_call",
			CallID:    a.funcCalls[i].CallID,
			Name:      a.funcCalls[i].Name,
			Arguments: a.funcCalls[i].Args.String(),
		})
	}

	return out
}

// SupplementResponseOutput fills resp.Output from accumulated stream content
// when the terminal event delivered an empty output array. It also fills empty
// function-call arguments from authoritative argument-done events.
func (a *BufferedResponseAccumulator) SupplementResponseOutput(resp *ResponsesResponse) {
	if resp == nil {
		return
	}
	if len(resp.Output) == 0 {
		if a.HasContent() {
			resp.Output = a.BuildOutput()
		}
		return
	}

	// The terminal event can carry a non-empty output array whose message has
	// no usable text. Trusting it as-is silently drops the text that already
	// streamed: the client gets an empty reply while usage still bills the
	// terminal output_tokens. Refill it from the accumulated deltas; non-empty
	// terminal text stays authoritative.
	if a.text.Len() > 0 && !responsesOutputHasText(resp.Output) {
		if !fillResponsesOutputText(resp.Output, a.text.String()) {
			resp.Output = append(resp.Output, ResponsesOutput{
				Type: "message",
				Role: "assistant",
				Content: []ResponsesContentPart{{
					Type: "output_text",
					Text: a.text.String(),
				}},
			})
		}
	}

	for outputIndex := range resp.Output {
		item := &resp.Output[outputIndex]
		if item.Type != "function_call" || item.Arguments != "" {
			continue
		}
		for funcIndex := range a.funcCalls {
			call := &a.funcCalls[funcIndex]
			matchesCallID := item.CallID != "" && item.CallID == call.CallID
			if !matchesCallID && call.OutputIndex != outputIndex {
				continue
			}
			if call.Args.Len() > 0 {
				item.Arguments = call.Args.String()
			}
			break
		}
	}
}

// responsesOutputHasText reports whether the terminal output already carries
// usable message text. Whitespace-only text does not count.
func responsesOutputHasText(output []ResponsesOutput) bool {
	for i := range output {
		if output[i].Type != "message" {
			continue
		}
		for _, part := range output[i].Content {
			if part.Type == "output_text" && strings.TrimSpace(part.Text) != "" {
				return true
			}
		}
	}
	return false
}

// fillResponsesOutputText writes text into the first empty output_text part of
// the first message item, adding an output_text part when that message has
// none. It returns false when the output holds no message item, leaving the
// caller to append one.
func fillResponsesOutputText(output []ResponsesOutput, text string) bool {
	for i := range output {
		if output[i].Type != "message" {
			continue
		}
		for j := range output[i].Content {
			if output[i].Content[j].Type != "output_text" {
				continue
			}
			if strings.TrimSpace(output[i].Content[j].Text) == "" {
				output[i].Content[j].Text = text
				return true
			}
		}
		output[i].Content = append(output[i].Content, ResponsesContentPart{
			Type: "output_text",
			Text: text,
		})
		return true
	}
	return false
}

// MeteringEnvelopeUnambiguous rejects duplicate receipt locations for new zero/correction evidence.
func MeteringEnvelopeUnambiguous(data []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return false
	}
	seen := map[string]bool{}
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return false
		}
		name, ok := token.(string)
		if !ok {
			return false
		}
		name = strings.ToLower(name)
		var raw json.RawMessage
		if decoder.Decode(&raw) != nil {
			return false
		}
		if name != "usage" && name != "response" {
			continue
		}
		if seen[name] {
			return false
		}
		seen[name] = true
		if name == "response" && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) && !MeteringEnvelopeUnambiguous(raw) {
			return false
		}
	}
	return true
}

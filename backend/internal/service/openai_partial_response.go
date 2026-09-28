package service

import (
	"bytes"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// A failed read can still contain authoritative usage. Only a complete JSON
// document or a fully delimited SSE frame may contribute billing evidence.
func (s *OpenAIGatewayService) partialOpenAIResponseUsage(c *gin.Context, body []byte) *OpenAIUsage {
	usage := &OpenAIUsage{}
	observer := upstreamResponseModelObserverFromContext(c)
	if observer == nil {
		observer = beginUpstreamResponseModelObservation(c)
	}
	observe := func(eventType string, payload []byte) {
		if !gjson.ValidBytes(payload) {
			return
		}
		observer.ObserveOpenAI(payload, eventType)
		s.parseSSEUsageBytesWithType(payload, eventType, usage)
	}
	if gjson.ValidBytes(body) {
		observe(strings.TrimSpace(gjson.GetBytes(body, "type").String()), body)
		return usage
	}
	var parser openAICompatSSEFrameParser
	for {
		line, remaining, found := bytes.Cut(body, []byte("\n"))
		if !found {
			break
		}
		body = remaining
		frame, complete := parser.AddLine(strings.TrimSuffix(string(line), "\r"))
		if complete {
			payload := []byte(frame.Data)
			observe(effectiveOpenAISSEEventType(payload, frame.EventType), payload)
		}
	}
	return usage
}

func (s *OpenAIGatewayService) handleOpenAIPartialReadFailure(resp *http.Response, c *gin.Context, body []byte, readErr error) *OpenAIUsage {
	usage := s.partialOpenAIResponseUsage(c, body)
	if openAIUsageHasTokens(usage) {
		message := "Failed to read upstream response"
		if errors.Is(readErr, ErrUpstreamResponseBodyTooLarge) {
			message = "Upstream response too large"
		}
		_ = s.writeOpenAINonStreamingProtocolError(resp, c, message, usage)
		return usage
	}
	if errors.Is(readErr, ErrUpstreamResponseBodyTooLarge) {
		openAITooLargeError(c)
	}
	return nil
}

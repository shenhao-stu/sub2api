package service

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var ErrCacheTTLOrderConflict = errors.New("cache_ttl_order_conflict: 1h cache breakpoints must precede 5m breakpoints; set ttl explicitly to preserve the intended order")

func isClaudeCacheModel(model string) bool {
	model = strings.ToLower(strings.TrimSpace(model))
	return strings.HasPrefix(model, "claude-") || strings.Contains(model, "/claude-") || strings.Contains(model, "anthropic.claude-")
}

// normalizeClaudeCacheTTL only supplies the platform default for existing
// breakpoints. Explicit values (including invalid values) remain the provider's
// validation responsibility. It never invents a breakpoint or a usage bucket.
func normalizeClaudeCacheTTL(body []byte, model string) ([]byte, error) {
	if !isClaudeCacheModel(model) {
		return body, nil
	}
	_, messages, tools, system := collectCacheControlPaths(body)
	paths := append(append(append([]string{}, tools...), system...), messages...)
	if gjson.GetBytes(body, "cache_control").Exists() {
		paths = append(paths, "cache_control")
	}
	out := body
	seen5m := false
	for _, path := range paths {
		cc := gjson.GetBytes(out, path)
		if cc.Get("type").String() != "ephemeral" {
			continue
		}
		ttl := cc.Get("ttl")
		if !ttl.Exists() {
			var err error
			out, err = sjson.SetBytes(out, path+".ttl", cacheTTLTarget1h)
			if err != nil {
				return nil, fmt.Errorf("set cache default: %w", err)
			}
			ttl = gjson.GetBytes(out, path+".ttl")
		}
		if ttl.Type != gjson.String {
			continue
		}
		if ttl.Str == cacheTTLTarget5m {
			seen5m = true
		}
		if ttl.Str == cacheTTLTarget1h && seen5m {
			return nil, ErrCacheTTLOrderConflict
		}
	}
	automatic := gjson.GetBytes(out, "cache_control")
	if automatic.Get("type").String() == "ephemeral" {
		autoTTL := automatic.Get("ttl").String()
		lastTTL := lastAnthropicCacheableTTL(out)
		if (autoTTL == "5m" || autoTTL == "1h") && lastTTL != "" && autoTTL != lastTTL {
			return nil, ErrCacheTTLOrderConflict
		}
	}
	return out, nil
}

// Automatic caching shares the final eligible block; its explicit TTL must
// agree. An unmarked final text/tool block is a new breakpoint and is allowed.
func lastAnthropicCacheableTTL(body []byte) string {
	last := ""
	visit := func(blocks gjson.Result, inlineTools bool) {
		if blocks.Type == gjson.String && blocks.Str != "" {
			last = ""
			return
		}
		if !blocks.IsArray() {
			return
		}
		blocks.ForEach(func(_, block gjson.Result) bool {
			if !block.IsObject() {
				return true
			}
			typ := block.Get("type").String()
			if typ == "thinking" || typ == "redacted_thinking" {
				return true
			}
			last = ""
			cc := block.Get("cache_control")
			if inlineTools && typ == "tool_addition" && block.Get("tool.type").String() == "tool_definition" && !cc.Exists() {
				cc = block.Get("tool.definition.cache_control")
			}
			if cc.Get("type").String() == "ephemeral" {
				if ttl := cc.Get("ttl").String(); ttl == "5m" || ttl == "1h" {
					last = ttl
				}
			}
			return true
		})
	}
	visit(gjson.GetBytes(body, "tools"), false)
	visit(gjson.GetBytes(body, "system"), false)
	gjson.GetBytes(body, "messages").ForEach(func(_, msg gjson.Result) bool {
		visit(msg.Get("content"), msg.Get("role").String() == "system")
		return true
	})
	return last
}

// WriteCacheTTLRequestError uses a stable, safe terminal client error. Builders
// call this before any network dispatch; the ingress parser calls it before
// acquiring account slots or reserving customer balance.
func WriteCacheTTLRequestError(c *gin.Context, err error) bool {
	if !errors.Is(err, ErrCacheTTLOrderConflict) {
		return false
	}
	if c == nil {
		return true
	}
	WriteAnthropicErrorResponse(c, http.StatusBadRequest, gin.H{"type": "error", "error": gin.H{"type": "invalid_request_error", "code": "cache_ttl_order_conflict", "message": ErrCacheTTLOrderConflict.Error()}})
	return true
}

func prepareClaudeCacheTTL(c *gin.Context, body []byte, model string) ([]byte, error) {
	out, err := normalizeClaudeCacheTTL(body, model)
	if err != nil {
		WriteCacheTTLRequestError(c, err)
	}
	return out, err
}

func rejectDeprecatedCacheTTLOverride(extra map[string]any) error {
	if enabled, ok := extra["cache_ttl_override_enabled"]; ok && enabled != nil && enabled != false {
		return infraerrors.BadRequest("CACHE_TTL_OVERRIDE_RETIRED", "Cache TTL billing overrides are retired; usage is billed from the upstream's reported cache buckets. Disable the override or omit it.")
	}
	return nil
}

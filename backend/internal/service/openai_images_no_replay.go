package service

import (
	"encoding/json"
	"errors"
	"github.com/tidwall/gjson"
	"net/http"
	"strings"
	"time"
)

// Even zero usage or a malformed receipt makes a rejection unsuitable for
// automatic replay. Absence is checked independently of meter parsing.
func imageRejectionHasNoGenerationEvidence(body []byte) bool {
	if !gjson.ValidBytes(body) {
		return false
	}
	var clear func(gjson.Result, int) bool
	clear = func(value gjson.Result, depth int) bool {
		if depth > 8 {
			return false
		}
		if value.IsObject() && hasDuplicateJSONObjectKeys(value) {
			return false
		}
		valid := true
		if value.IsObject() || value.IsArray() {
			value.ForEach(func(key, child gjson.Result) bool {
				switch strings.ToLower(key.String()) {
				case "usage", "svg_usage", "data", "output", "b64_json", "response", "tool_usage":
					valid = false
					return false
				}
				valid = clear(child, depth+1)
				return valid
			})
		}
		return valid
	}
	return clear(gjson.ParseBytes(body), 0)
}

// Accept only complete, internally consistent native image counters. Marshal
// known fields so arbitrary upstream metadata cannot escape in an error.
// Never infer a meter from requested quality, image count or partial totals.
func strictImageTerminalUsage(body []byte) (OpenAIUsage, json.RawMessage, bool) {
	none := func() (OpenAIUsage, json.RawMessage, bool) { return OpenAIUsage{}, nil, false }
	envelope, ok := receiptObject(body)
	if !ok {
		return none()
	}
	if _, exists := envelope["svg_usage"]; exists {
		return none()
	}
	meter, ok := receiptObject(envelope["usage"])
	if !ok {
		return none()
	}
	get := func(m map[string]json.RawMessage, key string) (int, bool) {
		n, valid := receiptInteger(m[key])
		return n, valid && n <= 100_000_000
	}
	in, a := get(meter, "input_tokens")
	out, b := get(meter, "output_tokens")
	total, d := get(meter, "total_tokens")
	detail, valid := receiptObject(meter["input_tokens_details"])
	if !a || !b || !d || !valid || in+out != total {
		return none()
	}
	text, a := get(detail, "text_tokens")
	image, b := get(detail, "image_tokens")
	if !a || !b || text+image != in {
		return none()
	}
	cached, ct, ci := 0, 0, 0
	if _, exists := detail["cached_tokens"]; exists {
		cached, valid = get(detail, "cached_tokens")
		if !valid {
			return none()
		}
	}
	if raw, exists := detail["cached_tokens_details"]; exists {
		split, valid := receiptObject(raw)
		if !valid {
			return none()
		}
		ct, a = get(split, "text_tokens")
		ci, b = get(split, "image_tokens")
		if !a || !b {
			return none()
		}
		if _, hasTotal := detail["cached_tokens"]; !hasTotal {
			cached = ct + ci
		}
	} else if cached > 0 {
		if image == 0 {
			ct = cached
		} else if text == 0 {
			ci = cached
		} else {
			return none()
		}
	}
	if cached != ct+ci || ct > text || ci > image || cached > in {
		return none()
	}
	if raw, exists := meter["output_tokens_details"]; exists {
		split, valid := receiptObject(raw)
		if !valid {
			return none()
		}
		ot, a := get(split, "text_tokens")
		oi, b := get(split, "image_tokens")
		if !a || !b || ot != 0 || oi != out {
			return none()
		}
	}
	canonical, _ := json.Marshal(map[string]any{
		"input_tokens": in, "output_tokens": out, "total_tokens": total,
		"input_tokens_details":  map[string]any{"text_tokens": text, "image_tokens": image, "cached_tokens": cached, "cached_tokens_details": map[string]int{"text_tokens": ct, "image_tokens": ci}},
		"output_tokens_details": map[string]int{"text_tokens": 0, "image_tokens": out},
	})
	return OpenAIUsage{InputTokens: in, ImageInputTokens: image, CacheReadInputTokens: cached, ImageCacheReadTokens: ci, OutputTokens: out, ImageOutputTokens: out}, canonical, true
}

func imageTerminalUsageResult(resp *http.Response, parsed *OpenAIImagesRequest, model, upstreamModel string, usage OpenAIUsage, started time.Time) *OpenAIForwardResult {
	return &OpenAIForwardResult{RequestID: resp.Header.Get("x-request-id"), UpstreamHeaders: resp.Header, ResponseHeaders: resp.Header.Clone(), Usage: usage, Model: model, UpstreamModel: upstreamModel, Stream: parsed.Stream, Duration: time.Since(started), ImageEndpoint: true, ImageSize: parsed.SizeTier, ImageInputSize: parsed.Size}
}

// A failed generation can report paid tokens without delivering an image.
// Preserve its real counters without inventing a billable image or retrying.
func OpenAIImageResultHasBillableUsage(result *OpenAIForwardResult) bool {
	return result != nil && (result.ImageCount > 0 || result.ImageEndpoint && openAIUsageHasTokens(&result.Usage))
}

func imageErrorHasFinalUsage(err error) bool {
	var terminal *OpenAIImagesUpstreamError
	return errors.As(err, &terminal) && len(terminal.Usage) > 0
}

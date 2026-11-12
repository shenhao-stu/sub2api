package service

import (
	"fmt"
	"regexp"
	"strings"
	"unsafe"

	"github.com/tidwall/gjson"
)

var nativeClaudeToolReferenceName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// ValidateNativeClaudeToolResults checks the stateless part of the native
// Messages tool result contract. The relay still owns session validation.
func ValidateNativeClaudeToolResults(messages []byte) error {
	var invalid error
	// MessagesRaw borrows immutable request bytes. All JSON views die here.
	gjson.Parse(unsafe.String(unsafe.SliceData(messages), len(messages))).ForEach(func(i, message gjson.Result) bool {
		content := message.Get("content")
		if !content.IsArray() {
			return true
		}
		seenOtherBlock := false
		content.ForEach(func(j, block gjson.Result) bool {
			if block.Get("type").Str != "tool_result" {
				seenOtherBlock = true
				return true
			}
			if seenOtherBlock {
				invalid = fmt.Errorf("messages[%d].content[%d]: tool results must precede other content", i.Int(), j.Int())
				return false
			}
			if err := validateNativeClaudeToolResult(block); err != nil {
				invalid = fmt.Errorf("messages[%d].content[%d].%w", i.Int(), j.Int(), err)
				return false
			}
			return true
		})
		return invalid == nil
	})
	return invalid
}

func validateNativeClaudeToolResult(block gjson.Result) error {
	if block.Get("tool_use_id").Str == "" {
		return fmt.Errorf("tool_use_id: expected a non-empty string")
	}
	if flag := block.Get("is_error"); flag.Exists() && flag.Type != gjson.True && flag.Type != gjson.False {
		return fmt.Errorf("is_error: expected a boolean")
	}
	content := block.Get("content")
	if !content.Exists() || content.Type == gjson.String {
		return nil
	}
	if !content.IsArray() {
		return fmt.Errorf("content: expected a string or an array")
	}

	var invalid error
	var sawSearch, sawOther bool
	content.ForEach(func(i, child gjson.Result) bool {
		if field, problem := nativeClaudeToolContentProblem(child); problem != "" {
			invalid = fmt.Errorf("content[%d].%s: %s", i.Int(), field, problem)
			return false
		}
		if child.Get("type").Str == "search_result" {
			sawSearch = true
		} else {
			sawOther = true
		}
		if sawSearch && sawOther {
			invalid = fmt.Errorf("content: search results must not be mixed with other content")
			return false
		}
		return true
	})
	return invalid
}

func nativeClaudeToolContentProblem(block gjson.Result) (field, message string) {
	switch kind := block.Get("type").Str; kind {
	case "text":
		if block.Get("text").Type != gjson.String {
			return "text", "expected a string"
		}
	case "tool_reference":
		if !nativeClaudeToolReferenceName.MatchString(block.Get("tool_name").Str) {
			return "tool_name", "expected 1–64 letters, digits, underscores or hyphens"
		}
	case "image", "document":
		source := block.Get("source")
		if source.Get("type").Str == "url" && strings.HasPrefix(source.Get("url").Str, "https://") {
			return "", ""
		}
		media := source.Get("media_type").Str
		encoding := source.Get("type").Str
		image := kind == "image" && encoding == "base64" &&
			(media == "image/png" || media == "image/jpeg" || media == "image/gif" || media == "image/webp")
		document := kind == "document" &&
			(encoding == "text" && media == "text/plain" || encoding == "base64" && media == "application/pdf")
		if !(image || document) || source.Get("data").Type != gjson.String {
			return "source", "expected a supported image or document source"
		}
	case "search_result":
		if block.Get("source").Type != gjson.String {
			return "source", "expected a string"
		}
		if block.Get("title").Type != gjson.String {
			return "title", "expected a string"
		}
		content := block.Get("content")
		textOnly := content.IsArray()
		content.ForEach(func(_, child gjson.Result) bool {
			textOnly = textOnly && child.Get("type").Str == "text" && child.Get("text").Type == gjson.String
			return textOnly
		})
		if !textOnly {
			return "content", "expected an array of text blocks"
		}
	default:
		return "type", "unsupported tool result content block for this Claude Code group"
	}
	return "", ""
}

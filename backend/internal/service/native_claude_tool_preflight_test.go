package service

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateNativeClaudeToolResults(t *testing.T) {
	for _, tc := range []struct {
		name, result, errorField string
	}{
		{"plain string", `"normal result"`, ""},
		{"omitted content", "", ""},
		{"valid CLI reference", `[{"type":"tool_reference","tool_name":"mcp__vendor__Read-File"},{"type":"text","text":"loaded"}]`, ""},
		{"URL image", `[{"type":"image","source":{"type":"url","url":"https://example.test/image.png"}}]`, ""},
		{"base64 PDF", `[{"type":"document","source":{"type":"base64","media_type":"application/pdf","data":"YQ=="}}]`, ""},
		{"search results", `[{"type":"search_result","source":"fixture","title":"fixture","content":[{"type":"text","text":"document"}]}]`, ""},
		{"search mixed with text", `[{"type":"search_result","source":"fixture","title":"fixture","content":[]},{"type":"text","text":"document"}]`, "search results must not be mixed"},
		{"oversize reference", `[{"type":"tool_reference","tool_name":"` + strings.Repeat("a", 65) + `"}]`, "tool_name"},
		{"invalid source", `[{"type":"image","source":{"type":"base64","media_type":"text/html","data":"YQ=="}}]`, ".source"},
		{"invalid text", `[{"type":"text","text":42}]`, ".text"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			content := ""
			if tc.result != "" {
				content = `,"content":` + tc.result
			}
			raw := []byte(`[{"role":"assistant","content":[{"type":"tool_use","id":"fixture","name":"Read","input":{"type":"tool_reference","tool_name":"ignored.argument"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"fixture"` + content + `},{"type":"text","text":"continue"}]}]`)
			err := ValidateNativeClaudeToolResults(raw)
			if tc.errorField != "" {
				require.ErrorContains(t, err, tc.errorField)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

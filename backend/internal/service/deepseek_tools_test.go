package service

import "testing"

func TestDeepSeekToolValidation(t *testing.T) {
	for _, tc := range []struct {
		kind, body string
		ok         bool
	}{
		{"search", `{"query":"中文","count":1,"ori_question":"query","localization":"1.1.1.1"}`, true},
		{"search", `{"query":" ","count":1}`, false}, {"search", `{"query":"abc","count":51}`, false},
		{"fetch", `{"url":"https://example.com"}`, true}, {"fetch", `{"url":"//example.com"}`, false}, {"fetch", `{"url":"https://example.com","stream":true}`, false},
	} {
		if (ValidateDeepSeekToolBody(DeepSeekToolModel("/v1/tools/"+tc.kind), []byte(tc.body)) == nil) != tc.ok {
			t.Error(tc)
		}
	}
	for _, tc := range []struct {
		kind, body      string
		billable, valid bool
	}{
		{"search", `{"results":[{"url":"https://example.com"}]}`, true, true}, {"search", `{"results":[]}`, false, true}, {"search", `{"results":[null]}`, false, false},
		{"fetch", `{"content":"body"}`, true, true}, {"fetch", `{"content":""}`, false, true}, {"fetch", `{"error":{}}`, false, false},
	} {
		v, err := deepSeekToolResult(DeepSeekToolModel("/v1/tools/"+tc.kind), []byte(tc.body))
		if v != tc.billable || (err == nil) != tc.valid {
			t.Error(tc)
		}
	}
}

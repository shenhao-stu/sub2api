package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func structuredFormatForward(t *testing.T, protocol, accountType, format string, stream bool) (*ForwardResult, error, *queuedHTTPUpstreamStub, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	const result = `{"candidates":[{"content":{"role":"model","parts":[{"text":"{\"value\":9007199254740993}"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":19,"cachedContentTokenCount":5,"candidatesTokenCount":3,"thoughtsTokenCount":2}}`
	payload := result
	if stream {
		payload = "data: " + result + "\n\n"
	}
	up := &queuedHTTPUpstreamStub{responses: []*http.Response{{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(payload))}}}
	account := &Account{ID: 811, Platform: PlatformGemini, Type: accountType, Credentials: map[string]any{"auth_mode": "apikey", "api_key": "isolated-test-key", "location": "global", "tier_id": "vertex"}, Concurrency: 1}
	svc := &GeminiMessagesCompatService{httpUpstream: up, cfg: &config.Config{}}
	history := `"messages":[{"role":"user","content":"return fixture"}],"response_format":` + format
	endpoint := "/v1/chat/completions"
	if protocol == "responses" {
		history = `"input":"return fixture","text":{"format":` + format + `}`
		endpoint = "/v1/responses"
	}
	body := []byte(fmt.Sprintf(`{"model":"gemini-4-argon","stream":%t,%s}`, stream, history))
	c, rec := newAntigravityCompatContext(http.MethodPost, endpoint, body)
	var got *ForwardResult
	var err error
	if protocol == "responses" {
		got, err = svc.ForwardAsResponses(context.Background(), c, account, body)
	} else {
		got, err = svc.ForwardAsChatCompletions(context.Background(), c, account, body)
	}
	return got, err, up, rec.Body.String()
}

func TestGeminiStructuredFormatNativeForward(t *testing.T) {
	const schema = `{"type":"object","properties":{"value":{"type":"integer","minimum":9007199254740993},"pattern":{"type":"string","enum":["oneOf","minLength"]}},"required":["value"],"additionalProperties":false}`
	for _, protocol := range []string{"chat", "responses"} {
		for _, accountType := range []string{AccountTypeAPIKey, AccountTypeServiceAccount} {
			for _, stream := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/stream=%t", protocol, accountType, stream), func(t *testing.T) {
					format := `{"type":"json_schema","name":"fixture","strict":true,"schema":` + schema + `}`
					if protocol == "chat" {
						format = `{"type":"json_schema","json_schema":{"name":"fixture","strict":true,"schema":` + schema + `}}`
					}
					result, err, up, wire := structuredFormatForward(t, protocol, accountType, format, stream)
					require.NoError(t, err, wire)
					require.NotNil(t, result)
					require.Equal(t, 1, up.callCount)
					native := up.requestBodies[0]
					require.Equal(t, "application/json", gjson.GetBytes(native, "generationConfig.responseMimeType").String())
					require.Equal(t, schema, gjson.GetBytes(native, "generationConfig.responseJsonSchema").Raw, "preserve schema bytes/order/numbers")
					require.Equal(t, "return fixture", gjson.GetBytes(native, "contents.0.parts.0.text").String())
					require.Equal(t, 14, result.Usage.InputTokens)
					require.Equal(t, 5, result.Usage.OutputTokens)
					require.Equal(t, 5, result.Usage.CacheReadInputTokens)
					require.NotNil(t, result.Usage.receiptMeasured)
					if protocol == "responses" && stream {
						require.Equal(t, 1, strings.Count(wire, "event: response.completed"))
					}
				})
			}
		}
	}
}

func TestGeminiStructuredFormatJSONObject(t *testing.T) {
	for _, protocol := range []string{"chat", "responses"} {
		result, err, up, wire := structuredFormatForward(t, protocol, AccountTypeAPIKey, `{"type":"json_object"}`, false)
		require.NoError(t, err, wire)
		require.NotNil(t, result)
		require.Equal(t, 1, up.callCount)
		require.Equal(t, "application/json", gjson.GetBytes(up.requestBodies[0], "generationConfig.responseMimeType").String())
		require.JSONEq(t, `{"type":"object"}`, gjson.GetBytes(up.requestBodies[0], "generationConfig.responseJsonSchema").Raw)
	}
}

func TestGeminiStructuredFormatUnsupportedWithoutDispatch(t *testing.T) {
	for _, schema := range []string{
		`null`, `true`, `[]`,
		`{"type":"object","properties":{"x":{"type":"string","minLength":2}}}`,
		`{"type":"object","properties":{"x":{"oneOf":[{"type":"integer"},{"type":"number"}]}}}`,
		`{"type":"array","items":{"type":"integer"},"uniqueItems":true}`,
		`{"type":"string","pattern":"^required$"}`,
		`{"type":"string","type":"number"}`,
		`{"type":"object","properties":{"x":{"type":"string"},"x":{"type":"number"}}}`,
		`{"type":"string|number"}`,
		`{"type":["string",4]}`,
		`{"type":"number","enum":[true]}`,
		`{"type":"object","required":"value"}`,
		`{"type":"array","minItems":-1}`,
		`{"type":"array","maxItems":1.5}`,
		`{"type":"array","items":false}`,
		`{"$ref":"https://example.invalid/schema"}`,
		`{"$ref":"#/$defs/missing"}`,
		`{"$defs":{"x":{"$ref":"#/$defs/x"}},"$ref":"#/$defs/x"}`,
		`{"$defs":{"x":{"type":"object"}},"$ref":"#/$defs/x","additionalProperties":false}`,
		`{"type":"object","properties":{"x":{"$id":"nested","type":"string"}}}`,
		`{"type":"string","format":"email"}`,
	} {
		for _, protocol := range []string{"chat", "responses"} {
			t.Run(protocol+"/"+schema, func(t *testing.T) {
				format := `{"type":"json_schema","name":"fixture","strict":true,"schema":` + schema + `}`
				if protocol == "chat" {
					format = `{"type":"json_schema","json_schema":{"name":"fixture","strict":true,"schema":` + schema + `}}`
				}
				result, err, up, wire := structuredFormatForward(t, protocol, AccountTypeAPIKey, format, true)
				require.Error(t, err)
				require.Nil(t, result)
				require.Zero(t, up.callCount)
				require.Equal(t, "invalid_request_error", gjson.Get(wire, "error.type").String())
				require.NotContains(t, wire, "example.invalid")
				require.NotContains(t, wire, "response.completed")
			})
		}
	}
}

func TestGeminiStructuredFormatReferencesAndDescription(t *testing.T) {
	const schema = `{"$defs":{"item":{"type":"object","properties":{"value":{"type":["integer","null"]}},"required":["value"],"additionalProperties":false}},"type":"array","items":{"$ref":"#/$defs/item"},"minItems":1,"maxItems":2,"description":"schema description"}`
	format := `{"type":"json_schema","json_schema":{"name":"fixture","description":"format description","strict":false,"schema":` + schema + `}}`
	_, err, up, wire := structuredFormatForward(t, "chat", AccountTypeAPIKey, format, false)
	require.NoError(t, err, wire)
	got := gjson.GetBytes(up.requestBodies[0], "generationConfig.responseJsonSchema")
	require.Equal(t, "#/$defs/item", got.Get("items.$ref").String())
	require.Equal(t, "format description\nschema description", got.Get("description").String())
	require.Equal(t, gjson.Get(schema, "$defs").Raw, got.Get("$defs").Raw)
}

func TestGeminiStructuredFormatDefaultUnchanged(t *testing.T) {
	for _, protocol := range []string{"chat", "responses"} {
		for _, format := range []string{`null`, `{"type":"text"}`} {
			_, err, up, wire := structuredFormatForward(t, protocol, AccountTypeAPIKey, format, false)
			require.NoError(t, err, wire)
			require.False(t, gjson.GetBytes(up.requestBodies[0], "generationConfig.responseJsonSchema").Exists())
			require.False(t, gjson.GetBytes(up.requestBodies[0], "generationConfig.responseMimeType").Exists())
		}
	}
}

func TestGeminiStructuredFormatEnvelopeRejected(t *testing.T) {
	for _, protocol := range []string{"chat", "responses"} {
		for _, format := range []string{`[]`, `true`, `42`, `"json_schema"`, `{}`, `{"type":3}`, `{"type":"unknown"}`, `{"type":"json_object","schema":{"type":"array"}}`, `{"type":"text","type":"json_object"}`} {
			result, err, up, _ := structuredFormatForward(t, protocol, AccountTypeAPIKey, format, false)
			require.Error(t, err)
			require.Nil(t, result)
			require.Zero(t, up.callCount)
		}
	}
}

func TestGeminiStructuredFormatToolHistoryPreserved(t *testing.T) {
	const history = `[{"role":"user","content":"before"},{"type":"function_call","call_id":"a","name":"lookup","arguments":"{\"id\":9007199254740993}","extra_content":{"google":{"thought_signature":"real-signature"}}},{"type":"function_call_output","call_id":"a","output":{"value":17}},{"role":"user","content":"return JSON"}]`
	const response = `{"candidates":[{"content":{"parts":[{"text":"{\"value\":17}"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":19,"candidatesTokenCount":3}}`
	body := `{"model":"gemini-4-argon","stream":true,"input":` + history + `,"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}],"tool_choice":"none","text":{"format":{"type":"json_schema","name":"result","strict":true,"schema":{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}}}}`
	result, err, up, wire := geminiResponsesFixture(t, body, "data: "+response+"\n\n")
	require.NoError(t, err, wire)
	require.Equal(t, 1, up.callCount)
	require.NotNil(t, result.Usage.receiptMeasured)
	native := up.requestBodies[0]
	require.Equal(t, "real-signature", gjson.GetBytes(native, "contents.1.parts.0.thoughtSignature").String())
	require.Equal(t, "9007199254740993", gjson.GetBytes(native, "contents.1.parts.0.functionCall.args.id").Raw)
	require.Equal(t, "17", gjson.GetBytes(native, "contents.2.parts.0.functionResponse.response.value").Raw)
	require.Equal(t, "return JSON", gjson.GetBytes(native, "contents.3.parts.0.text").String())
	require.Equal(t, "NONE", gjson.GetBytes(native, "toolConfig.functionCallingConfig.mode").String())
	require.Equal(t, "application/json", gjson.GetBytes(native, "generationConfig.responseMimeType").String())
	require.Equal(t, 1, strings.Count(wire, "event: response.completed"))
}

func TestGeminiStructuredFormatDialectAndReferenceDescription(t *testing.T) {
	for _, schema := range []string{
		`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"array","prefixItems":[{"type":"number"}],"items":{"type":"string"}}`,
		`{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","properties":{"x":{"type":"string"}}}`,
	} {
		format := `{"type":"json_schema","json_schema":{"name":"fixture","strict":true,"schema":` + schema + `}}`
		_, err, up, wire := structuredFormatForward(t, "chat", AccountTypeAPIKey, format, false)
		require.NoError(t, err, wire)
		require.Equal(t, schema, gjson.GetBytes(up.requestBodies[0], "generationConfig.responseJsonSchema").Raw)
	}
	for _, format := range []string{
		`{"type":"json_schema","json_schema":{"name":"fixture","schema":{"$schema":"http://json-schema.org/draft-07/schema#","type":"array","prefixItems":[{"type":"number"}]}}}`,
		`{"type":"json_schema","json_schema":{"name":"fixture","schema":{"$schema":"https://example.invalid/dialect","type":"object"}}}`,
		`{"type":"json_schema","json_schema":{"name":"fixture","description":"must survive","schema":{"$defs":{"x":{"type":"object"}},"$ref":"#/$defs/x"}}}`,
	} {
		result, err, up, _ := structuredFormatForward(t, "chat", AccountTypeAPIKey, format, false)
		require.Error(t, err)
		require.Nil(t, result)
		require.Zero(t, up.callCount)
	}
}

package service

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestGrokAgentMessagePreservesPayloadRouteAndReplay(t *testing.T) {
	body := []byte(`{"input":[
		{"type":"reasoning","id":"rs_native","summary":[{"type":"summary_text","text":"native summary"}],"encrypted_content":"native-opaque"},
		{"type":"function_call","name":"lookup","call_id":"call_1","arguments":"{}"},
		{"type":"function_call_output","call_id":"call_1","output":"done"},
		{"type":"agent_message","author":"/root","recipient":"/root/task","content":[
			{"type":"input_text","text":"Message Type: NEW_TASK\nPayload:\n"},
			{"type":"encrypted_content","encrypted_content":"Reply with the single word: ALPHA"},
			{"type":"text","text":"\nKeep this last."}
		]},
		{"type":"agent_message","author":"/root/task","recipient":"/root","content":"ALPHA"}
	]}`)
	original := append([]byte(nil), body...)
	patched, err := patchGrokResponsesBody(body, "grok-4.6")
	require.NoError(t, err)
	require.Equal(t, original, body)
	require.False(t, gjson.GetBytes(patched, `input.#(type=="agent_message")`).Exists())
	require.JSONEq(t, gjson.GetBytes(original, "input.0").Raw, gjson.GetBytes(patched, "input.0").Raw)
	require.Equal(t, "call_1", gjson.GetBytes(patched, "input.1.call_id").String())
	require.Equal(t, "call_1", gjson.GetBytes(patched, "input.2.call_id").String())
	require.Equal(t, "message", gjson.GetBytes(patched, "input.3.type").String())
	require.Equal(t, "user", gjson.GetBytes(patched, "input.3.role").String())
	require.Contains(t, gjson.GetBytes(patched, "input.3.content.0.text").String(), `from "/root" to "/root/task"`)
	require.Equal(t, "Message Type: NEW_TASK\nPayload:\n", gjson.GetBytes(patched, "input.3.content.1.text").String())
	require.Equal(t, "Reply with the single word: ALPHA", gjson.GetBytes(patched, "input.3.content.2.text").String())
	require.Equal(t, "\nKeep this last.", gjson.GetBytes(patched, "input.3.content.3.text").String())
	require.Equal(t, "ALPHA", gjson.GetBytes(patched, "input.4.content.1.text").String())

	again, err := patchGrokResponsesBody(patched, "grok-4.6")
	require.NoError(t, err)
	require.JSONEq(t, string(patched), string(again))
}

func TestGrokAgentMessagePreservesMediaOrder(t *testing.T) {
	body := []byte(`{"input":[{"type":"agent_message","author":"worker","recipient":"root","content":[
		{"type":"input_text","text":"before"},
		{"type":"input_image","image_url":"data:image/png;base64,QUE=","detail":"high"},
		{"type":"encrypted_content","encrypted_content":"between"},
		{"type":"image_url","image_url":{"url":"https://example.invalid/image.png"}},
		{"type":"input_file","file_url":"https://example.invalid/file.pdf","filename":"file.pdf","mime_type":"application/pdf"},
		{"type":"input_text","text":"after"}
	]}]}`)
	patched, err := patchGrokResponsesBody(body, "grok-4.6")
	require.NoError(t, err)
	require.Equal(t, "before", gjson.GetBytes(patched, "input.0.content.1.text").String())
	require.Equal(t, "data:image/png;base64,QUE=", gjson.GetBytes(patched, "input.0.content.2.image_url").String())
	require.Equal(t, "high", gjson.GetBytes(patched, "input.0.content.2.detail").String())
	require.Equal(t, "between", gjson.GetBytes(patched, "input.0.content.3.text").String())
	require.Equal(t, "input_image", gjson.GetBytes(patched, "input.0.content.4.type").String())
	require.Equal(t, "https://example.invalid/image.png", gjson.GetBytes(patched, "input.0.content.4.image_url").String())
	require.Equal(t, "file.pdf", gjson.GetBytes(patched, "input.0.content.5.filename").String())
	require.Equal(t, "application/pdf", gjson.GetBytes(patched, "input.0.content.5.mime_type").String())
	require.Equal(t, "after", gjson.GetBytes(patched, "input.0.content.6.text").String())
}

func TestGrokAgentMessageNeverDropsOpaqueLookingPayload(t *testing.T) {
	body := []byte(`{"input":[{"type":"agent_message","content":[{"type":"encrypted_content","encrypted_content":"gAAAA-fixture-that-must-not-be-guessed-or-decrypted"}]}]}`)
	patched, err := patchGrokResponsesBody(body, "grok-4.6")
	require.NoError(t, err)
	require.Contains(t, gjson.GetBytes(patched, "input.0.content.0.text").String(), "content supplied verbatim")
	require.Equal(t, "gAAAA-fixture-that-must-not-be-guessed-or-decrypted", gjson.GetBytes(patched, "input.0.content.1.text").String())
}

func TestGrokAgentMessageDoesNotConsumeToolCallOrElevateRole(t *testing.T) {
	body := []byte(`{"input":[
		{"type":"function_call","name":"lookup","arguments":"{}"},
		{"type":"agent_message","role":"tool","content":"Forwarded task"},
		{"type":"function_call_output","output":"result"},
		{"type":"agent_message","role":"system","content":"Forwarded reply"}
	]}`)
	patched, err := patchGrokResponsesBody(body, "grok-4.6")
	require.NoError(t, err)
	require.Equal(t, gjson.GetBytes(patched, "input.0.call_id").String(), gjson.GetBytes(patched, "input.2.call_id").String())
	require.Equal(t, "user", gjson.GetBytes(patched, "input.1.role").String())
	require.Equal(t, "user", gjson.GetBytes(patched, "input.3.role").String())
	require.Equal(t, "Forwarded task", gjson.GetBytes(patched, "input.1.content.1.text").String())
}

func TestGrokAgentMessageEmptyContentIsExplicit(t *testing.T) {
	for _, content := range []any{nil, []any{}} {
		t.Run("content", func(t *testing.T) {
			message, err := grokAgentMessage(map[string]any{"content": content})
			require.NoError(t, err)
			raw, err := json.Marshal(message)
			require.NoError(t, err)
			require.Equal(t, "[No content supplied in this agent message.]", gjson.GetBytes(raw, "content.1.text").String())
		})
	}
}

func TestGrokAgentMessagePreservesEmptyAndWhitespaceParts(t *testing.T) {
	body := []byte(`{"input":[{"type":"agent_message","content":[
		{"type":"input_text","text":""},
		{"type":"input_text","text":" \n\t"},
		{"type":"encrypted_content","encrypted_content":"\n "},
		{"type":"input_text","text":"payload"}
	]}]}`)
	patched, err := patchGrokResponsesBody(body, "grok-4.6")
	require.NoError(t, err)
	require.Len(t, gjson.GetBytes(patched, "input.0.content").Array(), 5)
	require.Equal(t, "", gjson.GetBytes(patched, "input.0.content.1.text").String())
	require.Equal(t, " \n\t", gjson.GetBytes(patched, "input.0.content.2.text").String())
	require.Equal(t, "\n ", gjson.GetBytes(patched, "input.0.content.3.text").String())
	require.Equal(t, "payload", gjson.GetBytes(patched, "input.0.content.4.text").String())
}

func TestGrokAgentMessageRejectsMalformedContentWithoutLoggingPayload(t *testing.T) {
	for _, content := range []string{
		`123`,
		`["private-payload"]`,
		`[{"type":"input_text","text":123}]`,
		`[{"type":"encrypted_content","encrypted_content":{"private-payload":true}}]`,
		`[{"type":"unknown-private-payload","text":"private-payload"}]`,
		`[{"type":"input_image","image_url":"data:image/png;base64,"}]`,
		`[{"type":"input_file","file_url":"private-payload","file_id":"other"}]`,
	} {
		t.Run(content, func(t *testing.T) {
			body := []byte(`{"input":[{"type":"agent_message","content":` + content + `}]}`)
			patched, err := patchGrokResponsesBody(body, "grok-4.6")
			require.Error(t, err)
			require.Nil(t, patched)
			require.Contains(t, err.Error(), "agent_message")
			require.NotContains(t, err.Error(), "private-payload")
		})
	}
}

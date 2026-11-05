package service

import (
	"bytes"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestReviewRelayPassthroughIssuedStateCanContinue(t *testing.T) {
	t.Run("named-event", func(t *testing.T) { reviewRelayPassthroughIssuedStateCanContinue(t, true) })
	t.Run("data-only", func(t *testing.T) { reviewRelayPassthroughIssuedStateCanContinue(t, false) })
}

func reviewRelayPassthroughIssuedStateCanContinue(t *testing.T, namedEvent bool) {
	gin.SetMode(gin.TestMode)
	response := func(id, state string) *http.Response {
		h := http.Header{"Content-Type": {"text/event-stream"}}
		h.Set("x-codex-turn-state", state)
		body := "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"" + id + "\",\"status\":\"completed\",\"model\":\"gpt-6.1-terra\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"fixture\"}]}],\"usage\":{\"input_tokens\":7,\"output_tokens\":2}}}\n\n"
		if namedEvent {
			body = "event: response.completed\n" + body
		}
		return &http.Response{StatusCode: 200, Header: h, Body: io.NopCloser(bytes.NewBufferString(body))}
	}
	u := &httpUpstreamRecorder{responses: []*http.Response{response("resp_review_a", "issued-state"), response("resp_review_b", "next-state")}}
	s, a := openCodeSessionTestService(), relayScopeAccount(true)
	s.httpUpstream = u
	a.Credentials["api_key"] = "test-only-key"
	body := []byte(`{"model":"gpt-6.1-terra","stream":true,"input":"synthetic"}`)
	first := relayScopeContext(1, 101, "getoken:134", "same-session")
	_, err := s.forwardOpenAIPassthrough(first.Request.Context(), first, a, body, body, "gpt-6.1-terra", false, nil, true, time.Now())
	require.NoError(t, err)
	require.Equal(t, "issued-state", first.Writer.Header().Get("x-codex-turn-state"))
	next := relayScopeContext(1, 101, "getoken:134", "same-session")
	next.Request.Header.Set("x-codex-turn-state", "issued-state")
	result, err := s.forwardOpenAIPassthrough(next.Request.Context(), next, a, body, body, "gpt-6.1-terra", false, nil, true, time.Now())
	require.NoError(t, err, "state actually delivered by the passthrough path must remain usable by its owner")
	require.Equal(t, 7, result.Usage.InputTokens)
	require.Len(t, u.requests, 2)
	require.Equal(t, "issued-state", u.requests[1].Header.Get("x-codex-turn-state"))
}

func TestReviewRelayMetadataMustNotReuseForeignThreadIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := []byte(`{"model":"gpt-6.1-terra","input":"synthetic"}`)
	for _, pass := range []bool{false, true} {
		s, a := openCodeSessionTestService(), relayScopeAccount(true)
		ca := relayScopeContext(1, 101, "getoken:134", "same-session")
		cb := relayScopeContext(1, 101, "getoken:135", "same-session")
		for _, c := range []*gin.Context{ca, cb} {
			c.Request.Header.Set("x-codex-turn-metadata", `{"thread_id":"shared-raw-thread","turn_id":"shared-turn"}`)
		}
		ra, err := relayScopeBuild(s, ca, a, body, pass)
		require.NoError(t, err)
		rb, err := relayScopeBuild(s, cb, a, body, pass)
		require.NoError(t, err)
		require.NotEqual(t, gjson.Get(ra.Header.Get("x-codex-turn-metadata"), "thread_id").String(), gjson.Get(rb.Header.Get("x-codex-turn-metadata"), "thread_id").String(), "two authenticated tenants cannot send the same raw upstream thread identity")
	}
}

func TestReviewRelayUnknownOpaqueTurnStateMustNotDispatch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, pass := range []bool{false, true} {
		s, a := openCodeSessionTestService(), relayScopeAccount(true)
		c := relayScopeContext(1, 101, "getoken:135", "same-session")
		c.Request.Header.Set("x-codex-turn-state", "opaque-state-from-another-user")
		_, err := relayScopeBuild(s, c, a, []byte(`{"model":"gpt-6.1-terra","input":"synthetic"}`), pass)
		require.Error(t, err, "unowned opaque continuation state must be rejected before dispatch, never silently dropped")
	}
}

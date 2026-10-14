//go:build unit

package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestGrokVideoImageInputReachesUpstreamWithoutChangingMode(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name, input, expected string
	}{
		{"official URL", `{"image":{"url":"https://example.com/frame.png"}}`, `{"image":{"url":"https://example.com/frame.png"}}`},
		{"inline image", `{"image":{"url":"data:image/png;base64,aW1n"}}`, `{"image":{"url":"data:image/png;base64,aW1n"}}`},
		{"string alias", `{"image":"https://example.com/frame.png"}`, `{"image":{"url":"https://example.com/frame.png"}}`},
		{"nested alias", `{"image":{"image_url":{"url":"data:image/png;base64,aW1n"},"detail":"high"}}`, `{"image":{"url":"data:image/png;base64,aW1n","detail":"high"}}`},
		{"single reference stays reference", `{"reference_images":[{"image_url":{"url":"https://example.com/ref.png"}}]}`, `{"reference_images":[{"url":"https://example.com/ref.png"}]}`},
		{"file reference preserved", `{"image":{"file_id":"file-1"},"future_option":{"enabled":true}}`, `{"image":{"file_id":"file-1"},"future_option":{"enabled":true}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"model":"grok-imagine-video-1.5","prompt":"slow camera pan","duration":8,"resolution":"480p",` + tc.input[1:])
			expected := `{"model":"grok-imagine-video-1.5","prompt":"slow camera pan","duration":8,"resolution":"480p",` + tc.expected[1:]
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/videos/generations", bytes.NewReader(body))
			account := &Account{ID: 63, Platform: PlatformGrok, Type: AccountTypeAPIKey,
				Credentials: map[string]any{"api_key": "test-key"}}
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: http.StatusOK,
				Header: http.Header{"Content-Type": []string{"application/json"}},
				Body:   io.NopCloser(strings.NewReader(`{"request_id":"video-input-1"}`))}}
			svc := &OpenAIGatewayService{httpUpstream: upstream}
			result, err := svc.ForwardGrokMedia(context.Background(), c, account, GrokMediaEndpointVideosGenerations, "", body, "application/json")
			require.NoError(t, err)
			require.Equal(t, xai.DefaultBaseURL+"/videos/generations", upstream.lastReq.URL.String())
			require.JSONEq(t, expected, string(upstream.lastBody))
			require.Equal(t, "video-input-1", result.ResponseID)
			require.Equal(t, "grok-imagine-video-1.5", result.BillingModel)
			require.Zero(t, result.VideoCount, "accepting a job is not completion billing")
			again, _, err := normalizeGrokMediaForwardBody(GrokMediaEndpointVideosGenerations, upstream.lastBody, "application/json")
			require.NoError(t, err)
			require.JSONEq(t, expected, string(again))
		})
	}
}

func TestGrokVideoInvalidImageInputIsNotSentOrBilled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, input := range []string{
		`"image_url":"https://example.com/frame.png"`,
		`"images":[{"url":"https://example.com/frame.png"}]`,
		`"image":{"url":{"url":"https://example.com/frame.png"}}`,
		`"image":{"image_url":{"url":123}}`,
		`"image":{"image_url":{}}`,
		`"image":" "`,
		`"image":{"url":" "}`,
		`"image":null`,
		`"image":[{"url":"https://example.com/frame.png"}]`,
		`"reference_images":{"url":"https://example.com/ref.png"}`,
		`"reference_images":[null]`,
		`"reference_images":[{"image_url":false}]`,
	} {
		t.Run(input, func(t *testing.T) {
			body := []byte(`{"model":"grok-imagine-video-1.5","prompt":"animate",` + input + `}`)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/videos/generations", bytes.NewReader(body))
			upstream := &httpUpstreamRecorder{}
			svc := &OpenAIGatewayService{httpUpstream: upstream}
			account := &Account{Platform: PlatformGrok, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "test-key"}}
			result, err := svc.ForwardGrokMedia(context.Background(), c, account, GrokMediaEndpointVideosGenerations, "", body, "application/json")
			require.Error(t, err)
			require.Nil(t, result)
			require.Nil(t, upstream.lastReq)
			var failover *UpstreamFailoverError
			require.False(t, errors.As(err, &failover))
			require.Equal(t, http.StatusBadRequest, recorder.Code)
			require.Equal(t, "invalid_request_error", gjson.GetBytes(recorder.Body.Bytes(), "error.type").String())
		})
	}
}

func TestGrokImageInputParsingDoesNotInterpretObjectsAsURLs(t *testing.T) {
	for _, image := range []string{`{"url":{}}`, `{"image_url":{}}`, `{"image_url":{"url":123}}`, `{"image_url":false}`} {
		info := ParseGrokMediaRequest("application/json", []byte(`{"image":`+image+`}`))
		require.Empty(t, info.InputImageURLs)
		require.False(t, info.HasInputImage())
	}
}

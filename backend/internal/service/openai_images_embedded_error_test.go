package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestImagesAPIKeyEmbeddedErrorPreservesStatusAndDoesNotBillOrRetry(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
		status        int
	}{
		{"unavailable", `{"error":{"type":"server_error","code":"upstream_unavailable","message":"Temporarily unavailable","status_code":503},"status_code":503}`, 503},
		{"parameter", `{"error":{"type":"invalid_request_error","code":"invalid_value","message":"Invalid image size","param":"size","status_code":400}}`, 400},
		{"top_level", `{"error":{"type":"server_error","code":"upstream_timeout","message":"Timed out"},"status_code":504}`, 504},
		{"invalid_status", `{"error":{"type":"server_error","code":"upstream_error","message":"Failed","status_code":503.5}}`, 502},
		{"conflicting_status", `{"error":{"type":"server_error","code":"upstream_error","message":"Failed","status_code":503},"status_code":400}`, 502},
		{"duplicate_status", `{"error":{"type":"server_error","code":"upstream_error","message":"Failed","status_code":503,"status_code":503}}`, 502},
		{"string_status", `{"error":{"type":"server_error","code":"upstream_error","message":"Failed","status_code":"503"}}`, 502},
		{"plain_error", `{"error":{"message":"failed"}}`, 502},
	} {
		for _, keepalive := range []bool{false, true} {
			t.Run(tc.name+map[bool]string{false: "/before_headers", true: "/after_heartbeat"}[keepalive], func(t *testing.T) {
				body := []byte(`{"model":"gpt-image-2.5-flare","prompt":"synthetic fixture"}`)
				c, rec := newOpenAIImagesTestContext(t, body)
				if keepalive {
					stop := StartOpenAIImagesJSONKeepalive(c, 5*time.Millisecond)
					defer stop()
					waitForOpenAIImagesJSONKeepalive(t, c)
				}
				upstream := &httpUpstreamRecorder{resp: &http.Response{
					StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}},
					Body: io.NopCloser(strings.NewReader(" \n" + tc.payload)),
				}}
				svc := newOpenAIImagesTestService(upstream)
				parsed, err := svc.ParseOpenAIImagesRequest(c, body)
				require.NoError(t, err)
				result, err := svc.ForwardImages(context.Background(), c, newOpenAIImagesAPIKeyAccount(), body, parsed, "")
				require.Nil(t, result)
				var imageErr *OpenAIImagesUpstreamError
				require.ErrorAs(t, err, &imageErr)
				require.Equal(t, tc.status, imageErr.StatusCode)
				var retry *UpstreamFailoverError
				require.NotErrorAs(t, err, &retry)
				require.Len(t, upstream.requests, 1)
				wantWire := tc.status
				if keepalive {
					wantWire = 200
				}
				require.Equal(t, wantWire, rec.Code)
				require.EqualValues(t, tc.status, gjson.Get(rec.Body.String(), "error.status_code").Int())
				require.EqualValues(t, tc.status, gjson.Get(rec.Body.String(), "status_code").Int())
				require.Equal(t, gjson.Get(tc.payload, "error.code").String(), gjson.Get(rec.Body.String(), "error.code").String())
				if tc.name == "parameter" {
					require.Equal(t, "size", gjson.Get(rec.Body.String(), "error.param").String())
				}
			})
		}
	}
}

func TestImagesAPIKeyStreamErrorKeepsDeliveredCount(t *testing.T) {
	for _, delivered := range []bool{false, true} {
		t.Run(map[bool]string{false: "no_image", true: "one_completed_image"}[delivered], func(t *testing.T) {
			body := []byte(`{"model":"gpt-image-2.5-flare","prompt":"synthetic fixture","stream":true}`)
			c, rec := newOpenAIImagesTestContext(t, body)
			frames := ": keepalive\n\n"
			if delivered {
				frames += "data: {\"type\":\"image_generation.completed\",\"b64_json\":\"aW1hZ2U=\",\"usage\":{\"input_tokens\":3,\"output_tokens\":5}}\n\n"
			}
			frames += "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"server_error\",\"code\":\"upstream_unavailable\",\"message\":\"Unavailable\",\"status_code\":503},\"status_code\":503}\n\n"
			frames += "data: {\"type\":\"image_generation.completed\",\"b64_json\":\"after-terminal-must-not-be-delivered\"}\n\n"
			upstream := &httpUpstreamRecorder{resp: &http.Response{
				StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(frames)),
			}}
			svc := newOpenAIImagesTestService(upstream)
			parsed, err := svc.ParseOpenAIImagesRequest(c, body)
			require.NoError(t, err)
			result, err := svc.ForwardImages(context.Background(), c, newOpenAIImagesAPIKeyAccount(), body, parsed, "")
			var imageErr *OpenAIImagesUpstreamError
			require.ErrorAs(t, err, &imageErr)
			require.Equal(t, 503, imageErr.StatusCode)
			require.Len(t, upstream.requests, 1)
			if delivered {
				require.NotNil(t, result)
				require.Equal(t, 1, result.ImageCount, "already delivered output remains billable once")
			} else {
				require.Nil(t, result, "an error alone cannot create billable images")
			}
			require.Equal(t, 200, rec.Code)
			require.Equal(t, 1, strings.Count(rec.Body.String(), `"code":"upstream_unavailable"`))
			require.NotContains(t, rec.Body.String(), "after-terminal-must-not-be-delivered")
		})
	}
}

func TestImagesEmbeddedJSONErrorDoesNotExposeOriginOrCredentials(t *testing.T) {
	body := []byte(`{"model":"gpt-image-2.5-flare","prompt":"synthetic fixture"}`)
	c, rec := newOpenAIImagesTestContext(t, body)
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(`{"error":{"type":"server_error","code":"upstream_unavailable","status_code":503,"message":"Post https://origin.invalid/private?key=synthetic-secret Bearer synthetic-token unavailable"}}`)),
	}}
	svc := newOpenAIImagesTestService(upstream)
	parsed, err := svc.ParseOpenAIImagesRequest(c, body)
	require.NoError(t, err)
	result, err := svc.ForwardImages(context.Background(), c, newOpenAIImagesAPIKeyAccount(), body, parsed, "")
	require.Error(t, err)
	require.Nil(t, result)
	for _, value := range []string{"origin.invalid", "synthetic-secret", "synthetic-token"} {
		require.NotContains(t, rec.Body.String(), value)
	}
	require.Equal(t, 503, rec.Code)
	require.Contains(t, rec.Body.String(), "unavailable")
}

type imageTerminalReadFailure struct{}

func (imageTerminalReadFailure) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestImagesAPIKeyTerminalErrorSurvivesLaterReadFailure(t *testing.T) {
	for _, timer := range []bool{false, true} {
		t.Run(map[bool]string{false: "reader", true: "timer"}[timer], func(t *testing.T) {
			body := []byte(`{"model":"gpt-image-2.5-flare","prompt":"synthetic","stream":true}`)
			c, rec := newOpenAIImagesTestContext(t, body)
			frame := "data: {\"error\":{\"type\":\"server_error\",\"code\":\"upstream_unavailable\",\"message\":\"Unavailable\",\"status_code\":503}}\n\n"
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200,
				Header: http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:   io.NopCloser(io.MultiReader(strings.NewReader(frame), imageTerminalReadFailure{})),
			}}
			svc := newOpenAIImagesTestService(upstream)
			if timer {
				svc.cfg.Gateway.ImageStreamDataIntervalTimeout = 1
			}
			parsed, err := svc.ParseOpenAIImagesRequest(c, body)
			require.NoError(t, err)
			result, err := svc.ForwardImages(context.Background(), c, newOpenAIImagesAPIKeyAccount(), body, parsed, "")
			require.Nil(t, result)
			var upstreamErr *OpenAIImagesUpstreamError
			require.ErrorAs(t, err, &upstreamErr)
			require.Equal(t, 503, upstreamErr.StatusCode)
			require.Equal(t, 1, strings.Count(rec.Body.String(), `"status_code":503`))
			require.Len(t, upstream.requests, 1)
		})
	}
}

func TestImagesAPIKeyJSONFallbackErrorCannotCreateBillableImage(t *testing.T) {
	body := []byte(`{"model":"gpt-image-2.5-flare","prompt":"synthetic","stream":true}`)
	c, _ := newOpenAIImagesTestContext(t, body)
	upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200,
		Header: http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:   io.NopCloser(strings.NewReader(`{"error":{"type":"server_error","message":"Unavailable","status_code":503},"data":[{"b64_json":"aW1hZ2U="}]}`)),
	}}
	svc := newOpenAIImagesTestService(upstream)
	parsed, err := svc.ParseOpenAIImagesRequest(c, body)
	require.NoError(t, err)
	result, err := svc.ForwardImages(context.Background(), c, newOpenAIImagesAPIKeyAccount(), body, parsed, "")
	require.Nil(t, result, "an error payload cannot create billable images")
	var upstreamErr *OpenAIImagesUpstreamError
	require.ErrorAs(t, err, &upstreamErr)
	require.Equal(t, 503, upstreamErr.StatusCode)
}

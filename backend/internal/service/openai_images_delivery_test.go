package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestForwardImagesAPIKeyRequiresDeliveredImages(t *testing.T) {
	for _, body := range []string{
		"error code: 502\n", `<html>Bad Gateway</html>`,
		`{"data":[]}`, `{"usage":{"images":2}}`,
	} {
		t.Run(body, func(t *testing.T) {
			request := []byte(`{"model":"gpt-image-2.5-sunburst","prompt":"draw a cat","n":2}`)
			c, rec := newOpenAIImagesTestContext(t, request)
			upstream := &httpUpstreamRecorder{resp: &http.Response{
				StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/plain"}},
				Body: io.NopCloser(strings.NewReader(body)),
			}}
			svc := newOpenAIImagesTestService(upstream)
			parsed, err := svc.ParseOpenAIImagesRequest(c, request)
			require.NoError(t, err)
			result, err := svc.ForwardImages(context.Background(), c, newOpenAIImagesAPIKeyAccount(), request, parsed, "")
			require.Nil(t, result, "no image means no billable result")
			var imageErr *OpenAIImagesUpstreamError
			require.ErrorAs(t, err, &imageErr)
			require.Equal(t, http.StatusBadGateway, rec.Code)
			require.Equal(t, "image_output_missing", gjson.Get(rec.Body.String(), "error.code").String())
		})
	}
}

func TestForwardImagesAPIKeyBillsOnlyDeliveredCount(t *testing.T) {
	request := []byte(`{"model":"gpt-image-2.5-sunburst","prompt":"draw a cat","n":2}`)
	c, rec := newOpenAIImagesTestContext(t, request)
	svc := newOpenAIImagesTestService(&httpUpstreamRecorder{resp: openAIImagesJSONResponse()})
	parsed, err := svc.ParseOpenAIImagesRequest(c, request)
	require.NoError(t, err)
	result, err := svc.ForwardImages(context.Background(), c, newOpenAIImagesAPIKeyAccount(), request, parsed, "")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, 1, result.ImageCount)
}

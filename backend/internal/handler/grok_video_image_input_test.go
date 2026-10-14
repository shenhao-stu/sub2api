//go:build unit

package handler

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGrokVideoInvalidImageRejectedBeforeAccountAdmission(t *testing.T) {
	h, slots, bindings, upstream := newGrokMediaSlotHandler(t, false, false)
	upstream.call = func(*http.Request, int64) (*http.Response, error) {
		t.Fatal("invalid image must not reach upstream")
		return nil, nil
	}
	h.grokMediaEligibilityProber = grokMediaSlotProber(func(context.Context, int64) (bool, string, error) {
		t.Fatal("invalid image must not probe or select an account")
		return false, "", nil
	})
	c, w := grokMediaSlotContext(context.Background(), true)
	c.Request.Body = io.NopCloser(strings.NewReader(`{"model":"grok-imagine-video-1.5","prompt":"animate","image_url":"https://example.com/frame.png"}`))
	h.GrokVideoGeneration(c)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "image.url")
	require.Zero(t, slots.released)
	require.Zero(t, bindings.writes)
	slots.assertReleased(t)
}

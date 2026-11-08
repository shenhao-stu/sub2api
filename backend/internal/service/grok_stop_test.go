package service

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGrokStopPreservesValuesAndOtherFields(t *testing.T) {
	for _, tc := range []struct{ in, out string }{
		{`"end"`, `["end"]`},
		{`""`, `[""]`},
		{`"  end\n\u4e2d\" "`, `["  end\n\u4e2d\" "]`},
		{`["end","other"]`, `["end","other"]`},
		{`[]`, `[]`}, {`null`, `null`}, {`42`, `42`},
		{`{"nested":"stop"}`, `{"nested":"stop"}`},
	} {
		body := []byte(`{"stop":` + tc.in + `,"messages":[{"role":"user","content":"synthetic","stop":"inner"}],"max_tokens":321}`)
		original := string(body)
		got, err := normalizeGrokChatStop(body, false)
		require.NoError(t, err)
		require.Equal(t, `{"stop":`+tc.out+`,"messages":[{"role":"user","content":"synthetic","stop":"inner"}],"max_tokens":321}`, string(got))
		require.Equal(t, original, string(body))
	}
	body := []byte(`{"messages":[{"role":"user","content":"synthetic"}]}`)
	got, err := normalizeGrokChatStop(body, false)
	require.NoError(t, err)
	require.Equal(t, body, got)
}

func TestGrokStopConcurrentReadDoesNotMutateRequest(t *testing.T) {
	body := []byte(`{"stop":"synthetic","metadata":{"stop":"untouched"}}`)
	original := string(body)
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			got, err := normalizeGrokChatStop(body, false)
			if err != nil || string(got) != `{"stop":["synthetic"],"metadata":{"stop":"untouched"}}` {
				t.Errorf("normalization failed: %v", err)
			}
		})
	}
	wg.Wait()
	require.Equal(t, original, string(body))
}

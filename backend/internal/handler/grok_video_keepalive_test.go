//go:build unit

package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGrokVideoJSONKeepaliveKeepsOneDocument(t *testing.T) {
	old := grokVideoJSONKeepaliveInterval
	grokVideoJSONKeepaliveInterval = 2 * time.Millisecond
	t.Cleanup(func() { grokVideoJSONKeepaliveInterval = old })
	for _, generation := range []bool{true, false} {
		for _, scenario := range []string{"slow headers", "slow body", "transport error", "upstream error"} {
			t.Run(map[bool]string{true: "generation", false: "status"}[generation]+"/"+scenario, func(t *testing.T) {
				h, slots, _, up := newGrokMediaSlotHandler(t, false, false)
				h.maxAccountSwitches = 0
				up.call = func(req *http.Request, id int64) (*http.Response, error) {
					if scenario != "slow body" {
						time.Sleep(15 * time.Millisecond)
					}
					if scenario == "transport error" {
						return nil, errors.New("connection failed")
					}
					status, payload := 200, `{"request_id":"task","status":"pending"}`
					if scenario == "upstream error" {
						status, payload = 400, `{"error":{"message":"invalid video request"}}`
					}
					var body io.ReadCloser = io.NopCloser(strings.NewReader(payload))
					if scenario == "slow body" {
						r, w := io.Pipe()
						body = r
						go func() { time.Sleep(15 * time.Millisecond); io.WriteString(w, payload); w.Close() }()
					}
					return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: body}, nil
				}
				c, w := grokMediaSlotContext(context.Background(), generation)
				if generation {
					h.GrokVideoGeneration(c)
				} else {
					h.GrokVideoStatus(c)
				}
				slots.assertReleased(t)
				require.Equal(t, 200, w.Code)
				require.True(t, strings.HasPrefix(w.Body.String(), " \n"), w.Body.String())
				require.True(t, json.Valid(w.Body.Bytes()), "response must be exactly one JSON document: %s", w.Body.String())
				if strings.Contains(scenario, "error") {
					require.Contains(t, w.Body.String(), `"error"`)
				} else {
					require.Contains(t, w.Body.String(), `"request_id":"task"`)
				}
				wantCalls := 1
				if generation && scenario == "transport error" {
					wantCalls = 3 // Existing failover across three accounts before acceptance.
				}
				require.Equal(t, wantCalls, up.calls, "keepalive must preserve the existing retry policy")
			})
		}
	}
}

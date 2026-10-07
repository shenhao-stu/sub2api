package commandcode

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

func TestBillingAccountBoundaryAndUnknownValues(t *testing.T) {
	client := Client{UserAgent: "command-code-cli/test", Version: "test", HTTPClient: testDoer(func(req *http.Request) (*http.Response, error) {
		require.Equal(t, BillingEndpoint, req.URL.String())
		require.Equal(t, http.MethodGet, req.Method)
		require.Equal(t, "Bearer account-only-key", req.Header.Get("Authorization"))
		require.Equal(t, "production", req.Header.Get("x-cli-environment"))
		require.Equal(t, "test", req.Header.Get("x-command-code-version"))
		deadline, ok := req.Context().Deadline()
		require.True(t, ok)
		require.LessOrEqual(t, time.Until(deadline), 20*time.Second)
		return fixtureResponse(req, 200, `{"credits":{"monthlyCredits":0},"windowLimits":{"fiveHour":{"remainingPercent":0,"resetAt":1790726400000},"weekly":{"used":null,"resetAt":"2026-10-01T00:00:00Z"}},"secret":"not-for-browser"}`), nil
	})}
	quota, err := client.BillingCredits(context.Background(), "account-only-key")
	require.NoError(t, err)
	require.NotNil(t, quota.Credits.MonthlyCredits)
	require.Zero(t, *quota.Credits.MonthlyCredits)
	require.Nil(t, quota.WindowLimits.Limited)
	require.Nil(t, quota.WindowLimits.Weekly.Used)
	require.Zero(t, *quota.WindowLimits.FiveHour.RemainingPercent)
	data, err := json.Marshal(quota)
	require.NoError(t, err)
	require.NotContains(t, string(data), "not-for-browser")
	require.Contains(t, string(data), "2026-10-01T00:00:00Z")
}

func TestBillingRejectsAmbiguousAndOversizedResponses(t *testing.T) {
	for _, body := range []string{
		`{"credits":{}} {"credits":{}}`, `{"error":{"message":"private"}}`,
		`{"credits":{"monthlyCredits":"private"}}`,
		`{"windowLimits":{"weekly":{"resetAt":"private"}}}`,
		`{"credits":{}}` + strings.Repeat(" ", maxBillingBody),
	} {
		client := Client{HTTPClient: testDoer(func(req *http.Request) (*http.Response, error) { return fixtureResponse(req, 200, body), nil })}
		_, err := client.BillingCredits(context.Background(), "key")
		require.ErrorIs(t, err, ErrProtocol)
		require.NotContains(t, err.Error(), "private")
	}
}

func TestCreditRejectionNormalization(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		exhausted  bool
	}{
		{"explicit", `{"error":{"code":"BAD_REQUEST","message":"You have insufficient credits to make this request. private details"}}`, 400, true},
		{"generic", `{"error":{"code":"BAD_REQUEST","message":"invalid model"}}`, 400, false},
		{"missing_code", `{"error":{"message":"You have insufficient credits to make this request."}}`, 400, false},
		{"request_echo", `{"error":{"code":"BAD_REQUEST","message":"invalid input"},"request":{"message":"You have insufficient credits to make this request."}}`, 400, false},
		{"wrong_status", `{"error":{"code":"BAD_REQUEST","message":"You have insufficient credits to make this request."}}`, 500, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := Client{HTTPClient: testDoer(func(req *http.Request) (*http.Response, error) { return fixtureResponse(req, tc.status, tc.body), nil })}
			resp, err := client.ChatCompletion(context.Background(), "key", []byte(testInput), false)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, tc.exhausted, IsCreditExhausted(resp.StatusCode, body))
			require.NotContains(t, string(body), "private")
			require.NotContains(t, string(body), "invalid model")
		})
	}
}

func TestHTTPErrorWithUsageCannotBecomeUnmeteredCreditFailover(t *testing.T) {
	client := Client{HTTPClient: testDoer(func(req *http.Request) (*http.Response, error) {
		return fixtureResponse(req, 400, `{"error":{"code":"BAD_REQUEST","message":"You have insufficient credits to make this request."},"usage":{"inputTokens":20,"outputTokens":3}}`), nil
	})}
	resp, err := client.ChatCompletion(context.Background(), "key", []byte(testInput), false)
	require.Nil(t, resp)
	var metered *UsageError
	require.True(t, errors.As(err, &metered))
	require.Equal(t, int64(20), metered.Usage["prompt_tokens"])
	require.Equal(t, int64(3), metered.Usage["completion_tokens"])
}

//go:build unit

package xai

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type ssoApprovalFailureClient struct {
	base          *ssoDeviceFakeClient
	status        int
	body          string
	approvalCalls int
}

func (c *ssoApprovalFailureClient) Do(req *http.Request) (*http.Response, error) {
	if req.URL.String() == SSOApproveURL {
		c.approvalCalls++
		return ssoDeviceResponse(c.status, nil, c.body), nil
	}
	return c.base.Do(req)
}

func TestConvertSSOToBuildBrowserVerificationStopsWithoutLeakingBody(t *testing.T) {
	client := &ssoApprovalFailureClient{base: &ssoDeviceFakeClient{t: t}, status: http.StatusForbidden, body: `<html>Could not verify this browser private-token private-cookie</html>`}
	_, err := ConvertSSOToBuild(context.Background(), "sso=synthetic-sso", &SSODeviceOptions{HTTPClient: client, Sleep: func(context.Context, time.Duration) error { return nil }})
	require.ErrorIs(t, err, ErrSSOBrowserVerificationRequired)
	require.NotErrorIs(t, err, ErrSSOUnauthorized)
	require.NotContains(t, err.Error(), "private-token")
	require.NotContains(t, err.Error(), "private-cookie")
	require.NotContains(t, err.Error(), "<html>")
	require.Equal(t, 1, client.approvalCalls)
	require.Zero(t, client.base.tokenCalls)
}

func TestConvertSSOToBuildClassifiesOnlyExplicitBrowserApproval403(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		browser    bool
	}{
		{"second official phrase", `<html>Request could not be verified private-body</html>`, http.StatusForbidden, true},
		{"ordinary forbidden", `<html>Forbidden private-body</html>`, http.StatusForbidden, false},
		{"401 is not browser verification", `Could not verify this browser private-body`, http.StatusUnauthorized, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &ssoApprovalFailureClient{base: &ssoDeviceFakeClient{t: t}, status: tc.status, body: tc.body}
			_, err := ConvertSSOToBuild(context.Background(), "sso=synthetic-sso", &SSODeviceOptions{HTTPClient: client, Sleep: func(context.Context, time.Duration) error { return nil }})
			require.Error(t, err)
			require.Equal(t, tc.browser, errors.Is(err, ErrSSOBrowserVerificationRequired))
			if !tc.browser {
				var statusErr SSOHTTPError
				require.ErrorAs(t, err, &statusErr)
				require.Equal(t, tc.status, statusErr.Status)
			}
			require.NotContains(t, err.Error(), "private-body")
			require.Equal(t, 1, client.approvalCalls)
			require.Zero(t, client.base.tokenCalls)
		})
	}
}

package repository

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestHTTPUpstreamDispatchedEOFIsNotSafeToReplay(t *testing.T) {
	for _, withTLS := range []bool{false, true} {
		t.Run(fmt.Sprint(withTLS), func(t *testing.T) {
			var received, traced atomic.Int32
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				received.Add(1)
				conn, _, err := w.(http.Hijacker).Hijack()
				if err == nil {
					conn.Close()
				}
			}))
			if withTLS {
				srv.StartTLS()
			} else {
				srv.Start()
			}
			defer srv.Close()
			s, do := lifecycleClient(t, srv, withTLS)
			ctx := httptrace.WithClientTrace(t.Context(), &httptrace.ClientTrace{WroteHeaders: func() { traced.Add(1) }})
			req, err := http.NewRequestWithContext(ctx, "POST", srv.URL, strings.NewReader(`{"model":"fixture"}`))
			require.NoError(t, err)
			_, err = do(req)
			require.ErrorIs(t, err, io.EOF)
			var attempt *service.HTTPUpstreamAttemptError
			require.ErrorAs(t, err, &attempt)
			require.True(t, attempt.RequestMayHaveBeenSent)
			require.Positive(t, attempt.Elapsed)
			require.EqualValues(t, 1, received.Load())
			require.Positive(t, traced.Load(), "existing trace must be composed")
			requireNoUpstreamInFlight(t, s)
		})
	}
}

func TestHTTPUpstreamDialFailureCanStillFailOver(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	listener.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", "http://"+addr, strings.NewReader("fixture"))
	require.NoError(t, err)
	_, err = doUpstreamRequest(&http.Client{}, req)
	var attempt *service.HTTPUpstreamAttemptError
	require.ErrorAs(t, err, &attempt)
	require.False(t, attempt.RequestMayHaveBeenSent)
	var ne *net.OpError
	require.True(t, errors.As(err, &ne))
}

package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/requestlifecycle"
	"github.com/stretchr/testify/require"
)

func startDrainTestServer(t *testing.T, handler http.Handler) (*http.Server, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := &http.Server{Handler: handler}
	installHTTPDrain(srv)
	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(func() { _ = srv.Close() })
	return srv, listener.Addr().String()
}

func awaitDrainSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for handler lifecycle signal")
	}
}

func TestHTTPShutdownWaitsForResponseAndSettlement(t *testing.T) {
	started, finish, settled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	srv, addr := startDrainTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-finish
		if r.Context().Err() == nil {
			_, _ = io.WriteString(w, "complete")
		}
		close(settled)
	}))
	response := make(chan string, 1)
	go func() {
		resp, err := http.Get("http://" + addr)
		if err != nil {
			response <- err.Error()
			return
		}
		defer func() { _ = resp.Body.Close() }()
		body, _ := io.ReadAll(resp.Body)
		response <- string(body)
	}()
	awaitDrainSignal(t, started)
	done := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		drained, err := ShutdownHTTPServer(ctx, srv)
		if !drained || err != nil {
			t.Errorf("shutdown: drained=%t err=%v", drained, err)
		}
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("shutdown returned before settlement")
	case <-time.After(30 * time.Millisecond):
	}
	close(finish)
	awaitDrainSignal(t, done)
	awaitDrainSignal(t, settled)
	require.Equal(t, "complete", <-response)
}

func TestHTTPShutdownWaitsForHijackedHandlerSettlement(t *testing.T) {
	for _, lateHijack := range []bool{false, true} {
		t.Run(map[bool]string{false: "existing", true: "late"}[lateHijack], func(t *testing.T) {
			entered, hijack, disconnected, settle := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			srv, addr := startDrainTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				<-hijack
				conn, _, err := http.NewResponseController(w).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				defer func() { _ = conn.Close() }()
				_, _ = io.Copy(io.Discard, conn)
				close(disconnected)
				<-settle
			}))
			conn, err := net.Dial("tcp", addr)
			require.NoError(t, err)
			defer func() { _ = conn.Close() }()
			_, err = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: test\r\n\r\n")
			require.NoError(t, err)
			awaitDrainSignal(t, entered)
			d, ok := srv.Handler.(*httpDrain)
			require.True(t, ok)
			if lateHijack {
				d.beginDrain()
				close(hijack)
			} else {
				close(hijack)
				require.Eventually(t, func() bool { d.mu.Lock(); defer d.mu.Unlock(); return len(d.hijacked) == 1 }, time.Second, time.Millisecond)
			}
			done := make(chan struct{})
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				drained, err := ShutdownHTTPServer(ctx, srv)
				if !drained || err != nil {
					t.Errorf("shutdown: drained=%t err=%v", drained, err)
				}
				close(done)
			}()
			awaitDrainSignal(t, disconnected)
			select {
			case <-done:
				t.Fatal("cleanup overtook hijacked handler settlement")
			default:
			}
			close(settle)
			awaitDrainSignal(t, done)
			d.mu.Lock()
			require.Empty(t, d.hijacked)
			d.mu.Unlock()
		})
	}
}

func TestHTTPShutdownDeadlineCancelsAndWaitsForHandler(t *testing.T) {
	started, cancelled, settled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	srv, addr := startDrainTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(cancelled)
		// Models the existing cancellation path's final usage settlement.
		time.Sleep(20 * time.Millisecond)
		close(settled)
	}))
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	require.NoError(t, err)
	awaitDrainSignal(t, started)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	drained, err := shutdownHTTPServer(ctx, srv, time.Second)
	require.True(t, drained)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	awaitDrainSignal(t, cancelled)
	awaitDrainSignal(t, settled)
}

func TestHTTPShutdownUncooperativeHandlerIsBoundedAndUnsafeForCleanup(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	srv, addr := startDrainTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-finish
	}))
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	require.NoError(t, err)
	awaitDrainSignal(t, started)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	before := time.Now()
	drained, err := shutdownHTTPServer(ctx, srv, 20*time.Millisecond)
	close(finish)
	require.False(t, drained)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(before), time.Second)
	d, ok := srv.Handler.(*httpDrain)
	require.True(t, ok)
	awaitDrainSignal(t, d.done)
}

func TestHTTPShutdownForceCancelsDetachedUpstreamBeforeSettlement(t *testing.T) {
	started, abort, upstreamCancelled, settled := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, ": upstream connected\n\n")
		require.NoError(t, http.NewResponseController(w).Flush())
		close(started)
		select {
		case <-r.Context().Done():
			close(upstreamCancelled)
		case <-abort:
		}
	}))
	t.Cleanup(upstream.Close)
	t.Cleanup(func() { close(abort) })
	srv, addr := startDrainTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := requestlifecycle.WithoutClientCancel(r.Context())
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, upstream.URL, nil)
		if err != nil {
			t.Error(err)
			return
		}
		resp, err := upstream.Client().Do(req)
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		// Dependency cleanup must wait for the existing detached billing phase.
		time.Sleep(20 * time.Millisecond)
		close(settled)
	}))
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	require.NoError(t, err)
	awaitDrainSignal(t, started)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	drained, err := shutdownHTTPServer(ctx, srv, time.Second)
	require.True(t, drained, "forced upstream cancellation must allow handler settlement to finish")
	require.ErrorIs(t, err, context.DeadlineExceeded, "forced close must remain distinguishable from graceful drain")
	awaitDrainSignal(t, upstreamCancelled)
	awaitDrainSignal(t, settled)
}

func TestHTTPDrainRejectsNewHandlersAndPreservesInterfaces(t *testing.T) {
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, flush := w.(http.Flusher)
		require.True(t, flush)
		_, _ = io.WriteString(w, "ok")
	})}
	installHTTPDrain(srv)
	before := httptest.NewRecorder()
	srv.Handler.ServeHTTP(before, httptest.NewRequest("GET", "/", nil))
	require.Equal(t, "ok", before.Body.String())
	d, ok := srv.Handler.(*httpDrain)
	require.True(t, ok)
	d.beginDrain()
	after := httptest.NewRecorder()
	srv.Handler.ServeHTTP(after, httptest.NewRequest("GET", "/", nil))
	require.Equal(t, http.StatusServiceUnavailable, after.Code)
}

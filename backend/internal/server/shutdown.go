package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/requestlifecycle"
)

const forcedHandlerDrainTimeout = 20 * time.Second

type requestConnKey struct{}

// httpDrain tracks handler completion, including handlers whose connections have
// been hijacked. It never wraps ResponseWriter, preserving streaming interfaces.
type httpDrain struct {
	next        http.Handler
	mu          sync.Mutex
	stopping    bool
	hijacked    map[net.Conn]struct{}
	active      int
	done        chan struct{}
	once        sync.Once
	force       context.Context
	forceCancel context.CancelFunc
}

func installHTTPDrain(srv *http.Server) {
	force, forceCancel := context.WithCancel(context.Background())
	d := &httpDrain{
		next: srv.Handler, hijacked: make(map[net.Conn]struct{}), done: make(chan struct{}),
		force: force, forceCancel: forceCancel,
	}
	srv.Handler = d
	previousContext, previousState := srv.ConnContext, srv.ConnState
	srv.ConnContext = func(ctx context.Context, conn net.Conn) context.Context {
		if previousContext != nil {
			ctx = previousContext(ctx, conn)
		}
		return context.WithValue(ctx, requestConnKey{}, conn)
	}
	srv.ConnState = func(conn net.Conn, state http.ConnState) {
		if previousState != nil {
			previousState(conn, state)
		}
		d.mu.Lock()
		closeConn := false
		switch state {
		case http.StateHijacked:
			d.hijacked[conn] = struct{}{}
			closeConn = d.stopping
		case http.StateClosed:
			delete(d.hijacked, conn)
		}
		d.mu.Unlock()
		if closeConn {
			_ = conn.Close()
		}
	}
}

func (d *httpDrain) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	if d.stopping {
		d.mu.Unlock()
		w.Header().Set("Connection", "close")
		http.Error(w, "server is shutting down", http.StatusServiceUnavailable)
		return
	}
	d.active++
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		if conn, ok := r.Context().Value(requestConnKey{}).(net.Conn); ok {
			delete(d.hijacked, conn)
		}
		d.active--
		if d.stopping && d.active == 0 {
			close(d.done)
		}
		d.mu.Unlock()
	}()
	d.next.ServeHTTP(w, r.WithContext(requestlifecycle.WithForceCancellation(r.Context(), d.force)))
}

func (d *httpDrain) beginDrain() {
	d.once.Do(func() {
		d.mu.Lock()
		d.stopping = true
		if d.active == 0 {
			close(d.done)
		}
		d.mu.Unlock()
		d.closeHijacked()
	})
}

func (d *httpDrain) closeHijacked() {
	d.mu.Lock()
	conns := make([]net.Conn, 0, len(d.hijacked))
	for conn := range d.hijacked {
		conns = append(conns, conn)
	}
	d.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

// ShutdownHTTPServer drains ordinary HTTP requests without cancelling them and
// closes client WebSockets so their existing handlers can settle usage. The bool
// reports whether dependency cleanup is safe; a deadline is never reported as a
// successful graceful drain.
func ShutdownHTTPServer(ctx context.Context, srv *http.Server) (bool, error) {
	return shutdownHTTPServer(ctx, srv, forcedHandlerDrainTimeout)
}

func shutdownHTTPServer(ctx context.Context, srv *http.Server, forceWait time.Duration) (bool, error) {
	d, ok := srv.Handler.(*httpDrain)
	if !ok {
		return false, errors.New("HTTP request drain tracking is not installed")
	}
	d.beginDrain()
	err := srv.Shutdown(ctx)
	select {
	case <-d.done:
		return true, err
	case <-ctx.Done():
		// Detached upstream requests must stop too, so handlers can settle known
		// usage before their dependencies are closed.
		d.forceCancel()
		// Close cancels ordinary request contexts and unblocks request-body reads.
		// Hijacked connections need their own close path.
		_ = srv.Close()
		d.closeHijacked()
	}
	deadline := time.NewTimer(forceWait)
	defer deadline.Stop()
	select {
	case <-d.done:
		return true, ctx.Err()
	case <-deadline.C:
		return false, errors.Join(ctx.Err(), errors.New("request handlers did not finish after forced connection close"))
	}
}

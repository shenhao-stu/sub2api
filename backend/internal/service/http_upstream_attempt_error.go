package service

import (
	"errors"
	"net"
	"time"
)

// HTTPUpstreamAttemptError retains transport evidence without claiming whether
// an upstream executed or billed a request. Once headers have been written, a
// failed generation must not be automatically submitted again on another account.
type HTTPUpstreamAttemptError struct {
	Err                    error
	RequestMayHaveBeenSent bool
	ConnectionReused       bool
	Elapsed                time.Duration
}

func (e *HTTPUpstreamAttemptError) Error() string { return e.Err.Error() }
func (e *HTTPUpstreamAttemptError) Unwrap() error { return e.Err }

func (e *HTTPUpstreamAttemptError) Timeout() bool {
	var n net.Error
	return errors.As(e.Err, &n) && n.Timeout()
}

func (e *HTTPUpstreamAttemptError) Temporary() bool {
	var n net.Error
	return errors.As(e.Err, &n) && n.Temporary()
}

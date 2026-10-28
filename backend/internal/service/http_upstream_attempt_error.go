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

func withUpstreamTransportAttempt(event OpsUpstreamErrorEvent, err error) OpsUpstreamErrorEvent {
	var attempt *HTTPUpstreamAttemptError
	if !errors.As(err, &attempt) {
		return event
	}
	event.DurationMs = attempt.Elapsed.Milliseconds()
	event.RequestMayHaveBeenSent = attempt.RequestMayHaveBeenSent
	event.ConnectionReused = attempt.ConnectionReused
	if attempt.RequestMayHaveBeenSent {
		event.Stage = "awaiting_response_headers"
		event.Reason = "dispatched_request_not_retried"
	}
	return event
}

func (e *HTTPUpstreamAttemptError) Timeout() bool {
	var n net.Error
	return errors.As(e.Err, &n) && n.Timeout()
}

func (e *HTTPUpstreamAttemptError) Temporary() bool {
	var n net.Error
	return errors.As(e.Err, &n) && n.Temporary()
}

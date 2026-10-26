//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOpenAIDispatchedTransportFailureDoesNotFailOverOrEvict(t *testing.T) {
	c, rec := newOpenAITransportErrTestContext()
	repo := &openaiTransportAccountRepoStub{}
	svc := &OpenAIGatewayService{accountRepo: repo}
	err := svc.handleOpenAIUpstreamTransportError(context.Background(), c, &Account{ID: 425, Platform: PlatformKimi}, &HTTPUpstreamAttemptError{Err: io.EOF, RequestMayHaveBeenSent: true, Elapsed: 270 * time.Second}, false)
	require.ErrorIs(t, err, io.EOF)
	var fo *UpstreamFailoverError
	require.False(t, errors.As(err, &fo), "dispatched POST has ambiguous billing; do not amplify it")
	require.Empty(t, repo.tempUnschedCalls)
	require.Empty(t, rec.Body.String())
}

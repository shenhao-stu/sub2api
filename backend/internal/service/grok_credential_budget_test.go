//go:build unit

package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestGrokCredentialBudgetAfterNeverResetsOrUnderflows(t *testing.T) {
	for _, test := range []struct {
		name               string
		remaining, elapsed time.Duration
		want               time.Duration
	}{
		{"partial", 15 * time.Second, 4 * time.Second, 11 * time.Second},
		{"exhausted", 4 * time.Second, 4 * time.Second, 0},
		{"overrun", 4 * time.Second, 20 * time.Second, 0},
		{"empty", 0, time.Second, 0},
		{"negative balance", -time.Second, -time.Second, 0},
		{"clock cannot add budget", time.Second, -time.Second, time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, grokCredentialBudgetAfter(test.remaining, test.elapsed))
		})
	}
}

func TestGrokCredentialBudgetAccumulatesAcquisitionAndExcludesUpstreamTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		parent := context.Background()
		for _, spent := range []time.Duration{2 * time.Second, 3 * time.Second} {
			acquire, cancel, exhausted := grokCredentialAcquisitionContext(parent, c)
			require.False(t, exhausted)
			require.NoError(t, acquire.Err())
			time.Sleep(spent)
			cancel()
			require.ErrorIs(t, acquire.Err(), context.Canceled)
			// A completed model call may take longer than the entire auth budget.
			time.Sleep(time.Minute)
		}
		requireCredentialRemaining(t, c, 10*time.Second)
		acquire, cancel, exhausted := grokCredentialAcquisitionContext(parent, c)
		require.False(t, exhausted)
		deadline, ok := acquire.Deadline()
		require.True(t, ok)
		require.Equal(t, 10*time.Second, deadline.Sub(time.Now()))
		time.Sleep(10 * time.Second)
		synctest.Wait()
		require.ErrorIs(t, acquire.Err(), context.DeadlineExceeded)
		cancel()
		requireCredentialRemaining(t, c, 0)
		time.Sleep(time.Hour)
		_, finish, exhausted := grokCredentialAcquisitionContext(parent, c)
		require.True(t, exhausted)
		require.Nil(t, finish)
	})
}

func TestGrokCredentialBudgetCancelSettlesOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		_, cancel, exhausted := grokCredentialAcquisitionContext(context.Background(), c)
		require.False(t, exhausted)
		time.Sleep(2 * time.Second)
		var callers sync.WaitGroup
		for range 8 {
			callers.Go(cancel)
		}
		callers.Wait()
		requireCredentialRemaining(t, c, 13*time.Second)
		time.Sleep(time.Minute)
		cancel()
		requireCredentialRemaining(t, c, 13*time.Second)
	})
}

func TestGrokCredentialBudgetHonorsParentCancellationAndDeadline(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "deadline"}[deadline], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent, stop := context.WithCancel(context.Background())
				if deadline {
					stop()
					parent, stop = context.WithTimeout(context.Background(), time.Second)
				}
				defer stop()
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				acquire, finish, exhausted := grokCredentialAcquisitionContext(parent, c)
				require.False(t, exhausted)
				if deadline {
					parentDeadline, _ := parent.Deadline()
					childDeadline, _ := acquire.Deadline()
					require.Equal(t, parentDeadline, childDeadline)
				}
				time.Sleep(time.Second)
				if !deadline {
					stop()
				}
				synctest.Wait()
				require.Error(t, parent.Err())
				require.ErrorIs(t, acquire.Err(), parent.Err())
				finish()
				requireCredentialRemaining(t, c, 14*time.Second)
			})
		})
	}
}

func TestGrokCredentialBudgetUnavailableStateDoesNotReset(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Set(grokCredentialFailoverRemainingKey, "invalid state")
	_, cancel, exhausted := grokCredentialAcquisitionContext(context.Background(), c)
	require.True(t, exhausted)
	require.Nil(t, cancel)
	parent := context.Background()
	acquire, cancel, exhausted := grokCredentialAcquisitionContext(parent, nil)
	require.Equal(t, parent, acquire)
	require.Nil(t, cancel)
	require.False(t, exhausted)
}

func TestGetRequestCredentialHealthyAccountAfterSlowUpstream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first := expiredGrokOAuthAccountForCredentialTest(9801)
		second := expiredGrokOAuthAccountForCredentialTest(9802)
		for _, account := range []*Account{first, second} {
			account.Credentials["access_token"] = "healthy-access"
			account.Credentials["expires_at"] = time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
		}
		repo := &tokenRefreshAccountRepo{}
		repo.accountsByID = map[int64]*Account{first.ID: first, second.ID: second}
		cache := &grokTokenCacheForProviderTest{token: "healthy-access"}
		svc := &OpenAIGatewayService{accountRepo: repo, grokTokenProvider: NewGrokTokenProvider(repo, cache)}
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		for _, account := range []*Account{first, second} {
			token, kind, err := svc.getRequestCredential(context.Background(), c, account)
			require.NoError(t, err)
			require.Equal(t, "healthy-access", token)
			require.Equal(t, "oauth", kind)
			time.Sleep(time.Minute)
		}
		requireCredentialRemaining(t, c, grokCredentialFailoverBudget)
		require.Zero(t, repo.setErrorCalls)
		require.Zero(t, repo.setTempUnschedCalls)
		require.False(t, svc.isOpenAIAccountRuntimeBlocked(first))
		require.False(t, svc.isOpenAIAccountRuntimeBlocked(second))
	})
}

type grokCredentialConfirmationDelayRepo struct {
	*grokCredentialCommitThenCancelRepo
	confirmationReads int
}

func (r *grokCredentialConfirmationDelayRepo) GetByID(ctx context.Context, id int64) (*Account, error) {
	account, err := r.tokenRefreshAccountRepo.GetByID(ctx, id)
	if err == nil && account.Status == StatusError {
		r.confirmationReads++
		time.Sleep(50 * time.Millisecond)
	}
	return account, err
}

type grokCredentialTimedRefresher struct{ *tokenRefresherStub }

func (r *grokCredentialTimedRefresher) Refresh(ctx context.Context, account *Account) (map[string]any, error) {
	time.Sleep(2 * time.Second)
	return r.tokenRefresherStub.Refresh(ctx, account)
}

func TestGetRequestCredentialBudgetIncludesRefreshAndStateConfirmation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		account := expiredGrokOAuthAccountForCredentialTest(9803)
		base := &tokenRefreshAccountRepo{}
		base.accountsByID = map[int64]*Account{account.ID: account}
		repo := &grokCredentialConfirmationDelayRepo{grokCredentialCommitThenCancelRepo: &grokCredentialCommitThenCancelRepo{
			tokenRefreshAccountRepo: base, returnErr: context.DeadlineExceeded,
		}}
		cache := &grokTokenCacheForProviderTest{lockResult: true}
		provider := NewGrokTokenProvider(repo, cache)
		refresher := &grokCredentialTimedRefresher{&tokenRefresherStub{
			err: infraerrors.New(http.StatusBadGateway, "GROK_OAUTH_TOKEN_REFRESH_FAILED", "invalid_grant"),
		}}
		provider.SetRefreshAPI(NewOAuthRefreshAPI(repo, cache), refresher)
		svc := &OpenAIGatewayService{accountRepo: repo, grokTokenProvider: provider}
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		_, _, err := svc.getRequestCredential(context.Background(), c, account)
		var failover *UpstreamFailoverError
		require.ErrorAs(t, err, &failover)
		require.Equal(t, GrokCredentialReasonRevoked, failover.Reason)
		require.Equal(t, 1, refresher.calls)
		require.Equal(t, 1, repo.confirmationReads)
		requireCredentialRemaining(t, c, grokCredentialFailoverBudget-2*time.Second-50*time.Millisecond)
		require.True(t, svc.isOpenAIAccountRuntimeBlocked(account))
		require.Equal(t, []string{GrokTokenCacheKey(account)}, cache.deletedKeys)
	})
}

func requireCredentialRemaining(t *testing.T, c *gin.Context, expected time.Duration) {
	t.Helper()
	raw, exists := c.Get(grokCredentialFailoverRemainingKey)
	require.True(t, exists)
	require.Equal(t, expected, raw)
}

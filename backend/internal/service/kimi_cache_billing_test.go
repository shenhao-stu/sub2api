package service

import (
	"context"
	"net/http"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type kimiBillingCapture struct {
	UsageBillingRepository
	commands []*UsageBillingCommand
}

func (r *kimiBillingCapture) Apply(_ context.Context, command *UsageBillingCommand) (*UsageBillingApplyResult, error) {
	copy := *command
	r.commands = append(r.commands, &copy)
	return &UsageBillingApplyResult{Applied: true}, nil
}

type kimiUsageCapture struct {
	UsageLogRepository
	rows []*UsageLog
}

func (r *kimiUsageCapture) Create(_ context.Context, row *UsageLog) (bool, error) {
	copy := *row
	r.rows = append(r.rows, &copy)
	return true, nil
}

// Exercise the real protocol adapter and RecordUsage, including the command
// sent to atomic billing. This is not a database transaction/deduplication test.
func TestKimiCacheTTLRecordUsage(t *testing.T) {
	for _, endpoint := range []string{"/v1/chat/completions", "/v1/messages", "/v1/responses"} {
		for _, stream := range []bool{false, true} {
			name := endpoint + "/json"
			if stream {
				name = endpoint + "/stream"
			}
			t.Run(name, func(t *testing.T) {
				extra := ""
				if stream {
					extra = `,"stream":true`
				}
				headers := make(http.Header)
				headers.Set("Msh-Usage-Cache-Write-Tokens-5m", "75")
				headers.Set("Msh-Usage-Cache-Write-Tokens-1h", "125")
				result, err, _, upstream := kimiDiagnosticForward(t, "kimi-k3.1-pro", endpoint, extra, false, headers)
				require.NoError(t, err)
				require.Equal(t, 1, upstream.calls)
				require.NotNil(t, result)
				require.Equal(t, 1000, result.Usage.InputTokens)
				// The fixture contains 80 reasoning tokens within completion=100.
				require.Equal(t, 100, result.Usage.OutputTokens)
				require.True(t, result.Usage.CacheCreationTTLKnown)
				require.Equal(t, 75, result.Usage.CacheCreation5mTokens)
				require.Equal(t, 125, result.Usage.CacheCreation1hTokens)

				for _, multiplier := range []float64{1, .75} {
					billingRepo, usageRepo := &kimiBillingCapture{}, &kimiUsageCapture{}
					cfg := &config.Config{}
					billing := NewBillingService(cfg, nil)
					svc := &OpenAIGatewayService{cfg: cfg, billingService: billing,
						resolver: NewModelPricingResolver(nil, billing), usageBillingRepo: billingRepo,
						usageLogRepo: usageRepo, deferredService: &DeferredService{}}
					inPrice, outPrice, readPrice, write5m, write1h := 1e-6, 2e-6, .1e-6, 1e-6, 2e-6
					group := &Group{ID: 41, Platform: PlatformKimi, RateMultiplier: multiplier,
						ModelPricing: []ChannelModelPricing{{Models: []string{"kimi-k3.1-pro"}, BillingMode: BillingModeToken,
							InputPrice: &inPrice, OutputPrice: &outPrice, CacheReadPrice: &readPrice,
							CacheWritePrice: &write5m, CacheWrite1hPrice: &write1h}}}
					key := &APIKey{ID: 254, GroupID: &group.ID, Group: group}
					err := svc.RecordUsage(context.Background(), &OpenAIRecordUsageInput{
						Result: result, APIKey: key, User: &User{ID: 1},
						Account:         &Account{ID: 425, Platform: PlatformKimi, Type: AccountTypeAPIKey},
						InboundEndpoint: endpoint, UpstreamEndpoint: "/v1/chat/completions",
					})
					require.NoError(t, err)
					require.Len(t, billingRepo.commands, 1)
					require.Len(t, usageRepo.rows, 1)
					cmd, row := billingRepo.commands[0], usageRepo.rows[0]
					require.Equal(t, 500, cmd.InputTokens, "read and write each deducted exactly once")
					require.Equal(t, 100, cmd.OutputTokens, "reasoning is already included")
					require.Equal(t, 300, cmd.CacheReadTokens)
					require.Equal(t, 200, cmd.CacheCreationTokens)
					require.Equal(t, 500, row.InputTokens)
					require.Equal(t, 75, row.CacheCreation5mTokens)
					require.Equal(t, 125, row.CacheCreation1hTokens)
					require.InDelta(t, .000325, row.CacheCreationCost, 1e-12)
					require.InDelta(t, .001055, row.TotalCost, 1e-12)
					require.InDelta(t, .001055*multiplier, row.ActualCost, 1e-12)
					require.InDelta(t, QuantizeUsageBillingAmount(.001055*multiplier), cmd.BalanceCost, 1e-12)
					require.Equal(t, row.RequestID, cmd.RequestID)
				}
			})
		}
	}
}

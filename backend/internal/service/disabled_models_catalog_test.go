//go:build unit

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestDisabledModelCatalogChannelAccountChain(t *testing.T) {
	for _, target := range []string{"gpt-5.5", "middle", "gpt-6.1-sol"} {
		t.Run(target, func(t *testing.T) {
			group := &Group{ID: 10, Platform: PlatformOpenAI}
			ch := Channel{ID: 1, Status: StatusActive, GroupIDs: []int64{10}, ModelMapping: map[string]map[string]string{PlatformOpenAI: {"alias": target}}}
			channels := newTestChannelService(makeStandardRepo(ch, map[int64]string{10: PlatformOpenAI}))
			accounts := []Account{{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Credentials: map[string]any{"model_mapping": map[string]any{"alias": "alias", "middle": "gpt-5.5", "gpt-6.1-sol": "gpt-6.1-sol"}}}}
			repo := codexModelsVisibilityAccountRepo{byGroup: map[int64][]Account{10: accounts}}
			gateway := &GatewayService{cfg: disabledModelsFixture(), accountRepo: repo, channelService: channels}
			want := []string{}
			if target == "gpt-6.1-sol" {
				want = []string{"alias"}
			}
			require.Equal(t, want, gateway.FilterDisabledModelIDs(context.Background(), &group.ID, []string{"alias"}))
			openai := &OpenAIGatewayService{cfg: disabledModelsFixture(), channelService: channels}
			for _, codex := range []bool{false, true} {
				field, id := "data", "id"
				if codex {
					field, id = "models", "slug"
				}
				body := []byte(`{"metadata":"preserved","` + field + `":[{"` + id + `":"alias","display_name":"Alias"}]}`)
				filtered, err := openai.filterDisabledChannelModelsBody(context.Background(), group, accounts, body, codex)
				require.NoError(t, err)
				require.Equal(t, len(want), len(gjson.GetBytes(filtered, field).Array()))
				require.Equal(t, "preserved", gjson.GetBytes(filtered, "metadata").String())
				if len(want) > 0 {
					require.Equal(t, body, filtered)
				}
			}
		})
	}
}

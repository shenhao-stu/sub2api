package service

import (
	"encoding/json"
	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
)

func grokSearchMeter(count int) *apicompat.ServerToolUsage {
	if count <= 0 {
		return nil
	}
	return &apicompat.ServerToolUsage{WebSearchRequests: count}
}

func grokResponseSearchCount(account *Account, response *apicompat.ResponsesResponse) int {
	if account == nil || !account.IsGrok() || response == nil {
		return 0
	}
	data, err := json.Marshal(response)
	if err != nil {
		return 0
	}
	return countGrokNativeSearchCallsFromJSONBytes(data)
}

func hasMeteredSearch(usage *OpenAIUsage) bool {
	return usage != nil && usage.ServerToolUse != nil && usage.ServerToolUse.WebSearchRequests > 0
}

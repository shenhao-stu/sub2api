package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"slices"

	"strings"
	"sync"

	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

// Scope one existing OpenAI Images group to a separate private Grok OAuth pool.
// Format is clientGroup/personalPool/exactModel[,exactModel]. Empty disables it.
// Group membership, entitlements and account concurrency are owned by Sub2API.
func personalGrokImagePool(policy string, group *service.Group, parsed *service.OpenAIImagesRequest, model, mapped string, body []byte) (int64, bool) {
	if group == nil || group.Platform != service.PlatformOpenAI || parsed == nil || parsed.Stream || parsed.Multipart || parsed.IsEdits() || parsed.N != 1 || parsed.Model != model || (mapped != "" && mapped != model) {
		return 0, false
	}
	pool, ok := service.PersonalGrokImageSource(policy, group.ID, model)
	if !ok {
		return 0, false
	}
	// Preserve native Origin behavior for streaming, edits, exact geometry and
	// parameters that xAI may drop, right through to the original OpenAI handler.
	fieldsJSON := json.NewDecoder(bytes.NewReader(body))
	first, err := fieldsJSON.Token()
	if err != nil || first != json.Delim('{') {
		return 0, false
	}
	seen := map[string]bool{}
	for fieldsJSON.More() {
		raw, err := fieldsJSON.Token()
		key, valid := raw.(string)
		if err != nil || !valid || seen[key] {
			return 0, false
		}
		seen[key] = true
		switch key {
		case "model", "prompt", "size", "n", "response_format":
		default:
			return 0, false
		}
		var value json.RawMessage
		if err := fieldsJSON.Decode(&value); err != nil {
			return 0, false
		}
		if key == "n" {
			if string(value) != "1" {
				return 0, false
			}
			continue
		}
		var wire string
		if json.Unmarshal(value, &wire) != nil {
			return 0, false
		}
		if key == "model" {
			if wire != model {
				return 0, false
			}
		}
	}
	if !seen["model"] || !seen["prompt"] {
		return 0, false
	}
	if !strings.EqualFold(parsed.Size, "1k") && !strings.EqualFold(parsed.Size, "2k") && seen["size"] {
		return 0, false
	}
	if parsed.ResponseFormat != "url" && parsed.ResponseFormat != "b64_json" {
		return 0, false
	}
	return pool, true
}

func (h *OpenAIGatewayHandler) chooseGrokPrimary(ctx context.Context, pool int64, model string) (*service.AccountSelectionResult, service.OpenAIAccountScheduleDecision, error) {
	// No client sticky state enters or is written to the donor pool. Only an
	// immediately admitted candidate is accepted; lack of capacity keeps Origin.
	skip := map[int64]struct{}{}
	var decision service.OpenAIAccountScheduleDecision
	for attempts := 0; attempts < 3; attempts++ {
		selected, d, err := h.gatewayService.SelectAccountWithSchedulerForCapability(
			service.WithOpenAIProfitControlSuppressed(ctx), &pool, "", "", model, skip,
			service.OpenAIUpstreamTransportHTTPSSE, service.OpenAIEndpointCapabilityGrokMediaGeneration,
			false, false, false, service.PlatformGrok,
		)
		decision = d
		if err != nil {
			if errors.Is(err, service.ErrNoAvailableAccounts) {
				return nil, d, nil
			}
			return nil, d, err
		}
		if selected == nil || selected.Account == nil || !selected.Acquired {
			return nil, d, nil
		}
		release := selected.ReleaseFunc
		if release == nil {
			release = func() {}
		}
		selected.ReleaseFunc = sync.OnceFunc(release)
		// Protect the lease while probing, then transfer ownership to the paid
		// forwarder, whose upstream deliberately outlives the client's socket.
		stopCancellation := context.AfterFunc(ctx, selected.ReleaseFunc)
		account := selected.Account
		bound := slices.Contains(account.GroupIDs, pool) || slices.ContainsFunc(account.AccountGroups, func(g service.AccountGroup) bool { return g.GroupID == pool })
		target, parseErr := url.Parse(account.GetGrokMediaBaseURL())
		if bound && account.IsGrokOAuth() && parseErr == nil && target.Scheme == "https" && target.User == nil && target.Hostname() != "" && xai.IsOfficialBaseURLHost(target.Hostname()) && account.GetMappedModel(model) == model {
			eligible, _, _ := h.ensureGrokMediaAccountEligibility(ctx, account)
			if eligible && stopCancellation() && ctx.Err() == nil {
				return selected, d, nil
			}
		}
		stopCancellation()
		selected.ReleaseFunc()
		if ctx.Err() != nil {
			return nil, d, ctx.Err()
		}
		skip[account.ID] = struct{}{}
	}
	return nil, decision, nil
}

func personalGrokCredentialCanFallback(err error) bool {
	var failure *service.UpstreamFailoverError
	if !errors.As(err, &failure) || failure.Stage != service.GatewayFailureStageAccountAuth {
		return false
	}
	return failure.ShouldRetryNextAccount() || (failure.Scope == service.GatewayFailureScopeProvider && (failure.Reason == service.GrokCredentialReasonProviderConfig || failure.Reason == service.GrokCredentialReasonProviderDown))
}

func personalImagePolicy() string { return os.Getenv("SUB2API_PERSONAL_GROK_IMAGE_POOL") }

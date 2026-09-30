package commandcode

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const BillingEndpoint = "https://api.commandcode.ai/alpha/billing/credits"
const CreditExhaustedCode = "commandcode_credit_exhausted"
const maxBillingBody = 1 << 20

// Only the official structured rejection establishes exhausted credit. A
// rounded balance, generic 400, or text echoed from a request does not.
func IsCreditExhausted(status int, body []byte) bool {
	if status != http.StatusBadRequest || len(body) > maxBillingBody {
		return false
	}
	var payload struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return false
	}
	return payload.Error.Code == CreditExhaustedCode ||
		(payload.Error.Code == "BAD_REQUEST" && strings.HasPrefix(strings.ToLower(strings.TrimSpace(payload.Error.Message)),
			"you have insufficient credits to make this request."))
}

type BillingCredits struct {
	Credits *struct {
		MonthlyCredits *float64 `json:"monthlyCredits"`
	} `json:"credits"`
	WindowLimits *struct {
		FiveHour *BillingWindow `json:"fiveHour"`
		Weekly   *BillingWindow `json:"weekly"`
		Limited  *bool          `json:"limited"`
	} `json:"windowLimits"`
	FetchedAt time.Time `json:"fetchedAt"`
}

type BillingWindow struct {
	Cap              *float64   `json:"cap"`
	Used             *float64   `json:"used"`
	Limit            *float64   `json:"limit"`
	Remaining        *float64   `json:"remaining"`
	RemainingPercent *float64   `json:"remainingPercent"`
	ResetAt          *ResetTime `json:"resetAt"`
}

// ResetTime accepts ISO timestamps and epoch seconds/milliseconds, and emits
// only an ISO timestamp. Unexpected upstream strings never reach the UI.
type ResetTime struct{ time.Time }

func (r *ResetTime) UnmarshalJSON(data []byte) error {
	var text string
	if json.Unmarshal(data, &text) == nil {
		value, err := time.Parse(time.RFC3339Nano, text)
		if err != nil {
			return ErrProtocol
		}
		r.Time = value.UTC()
		return nil
	}
	value, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil || value <= 0 || value > 253402300799000 {
		return ErrProtocol
	}
	if value >= 1e12 {
		r.Time = time.UnixMilli(value).UTC()
	} else {
		r.Time = time.Unix(value, 0).UTC()
	}
	if r.Year() > 9999 {
		return ErrProtocol
	}
	return nil
}

func (c *Client) BillingCredits(ctx context.Context, key string) (*BillingCredits, error) {
	if c == nil || c.HTTPClient == nil || !validCredential(key) {
		return nil, ErrRequest
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, BillingEndpoint, nil)
	if err != nil {
		return nil, ErrRequest
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Accept", "application/json")
	c.applyCLIHeaders(req)
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, ErrTransport
	}
	if resp == nil || resp.Body == nil {
		return nil, ErrUpstream
	}
	defer resp.Body.Close()
	if resp.Request != nil && (resp.Request.URL == nil || resp.Request.URL.String() != BillingEndpoint) {
		return nil, ErrUpstream
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Command Code billing returned HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBillingBody+1))
	var result BillingCredits
	if err != nil || len(data) > maxBillingBody || json.Unmarshal(data, &result) != nil || (result.Credits == nil && result.WindowLimits == nil) {
		return nil, ErrProtocol
	}
	result.FetchedAt = time.Now().UTC()
	return &result, nil
}

func (c *Client) applyCLIHeaders(req *http.Request) {
	req.Header.Set("x-cli-environment", "production")
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	if c.Version != "" {
		req.Header.Set("x-command-code-version", c.Version)
	}
}

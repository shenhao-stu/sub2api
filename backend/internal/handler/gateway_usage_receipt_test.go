package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type receiptRepository struct {
	service.UsageLogRepository
	key    int64
	id     string
	result *service.UsageReceipt
	err    error
}

func (r *receiptRepository) GetUsageReceipt(_ context.Context, key int64, id string) (*service.UsageReceipt, error) {
	r.key, r.id = key, id
	if r.result == nil {
		return nil, r.err
	}
	out := *r.result
	return &out, r.err
}

func TestKeyUsageReceiptUsesAuthenticatedKeyAndOmitsPrivateFields(t *testing.T) {
	const id = "2c4e888b-836b-4992-9ff1-dc1699488902"
	repo := &receiptRepository{result: &service.UsageReceipt{Model: "model", InputTokens: 10, OutputTokens: 64000, RecordedAt: time.Now()}}
	h := &GatewayHandler{usageService: service.NewUsageService(repo, nil, nil, nil)}
	c, w := newKeyBillingContext(&service.APIKey{ID: 11, Key: "synthetic-private-key", UserID: 7})
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/sub2api/usage/"+id+"?api_key_id=22&user_id=99", nil)
	c.Params = gin.Params{{Key: "request_id", Value: id}}
	h.KeyUsageReceipt(c)
	if w.Code != 200 || repo.key != 11 || repo.id != id {
		t.Fatalf("wrong receipt scope: %d %d %q", w.Code, repo.key, repo.id)
	}
	var got service.UsageReceipt
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.OutputTokens != 64000 || got.RequestID != id || got.Version != 1 || got.Object != "sub2api.usage_receipt" {
		t.Fatalf("recorded quantities changed: %+v", got)
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("receipt must not be shared-cached")
	}
	for _, private := range []string{"synthetic-private-key", "api_key_id", "user_id", "account_id", "actual_cost", "rate_multiplier"} {
		if strings.Contains(w.Body.String(), private) {
			t.Fatalf("unexpected private field %s", private)
		}
	}
}

func TestKeyUsageReceiptUnknownIsNotZeroAndErrorsAreSafe(t *testing.T) {
	const id = "2c4e888b-836b-4992-9ff1-dc1699488902"
	for _, tc := range []struct {
		name   string
		key    *service.APIKey
		id     string
		err    error
		status int
	}{
		{"unauthenticated", nil, id, nil, 401},
		{"invalid_id", &service.APIKey{ID: 11}, "../../messages", nil, 400},
		{"not_found", &service.APIKey{ID: 11}, id, service.ErrUsageLogNotFound, 404},
		{"storage_error", &service.APIKey{ID: 11}, id, errors.New("database password sensitive"), 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &receiptRepository{err: tc.err}
			h := &GatewayHandler{usageService: service.NewUsageService(repo, nil, nil, nil)}
			c, w := newKeyBillingContext(tc.key)
			c.Params = gin.Params{{Key: "request_id", Value: tc.id}}
			h.KeyUsageReceipt(c)
			if w.Code != tc.status {
				t.Fatalf("status=%d want %d", w.Code, tc.status)
			}
			if strings.Contains(w.Body.String(), "output_tokens") || strings.Contains(w.Body.String(), "password") {
				t.Fatal("unknown usage became a zero receipt or raw error leaked")
			}
			if (tc.key == nil || tc.name == "invalid_id") && repo.key != 0 {
				t.Fatal("unauthenticated/invalid request reached storage")
			}
		})
	}
}

func TestKeyUsageReceiptPendingIsAcceptedWithoutMeter(t *testing.T) {
	const id = "2c4e888b-836b-4992-9ff1-dc1699488902"
	repo := &receiptRepository{err: service.ErrUsageReceiptPending}
	h := &GatewayHandler{usageService: service.NewUsageService(repo, nil, nil, nil)}
	c, w := newKeyBillingContext(&service.APIKey{ID: 11, Key: "synthetic-private-key", UserID: 7})
	c.Params = gin.Params{{Key: "request_id", Value: id}}
	h.KeyUsageReceipt(c)
	if w.Code != http.StatusAccepted {
		t.Fatalf("pending receipt status=%d", w.Code)
	}
	if w.Header().Get("Retry-After") != "60" {
		t.Fatalf("pending receipt retry hint=%q", w.Header().Get("Retry-After"))
	}
	var got service.PendingUsageReceipt
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Object != "sub2api.usage_receipt" || got.Version != 2 || got.RequestID != id || got.State != "pending" {
		t.Fatalf("unsafe or incomplete pending receipt: %+v", got)
	}
	if strings.Contains(w.Body.String(), "output_tokens") || strings.Contains(w.Body.String(), "synthetic-private-key") {
		t.Fatal("pending receipt exposed a meter or credential")
	}
}

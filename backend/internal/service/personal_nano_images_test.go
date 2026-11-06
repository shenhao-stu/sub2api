//go:build unit

package service

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPrivateNanoInputsStayOnOriginalProvider(t *testing.T) {
	suffixes := []struct {
		suffix string
		native bool
	}{
		{"", true}, {`,"n":1,"quality":"auto","response_format":"b64_json","stream":false`, true}, {`,"size":"1024x1024"`, true}, {`,"aspect_ratio":"1:1","image_size":"1K"`, true},
		{`,"background":"transparent"`, false}, {`,"output_format":"webp"`, false}, {`,"size":"2048x2048"`, false}, {`,"image_size":"4K"`, false}, {`,"aspect_ratio":"3:2"`, false}, {`,"size":"1024x1024","image_size":"2K"`, false}, {`,"quality":"high"`, false}, {`,"quality":null`, false}, {`,"prompt":"duplicate"`, false}, {`,"response_format":"url"`, false}, {`,"n":"1"`, false}, {`,"stream":"false"`, false}, {`,"stream":true`, false}, {`,"n":1.0`, false}, {`,"n":{}`, false}, {`,"MODEL":"different"`, false}, {`,"image":"https://example.invalid/file"`, false},
	}
	for i, tc := range suffixes {
		body := []byte(`{"model":"nano-banana-2","prompt":"fixture"` + tc.suffix + `}`)
		require.Equal(t, tc.native, nanoSimpleGeneration(body, ""), "case %d", i)
	}
	require.True(t, nanoSimpleGeneration([]byte(`{"model":"nano-banana-2","prompt":"x","size":"2048x2048"}`), "1K,2K"))
	require.False(t, nanoSimpleGeneration([]byte(`{"model":"nano-banana-2","prompt":"x"} {}`), "1K"))
}

func TestPrivateNanoPeerAndProof(t *testing.T) {
	for _, address := range []string{"http://127.0.0.1:4001", "http://192.168.12.2:41", "https://10.0.0.2:4343"} {
		_, ok := pinnedNanoPeer(address)
		require.True(t, ok)
	}
	for _, address := range []string{"http://example.invalid:80", "https://1.1.1.1:443", "http://127.0.0.1:4/v1", "http://127.0.0.1:4?secret=1", "http://u@127.0.0.1:44", "http://127.0.0.1:0", "http://127.0.0.1:4/#x", "http://2130706433:80"} {
		_, ok := pinnedNanoPeer(address)
		require.False(t, ok, address)
	}
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	const id = "tenant-logical-id"
	r := &PersonalNanoImageRoute{PoolID: 555, peer: "http://127.0.0.1:4521", verify: pub, logicalID: id}
	a := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Extra: map[string]any{"personal_nano_image_adapter": true}, GroupIDs: []int64{555}, Credentials: map[string]any{"base_url": r.peer, "model_mapping": map[string]any{personalNanoModel: personalNanoModel}}}
	require.False(t, nanoSchedulerEligible(context.Background(), a))
	require.True(t, nanoSchedulerEligible(r.Context(context.Background()), a))
	require.False(t, nanoSchedulerEligible(r.Context(context.Background()), &Account{Platform: PlatformOpenAI}))
	req, err := r.outgoing(context.Background(), a, "fixture-key", []byte(`{}`))
	require.NoError(t, err)
	require.Nil(t, req.GetBody)
	require.Empty(t, req.Header.Values("Idempotency-Key"))
	req2, err := r.outgoing(context.Background(), a, "fixture-key", nil)
	require.NoError(t, err)
	require.NotEqual(t, req.Header.Get("X-Nano-Attempt"), req2.Header.Get("X-Nano-Attempt"))
	res := httptest.NewRecorder()
	res.WriteHeader(429)
	resp := res.Result()
	body := []byte(`{"error":{"code":"adapter_busy","type":"image_request_error","message":"full"}}`)
	canonical := fmt.Sprintf("nano-admission-v1\n429\nPOST\n/v1/images/generations\n%s\n%s\nadapter_busy", id, req.Header.Get("X-Nano-Attempt"))
	proof := base64.StdEncoding.EncodeToString(ed25519.Sign(priv, []byte(canonical)))
	resp.Header.Set("X-Nano-Admission-Proof", proof)
	require.True(t, r.declined(req, resp, body))
	require.False(t, r.declined(req2, resp, body))
	require.False(t, r.declined(req, resp, []byte(`{"error":{"code":"other"}}`)))
	require.False(t, r.declined(req, resp, []byte(`{"error":{"code":"adapter_busy"},"usage":{"total_tokens":1}}`)))
	resp.Header.Add("X-Nano-Admission-Proof", proof)
	require.False(t, r.declined(req, resp, body))
	resp.StatusCode = http.StatusServiceUnavailable
	require.False(t, r.declined(req, resp, body))
}

func TestPersonalImageIntentScopeAndRetention(t *testing.T) {
	t.Setenv("SUB2API_PERSONAL_NANO_IMAGE_POOL", "71/72")
	id := int64(71)
	key := &APIKey{ID: 86, UserID: 85, GroupID: &id, Group: &Group{ID: id, Platform: PlatformOpenAI}}
	headers := http.Header{"Idempotency-Key": {"client-opaque-key"}}
	first, err := PersonalImageIntent(context.Background(), key, personalNanoModel, headers)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Len(t, first.IdempotencyKeyHash, 64)
	require.NotContains(t, first.IdempotencyKeyHash, "client")
	require.Equal(t, IdempotencyStatusSucceeded, first.Status)
	require.Nil(t, first.ResponseBody)
	require.WithinDuration(t, time.Now().Add(30*24*time.Hour), first.ExpiresAt, time.Minute)
	duplicate, err := PersonalImageIntent(context.Background(), key, personalNanoModel, headers)
	require.NoError(t, err)
	require.Equal(t, first.IdempotencyKeyHash, duplicate.IdempotencyKeyHash)
	another, err := PersonalImageIntent(WithAccountAffinityTags(context.Background(), []string{"trusted-tenant"}), key, personalNanoModel, headers)
	require.NoError(t, err)
	require.NotEqual(t, first.IdempotencyKeyHash, another.IdempotencyKeyHash)
	key.ID++
	another, err = PersonalImageIntent(context.Background(), key, personalNanoModel, headers)
	require.NoError(t, err)
	require.NotEqual(t, first.IdempotencyKeyHash, another.IdempotencyKeyHash)
	key.ID = 0
	_, err = PersonalImageIntent(context.Background(), key, personalNanoModel, headers)
	require.Error(t, err)
	other, err := PersonalImageIntent(context.Background(), key, "unrelated-model", headers)
	require.NoError(t, err)
	require.Nil(t, other)
}

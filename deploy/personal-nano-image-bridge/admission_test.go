package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func admissionReq(b *bridge, peer, id, attempt string, edit bool) *httptest.ResponseRecorder {
	path := gen
	if edit {
		path = "/v1/images/edits"
	}
	r := httptest.NewRequest("POST", path, strings.NewReader(minimal))
	r.RemoteAddr = peer
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer pro-key")
	r.Header.Set("X-Request-ID", id)
	r.Header.Set("X-Nano-Attempt", attempt)
	r.Header.Set("X-Forwarded-For", "172.22.0.232")
	r.Header.Set("X-Client-Request-ID", "ignored")
	r.Header.Set("Idempotency-Key", "also-ignored")
	w := httptest.NewRecorder()
	b.ServeHTTP(w, r)
	return w
}
func TestOnlyFreshUndispatchedPeerReceivesAdmissionProof(t *testing.T) {
	pub, priv, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	const peer = "172.22.0.232:5567"
	const id = "logical-customer-key-1"
	const attempt = "00112233445566778899aabbccddeeff"
	var calls atomic.Int32
	b := mockBridge(t, func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); w.WriteHeader(429) })
	b.admission = admission{priv, map[netip.Addr]bool{netip.MustParseAddr("172.22.0.232"): true}}
	b.slots <- struct{}{}
	b.slots <- struct{}{}
	for _, invalid := range []struct {
		peer, id, c string
		status      int
	}{{"172.22.0.233:5567", id, attempt, 403}, {peer, id, "deadbeef", 400}, {peer, "", attempt, 400}} {
		w := admissionReq(b, invalid.peer, invalid.id, invalid.c, false)
		if w.Code != invalid.status || w.Header().Get(proofHeader) != "" {
			t.Fatal("invalid offered proof", w.Code)
		}
	}
	w := admissionReq(b, peer, id, attempt, false)
	if w.Code != 429 || w.Header().Get("X-Request-ID") != id {
		t.Fatal(w.Code)
	}
	sig, e := base64.StdEncoding.DecodeString(w.Header().Get(proofHeader))
	wire := "nano-admission-v1\n429\nPOST\n/v1/images/generations\nlogical-customer-key-1\n00112233445566778899aabbccddeeff\nadapter_busy"
	if e != nil || !ed25519.Verify(pub, []byte(wire), sig) || ed25519.Verify(pub, []byte(wire+"\n"), sig) {
		t.Fatal("signature mismatch")
	}
	if w = admissionReq(b, peer, id, attempt, true); w.Code != 409 || w.Header().Get(proofHeader) != "" {
		t.Fatal("duplicate signed", w.Code)
	}
	<-b.slots
	<-b.slots
	if w = admissionReq(b, peer, id, attempt, false); w.Code != 409 || calls.Load() != 0 {
		t.Fatal("previous negative claim dispatched")
	}
	if w = admissionReq(b, peer, "new", attempt, false); w.Code != 429 || w.Header().Get(proofHeader) != "" || calls.Load() != 1 {
		t.Fatal("native 429 signed")
	}
	b.slots <- struct{}{}
	b.slots <- struct{}{}
	if w = admissionReq(b, peer, "new", attempt, false); w.Code != 409 || w.Header().Get(proofHeader) != "" {
		t.Fatal("prior paid failure signed")
	}
}
func TestConcurrentSaturationAndClaimCanNeverBothWin(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	b := mockBridge(t, func(w http.ResponseWriter, _ *http.Request) { t.Error("unexpected call") })
	b.admission = admission{priv, map[netip.Addr]bool{netip.MustParseAddr("172.22.0.232"): true}}
	b.slots <- struct{}{}
	b.slots <- struct{}{}
	const id = "logical"
	var wg sync.WaitGroup
	var accepted, proof atomic.Int32
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				if claim(b.stateDir, id) == nil {
					accepted.Add(1)
				}
				return
			}
			w := admissionReq(b, "172.22.0.232:1", id, "0123456789abcdef0123456789abcdef", false)
			if w.Header().Get(proofHeader) != "" {
				proof.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if accepted.Load()+proof.Load() != 1 {
		t.Fatal(accepted.Load(), proof.Load())
	}
}
func TestAdmissionConfigRejectsBroadPeers(t *testing.T) {
	t.Setenv("NANO_ADMISSION_SIGNER", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	for _, s := range []string{"", "172.22.0.0/16", "0.0.0.0", "172.22.0.232,8.8.8.8", "localhost"} {
		t.Setenv("NANO_ADMISSION_PEERS", s)
		if _, e := admissionFromEnv(); e == nil {
			t.Fatal(s)
		}
	}
	t.Setenv("NANO_ADMISSION_PEERS", "172.22.0.232/32,127.0.0.1")
	a, e := admissionFromEnv()
	if e != nil || !a.allowsPeer("172.22.0.232:1") || a.allowsPeer("172.22.0.233:1") {
		t.Fatal("peer policy")
	}
}

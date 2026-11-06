package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var safeID = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

type route struct{ key, credential, model string }
type bridge struct {
	upstream, stateDir string
	routes             []route
	slots              chan struct{}
	client             *http.Client
	admission          admission
}

func jsonReply(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func fail(w http.ResponseWriter, status int, code, message string) {
	jsonReply(w, status, obj{"error": obj{"code": code, "message": message, "type": "image_request_error"}})
}

// An unsuccessful image may still have metered native tokens. Expose only measured
// fields, never a provider error body that may contain prompts or credentials.
func providerFailure(w http.ResponseWriter, status int, code, message string, raw []byte) {
	var meta struct{ UsageMetadata map[string]json.RawMessage }
	if json.Unmarshal(raw, &meta) != nil {
		meta.UsageMetadata = nil
	}
	out := usageFields(meta.UsageMetadata)
	out["error"] = obj{"code": code, "message": message, "type": "image_request_error"}
	jsonReply(w, status, out)
}

func (b *bridge) findRoute(auth string) *route {
	token, ok := strings.CutPrefix(auth, "Bearer ")
	if !ok || token == "" {
		return nil
	}
	h := sha256.Sum256([]byte(token))
	for i, r := range b.routes {
		want := sha256.Sum256([]byte(r.key))
		if subtle.ConstantTimeCompare(want[:], h[:]) == 1 {
			return &b.routes[i]
		}
	}
	return nil
}

func oneShotClient() *http.Client {
	// A fresh connection + no GetBody or idempotency headers also prevents transport retries.
	return &http.Client{Timeout: 170 * time.Second, Transport: &http.Transport{DisableKeepAlives: true, DisableCompression: true, MaxResponseHeaderBytes: 64 << 10, DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext}, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
}

// Remember caller-provided request IDs even across process restarts. Empty markers contain no payload or key.
func claim(dir, id string) error {
	digest := sha256.Sum256([]byte(id))
	f, err := os.OpenFile(filepath.Join(dir, hex.EncodeToString(digest[:])), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func claimOrReject(w http.ResponseWriter, dir, id string) bool {
	if err := claim(dir, id); err != nil {
		if errors.Is(err, os.ErrExist) {
			fail(w, 409, "already_dispatched", "This request ID already has a terminal admission or dispatch claim; reconcile the original result")
		} else {
			fail(w, 503, "claim_failed", "Cannot reserve request ID; no dispatch")
		}
		return false
	}
	return true
}

func (b *bridge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !b.admission.allowsPeer(r.RemoteAddr) {
		fail(w, 403, "invalid_peer", "Private peer not permitted")
		return
	}
	if len(r.Header.Values("Authorization")) != 1 {
		fail(w, 401, "invalid_api_key", "Bearer authentication required")
		return
	}
	pool := b.findRoute(r.Header.Get("Authorization"))
	if pool == nil {
		fail(w, 401, "invalid_api_key", "Bearer authentication required")
		return
	}
	if r.URL.RawQuery != "" {
		fail(w, 400, "unsupported_query", "Query parameters are unsupported")
		return
	}
	if r.URL.Path == "/v1/models" && r.Method == "GET" {
		jsonReply(w, 200, obj{"object": "list", "data": []obj{{"object": "model", "id": publicModel, "owned_by": "google"}}})
		return
	}
	edit := r.URL.Path == "/v1/images/edits"
	if r.Method != "POST" || (!edit && r.URL.Path != "/v1/images/generations") {
		fail(w, 404, "unknown_endpoint", "Unknown endpoint")
		return
	}
	supplied, attempt, idErr := requestIdentity(r.Header)
	if idErr != nil {
		fail(w, 400, "invalid_request_id", idErr.Error())
		return
	}
	if attempt != "" && len(b.admission.signer) == 0 {
		fail(w, 503, "proof_unavailable", "Admission verification is not configured; no dispatch")
		return
	}
	select {
	case b.slots <- struct{}{}:
		defer func() { <-b.slots }()
	default:
		busy(w, b, r.URL.Path, supplied, attempt)
		return
	}
	if r.Header.Get("Content-Encoding") != "" {
		fail(w, 400, "invalid_encoding", "Compressed requests are unsupported")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequest)
	f, imgs, err := readRequest(r, edit)
	if err != nil {
		fail(w, 400, "invalid_request", err.Error())
		return
	}
	payload, err := buildNative(f, imgs)
	if err != nil {
		fail(w, 400, "invalid_request", err.Error())
		return
	}
	body, err := json.Marshal(payload)
	if err != nil {
		fail(w, 500, "encoding_failed", "Cannot encode request")
		return
	}
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		fail(w, 500, "id_failed", "Could not allocate request ID")
		return
	}
	id := hex.EncodeToString(nonce[:])
	w.Header().Set("X-Request-Id", id)
	// Never let disconnects trigger replay. The HTTP client's hard deadline still bounds each dispatch.
	ctx, cancel := context.WithTimeout(context.Background(), 170*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", b.upstream+"/v1beta/models/"+pool.model+":generateContent", bytes.NewReader(body))
	if err != nil {
		fail(w, 500, "invalid_upstream", "Invalid configured upstream")
		return
	}
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+pool.credential)
	req.Header.Set("X-Request-ID", id)
	if supplied != "" && !claimOrReject(w, b.stateDir, supplied) {
		return
	}
	resp, err := b.client.Do(req)
	if err != nil {
		fail(w, 502, "unknown_upstream_outcome", "CPA outcome is uncertain; do not retry")
		return
	}
	defer resp.Body.Close()
	if trace := resp.Header.Get("X-Cpa-Trace-Id"); safeID.MatchString(trace) {
		w.Header().Set("X-Cpa-Trace-Id", trace)
	}
	if resp.StatusCode != 200 {
		// Hide provider error bodies (they may echo user input or credential details).
		status := resp.StatusCode
		if status < 400 || status > 599 {
			status = 502
		}
		errorBody, _ := readLimited(resp.Body, 1<<20)
		providerFailure(w, status, "upstream_rejected", fmt.Sprintf("CPA returned HTTP %d; no retry was issued", resp.StatusCode), errorBody)
		return
	}
	raw, err := readLimited(resp.Body, maxResponse)
	if err != nil {
		fail(w, 502, "unknown_upstream_outcome", "CPA response incomplete or oversized; do not retry")
		return
	}
	result, err := imageResult(raw)
	if err != nil {
		providerFailure(w, 502, "invalid_provider_result", err.Error()+"; do not retry", raw)
		return
	}
	result["created"] = time.Now().Unix()
	jsonReply(w, 200, result)
}

// Use a literal private IPv4 address for the trusted upstream and bind address. Client inputs never select a host.
func internalAddress(addr string) bool {
	ip, err := netip.ParseAddrPort(addr)
	return err == nil && ip.Port() != 0 && ip.Addr().Is4() && (ip.Addr().IsPrivate() || ip.Addr().IsLoopback())
}
func envDefault(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func main() {
	if len(os.Args) > 1 {
		if len(os.Args) != 2 || os.Args[1] != "admission-public-key" {
			log.Fatal("Unknown command")
		}
		signer, err := signerFromEnv()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(base64.StdEncoding.EncodeToString(signer.Public().(ed25519.PublicKey)))
		return
	}
	admissionConfig, err := admissionFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	bind := envDefault("NANO_BIND", "127.0.0.1:57300")
	up := envDefault("NANO_CPA_ADDR", "127.0.0.1:57240")
	if !internalAddress(bind) || !internalAddress(up) || bind == up {
		log.Fatal("Use distinct loopback/private numeric bind and CPA addresses")
	}
	state := os.Getenv("NANO_STATE_DIR")
	if !filepath.IsAbs(state) {
		log.Fatal("NANO_STATE_DIR must be an absolute private directory")
	}
	if err := os.MkdirAll(state, 0700); err != nil {
		log.Fatal("Cannot create state directory")
	}
	mode, err := os.Lstat(state)
	if err != nil || !mode.IsDir() || mode.Mode().Perm()&0077 != 0 {
		log.Fatal("State directory must be private to the service account")
	}
	b := bridge{upstream: "http://" + up, stateDir: state, slots: make(chan struct{}, 2), client: oneShotClient(), admission: admissionConfig}
	for _, pool := range []string{"PRO", "ULTRA"} {
		key, cpa := os.Getenv("NANO_"+pool+"_KEY"), os.Getenv("NANO_"+pool+"_CPA_KEY")
		if key == "" && cpa == "" {
			continue
		}
		if len(key) < 24 || len(cpa) < 8 || key == cpa || strings.ContainsAny(key+cpa, " \t\n\r") {
			log.Fatal("Invalid bridge/CPA credential configuration")
		}
		model := flashModel
		if pool == "ULTRA" {
			model = "ultra/" + model
		}
		b.routes = append(b.routes, route{key, cpa, model})
	}
	if len(b.routes) == 0 || (len(b.routes) == 2 && (b.routes[0].key == b.routes[1].key || b.routes[0].key == b.routes[1].credential || b.routes[1].key == b.routes[0].credential)) {
		log.Fatal("One or two distinct pool credentials are required")
	}
	server := http.Server{Addr: bind, Handler: &b, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 25 * time.Second, WriteTimeout: 210 * time.Second, IdleTimeout: 10 * time.Second, MaxHeaderBytes: 16 << 10, ErrorLog: log.New(io.Discard, "", 0)}
	log.Print("Nano image bridge listening on private address")
	log.Fatal(server.ListenAndServe())
}

package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"os"
	"regexp"
	"strings"
)

const attemptHeader = "X-Nano-Attempt"
const proofHeader = "X-Nano-Admission-Proof"

var challengePattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

type admission struct {
	signer ed25519.PrivateKey
	peers  map[netip.Addr]bool
}

func signerFromEnv() (ed25519.PrivateKey, error) {
	encoded := os.Getenv("NANO_ADMISSION_SIGNER")
	b, e := base64.StdEncoding.Strict().DecodeString(encoded)
	if e != nil || len(b) != ed25519.SeedSize || base64.StdEncoding.EncodeToString(b) != encoded {
		return nil, errors.New("invalid admission signer")
	}
	return ed25519.NewKeyFromSeed(b), nil
}
func admissionFromEnv() (admission, error) {
	encoded := os.Getenv("NANO_ADMISSION_SIGNER")
	peerCSV := os.Getenv("NANO_ADMISSION_PEERS")
	if encoded == "" && peerCSV == "" {
		return admission{}, nil
	}
	if encoded == "" || peerCSV == "" {
		return admission{}, errors.New("configure both admission signer and exact peers")
	}
	signer, err := signerFromEnv()
	if err != nil {
		return admission{}, err
	}
	peers := map[netip.Addr]bool{}
	for _, value := range strings.Split(peerCSV, ",") {
		// Explicit host addresses only. No broad subnet, external interface, hostname or wildcard.
		literal := strings.TrimSuffix(strings.TrimSpace(value), "/32")
		peer, err := netip.ParseAddr(literal)
		if err != nil || !peer.Is4() || !(peer.IsLoopback() || peer.IsPrivate()) {
			return admission{}, errors.New("invalid admission peer: use exact private IPv4 addresses")
		}
		peers[peer] = true
	}
	return admission{signer: signer, peers: peers}, nil
}
func (a admission) allowsPeer(peerAddr string) bool {
	if len(a.peers) == 0 {
		return len(a.signer) == 0
	}
	host, _, err := net.SplitHostPort(peerAddr)
	if err != nil {
		return false
	}
	peer, err := netip.ParseAddr(host)
	return err == nil && a.peers[peer.Unmap()]
}

// A challenge selects X-Request-ID exclusively; rewriteable client headers cannot choose the claim identity.
func requestIdentity(h http.Header) (id, attempt string, err error) {
	challenges := h.Values(attemptHeader)
	if len(challenges) > 0 {
		ids := h.Values("X-Request-ID")
		if len(challenges) != 1 || !challengePattern.MatchString(challenges[0]) || len(ids) != 1 || !safeID.MatchString(ids[0]) {
			return "", "", errors.New("invalid admission challenge or logical request ID")
		}
		return ids[0], challenges[0], nil
	}
	for _, name := range []string{"Idempotency-Key", "X-Client-Request-ID", "X-Request-ID"} {
		ids := h.Values(name)
		if len(ids) == 0 {
			continue
		}
		if len(ids) != 1 || !safeID.MatchString(ids[0]) {
			return "", "", errors.New("invalid request identifier")
		}
		if id == "" {
			id = ids[0]
		}
	}
	return id, "", nil
}

func busy(w http.ResponseWriter, b *bridge, path, id, attempt string) {
	if attempt != "" {
		// The negative claim competes atomically with every paid claim, including those for other pools.
		// A previous claim can never receive a signed decline. Existing/unknown outcomes stay terminal.
		if !claimOrReject(w, b.stateDir, id) {
			return
		}
		wire := strings.Join([]string{"nano-admission-v1", "429", "POST", path, id, attempt, "adapter_busy"}, "\n")
		signature := ed25519.Sign(b.admission.signer, []byte(wire))
		w.Header().Set(proofHeader, base64.StdEncoding.EncodeToString(signature))
		w.Header().Set("X-Request-ID", id)
	}
	fail(w, 429, "adapter_busy", "Image capacity is currently in use; no dispatch")
}

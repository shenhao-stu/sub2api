package service

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"

	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

const personalNanoModel = "nano-banana-2"
const privateNanoPath = "/v1/images/generations"

// The adapter signs a durable refusal before calling its provider. Only this
// error grants permission to attempt the original image provider.
var ErrPersonalNanoCapacity = errors.New("private image adapter declined before dispatch")

type nanoRouteKey struct{}
type PersonalNanoImageRoute struct {
	PoolID    int64
	peer      string
	verify    ed25519.PublicKey
	logicalID string
}

func PreparePersonalNanoImageRoute(key *APIKey, req *OpenAIImagesRequest, model, mapped string, body []byte, intent *IdempotencyRecord) *PersonalNanoImageRoute {
	if key == nil || key.Group == nil || key.GroupID == nil || *key.GroupID != key.Group.ID || key.Group.Platform != PlatformOpenAI || req == nil || req.Model != personalNanoModel || model != personalNanoModel || (mapped != "" && mapped != model) || req.IsEdits() || req.Stream || req.Multipart || req.N != 1 {
		return nil
	}
	pool, ok := nanoImagePool(key.Group.ID)
	if !ok {
		return nil
	}
	media, _, err := mime.ParseMediaType(req.ContentType)
	if err != nil || media != "application/json" || !nanoSimpleGeneration(body, os.Getenv("SUB2API_PERSONAL_NANO_IMAGE_TIERS")) {
		return nil
	}
	if intent == nil || intent.IdempotencyKeyHash == "" {
		return nil
	}
	peer, ok := pinnedNanoPeer(os.Getenv("SUB2API_PERSONAL_NANO_IMAGE_PEER"))
	if !ok {
		return nil
	}
	verification, err := base64.StdEncoding.DecodeString(os.Getenv("SUB2API_PERSONAL_NANO_ADMISSION_PUBLIC_KEY"))
	if err != nil || len(verification) != ed25519.PublicKeySize {
		return nil
	}

	return &PersonalNanoImageRoute{PoolID: pool, peer: peer, verify: verification, logicalID: intent.IdempotencyKeyHash}
}

// Only numeric private IPv4 or loopback addresses are permitted. No DNS,
// traffic proxy, redirects, userinfo or mutable URL path enters the connection.
func pinnedNanoPeer(config string) (string, bool) {
	u, err := url.Parse(config)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.ForceQuery || u.Path != "" || u.Port() == "" {
		return "", false
	}
	ip, err := netip.ParseAddr(u.Hostname())
	p, perr := strconv.ParseUint(u.Port(), 10, 16)
	if err != nil || !ip.Is4() || (!ip.IsPrivate() && !ip.IsLoopback()) || perr != nil || p == 0 {
		return "", false
	}
	canonical := u.Scheme + "://" + net.JoinHostPort(ip.String(), strconv.FormatUint(p, 10))
	return canonical, canonical == config
}

// Use the intersection of the public Images and private peer capabilities.
// Unsupported parameters, tiers, aliases and duplicate keys keep their original
// provider. Rejecting early inside the adapter would lose this fallback.
func nanoSimpleGeneration(body []byte, tiers string) bool {
	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	open, err := d.Token()
	if err != nil || open != json.Delim('{') {
		return false
	}
	fields := map[string]any{}
	for d.More() {
		token, err := d.Token()
		name, ok := token.(string)
		if err != nil || !ok {
			return false
		}
		if _, seen := fields[name]; seen {
			return false
		}
		var v any
		if d.Decode(&v) != nil {
			return false
		}
		switch name {
		case "model":
			if v != personalNanoModel {
				return false
			}
		case "prompt":
			s, ok := v.(string)
			if !ok || len(s) > 16<<10 || strings.TrimSpace(s) == "" {
				return false
			}
		case "n":
			if v != json.Number("1") {
				return false
			}
		case "stream":
			if v != false {
				return false
			}
		case "response_format":
			if v != "b64_json" {
				return false
			}
		case "quality":
			if v != "auto" {
				return false
			}
		case "aspect_ratio":
			if v != "1:1" {
				return false
			}
		case "image_size":
			s, ok := v.(string)
			if !ok || !slices.Contains([]string{"1K", "2K", "4K"}, s) {
				return false
			}
		case "size":
			s, ok := v.(string)
			if !ok || !slices.Contains([]string{"auto", "1024x1024", "2048x2048", "4096x4096"}, s) {
				return false
			}
		default:
			return false
		}
		fields[name] = v
	}
	if _, err = d.Token(); err != nil {
		return false
	}
	if _, err = d.Token(); err != io.EOF {
		return false
	}
	if fields["model"] == nil || fields["prompt"] == nil {
		return false
	}
	tier, _ := fields["image_size"].(string)
	size, _ := fields["size"].(string)
	sq := map[string]string{"1024x1024": "1K", "2048x2048": "2K", "4096x4096": "4K"}[size]
	if sq != "" && tier != "" && tier != sq {
		return false
	}
	if tier == "" {
		tier = sq
	}
	if tier == "" {
		tier = "1K"
	}
	if tiers == "" {
		tiers = "1K"
	}
	return slices.Contains(strings.Split(tiers, ","), tier)
}

func (r *PersonalNanoImageRoute) Context(ctx context.Context) context.Context {
	return context.WithValue(ctx, nanoRouteKey{}, r)
}
func nanoRouteFromContext(ctx context.Context) *PersonalNanoImageRoute {
	if ctx == nil {
		return nil
	}
	r, _ := ctx.Value(nanoRouteKey{}).(*PersonalNanoImageRoute)
	return r
}
func personalNanoAccount(a *Account) bool {
	if a == nil {
		return false
	}
	flag, _ := a.Extra["personal_nano_image_adapter"].(bool)
	return flag
}
func (r *PersonalNanoImageRoute) Allows(a *Account) bool {
	if r == nil || !personalNanoAccount(a) || a.Platform != PlatformOpenAI || a.Type != AccountTypeAPIKey || a.ProxyID != nil || a.GetMappedModel(personalNanoModel) != personalNanoModel {
		return false
	}
	base := strings.TrimSuffix(a.GetOpenAIBaseURL(), "/")
	if base != r.peer && base != r.peer+"/v1" {
		return false
	}
	return slices.Contains(a.GroupIDs, r.PoolID) || slices.ContainsFunc(a.AccountGroups, func(g AccountGroup) bool { return g.GroupID == r.PoolID })
}
func nanoSchedulerEligible(ctx context.Context, account *Account) bool {
	r := nanoRouteFromContext(ctx)
	if r != nil {
		return r.Allows(account)
	}
	return !personalNanoAccount(account)
}

// The pinned local peer is the sole exception for this transport. It does not
// alter the URL allowlist, proxy selection, plugins or TLS policy of any other
// account. Redirects and replay of paid POST requests are disabled.
var nanoClient = &http.Client{
	Transport:     &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext, IdleConnTimeout: 30 * time.Second, ResponseHeaderTimeout: 180 * time.Second},
	Timeout:       185 * time.Second,
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

func (r *PersonalNanoImageRoute) outgoing(ctx context.Context, account *Account, token string, body []byte) (*http.Request, error) {
	if !r.Allows(account) {
		return nil, errors.New("private image adapter is unavailable")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.peer+privateNanoPath, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	challenge := make([]byte, 16)
	if _, err = rand.Read(challenge); err != nil {
		return nil, err
	}
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Request-ID", r.logicalID)
	req.Header.Set("X-Nano-Attempt", hex.EncodeToString(challenge))
	return req, nil
}
func (r *PersonalNanoImageRoute) declined(req *http.Request, resp *http.Response, body []byte) bool {
	// Require the exact error envelope. Extra counters (including native
	// Gemini usage metadata) or altered headers make the refusal terminal.
	if resp.StatusCode != http.StatusTooManyRequests || !gjson.ValidBytes(body) || !imageRejectionHasNoGenerationEvidence(body) {
		return false
	}
	root := gjson.ParseBytes(body)
	info := root.Get("error")
	if len(root.Map()) != 1 || len(info.Map()) != 3 || info.Get("code").String() != "adapter_busy" || info.Get("type").String() != "image_request_error" || info.Get("message").Type != gjson.String {
		return false
	}
	if len(resp.Header.Values("X-Nano-Admission-Proof")) != 1 {
		return false
	}
	sig, err := base64.StdEncoding.DecodeString(resp.Header.Get("X-Nano-Admission-Proof"))
	if err != nil {
		return false
	}
	msg := fmt.Sprintf("nano-admission-v1\n429\nPOST\n%s\n%s\n%s\nadapter_busy", privateNanoPath, r.logicalID, req.Header.Get("X-Nano-Attempt"))
	return ed25519.Verify(r.verify, []byte(msg), sig)
}

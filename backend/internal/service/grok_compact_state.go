package service

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

// The subscription CLI endpoint does not expose /responses/compact and may
// omit encrypted reasoning on a summary-only turn. Seal the actual generated
// summary as gateway state so strict clients can retain just encrypted_content.
// This namespace is never forwarded to xAI as provider reasoning ciphertext.
const grokCompactStatePrefix = "sub2api.grok.compact.v1."
const grokCompactStateLimit = 1024 * 1024

func (s *OpenAIGatewayService) grokCompactStateCipher() (cipher.AEAD, error) {
	if s == nil || s.cfg == nil || len(s.cfg.JWT.Secret) < 32 {
		return nil, fmt.Errorf("grok compaction state key unavailable")
	}
	derive := hmac.New(sha256.New, []byte(s.cfg.JWT.Secret))
	_, _ = derive.Write([]byte(grokCompactStatePrefix))
	block, err := aes.NewCipher(derive.Sum(nil))
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func (s *OpenAIGatewayService) sealGrokCompactSummary(summary string) (string, error) {
	if strings.TrimSpace(summary) == "" || len(summary) > grokCompactStateLimit {
		return "", fmt.Errorf("invalid Grok compaction summary")
	}
	aead, err := s.grokCompactStateCipher()
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := aead.Seal(nonce, nonce, []byte(summary), []byte(grokCompactStatePrefix))
	return grokCompactStatePrefix + base64.RawURLEncoding.EncodeToString(sealed), nil
}

func (s *OpenAIGatewayService) openGrokCompactSummary(blob string) (string, error) {
	if !strings.HasPrefix(blob, grokCompactStatePrefix) || len(blob) > 2*grokCompactStateLimit {
		return "", fmt.Errorf("invalid Grok compaction state")
	}
	aead, err := s.grokCompactStateCipher()
	if err != nil {
		return "", err
	}
	sealed, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(blob, grokCompactStatePrefix))
	if err != nil || len(sealed) < aead.NonceSize()+aead.Overhead() {
		return "", fmt.Errorf("invalid Grok compaction state")
	}
	plain, err := aead.Open(nil, sealed[:aead.NonceSize()], sealed[aead.NonceSize():], []byte(grokCompactStatePrefix))
	if err != nil || len(plain) > grokCompactStateLimit || strings.TrimSpace(string(plain)) == "" {
		return "", fmt.Errorf("invalid Grok compaction state")
	}
	return string(plain), nil
}

func (s *OpenAIGatewayService) restoreGrokCompactState(body []byte) ([]byte, error) {
	if !strings.Contains(string(body), grokCompactStatePrefix) {
		return body, nil
	}
	var payload map[string]any
	if err := decodeOpenAIJSONUseNumber(body, &payload); err != nil {
		return nil, err
	}
	items, ok := payload["input"].([]any)
	if !ok {
		return body, nil
	}
	changed := false
	for i, raw := range items {
		item, ok := raw.(map[string]any)
		if !ok || !isOpenAICompactionType(stringValue(item["type"])) {
			continue
		}
		blob := stringValue(item["encrypted_content"])
		if !strings.HasPrefix(blob, grokCompactStatePrefix) {
			continue
		}
		summary, err := s.openGrokCompactSummary(blob)
		if err != nil {
			return nil, err
		}
		items[i] = map[string]any{"type": "message", "role": "user", "content": []any{
			map[string]any{"type": "input_text", "text": "<conversation_summary>\n" + summary + "\n</conversation_summary>"},
		}}
		changed = true
	}
	if !changed {
		return body, nil
	}
	return json.Marshal(payload)
}

func (s *OpenAIGatewayService) convertGrokCompactResponse(body []byte) ([]byte, error) {
	return convertGrokResponseToOpenAICompactWithSummary(body, s.sealGrokCompactSummary)
}

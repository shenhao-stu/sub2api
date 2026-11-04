package service

import (
	"strings"
	"testing"
	"unsafe"
)

func TestOpsQueueSanitizedPlaintextOwnsBoundedBackingStorage(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want string
	}{
		{"truncated", strings.Repeat("e", 1<<20), strings.Repeat("e", OpsErrorLogQueueBodyMaxBytes)},
		{"trimmed", strings.Repeat(" ", 1<<20) + "upstream unavailable", "upstream unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bounded, _ := SanitizeOpsErrorBodyForQueue(tc.raw)
			if bounded != tc.want {
				t.Fatalf("sanitized plaintext changed: got %d bytes, want %d", len(bounded), len(tc.want))
			}
			start := uintptr(unsafe.Pointer(unsafe.StringData(tc.raw)))
			retained := uintptr(unsafe.Pointer(unsafe.StringData(bounded)))
			if retained >= start && retained < start+uintptr(len(tc.raw)) {
				t.Fatalf("queued %d-byte string retains %d-byte input allocation", len(bounded), len(tc.raw))
			}
		})
	}
}

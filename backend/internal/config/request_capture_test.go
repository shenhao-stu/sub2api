//go:build unit

package config

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestCaptureEnvironmentIsDefaultOffAndRejectsInvalidScope(t *testing.T) {
	const prefix = "SUB2API_REQUEST_CAPTURE_"
	for _, name := range []string{"GROUP_IDS", "DIR", "MIN_FREE_BYTES", "MAX_CONCURRENT"} {
		t.Setenv(prefix+name, "")
	}
	cfg, err := LoadRequestCaptureFromEnv()
	require.NoError(t, err)
	require.Empty(t, cfg.GroupIDs)
	require.NoError(t, cfg.Validate(0))
	for _, test := range []struct{ groups, dir, free, concurrent string }{
		{"42", "", "", ""}, {"", "/private/capture", "", ""},
		{"*", "/private/capture", "", ""}, {"0", "/private/capture", "", ""},
		{"42,,43", "/private/capture", "", ""}, {"42,42", "/private/capture", "", ""},
		{"42", "relative", "", ""}, {"42", "/", "", ""},
		{"42", "/private/capture", "-1", ""}, {"42", "/private/capture", "0", ""},
		{"42", "/private/capture", "", "0"}, {"42", "/private/capture", "", "17"},
	} {
		t.Setenv(prefix+"GROUP_IDS", test.groups)
		t.Setenv(prefix+"DIR", test.dir)
		t.Setenv(prefix+"MIN_FREE_BYTES", test.free)
		t.Setenv(prefix+"MAX_CONCURRENT", test.concurrent)
		_, err := LoadRequestCaptureFromEnv()
		require.Error(t, err)
	}
	t.Setenv(prefix+"GROUP_IDS", "42, 43")
	t.Setenv(prefix+"DIR", filepath.Join(t.TempDir(), "capture"))
	t.Setenv(prefix+"MIN_FREE_BYTES", "21474836480")
	t.Setenv(prefix+"MAX_CONCURRENT", "16")
	cfg, err = LoadRequestCaptureFromEnv()
	require.NoError(t, err)
	require.Equal(t, []int64{42, 43}, cfg.GroupIDs)
	require.Equal(t, uint64(20<<30), cfg.MinFreeBytes)
	require.Equal(t, 16, cfg.MaxConcurrent)
	require.Error(t, cfg.Validate(0))
	require.NoError(t, cfg.Validate(256<<20))
	cfg.Directory = "/private/\x00capture"
	require.Error(t, cfg.Validate(256<<20))
}

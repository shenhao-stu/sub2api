package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// RequestCaptureConfig never accepts client headers or public configuration.
type RequestCaptureConfig struct {
	GroupIDs      []int64
	Directory     string
	MinFreeBytes  uint64
	MaxConcurrent int
}

func LoadRequestCaptureFromEnv() (RequestCaptureConfig, error) {
	const prefix = "SUB2API_REQUEST_CAPTURE_"
	groups := strings.TrimSpace(os.Getenv(prefix + "GROUP_IDS"))
	dir := strings.TrimSpace(os.Getenv(prefix + "DIR"))
	free := strings.TrimSpace(os.Getenv(prefix + "MIN_FREE_BYTES"))
	concurrent := strings.TrimSpace(os.Getenv(prefix + "MAX_CONCURRENT"))
	if groups == "" && dir == "" && free == "" && concurrent == "" {
		return RequestCaptureConfig{}, nil
	}
	cfg := RequestCaptureConfig{Directory: dir, MinFreeBytes: 5 << 30, MaxConcurrent: 2}
	if groups == "" || dir == "" {
		return RequestCaptureConfig{}, fmt.Errorf("request capture requires GROUP_IDS and DIR together")
	}
	for _, value := range strings.Split(groups, ",") {
		id, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil || id <= 0 {
			return RequestCaptureConfig{}, fmt.Errorf("request capture GROUP_IDS must contain positive integer IDs")
		}
		cfg.GroupIDs = append(cfg.GroupIDs, id)
	}
	if free != "" {
		value, err := strconv.ParseUint(free, 10, 64)
		if err != nil || value == 0 {
			return RequestCaptureConfig{}, fmt.Errorf("request capture MIN_FREE_BYTES must be a positive integer")
		}
		cfg.MinFreeBytes = value
	}
	if concurrent != "" {
		value, err := strconv.Atoi(concurrent)
		if err != nil {
			return RequestCaptureConfig{}, fmt.Errorf("request capture MAX_CONCURRENT must be between 1 and 16")
		}
		cfg.MaxConcurrent = value
	}
	// The effective body limit is validated after the normal server defaults load.
	if err := cfg.Validate(1); err != nil {
		return RequestCaptureConfig{}, err
	}
	return cfg, nil
}

func (c RequestCaptureConfig) Validate(bodyLimit int64) error {
	if len(c.GroupIDs) == 0 && c.Directory == "" && c.MinFreeBytes == 0 && c.MaxConcurrent == 0 {
		return nil
	}
	if len(c.GroupIDs) == 0 || !filepath.IsAbs(c.Directory) || strings.ContainsRune(c.Directory, 0) || filepath.Clean(c.Directory) == string(filepath.Separator) {
		return fmt.Errorf("request capture requires explicit GROUP_IDS and an absolute non-root DIR")
	}
	seen := make(map[int64]bool, len(c.GroupIDs))
	for _, id := range c.GroupIDs {
		if id <= 0 || seen[id] {
			return fmt.Errorf("request capture GROUP_IDS must be unique positive IDs")
		}
		seen[id] = true
	}
	if c.MaxConcurrent < 1 || c.MaxConcurrent > 16 || c.MinFreeBytes == 0 {
		return fmt.Errorf("request capture requires positive MIN_FREE_BYTES and MAX_CONCURRENT between 1 and 16")
	}
	if bodyLimit <= 0 {
		return fmt.Errorf("request capture requires a positive server request body limit")
	}
	return nil
}

// Match the existing global MaxBytesHandler fallback, without changing it.
func (c *Config) RequestCaptureBodyLimit() int64 {
	if c.Server.MaxRequestBodySize > 0 {
		return c.Server.MaxRequestBodySize
	}
	return c.Gateway.MaxBodySize
}

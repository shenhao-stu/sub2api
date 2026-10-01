package xai

import (
	"strings"
	"sync/atomic"

	"golang.org/x/mod/semver"
)

// CLIVersionPolicy separates operator pins from the last verified official release.
// Disabling synchronization freezes the last verified version, including across restarts.
type CLIVersionPolicy struct {
	Manual string
	Synced string
}

func (p CLIVersionPolicy) Resolve(override string) (version, source string) {
	for _, candidate := range []struct{ version, source string }{
		{override, "environment"}, {p.Manual, "manual"},
	} {
		version := strings.TrimSpace(candidate.version)
		if IsSupportedCLIVersion(version) {
			return version, candidate.source
		}
	}
	if IsSupportedCLIVersion(p.Synced) && semver.Compare("v"+p.Synced, "v"+CLIClientVersion) >= 0 {
		return p.Synced, "synchronized"
	}
	return CLIClientVersion, "builtin"
}

// CLIIdentity publishes an immutable policy to every request path. The server
// installs DB-backed policies; standalone clients retain the built-in baseline.
type CLIIdentity struct {
	policy atomic.Pointer[CLIVersionPolicy]
}

var DefaultCLIIdentity = &CLIIdentity{}

func (i *CLIIdentity) Policy() CLIVersionPolicy {
	if p := i.policy.Load(); p != nil {
		return *p
	}
	return CLIVersionPolicy{}
}

func (i *CLIIdentity) Update(p CLIVersionPolicy) { i.policy.Store(&p) }

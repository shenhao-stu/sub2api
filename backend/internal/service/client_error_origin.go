package service

import "regexp"

// Provider addresses are operational details, not client-facing diagnostics.
var publicErrorURL = regexp.MustCompile(`(?i)(?:[a-z][a-z0-9+.-]*://|//)[^\s<>"'` + "`" + `]+`)
var privateProviderHost = regexp.MustCompile(`(?i)(?:[a-z0-9_-]+\.)*derouter\.ai(?:[/:?#][^\s<>"'` + "`" + `]*)?`)

func redactClientErrorOrigins(message string) string {
	message = publicErrorURL.ReplaceAllString(message, "[upstream address]")
	return privateProviderHost.ReplaceAllString(message, "[upstream address]")
}

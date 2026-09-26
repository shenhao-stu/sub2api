package handler

import "testing"

func TestOpenAIWSTurnBillingIdentity(t *testing.T) {
	first := openAIWSTurnBillingRequestID("server-session-a", 1, "")
	if first == openAIWSTurnBillingRequestID("server-session-a", 2, "") || first == openAIWSTurnBillingRequestID("server-session-b", 1, "") {
		t.Fatal("independent generated work shared a billing identity")
	}
	if first != openAIWSTurnBillingRequestID("server-session-a", 1, " ") {
		t.Fatal("retry of the same turn lost its billing identity")
	}
	if got := openAIWSTurnBillingRequestID("server-session-a", 2, " resp_vendor "); got != "resp_vendor" {
		t.Fatalf("upstream id not retained for deduplication: %q", got)
	}
}

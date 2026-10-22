package service

import "testing"

func TestNanoBanana21UsesNativeAPIKeyImageTransport(t *testing.T) {
	for _, model := range []string{"nano-banana-2.1", "gemini-nano-banana-2.1"} {
		t.Run(model, func(t *testing.T) {
			if err := validateCompatibleImagesModel(model); err != nil {
				t.Fatal(err)
			}
			r := &OpenAIImagesRequest{RequiredCapability: OpenAIImagesCapabilityNative}
			if r.RequiredCapabilityForModel(model) != OpenAIImagesCapabilityAPIKey {
				t.Fatal("compatible image model must not reach OAuth native slots")
			}
		})
	}
	for _, model := range []string{"nano-banana-unknown", "gemini-nano-banana-99", "gemini-3.8-flash"} {
		if err := validateCompatibleImagesModel(model); err == nil {
			t.Fatalf("unrecognized image model %s accepted", model)
		}
	}
}

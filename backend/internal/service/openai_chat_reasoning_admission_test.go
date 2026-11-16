package service

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIChatReasoningAdmissionUsesSelectedModel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tt := range []struct {
		name, model, mapped, params string
		reject                      bool
		param                       string
	}{
		{"none", "gpt-6-astra", "", `"reasoning_effort":"none"`, true, "reasoning_effort"},
		{"alias to astra", "public-alias", "gpt-6-astra", `"reasoning_effort":"none"`, true, "reasoning_effort"},
		{"mapped away", "gpt-6-astra", "gpt-6-sol", `"reasoning_effort":"none"`, false, ""},
		{"other model", "gpt-6-sol", "", `"reasoning_effort":"none"`, false, ""},
		{"default", "gpt-6-astra", "", `"stream":true`, false, ""},
		{"low", "gpt-6-astra", "", `"reasoning_effort":"low"`, false, ""},
		{"medium", "gpt-6-astra", "", `"reasoning_effort":"medium"`, false, ""},
		{"high", "gpt-6-astra", "", `"reasoning_effort":"high"`, false, ""},
		{"xhigh", "gpt-6-astra", "", `"reasoning_effort":"xhigh"`, false, ""},
		{"max", "gpt-6-astra", "", `"reasoning_effort":"max"`, false, ""},
		{"responses shape", "gpt-6-astra", "", `"input":"synthetic","reasoning":{"effort":"none"}`, true, "reasoning.effort"},
		{"chat ignores nested effort", "gpt-6-astra", "", `"messages":[{"role":"user","content":"synthetic"}],"reasoning_effort":"high","reasoning":{"effort":"none"}`, false, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			account := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{}}
			if tt.mapped != "" {
				account.Credentials["model_mapping"] = map[string]any{tt.model: tt.mapped}
			}
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			body := []byte(`{"model":"` + tt.model + `",` + tt.params + `}`)
			before := string(body)
			err := (&OpenAIGatewayService{}).ValidateOpenAIChatReasoningEffort(c, account, body)
			require.Equal(t, before, string(body), "must not rewrite requested effort")
			if tt.reject {
				require.Error(t, err)
				require.Equal(t, http.StatusBadRequest, w.Code)
				require.Equal(t, "unsupported_value", gjson.GetBytes(w.Body.Bytes(), "error.code").String())
				require.Equal(t, tt.param, gjson.GetBytes(w.Body.Bytes(), "error.param").String())
			} else {
				require.NoError(t, err)
				require.Empty(t, w.Body.String())
			}
		})
	}
}

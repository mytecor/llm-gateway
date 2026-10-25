package policy

import (
	"testing"
)

func TestCompileJSONRejectsLegacyFields(t *testing.T) {
	cases := map[string]string{
		"match": `{"match":{"model":"standard"},"action":"filter","where":{"model":{"eq":"standard"}}}`,
		"retry.scope": `{
			"route":"standard","action":"retry","target":"standard.retry","attempts":1,"scope":"next"}`,
		"retry.count": `{
			"route":"standard","action":"retry","target":"standard.retry","attempts":1,"count":1}`,
		"retry.on": `{
			"route":"standard","action":"retry","target":"standard.retry","attempts":1,"on":["429"]}`,
		"fallback.on": `{
			"route":"standard","action":"fallback","target":"standard.fallback","on":["5xx"]}`,
		"fallback_strategy": `{
			"route":"standard","action":"fallback","target":"standard.fallback","fallback_strategy":"race"}`,
	}
	for name, rule := range cases {
		if _, err := decodeRule([]byte(rule), 0); err == nil {
			t.Fatalf("legacy field %q was silently accepted", name)
		}
	}
}

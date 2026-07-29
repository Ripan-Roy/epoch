package auth

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	adminToken   = "epoch-dev-admin-v1"
	readerToken  = "epoch-dev-reader-v1"
	controlToken = "epoch-dev-control-v1"
)

func TestBootstrapPolicyMatchesCrossLanguageDecisionCorpus(t *testing.T) {
	policy := loadExamplePolicy(t)
	var corpus struct {
		FormatVersion int `json:"format_version"`
		Cases         []struct {
			Name    string `json:"name"`
			Token   string `json:"token"`
			Action  Action `json:"action"`
			Scope   Scope  `json:"scope"`
			Allowed bool   `json:"allowed"`
		} `json:"cases"`
	}
	decodeFixture(t, "bootstrap-policy-v1-decisions.json", &corpus)
	if corpus.FormatVersion != 1 {
		t.Fatalf("decision corpus format version = %d", corpus.FormatVersion)
	}
	for _, testCase := range corpus.Cases {
		t.Run(testCase.Name, func(t *testing.T) {
			principal, err := policy.AuthenticateBearer("Bearer " + testCase.Token)
			if err != nil {
				t.Fatalf("AuthenticateBearer() error = %v", err)
			}
			if got := principal.Allows(testCase.Action, testCase.Scope); got != testCase.Allowed {
				t.Fatalf("Allows(%q, %+v) = %t, want %t", testCase.Action, testCase.Scope, got, testCase.Allowed)
			}
		})
	}
}

func TestBootstrapPolicyAuthenticationFailsClosedWithoutLeakingCredentials(t *testing.T) {
	policy := loadExamplePolicy(t)
	tests := []struct {
		name   string
		header string
		kind   AuthenticationErrorKind
	}{
		{name: "missing", kind: AuthenticationMissing},
		{name: "wrong scheme", header: "Basic abc", kind: AuthenticationMalformed},
		{name: "empty bearer", header: "Bearer ", kind: AuthenticationMalformed},
		{name: "extra field", header: "Bearer one two", kind: AuthenticationMalformed},
		{name: "unknown", header: "Bearer not-a-real-token", kind: AuthenticationInvalid},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := policy.AuthenticateBearer(testCase.header)
			var authenticationError *AuthenticationError
			if !errors.As(err, &authenticationError) || authenticationError.Kind != testCase.kind {
				t.Fatalf("AuthenticateBearer() error = %#v, want kind %q", err, testCase.kind)
			}
			if (testCase.header != "" && strings.Contains(err.Error(), testCase.header)) ||
				strings.Contains(err.Error(), "not-a-real-token") {
				t.Fatalf("authentication error leaked credential material: %v", err)
			}
		})
	}
}

func TestBootstrapPolicyRejectsAmbiguousOrUnboundedDocuments(t *testing.T) {
	valid := fixtureBytes(t, "bootstrap-policy-v1.example.json")
	var document map[string]any
	if err := json.Unmarshal(valid, &document); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{
			name: "unknown format",
			mutate: func(candidate map[string]any) {
				candidate["format_version"] = float64(2)
			},
		},
		{
			name: "unknown field",
			mutate: func(candidate map[string]any) {
				candidate["unexpected"] = true
			},
		},
		{
			name: "duplicate principal id",
			mutate: func(candidate map[string]any) {
				list := candidate["principals"].([]any)
				duplicate := cloneMap(list[0].(map[string]any))
				duplicate["token_sha256"] = strings.Repeat("1", 64)
				candidate["principals"] = append(list, duplicate)
			},
		},
		{
			name: "duplicate token fingerprint",
			mutate: func(candidate map[string]any) {
				list := candidate["principals"].([]any)
				duplicate := cloneMap(list[0].(map[string]any))
				duplicate["id"] = "duplicate-token"
				candidate["principals"] = append(list, duplicate)
			},
		},
		{
			name: "unknown action",
			mutate: func(candidate map[string]any) {
				list := candidate["principals"].([]any)
				list[0].(map[string]any)["actions"] = []any{"root"}
			},
		},
		{
			name: "uppercase fingerprint",
			mutate: func(candidate map[string]any) {
				list := candidate["principals"].([]any)
				list[0].(map[string]any)["token_sha256"] = strings.ToUpper(
					list[0].(map[string]any)["token_sha256"].(string),
				)
			},
		},
		{
			name: "partial wildcard",
			mutate: func(candidate map[string]any) {
				list := candidate["principals"].([]any)
				scope := list[0].(map[string]any)["scope"].(map[string]any)
				scope["organization"] = "acme-*"
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var candidate map[string]any
			if err := json.Unmarshal(valid, &candidate); err != nil {
				t.Fatal(err)
			}
			testCase.mutate(candidate)
			encoded, err := json.Marshal(candidate)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ParsePolicy(encoded); err == nil {
				t.Fatal("ParsePolicy() succeeded")
			}
		})
	}
}

func TestPrincipalIdentityAndActionsAreImmutableCopies(t *testing.T) {
	policy := loadExamplePolicy(t)
	principal, err := policy.AuthenticateBearer("Bearer " + adminToken)
	if err != nil {
		t.Fatal(err)
	}
	if principal.ID() != "development-admin" || principal.PolicyID() != "epoch-development-v1" {
		t.Fatalf("principal = id %q, policy %q", principal.ID(), principal.PolicyID())
	}
	actions := principal.Actions()
	actions[0] = Action("corrupted")
	if !principal.Allows(
		ActionResourceApply,
		Scope{Organization: "any", Project: "any", Environment: "any", Namespace: "any"},
	) {
		t.Fatal("mutating Actions() result changed the principal")
	}
}

func TestPolicyFormattingNeverExposesCredentialFingerprints(t *testing.T) {
	policy := loadExamplePolicy(t)
	for _, formatted := range []string{
		fmt.Sprintf("%v", policy),
		fmt.Sprintf("%+v", policy),
		fmt.Sprintf("%#v", policy),
	} {
		if strings.Contains(formatted, adminToken) ||
			strings.Contains(formatted, "dae2068c") ||
			strings.Contains(formatted, "[218 226") {
			t.Fatalf("formatted policy leaked credential material: %s", formatted)
		}
	}
}

func loadExamplePolicy(t *testing.T) *Policy {
	t.Helper()
	policy, err := LoadPolicy(fixturePath("bootstrap-policy-v1.example.json"))
	if err != nil {
		t.Fatalf("LoadPolicy() error = %v", err)
	}
	return policy
}

func decodeFixture(t *testing.T, name string, target any) {
	t.Helper()
	if err := json.Unmarshal(fixtureBytes(t, name), target); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
}

func fixtureBytes(t *testing.T, name string) []byte {
	t.Helper()
	encoded, err := os.ReadFile(fixturePath(name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return encoded
}

func fixturePath(name string) string {
	return filepath.Join("..", "..", "..", "spec", "auth", name)
}

func cloneMap(source map[string]any) map[string]any {
	clone := make(map[string]any, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

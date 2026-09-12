package auth

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
				candidate["format_version"] = float64(3)
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
	duplicateField := strings.Replace(
		string(valid),
		`"format_version": 1`,
		`"format_version": 1, "format_version": 1`,
		1,
	)
	if _, err := ParsePolicy([]byte(duplicateField)); err == nil {
		t.Fatal("ParsePolicy() accepted a duplicate JSON field")
	}
}

func TestOIDCEdDSAAuthenticationEnforcesSignatureLifetimeRoleScopeAndRevocation(t *testing.T) {
	policy, err := LoadPolicy(fixturePath("identity-policy-v2.example.json"))
	if err != nil {
		t.Fatal(err)
	}
	claims := map[string]any{
		"iss":                "https://identity.epoch.example",
		"aud":                []string{"unrelated", "epoch-api"},
		"sub":                "workload-orders",
		"iat":                uint64(1_000),
		"nbf":                uint64(1_000),
		"exp":                uint64(1_600),
		"jti":                "active-token-1",
		"epoch_roles":        []string{"reader", "writer"},
		"epoch_organization": "acme",
		"epoch_project":      "payments",
		"epoch_environment":  "production",
		"epoch_namespace":    "orders",
	}
	token := signedOIDCToken(t, claims)
	principal, err := policy.AuthenticateBearerAt("Bearer "+token, time.Unix(1_200, 0))
	if err != nil {
		t.Fatal(err)
	}
	if principal.AuthenticationMethod() != AuthenticationOIDCEdDSA ||
		!strings.HasPrefix(principal.ID(), "oidc:") ||
		!strings.HasSuffix(principal.ID(), ":workload-orders") {
		t.Fatalf("OIDC principal = %#v", principal)
	}
	if !principal.Allows(ActionDataWrite, Scope{
		Organization: "acme", Project: "payments", Environment: "production", Namespace: "orders",
	}) || principal.Allows(ActionDataWrite, Scope{
		Organization: "acme", Project: "payments", Environment: "production", Namespace: "other",
	}) {
		t.Fatal("OIDC role/scope evaluation did not fail closed")
	}
	assertAuthenticationKind(t, policy, token, time.Unix(1_631, 0), AuthenticationExpired)
	assertAuthenticationKind(t, policy, token, time.Unix(969, 0), AuthenticationNotYetValid)

	revokedClaims := cloneAnyMap(claims)
	revokedClaims["jti"] = "revoked-token-1"
	assertAuthenticationKind(
		t,
		policy,
		signedOIDCToken(t, revokedClaims),
		time.Unix(1_200, 0),
		AuthenticationRevoked,
	)
	wrongAudienceClaims := cloneAnyMap(claims)
	wrongAudienceClaims["aud"] = "another-service"
	assertAuthenticationKind(
		t,
		policy,
		signedOIDCToken(t, wrongAudienceClaims),
		time.Unix(1_200, 0),
		AuthenticationInvalid,
	)
}

func TestOIDCPolicyRejectsUnknownAlgorithmsKeysClaimsAndUnboundedLifetimes(t *testing.T) {
	valid := fixtureBytes(t, "identity-policy-v2.example.json")
	var issuerWithPath map[string]any
	if err := json.Unmarshal(valid, &issuerWithPath); err != nil {
		t.Fatal(err)
	}
	issuerWithPath["oidc"].(map[string]any)["issuers"].([]any)[0].(map[string]any)["issuer"] =
		"https://identity.epoch.example/realms/production/"
	encodedIssuerWithPath, err := json.Marshal(issuerWithPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParsePolicy(encodedIssuerWithPath); err != nil {
		t.Fatalf("valid path-based issuer rejected: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{
			name: "duplicate claims",
			mutate: func(candidate map[string]any) {
				oidc := candidate["oidc"].(map[string]any)
				oidc["scope_claims"].(map[string]any)["organization"] = "epoch_roles"
			},
		},
		{
			name: "non HTTPS issuer",
			mutate: func(candidate map[string]any) {
				issuer := candidate["oidc"].(map[string]any)["issuers"].([]any)[0].(map[string]any)
				issuer["issuer"] = "http://issuer"
			},
		},
		{
			name: "issuer without a hostname character",
			mutate: func(candidate map[string]any) {
				issuer := candidate["oidc"].(map[string]any)["issuers"].([]any)[0].(map[string]any)
				issuer["issuer"] = "https://-"
			},
		},
		{
			name: "issuer with user information",
			mutate: func(candidate map[string]any) {
				issuer := candidate["oidc"].(map[string]any)["issuers"].([]any)[0].(map[string]any)
				issuer["issuer"] = "https://user@issuer.example"
			},
		},
		{
			name: "issuer with query",
			mutate: func(candidate map[string]any) {
				issuer := candidate["oidc"].(map[string]any)["issuers"].([]any)[0].(map[string]any)
				issuer["issuer"] = "https://issuer.example?tenant=1"
			},
		},
		{
			name: "unsupported curve",
			mutate: func(candidate map[string]any) {
				issuer := candidate["oidc"].(map[string]any)["issuers"].([]any)[0].(map[string]any)
				issuer["keys"].([]any)[0].(map[string]any)["crv"] = "X25519"
			},
		},
		{
			name: "unbounded lifetime",
			mutate: func(candidate map[string]any) {
				issuer := candidate["oidc"].(map[string]any)["issuers"].([]any)[0].(map[string]any)
				issuer["maximum_token_lifetime_seconds"] = float64(86_401)
			},
		},
		{
			name: "unknown field",
			mutate: func(candidate map[string]any) {
				candidate["oidc"].(map[string]any)["discovery"] = true
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

func TestOIDCAuthenticationRejectsDuplicateSignedHeaderAndClaimNames(t *testing.T) {
	policy, err := LoadPolicy(fixturePath("identity-policy-v2.example.json"))
	if err != nil {
		t.Fatal(err)
	}
	claims := []byte(`{"iss":"https://identity.epoch.example","aud":"epoch-api","sub":"workload-orders","iat":1000,"exp":1600,"jti":"active-token-1","epoch_roles":["reader"],"epoch_organization":"acme","epoch_project":"payments","epoch_environment":"production","epoch_namespace":"orders"}`)
	duplicateHeader := []byte(`{"alg":"EdDSA","alg":"EdDSA","kid":"epoch-test-ed25519-1","typ":"JWT"}`)
	assertAuthenticationKind(
		t,
		policy,
		signedOIDCBytes(t, duplicateHeader, claims),
		time.Unix(1_200, 0),
		AuthenticationMalformed,
	)
	header := []byte(`{"alg":"EdDSA","kid":"epoch-test-ed25519-1","typ":"JWT"}`)
	duplicateClaims := bytes.Replace(
		claims,
		[]byte(`"sub":"workload-orders"`),
		[]byte(`"sub":"workload-orders","sub":"workload-orders"`),
		1,
	)
	assertAuthenticationKind(
		t,
		policy,
		signedOIDCBytes(t, header, duplicateClaims),
		time.Unix(1_200, 0),
		AuthenticationMalformed,
	)
	unknownHeader := []byte(`{"alg":"EdDSA","kid":"epoch-test-ed25519-1","crit":[]}`)
	assertAuthenticationKind(
		t,
		policy,
		signedOIDCBytes(t, unknownHeader, claims),
		time.Unix(1_200, 0),
		AuthenticationInvalid,
	)
}

func signedOIDCToken(t *testing.T, claims map[string]any) string {
	t.Helper()
	header := map[string]any{"alg": "EdDSA", "kid": "epoch-test-ed25519-1", "typ": "JWT"}
	headerBytes, _ := json.Marshal(header)
	claimsBytes, _ := json.Marshal(claims)
	return signedOIDCBytes(t, headerBytes, claimsBytes)
}

func signedOIDCBytes(t *testing.T, headerBytes, claimsBytes []byte) string {
	t.Helper()
	seed, err := base64.RawURLEncoding.DecodeString("nWGxne_9WmC6hEr0kuwsxERJxWl7MmkZcDusAxyuf2A")
	if err != nil {
		t.Fatal(err)
	}
	privateKey := ed25519.NewKeyFromSeed(seed)
	signingInput := base64.RawURLEncoding.EncodeToString(headerBytes) + "." +
		base64.RawURLEncoding.EncodeToString(claimsBytes)
	signature := ed25519.Sign(privateKey, []byte(signingInput))
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func assertAuthenticationKind(
	t *testing.T,
	policy *Policy,
	token string,
	now time.Time,
	want AuthenticationErrorKind,
) {
	t.Helper()
	_, err := policy.AuthenticateBearerAt("Bearer "+token, now)
	var authenticationError *AuthenticationError
	if !errors.As(err, &authenticationError) || authenticationError.Kind != want {
		t.Fatalf("AuthenticateBearerAt() error = %#v, want %q", err, want)
	}
}

func cloneAnyMap(source map[string]any) map[string]any {
	clone := make(map[string]any, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
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

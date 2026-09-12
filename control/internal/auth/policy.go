// Package auth implements Epoch's bounded bootstrap authentication and
// authorization policy. The bootstrap format is deliberately small: it is a
// migration bridge to OIDC and workload mTLS, not a replacement for them.
package auth

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	bootstrapPolicyFormatVersion = 1
	oidcPolicyFormatVersion      = 2
	maxPolicyBytes               = 1 << 20
	maxPrincipals                = 256
	maxActions                   = 32
	maxBearerHeader              = 8 << 10
	maxBearerToken               = 4 << 10
	maxOIDCIssuers               = 16
	maxOIDCKeysPerIssuer         = 16
	maxOIDCAudiences             = 16
	maxOIDCRoles                 = 64
	maxOIDCRoleIDsPerToken       = 16
	maxRevokedTokenIDs           = 4096
	maxClockSkewSeconds          = 300
	maxTokenLifetimeSeconds      = 86400
)

var (
	policyIDPattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)
	principalPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@/-]*$`)
	scopePattern     = regexp.MustCompile(`^(\*|[A-Za-z0-9][A-Za-z0-9._-]*)$`)
)

// Action is one stable authorization verb shared by the Go and Rust
// management boundaries.
type Action string

const (
	ActionAuditRead      Action = "audit.read"
	ActionBackupCreate   Action = "backup.create"
	ActionCatalogApply   Action = "catalog.apply"
	ActionCatalogDelete  Action = "catalog.delete"
	ActionCatalogRead    Action = "catalog.read"
	ActionDataRead       Action = "data.read"
	ActionDataWrite      Action = "data.write"
	ActionResourceApply  Action = "resource.apply"
	ActionResourceDelete Action = "resource.delete"
	ActionResourceRead   Action = "resource.read"
	ActionRouteRead      Action = "route.read"
	ActionTopologyRead   Action = "topology.read"
)

var validActions = map[Action]struct{}{
	ActionAuditRead:      {},
	ActionBackupCreate:   {},
	ActionCatalogApply:   {},
	ActionCatalogDelete:  {},
	ActionCatalogRead:    {},
	ActionDataRead:       {},
	ActionDataWrite:      {},
	ActionResourceApply:  {},
	ActionResourceDelete: {},
	ActionResourceRead:   {},
	ActionRouteRead:      {},
	ActionTopologyRead:   {},
}

// Scope identifies the tenant hierarchy evaluated by one authorization
// decision. Empty values are valid targets for standalone/local resources but
// can only be matched by a wildcard policy component.
type Scope struct {
	Organization string `json:"organization"`
	Project      string `json:"project"`
	Environment  string `json:"environment"`
	Namespace    string `json:"namespace"`
}

type policyDocument struct {
	FormatVersion int                 `json:"format_version"`
	PolicyID      string              `json:"policy_id"`
	Principals    []principalDocument `json:"principals"`
	OIDC          *oidcDocument       `json:"oidc,omitempty"`
}

type principalDocument struct {
	ID          string   `json:"id"`
	TokenSHA256 string   `json:"token_sha256"`
	Actions     []Action `json:"actions"`
	Scope       Scope    `json:"scope"`
}

type oidcDocument struct {
	RoleClaim        string                  `json:"role_claim"`
	ScopeClaims      oidcScopeClaimsDocument `json:"scope_claims"`
	Roles            []oidcRoleDocument      `json:"roles"`
	Issuers          []oidcIssuerDocument    `json:"issuers"`
	RevokedJTISHA256 []string                `json:"revoked_jti_sha256,omitempty"`
}

type oidcScopeClaimsDocument struct {
	Organization string `json:"organization"`
	Project      string `json:"project"`
	Environment  string `json:"environment"`
	Namespace    string `json:"namespace"`
}

type oidcRoleDocument struct {
	ID      string   `json:"id"`
	Actions []Action `json:"actions"`
}

type oidcIssuerDocument struct {
	Issuer                      string            `json:"issuer"`
	Audiences                   []string          `json:"audiences"`
	ClockSkewSeconds            uint64            `json:"clock_skew_seconds"`
	MaximumTokenLifetimeSeconds uint64            `json:"maximum_token_lifetime_seconds"`
	Keys                        []oidcKeyDocument `json:"keys"`
}

type oidcKeyDocument struct {
	KeyID string `json:"kid"`
	Type  string `json:"kty"`
	Curve string `json:"crv"`
	X     string `json:"x"`
}

type oidcConfiguration struct {
	roleClaim              string
	scopeClaims            oidcScopeClaimsDocument
	roles                  []oidcRole
	issuers                []oidcIssuer
	revokedJTIFingerprints [][sha256.Size]byte
}

type oidcRole struct {
	id      string
	actions []Action
}

type oidcIssuer struct {
	issuer                      string
	audiences                   []string
	clockSkewSeconds            uint64
	maximumTokenLifetimeSeconds uint64
	keys                        []oidcKey
}

type oidcKey struct {
	id        string
	publicKey ed25519.PublicKey
}

type policyPrincipal struct {
	id          string
	fingerprint [sha256.Size]byte
	actions     []Action
	actionSet   map[Action]struct{}
	scope       Scope
}

// Policy is an immutable in-memory bootstrap policy.
type Policy struct {
	id         string
	principals []policyPrincipal
	oidc       *oidcConfiguration
}

// Format prevents token fingerprints from appearing in accidental structured
// or diagnostic formatting.
func (policy *Policy) Format(state fmt.State, _ rune) {
	if policy == nil {
		_, _ = io.WriteString(state, "auth.Policy<nil>")
		return
	}
	_, _ = fmt.Fprintf(
		state,
		"auth.Policy{id:%q, principals:%d}",
		policy.id,
		len(policy.principals),
	)
}

// Principal is an authenticated immutable view of one policy principal.
type Principal struct {
	id                   string
	policyID             string
	actions              []Action
	actionSet            map[Action]struct{}
	scope                Scope
	authenticationMethod AuthenticationMethod
}

// AuthenticationMethod records the credential class without retaining it.
type AuthenticationMethod string

const (
	AuthenticationBootstrapToken AuthenticationMethod = "bootstrap_token"
	AuthenticationOIDCEdDSA      AuthenticationMethod = "oidc_eddsa"
)

// ID returns the stable policy identifier.
func (policy *Policy) ID() string {
	if policy == nil {
		return ""
	}
	return policy.id
}

// AuthenticationErrorKind classifies an authentication failure without
// exposing any credential material.
type AuthenticationErrorKind string

const (
	AuthenticationMissing     AuthenticationErrorKind = "missing"
	AuthenticationMalformed   AuthenticationErrorKind = "malformed"
	AuthenticationInvalid     AuthenticationErrorKind = "invalid"
	AuthenticationExpired     AuthenticationErrorKind = "expired"
	AuthenticationNotYetValid AuthenticationErrorKind = "not_yet_valid"
	AuthenticationRevoked     AuthenticationErrorKind = "revoked"
)

// AuthenticationError is safe to return to an API caller or audit sink.
type AuthenticationError struct {
	Kind AuthenticationErrorKind
}

func (authError *AuthenticationError) Error() string {
	switch authError.Kind {
	case AuthenticationMissing:
		return "bearer credential is required"
	case AuthenticationMalformed:
		return "bearer credential is malformed"
	case AuthenticationExpired:
		return "bearer credential is expired"
	case AuthenticationNotYetValid:
		return "bearer credential is not yet valid"
	case AuthenticationRevoked:
		return "bearer credential is revoked"
	default:
		return "bearer credential is invalid"
	}
}

// LoadPolicy reads and validates one bounded policy document.
func LoadPolicy(path string) (*Policy, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("auth policy path is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open auth policy: %w", err)
	}
	defer file.Close()
	encoded, err := io.ReadAll(io.LimitReader(file, maxPolicyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read auth policy: %w", err)
	}
	if len(encoded) > maxPolicyBytes {
		return nil, fmt.Errorf("auth policy exceeds %d bytes", maxPolicyBytes)
	}
	return ParsePolicy(encoded)
}

// ParsePolicy validates one in-memory policy document.
func ParsePolicy(encoded []byte) (*Policy, error) {
	if len(encoded) == 0 {
		return nil, errors.New("auth policy is empty")
	}
	if len(encoded) > maxPolicyBytes {
		return nil, fmt.Errorf("auth policy exceeds %d bytes", maxPolicyBytes)
	}
	if _, err := decodeUnambiguousJSON(encoded); err != nil {
		return nil, fmt.Errorf("decode auth policy: %w", err)
	}
	var document policyDocument
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode auth policy: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("auth policy must contain one JSON document")
		}
		return nil, fmt.Errorf("decode trailing auth policy data: %w", err)
	}
	if err := validatePolicyDocument(document); err != nil {
		return nil, err
	}
	principals := make([]policyPrincipal, 0, len(document.Principals))
	for _, raw := range document.Principals {
		fingerprintBytes, _ := hex.DecodeString(raw.TokenSHA256)
		var fingerprint [sha256.Size]byte
		copy(fingerprint[:], fingerprintBytes)
		actions := append([]Action(nil), raw.Actions...)
		actionSet := make(map[Action]struct{}, len(actions))
		for _, action := range actions {
			actionSet[action] = struct{}{}
		}
		principals = append(principals, policyPrincipal{
			id:          raw.ID,
			fingerprint: fingerprint,
			actions:     actions,
			actionSet:   actionSet,
			scope:       raw.Scope,
		})
	}
	oidc, err := buildOIDCConfiguration(document.OIDC)
	if err != nil {
		return nil, err
	}
	return &Policy{id: document.PolicyID, principals: principals, oidc: oidc}, nil
}

func validatePolicyDocument(document policyDocument) error {
	if document.FormatVersion != bootstrapPolicyFormatVersion &&
		document.FormatVersion != oidcPolicyFormatVersion {
		return fmt.Errorf(
			"auth policy format_version must be %d or %d",
			bootstrapPolicyFormatVersion,
			oidcPolicyFormatVersion,
		)
	}
	if document.FormatVersion == bootstrapPolicyFormatVersion && document.OIDC != nil {
		return errors.New("auth OIDC configuration requires format_version 2")
	}
	if !validBoundedValue(document.PolicyID, 128, policyIDPattern) {
		return errors.New("auth policy_id is invalid")
	}
	if len(document.Principals) > maxPrincipals ||
		(len(document.Principals) == 0 && document.OIDC == nil) {
		return fmt.Errorf("auth policy must contain OIDC or between 1 and %d bootstrap principals", maxPrincipals)
	}
	ids := make(map[string]struct{}, len(document.Principals))
	fingerprints := make(map[string]struct{}, len(document.Principals))
	for index, principal := range document.Principals {
		if !validBoundedValue(principal.ID, 128, principalPattern) {
			return fmt.Errorf("auth principal %d has an invalid id", index)
		}
		if _, exists := ids[principal.ID]; exists {
			return fmt.Errorf("auth principal id %q is duplicated", principal.ID)
		}
		ids[principal.ID] = struct{}{}
		if len(principal.TokenSHA256) != sha256.Size*2 ||
			principal.TokenSHA256 != strings.ToLower(principal.TokenSHA256) {
			return fmt.Errorf("auth principal %q has an invalid token_sha256", principal.ID)
		}
		if _, err := hex.DecodeString(principal.TokenSHA256); err != nil {
			return fmt.Errorf("auth principal %q has an invalid token_sha256", principal.ID)
		}
		if _, exists := fingerprints[principal.TokenSHA256]; exists {
			return errors.New("auth token_sha256 fingerprints must be unique")
		}
		fingerprints[principal.TokenSHA256] = struct{}{}
		if len(principal.Actions) == 0 || len(principal.Actions) > maxActions {
			return fmt.Errorf(
				"auth principal %q must contain between 1 and %d actions",
				principal.ID,
				maxActions,
			)
		}
		seenActions := make(map[Action]struct{}, len(principal.Actions))
		for _, action := range principal.Actions {
			if _, valid := validActions[action]; !valid {
				return fmt.Errorf("auth principal %q has unknown action %q", principal.ID, action)
			}
			if _, exists := seenActions[action]; exists {
				return fmt.Errorf("auth principal %q repeats action %q", principal.ID, action)
			}
			seenActions[action] = struct{}{}
		}
		if err := validateScope(principal.ID, principal.Scope); err != nil {
			return err
		}
	}
	if document.OIDC != nil {
		if err := validateOIDCDocument(*document.OIDC); err != nil {
			return err
		}
	}
	return nil
}

func validateOIDCDocument(document oidcDocument) error {
	claimNames := []string{
		document.RoleClaim,
		document.ScopeClaims.Organization,
		document.ScopeClaims.Project,
		document.ScopeClaims.Environment,
		document.ScopeClaims.Namespace,
	}
	uniqueClaims := make(map[string]struct{}, len(claimNames))
	for _, claim := range claimNames {
		if !validClaimName(claim) {
			return errors.New("auth OIDC role and scope claim names must be valid")
		}
		if _, duplicate := uniqueClaims[claim]; duplicate {
			return errors.New("auth OIDC role and scope claim names must be distinct")
		}
		uniqueClaims[claim] = struct{}{}
	}
	if len(document.Roles) == 0 || len(document.Roles) > maxOIDCRoles {
		return fmt.Errorf("auth OIDC policy must contain between 1 and %d roles", maxOIDCRoles)
	}
	roleIDs := make(map[string]struct{}, len(document.Roles))
	for _, role := range document.Roles {
		if !validBoundedValue(role.ID, 128, policyIDPattern) {
			return errors.New("auth OIDC role ID is invalid")
		}
		if _, duplicate := roleIDs[role.ID]; duplicate {
			return errors.New("auth OIDC role IDs must be unique")
		}
		roleIDs[role.ID] = struct{}{}
		if err := validateActions("OIDC role "+role.ID, role.Actions); err != nil {
			return err
		}
	}
	if len(document.Issuers) == 0 || len(document.Issuers) > maxOIDCIssuers {
		return fmt.Errorf("auth OIDC policy must contain between 1 and %d issuers", maxOIDCIssuers)
	}
	issuerIDs := make(map[string]struct{}, len(document.Issuers))
	for _, issuer := range document.Issuers {
		if !validHTTPSIssuer(issuer.Issuer) {
			return errors.New("auth OIDC issuer must be a bounded HTTPS URL without query or fragment")
		}
		if _, duplicate := issuerIDs[issuer.Issuer]; duplicate {
			return errors.New("auth OIDC issuers must be unique")
		}
		issuerIDs[issuer.Issuer] = struct{}{}
		if len(issuer.Audiences) == 0 || len(issuer.Audiences) > maxOIDCAudiences {
			return fmt.Errorf("auth OIDC issuer must contain between 1 and %d audiences", maxOIDCAudiences)
		}
		audiences := make(map[string]struct{}, len(issuer.Audiences))
		for _, audience := range issuer.Audiences {
			if !validOIDCIdentifier(audience) {
				return errors.New("auth OIDC audience is invalid")
			}
			if _, duplicate := audiences[audience]; duplicate {
				return errors.New("auth OIDC audiences must be unique")
			}
			audiences[audience] = struct{}{}
		}
		if issuer.ClockSkewSeconds > maxClockSkewSeconds ||
			issuer.MaximumTokenLifetimeSeconds == 0 ||
			issuer.MaximumTokenLifetimeSeconds > maxTokenLifetimeSeconds {
			return fmt.Errorf(
				"auth OIDC clock skew must be at most %d seconds and token lifetime between 1 and %d seconds",
				maxClockSkewSeconds,
				maxTokenLifetimeSeconds,
			)
		}
		if len(issuer.Keys) == 0 || len(issuer.Keys) > maxOIDCKeysPerIssuer {
			return fmt.Errorf("auth OIDC issuer must contain between 1 and %d keys", maxOIDCKeysPerIssuer)
		}
		keyIDs := make(map[string]struct{}, len(issuer.Keys))
		for _, key := range issuer.Keys {
			if !validBoundedValue(key.KeyID, 128, policyIDPattern) {
				return errors.New("auth OIDC key ID is invalid")
			}
			if _, duplicate := keyIDs[key.KeyID]; duplicate {
				return errors.New("auth OIDC key IDs must be unique per issuer")
			}
			keyIDs[key.KeyID] = struct{}{}
			if key.Type != "OKP" || key.Curve != "Ed25519" {
				return errors.New("auth OIDC keys must use kty OKP and crv Ed25519")
			}
			decoded, err := decodeCanonicalBase64URL(key.X)
			if err != nil || len(decoded) != ed25519.PublicKeySize {
				return errors.New("auth OIDC Ed25519 public key must be canonical base64url containing 32 bytes")
			}
		}
	}
	if len(document.RevokedJTISHA256) > maxRevokedTokenIDs {
		return fmt.Errorf("auth OIDC revocation list supports at most %d token IDs", maxRevokedTokenIDs)
	}
	revoked := make(map[string]struct{}, len(document.RevokedJTISHA256))
	for _, fingerprint := range document.RevokedJTISHA256 {
		if len(fingerprint) != sha256.Size*2 || fingerprint != strings.ToLower(fingerprint) {
			return errors.New("auth OIDC revoked token fingerprint is invalid")
		}
		if _, err := hex.DecodeString(fingerprint); err != nil {
			return errors.New("auth OIDC revoked token fingerprint is invalid")
		}
		if _, duplicate := revoked[fingerprint]; duplicate {
			return errors.New("auth OIDC revoked token fingerprints must be unique")
		}
		revoked[fingerprint] = struct{}{}
	}
	return nil
}

func buildOIDCConfiguration(document *oidcDocument) (*oidcConfiguration, error) {
	if document == nil {
		return nil, nil
	}
	configuration := &oidcConfiguration{
		roleClaim:              document.RoleClaim,
		scopeClaims:            document.ScopeClaims,
		roles:                  make([]oidcRole, 0, len(document.Roles)),
		issuers:                make([]oidcIssuer, 0, len(document.Issuers)),
		revokedJTIFingerprints: make([][sha256.Size]byte, 0, len(document.RevokedJTISHA256)),
	}
	for _, role := range document.Roles {
		configuration.roles = append(configuration.roles, oidcRole{
			id: role.ID, actions: append([]Action(nil), role.Actions...),
		})
	}
	for _, source := range document.Issuers {
		issuer := oidcIssuer{
			issuer:                      source.Issuer,
			audiences:                   append([]string(nil), source.Audiences...),
			clockSkewSeconds:            source.ClockSkewSeconds,
			maximumTokenLifetimeSeconds: source.MaximumTokenLifetimeSeconds,
			keys:                        make([]oidcKey, 0, len(source.Keys)),
		}
		for _, sourceKey := range source.Keys {
			decoded, err := decodeCanonicalBase64URL(sourceKey.X)
			if err != nil {
				return nil, err
			}
			issuer.keys = append(issuer.keys, oidcKey{
				id: sourceKey.KeyID, publicKey: ed25519.PublicKey(append([]byte(nil), decoded...)),
			})
		}
		configuration.issuers = append(configuration.issuers, issuer)
	}
	for _, encoded := range document.RevokedJTISHA256 {
		decoded, err := hex.DecodeString(encoded)
		if err != nil {
			return nil, err
		}
		var fingerprint [sha256.Size]byte
		copy(fingerprint[:], decoded)
		configuration.revokedJTIFingerprints = append(configuration.revokedJTIFingerprints, fingerprint)
	}
	return configuration, nil
}

func validateActions(label string, actions []Action) error {
	if len(actions) == 0 || len(actions) > maxActions {
		return fmt.Errorf("auth %s must contain between 1 and %d actions", label, maxActions)
	}
	seen := make(map[Action]struct{}, len(actions))
	for _, action := range actions {
		if _, valid := validActions[action]; !valid {
			return fmt.Errorf("auth %s has unknown action %q", label, action)
		}
		if _, duplicate := seen[action]; duplicate {
			return fmt.Errorf("auth %s repeats action %q", label, action)
		}
		seen[action] = struct{}{}
	}
	return nil
}

func validClaimName(value string) bool {
	if value == "" || len(value) > 128 || !isASCIIAlphaNumeric(value[0]) {
		return false
	}
	for index := range len(value) {
		character := value[index]
		if !isASCIIAlphaNumeric(character) && character != '.' && character != '_' && character != '-' {
			return false
		}
	}
	return true
}

func validOIDCIdentifier(value string) bool {
	if value == "" || len(value) > 128 || !isASCIIAlphaNumeric(value[0]) {
		return false
	}
	for index := range len(value) {
		character := value[index]
		if !isASCIIAlphaNumeric(character) && !strings.ContainsRune("._:/-", rune(character)) {
			return false
		}
	}
	return true
}

func isASCIIAlphaNumeric(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9'
}

func validHTTPSIssuer(value string) bool {
	if !strings.HasPrefix(value, "https://") || len(value) > 256 || strings.ContainsAny(value, "?#\\") {
		return false
	}
	for index := range len(value) {
		if value[index] < 0x21 || value[index] > 0x7e {
			return false
		}
	}
	parsed, err := url.Parse(value)
	return err == nil && parsed.Scheme == "https" && parsed.Opaque == "" &&
		parsed.Host != "" && validOIDCHost(parsed.Hostname()) && parsed.User == nil &&
		parsed.RawQuery == "" && !parsed.ForceQuery && parsed.Fragment == ""
}

func validOIDCHost(host string) bool {
	if net.ParseIP(host) != nil {
		return true
	}
	if host == "" || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || !isASCIIAlphaNumeric(label[0]) ||
			!isASCIIAlphaNumeric(label[len(label)-1]) {
			return false
		}
		for index := range len(label) {
			if !isASCIIAlphaNumeric(label[index]) && label[index] != '-' {
				return false
			}
		}
	}
	return true
}

func hasUnknownJWTHeader(header map[string]any) bool {
	for name := range header {
		if name != "alg" && name != "kid" && name != "typ" {
			return true
		}
	}
	return false
}

func decodeCanonicalBase64URL(encoded string) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != encoded {
		return nil, errors.New("value is not canonical base64url")
	}
	return decoded, nil
}

func validateScope(principalID string, scope Scope) error {
	values := []struct {
		name  string
		value string
	}{
		{name: "organization", value: scope.Organization},
		{name: "project", value: scope.Project},
		{name: "environment", value: scope.Environment},
		{name: "namespace", value: scope.Namespace},
	}
	for _, item := range values {
		if !validBoundedValue(item.value, 128, scopePattern) {
			return fmt.Errorf(
				"auth principal %q has invalid %s scope",
				principalID,
				item.name,
			)
		}
	}
	return nil
}

func validBoundedValue(value string, maximum int, pattern *regexp.Regexp) bool {
	return len(value) > 0 && len(value) <= maximum && pattern.MatchString(value)
}

// AuthenticateBearer authenticates a strict Authorization header. Matching
// scans every configured fingerprint with constant-time comparisons.
func (policy *Policy) AuthenticateBearer(header string) (Principal, error) {
	return policy.AuthenticateBearerAt(header, time.Now())
}

// AuthenticateBearerAt authenticates at an explicit time for deterministic
// expiry and clock-skew tests.
func (policy *Policy) AuthenticateBearerAt(header string, now time.Time) (Principal, error) {
	if policy == nil {
		return Principal{}, errors.New("auth policy is not configured")
	}
	if header == "" {
		return Principal{}, &AuthenticationError{Kind: AuthenticationMissing}
	}
	if len(header) > maxBearerHeader ||
		!strings.HasPrefix(header, "Bearer ") {
		return Principal{}, &AuthenticationError{Kind: AuthenticationMalformed}
	}
	token := strings.TrimPrefix(header, "Bearer ")
	if token == "" || len(token) > maxBearerToken || strings.ContainsAny(token, " \t\r\n") {
		return Principal{}, &AuthenticationError{Kind: AuthenticationMalformed}
	}
	if strings.Count(token, ".") == 2 {
		return policy.authenticateOIDC(token, now)
	}
	candidate := sha256.Sum256([]byte(token))
	matched := -1
	for index := range policy.principals {
		if subtle.ConstantTimeCompare(
			candidate[:],
			policy.principals[index].fingerprint[:],
		) == 1 {
			matched = index
		}
	}
	if matched < 0 {
		return Principal{}, &AuthenticationError{Kind: AuthenticationInvalid}
	}
	stored := policy.principals[matched]
	return Principal{
		id:                   stored.id,
		policyID:             policy.id,
		actions:              append([]Action(nil), stored.actions...),
		actionSet:            cloneActionSet(stored.actionSet),
		scope:                stored.scope,
		authenticationMethod: AuthenticationBootstrapToken,
	}, nil
}

func (policy *Policy) authenticateOIDC(token string, now time.Time) (Principal, error) {
	if policy.oidc == nil {
		return Principal{}, &AuthenticationError{Kind: AuthenticationInvalid}
	}
	segments := strings.Split(token, ".")
	if len(segments) != 3 || segments[0] == "" || segments[1] == "" || segments[2] == "" {
		return Principal{}, &AuthenticationError{Kind: AuthenticationMalformed}
	}
	headerBytes, err := decodeCanonicalBase64URL(segments[0])
	if err != nil {
		return Principal{}, &AuthenticationError{Kind: AuthenticationMalformed}
	}
	claimsBytes, err := decodeCanonicalBase64URL(segments[1])
	if err != nil {
		return Principal{}, &AuthenticationError{Kind: AuthenticationMalformed}
	}
	signature, err := decodeCanonicalBase64URL(segments[2])
	if err != nil || len(signature) != ed25519.SignatureSize {
		return Principal{}, &AuthenticationError{Kind: AuthenticationMalformed}
	}
	header, err := decodeJSONObject(headerBytes)
	if err != nil {
		return Principal{}, &AuthenticationError{Kind: AuthenticationMalformed}
	}
	if len(header) < 2 || len(header) > 3 ||
		hasUnknownJWTHeader(header) || requiredString(header, "alg") != "EdDSA" {
		return Principal{}, &AuthenticationError{Kind: AuthenticationInvalid}
	}
	if tokenType, exists := header["typ"]; exists {
		if typed, ok := tokenType.(string); !ok || typed != "JWT" {
			return Principal{}, &AuthenticationError{Kind: AuthenticationInvalid}
		}
	}
	keyID := requiredString(header, "kid")
	if keyID == "" {
		return Principal{}, &AuthenticationError{Kind: AuthenticationInvalid}
	}
	claims, err := decodeJSONObject(claimsBytes)
	if err != nil {
		return Principal{}, &AuthenticationError{Kind: AuthenticationMalformed}
	}
	issuerID := requiredString(claims, "iss")
	var issuer *oidcIssuer
	for index := range policy.oidc.issuers {
		if policy.oidc.issuers[index].issuer == issuerID {
			issuer = &policy.oidc.issuers[index]
			break
		}
	}
	if issuer == nil {
		return Principal{}, &AuthenticationError{Kind: AuthenticationInvalid}
	}
	var key *oidcKey
	for index := range issuer.keys {
		if issuer.keys[index].id == keyID {
			key = &issuer.keys[index]
			break
		}
	}
	if key == nil || !ed25519.Verify(
		key.publicKey,
		[]byte(segments[0]+"."+segments[1]),
		signature,
	) {
		return Principal{}, &AuthenticationError{Kind: AuthenticationInvalid}
	}
	if !audienceAllowed(claims["aud"], issuer.audiences) {
		return Principal{}, &AuthenticationError{Kind: AuthenticationInvalid}
	}
	issuedAt, ok := uint64Claim(claims, "iat")
	if !ok {
		return Principal{}, &AuthenticationError{Kind: AuthenticationInvalid}
	}
	expiresAt, ok := uint64Claim(claims, "exp")
	if !ok || expiresAt <= issuedAt || expiresAt-issuedAt > issuer.maximumTokenLifetimeSeconds {
		return Principal{}, &AuthenticationError{Kind: AuthenticationInvalid}
	}
	notBefore := issuedAt
	if _, exists := claims["nbf"]; exists {
		var valid bool
		notBefore, valid = uint64Claim(claims, "nbf")
		if !valid {
			return Principal{}, &AuthenticationError{Kind: AuthenticationInvalid}
		}
	}
	unix := now.Unix()
	if unix < 0 {
		return Principal{}, &AuthenticationError{Kind: AuthenticationNotYetValid}
	}
	nowSeconds := uint64(unix)
	if saturatingAdd(nowSeconds, issuer.clockSkewSeconds) < issuedAt ||
		saturatingAdd(nowSeconds, issuer.clockSkewSeconds) < notBefore {
		return Principal{}, &AuthenticationError{Kind: AuthenticationNotYetValid}
	}
	if nowSeconds > saturatingAdd(expiresAt, issuer.clockSkewSeconds) {
		return Principal{}, &AuthenticationError{Kind: AuthenticationExpired}
	}
	tokenID := requiredString(claims, "jti")
	if !validOIDCTokenID(tokenID) {
		return Principal{}, &AuthenticationError{Kind: AuthenticationInvalid}
	}
	fingerprint := sha256.Sum256([]byte(tokenID))
	for _, revoked := range policy.oidc.revokedJTIFingerprints {
		if subtle.ConstantTimeCompare(fingerprint[:], revoked[:]) == 1 {
			return Principal{}, &AuthenticationError{Kind: AuthenticationRevoked}
		}
	}
	roleIDs, ok := stringArrayClaim(claims, policy.oidc.roleClaim, maxOIDCRoleIDsPerToken)
	if !ok {
		return Principal{}, &AuthenticationError{Kind: AuthenticationInvalid}
	}
	actions := make([]Action, 0)
	actionSet := make(map[Action]struct{})
	for _, roleID := range roleIDs {
		var role *oidcRole
		for index := range policy.oidc.roles {
			if policy.oidc.roles[index].id == roleID {
				role = &policy.oidc.roles[index]
				break
			}
		}
		if role == nil {
			return Principal{}, &AuthenticationError{Kind: AuthenticationInvalid}
		}
		for _, action := range role.actions {
			if _, exists := actionSet[action]; !exists {
				actionSet[action] = struct{}{}
				actions = append(actions, action)
			}
		}
	}
	if len(actions) == 0 {
		return Principal{}, &AuthenticationError{Kind: AuthenticationInvalid}
	}
	scope := Scope{
		Organization: requiredString(claims, policy.oidc.scopeClaims.Organization),
		Project:      requiredString(claims, policy.oidc.scopeClaims.Project),
		Environment:  requiredString(claims, policy.oidc.scopeClaims.Environment),
		Namespace:    requiredString(claims, policy.oidc.scopeClaims.Namespace),
	}
	if validateScope("OIDC token", scope) != nil {
		return Principal{}, &AuthenticationError{Kind: AuthenticationInvalid}
	}
	subject := requiredString(claims, "sub")
	if !validBoundedValue(subject, 96, principalPattern) {
		return Principal{}, &AuthenticationError{Kind: AuthenticationInvalid}
	}
	issuerFingerprint := sha256.Sum256([]byte(issuer.issuer))
	principalID := fmt.Sprintf("oidc:%x:%s", issuerFingerprint[:8], subject)
	if !validBoundedValue(principalID, 128, principalPattern) {
		return Principal{}, &AuthenticationError{Kind: AuthenticationInvalid}
	}
	return Principal{
		id:                   principalID,
		policyID:             policy.id,
		actions:              actions,
		actionSet:            actionSet,
		scope:                scope,
		authenticationMethod: AuthenticationOIDCEdDSA,
	}, nil
}

func decodeJSONObject(encoded []byte) (map[string]any, error) {
	decoded, err := decodeUnambiguousJSON(encoded)
	if err != nil {
		return nil, errors.New("invalid JSON object")
	}
	value, ok := decoded.(map[string]any)
	if !ok || value == nil {
		return nil, errors.New("invalid JSON object")
	}
	return value, nil
}

// decodeUnambiguousJSON rejects duplicate object names at every depth. The
// standard decoder otherwise keeps the last value, which is unsafe for signed
// identity claims and cross-language authorization policy documents.
func decodeUnambiguousJSON(encoded []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	value, err := readUnambiguousJSONValue(decoder)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("JSON input contains trailing data")
		}
		return nil, fmt.Errorf("decode trailing JSON data: %w", err)
	}
	return value, nil
}

func readUnambiguousJSONValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return token, nil
	}
	switch delimiter {
	case '{':
		object := make(map[string]any)
		for decoder.More() {
			nameToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			name, ok := nameToken.(string)
			if !ok {
				return nil, errors.New("JSON object name is not a string")
			}
			if _, duplicate := object[name]; duplicate {
				return nil, fmt.Errorf("JSON object repeats field %q", name)
			}
			value, err := readUnambiguousJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			object[name] = value
		}
		if closing, err := decoder.Token(); err != nil || closing != json.Delim('}') {
			return nil, errors.New("JSON object is not terminated")
		}
		return object, nil
	case '[':
		array := make([]any, 0)
		for decoder.More() {
			value, err := readUnambiguousJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		if closing, err := decoder.Token(); err != nil || closing != json.Delim(']') {
			return nil, errors.New("JSON array is not terminated")
		}
		return array, nil
	default:
		return nil, errors.New("JSON contains an invalid delimiter")
	}
}

func requiredString(claims map[string]any, name string) string {
	value, ok := claims[name].(string)
	if !ok || value == "" || len(value) > 256 {
		return ""
	}
	return value
}

func uint64Claim(claims map[string]any, name string) (uint64, bool) {
	number, ok := claims[name].(json.Number)
	if !ok {
		return 0, false
	}
	value, err := strconv.ParseUint(string(number), 10, 64)
	return value, err == nil
}

func stringArrayClaim(claims map[string]any, name string, maximum int) ([]string, bool) {
	values, ok := claims[name].([]any)
	if !ok || len(values) == 0 || len(values) > maximum {
		return nil, false
	}
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, raw := range values {
		value, ok := raw.(string)
		if !ok || !validBoundedValue(value, 128, policyIDPattern) {
			return nil, false
		}
		if _, duplicate := seen[value]; duplicate {
			return nil, false
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result, true
}

func audienceAllowed(raw any, configured []string) bool {
	values := make([]string, 0, maxOIDCAudiences)
	switch audience := raw.(type) {
	case string:
		values = append(values, audience)
	case []any:
		if len(audience) == 0 || len(audience) > maxOIDCAudiences {
			return false
		}
		for _, rawValue := range audience {
			value, ok := rawValue.(string)
			if !ok {
				return false
			}
			values = append(values, value)
		}
	default:
		return false
	}
	for _, actual := range values {
		for _, expected := range configured {
			if actual == expected {
				return true
			}
		}
	}
	return false
}

func validOIDCTokenID(value string) bool {
	return validBoundedValue(value, 128, policyIDPattern)
}

func saturatingAdd(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}

// ID returns the stable principal identity.
func (principal Principal) ID() string {
	return principal.id
}

// PolicyID returns the policy that authenticated this principal.
func (principal Principal) PolicyID() string {
	return principal.policyID
}

// Actions returns a defensive copy of the principal's granted actions.
func (principal Principal) Actions() []Action {
	return append([]Action(nil), principal.actions...)
}

// Scope returns the immutable scope granted to the principal.
func (principal Principal) Scope() Scope {
	return principal.scope
}

// AuthenticationMethod returns the credential class without exposing it.
func (principal Principal) AuthenticationMethod() AuthenticationMethod {
	return principal.authenticationMethod
}

// Allows evaluates action and hierarchical scope without implicit grants.
func (principal Principal) Allows(action Action, target Scope) bool {
	if !principal.HasAction(action) {
		return false
	}
	return scopeComponentMatches(principal.scope.Organization, target.Organization) &&
		scopeComponentMatches(principal.scope.Project, target.Project) &&
		scopeComponentMatches(principal.scope.Environment, target.Environment) &&
		scopeComponentMatches(principal.scope.Namespace, target.Namespace)
}

// HasAction reports whether the principal has the verb before scope
// evaluation.
func (principal Principal) HasAction(action Action) bool {
	_, allowed := principal.actionSet[action]
	return allowed
}

func scopeComponentMatches(granted, target string) bool {
	return granted == "*" || granted == target
}

func cloneActionSet(source map[Action]struct{}) map[Action]struct{} {
	clone := make(map[Action]struct{}, len(source))
	for action := range source {
		clone[action] = struct{}{}
	}
	return clone
}

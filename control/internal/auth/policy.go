// Package auth implements Epoch's bounded bootstrap authentication and
// authorization policy. The bootstrap format is deliberately small: it is a
// migration bridge to OIDC and workload mTLS, not a replacement for them.
package auth

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

const (
	policyFormatVersion = 1
	maxPolicyBytes      = 1 << 20
	maxPrincipals       = 256
	maxActions          = 32
	maxBearerHeader     = 8 << 10
	maxBearerToken      = 4 << 10
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
	ActionCatalogApply   Action = "catalog.apply"
	ActionCatalogDelete  Action = "catalog.delete"
	ActionCatalogRead    Action = "catalog.read"
	ActionDataRead       Action = "data.read"
	ActionDataWrite      Action = "data.write"
	ActionResourceApply  Action = "resource.apply"
	ActionResourceDelete Action = "resource.delete"
	ActionResourceRead   Action = "resource.read"
	ActionRouteRead      Action = "route.read"
)

var validActions = map[Action]struct{}{
	ActionCatalogApply:   {},
	ActionCatalogDelete:  {},
	ActionCatalogRead:    {},
	ActionDataRead:       {},
	ActionDataWrite:      {},
	ActionResourceApply:  {},
	ActionResourceDelete: {},
	ActionResourceRead:   {},
	ActionRouteRead:      {},
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
}

type principalDocument struct {
	ID          string   `json:"id"`
	TokenSHA256 string   `json:"token_sha256"`
	Actions     []Action `json:"actions"`
	Scope       Scope    `json:"scope"`
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
	id        string
	policyID  string
	actions   []Action
	actionSet map[Action]struct{}
	scope     Scope
}

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
	AuthenticationMissing   AuthenticationErrorKind = "missing"
	AuthenticationMalformed AuthenticationErrorKind = "malformed"
	AuthenticationInvalid   AuthenticationErrorKind = "invalid"
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
	return &Policy{id: document.PolicyID, principals: principals}, nil
}

func validatePolicyDocument(document policyDocument) error {
	if document.FormatVersion != policyFormatVersion {
		return fmt.Errorf(
			"auth policy format_version must be %d",
			policyFormatVersion,
		)
	}
	if !validBoundedValue(document.PolicyID, 128, policyIDPattern) {
		return errors.New("auth policy_id is invalid")
	}
	if len(document.Principals) == 0 || len(document.Principals) > maxPrincipals {
		return fmt.Errorf("auth policy must contain between 1 and %d principals", maxPrincipals)
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
	return nil
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
		id:        stored.id,
		policyID:  policy.id,
		actions:   append([]Action(nil), stored.actions...),
		actionSet: cloneActionSet(stored.actionSet),
		scope:     stored.scope,
	}, nil
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

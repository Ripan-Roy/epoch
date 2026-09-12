//! Bounded bootstrap identity and authorization contracts.
//!
//! This bootstrap format is a migration bridge to OIDC and workload mTLS. It
//! stores only SHA-256 token fingerprints, uses deny-by-default action/scope
//! evaluation, and never exposes credential material from authenticated
//! principals or errors.

use std::{
    collections::HashSet,
    fmt,
    fs::File,
    io::{Read, Take},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::signature::{ED25519, UnparsedPublicKey};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;

mod audit;

pub use audit::{
    AuditEventDocument, AuditJournal, AuditJournalError, AuditJournalRecord, AuditPage,
};

const BOOTSTRAP_POLICY_FORMAT_VERSION: u32 = 1;
const OIDC_POLICY_FORMAT_VERSION: u32 = 2;
const MAX_POLICY_BYTES: u64 = 1 << 20;
const MAX_PRINCIPALS: usize = 256;
const MAX_ACTIONS: usize = 32;
const MAX_BEARER_HEADER: usize = 8 << 10;
const MAX_BEARER_TOKEN: usize = 4 << 10;
const MAX_AUDIT_FIELD_BYTES: usize = 256;
const MAX_REQUEST_ID_BYTES: usize = 128;
const MAX_OIDC_ISSUERS: usize = 16;
const MAX_OIDC_KEYS_PER_ISSUER: usize = 16;
const MAX_OIDC_AUDIENCES: usize = 16;
const MAX_OIDC_ROLES: usize = 64;
const MAX_OIDC_ROLE_IDS_PER_TOKEN: usize = 16;
const MAX_REVOKED_TOKEN_IDS: usize = 4_096;
const MAX_CLOCK_SKEW_SECONDS: u64 = 300;
const MAX_TOKEN_LIFETIME_SECONDS: u64 = 86_400;
const ED25519_PUBLIC_KEY_BYTES: usize = 32;

/// One stable authorization verb shared by Go and Rust.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum Action {
    #[serde(rename = "audit.read")]
    AuditRead,
    #[serde(rename = "backup.create")]
    BackupCreate,
    #[serde(rename = "catalog.apply")]
    CatalogApply,
    #[serde(rename = "catalog.delete")]
    CatalogDelete,
    #[serde(rename = "catalog.read")]
    CatalogRead,
    #[serde(rename = "data.read")]
    DataRead,
    #[serde(rename = "data.write")]
    DataWrite,
    #[serde(rename = "resource.apply")]
    ResourceApply,
    #[serde(rename = "resource.delete")]
    ResourceDelete,
    #[serde(rename = "resource.read")]
    ResourceRead,
    #[serde(rename = "route.read")]
    RouteRead,
    #[serde(rename = "topology.read")]
    TopologyRead,
}

impl Action {
    /// Returns the stable wire representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AuditRead => "audit.read",
            Self::BackupCreate => "backup.create",
            Self::CatalogApply => "catalog.apply",
            Self::CatalogDelete => "catalog.delete",
            Self::CatalogRead => "catalog.read",
            Self::DataRead => "data.read",
            Self::DataWrite => "data.write",
            Self::ResourceApply => "resource.apply",
            Self::ResourceDelete => "resource.delete",
            Self::ResourceRead => "resource.read",
            Self::RouteRead => "route.read",
            Self::TopologyRead => "topology.read",
        }
    }
}

/// Tenant hierarchy evaluated by one authorization decision.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceScope {
    pub organization: String,
    pub project: String,
    pub environment: String,
    pub namespace: String,
}

impl ResourceScope {
    /// Creates a target scope. Target fields may be empty for local resources;
    /// only wildcard policy fields match an empty target.
    pub fn new(
        organization: impl Into<String>,
        project: impl Into<String>,
        environment: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            organization: organization.into(),
            project: project.into(),
            environment: environment.into(),
            namespace: namespace.into(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyDocument {
    format_version: u32,
    policy_id: String,
    #[serde(default)]
    principals: Vec<PrincipalDocument>,
    #[serde(default)]
    oidc: Option<OidcDocument>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrincipalDocument {
    id: String,
    token_sha256: String,
    actions: Vec<Action>,
    scope: ResourceScope,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OidcDocument {
    role_claim: String,
    scope_claims: OidcScopeClaimsDocument,
    roles: Vec<OidcRoleDocument>,
    issuers: Vec<OidcIssuerDocument>,
    #[serde(default)]
    revoked_jti_sha256: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OidcScopeClaimsDocument {
    organization: String,
    project: String,
    environment: String,
    namespace: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OidcRoleDocument {
    id: String,
    actions: Vec<Action>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OidcIssuerDocument {
    issuer: String,
    audiences: Vec<String>,
    clock_skew_seconds: u64,
    maximum_token_lifetime_seconds: u64,
    keys: Vec<OidcKeyDocument>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OidcKeyDocument {
    kid: String,
    kty: String,
    crv: String,
    x: String,
}

#[derive(Debug, Clone)]
struct OidcConfiguration {
    role_claim: String,
    scope_claims: OidcScopeClaims,
    roles: Vec<OidcRole>,
    issuers: Vec<OidcIssuer>,
    revoked_jti_fingerprints: Vec<[u8; 32]>,
}

#[derive(Debug, Clone)]
struct OidcScopeClaims {
    organization: String,
    project: String,
    environment: String,
    namespace: String,
}

#[derive(Debug, Clone)]
struct OidcRole {
    id: String,
    actions: Vec<Action>,
}

#[derive(Debug, Clone)]
struct OidcIssuer {
    issuer: String,
    audiences: Vec<String>,
    clock_skew_seconds: u64,
    maximum_token_lifetime_seconds: u64,
    keys: Vec<OidcKey>,
}

#[derive(Clone)]
struct OidcKey {
    kid: String,
    public_key: [u8; ED25519_PUBLIC_KEY_BYTES],
}

struct VerifiedOidcToken<'a> {
    issuer: &'a OidcIssuer,
    claims: Map<String, Value>,
}

impl std::fmt::Debug for OidcKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OidcKey")
            .field("kid", &self.kid)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct StoredPrincipal {
    id: String,
    fingerprint: [u8; 32],
    actions: Vec<Action>,
    action_set: HashSet<Action>,
    scope: ResourceScope,
}

impl std::fmt::Debug for StoredPrincipal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoredPrincipal")
            .field("id", &self.id)
            .field("actions", &self.actions)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

/// Immutable, bounded bootstrap policy.
#[derive(Debug, Clone)]
pub struct BootstrapPolicy {
    id: String,
    principals: Vec<StoredPrincipal>,
    oidc: Option<OidcConfiguration>,
}

/// Authenticated immutable principal view.
#[derive(Debug, Clone)]
pub struct Principal {
    id: String,
    policy_id: String,
    actions: Vec<Action>,
    action_set: HashSet<Action>,
    scope: ResourceScope,
    authentication_method: AuthenticationMethod,
}

/// JSON value decoder that rejects duplicate object names at every depth.
/// `serde_json::Value` otherwise accepts the last value, which is unsafe for
/// signed claims and authorization policy documents interpreted by multiple
/// implementations.
struct UnambiguousJson(Value);

impl<'de> Deserialize<'de> for UnambiguousJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(UnambiguousJsonVisitor)
    }
}

struct UnambiguousJsonVisitor;

impl<'de> Visitor<'de> for UnambiguousJsonVisitor {
    type Value = UnambiguousJson;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an unambiguous JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(UnambiguousJson(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(UnambiguousJson(Value::Number(value.into())))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(UnambiguousJson(Value::Number(value.into())))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .map(UnambiguousJson)
            .ok_or_else(|| E::custom("JSON number is not finite"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_string(value.to_owned())
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(UnambiguousJson(Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(UnambiguousJson(Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(UnambiguousJson(Value::Null))
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(UnambiguousJson(value)) = sequence.next_element()? {
            values.push(value);
        }
        Ok(UnambiguousJson(Value::Array(values)))
    }

    fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = Map::new();
        while let Some(name) = object.next_key::<String>()? {
            if values.contains_key(&name) {
                return Err(de::Error::custom(format!(
                    "JSON object repeats field {name:?}"
                )));
            }
            let UnambiguousJson(value) = object.next_value()?;
            values.insert(name, value);
        }
        Ok(UnambiguousJson(Value::Object(values)))
    }
}

/// Credential class that established one principal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthenticationMethod {
    BootstrapToken,
    OidcEddsa,
}

impl AuthenticationMethod {
    /// Returns the stable audit representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BootstrapToken => "bootstrap_token",
            Self::OidcEddsa => "oidc_eddsa",
        }
    }
}

/// Policy loading or validation failure.
#[derive(Debug, Error)]
pub enum PolicyError {
    #[error("auth policy I/O failed: {0}")]
    Io(String),
    #[error("auth policy is invalid: {0}")]
    Invalid(String),
}

/// Stable authentication failure classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthenticationErrorKind {
    Missing,
    Malformed,
    Invalid,
    Expired,
    NotYetValid,
    Revoked,
}

/// Stable authorization outcome emitted to audit logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    Allow,
    Deny,
}

impl Decision {
    /// Returns the stable wire representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
        }
    }
}

/// Credential-free reason for one authorization outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionReason {
    PolicyGrant,
    ActionNotGranted,
    ScopeMismatch,
    MissingCredential,
    MalformedCredential,
    InvalidCredential,
    ExpiredCredential,
    NotYetValidCredential,
    RevokedCredential,
}

impl DecisionReason {
    /// Returns the stable wire representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PolicyGrant => "policy_grant",
            Self::ActionNotGranted => "action_not_granted",
            Self::ScopeMismatch => "scope_mismatch",
            Self::MissingCredential => "missing_credential",
            Self::MalformedCredential => "malformed_credential",
            Self::InvalidCredential => "invalid_credential",
            Self::ExpiredCredential => "expired_credential",
            Self::NotYetValidCredential => "not_yet_valid_credential",
            Self::RevokedCredential => "revoked_credential",
        }
    }
}

/// One bounded authorization decision. Credential material has no field in
/// this contract and therefore cannot be serialized accidentally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DecisionEvent {
    request_id: String,
    principal_id: String,
    policy_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    authentication_method: Option<AuthenticationMethod>,
    action: Action,
    decision: Decision,
    reason: DecisionReason,
    scope: ResourceScope,
}

/// Input fields for one authorization decision. Keeping the record as one
/// value prevents positional arguments from being swapped at call sites.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionEventFields {
    pub request_id: String,
    pub principal_id: String,
    pub policy_id: String,
    pub authentication_method: Option<AuthenticationMethod>,
    pub action: Action,
    pub decision: Decision,
    pub reason: DecisionReason,
    pub scope: ResourceScope,
}

/// Invalid or unbounded audit decision.
#[derive(Debug, Error)]
#[error("authorization audit event is invalid: {0}")]
pub struct AuditEventError(String);

impl DecisionEvent {
    /// Constructs one validated, bounded audit decision.
    pub fn new(fields: DecisionEventFields) -> Result<Self, AuditEventError> {
        let event = Self {
            request_id: fields.request_id,
            principal_id: fields.principal_id,
            policy_id: fields.policy_id,
            authentication_method: fields.authentication_method,
            action: fields.action,
            decision: fields.decision,
            reason: fields.reason,
            scope: fields.scope,
        };
        event.validate()?;
        Ok(event)
    }

    fn validate(&self) -> Result<(), AuditEventError> {
        for (name, value) in [
            ("request_id", self.request_id.as_str()),
            ("principal_id", self.principal_id.as_str()),
            ("policy_id", self.policy_id.as_str()),
        ] {
            if value.is_empty() || value.len() > MAX_AUDIT_FIELD_BYTES {
                return Err(AuditEventError(format!(
                    "{name} must contain between 1 and {MAX_AUDIT_FIELD_BYTES} bytes"
                )));
            }
        }
        if self.request_id.len() > MAX_REQUEST_ID_BYTES
            || !self
                .request_id
                .bytes()
                .all(|byte| matches!(byte, 0x21..=0x7e))
        {
            return Err(AuditEventError(format!(
                "request_id must contain 1 to {MAX_REQUEST_ID_BYTES} printable ASCII bytes"
            )));
        }
        if !valid_principal_id(&self.principal_id) {
            return Err(AuditEventError("principal_id is invalid".into()));
        }
        if !valid_policy_id(&self.policy_id) {
            return Err(AuditEventError("policy_id is invalid".into()));
        }
        for (name, value) in [
            ("organization", self.scope.organization.as_str()),
            ("project", self.scope.project.as_str()),
            ("environment", self.scope.environment.as_str()),
            ("namespace", self.scope.namespace.as_str()),
        ] {
            if !value.is_empty() && !valid_scope_value(value) {
                return Err(AuditEventError(format!("{name} scope is invalid")));
            }
        }
        Ok(())
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn principal_id(&self) -> &str {
        &self.principal_id
    }

    pub fn policy_id(&self) -> &str {
        &self.policy_id
    }

    pub const fn authentication_method(&self) -> Option<AuthenticationMethod> {
        self.authentication_method
    }

    pub const fn action(&self) -> Action {
        self.action
    }

    pub const fn decision(&self) -> Decision {
        self.decision
    }

    pub const fn reason(&self) -> DecisionReason {
        self.reason
    }

    pub const fn scope(&self) -> &ResourceScope {
        &self.scope
    }
}

/// Authentication failure that never includes credential material.
#[derive(Debug, Error)]
#[error("{message}")]
pub struct AuthenticationError {
    kind: AuthenticationErrorKind,
    message: &'static str,
}

impl AuthenticationError {
    /// Returns the stable failure class.
    pub const fn kind(&self) -> AuthenticationErrorKind {
        self.kind
    }

    const fn missing() -> Self {
        Self {
            kind: AuthenticationErrorKind::Missing,
            message: "bearer credential is required",
        }
    }

    const fn malformed() -> Self {
        Self {
            kind: AuthenticationErrorKind::Malformed,
            message: "bearer credential is malformed",
        }
    }

    const fn invalid() -> Self {
        Self {
            kind: AuthenticationErrorKind::Invalid,
            message: "bearer credential is invalid",
        }
    }

    const fn expired() -> Self {
        Self {
            kind: AuthenticationErrorKind::Expired,
            message: "bearer credential is expired",
        }
    }

    const fn not_yet_valid() -> Self {
        Self {
            kind: AuthenticationErrorKind::NotYetValid,
            message: "bearer credential is not yet valid",
        }
    }

    const fn revoked() -> Self {
        Self {
            kind: AuthenticationErrorKind::Revoked,
            message: "bearer credential is revoked",
        }
    }
}

impl BootstrapPolicy {
    /// Loads one bounded policy document from disk.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, PolicyError> {
        let path = path.as_ref();
        if path.as_os_str().is_empty() {
            return Err(PolicyError::Invalid("policy path is required".into()));
        }
        let file = File::open(path).map_err(|error| PolicyError::Io(error.to_string()))?;
        let mut reader: Take<File> = file.take(MAX_POLICY_BYTES + 1);
        let mut encoded = Vec::new();
        reader
            .read_to_end(&mut encoded)
            .map_err(|error| PolicyError::Io(error.to_string()))?;
        if encoded.len() as u64 > MAX_POLICY_BYTES {
            return Err(PolicyError::Invalid(format!(
                "policy exceeds {MAX_POLICY_BYTES} bytes"
            )));
        }
        Self::from_json(&encoded)
    }

    /// Parses and validates one in-memory policy document.
    pub fn from_json(encoded: &[u8]) -> Result<Self, PolicyError> {
        if encoded.is_empty() {
            return Err(PolicyError::Invalid("policy is empty".into()));
        }
        if encoded.len() as u64 > MAX_POLICY_BYTES {
            return Err(PolicyError::Invalid(format!(
                "policy exceeds {MAX_POLICY_BYTES} bytes"
            )));
        }
        let UnambiguousJson(value) = serde_json::from_slice(encoded)
            .map_err(|error| PolicyError::Invalid(error.to_string()))?;
        let document: PolicyDocument = serde_json::from_value(value)
            .map_err(|error| PolicyError::Invalid(error.to_string()))?;
        validate_document(&document)?;
        let mut principals = Vec::with_capacity(document.principals.len());
        for raw in document.principals {
            let fingerprint = decode_fingerprint(&raw.token_sha256)?;
            let action_set = raw.actions.iter().copied().collect();
            principals.push(StoredPrincipal {
                id: raw.id,
                fingerprint,
                actions: raw.actions,
                action_set,
                scope: raw.scope,
            });
        }
        let oidc = document.oidc.map(build_oidc_configuration).transpose()?;
        Ok(Self {
            id: document.policy_id,
            principals,
            oidc,
        })
    }

    /// Authenticates a strict Authorization header. Every configured
    /// fingerprint is scanned using a constant-time comparison.
    pub fn authenticate_bearer(
        &self,
        header: Option<&str>,
    ) -> Result<Principal, AuthenticationError> {
        let now_seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_secs());
        self.authenticate_bearer_at(header, now_seconds)
    }

    /// Authenticates at an explicit Unix time for deterministic expiry and
    /// clock-skew tests.
    pub fn authenticate_bearer_at(
        &self,
        header: Option<&str>,
        now_seconds: u64,
    ) -> Result<Principal, AuthenticationError> {
        let Some(header) = header else {
            return Err(AuthenticationError::missing());
        };
        let Some(token) = header.strip_prefix("Bearer ") else {
            return Err(AuthenticationError::malformed());
        };
        if header.len() > MAX_BEARER_HEADER
            || token.is_empty()
            || token.len() > MAX_BEARER_TOKEN
            || token.bytes().any(|byte| byte.is_ascii_whitespace())
        {
            return Err(AuthenticationError::malformed());
        }
        if token.bytes().filter(|byte| *byte == b'.').count() == 2 {
            return self.authenticate_oidc(token, now_seconds);
        }
        let candidate: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let mut matched = None;
        for principal in &self.principals {
            if constant_time_eq(&candidate, &principal.fingerprint) {
                matched = Some(principal);
            }
        }
        let Some(stored) = matched else {
            return Err(AuthenticationError::invalid());
        };
        Ok(Principal {
            id: stored.id.clone(),
            policy_id: self.id.clone(),
            actions: stored.actions.clone(),
            action_set: stored.action_set.clone(),
            scope: stored.scope.clone(),
            authentication_method: AuthenticationMethod::BootstrapToken,
        })
    }

    /// Returns the stable policy identifier.
    pub fn id(&self) -> &str {
        &self.id
    }

    fn authenticate_oidc(
        &self,
        token: &str,
        now_seconds: u64,
    ) -> Result<Principal, AuthenticationError> {
        let oidc = self
            .oidc
            .as_ref()
            .ok_or_else(AuthenticationError::invalid)?;
        let verified = verify_oidc_token(oidc, token)?;
        validate_audience(&verified.claims, &verified.issuer.audiences)?;
        validate_oidc_lifetime(verified.issuer, &verified.claims, now_seconds)?;
        validate_oidc_token_id(oidc, &verified.claims)?;
        let (actions, action_set) = oidc_actions(oidc, &verified.claims)?;
        let scope = oidc_scope(oidc, &verified.claims)?;
        let principal_id = oidc_principal_id(verified.issuer, &verified.claims)?;
        Ok(Principal {
            id: principal_id,
            policy_id: self.id.clone(),
            actions,
            action_set,
            scope,
            authentication_method: AuthenticationMethod::OidcEddsa,
        })
    }
}

fn verify_oidc_token<'a>(
    oidc: &'a OidcConfiguration,
    token: &str,
) -> Result<VerifiedOidcToken<'a>, AuthenticationError> {
    let mut segments = token.split('.');
    let header_segment = segments.next().ok_or_else(AuthenticationError::malformed)?;
    let claims_segment = segments.next().ok_or_else(AuthenticationError::malformed)?;
    let signature_segment = segments.next().ok_or_else(AuthenticationError::malformed)?;
    if segments.next().is_some()
        || header_segment.is_empty()
        || claims_segment.is_empty()
        || signature_segment.is_empty()
    {
        return Err(AuthenticationError::malformed());
    }
    let header = unambiguous_object(&decode_base64url_segment(header_segment)?)?;
    if header.len() < 2
        || header.len() > 3
        || header
            .keys()
            .any(|name| !matches!(name.as_str(), "alg" | "kid" | "typ"))
        || header.get("alg").and_then(Value::as_str) != Some("EdDSA")
        || header
            .get("typ")
            .is_some_and(|value| value.as_str() != Some("JWT"))
    {
        return Err(AuthenticationError::invalid());
    }
    let kid = required_string_claim(&header, "kid")?;
    let claims = unambiguous_object(&decode_base64url_segment(claims_segment)?)?;
    let issuer_value = required_string_claim(&claims, "iss")?;
    let issuer = oidc
        .issuers
        .iter()
        .find(|candidate| candidate.issuer == issuer_value)
        .ok_or_else(AuthenticationError::invalid)?;
    let key = issuer
        .keys
        .iter()
        .find(|candidate| candidate.kid == kid)
        .ok_or_else(AuthenticationError::invalid)?;
    let signature = decode_base64url_segment(signature_segment)?;
    if signature.len() != 64 {
        return Err(AuthenticationError::malformed());
    }
    let signed = format!("{header_segment}.{claims_segment}");
    UnparsedPublicKey::new(&ED25519, key.public_key)
        .verify(signed.as_bytes(), &signature)
        .map_err(|_| AuthenticationError::invalid())?;
    Ok(VerifiedOidcToken { issuer, claims })
}

fn validate_oidc_lifetime(
    issuer: &OidcIssuer,
    claims: &Map<String, Value>,
    now_seconds: u64,
) -> Result<(), AuthenticationError> {
    let issued_at = required_u64_claim(claims, "iat")?;
    let expires_at = required_u64_claim(claims, "exp")?;
    let not_before = optional_u64_claim(claims, "nbf")?.unwrap_or(issued_at);
    if expires_at <= issued_at || expires_at - issued_at > issuer.maximum_token_lifetime_seconds {
        return Err(AuthenticationError::invalid());
    }
    if now_seconds.saturating_add(issuer.clock_skew_seconds) < issued_at
        || now_seconds.saturating_add(issuer.clock_skew_seconds) < not_before
    {
        return Err(AuthenticationError::not_yet_valid());
    }
    if now_seconds > expires_at.saturating_add(issuer.clock_skew_seconds) {
        return Err(AuthenticationError::expired());
    }
    Ok(())
}

fn validate_oidc_token_id(
    oidc: &OidcConfiguration,
    claims: &Map<String, Value>,
) -> Result<(), AuthenticationError> {
    let token_id = required_string_claim(claims, "jti")?;
    if !valid_identifier(token_id, |byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-')
    }) {
        return Err(AuthenticationError::invalid());
    }
    let fingerprint: [u8; 32] = Sha256::digest(token_id.as_bytes()).into();
    if oidc
        .revoked_jti_fingerprints
        .iter()
        .any(|revoked| constant_time_eq(&fingerprint, revoked))
    {
        return Err(AuthenticationError::revoked());
    }
    Ok(())
}

fn oidc_actions(
    oidc: &OidcConfiguration,
    claims: &Map<String, Value>,
) -> Result<(Vec<Action>, HashSet<Action>), AuthenticationError> {
    let role_ids =
        required_string_array_claim(claims, &oidc.role_claim, MAX_OIDC_ROLE_IDS_PER_TOKEN)?;
    let mut actions = Vec::new();
    let mut action_set = HashSet::new();
    for role_id in role_ids {
        let role = oidc
            .roles
            .iter()
            .find(|candidate| candidate.id == role_id)
            .ok_or_else(AuthenticationError::invalid)?;
        for action in &role.actions {
            if action_set.insert(*action) {
                actions.push(*action);
            }
        }
    }
    if actions.is_empty() {
        return Err(AuthenticationError::invalid());
    }
    Ok((actions, action_set))
}

fn oidc_scope(
    oidc: &OidcConfiguration,
    claims: &Map<String, Value>,
) -> Result<ResourceScope, AuthenticationError> {
    let scope = ResourceScope::new(
        required_string_claim(claims, &oidc.scope_claims.organization)?,
        required_string_claim(claims, &oidc.scope_claims.project)?,
        required_string_claim(claims, &oidc.scope_claims.environment)?,
        required_string_claim(claims, &oidc.scope_claims.namespace)?,
    );
    validate_scope("OIDC token", &scope).map_err(|_| AuthenticationError::invalid())?;
    Ok(scope)
}

fn oidc_principal_id(
    issuer: &OidcIssuer,
    claims: &Map<String, Value>,
) -> Result<String, AuthenticationError> {
    let subject = required_string_claim(claims, "sub")?;
    if subject.len() > 96 || !valid_principal_id(subject) {
        return Err(AuthenticationError::invalid());
    }
    let issuer_digest = Sha256::digest(issuer.issuer.as_bytes());
    let principal_id = format!("oidc:{}:{subject}", encode_lower_hex(&issuer_digest[..8]));
    valid_principal_id(&principal_id)
        .then_some(principal_id)
        .ok_or_else(AuthenticationError::invalid)
}

impl Principal {
    /// Returns the stable principal identity.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the policy that authenticated this principal.
    pub fn policy_id(&self) -> &str {
        &self.policy_id
    }

    /// Returns the granted actions.
    pub fn actions(&self) -> &[Action] {
        &self.actions
    }

    /// Returns the granted tenant scope.
    pub const fn scope(&self) -> &ResourceScope {
        &self.scope
    }

    /// Returns the credential class that established this principal.
    pub const fn authentication_method(&self) -> AuthenticationMethod {
        self.authentication_method
    }

    /// Evaluates an action and target hierarchy without implicit grants.
    pub fn allows(&self, action: Action, target: &ResourceScope) -> bool {
        self.action_set.contains(&action)
            && scope_component_matches(&self.scope.organization, &target.organization)
            && scope_component_matches(&self.scope.project, &target.project)
            && scope_component_matches(&self.scope.environment, &target.environment)
            && scope_component_matches(&self.scope.namespace, &target.namespace)
    }

    /// Reports whether the principal holds an action before scope evaluation.
    pub fn has_action(&self, action: Action) -> bool {
        self.action_set.contains(&action)
    }
}

fn validate_document(document: &PolicyDocument) -> Result<(), PolicyError> {
    if !matches!(
        document.format_version,
        BOOTSTRAP_POLICY_FORMAT_VERSION | OIDC_POLICY_FORMAT_VERSION
    ) {
        return Err(PolicyError::Invalid(format!(
            "format_version must be {BOOTSTRAP_POLICY_FORMAT_VERSION} or {OIDC_POLICY_FORMAT_VERSION}"
        )));
    }
    if document.format_version == BOOTSTRAP_POLICY_FORMAT_VERSION && document.oidc.is_some() {
        return Err(PolicyError::Invalid(
            "OIDC configuration requires format_version 2".into(),
        ));
    }
    if !valid_policy_id(&document.policy_id) {
        return Err(PolicyError::Invalid("policy_id is invalid".into()));
    }
    if document.principals.len() > MAX_PRINCIPALS
        || (document.principals.is_empty() && document.oidc.is_none())
    {
        return Err(PolicyError::Invalid(format!(
            "policy must contain an OIDC configuration or between 1 and {MAX_PRINCIPALS} bootstrap principals"
        )));
    }
    let mut ids = HashSet::with_capacity(document.principals.len());
    let mut fingerprints = HashSet::with_capacity(document.principals.len());
    for principal in &document.principals {
        if !valid_principal_id(&principal.id) {
            return Err(PolicyError::Invalid(format!(
                "principal {} has an invalid id",
                principal.id
            )));
        }
        if !ids.insert(&principal.id) {
            return Err(PolicyError::Invalid(format!(
                "principal id {} is duplicated",
                principal.id
            )));
        }
        decode_fingerprint(&principal.token_sha256)?;
        if !fingerprints.insert(&principal.token_sha256) {
            return Err(PolicyError::Invalid(
                "token fingerprints must be unique".into(),
            ));
        }
        if principal.actions.is_empty() || principal.actions.len() > MAX_ACTIONS {
            return Err(PolicyError::Invalid(format!(
                "principal {} must contain between 1 and {MAX_ACTIONS} actions",
                principal.id
            )));
        }
        let mut actions = HashSet::with_capacity(principal.actions.len());
        for action in &principal.actions {
            if !actions.insert(*action) {
                return Err(PolicyError::Invalid(format!(
                    "principal {} repeats action {}",
                    principal.id,
                    action.as_str()
                )));
            }
        }
        validate_scope(&principal.id, &principal.scope)?;
    }
    if let Some(oidc) = &document.oidc {
        validate_oidc_document(oidc)?;
    }
    Ok(())
}

fn validate_oidc_document(document: &OidcDocument) -> Result<(), PolicyError> {
    let claim_names = [
        document.role_claim.as_str(),
        document.scope_claims.organization.as_str(),
        document.scope_claims.project.as_str(),
        document.scope_claims.environment.as_str(),
        document.scope_claims.namespace.as_str(),
    ];
    if claim_names.iter().any(|claim| !valid_claim_name(claim))
        || claim_names.iter().copied().collect::<HashSet<_>>().len() != claim_names.len()
    {
        return Err(PolicyError::Invalid(
            "OIDC role and scope claim names must be valid and distinct".into(),
        ));
    }
    if document.roles.is_empty() || document.roles.len() > MAX_OIDC_ROLES {
        return Err(PolicyError::Invalid(format!(
            "OIDC policy must contain between 1 and {MAX_OIDC_ROLES} roles"
        )));
    }
    let mut role_ids = HashSet::with_capacity(document.roles.len());
    for role in &document.roles {
        if !valid_policy_id(&role.id) || !role_ids.insert(role.id.as_str()) {
            return Err(PolicyError::Invalid(
                "OIDC role IDs must be valid and unique".into(),
            ));
        }
        validate_actions(&format!("OIDC role {}", role.id), &role.actions)?;
    }
    if document.issuers.is_empty() || document.issuers.len() > MAX_OIDC_ISSUERS {
        return Err(PolicyError::Invalid(format!(
            "OIDC policy must contain between 1 and {MAX_OIDC_ISSUERS} issuers"
        )));
    }
    let mut issuer_ids = HashSet::with_capacity(document.issuers.len());
    for issuer in &document.issuers {
        if !valid_https_issuer(&issuer.issuer) || !issuer_ids.insert(issuer.issuer.as_str()) {
            return Err(PolicyError::Invalid(
                "OIDC issuer URLs must be valid, HTTPS, bounded, and unique".into(),
            ));
        }
        validate_oidc_issuer(issuer)?;
    }
    if document.revoked_jti_sha256.len() > MAX_REVOKED_TOKEN_IDS {
        return Err(PolicyError::Invalid(format!(
            "OIDC revocation list supports at most {MAX_REVOKED_TOKEN_IDS} token IDs"
        )));
    }
    let mut revoked = HashSet::with_capacity(document.revoked_jti_sha256.len());
    for fingerprint in &document.revoked_jti_sha256 {
        decode_fingerprint(fingerprint)?;
        if !revoked.insert(fingerprint.as_str()) {
            return Err(PolicyError::Invalid(
                "OIDC revoked token fingerprints must be unique".into(),
            ));
        }
    }
    Ok(())
}

fn validate_oidc_issuer(issuer: &OidcIssuerDocument) -> Result<(), PolicyError> {
    if issuer.audiences.is_empty() || issuer.audiences.len() > MAX_OIDC_AUDIENCES {
        return Err(PolicyError::Invalid(format!(
            "OIDC issuer must contain between 1 and {MAX_OIDC_AUDIENCES} audiences"
        )));
    }
    let mut audiences = HashSet::with_capacity(issuer.audiences.len());
    if issuer.audiences.iter().any(|audience| {
        !valid_identifier(audience, |byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'/' | b'-')
        }) || !audiences.insert(audience.as_str())
    }) {
        return Err(PolicyError::Invalid(
            "OIDC audiences must be valid and unique".into(),
        ));
    }
    if issuer.clock_skew_seconds > MAX_CLOCK_SKEW_SECONDS
        || issuer.maximum_token_lifetime_seconds == 0
        || issuer.maximum_token_lifetime_seconds > MAX_TOKEN_LIFETIME_SECONDS
    {
        return Err(PolicyError::Invalid(format!(
            "OIDC clock skew must be at most {MAX_CLOCK_SKEW_SECONDS} seconds and token lifetime between 1 and {MAX_TOKEN_LIFETIME_SECONDS} seconds"
        )));
    }
    if issuer.keys.is_empty() || issuer.keys.len() > MAX_OIDC_KEYS_PER_ISSUER {
        return Err(PolicyError::Invalid(format!(
            "OIDC issuer must contain between 1 and {MAX_OIDC_KEYS_PER_ISSUER} keys"
        )));
    }
    let mut key_ids = HashSet::with_capacity(issuer.keys.len());
    for key in &issuer.keys {
        if !valid_policy_id(&key.kid) || !key_ids.insert(key.kid.as_str()) {
            return Err(PolicyError::Invalid(
                "OIDC key IDs must be valid and unique per issuer".into(),
            ));
        }
        if key.kty != "OKP" || key.crv != "Ed25519" {
            return Err(PolicyError::Invalid(
                "OIDC keys must use kty OKP and crv Ed25519".into(),
            ));
        }
        let decoded = decode_base64url_policy_value(&key.x, "OIDC Ed25519 public key")?;
        if decoded.len() != ED25519_PUBLIC_KEY_BYTES {
            return Err(PolicyError::Invalid(
                "OIDC Ed25519 public keys must contain exactly 32 bytes".into(),
            ));
        }
    }
    Ok(())
}

fn build_oidc_configuration(document: OidcDocument) -> Result<OidcConfiguration, PolicyError> {
    let issuers = document
        .issuers
        .into_iter()
        .map(|issuer| {
            let keys = issuer
                .keys
                .into_iter()
                .map(|key| {
                    let decoded = decode_base64url_policy_value(&key.x, "OIDC Ed25519 public key")?;
                    let public_key = decoded.try_into().map_err(|_| {
                        PolicyError::Invalid(
                            "OIDC Ed25519 public keys must contain exactly 32 bytes".into(),
                        )
                    })?;
                    Ok(OidcKey {
                        kid: key.kid,
                        public_key,
                    })
                })
                .collect::<Result<Vec<_>, PolicyError>>()?;
            Ok(OidcIssuer {
                issuer: issuer.issuer,
                audiences: issuer.audiences,
                clock_skew_seconds: issuer.clock_skew_seconds,
                maximum_token_lifetime_seconds: issuer.maximum_token_lifetime_seconds,
                keys,
            })
        })
        .collect::<Result<Vec<_>, PolicyError>>()?;
    Ok(OidcConfiguration {
        role_claim: document.role_claim,
        scope_claims: OidcScopeClaims {
            organization: document.scope_claims.organization,
            project: document.scope_claims.project,
            environment: document.scope_claims.environment,
            namespace: document.scope_claims.namespace,
        },
        roles: document
            .roles
            .into_iter()
            .map(|role| OidcRole {
                id: role.id,
                actions: role.actions,
            })
            .collect(),
        issuers,
        revoked_jti_fingerprints: document
            .revoked_jti_sha256
            .iter()
            .map(|fingerprint| decode_fingerprint(fingerprint))
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn validate_actions(label: &str, actions: &[Action]) -> Result<(), PolicyError> {
    if actions.is_empty() || actions.len() > MAX_ACTIONS {
        return Err(PolicyError::Invalid(format!(
            "{label} must contain between 1 and {MAX_ACTIONS} actions"
        )));
    }
    let mut unique = HashSet::with_capacity(actions.len());
    if actions.iter().any(|action| !unique.insert(*action)) {
        return Err(PolicyError::Invalid(format!("{label} repeats an action")));
    }
    Ok(())
}

fn valid_claim_name(value: &str) -> bool {
    valid_identifier(value, |byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
    })
}

fn valid_https_issuer(value: &str) -> bool {
    if !value.starts_with("https://")
        || value.len() > 256
        || value
            .bytes()
            .any(|byte| !(0x21..=0x7e).contains(&byte) || matches!(byte, b'?' | b'#' | b'\\'))
    {
        return false;
    }
    let Ok(parsed) = url::Url::parse(value) else {
        return false;
    };
    parsed.scheme() == "https"
        && parsed.host_str().is_some_and(valid_oidc_host)
        && parsed.username().is_empty()
        && parsed.password().is_none()
        && parsed.query().is_none()
        && parsed.fragment().is_none()
}

fn valid_oidc_host(host: &str) -> bool {
    if host.parse::<std::net::IpAddr>().is_ok() {
        return true;
    }
    !host.is_empty()
        && host.len() <= 253
        && host.split('.').all(|label| {
            let bytes = label.as_bytes();
            !bytes.is_empty()
                && bytes.len() <= 63
                && bytes[0].is_ascii_alphanumeric()
                && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
                && bytes
                    .iter()
                    .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'-')
        })
}

fn decode_base64url_policy_value(encoded: &str, label: &str) -> Result<Vec<u8>, PolicyError> {
    let decoded = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| PolicyError::Invalid(format!("{label} is not canonical base64url")))?;
    if URL_SAFE_NO_PAD.encode(&decoded) != encoded {
        return Err(PolicyError::Invalid(format!(
            "{label} is not canonical base64url"
        )));
    }
    Ok(decoded)
}

fn decode_base64url_segment(encoded: &str) -> Result<Vec<u8>, AuthenticationError> {
    let decoded = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| AuthenticationError::malformed())?;
    if URL_SAFE_NO_PAD.encode(&decoded) != encoded {
        return Err(AuthenticationError::malformed());
    }
    Ok(decoded)
}

fn unambiguous_object(encoded: &[u8]) -> Result<Map<String, Value>, AuthenticationError> {
    let UnambiguousJson(value) =
        serde_json::from_slice(encoded).map_err(|_| AuthenticationError::malformed())?;
    value
        .as_object()
        .cloned()
        .ok_or_else(AuthenticationError::malformed)
}

fn required_string_claim<'a>(
    claims: &'a Map<String, Value>,
    name: &str,
) -> Result<&'a str, AuthenticationError> {
    claims
        .get(name)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= 256)
        .ok_or_else(AuthenticationError::invalid)
}

fn required_u64_claim(claims: &Map<String, Value>, name: &str) -> Result<u64, AuthenticationError> {
    claims
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(AuthenticationError::invalid)
}

fn optional_u64_claim(
    claims: &Map<String, Value>,
    name: &str,
) -> Result<Option<u64>, AuthenticationError> {
    claims
        .get(name)
        .map(|value| value.as_u64().ok_or_else(AuthenticationError::invalid))
        .transpose()
}

fn required_string_array_claim<'a>(
    claims: &'a Map<String, Value>,
    name: &str,
    maximum: usize,
) -> Result<Vec<&'a str>, AuthenticationError> {
    let values = claims
        .get(name)
        .and_then(Value::as_array)
        .filter(|values| !values.is_empty() && values.len() <= maximum)
        .ok_or_else(AuthenticationError::invalid)?;
    let mut unique = HashSet::with_capacity(values.len());
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|value| valid_policy_id(value) && unique.insert(*value))
                .ok_or_else(AuthenticationError::invalid)
        })
        .collect()
}

fn validate_audience(
    claims: &Map<String, Value>,
    configured: &[String],
) -> Result<(), AuthenticationError> {
    let audiences = match claims.get("aud") {
        Some(Value::String(audience)) => vec![audience.as_str()],
        Some(Value::Array(values)) if !values.is_empty() && values.len() <= MAX_OIDC_AUDIENCES => {
            values
                .iter()
                .map(|value| value.as_str().ok_or_else(AuthenticationError::invalid))
                .collect::<Result<Vec<_>, _>>()?
        }
        _ => return Err(AuthenticationError::invalid()),
    };
    if audiences
        .iter()
        .any(|audience| configured.iter().any(|allowed| allowed == audience))
    {
        Ok(())
    } else {
        Err(AuthenticationError::invalid())
    }
}

fn validate_scope(principal: &str, scope: &ResourceScope) -> Result<(), PolicyError> {
    for (name, value) in [
        ("organization", scope.organization.as_str()),
        ("project", scope.project.as_str()),
        ("environment", scope.environment.as_str()),
        ("namespace", scope.namespace.as_str()),
    ] {
        if !valid_scope_value(value) {
            return Err(PolicyError::Invalid(format!(
                "principal {principal} has invalid {name} scope"
            )));
        }
    }
    Ok(())
}

fn valid_policy_id(value: &str) -> bool {
    valid_identifier(value, |byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-')
    })
}

fn valid_principal_id(value: &str) -> bool {
    valid_identifier(value, |byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'@' | b'/' | b'-')
    })
}

fn valid_scope_value(value: &str) -> bool {
    value == "*"
        || valid_identifier(value, |byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
        })
}

fn valid_identifier(value: &str, allowed: impl Fn(u8) -> bool) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && bytes[0].is_ascii_alphanumeric()
        && bytes.iter().copied().all(allowed)
}

fn decode_fingerprint(encoded: &str) -> Result<[u8; 32], PolicyError> {
    if encoded.len() != 64
        || encoded
            .bytes()
            .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
    {
        return Err(PolicyError::Invalid(
            "token_sha256 must be 64 lowercase hexadecimal characters".into(),
        ));
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in encoded.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(decoded)
}

fn hex_nibble(byte: u8) -> Result<u8, PolicyError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(PolicyError::Invalid(
            "token_sha256 contains invalid hexadecimal".into(),
        )),
    }
}

fn constant_time_eq(left: &[u8; 32], right: &[u8; 32]) -> bool {
    let mut difference = 0_u8;
    for (left_byte, right_byte) in left.iter().zip(right) {
        difference |= left_byte ^ right_byte;
    }
    difference == 0
}

fn encode_lower_hex(bytes: &[u8]) -> String {
    use fmt::Write as _;

    bytes.iter().fold(
        String::with_capacity(bytes.len().saturating_mul(2)),
        |mut encoded, byte| {
            write!(encoded, "{byte:02x}").expect("writing to a String cannot fail");
            encoded
        },
    )
}

fn scope_component_matches(granted: &str, target: &str) -> bool {
    granted == "*" || granted == target
}

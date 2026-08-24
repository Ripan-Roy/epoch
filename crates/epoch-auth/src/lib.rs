//! Bounded bootstrap identity and authorization contracts.
//!
//! This bootstrap format is a migration bridge to OIDC and workload mTLS. It
//! stores only SHA-256 token fingerprints, uses deny-by-default action/scope
//! evaluation, and never exposes credential material from authenticated
//! principals or errors.

use std::{
    collections::HashSet,
    fs::File,
    io::{Read, Take},
    path::Path,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const POLICY_FORMAT_VERSION: u32 = 1;
const MAX_POLICY_BYTES: u64 = 1 << 20;
const MAX_PRINCIPALS: usize = 256;
const MAX_ACTIONS: usize = 32;
const MAX_BEARER_HEADER: usize = 8 << 10;
const MAX_BEARER_TOKEN: usize = 4 << 10;
const MAX_AUDIT_FIELD_BYTES: usize = 256;

/// One stable authorization verb shared by Go and Rust.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum Action {
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
    principals: Vec<PrincipalDocument>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrincipalDocument {
    id: String,
    token_sha256: String,
    actions: Vec<Action>,
    scope: ResourceScope,
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
}

/// Authenticated immutable principal view.
#[derive(Debug, Clone)]
pub struct Principal {
    id: String,
    policy_id: String,
    actions: Vec<Action>,
    action_set: HashSet<Action>,
    scope: ResourceScope,
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
}

/// Stable authorization outcome emitted to audit logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionReason {
    PolicyGrant,
    ActionNotGranted,
    ScopeMismatch,
    MissingCredential,
    MalformedCredential,
    InvalidCredential,
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
    action: Action,
    decision: Decision,
    reason: DecisionReason,
    scope: ResourceScope,
}

/// Invalid or unbounded audit decision.
#[derive(Debug, Error)]
#[error("authorization audit event is invalid: {0}")]
pub struct AuditEventError(String);

impl DecisionEvent {
    /// Constructs one validated, bounded audit decision.
    pub fn new(
        request_id: impl Into<String>,
        principal_id: impl Into<String>,
        policy_id: impl Into<String>,
        action: Action,
        decision: Decision,
        reason: DecisionReason,
        scope: ResourceScope,
    ) -> Result<Self, AuditEventError> {
        let event = Self {
            request_id: request_id.into(),
            principal_id: principal_id.into(),
            policy_id: policy_id.into(),
            action,
            decision,
            reason,
            scope,
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
        for (name, value) in [
            ("organization", self.scope.organization.as_str()),
            ("project", self.scope.project.as_str()),
            ("environment", self.scope.environment.as_str()),
            ("namespace", self.scope.namespace.as_str()),
        ] {
            if value.len() > 128 {
                return Err(AuditEventError(format!("{name} scope exceeds 128 bytes")));
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
        let document: PolicyDocument = serde_json::from_slice(encoded)
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
        Ok(Self {
            id: document.policy_id,
            principals,
        })
    }

    /// Authenticates a strict Authorization header. Every configured
    /// fingerprint is scanned using a constant-time comparison.
    pub fn authenticate_bearer(
        &self,
        header: Option<&str>,
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
        })
    }

    /// Returns the stable policy identifier.
    pub fn id(&self) -> &str {
        &self.id
    }
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
    if document.format_version != POLICY_FORMAT_VERSION {
        return Err(PolicyError::Invalid(format!(
            "format_version must be {POLICY_FORMAT_VERSION}"
        )));
    }
    if !valid_policy_id(&document.policy_id) {
        return Err(PolicyError::Invalid("policy_id is invalid".into()));
    }
    if document.principals.is_empty() || document.principals.len() > MAX_PRINCIPALS {
        return Err(PolicyError::Invalid(format!(
            "policy must contain between 1 and {MAX_PRINCIPALS} principals"
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
    Ok(())
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

fn scope_component_matches(granted: &str, target: &str) -> bool {
    granted == "*" || granted == target
}

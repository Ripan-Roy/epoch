//! Leader-owned dispatch for API destinations, endpoint pools, functions, and connectors.
//!
//! Replicated state contains only references, leases, and outcomes. Secrets and
//! network I/O remain node-local. Stable idempotency keys make crash retries safe
//! for cooperating destinations.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{self, Formatter},
    fs::File,
    io::Read,
    path::Path,
    sync::{Arc, RwLock},
    time::Duration,
};

use epoch_bus::{
    CloudEventsMode, ConnectorBatchCommit, ConnectorDirection, ConnectorRecordResult,
    ConnectorStatus, DestinationAuth, EndpointRoute, EventIntegrationState, FunctionStatus,
    IntegrationOperation, SubscriptionTarget,
};
use epoch_consensus::ConsensusRole;
use epoch_core::Clock;
use epoch_tablet::{
    BusTabletCommand, BusTabletDelivery, BusTabletOperation, BusTabletOperationResult,
    BusTabletOutcome,
};
use reqwest::{
    StatusCode,
    header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;

use crate::{
    bus_tablet::BusTabletService,
    consensus::ConsensusProbeHandle,
    delivery_proposal::{ProposalRoute, propose_and_wait},
    tablet_materializer::{MaterializedTabletRoute, TabletDirectory},
    webhook_delivery::{safe_http_client_for_target, safe_http_target},
};

pub const DEFAULT_MANAGED_TARGET_DELIVERY_INTERVAL: Duration = Duration::from_millis(100);
pub const MAX_MANAGED_TARGET_DELIVERY_INTERVAL: Duration = Duration::from_mins(1);
const DISPATCHER: &str = "epoch-managed-target-v1";
const DISPATCHER_EPOCH: u64 = 1;
const MAX_SECRET_FILE_BYTES: u64 = 1024 * 1024;
const MAX_SECRETS: usize = 1_024;
const MAX_CONNECTOR_CREDENTIAL_PROPERTIES: usize = 64;
const MAX_SECRET_BYTES: usize = 64 * 1024;
const MAX_METADATA_BYTES: usize = 4 * 1024;
const MAX_OAUTH_RESPONSE_BYTES: usize = 128 * 1024;
const MAX_OAUTH_TOKEN_BYTES: usize = 64 * 1024;
const DEFAULT_OAUTH_TOKEN_TTL_MS: u64 = 5 * 60 * 1_000;
const OAUTH_EXPIRY_SAFETY_MS: u64 = 30 * 1_000;
const MAX_TARGET_TIMEOUT_MS: u64 = 30_000;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SecretFile {
    format_version: u16,
    secrets: Vec<SecretEntry>,
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum SecretEntry {
    ApiKey {
        reference: String,
        value: String,
        #[serde(default)]
        header: Option<String>,
    },
    Bearer {
        reference: String,
        token: String,
    },
    Oauth2Client {
        reference: String,
        client_id: String,
        client_secret: String,
        #[serde(default)]
        token_url: Option<String>,
        #[serde(default)]
        scopes: Vec<String>,
    },
    ConnectorCredentials {
        reference: String,
        values: BTreeMap<String, String>,
    },
}

impl SecretEntry {
    fn reference(&self) -> &str {
        match self {
            Self::ApiKey { reference, .. }
            | Self::Bearer { reference, .. }
            | Self::Oauth2Client { reference, .. }
            | Self::ConnectorCredentials { reference, .. } => reference,
        }
    }

    fn validate(&self) -> Result<(), ManagedTargetDeliveryError> {
        validate_identifier("secret reference", self.reference())?;
        match self {
            Self::ApiKey { value, header, .. } => {
                validate_secret("API key", value)?;
                if let Some(header) = header {
                    validate_header_name(header)?;
                }
            }
            Self::Bearer { token, .. } => validate_secret("bearer token", token)?,
            Self::Oauth2Client {
                client_id,
                client_secret,
                token_url,
                scopes,
                ..
            } => {
                validate_metadata("OAuth client ID", client_id)?;
                validate_secret("OAuth client secret", client_secret)?;
                if let Some(token_url) = token_url {
                    validate_external_url("OAuth token URL", token_url)?;
                }
                if scopes.len() > 64 {
                    return Err(ManagedTargetDeliveryError::Configuration(
                        "OAuth secret cannot contain more than 64 scopes".into(),
                    ));
                }
                for scope in scopes {
                    validate_metadata("OAuth scope", scope)?;
                }
            }
            Self::ConnectorCredentials { values, .. } => {
                if values.is_empty() || values.len() > MAX_CONNECTOR_CREDENTIAL_PROPERTIES {
                    return Err(ManagedTargetDeliveryError::Configuration(format!(
                        "connector credentials must contain 1-{MAX_CONNECTOR_CREDENTIAL_PROPERTIES} properties"
                    )));
                }
                for (key, value) in values {
                    validate_identifier("connector credential property", key)?;
                    validate_secret("connector credential value", value)?;
                }
            }
        }
        Ok(())
    }
}

impl fmt::Debug for SecretEntry {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagedSecret")
            .field("reference", &self.reference())
            .field("value", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Default)]
pub struct ManagedSecretStore {
    entries: BTreeMap<String, SecretEntry>,
}

impl fmt::Debug for ManagedSecretStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagedSecretStore")
            .field("references", &self.entries.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl ManagedSecretStore {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ManagedTargetDeliveryError> {
        let mut file = File::open(path.as_ref()).map_err(|error| {
            ManagedTargetDeliveryError::Configuration(format!(
                "managed-target secret file could not be read: {error}"
            ))
        })?;
        let mut bytes = Vec::new();
        file.by_ref()
            .take(MAX_SECRET_FILE_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| {
                ManagedTargetDeliveryError::Configuration(format!(
                    "managed-target secret file could not be read: {error}"
                ))
            })?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_SECRET_FILE_BYTES {
            return Err(ManagedTargetDeliveryError::Configuration(format!(
                "managed-target secret file exceeds {MAX_SECRET_FILE_BYTES} bytes"
            )));
        }
        let file: SecretFile = serde_json::from_slice(&bytes).map_err(|error| {
            ManagedTargetDeliveryError::Configuration(format!(
                "managed-target secret file is invalid: {error}"
            ))
        })?;
        if file.format_version != 1 || file.secrets.len() > MAX_SECRETS {
            return Err(ManagedTargetDeliveryError::Configuration(format!(
                "managed-target secret file must be format version 1 with at most {MAX_SECRETS} entries"
            )));
        }
        let mut entries = BTreeMap::new();
        for entry in file.secrets {
            entry.validate()?;
            let reference = entry.reference().to_owned();
            if entries.insert(reference.clone(), entry).is_some() {
                return Err(ManagedTargetDeliveryError::Configuration(format!(
                    "managed-target secret reference {reference} is duplicated"
                )));
            }
        }
        Ok(Self { entries })
    }

    fn get(&self, reference: &str) -> Option<&SecretEntry> {
        self.entries.get(reference)
    }

    pub(crate) fn connector_credentials(
        &self,
        reference: &str,
    ) -> Result<&BTreeMap<String, String>, ManagedTargetDeliveryError> {
        match self.get(reference) {
            Some(SecretEntry::ConnectorCredentials { values, .. }) => Ok(values),
            Some(_) => Err(ManagedTargetDeliveryError::Configuration(format!(
                "connector secret reference {reference} has the wrong kind"
            ))),
            None => Err(ManagedTargetDeliveryError::Configuration(format!(
                "connector secret reference {reference} is unavailable"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ManagedTargetDeliveryConfig {
    pub interval: Duration,
    pub allow_http_loopback: bool,
    pub secrets: Arc<ManagedSecretStore>,
}

impl Default for ManagedTargetDeliveryConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_MANAGED_TARGET_DELIVERY_INTERVAL,
            allow_http_loopback: false,
            secrets: Arc::new(ManagedSecretStore::default()),
        }
    }
}

impl ManagedTargetDeliveryConfig {
    pub fn validate(&self) -> Result<(), ManagedTargetDeliveryError> {
        if self.interval.is_zero() || self.interval > MAX_MANAGED_TARGET_DELIVERY_INTERVAL {
            return Err(ManagedTargetDeliveryError::Configuration(format!(
                "managed-target delivery interval must be between 1 ms and {} ms",
                MAX_MANAGED_TARGET_DELIVERY_INTERVAL.as_millis()
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ManagedTargetDeliveryPass {
    pub tablets_examined: u64,
    pub leaders_examined: u64,
    pub subscriptions_examined: u64,
    pub leases_acquired: u64,
    pub delivered: u64,
    pub retry_scheduled: u64,
    pub dead_lettered: u64,
    pub endpoint_failovers: u64,
    pub connector_checkpoints: u64,
    pub errors: u64,
}

#[derive(Default)]
pub struct ManagedTargetDeliveryStatus {
    enabled: bool,
    interval_ms: u64,
    passes: std::sync::atomic::AtomicU64,
    tablets_examined: std::sync::atomic::AtomicU64,
    leaders_examined: std::sync::atomic::AtomicU64,
    subscriptions_examined: std::sync::atomic::AtomicU64,
    leases_acquired: std::sync::atomic::AtomicU64,
    delivered: std::sync::atomic::AtomicU64,
    retry_scheduled: std::sync::atomic::AtomicU64,
    dead_lettered: std::sync::atomic::AtomicU64,
    endpoint_failovers: std::sync::atomic::AtomicU64,
    connector_checkpoints: std::sync::atomic::AtomicU64,
    errors: std::sync::atomic::AtomicU64,
    last_pass_at_ms: std::sync::atomic::AtomicU64,
    last_error: RwLock<Option<String>>,
}

impl fmt::Debug for ManagedTargetDeliveryStatus {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagedTargetDeliveryStatus")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl ManagedTargetDeliveryStatus {
    pub fn new(interval: Duration) -> Arc<Self> {
        Arc::new(Self {
            enabled: true,
            interval_ms: u64::try_from(interval.as_millis()).unwrap_or(u64::MAX),
            ..Self::default()
        })
    }

    pub fn record(&self, now_ms: u64, pass: ManagedTargetDeliveryPass, last_error: Option<String>) {
        use std::sync::atomic::Ordering;
        self.passes.fetch_add(1, Ordering::Relaxed);
        self.tablets_examined
            .fetch_add(pass.tablets_examined, Ordering::Relaxed);
        self.leaders_examined
            .fetch_add(pass.leaders_examined, Ordering::Relaxed);
        self.subscriptions_examined
            .fetch_add(pass.subscriptions_examined, Ordering::Relaxed);
        self.leases_acquired
            .fetch_add(pass.leases_acquired, Ordering::Relaxed);
        self.delivered.fetch_add(pass.delivered, Ordering::Relaxed);
        self.retry_scheduled
            .fetch_add(pass.retry_scheduled, Ordering::Relaxed);
        self.dead_lettered
            .fetch_add(pass.dead_lettered, Ordering::Relaxed);
        self.endpoint_failovers
            .fetch_add(pass.endpoint_failovers, Ordering::Relaxed);
        self.connector_checkpoints
            .fetch_add(pass.connector_checkpoints, Ordering::Relaxed);
        self.errors.fetch_add(pass.errors, Ordering::Relaxed);
        self.last_pass_at_ms.store(now_ms, Ordering::Relaxed);
        if let Some(last_error) = last_error
            && let Ok(mut error) = self.last_error.write()
        {
            *error = Some(last_error);
        }
    }

    pub fn snapshot(&self) -> ManagedTargetDeliveryStatusSnapshot {
        use std::sync::atomic::Ordering;
        ManagedTargetDeliveryStatusSnapshot {
            enabled: self.enabled,
            interval_ms: self.interval_ms,
            passes: self.passes.load(Ordering::Relaxed),
            tablets_examined: self.tablets_examined.load(Ordering::Relaxed),
            leaders_examined: self.leaders_examined.load(Ordering::Relaxed),
            subscriptions_examined: self.subscriptions_examined.load(Ordering::Relaxed),
            leases_acquired: self.leases_acquired.load(Ordering::Relaxed),
            delivered: self.delivered.load(Ordering::Relaxed),
            retry_scheduled: self.retry_scheduled.load(Ordering::Relaxed),
            dead_lettered: self.dead_lettered.load(Ordering::Relaxed),
            endpoint_failovers: self.endpoint_failovers.load(Ordering::Relaxed),
            connector_checkpoints: self.connector_checkpoints.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            last_pass_at_ms: self.last_pass_at_ms.load(Ordering::Relaxed),
            last_error: self.last_error.read().ok().and_then(|error| error.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ManagedTargetDeliveryStatusSnapshot {
    pub enabled: bool,
    pub interval_ms: u64,
    pub passes: u64,
    pub tablets_examined: u64,
    pub leaders_examined: u64,
    pub subscriptions_examined: u64,
    pub leases_acquired: u64,
    pub delivered: u64,
    pub retry_scheduled: u64,
    pub dead_lettered: u64,
    pub endpoint_failovers: u64,
    pub connector_checkpoints: u64,
    pub errors: u64,
    pub last_pass_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Error)]
pub enum ManagedTargetDeliveryError {
    #[error("invalid managed-target delivery configuration: {0}")]
    Configuration(String),
    #[error("managed-target delivery state is unavailable: {0}")]
    State(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AttemptDisposition {
    Acknowledge,
    Retry(String),
    Reject(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AuthenticationPlan {
    None,
    Destination(DestinationAuth),
    SecretReference(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedTarget {
    url: Url,
    authentication: AuthenticationPlan,
    cloud_events_mode: CloudEventsMode,
    timeout_ms: u64,
    identity_header: Option<(&'static str, String)>,
    endpoint_route: Option<EndpointRoute>,
    connector: Option<String>,
}

#[derive(Debug, Clone)]
struct OAuthToken {
    value: String,
    expires_at_ms: u64,
}

#[derive(Debug)]
pub struct ManagedTargetDeliveryWorker {
    config: ManagedTargetDeliveryConfig,
    commit_wait: Duration,
    oauth_tokens: RwLock<BTreeMap<String, OAuthToken>>,
}

pub(crate) struct ConnectorSourceFetch<'a> {
    pub target: &'a Url,
    pub secret_reference: Option<&'a str>,
    pub connector_identity: &'a str,
    pub source_position: &'a str,
    pub timeout: Duration,
    pub maximum_response_bytes: usize,
    pub now_ms: u64,
}

impl ManagedTargetDeliveryWorker {
    pub fn new(
        config: ManagedTargetDeliveryConfig,
        commit_wait: Duration,
    ) -> Result<Self, ManagedTargetDeliveryError> {
        config.validate()?;
        if commit_wait.is_zero() {
            return Err(ManagedTargetDeliveryError::Configuration(
                "managed-target proposal commit wait must be non-zero".into(),
            ));
        }
        Ok(Self {
            config,
            commit_wait,
            oauth_tokens: RwLock::new(BTreeMap::new()),
        })
    }

    pub const fn config(&self) -> &ManagedTargetDeliveryConfig {
        &self.config
    }

    /// Fetches one bounded source-connector batch through the same credential,
    /// DNS pinning, redirect, proxy, and timeout boundary as managed targets.
    pub(crate) async fn fetch_connector_source(
        &self,
        request: ConnectorSourceFetch<'_>,
    ) -> Result<Option<Vec<u8>>, AttemptDisposition> {
        let mut headers = HeaderMap::new();
        insert_header(&mut headers, "accept", "application/json")
            .map_err(AttemptDisposition::Reject)?;
        insert_header(
            &mut headers,
            "epoch-connector-identity",
            request.connector_identity,
        )
        .map_err(AttemptDisposition::Reject)?;
        insert_header(
            &mut headers,
            "epoch-connector-position",
            request.source_position,
        )
        .map_err(AttemptDisposition::Reject)?;
        if let Some(reference) = request.secret_reference {
            self.apply_authentication(
                &mut headers,
                &AuthenticationPlan::SecretReference(reference.to_owned()),
                request.now_ms,
                request.timeout,
            )
            .await?;
        }
        let client = safe_http_client_for_target(request.target, self.config.allow_http_loopback)
            .await
            .map_err(|reason| {
                if reason.starts_with("dns_") || reason == "request_client_failed" {
                    AttemptDisposition::Retry(reason)
                } else {
                    AttemptDisposition::Reject(reason)
                }
            })?;
        let response = tokio::time::timeout(
            request.timeout,
            client
                .get(request.target.clone())
                .headers(headers)
                .timeout(request.timeout)
                .send(),
        )
        .await
        .map_err(|_| AttemptDisposition::Retry("source_request_timeout".into()))?
        .map_err(|error| AttemptDisposition::Retry(request_reason(&error)))?;
        if response.status() == StatusCode::NO_CONTENT {
            return Ok(None);
        }
        if response.status() != StatusCode::OK {
            return Err(classify_status(response.status()));
        }
        read_bounded_response_as(response, request.maximum_response_bytes, "source_response")
            .await
            .map(Some)
    }

    async fn execute(
        &self,
        delivery: &BusTabletDelivery,
        target: &ResolvedTarget,
        now_ms: u64,
    ) -> AttemptDisposition {
        let remaining_ms = match delivery.lease_deadline_ms.checked_sub(now_ms) {
            Some(remaining) if remaining > 0 => remaining,
            _ => return AttemptDisposition::Retry("delivery_lease_expired".into()),
        };
        let timeout = Duration::from_millis(remaining_ms.min(target.timeout_ms));
        let (body, mut headers) = match prepare_cloud_event(delivery, target.cloud_events_mode) {
            Ok(prepared) => prepared,
            Err(reason) => return AttemptDisposition::Reject(reason),
        };
        if let Some((name, value)) = &target.identity_header
            && let Err(reason) = insert_header(&mut headers, name, value)
        {
            return AttemptDisposition::Reject(reason);
        }
        if let Err(reason) = insert_header(
            &mut headers,
            "idempotency-key",
            &target_idempotency_key(&delivery.delivery_id),
        ) {
            return AttemptDisposition::Reject(reason);
        }
        if let Err(disposition) = self
            .apply_authentication(&mut headers, &target.authentication, now_ms, timeout)
            .await
        {
            return disposition;
        }
        let client =
            match safe_http_client_for_target(&target.url, self.config.allow_http_loopback).await {
                Ok(client) => client,
                Err(reason) if reason.starts_with("dns_") || reason == "request_client_failed" => {
                    return AttemptDisposition::Retry(reason);
                }
                Err(reason) => return AttemptDisposition::Reject(reason),
            };
        match tokio::time::timeout(
            timeout,
            client
                .post(target.url.clone())
                .headers(headers)
                .body(body)
                .timeout(timeout)
                .send(),
        )
        .await
        {
            Err(_) => AttemptDisposition::Retry("request_timeout".into()),
            Ok(Err(error)) => AttemptDisposition::Retry(request_reason(&error)),
            Ok(Ok(response)) => classify_status(response.status()),
        }
    }

    async fn apply_authentication(
        &self,
        headers: &mut HeaderMap,
        plan: &AuthenticationPlan,
        now_ms: u64,
        timeout: Duration,
    ) -> Result<(), AttemptDisposition> {
        match plan {
            AuthenticationPlan::None | AuthenticationPlan::Destination(DestinationAuth::None) => {
                Ok(())
            }
            AuthenticationPlan::Destination(DestinationAuth::ApiKey { secret_ref, header }) => {
                let SecretEntry::ApiKey { value, .. } = self.secret(secret_ref)? else {
                    return Err(AttemptDisposition::Retry("secret_kind_mismatch".into()));
                };
                insert_header(headers, header, value).map_err(AttemptDisposition::Reject)
            }
            AuthenticationPlan::Destination(DestinationAuth::OAuth2 {
                secret_ref,
                token_url,
                scopes,
            }) => {
                let token = self
                    .oauth_token(secret_ref, Some(token_url), scopes, now_ms, timeout)
                    .await?;
                insert_bearer(headers, &token).map_err(AttemptDisposition::Reject)
            }
            AuthenticationPlan::SecretReference(reference) => match self.secret(reference)? {
                SecretEntry::ApiKey { value, header, .. } => {
                    insert_header(headers, header.as_deref().unwrap_or("x-api-key"), value)
                        .map_err(AttemptDisposition::Reject)
                }
                SecretEntry::Bearer { token, .. } => {
                    insert_bearer(headers, token).map_err(AttemptDisposition::Reject)
                }
                SecretEntry::Oauth2Client {
                    token_url, scopes, ..
                } => {
                    let token = self
                        .oauth_token(reference, token_url.as_deref(), scopes, now_ms, timeout)
                        .await?;
                    insert_bearer(headers, &token).map_err(AttemptDisposition::Reject)
                }
                SecretEntry::ConnectorCredentials { .. } => {
                    Err(AttemptDisposition::Retry("secret_kind_mismatch".into()))
                }
            },
        }
    }

    fn secret(&self, reference: &str) -> Result<&SecretEntry, AttemptDisposition> {
        self.config
            .secrets
            .get(reference)
            .ok_or_else(|| AttemptDisposition::Retry("secret_unavailable".into()))
    }

    async fn oauth_token(
        &self,
        reference: &str,
        explicit_token_url: Option<&str>,
        explicit_scopes: &[String],
        now_ms: u64,
        timeout: Duration,
    ) -> Result<String, AttemptDisposition> {
        let SecretEntry::Oauth2Client {
            client_id,
            client_secret,
            token_url: stored_url,
            scopes: stored_scopes,
            ..
        } = self.secret(reference)?
        else {
            return Err(AttemptDisposition::Retry("secret_kind_mismatch".into()));
        };
        let token_url = explicit_token_url
            .or(stored_url.as_deref())
            .ok_or_else(|| AttemptDisposition::Retry("oauth_token_url_unavailable".into()))?;
        let scopes = if explicit_scopes.is_empty() {
            stored_scopes
        } else {
            explicit_scopes
        };
        let cache_key = oauth_cache_key(reference, token_url, scopes);
        if let Some(token) = self
            .oauth_tokens
            .read()
            .ok()
            .and_then(|tokens| tokens.get(&cache_key).cloned())
            .filter(|token| token.expires_at_ms > now_ms.saturating_add(OAUTH_EXPIRY_SAFETY_MS))
        {
            return Ok(token.value);
        }
        let target = safe_http_target(token_url, self.config.allow_http_loopback)
            .map_err(AttemptDisposition::Reject)?;
        let client = safe_http_client_for_target(&target, self.config.allow_http_loopback)
            .await
            .map_err(|reason| {
                if reason.starts_with("dns_") || reason == "request_client_failed" {
                    AttemptDisposition::Retry(reason)
                } else {
                    AttemptDisposition::Reject(reason)
                }
            })?;
        let mut form = vec![("grant_type", "client_credentials".to_owned())];
        if !scopes.is_empty() {
            form.push(("scope", scopes.join(" ")));
        }
        let response = tokio::time::timeout(
            timeout,
            client
                .post(target)
                .basic_auth(client_id, Some(client_secret))
                .form(&form)
                .timeout(timeout)
                .send(),
        )
        .await
        .map_err(|_| AttemptDisposition::Retry("oauth_request_timeout".into()))?
        .map_err(|_| AttemptDisposition::Retry("oauth_request_failed".into()))?;
        if !response.status().is_success() {
            return Err(
                if response.status().is_server_error()
                    || response.status() == StatusCode::TOO_MANY_REQUESTS
                {
                    AttemptDisposition::Retry("oauth_service_unavailable".into())
                } else {
                    AttemptDisposition::Reject("oauth_credentials_rejected".into())
                },
            );
        }
        let bytes = read_bounded_response(response, MAX_OAUTH_RESPONSE_BYTES).await?;
        let response: OAuthTokenResponse = serde_json::from_slice(&bytes)
            .map_err(|_| AttemptDisposition::Retry("oauth_response_invalid".into()))?;
        if response.access_token.is_empty()
            || response.access_token.len() > MAX_OAUTH_TOKEN_BYTES
            || response
                .token_type
                .as_deref()
                .is_some_and(|kind| !kind.eq_ignore_ascii_case("bearer"))
        {
            return Err(AttemptDisposition::Retry("oauth_response_invalid".into()));
        }
        let ttl_ms = response
            .expires_in
            .and_then(|seconds| seconds.checked_mul(1_000))
            .unwrap_or(DEFAULT_OAUTH_TOKEN_TTL_MS);
        if let Ok(mut tokens) = self.oauth_tokens.write() {
            tokens.insert(
                cache_key,
                OAuthToken {
                    value: response.access_token.clone(),
                    expires_at_ms: now_ms.saturating_add(ttl_ms),
                },
            );
        }
        Ok(response.access_token)
    }
}

#[derive(Debug, Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
    #[serde(default)]
    token_type: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
}

pub async fn run_managed_target_delivery_pass(
    directory: &TabletDirectory,
    worker: &ManagedTargetDeliveryWorker,
    clock: &dyn Clock,
) -> (ManagedTargetDeliveryPass, Option<String>) {
    let routes = match directory.routes() {
        Ok(routes) => routes,
        Err(error) => {
            return (
                ManagedTargetDeliveryPass {
                    errors: 1,
                    ..ManagedTargetDeliveryPass::default()
                },
                Some(error.to_string()),
            );
        }
    };
    let mut pass = ManagedTargetDeliveryPass::default();
    let mut last_error = None;
    for route in routes {
        pass.tablets_examined = pass.tablets_examined.saturating_add(1);
        let Some(service) = route.bus_service() else {
            continue;
        };
        if let Err(error) = dispatch_route(&route, &service, worker, clock, &mut pass).await {
            pass.errors = pass.errors.saturating_add(1);
            last_error = Some(error.to_string());
        }
    }
    (pass, last_error)
}

async fn dispatch_route(
    route: &MaterializedTabletRoute,
    service: &BusTabletService,
    worker: &ManagedTargetDeliveryWorker,
    clock: &dyn Clock,
    pass: &mut ManagedTargetDeliveryPass,
) -> Result<(), ManagedTargetDeliveryError> {
    let consensus = route.consensus();
    let status = consensus
        .status()
        .await
        .map_err(|error| ManagedTargetDeliveryError::State(error.to_string()))?;
    if status.role != ConsensusRole::Leader || status.fail_stopped {
        return Ok(());
    }
    pass.leaders_examined = pass.leaders_examined.saturating_add(1);
    let now_ms = clock.wall_time_ms();
    let candidates = service
        .managed_target_delivery_candidates(now_ms)
        .map_err(ManagedTargetDeliveryError::State)?;
    for candidate in candidates {
        pass.subscriptions_examined = pass.subscriptions_examined.saturating_add(1);
        dispatch_candidate(&consensus, service, worker, clock, pass, candidate, now_ms).await?;
    }
    Ok(())
}

async fn dispatch_candidate(
    consensus: &ConsensusProbeHandle,
    service: &BusTabletService,
    worker: &ManagedTargetDeliveryWorker,
    clock: &dyn Clock,
    pass: &mut ManagedTargetDeliveryPass,
    candidate: epoch_bus::ManagedTargetDeliveryCandidate,
    observed_at_ms: u64,
) -> Result<(), ManagedTargetDeliveryError> {
    let Some(delivery) =
        acquire_candidate(consensus, service, worker, candidate, observed_at_ms).await?
    else {
        return Ok(());
    };
    pass.leases_acquired = pass.leases_acquired.saturating_add(1);
    let integration = service
        .integration_state()
        .map_err(ManagedTargetDeliveryError::State)?;
    let resolution = resolve_target(
        &delivery,
        &integration,
        worker.config.allow_http_loopback,
        clock.wall_time_ms(),
    );
    let (target, disposition) = match resolution {
        Ok(target) => {
            let disposition = worker
                .execute(&delivery, &target, clock.wall_time_ms())
                .await;
            (Some(target), disposition)
        }
        Err(disposition) => (None, disposition),
    };

    if let Some(target) = &target
        && should_failover(&disposition)
        && let Some(endpoint) = &target.endpoint_route
    {
        observe_failed_endpoint(consensus, service, worker, clock, endpoint, &delivery).await?;
        pass.endpoint_failovers = pass.endpoint_failovers.saturating_add(1);
    }
    let connector_checkpoint = target
        .as_ref()
        .and_then(|target| target.connector.as_deref())
        .or_else(|| match &delivery.target {
            SubscriptionTarget::Connector { resource }
                if integration
                    .connectors()
                    .connector(resource)
                    .is_some_and(|connector| connector.status == ConnectorStatus::Active) =>
            {
                Some(resource.as_str())
            }
            _ => None,
        });
    if let Some(connector) = connector_checkpoint {
        commit_connector_outcome(
            consensus,
            service,
            worker,
            clock,
            connector,
            &delivery,
            &disposition,
            &integration,
        )
        .await?;
        pass.connector_checkpoints = pass.connector_checkpoints.saturating_add(1);
    }

    settle_source(
        consensus,
        service,
        worker,
        clock,
        pass,
        &delivery,
        sanitize_disposition(disposition),
    )
    .await
}

async fn acquire_candidate(
    consensus: &ConsensusProbeHandle,
    service: &BusTabletService,
    worker: &ManagedTargetDeliveryWorker,
    candidate: epoch_bus::ManagedTargetDeliveryCandidate,
    observed_at_ms: u64,
) -> Result<Option<BusTabletDelivery>, ManagedTargetDeliveryError> {
    let status = consensus
        .status()
        .await
        .map_err(|error| ManagedTargetDeliveryError::State(error.to_string()))?;
    if status.role != ConsensusRole::Leader || status.fail_stopped {
        return Ok(None);
    }
    let acquire_time = observed_at_ms.max(
        service
            .last_applied_time_ms()
            .map_err(ManagedTargetDeliveryError::State)?,
    );
    let command = BusTabletCommand::new(
        service.scope(),
        attempt_key(
            "managed-target-acquire",
            &candidate.delivery_id,
            candidate.next_attempt,
        ),
        acquire_time,
        BusTabletOperation::AcquireDeliveries {
            subscription: candidate.subscription,
            dispatcher: DISPATCHER.into(),
            dispatcher_epoch: DISPATCHER_EPOCH,
            max_deliveries: 1,
            expected_delivery_id: Some(candidate.delivery_id),
            destination: None,
        },
    )
    .map_err(|error| ManagedTargetDeliveryError::State(error.to_string()))?;
    let receipt = propose_bus_command(
        consensus,
        service,
        command,
        status.term.get(),
        worker.commit_wait,
        "managed-target acquire",
    )
    .await?;
    let BusTabletOutcome::Applied {
        result: BusTabletOperationResult::DeliveriesAcquired { deliveries },
    } = receipt.outcome
    else {
        return Ok(None);
    };
    Ok(deliveries.into_iter().next())
}

fn resolve_target(
    delivery: &BusTabletDelivery,
    integration: &EventIntegrationState,
    allow_http_loopback: bool,
    now_ms: u64,
) -> Result<ResolvedTarget, AttemptDisposition> {
    let lease_timeout_ms = delivery
        .lease_deadline_ms
        .saturating_sub(now_ms)
        .clamp(1, MAX_TARGET_TIMEOUT_MS);
    match &delivery.target {
        SubscriptionTarget::ApiDestination {
            url,
            auth,
            cloud_events_mode,
        } => Ok(ResolvedTarget {
            url: resolve_safe_url(url, allow_http_loopback)?,
            authentication: AuthenticationPlan::Destination(auth.clone()),
            cloud_events_mode: *cloud_events_mode,
            timeout_ms: lease_timeout_ms,
            identity_header: None,
            endpoint_route: None,
            connector: None,
        }),
        SubscriptionTarget::EndpointPool {
            pool,
            auth,
            cloud_events_mode,
        } => {
            let route = integration
                .endpoints()
                .route(pool)
                .map_err(|_| AttemptDisposition::Retry("endpoint_pool_unavailable".into()))?;
            Ok(ResolvedTarget {
                url: resolve_safe_url(&route.endpoint, allow_http_loopback)?,
                authentication: AuthenticationPlan::Destination(auth.clone()),
                cloud_events_mode: *cloud_events_mode,
                timeout_ms: lease_timeout_ms,
                identity_header: Some(("epoch-endpoint-pool", pool.clone())),
                endpoint_route: Some(route),
                connector: None,
            })
        }
        SubscriptionTarget::Function { resource } => resolve_function_target(
            resource,
            delivery,
            integration,
            allow_http_loopback,
            lease_timeout_ms,
        ),
        SubscriptionTarget::Connector { resource } => {
            resolve_connector_target(resource, integration, allow_http_loopback, lease_timeout_ms)
        }
        SubscriptionTarget::Pull
        | SubscriptionTarget::Queue { .. }
        | SubscriptionTarget::Stream { .. }
        | SubscriptionTarget::Webhook { .. }
        | SubscriptionTarget::Http { .. } => Err(AttemptDisposition::Reject(
            "managed_target_kind_invalid".into(),
        )),
    }
}

fn resolve_function_target(
    resource: &str,
    delivery: &BusTabletDelivery,
    integration: &EventIntegrationState,
    allow_http_loopback: bool,
    lease_timeout_ms: u64,
) -> Result<ResolvedTarget, AttemptDisposition> {
    let function = integration
        .function(resource)
        .ok_or_else(|| AttemptDisposition::Retry("function_unavailable".into()))?;
    if function.status != FunctionStatus::Active {
        return Err(AttemptDisposition::Retry("function_paused".into()));
    }
    let payload_size = serde_json::to_vec(&delivery.envelope.payload)
        .map_err(|_| AttemptDisposition::Reject("function_input_invalid".into()))?
        .len();
    if payload_size > function.definition.max_input_bytes {
        return Err(AttemptDisposition::Reject(
            "function_input_too_large".into(),
        ));
    }
    let url = resolve_safe_url(&function.definition.endpoint, allow_http_loopback)?;
    enforce_allowlist(&url, &function.definition.outbound_allowlist, "function")?;
    Ok(ResolvedTarget {
        url,
        authentication: function.definition.secret_ref.clone().map_or(
            AuthenticationPlan::None,
            AuthenticationPlan::SecretReference,
        ),
        cloud_events_mode: CloudEventsMode::Structured,
        timeout_ms: function.definition.timeout_ms.min(lease_timeout_ms),
        identity_header: Some((
            "epoch-function-identity",
            function.definition.identity.clone(),
        )),
        endpoint_route: None,
        connector: None,
    })
}

fn resolve_connector_target(
    resource: &str,
    integration: &EventIntegrationState,
    allow_http_loopback: bool,
    lease_timeout_ms: u64,
) -> Result<ResolvedTarget, AttemptDisposition> {
    let connector = integration
        .connectors()
        .connector(resource)
        .ok_or_else(|| AttemptDisposition::Retry("connector_unavailable".into()))?;
    if connector.status != ConnectorStatus::Active {
        return Err(AttemptDisposition::Retry("connector_paused".into()));
    }
    if connector.spec.direction == ConnectorDirection::Source {
        return Err(AttemptDisposition::Reject(
            "connector_direction_invalid".into(),
        ));
    }
    let endpoint = connector
        .spec
        .config
        .get("endpoint")
        .ok_or_else(|| AttemptDisposition::Reject("connector_endpoint_missing".into()))?;
    let url = resolve_safe_url(endpoint, allow_http_loopback)?;
    enforce_allowlist(&url, &connector.spec.outbound_allowlist, "connector")?;
    let timeout_ms = connector_timeout_ms(&connector.spec.config)?;
    let cloud_events_mode = connector_cloud_events_mode(&connector.spec.config)?;
    let authentication = match connector.spec.secret_refs.len() {
        0 => AuthenticationPlan::None,
        1 => AuthenticationPlan::SecretReference(
            connector
                .spec
                .secret_refs
                .iter()
                .next()
                .cloned()
                .unwrap_or_default(),
        ),
        _ => {
            return Err(AttemptDisposition::Reject(
                "connector_auth_ambiguous".into(),
            ));
        }
    };
    Ok(ResolvedTarget {
        url,
        authentication,
        cloud_events_mode,
        timeout_ms: timeout_ms.min(lease_timeout_ms),
        identity_header: Some(("epoch-connector-identity", connector.spec.identity.clone())),
        endpoint_route: None,
        connector: Some(resource.into()),
    })
}

fn connector_timeout_ms(config: &BTreeMap<String, String>) -> Result<u64, AttemptDisposition> {
    let timeout_ms = config
        .get("timeout_ms")
        .map_or(Ok(MAX_TARGET_TIMEOUT_MS), |value| {
            value
                .parse::<u64>()
                .map_err(|_| AttemptDisposition::Reject("connector_timeout_invalid".into()))
        })?;
    if timeout_ms == 0 || timeout_ms > MAX_TARGET_TIMEOUT_MS {
        return Err(AttemptDisposition::Reject(
            "connector_timeout_invalid".into(),
        ));
    }
    Ok(timeout_ms)
}

fn connector_cloud_events_mode(
    config: &BTreeMap<String, String>,
) -> Result<CloudEventsMode, AttemptDisposition> {
    match config.get("cloud_events_mode").map(String::as_str) {
        None | Some("structured") => Ok(CloudEventsMode::Structured),
        Some("binary") => Ok(CloudEventsMode::Binary),
        Some(_) => Err(AttemptDisposition::Reject(
            "connector_cloud_events_mode_invalid".into(),
        )),
    }
}

fn resolve_safe_url(value: &str, allow_http_loopback: bool) -> Result<Url, AttemptDisposition> {
    safe_http_target(value, allow_http_loopback)
        .map_err(|_| AttemptDisposition::Reject("unsafe_target".into()))
}

pub(crate) fn enforce_allowlist(
    url: &Url,
    allowlist: &BTreeSet<String>,
    target_kind: &str,
) -> Result<(), AttemptDisposition> {
    let host = url
        .host_str()
        .ok_or_else(|| AttemptDisposition::Reject("unsafe_target".into()))?;
    if allowlist
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(host))
    {
        Ok(())
    } else {
        Err(AttemptDisposition::Reject(format!(
            "{target_kind}_egress_denied"
        )))
    }
}

async fn observe_failed_endpoint(
    consensus: &ConsensusProbeHandle,
    service: &BusTabletService,
    worker: &ManagedTargetDeliveryWorker,
    clock: &dyn Clock,
    endpoint: &EndpointRoute,
    delivery: &BusTabletDelivery,
) -> Result<(), ManagedTargetDeliveryError> {
    apply_integration(
        consensus,
        service,
        worker.commit_wait,
        clock,
        attempt_key(
            "managed-endpoint-failover",
            &delivery.delivery_id,
            delivery.attempt,
        ),
        IntegrationOperation::ObserveEndpoint {
            observation: epoch_bus::EndpointObservation {
                pool: endpoint.pool.clone(),
                endpoint: endpoint.endpoint.clone(),
                region: endpoint.region.clone(),
                priority: endpoint.priority,
                healthy: false,
                observed_at_ms: 0,
            },
        },
        "managed endpoint failover",
    )
    .await
}

#[allow(
    clippy::too_many_arguments,
    reason = "checkpointing binds source state, delivery identity, and external outcome"
)]
async fn commit_connector_outcome(
    consensus: &ConsensusProbeHandle,
    service: &BusTabletService,
    worker: &ManagedTargetDeliveryWorker,
    clock: &dyn Clock,
    connector: &str,
    delivery: &BusTabletDelivery,
    disposition: &AttemptDisposition,
    integration: &EventIntegrationState,
) -> Result<(), ManagedTargetDeliveryError> {
    let result = match disposition {
        AttemptDisposition::Acknowledge => ConnectorRecordResult::Applied {
            record_id: delivery.envelope.id.clone(),
        },
        AttemptDisposition::Retry(reason) => ConnectorRecordResult::RetryableFailure {
            record_id: delivery.envelope.id.clone(),
            reason: sanitize_reason(reason, "target_retry"),
        },
        AttemptDisposition::Reject(reason) => ConnectorRecordResult::RoutedToError {
            record_id: delivery.envelope.id.clone(),
            reason: sanitize_reason(reason, "target_rejected"),
        },
    };
    let source_from = integration.connectors().checkpoint(connector).map_or_else(
        || delivery.publish_position.saturating_sub(1).to_string(),
        |checkpoint| checkpoint.source_position.clone(),
    );
    apply_integration(
        consensus,
        service,
        worker.commit_wait,
        clock,
        attempt_key(
            "managed-connector-commit",
            &delivery.delivery_id,
            delivery.attempt,
        ),
        IntegrationOperation::CommitConnectorBatch {
            name: connector.to_owned(),
            commit: ConnectorBatchCommit {
                batch_id: attempt_key(
                    "managed-connector-batch",
                    &delivery.delivery_id,
                    delivery.attempt,
                ),
                source_from,
                source_to: delivery.publish_position.to_string(),
                target_idempotency_key: target_idempotency_key(&delivery.delivery_id),
                records: vec![result],
                committed_at_ms: 0,
            },
        },
        "managed connector checkpoint",
    )
    .await
}

async fn apply_integration(
    consensus: &ConsensusProbeHandle,
    service: &BusTabletService,
    commit_wait: Duration,
    clock: &dyn Clock,
    idempotency_key: String,
    operation: IntegrationOperation,
    label: &'static str,
) -> Result<(), ManagedTargetDeliveryError> {
    let status = consensus
        .status()
        .await
        .map_err(|error| ManagedTargetDeliveryError::State(error.to_string()))?;
    if status.role != ConsensusRole::Leader || status.fail_stopped {
        return Err(ManagedTargetDeliveryError::State(format!(
            "{label} lost source leadership"
        )));
    }
    let applied_at_ms = clock.wall_time_ms().max(
        service
            .last_applied_time_ms()
            .map_err(ManagedTargetDeliveryError::State)?,
    );
    let command = BusTabletCommand::new(
        service.scope(),
        idempotency_key,
        applied_at_ms,
        BusTabletOperation::ApplyIntegration {
            operation: Box::new(operation),
        },
    )
    .map_err(|error| ManagedTargetDeliveryError::State(error.to_string()))?;
    let receipt = propose_bus_command(
        consensus,
        service,
        command,
        status.term.get(),
        commit_wait,
        label,
    )
    .await?;
    if matches!(
        receipt.outcome,
        BusTabletOutcome::Applied {
            result: BusTabletOperationResult::IntegrationApplied { .. }
        }
    ) {
        Ok(())
    } else {
        Err(ManagedTargetDeliveryError::State(format!(
            "{label} was rejected"
        )))
    }
}

async fn settle_source(
    consensus: &ConsensusProbeHandle,
    service: &BusTabletService,
    worker: &ManagedTargetDeliveryWorker,
    clock: &dyn Clock,
    pass: &mut ManagedTargetDeliveryPass,
    delivery: &BusTabletDelivery,
    disposition: AttemptDisposition,
) -> Result<(), ManagedTargetDeliveryError> {
    let status = consensus
        .status()
        .await
        .map_err(|error| ManagedTargetDeliveryError::State(error.to_string()))?;
    if status.role != ConsensusRole::Leader || status.fail_stopped {
        return Ok(());
    }
    let (operation, label) = settlement_operation(delivery, disposition);
    let command = BusTabletCommand::new(
        service.scope(),
        attempt_key(label, &delivery.delivery_id, delivery.attempt),
        clock.wall_time_ms().max(
            service
                .last_applied_time_ms()
                .map_err(ManagedTargetDeliveryError::State)?,
        ),
        operation,
    )
    .map_err(|error| ManagedTargetDeliveryError::State(error.to_string()))?;
    let receipt = propose_bus_command(
        consensus,
        service,
        command,
        status.term.get(),
        worker.commit_wait,
        label,
    )
    .await?;
    match receipt.outcome {
        BusTabletOutcome::Applied {
            result: BusTabletOperationResult::DeliveryAcknowledged { .. },
        } => pass.delivered = pass.delivered.saturating_add(1),
        BusTabletOutcome::Applied {
            result: BusTabletOperationResult::DeliveryFailed { .. },
        } => pass.retry_scheduled = pass.retry_scheduled.saturating_add(1),
        BusTabletOutcome::Applied {
            result: BusTabletOperationResult::DeliveryRejected { .. },
        } => pass.dead_lettered = pass.dead_lettered.saturating_add(1),
        BusTabletOutcome::Applied { .. } | BusTabletOutcome::Rejected { .. } => {}
    }
    Ok(())
}

fn settlement_operation(
    delivery: &BusTabletDelivery,
    disposition: AttemptDisposition,
) -> (BusTabletOperation, &'static str) {
    match disposition {
        AttemptDisposition::Acknowledge => (
            BusTabletOperation::AcknowledgeDelivery {
                delivery_id: delivery.delivery_id.clone(),
                dispatcher: DISPATCHER.into(),
                dispatcher_epoch: DISPATCHER_EPOCH,
                lease_token: delivery.lease_token.clone(),
            },
            "managed-target-ack",
        ),
        AttemptDisposition::Retry(reason) => (
            BusTabletOperation::FailDelivery {
                delivery_id: delivery.delivery_id.clone(),
                dispatcher: DISPATCHER.into(),
                dispatcher_epoch: DISPATCHER_EPOCH,
                lease_token: delivery.lease_token.clone(),
                reason,
            },
            "managed-target-retry",
        ),
        AttemptDisposition::Reject(reason) => (
            BusTabletOperation::RejectDelivery {
                delivery_id: delivery.delivery_id.clone(),
                dispatcher: DISPATCHER.into(),
                dispatcher_epoch: DISPATCHER_EPOCH,
                lease_token: delivery.lease_token.clone(),
                reason,
            },
            "managed-target-reject",
        ),
    }
}

async fn propose_bus_command(
    consensus: &ConsensusProbeHandle,
    service: &BusTabletService,
    command: BusTabletCommand,
    expected_term: u64,
    commit_wait: Duration,
    label: &'static str,
) -> Result<epoch_tablet::BusTabletReceipt, ManagedTargetDeliveryError> {
    let proposal_id = command
        .proposal_id(service.scope())
        .map_err(|error| ManagedTargetDeliveryError::State(error.to_string()))?;
    let payload = command
        .encode(service.scope())
        .map_err(|error| ManagedTargetDeliveryError::State(error.to_string()))?;
    let committed = propose_and_wait(
        consensus,
        proposal_id,
        expected_term,
        payload,
        commit_wait,
        label,
        ProposalRoute::LeaderOnly,
    )
    .await
    .map_err(ManagedTargetDeliveryError::State)?;
    service
        .committed_receipt(&committed)
        .map_err(ManagedTargetDeliveryError::State)
}

fn prepare_cloud_event(
    delivery: &BusTabletDelivery,
    mode: CloudEventsMode,
) -> Result<(Vec<u8>, HeaderMap), String> {
    let mut headers = HeaderMap::new();
    insert_header(&mut headers, "epoch-delivery-id", &delivery.delivery_id)?;
    insert_header(
        &mut headers,
        "epoch-delivery-attempt",
        &delivery.attempt.to_string(),
    )?;
    insert_header(&mut headers, "epoch-subscription", &delivery.subscription)?;
    if let Some(traceparent) = &delivery.envelope.traceparent {
        insert_header(&mut headers, "traceparent", traceparent)?;
    }
    match mode {
        CloudEventsMode::Binary => {
            insert_header(&mut headers, "ce-specversion", "1.0")?;
            insert_header(&mut headers, "ce-id", &delivery.envelope.id)?;
            insert_header(&mut headers, "ce-source", &delivery.envelope.source)?;
            insert_header(&mut headers, "ce-type", &delivery.envelope.event_type)?;
            if let Some(subject) = &delivery.envelope.subject {
                insert_header(&mut headers, "ce-subject", subject)?;
            }
            if let Some(schema_ref) = &delivery.envelope.schema_ref {
                insert_header(&mut headers, "ce-dataschema", schema_ref)?;
            }
            insert_header(
                &mut headers,
                CONTENT_TYPE.as_str(),
                &delivery.envelope.content_type,
            )?;
            serde_json::to_vec(&delivery.envelope.payload)
                .map(|body| (body, headers))
                .map_err(|_| "event_payload_invalid".into())
        }
        CloudEventsMode::Structured => {
            let mut event = serde_json::Map::from_iter([
                ("specversion".into(), Value::String("1.0".into())),
                ("id".into(), Value::String(delivery.envelope.id.clone())),
                (
                    "source".into(),
                    Value::String(delivery.envelope.source.clone()),
                ),
                (
                    "type".into(),
                    Value::String(delivery.envelope.event_type.clone()),
                ),
                (
                    "datacontenttype".into(),
                    Value::String(delivery.envelope.content_type.clone()),
                ),
                ("data".into(), delivery.envelope.payload.clone()),
            ]);
            if let Some(subject) = &delivery.envelope.subject {
                event.insert("subject".into(), Value::String(subject.clone()));
            }
            if let Some(schema_ref) = &delivery.envelope.schema_ref {
                event.insert("dataschema".into(), Value::String(schema_ref.clone()));
            }
            insert_header(
                &mut headers,
                CONTENT_TYPE.as_str(),
                "application/cloudevents+json",
            )?;
            serde_json::to_vec(&event)
                .map(|body| (body, headers))
                .map_err(|_| "event_payload_invalid".into())
        }
    }
}

fn insert_header(headers: &mut HeaderMap, name: &str, value: &str) -> Result<(), String> {
    let name = HeaderName::from_bytes(name.as_bytes())
        .map_err(|_| "invalid_target_header_name".to_owned())?;
    let value =
        HeaderValue::from_str(value).map_err(|_| "invalid_target_header_value".to_owned())?;
    headers.insert(name, value);
    Ok(())
}

fn insert_bearer(headers: &mut HeaderMap, token: &str) -> Result<(), String> {
    insert_header(headers, AUTHORIZATION.as_str(), &format!("Bearer {token}"))
}

async fn read_bounded_response(
    response: reqwest::Response,
    maximum: usize,
) -> Result<Vec<u8>, AttemptDisposition> {
    read_bounded_response_as(response, maximum, "oauth_response").await
}

async fn read_bounded_response_as(
    mut response: reqwest::Response,
    maximum: usize,
    error_prefix: &str,
) -> Result<Vec<u8>, AttemptDisposition> {
    if response
        .content_length()
        .is_some_and(|length| length > u64::try_from(maximum).unwrap_or(u64::MAX))
    {
        return Err(AttemptDisposition::Retry(format!(
            "{error_prefix}_too_large"
        )));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| AttemptDisposition::Retry(format!("{error_prefix}_invalid")))?
    {
        if bytes.len().saturating_add(chunk.len()) > maximum {
            return Err(AttemptDisposition::Retry(format!(
                "{error_prefix}_too_large"
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn classify_status(status: StatusCode) -> AttemptDisposition {
    if status.is_success() {
        AttemptDisposition::Acknowledge
    } else if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        AttemptDisposition::Retry(format!("http_status_{}", status.as_u16()))
    } else {
        AttemptDisposition::Reject(format!("http_status_{}", status.as_u16()))
    }
}

fn request_reason(error: &reqwest::Error) -> String {
    if error.is_timeout() {
        "request_timeout".into()
    } else if error.is_connect() {
        "connection_failed".into()
    } else {
        "request_failed".into()
    }
}

fn should_failover(disposition: &AttemptDisposition) -> bool {
    matches!(
        disposition,
        AttemptDisposition::Retry(reason)
            if reason == "connection_failed"
                || reason == "request_failed"
                || reason == "request_timeout"
                || reason == "dns_resolution_failed"
                || reason == "request_client_failed"
                || reason == "http_status_429"
                || reason.starts_with("http_status_5")
    )
}

fn sanitize_disposition(disposition: AttemptDisposition) -> AttemptDisposition {
    match disposition {
        AttemptDisposition::Acknowledge => AttemptDisposition::Acknowledge,
        AttemptDisposition::Retry(reason) => {
            AttemptDisposition::Retry(sanitize_reason(&reason, "target_retry"))
        }
        AttemptDisposition::Reject(reason) => {
            AttemptDisposition::Reject(sanitize_reason(&reason, "target_rejected"))
        }
    }
}

fn sanitize_reason(reason: &str, fallback: &str) -> String {
    if !reason.is_empty()
        && reason.len() <= 128
        && reason
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        reason.to_owned()
    } else {
        fallback.to_owned()
    }
}

fn attempt_key(prefix: &str, identity: &str, attempt: u32) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/managed-target/attempt/v1\0");
    hasher.update(identity.as_bytes());
    hasher.update(attempt.to_be_bytes());
    format!("{prefix}-{}", lower_hex(&hasher.finalize()))
}

fn target_idempotency_key(identity: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/managed-target/side-effect/v1\0");
    hasher.update(identity.as_bytes());
    format!("epoch-{}", lower_hex(&hasher.finalize()))
}

fn oauth_cache_key(reference: &str, token_url: &str, scopes: &[String]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/managed-target/oauth-cache/v1\0");
    hasher.update(reference.as_bytes());
    hasher.update([0]);
    hasher.update(token_url.as_bytes());
    for scope in scopes {
        hasher.update([0]);
        hasher.update(scope.as_bytes());
    }
    lower_hex(&hasher.finalize())
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn validate_identifier(field: &str, value: &str) -> Result<(), ManagedTargetDeliveryError> {
    if value.is_empty()
        || value.len() > 128
        || !value.as_bytes()[0].is_ascii_alphanumeric()
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(ManagedTargetDeliveryError::Configuration(format!(
            "{field} must be a 1-128 byte identifier"
        )));
    }
    Ok(())
}

fn validate_secret(field: &str, value: &str) -> Result<(), ManagedTargetDeliveryError> {
    if value.is_empty() || value.len() > MAX_SECRET_BYTES || value.chars().any(char::is_control) {
        return Err(ManagedTargetDeliveryError::Configuration(format!(
            "{field} must contain 1-{MAX_SECRET_BYTES} printable bytes"
        )));
    }
    Ok(())
}

fn validate_metadata(field: &str, value: &str) -> Result<(), ManagedTargetDeliveryError> {
    if value.is_empty() || value.len() > MAX_METADATA_BYTES || value.chars().any(char::is_control) {
        return Err(ManagedTargetDeliveryError::Configuration(format!(
            "{field} must contain 1-{MAX_METADATA_BYTES} printable bytes"
        )));
    }
    Ok(())
}

fn validate_header_name(value: &str) -> Result<(), ManagedTargetDeliveryError> {
    HeaderName::from_bytes(value.as_bytes())
        .map(|_| ())
        .map_err(|_| ManagedTargetDeliveryError::Configuration("API key header is invalid".into()))
}

fn validate_external_url(field: &str, value: &str) -> Result<(), ManagedTargetDeliveryError> {
    let url = Url::parse(value).map_err(|error| {
        ManagedTargetDeliveryError::Configuration(format!("{field} is invalid: {error}"))
    })?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(ManagedTargetDeliveryError::Configuration(format!(
            "{field} must be an absolute credential-free HTTP(S) URL without a fragment"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write as _,
        sync::{
            Mutex,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
    };

    use axum::{Json, Router, extract::State, routing::post};
    use epoch_bus::{
        ConnectorKind, ConnectorSpec, EndpointObservation, FunctionDefinition, IntegrationOperation,
    };
    use serde_json::json;
    use tempfile::NamedTempFile;

    use super::*;

    fn delivery(target: SubscriptionTarget) -> BusTabletDelivery {
        BusTabletDelivery {
            delivery_id: "delivery-1".into(),
            publish_position: 7,
            subscription: "orders".into(),
            target,
            envelope: epoch_tablet::BusTabletEnvelope {
                id: "event-1".into(),
                source: "urn:test".into(),
                event_type: "order.created".into(),
                subject: Some("order-7".into()),
                time_ms: 100,
                key: None,
                headers: BTreeMap::new(),
                content_type: "application/json".into(),
                schema_ref: None,
                traceparent: Some("00-00000000000000000000000000000001-0000000000000001-01".into()),
                payload: json!({"order_id": 7}),
                deliver_at_ms: None,
                ttl_ms: None,
                priority: 0,
                dedupe_id: None,
                transaction_id: None,
                extensions: BTreeMap::new(),
            },
            route_plan_version: 1,
            destination: None,
            attempt: 1,
            lease_token: "lease".into(),
            lease_deadline_ms: 10_000,
        }
    }

    #[test]
    fn secret_store_is_strict_and_debug_output_is_redacted() {
        let mut file = NamedTempFile::new().unwrap();
        write!(
            file,
            "{}",
            json!({
                "format_version": 1,
                "secrets": [{
                    "kind": "api_key",
                    "reference": "payments",
                    "value": "super-secret-value",
                    "header": "x-api-key"
                }]
            })
        )
        .unwrap();
        let store = ManagedSecretStore::load(file.path()).unwrap();
        let debug = format!("{store:?}");
        assert!(debug.contains("payments"));
        assert!(!debug.contains("super-secret-value"));
    }

    #[test]
    fn connector_credentials_are_typed_accessible_and_redacted() {
        let mut file = NamedTempFile::new().unwrap();
        write!(
            file,
            "{}",
            json!({
                "format_version": 1,
                "secrets": [{
                    "kind": "connector_credentials",
                    "reference": "orders-database",
                    "values": {
                        "username": "epoch-reader",
                        "password": "database-super-secret"
                    }
                }]
            })
        )
        .unwrap();
        let store = ManagedSecretStore::load(file.path()).unwrap();

        assert_eq!(
            store.connector_credentials("orders-database").unwrap()["username"],
            "epoch-reader"
        );
        let debug = format!("{store:?}");
        assert!(debug.contains("orders-database"));
        assert!(!debug.contains("epoch-reader"));
        assert!(!debug.contains("database-super-secret"));
        assert!(store.connector_credentials("missing").is_err());
    }

    #[test]
    fn cloud_events_modes_preserve_context_and_idempotency() {
        let delivery = delivery(SubscriptionTarget::ApiDestination {
            url: "https://events.example.com".into(),
            auth: DestinationAuth::None,
            cloud_events_mode: CloudEventsMode::Structured,
        });
        let (structured, headers) =
            prepare_cloud_event(&delivery, CloudEventsMode::Structured).unwrap();
        let body: Value = serde_json::from_slice(&structured).unwrap();
        assert_eq!(body["specversion"], "1.0");
        assert_eq!(body["data"]["order_id"], 7);
        assert_eq!(headers[CONTENT_TYPE], "application/cloudevents+json");

        let (binary, headers) = prepare_cloud_event(&delivery, CloudEventsMode::Binary).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&binary).unwrap()["order_id"],
            7
        );
        assert_eq!(headers["ce-id"], "event-1");
        assert_eq!(
            target_idempotency_key("delivery-1"),
            target_idempotency_key("delivery-1")
        );
        assert_ne!(
            target_idempotency_key("delivery-1"),
            target_idempotency_key("delivery-2")
        );
    }

    #[test]
    fn functions_resolve_only_when_active_and_allowlisted() {
        let mut integration = EventIntegrationState::default();
        integration
            .apply(
                IntegrationOperation::UpsertFunction {
                    definition: FunctionDefinition {
                        name: "invoice".into(),
                        endpoint: "https://functions.example.com/invoice".into(),
                        identity: "invoice-runtime".into(),
                        secret_ref: None,
                        timeout_ms: 500,
                        max_input_bytes: 1024,
                        outbound_allowlist: BTreeSet::from(["functions.example.com".into()]),
                    },
                },
                1,
            )
            .unwrap();
        let resolved = resolve_target(
            &delivery(SubscriptionTarget::Function {
                resource: "invoice".into(),
            }),
            &integration,
            false,
            1,
        )
        .unwrap();
        assert_eq!(
            resolved.url.as_str(),
            "https://functions.example.com/invoice"
        );
        assert_eq!(resolved.timeout_ms, 500);
    }

    #[test]
    fn endpoint_pools_fail_over_deterministically() {
        let mut integration = EventIntegrationState::default();
        for observation in [
            EndpointObservation {
                pool: "payments".into(),
                endpoint: "https://primary.example.com/events".into(),
                region: "us-east-1".into(),
                priority: 1,
                healthy: false,
                observed_at_ms: 1,
            },
            EndpointObservation {
                pool: "payments".into(),
                endpoint: "https://secondary.example.com/events".into(),
                region: "us-west-2".into(),
                priority: 2,
                healthy: true,
                observed_at_ms: 1,
            },
        ] {
            integration
                .apply(IntegrationOperation::ObserveEndpoint { observation }, 1)
                .unwrap();
        }
        let resolved = resolve_target(
            &delivery(SubscriptionTarget::EndpointPool {
                pool: "payments".into(),
                auth: DestinationAuth::None,
                cloud_events_mode: CloudEventsMode::Binary,
            }),
            &integration,
            false,
            1,
        )
        .unwrap();
        assert_eq!(resolved.url.host_str(), Some("secondary.example.com"));
    }

    #[test]
    fn connector_resolution_requires_endpoint_direction_and_allowlist() {
        let mut integration = EventIntegrationState::default();
        integration
            .apply(
                IntegrationOperation::UpsertConnector {
                    spec: ConnectorSpec {
                        name: "warehouse".into(),
                        kind: ConnectorKind::CustomManaged,
                        direction: ConnectorDirection::Target,
                        secret_refs: BTreeSet::new(),
                        outbound_allowlist: BTreeSet::from(["warehouse.example.com".into()]),
                        identity: "warehouse-writer".into(),
                        config: BTreeMap::from([
                            (
                                "endpoint".into(),
                                "https://warehouse.example.com/events".into(),
                            ),
                            ("cloud_events_mode".into(), "structured".into()),
                            ("timeout_ms".into(), "2500".into()),
                        ]),
                    },
                },
                1,
            )
            .unwrap();
        let resolved = resolve_target(
            &delivery(SubscriptionTarget::Connector {
                resource: "warehouse".into(),
            }),
            &integration,
            false,
            1,
        )
        .unwrap();
        assert_eq!(resolved.connector.as_deref(), Some("warehouse"));
        assert_eq!(resolved.timeout_ms, 2500);
    }

    #[test]
    fn http_statuses_have_bounded_retry_semantics() {
        assert_eq!(
            classify_status(StatusCode::OK),
            AttemptDisposition::Acknowledge
        );
        assert_eq!(
            classify_status(StatusCode::TOO_MANY_REQUESTS),
            AttemptDisposition::Retry("http_status_429".into())
        );
        assert_eq!(
            classify_status(StatusCode::BAD_REQUEST),
            AttemptDisposition::Reject("http_status_400".into())
        );
    }

    #[derive(Default)]
    struct HttpTestState {
        token_calls: AtomicU64,
        token_authorized: AtomicBool,
        requests: Mutex<Vec<(HeaderMap, Value)>>,
    }

    async fn issue_token(
        State(state): State<Arc<HttpTestState>>,
        headers: HeaderMap,
    ) -> Json<Value> {
        state.token_calls.fetch_add(1, Ordering::Relaxed);
        state.token_authorized.store(
            headers
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with("Basic ")),
            Ordering::Relaxed,
        );
        Json(json!({
            "access_token": "short-lived-access-token",
            "token_type": "Bearer",
            "expires_in": 300
        }))
    }

    async fn receive_target(
        State(state): State<Arc<HttpTestState>>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> StatusCode {
        state.requests.lock().unwrap().push((headers, body));
        StatusCode::ACCEPTED
    }

    #[tokio::test]
    async fn oauth_is_dns_safe_cached_and_applied_to_the_target_request() {
        let state = Arc::new(HttpTestState::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_state = Arc::clone(&state);
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/token", post(issue_token))
                    .route("/target", post(receive_target))
                    .with_state(server_state),
            )
            .await
        });
        let reference = "payments-oauth".to_owned();
        let secrets = ManagedSecretStore {
            entries: BTreeMap::from([(
                reference.clone(),
                SecretEntry::Oauth2Client {
                    reference: reference.clone(),
                    client_id: "client".into(),
                    client_secret: "client-secret".into(),
                    token_url: None,
                    scopes: Vec::new(),
                },
            )]),
        };
        let worker = ManagedTargetDeliveryWorker::new(
            ManagedTargetDeliveryConfig {
                interval: Duration::from_millis(100),
                allow_http_loopback: true,
                secrets: Arc::new(secrets),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        let target = ResolvedTarget {
            url: Url::parse(&format!("http://{address}/target")).unwrap(),
            authentication: AuthenticationPlan::Destination(DestinationAuth::OAuth2 {
                secret_ref: reference,
                token_url: format!("http://{address}/token"),
                scopes: vec!["events.write".into()],
            }),
            cloud_events_mode: CloudEventsMode::Structured,
            timeout_ms: 1_000,
            identity_header: None,
            endpoint_route: None,
            connector: None,
        };
        let mut delivery = delivery(SubscriptionTarget::ApiDestination {
            url: target.url.to_string(),
            auth: DestinationAuth::None,
            cloud_events_mode: CloudEventsMode::Structured,
        });
        delivery.lease_deadline_ms = 5_000;

        assert_eq!(
            worker.execute(&delivery, &target, 1).await,
            AttemptDisposition::Acknowledge
        );
        assert_eq!(
            worker.execute(&delivery, &target, 2).await,
            AttemptDisposition::Acknowledge
        );
        assert_eq!(state.token_calls.load(Ordering::Relaxed), 1);
        assert!(state.token_authorized.load(Ordering::Relaxed));
        let requests = state.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].0[AUTHORIZATION],
            "Bearer short-lived-access-token"
        );
        assert_eq!(requests[0].1["data"]["order_id"], 7);
        assert!(requests[0].0.contains_key("idempotency-key"));
        drop(requests);
        server.abort();
    }
}

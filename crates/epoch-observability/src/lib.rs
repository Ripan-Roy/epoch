//! Bounded observability contracts shared by Epoch runtimes.
//!
//! Customer-controlled resource names never become metric labels. Tenant
//! scopes are represented by a deterministic fingerprint and admitted through
//! a fixed-size registry; excess tenants share the `overflow` series. This
//! keeps the exported Prometheus surface useful without allowing cardinality
//! to grow with requests.

#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, VecDeque},
    fmt::{self, Write as _},
    sync::{Arc, Mutex},
    time::Duration,
};

use serde::Serialize;
use thiserror::Error;

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
const UNSCOPED_TENANT: &str = "none";
const OVERFLOW_TENANT: &str = "overflow";
const MAX_STAGE_SAMPLES: usize = 1_024;

/// Fixed request-duration buckets in milliseconds.
pub const HISTOGRAM_BOUNDS_MS: [u64; 12] =
    [1, 2, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000];

/// Invalid or unsafe observability configuration.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ObservabilityError {
    /// A label would violate the bounded public contract.
    #[error("invalid observability label: {0}")]
    InvalidLabel(&'static str),
    /// No tenant series could be admitted.
    #[error("maximum admitted tenants must be between 1 and 4096")]
    InvalidTenantLimit,
    /// A trace context was not canonical W3C Trace Context version 00.
    #[error("invalid W3C traceparent")]
    InvalidTraceParent,
    /// OTLP/HTTP needs one unambiguous base URL without embedded credentials.
    #[error(
        "OTLP endpoint must be an http(s) base URL without credentials, path, query, or fragment"
    )]
    InvalidOtlpEndpoint,
}

/// Validates and canonicalizes the process-wide OTLP/HTTP base URL.
pub fn validate_otlp_http_base(value: &str) -> Result<String, ObservabilityError> {
    let endpoint = url::Url::parse(value).map_err(|_| ObservabilityError::InvalidOtlpEndpoint)?;
    if !matches!(endpoint.scheme(), "http" | "https")
        || endpoint.host().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.path() != "/"
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(ObservabilityError::InvalidOtlpEndpoint);
    }
    Ok(endpoint.to_string().trim_end_matches('/').to_owned())
}

/// Resolves the OTLP/HTTP traces path from a validated base URL.
#[must_use]
pub fn otlp_traces_endpoint(base: &str) -> String {
    format!("{base}/v1/traces")
}

/// Canonical W3C Trace Context `traceparent` version 00.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceParent {
    trace_id: [u8; 16],
    parent_id: [u8; 8],
    flags: u8,
}

impl TraceParent {
    /// Parses a strict lowercase version-00 trace context.
    pub fn parse(value: &str) -> Result<Self, ObservabilityError> {
        epoch_core::validate_traceparent(value)
            .map_err(|_| ObservabilityError::InvalidTraceParent)?;
        let bytes = value.as_bytes();
        let trace_id = decode_hex::<16>(&bytes[3..35])?;
        let parent_id = decode_hex::<8>(&bytes[36..52])?;
        let [flags] = decode_hex::<1>(&bytes[53..55])?;
        Ok(Self {
            trace_id,
            parent_id,
            flags,
        })
    }

    /// Trace Context version supported by this parser.
    pub const fn version(self) -> u8 {
        0
    }

    /// Returns the canonical lowercase trace identifier.
    pub fn trace_id(self) -> String {
        encode_hex(&self.trace_id)
    }

    /// Returns the canonical lowercase parent identifier.
    pub fn parent_id(self) -> String {
        encode_hex(&self.parent_id)
    }

    /// Whether the sampled flag is set.
    pub const fn sampled(self) -> bool {
        self.flags & 1 == 1
    }
}

impl fmt::Display for TraceParent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "00-{}-{}-{:02x}",
            encode_hex(&self.trace_id),
            encode_hex(&self.parent_id),
            self.flags
        )
    }
}

fn decode_hex<const N: usize>(value: &[u8]) -> Result<[u8; N], ObservabilityError> {
    if value.len() != N * 2 {
        return Err(ObservabilityError::InvalidTraceParent);
    }
    let mut output = [0; N];
    for (index, pair) in value.chunks_exact(2).enumerate() {
        output[index] = (lower_hex(pair[0])? << 4) | lower_hex(pair[1])?;
    }
    Ok(output)
}

const fn lower_hex(value: u8) -> Result<u8, ObservabilityError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(ObservabilityError::InvalidTraceParent),
    }
}

fn encode_hex(value: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

/// Validated four-part tenant scope whose debug representation is redacted.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TenantScope {
    canonical: Arc<str>,
    fingerprint: Arc<str>,
}

impl TenantScope {
    /// Constructs a bounded tenant scope.
    pub fn new(
        organization: &str,
        project: &str,
        environment: &str,
        namespace: &str,
    ) -> Result<Self, ObservabilityError> {
        for value in [organization, project, environment, namespace] {
            if !valid_scope_segment(value) {
                return Err(ObservabilityError::InvalidLabel("tenant scope"));
            }
        }
        let canonical = [organization, project, environment, namespace].join("\u{1f}");
        let fingerprint = format!("{:016x}", fnv1a64(canonical.as_bytes()));
        Ok(Self {
            canonical: Arc::from(canonical),
            fingerprint: Arc::from(fingerprint),
        })
    }

    fn unscoped() -> Self {
        Self {
            canonical: Arc::from(UNSCOPED_TENANT),
            fingerprint: Arc::from(UNSCOPED_TENANT),
        }
    }

    /// Returns the stable non-cryptographic metric label.
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    fn is_unscoped(&self) -> bool {
        self.canonical.as_ref() == UNSCOPED_TENANT
    }
}

impl fmt::Debug for TenantScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TenantScope")
            .field("fingerprint", &self.fingerprint)
            .finish_non_exhaustive()
    }
}

fn valid_scope_segment(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn fnv1a64(value: &[u8]) -> u64 {
    value.iter().fold(FNV_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

/// Low-cardinality request labels derived from a route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationDimensions {
    /// One fixed profile name.
    pub profile: &'static str,
    /// One fixed operation family.
    pub operation: &'static str,
    /// Bounded tenant scope.
    pub tenant: TenantScope,
}

impl OperationDimensions {
    /// Constructs dimensions from already normalized static labels.
    pub const fn new(profile: &'static str, operation: &'static str, tenant: TenantScope) -> Self {
        Self {
            profile,
            operation,
            tenant,
        }
    }

    /// Constructs dimensions for an endpoint without a tenant scope.
    pub fn unscoped(profile: &'static str, operation: &'static str) -> Self {
        Self::new(profile, operation, TenantScope::unscoped())
    }
}

/// Converts an HTTP path into fixed metric dimensions.
///
/// The returned value never retains resource names, shards, or arbitrary
/// operation suffixes.
pub fn classify_http_request(method: &str, path: &str) -> OperationDimensions {
    let segments: Vec<_> = path.split('/').filter(|part| !part.is_empty()).collect();
    if let Some(dimensions) = classify_regional(method, &segments) {
        return dimensions;
    }
    classify_standalone(method, &segments)
}

fn classify_regional(method: &str, segments: &[&str]) -> Option<OperationDimensions> {
    if segments.len() >= 13
        && segments[0] == "v1"
        && segments[1] == "organizations"
        && segments[3] == "projects"
        && segments[5] == "environments"
        && segments[7] == "namespaces"
        && segments[11] == "shards"
    {
        let tenant = TenantScope::new(segments[2], segments[4], segments[6], segments[8]).ok()?;
        let profile = normalize_profile(segments[9]);
        let suffix = segments.get(13).copied().unwrap_or("");
        return Some(OperationDimensions::new(
            profile,
            normalize_operation(profile, method, suffix),
            tenant,
        ));
    }
    if segments.len() >= 12
        && segments[..4] == ["experimental", "v1", "regional", "resources"]
        && segments[10] == "shards"
    {
        let tenant = TenantScope::new(segments[4], segments[5], segments[6], segments[7]).ok()?;
        let profile = normalize_profile(segments[8]);
        let suffix = if segments.get(12) == Some(&"data") {
            segments.get(13).copied().unwrap_or("")
        } else {
            "route"
        };
        return Some(OperationDimensions::new(
            profile,
            normalize_operation(profile, method, suffix),
            tenant,
        ));
    }
    None
}

fn classify_standalone(method: &str, segments: &[&str]) -> OperationDimensions {
    if segments == ["healthz"] || segments == ["readyz"] {
        return OperationDimensions::unscoped("runtime", "health");
    }
    if segments == ["metrics"] {
        return OperationDimensions::unscoped("runtime", "metrics");
    }
    if segments.first() == Some(&"v1") {
        let profile = segments
            .get(1)
            .map_or("runtime", |value| normalize_profile(value));
        let suffix = segments.get(3).copied().unwrap_or("");
        return OperationDimensions::unscoped(
            profile,
            normalize_operation(profile, method, suffix),
        );
    }
    OperationDimensions::unscoped("runtime", "other")
}

const fn normalize_profile(value: &str) -> &'static str {
    match value.as_bytes() {
        b"cache" | b"caches" => "cache",
        b"stream" | b"streams" => "stream",
        b"queue" | b"queues" => "queue",
        b"bus" | b"buses" => "bus",
        b"connector" | b"connectors" => "connector",
        b"catalog" | b"resources" => "control",
        _ => "runtime",
    }
}

fn normalize_operation(profile: &str, method: &str, suffix: &str) -> &'static str {
    let suffix = suffix
        .trim_matches('/')
        .split('/')
        .next()
        .unwrap_or_default();
    match (profile, method, suffix) {
        ("cache", "GET", "keys" | "reads" | "query") => "get",
        ("cache", "DELETE", "keys") | ("control", "DELETE", _) => "delete",
        ("cache", _, "mutations" | "transactions" | "multiplex" | "keys" | "increment") => "write",
        ("stream", "POST", "records" | "batches" | "mutations") => "produce",
        ("stream", "GET", "records" | "fetch" | "long-poll") => "fetch",
        ("stream", _, "groups" | "sessions" | "claims") => "consume",
        ("queue", "POST", "messages" | "enqueue") | ("bus", "POST", "events" | "publish") => {
            "publish"
        }
        ("queue", _, "acquire" | "receive") => "acquire",
        ("queue", _, "settle" | "ack" | "nack" | "release") => "settle",
        ("queue", "GET", "counts" | "history") => "inspect",
        ("queue", _, "dead-letters" | "redrive") => "redrive",
        (_, "GET", "") | ("bus", _, "deliveries" | "subscriptions") => "route",
        ("bus", _, "replay" | "archive") => "replay",
        ("connector", _, "poll" | "sources") => "source",
        ("connector", _, "checkpoint" | "checkpoints") => "checkpoint",
        ("control", "GET", _) => "read",
        ("control", _, _) => "apply",
        (_, _, "route") => "route",
        (_, _, "") => "manage",
        _ => "other",
    }
}

/// Coarse request result suitable for bounded labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// Successful or redirected response.
    Success,
    /// Authentication or authorization rejection.
    Auth,
    /// Capacity or rate-limit rejection.
    Quota,
    /// Other caller error.
    ClientError,
    /// Server-side failure.
    ServerError,
}

impl Outcome {
    /// Classifies one HTTP status without exposing the exact code as a label.
    pub const fn from_status(status: u16) -> Self {
        match status {
            200..=399 => Self::Success,
            401 | 403 => Self::Auth,
            429 => Self::Quota,
            400..=499 => Self::ClientError,
            _ => Self::ServerError,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Auth => "auth",
            Self::Quota => "quota",
            Self::ClientError => "client_error",
            Self::ServerError => "server_error",
        }
    }
}

/// One completed public request.
#[derive(Debug, Clone)]
pub struct RequestObservation {
    /// Normalized fixed dimensions.
    pub dimensions: OperationDimensions,
    /// Coarse result class.
    pub outcome: Outcome,
    /// End-to-end server time.
    pub elapsed: Duration,
}

/// Compatibility wire protocol with a fixed metric label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    /// Redis serialization protocol.
    Redis,
    /// Kafka request protocol.
    Kafka,
    /// AMQP 0-9-1 as used by `RabbitMQ` clients.
    Amqp091,
}

impl Protocol {
    /// Stable low-cardinality representation for metrics and traces.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Redis => "redis",
            Self::Kafka => "kafka",
            Self::Amqp091 => "amqp091",
        }
    }
}

/// Closed compatibility operation families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolOperation {
    /// Connection/session establishment.
    Connect,
    /// Cache read family.
    CacheRead,
    /// Cache mutation family.
    CacheWrite,
    /// Stream production.
    Produce,
    /// Stream fetch.
    Fetch,
    /// Consumer group coordination.
    Group,
    /// Queue or exchange declaration.
    Declare,
    /// Queue/event publication.
    Publish,
    /// Queue consumption.
    Consume,
    /// Acknowledgement or rejection.
    Settle,
    /// Supported operation outside a more specific family.
    Other,
}

impl ProtocolOperation {
    /// Stable low-cardinality representation for metrics and traces.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::CacheRead => "cache_read",
            Self::CacheWrite => "cache_write",
            Self::Produce => "produce",
            Self::Fetch => "fetch",
            Self::Group => "group",
            Self::Declare => "declare",
            Self::Publish => "publish",
            Self::Consume => "consume",
            Self::Settle => "settle",
            Self::Other => "other",
        }
    }
}

/// One completed compatibility protocol operation.
#[derive(Debug, Clone)]
pub struct ProtocolObservation {
    /// Closed wire protocol.
    pub protocol: Protocol,
    /// Closed operation family.
    pub operation: ProtocolOperation,
    /// Coarse result class.
    pub outcome: Outcome,
    /// End-to-end gateway time.
    pub elapsed: Duration,
}

/// Latency stage used by the operational diagnostic view.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    /// Admission, rate, or capacity policy.
    Quota,
    /// Partition selection or concentrated key traffic.
    HotPartition,
    /// Majority acknowledgement or replica catch-up.
    Replication,
    /// WAL, snapshot, object, or cold-value I/O.
    Storage,
    /// Discovery, proxying, or leader redirects.
    Routing,
    /// Webhook, connector, or managed target execution.
    Target,
    /// Time outside the measured server stages.
    Client,
}

impl Stage {
    const fn label(self) -> &'static str {
        match self {
            Self::Quota => "quota",
            Self::HotPartition => "hot_partition",
            Self::Replication => "replication",
            Self::Storage => "storage",
            Self::Routing => "routing",
            Self::Target => "target",
            Self::Client => "client",
        }
    }
}

/// User-facing attribution for tail latency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LatencyCause {
    /// Quota or admission is limiting the request.
    Quota,
    /// One partition is materially hotter than peers.
    HotPartition,
    /// Replication acknowledgement dominates.
    Replication,
    /// Storage I/O dominates.
    Storage,
    /// Routing or leader discovery dominates.
    Routing,
    /// An external target dominates.
    Target,
    /// Server stages do not explain observed latency.
    Client,
}

impl From<Stage> for LatencyCause {
    fn from(stage: Stage) -> Self {
        match stage {
            Stage::Quota => Self::Quota,
            Stage::HotPartition => Self::HotPartition,
            Stage::Replication => Self::Replication,
            Stage::Storage => Self::Storage,
            Stage::Routing => Self::Routing,
            Stage::Target => Self::Target,
            Stage::Client => Self::Client,
        }
    }
}

/// Deterministic latency diagnosis returned to operational surfaces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LatencyDiagnosis {
    /// Dominant cause.
    pub cause: LatencyCause,
    /// Dominant measured stage.
    pub stage: Stage,
    /// Exact p99 from the bounded recent sample window.
    pub observed_p99_ms: u64,
    /// Number of samples supporting the result.
    pub samples: usize,
    /// Bounded operator action.
    pub recommendation: &'static str,
}

/// Bounded registry health exposed alongside metrics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MetricsSnapshot {
    /// Number of exact tenant fingerprints admitted.
    pub tenant_count: usize,
    /// Number of observations assigned to the overflow tenant.
    pub overflowed_tenants: u64,
    /// Number of request series currently allocated.
    pub request_series: usize,
    /// Number of stage series currently allocated.
    pub stage_series: usize,
    /// Number of compatibility protocol series currently allocated.
    pub protocol_series: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RequestKey {
    tenant: String,
    profile: &'static str,
    operation: &'static str,
    outcome: Outcome,
}

#[derive(Debug, Default)]
struct RequestSeries {
    count: u64,
    sum_nanos: u128,
    buckets: [u64; HISTOGRAM_BOUNDS_MS.len()],
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StageKey {
    tenant: String,
    profile: &'static str,
    stage: Stage,
    outcome: Outcome,
}

#[derive(Debug, Default)]
struct StageSeries {
    samples_ms: VecDeque<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ProtocolKey {
    protocol: Protocol,
    operation: ProtocolOperation,
    outcome: Outcome,
}

#[derive(Debug, Default)]
struct RegistryState {
    tenants: BTreeMap<String, String>,
    overflowed_tenants: u64,
    requests: BTreeMap<RequestKey, RequestSeries>,
    stages: BTreeMap<StageKey, StageSeries>,
    protocols: BTreeMap<ProtocolKey, RequestSeries>,
}

/// Thread-safe bounded metrics and diagnostic registry.
#[derive(Debug, Clone)]
pub struct MetricsRegistry {
    service: Arc<str>,
    max_tenants: usize,
    state: Arc<Mutex<RegistryState>>,
}

impl MetricsRegistry {
    /// Creates a registry with a fixed exact-tenant admission limit.
    pub fn new(service: &str, max_tenants: usize) -> Result<Self, ObservabilityError> {
        if !valid_metric_label(service) {
            return Err(ObservabilityError::InvalidLabel("service"));
        }
        if !(1..=4_096).contains(&max_tenants) {
            return Err(ObservabilityError::InvalidTenantLimit);
        }
        Ok(Self {
            service: Arc::from(service),
            max_tenants,
            state: Arc::new(Mutex::new(RegistryState::default())),
        })
    }

    /// Records a completed request.
    pub fn record_request(&self, observation: &RequestObservation) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let tenant = admit_tenant(&mut state, self.max_tenants, &observation.dimensions.tenant);
        let series = state
            .requests
            .entry(RequestKey {
                tenant,
                profile: observation.dimensions.profile,
                operation: observation.dimensions.operation,
                outcome: observation.outcome,
            })
            .or_default();
        observe_histogram(series, observation.elapsed);
    }

    /// Records a completed compatibility operation with closed labels.
    pub fn record_protocol(&self, observation: &ProtocolObservation) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let series = state
            .protocols
            .entry(ProtocolKey {
                protocol: observation.protocol,
                operation: observation.operation,
                outcome: observation.outcome,
            })
            .or_default();
        observe_histogram(series, observation.elapsed);
    }

    /// Records one named latency stage in a bounded recent-sample window.
    pub fn record_stage(
        &self,
        tenant: &TenantScope,
        profile: &'static str,
        stage: Stage,
        elapsed: Duration,
        outcome: Outcome,
    ) {
        let mut registry = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let tenant = admit_tenant(&mut registry, self.max_tenants, tenant);
        let series = registry
            .stages
            .entry(StageKey {
                tenant,
                profile,
                stage,
                outcome,
            })
            .or_default();
        if series.samples_ms.len() == MAX_STAGE_SAMPLES {
            series.samples_ms.pop_front();
        }
        let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        series.samples_ms.push_back(elapsed_ms);
    }

    /// Attributes p99 latency to the slowest measured stage for a tenant/profile.
    pub fn diagnose(&self, tenant: &TenantScope, profile: &str) -> Option<LatencyDiagnosis> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let tenant = if tenant.is_unscoped() {
            UNSCOPED_TENANT
        } else {
            state.tenants.get(tenant.canonical.as_ref())?.as_str()
        };
        state
            .stages
            .iter()
            .filter(|(key, _)| {
                key.tenant == tenant && key.profile == profile && key.outcome == Outcome::Success
            })
            .filter_map(|(key, series)| {
                percentile(&series.samples_ms, 99)
                    .map(|p99| (key.stage, p99, series.samples_ms.len()))
            })
            .max_by_key(|(stage, p99, _)| (*p99, std::cmp::Reverse(*stage)))
            .map(|(stage, observed_p99_ms, samples)| LatencyDiagnosis {
                cause: stage.into(),
                stage,
                observed_p99_ms,
                samples,
                recommendation: recommendation(stage),
            })
    }

    /// Returns bounded registry health without exposing tenant values.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        MetricsSnapshot {
            tenant_count: state.tenants.len(),
            overflowed_tenants: state.overflowed_tenants,
            request_series: state.requests.len(),
            stage_series: state.stages.len(),
            protocol_series: state.protocols.len(),
        }
    }

    /// Renders deterministic Prometheus text exposition.
    pub fn render_prometheus(&self) -> String {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut output = String::new();
        output.push_str("# HELP epoch_http_requests_total Completed Epoch HTTP requests.\n");
        output.push_str("# TYPE epoch_http_requests_total counter\n");
        for (key, series) in &state.requests {
            let labels = request_labels(&self.service, key);
            let _ = writeln!(
                output,
                "epoch_http_requests_total{{{labels}}} {}",
                series.count
            );
        }
        output.push_str(
            "# HELP epoch_http_request_duration_seconds End-to-end Epoch HTTP request duration.\n",
        );
        output.push_str("# TYPE epoch_http_request_duration_seconds histogram\n");
        for (key, series) in &state.requests {
            let labels = request_labels(&self.service, key);
            for (bound, count) in HISTOGRAM_BOUNDS_MS.iter().zip(series.buckets) {
                let _ = writeln!(
                    output,
                    "epoch_http_request_duration_seconds_bucket{{{labels},le=\"{}\"}} {count}",
                    format_seconds(*bound)
                );
            }
            let _ = writeln!(
                output,
                "epoch_http_request_duration_seconds_bucket{{{labels},le=\"+Inf\"}} {}",
                series.count
            );
            let _ = writeln!(
                output,
                "epoch_http_request_duration_seconds_sum{{{labels}}} {}",
                format_nanos_seconds(series.sum_nanos)
            );
            let _ = writeln!(
                output,
                "epoch_http_request_duration_seconds_count{{{labels}}} {}",
                series.count
            );
        }
        output.push_str(
            "# HELP epoch_observability_tenants Number of exact tenant fingerprints admitted.\n",
        );
        output.push_str("# TYPE epoch_observability_tenants gauge\n");
        let _ = writeln!(
            output,
            "epoch_observability_tenants{{service=\"{}\"}} {}",
            self.service,
            state.tenants.len()
        );
        output.push_str("# HELP epoch_observability_overflow_total Tenant observations assigned to the bounded overflow series.\n");
        output.push_str("# TYPE epoch_observability_overflow_total counter\n");
        let _ = writeln!(
            output,
            "epoch_observability_overflow_total{{service=\"{}\"}} {}",
            self.service, state.overflowed_tenants
        );
        output.push_str("# HELP epoch_operation_stage_duration_p99_seconds Recent operation stage p99 duration.\n");
        output.push_str("# TYPE epoch_operation_stage_duration_p99_seconds gauge\n");
        for (key, series) in &state.stages {
            if let Some(p99_ms) = percentile(&series.samples_ms, 99) {
                let _ = writeln!(
                    output,
                    "epoch_operation_stage_duration_p99_seconds{{service=\"{}\",tenant=\"{}\",profile=\"{}\",stage=\"{}\",outcome=\"{}\"}} {}",
                    self.service,
                    key.tenant,
                    key.profile,
                    key.stage.label(),
                    key.outcome.label(),
                    format_seconds(p99_ms)
                );
            }
        }
        render_protocol_metrics(&mut output, &self.service, &state.protocols);
        output
    }
}

fn valid_metric_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn admit_tenant(state: &mut RegistryState, max_tenants: usize, tenant: &TenantScope) -> String {
    if tenant.is_unscoped() {
        return UNSCOPED_TENANT.to_owned();
    }
    if let Some(label) = state.tenants.get(tenant.canonical.as_ref()) {
        return label.clone();
    }
    if state.tenants.len() < max_tenants {
        let label = tenant.fingerprint().to_owned();
        state
            .tenants
            .insert(tenant.canonical.to_string(), label.clone());
        return label;
    }
    state.overflowed_tenants = state.overflowed_tenants.saturating_add(1);
    OVERFLOW_TENANT.to_owned()
}

fn percentile(samples: &VecDeque<u64>, percentile: usize) -> Option<u64> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted: Vec<_> = samples.iter().copied().collect();
    sorted.sort_unstable();
    let rank = (percentile * sorted.len()).div_ceil(100).saturating_sub(1);
    sorted.get(rank).copied()
}

const fn recommendation(stage: Stage) -> &'static str {
    match stage {
        Stage::Quota => "Inspect the limiting quota and capacity headroom before raising limits.",
        Stage::HotPartition => {
            "Inspect partition heat and split or redistribute the hot key range."
        }
        Stage::Replication => {
            "Inspect replica lag, quorum health, and cross-zone acknowledgement time."
        }
        Stage::Storage => "Inspect WAL, snapshot, object-tier, and cold-storage latency.",
        Stage::Routing => "Inspect leader churn, route refreshes, and gateway-to-tablet latency.",
        Stage::Target => "Inspect downstream target latency, throttling, retries, and backlog.",
        Stage::Client => "Inspect client queueing, connection reuse, and network latency.",
    }
}

fn request_labels(service: &str, key: &RequestKey) -> String {
    format!(
        "service=\"{service}\",tenant=\"{}\",profile=\"{}\",operation=\"{}\",outcome=\"{}\"",
        key.tenant,
        key.profile,
        key.operation,
        key.outcome.label()
    )
}

fn protocol_labels(service: &str, key: &ProtocolKey) -> String {
    format!(
        "service=\"{service}\",protocol=\"{}\",operation=\"{}\",outcome=\"{}\"",
        key.protocol.as_str(),
        key.operation.as_str(),
        key.outcome.label()
    )
}

fn render_protocol_metrics(
    output: &mut String,
    service: &str,
    protocols: &BTreeMap<ProtocolKey, RequestSeries>,
) {
    output.push_str(
        "# HELP epoch_compat_requests_total Completed compatibility protocol operations.\n",
    );
    output.push_str("# TYPE epoch_compat_requests_total counter\n");
    for (key, series) in protocols {
        let labels = protocol_labels(service, key);
        let _ = writeln!(
            output,
            "epoch_compat_requests_total{{{labels}}} {}",
            series.count
        );
    }
    output.push_str(
        "# HELP epoch_compat_request_duration_seconds Compatibility protocol operation duration.\n",
    );
    output.push_str("# TYPE epoch_compat_request_duration_seconds histogram\n");
    for (key, series) in protocols {
        let labels = protocol_labels(service, key);
        for (bound, count) in HISTOGRAM_BOUNDS_MS.iter().zip(series.buckets) {
            let _ = writeln!(
                output,
                "epoch_compat_request_duration_seconds_bucket{{{labels},le=\"{}\"}} {count}",
                format_seconds(*bound)
            );
        }
        let _ = writeln!(
            output,
            "epoch_compat_request_duration_seconds_bucket{{{labels},le=\"+Inf\"}} {}",
            series.count
        );
        let _ = writeln!(
            output,
            "epoch_compat_request_duration_seconds_sum{{{labels}}} {}",
            format_nanos_seconds(series.sum_nanos)
        );
        let _ = writeln!(
            output,
            "epoch_compat_request_duration_seconds_count{{{labels}}} {}",
            series.count
        );
    }
}

fn observe_histogram(series: &mut RequestSeries, elapsed: Duration) {
    series.count = series.count.saturating_add(1);
    series.sum_nanos = series.sum_nanos.saturating_add(elapsed.as_nanos());
    for (bucket, bound_ms) in series.buckets.iter_mut().zip(HISTOGRAM_BOUNDS_MS) {
        if elapsed <= Duration::from_millis(bound_ms) {
            *bucket = bucket.saturating_add(1);
        }
    }
}

fn format_seconds(milliseconds: u64) -> String {
    if milliseconds.is_multiple_of(1_000) {
        (milliseconds / 1_000).to_string()
    } else {
        format!("{}.{:03}", milliseconds / 1_000, milliseconds % 1_000)
            .trim_end_matches('0')
            .to_owned()
    }
}

fn format_nanos_seconds(nanoseconds: u128) -> String {
    let seconds = nanoseconds / 1_000_000_000;
    let fraction = nanoseconds % 1_000_000_000;
    if fraction == 0 {
        seconds.to_string()
    } else {
        format!("{seconds}.{fraction:09}")
            .trim_end_matches('0')
            .to_owned()
    }
}

//! HTTP metrics, correlation, and bounded operator diagnostics.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    extract::{Query, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header::CONTENT_TYPE},
    middleware,
    middleware::Next,
    response::{IntoResponse, Response},
    routing::get,
};
use epoch_observability::{
    MetricsRegistry, Outcome, RequestObservation, TenantScope, TraceParent, classify_http_request,
};
use opentelemetry::propagation::Extractor;
use serde::Deserialize;
use serde_json::json;
use tracing::{Instrument as _, info, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

const REQUEST_ID_HEADER: &str = "x-request-id";
const TRACEPARENT_HEADER: &str = "traceparent";
const MAX_REQUEST_ID_BYTES: usize = 128;
static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
struct HttpObservabilityState {
    metrics: MetricsRegistry,
}

/// Applies correlation, structured request logging, and bounded metrics.
pub fn with_http_observability(router: Router, metrics: MetricsRegistry) -> Router {
    router.layer(middleware::from_fn_with_state(
        HttpObservabilityState { metrics },
        observe_request,
    ))
}

async fn observe_request(
    State(state): State<HttpObservabilityState>,
    mut request: Request,
    next: Next,
) -> Response {
    let started = Instant::now();
    let request_id = request_id(request.headers());
    let request_id_header = HeaderValue::from_str(&request_id)
        .expect("generated or validated request ID must be a valid header");
    request
        .headers_mut()
        .insert(REQUEST_ID_HEADER, request_id_header.clone());

    let dimensions = classify_http_request(request.method().as_str(), request.uri().path());
    let trace_id = request
        .headers()
        .get(TRACEPARENT_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| TraceParent::parse(value).ok())
        .map_or_else(String::new, TraceParent::trace_id);
    let span = info_span!(
        "epoch.http.request",
        request.id = %request_id,
        trace.id = %trace_id,
        http.request.method = %request.method(),
        url.path = %request.uri().path(),
        epoch.profile = dimensions.profile,
        epoch.operation = dimensions.operation,
        epoch.tenant = dimensions.tenant.fingerprint(),
        http.response.status_code = tracing::field::Empty,
        duration_ms = tracing::field::Empty,
    );
    let parent = opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor(request.headers()))
    });
    let _ = span.set_parent(parent);

    let mut response = next.run(request).instrument(span.clone()).await;
    let elapsed = started.elapsed();
    response
        .headers_mut()
        .insert(REQUEST_ID_HEADER, request_id_header);
    state.metrics.record_request(&RequestObservation {
        dimensions,
        outcome: Outcome::from_status(response.status().as_u16()),
        elapsed,
    });
    span.record("http.response.status_code", response.status().as_u16());
    span.record(
        "duration_ms",
        u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
    );
    span.in_scope(|| {
        info!(
            event.id = %request_id,
            "Epoch HTTP request completed"
        );
    });
    response
}

/// Internal-only operational surface. Callers choose the listener boundary.
pub fn metrics_router(metrics: MetricsRegistry) -> Router {
    Router::new()
        .route("/healthz", get(observability_health))
        .route("/metrics", get(prometheus_metrics))
        .route("/v1/diagnostics/latency", get(latency_diagnostic))
        .with_state(metrics)
}

async fn observability_health(State(metrics): State<MetricsRegistry>) -> Json<serde_json::Value> {
    Json(json!({
        "status": "ok",
        "registry": metrics.snapshot(),
    }))
}

async fn prometheus_metrics(State(metrics): State<MetricsRegistry>) -> Response {
    (
        [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        metrics.render_prometheus(),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
struct LatencyDiagnosticQuery {
    organization: String,
    project: String,
    environment: String,
    namespace: String,
    profile: String,
}

async fn latency_diagnostic(
    State(metrics): State<MetricsRegistry>,
    Query(query): Query<LatencyDiagnosticQuery>,
) -> Response {
    let tenant = match TenantScope::new(
        &query.organization,
        &query.project,
        &query.environment,
        &query.namespace,
    ) {
        Ok(tenant) => tenant,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "code": "invalid_argument", "message": error.to_string() })),
            )
                .into_response();
        }
    };
    match metrics.diagnose(&tenant, &query.profile) {
        Some(diagnosis) => Json(diagnosis).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "code": "not_found",
                "message": "no recent successful latency samples match this tenant and profile"
            })),
        )
            .into_response(),
    }
}

fn request_id(headers: &HeaderMap) -> String {
    let values: Vec<_> = headers.get_all(REQUEST_ID_HEADER).iter().collect();
    if values.len() == 1
        && let Ok(candidate) = values[0].to_str()
        && valid_request_id(candidate)
    {
        return candidate.to_owned();
    }
    let unix_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let sequence = REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("request-{unix_nanos:x}-{sequence:x}")
}

fn valid_request_id(candidate: &str) -> bool {
    !candidate.is_empty()
        && candidate.len() <= MAX_REQUEST_ID_BYTES
        && candidate.bytes().all(|byte| matches!(byte, 0x21..=0x7e))
}

struct HeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(axum::http::HeaderName::as_str).collect()
    }
}

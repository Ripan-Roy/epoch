//! Authentication, scoped authorization, and audit enforcement for the
//! regional Rust HTTP boundary.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Extension, Json, Router,
    extract::{Query, Request, State},
    http::{
        HeaderMap, HeaderValue, Method, StatusCode,
        header::{AUTHORIZATION, HeaderName},
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use epoch_auth::{
    Action, AuditJournal, AuthenticationError, AuthenticationErrorKind, AuthenticationMethod,
    BootstrapPolicy, Decision, DecisionEvent, DecisionEventFields, DecisionReason, Principal,
    ResourceScope,
};
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use tracing::{error, info};

const CATALOG_ROOT: &str = "/experimental/v1/regional/catalog";
const CATALOG_RESOURCE_PREFIX: &str = "/experimental/v1/regional/catalog/resources/";
const CATALOG_TABLET_PREFIX: &str = "/experimental/v1/regional/catalog/tablets/";
const RESOURCE_PREFIX: &str = "/experimental/v1/regional/resources/";
const TOPOLOGY_PATH: &str = "/experimental/v1/regional/topology";
const BACKUP_PATH: &str = "/v1/admin/backups";
const AUDIT_PATH: &str = "/v1/admin/audit/events";
const NATIVE_RESOURCE_PREFIX: &str = "/v1/organizations/";
const MAX_REQUEST_ID_BYTES: usize = 128;
static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

#[derive(Debug, Clone)]
struct RegionalAuthState {
    policy: Arc<BootstrapPolicy>,
    audit: Arc<AuditJournal>,
}

#[derive(Debug, Serialize)]
struct AuthErrorBody {
    code: &'static str,
    message: &'static str,
}

/// Applies deny-by-default authentication and scoped authorization to every
/// route already registered in a regional router.
pub fn with_regional_auth(
    router: Router,
    policy: Arc<BootstrapPolicy>,
    audit: Arc<AuditJournal>,
) -> Router {
    let state = RegionalAuthState { policy, audit };
    let router = router.route(AUDIT_PATH, get(read_audit_events));
    router.route_layer(middleware::from_fn_with_state(
        state,
        authorize_regional_request,
    ))
}

async fn authorize_regional_request(
    State(state): State<RegionalAuthState>,
    mut request: Request,
    next: Next,
) -> Response {
    let request_id = request_id(request.headers());
    let action = action_for_request(request.method(), request.uri().path());
    let authorization = authorization_header(request.headers());
    let principal = match state.policy.authenticate_bearer(authorization) {
        Ok(principal) => principal,
        Err(authentication_error) => {
            if record_decision(
                &state.audit,
                authentication_failure_fields(
                    &request_id,
                    state.policy.id(),
                    action,
                    &authentication_error,
                ),
            )
            .is_err()
            {
                return auth_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "audit_unavailable",
                    "audit journal is unavailable",
                    &request_id,
                );
            }
            return auth_error(
                StatusCode::UNAUTHORIZED,
                "unauthenticated",
                "valid bearer authentication is required",
                &request_id,
            );
        }
    };
    let Ok(scope) = scope_for_path(request.uri().path()) else {
        return auth_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "regional resource path is invalid",
            &request_id,
        );
    };
    let decision_scope = if action == Action::AuditRead {
        principal.scope().clone()
    } else {
        scope.clone()
    };
    let allowed = if action == Action::AuditRead {
        principal.has_action(action)
    } else {
        principal.allows(action, &scope)
    };
    let reason = decision_reason(&principal, action, allowed);
    if record_decision(
        &state.audit,
        authorization_decision_fields(
            &request_id,
            &principal,
            action,
            allowed,
            reason,
            decision_scope,
        ),
    )
    .is_err()
    {
        return auth_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "audit_unavailable",
            "audit journal is unavailable",
            &request_id,
        );
    }
    if !allowed {
        return auth_error(
            StatusCode::FORBIDDEN,
            "permission_denied",
            "principal is not authorized for this regional action",
            &request_id,
        );
    }

    // Downstream profile routers do not need credential material. Removing it
    // narrows the chance of future middleware accidentally serializing it.
    request.headers_mut().remove(AUTHORIZATION);
    request.extensions_mut().insert(principal);
    request.extensions_mut().insert(Arc::clone(&state.audit));
    request
        .headers_mut()
        .insert(REQUEST_ID_HEADER.clone(), request_id_header(&request_id));
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(REQUEST_ID_HEADER.clone(), request_id_header(&request_id));
    response
}

fn authorization_header(headers: &HeaderMap) -> Option<&str> {
    let values: Vec<_> = headers.get_all(AUTHORIZATION).iter().collect();
    if values.len() != 1 {
        return if values.is_empty() { None } else { Some("") };
    }
    values[0].to_str().ok().or(Some(""))
}

fn authentication_reason(error: &AuthenticationError) -> DecisionReason {
    match error.kind() {
        AuthenticationErrorKind::Missing => DecisionReason::MissingCredential,
        AuthenticationErrorKind::Malformed => DecisionReason::MalformedCredential,
        AuthenticationErrorKind::Invalid => DecisionReason::InvalidCredential,
        AuthenticationErrorKind::Expired => DecisionReason::ExpiredCredential,
        AuthenticationErrorKind::NotYetValid => DecisionReason::NotYetValidCredential,
        AuthenticationErrorKind::Revoked => DecisionReason::RevokedCredential,
    }
}

fn authentication_failure_fields(
    request_id: &str,
    policy_id: &str,
    action: Action,
    error: &AuthenticationError,
) -> DecisionEventFields {
    DecisionEventFields {
        request_id: request_id.to_owned(),
        principal_id: "anonymous".into(),
        policy_id: policy_id.to_owned(),
        authentication_method: None,
        action,
        decision: Decision::Deny,
        reason: authentication_reason(error),
        scope: ResourceScope::new("", "", "", ""),
    }
}

fn authorization_decision_fields(
    request_id: &str,
    principal: &Principal,
    action: Action,
    allowed: bool,
    reason: DecisionReason,
    scope: ResourceScope,
) -> DecisionEventFields {
    DecisionEventFields {
        request_id: request_id.to_owned(),
        principal_id: principal.id().to_owned(),
        policy_id: principal.policy_id().to_owned(),
        authentication_method: Some(principal.authentication_method()),
        action,
        decision: if allowed {
            Decision::Allow
        } else {
            Decision::Deny
        },
        reason,
        scope,
    }
}

fn decision_reason(principal: &Principal, action: Action, allowed: bool) -> DecisionReason {
    if allowed {
        DecisionReason::PolicyGrant
    } else if principal.has_action(action) {
        DecisionReason::ScopeMismatch
    } else {
        DecisionReason::ActionNotGranted
    }
}

fn action_for_request(method: &Method, path: &str) -> Action {
    if path == AUDIT_PATH {
        return Action::AuditRead;
    }
    if path == BACKUP_PATH {
        return Action::BackupCreate;
    }
    if path == TOPOLOGY_PATH {
        return Action::TopologyRead;
    }
    if path == CATALOG_ROOT
        || path.starts_with(CATALOG_RESOURCE_PREFIX)
        || path.starts_with(CATALOG_TABLET_PREFIX)
    {
        return match *method {
            Method::PUT | Method::POST | Method::PATCH => Action::CatalogApply,
            Method::DELETE => Action::CatalogDelete,
            _ => Action::CatalogRead,
        };
    }
    if let Ok(Some(native)) = native_resource_request(path) {
        return if native.data_operation {
            if *method == Method::GET || (*method == Method::POST && native.query_post) {
                Action::DataRead
            } else {
                Action::DataWrite
            }
        } else {
            Action::RouteRead
        };
    }
    let data_segments = path
        .strip_prefix(RESOURCE_PREFIX)
        .map(|remainder| remainder.split('/').collect::<Vec<_>>())
        .filter(|segments| segments.get(8) == Some(&"data"));
    if let Some(segments) = data_segments {
        let is_bus_query = *method == Method::POST
            && segments.get(4) == Some(&"event-bus")
            && matches!(
                segments.get(9..),
                Some(["archive", "replay"] | ["deliveries", "query"])
            );
        if *method == Method::GET || is_bus_query {
            Action::DataRead
        } else {
            Action::DataWrite
        }
    } else {
        Action::RouteRead
    }
}

fn scope_for_path(path: &str) -> Result<ResourceScope, ()> {
    if path == CATALOG_ROOT
        || path == TOPOLOGY_PATH
        || path == BACKUP_PATH
        || path == AUDIT_PATH
        || path.starts_with(CATALOG_TABLET_PREFIX)
    {
        return Ok(ResourceScope::new("", "", "", ""));
    }
    if let Some(native) = native_resource_request(path)? {
        return Ok(native.scope);
    }
    let remainder = path
        .strip_prefix(CATALOG_RESOURCE_PREFIX)
        .or_else(|| path.strip_prefix(RESOURCE_PREFIX))
        .ok_or(())?;
    let segments: Vec<_> = remainder.split('/').collect();
    if segments.len() < 6 {
        return Err(());
    }
    let scope = ResourceScope::new(
        decode_segment(segments[0])?,
        decode_segment(segments[1])?,
        decode_segment(segments[2])?,
        decode_segment(segments[3])?,
    );
    valid_target_scope(&scope).then_some(scope).ok_or(())
}

#[derive(Debug)]
struct NativeResourceRequest {
    scope: ResourceScope,
    data_operation: bool,
    query_post: bool,
}

fn native_resource_request(path: &str) -> Result<Option<NativeResourceRequest>, ()> {
    let Some(remainder) = path.strip_prefix(NATIVE_RESOURCE_PREFIX) else {
        return Ok(None);
    };
    let segments: Vec<_> = remainder.split('/').collect();
    if segments.len() < 11
        || segments[1] != "projects"
        || segments[3] != "environments"
        || segments[5] != "namespaces"
        || !matches!(segments[7], "streams" | "queues" | "caches" | "buses")
        || segments[9] != "shards"
        || segments[10].parse::<u32>().is_err()
        || (segments.len() > 11 && segments[11..].iter().any(|segment| segment.is_empty()))
    {
        return Err(());
    }
    for segment in [
        segments[0],
        segments[2],
        segments[4],
        segments[6],
        segments[8],
    ] {
        if decode_segment(segment)?.is_empty() {
            return Err(());
        }
    }
    let scope = ResourceScope::new(
        decode_segment(segments[0])?,
        decode_segment(segments[2])?,
        decode_segment(segments[4])?,
        decode_segment(segments[6])?,
    );
    if !valid_target_scope(&scope) {
        return Err(());
    }
    Ok(Some(NativeResourceRequest {
        scope,
        data_operation: segments.len() > 11,
        query_post: segments[7] == "buses"
            && matches!(
                segments.get(11..),
                Some(["archive", "replay"] | ["deliveries", "query"])
            ),
    }))
}

fn decode_segment(segment: &str) -> Result<String, ()> {
    percent_decode_str(segment)
        .decode_utf8()
        .map(String::from)
        .map_err(|_| ())
}

fn valid_target_scope(scope: &ResourceScope) -> bool {
    [
        scope.organization.as_str(),
        scope.project.as_str(),
        scope.environment.as_str(),
        scope.namespace.as_str(),
    ]
    .into_iter()
    .all(valid_target_scope_value)
}

fn valid_target_scope_value(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && bytes[0].is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn request_id(headers: &HeaderMap) -> String {
    let values: Vec<_> = headers.get_all(&REQUEST_ID_HEADER).iter().collect();
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

fn request_id_header(request_id: &str) -> HeaderValue {
    HeaderValue::from_str(request_id).expect("validated request ID must be a valid header")
}

fn auth_error(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    request_id: &str,
) -> Response {
    let mut response = (status, Json(AuthErrorBody { code, message })).into_response();
    response
        .headers_mut()
        .insert(REQUEST_ID_HEADER.clone(), request_id_header(request_id));
    response
}

fn record_decision(
    audit: &AuditJournal,
    fields: DecisionEventFields,
) -> Result<(), epoch_auth::AuditJournalError> {
    let event = match DecisionEvent::new(fields) {
        Ok(event) => event,
        Err(audit_error) => {
            error!(error = %audit_error, "authorization audit event rejected");
            return Err(epoch_auth::AuditJournalError::InvalidEvent(
                audit_error.to_string(),
            ));
        }
    };
    info!(
        event_type = "authorization.decision",
        request_id = event.request_id(),
        principal_id = event.principal_id(),
        policy_id = event.policy_id(),
        authentication_method = event
            .authentication_method()
            .map_or("none", AuthenticationMethod::as_str),
        action = event.action().as_str(),
        decision = event.decision().as_str(),
        reason = event.reason().as_str(),
        organization = event.scope().organization,
        project = event.scope().project,
        environment = event.scope().environment,
        namespace = event.scope().namespace,
        "authorization decision"
    );
    audit.record(&event)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuditPageQuery {
    after_sequence: Option<String>,
    limit: Option<usize>,
}

async fn read_audit_events(
    Extension(principal): Extension<Principal>,
    Extension(audit): Extension<Arc<AuditJournal>>,
    Query(query): Query<AuditPageQuery>,
) -> Response {
    let after_sequence = match query.after_sequence.as_deref() {
        None => 0,
        Some(value) => match value.parse::<u64>() {
            Ok(parsed) if parsed.to_string() == value => parsed,
            _ => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(AuthErrorBody {
                        code: "invalid_argument",
                        message: "after_sequence must be a canonical decimal uint64",
                    }),
                )
                    .into_response();
            }
        },
    };
    let limit = query.limit.unwrap_or(100);
    match audit.read_page(after_sequence, limit, &principal) {
        Ok(page) => Json(page).into_response(),
        Err(epoch_auth::AuditJournalError::InvalidPageSize) => (
            StatusCode::BAD_REQUEST,
            Json(AuthErrorBody {
                code: "invalid_argument",
                message: "limit must be between 1 and 1000",
            }),
        )
            .into_response(),
        Err(error) => {
            error!(error = %error, "audit journal export failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(AuthErrorBody {
                    code: "audit_unavailable",
                    message: "audit journal could not be verified",
                }),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STREAM_ROUTE: &str = "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/streams/orders/shards/0";
    const QUEUE_ROUTE: &str = "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/queues/jobs/shards/0";
    const CACHE_ROUTE: &str = "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/caches/sessions/shards/0";
    const BUS_ROUTE: &str = "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/buses/events/shards/0";

    #[test]
    fn regional_backup_uses_a_dedicated_cluster_scoped_action() {
        assert_eq!(
            action_for_request(&Method::POST, BACKUP_PATH),
            Action::BackupCreate
        );
        assert_eq!(
            scope_for_path(BACKUP_PATH).unwrap(),
            ResourceScope::new("", "", "", "")
        );
    }

    #[test]
    fn tablet_membership_plans_are_cluster_scoped_catalog_mutations() {
        let path = "/experimental/v1/regional/catalog/tablets/41/membership";
        assert_eq!(
            action_for_request(&Method::POST, path),
            Action::CatalogApply
        );
        assert_eq!(
            scope_for_path(path).unwrap(),
            ResourceScope::new("", "", "", "")
        );
    }

    #[test]
    fn native_stream_routes_use_data_actions_only_after_the_shard_boundary() {
        assert_eq!(
            action_for_request(&Method::GET, STREAM_ROUTE),
            Action::RouteRead
        );
        assert_eq!(
            action_for_request(&Method::GET, &format!("{STREAM_ROUTE}/records")),
            Action::DataRead
        );
        assert_eq!(
            action_for_request(&Method::POST, &format!("{STREAM_ROUTE}/records")),
            Action::DataWrite
        );
        assert_eq!(
            action_for_request(
                &Method::PUT,
                &format!("{STREAM_ROUTE}/groups/billing/offsets")
            ),
            Action::DataWrite
        );
    }

    #[test]
    fn native_stream_scope_is_fully_qualified_and_strict() {
        assert_eq!(
            scope_for_path(STREAM_ROUTE).unwrap(),
            ResourceScope::new("acme", "shop", "dev", "core")
        );
        assert_eq!(
            scope_for_path(&format!("{STREAM_ROUTE}/groups/billing/lag")).unwrap(),
            ResourceScope::new("acme", "shop", "dev", "core")
        );
        assert!(scope_for_path("/v1/organizations/acme/projects/shop").is_err());
        assert!(
            scope_for_path(
                "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/tables/jobs/shards/0"
            )
            .is_err()
        );
    }

    #[test]
    fn native_queue_routes_use_the_same_scope_and_data_action_boundary() {
        assert_eq!(
            action_for_request(&Method::GET, QUEUE_ROUTE),
            Action::RouteRead
        );
        assert_eq!(
            action_for_request(&Method::GET, &format!("{QUEUE_ROUTE}/counts")),
            Action::DataRead
        );
        assert_eq!(
            action_for_request(&Method::POST, &format!("{QUEUE_ROUTE}/mutations")),
            Action::DataWrite
        );
        assert_eq!(
            scope_for_path(&format!("{QUEUE_ROUTE}/consumers/worker-a/flow")).unwrap(),
            ResourceScope::new("acme", "shop", "dev", "core")
        );
    }

    #[test]
    fn native_cache_routes_use_the_same_scope_and_data_action_boundary() {
        assert_eq!(
            action_for_request(&Method::GET, CACHE_ROUTE),
            Action::RouteRead
        );
        assert_eq!(
            action_for_request(&Method::GET, &format!("{CACHE_ROUTE}/observations")),
            Action::DataRead
        );
        assert_eq!(
            action_for_request(&Method::POST, &format!("{CACHE_ROUTE}/mutations")),
            Action::DataWrite
        );
        assert_eq!(
            scope_for_path(&format!("{CACHE_ROUTE}/observations")).unwrap(),
            ResourceScope::new("acme", "shop", "dev", "core")
        );
    }

    #[test]
    fn native_bus_routes_keep_query_posts_read_only() {
        assert_eq!(
            action_for_request(&Method::GET, BUS_ROUTE),
            Action::RouteRead
        );
        assert_eq!(
            action_for_request(&Method::POST, &format!("{BUS_ROUTE}/archive/replay")),
            Action::DataRead
        );
        assert_eq!(
            action_for_request(&Method::POST, &format!("{BUS_ROUTE}/deliveries/query")),
            Action::DataRead
        );
        assert_eq!(
            action_for_request(&Method::POST, &format!("{BUS_ROUTE}/mutations")),
            Action::DataWrite
        );
        assert_eq!(
            scope_for_path(&format!("{BUS_ROUTE}/deliveries/query")).unwrap(),
            ResourceScope::new("acme", "shop", "dev", "core")
        );
    }
}

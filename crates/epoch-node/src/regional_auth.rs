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
    Json, Router,
    extract::{Request, State},
    http::{
        HeaderMap, HeaderValue, Method, StatusCode,
        header::{AUTHORIZATION, HeaderName},
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use epoch_auth::{
    Action, AuthenticationError, AuthenticationErrorKind, BootstrapPolicy, Decision, DecisionEvent,
    DecisionReason, Principal, ResourceScope,
};
use percent_encoding::percent_decode_str;
use serde::Serialize;
use tracing::{error, info};

const CATALOG_ROOT: &str = "/experimental/v1/regional/catalog";
const CATALOG_RESOURCE_PREFIX: &str = "/experimental/v1/regional/catalog/resources/";
const RESOURCE_PREFIX: &str = "/experimental/v1/regional/resources/";
const TOPOLOGY_PATH: &str = "/experimental/v1/regional/topology";
const MAX_REQUEST_ID_BYTES: usize = 128;
static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

#[derive(Debug, Clone)]
struct RegionalAuthState {
    policy: Arc<BootstrapPolicy>,
}

#[derive(Debug, Serialize)]
struct AuthErrorBody {
    code: &'static str,
    message: &'static str,
}

/// Applies deny-by-default authentication and scoped authorization to every
/// route already registered in a regional router.
pub fn with_regional_auth(router: Router, policy: Arc<BootstrapPolicy>) -> Router {
    router.route_layer(middleware::from_fn_with_state(
        RegionalAuthState { policy },
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
            record_decision(
                &request_id,
                "anonymous",
                state.policy.id(),
                action,
                Decision::Deny,
                authentication_reason(&authentication_error),
                ResourceScope::new("", "", "", ""),
            );
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
    let allowed = principal.allows(action, &scope);
    let reason = decision_reason(&principal, action, allowed);
    record_decision(
        &request_id,
        principal.id(),
        principal.policy_id(),
        action,
        if allowed {
            Decision::Allow
        } else {
            Decision::Deny
        },
        reason,
        scope,
    );
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
    if path == TOPOLOGY_PATH {
        return Action::TopologyRead;
    }
    if path == CATALOG_ROOT || path.starts_with(CATALOG_RESOURCE_PREFIX) {
        return match *method {
            Method::PUT | Method::POST | Method::PATCH => Action::CatalogApply,
            Method::DELETE => Action::CatalogDelete,
            _ => Action::CatalogRead,
        };
    }
    let is_data_route = path
        .strip_prefix(RESOURCE_PREFIX)
        .and_then(|remainder| remainder.split('/').nth(8))
        == Some("data");
    if is_data_route {
        if *method == Method::GET {
            Action::DataRead
        } else {
            Action::DataWrite
        }
    } else {
        Action::RouteRead
    }
}

fn scope_for_path(path: &str) -> Result<ResourceScope, ()> {
    if path == CATALOG_ROOT || path == TOPOLOGY_PATH {
        return Ok(ResourceScope::new("", "", "", ""));
    }
    let remainder = path
        .strip_prefix(CATALOG_RESOURCE_PREFIX)
        .or_else(|| path.strip_prefix(RESOURCE_PREFIX))
        .ok_or(())?;
    let segments: Vec<_> = remainder.split('/').collect();
    if segments.len() < 6 {
        return Err(());
    }
    Ok(ResourceScope::new(
        decode_segment(segments[0])?,
        decode_segment(segments[1])?,
        decode_segment(segments[2])?,
        decode_segment(segments[3])?,
    ))
}

fn decode_segment(segment: &str) -> Result<String, ()> {
    percent_decode_str(segment)
        .decode_utf8()
        .map(String::from)
        .map_err(|_| ())
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

#[allow(clippy::too_many_arguments)]
fn record_decision(
    request_id: &str,
    principal_id: &str,
    policy_id: &str,
    action: Action,
    decision: Decision,
    reason: DecisionReason,
    scope: ResourceScope,
) {
    let event = match DecisionEvent::new(
        request_id,
        principal_id,
        policy_id,
        action,
        decision,
        reason,
        scope,
    ) {
        Ok(event) => event,
        Err(audit_error) => {
            error!(error = %audit_error, "authorization audit event rejected");
            return;
        }
    };
    info!(
        event_type = "authorization.decision",
        request_id = event.request_id(),
        principal_id = event.principal_id(),
        policy_id = event.policy_id(),
        action = event.action().as_str(),
        decision = event.decision().as_str(),
        reason = event.reason().as_str(),
        organization = event.scope().organization,
        project = event.scope().project,
        environment = event.scope().environment,
        namespace = event.scope().namespace,
        "authorization decision"
    );
}

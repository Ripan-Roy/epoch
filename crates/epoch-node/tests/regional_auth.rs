use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header::AUTHORIZATION},
    routing::any,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use epoch_auth::{AuditJournal, BootstrapPolicy};
use epoch_node::regional_auth::with_regional_auth;
use http_body_util::BodyExt as _;
use ring::signature::Ed25519KeyPair;
use serde_json::json;
use tempfile::tempdir;
use tower::ServiceExt;

const POLICY: &[u8] = include_bytes!("../../../spec/auth/bootstrap-policy-v1.example.json");
const IDENTITY_POLICY: &[u8] = include_bytes!("../../../spec/auth/identity-policy-v2.example.json");
const OIDC_SIGNING_SEED: [u8; 32] = [
    0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec, 0x2c, 0xc4,
    0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03, 0x1c, 0xae, 0x7f, 0x60,
];
const CATALOG_RESOURCE: &str = "/experimental/v1/regional/catalog/resources/{organization}/{project}/{environment}/{namespace}/{kind}/{name}";
const CATALOG_TABLET_MEMBERSHIP: &str =
    "/experimental/v1/regional/catalog/tablets/{tablet_id}/membership";
const CONTROL_ROUTE: &str = "/experimental/v1/regional/control/{*operation}";
const RESOURCE_ROUTE: &str = "/experimental/v1/regional/resources/{organization}/{project}/{environment}/{namespace}/{kind}/{name}/shards/{shard}";
const DATA_ROUTE: &str = "/experimental/v1/regional/resources/{organization}/{project}/{environment}/{namespace}/{kind}/{name}/shards/{shard}/data/{*operation}";
const TOPOLOGY_ROUTE: &str = "/experimental/v1/regional/topology";
const BACKUP_ROUTE: &str = "/v1/admin/backups";
const AUDIT_ROUTE: &str = "/v1/admin/audit/events";
const NATIVE_STREAM_ROUTE: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/streams/{name}/shards/{shard}";
const NATIVE_STREAM_DATA: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/streams/{name}/shards/{shard}/{*operation}";
const NATIVE_QUEUE_ROUTE: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/queues/{name}/shards/{shard}";
const NATIVE_QUEUE_DATA: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/queues/{name}/shards/{shard}/{*operation}";
const NATIVE_CACHE_ROUTE: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/caches/{name}/shards/{shard}";
const NATIVE_CACHE_DATA: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/caches/{name}/shards/{shard}/{*operation}";
const NATIVE_BUS_ROUTE: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/buses/{name}/shards/{shard}";
const NATIVE_BUS_DATA: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/buses/{name}/shards/{shard}/{*operation}";

fn protected_router() -> Router {
    protected_router_with_policy(POLICY)
}

fn protected_router_with_policy(policy_document: &[u8]) -> Router {
    let router = Router::new()
        .route(CATALOG_RESOURCE, any(|| async { StatusCode::NO_CONTENT }))
        .route(
            CATALOG_TABLET_MEMBERSHIP,
            any(|| async { StatusCode::NO_CONTENT }),
        )
        .route(CONTROL_ROUTE, any(|| async { StatusCode::NO_CONTENT }))
        .route(RESOURCE_ROUTE, any(|| async { StatusCode::NO_CONTENT }))
        .route(DATA_ROUTE, any(|| async { StatusCode::NO_CONTENT }))
        .route(
            NATIVE_STREAM_ROUTE,
            any(|| async { StatusCode::NO_CONTENT }),
        )
        .route(NATIVE_STREAM_DATA, any(|| async { StatusCode::NO_CONTENT }))
        .route(NATIVE_QUEUE_ROUTE, any(|| async { StatusCode::NO_CONTENT }))
        .route(NATIVE_QUEUE_DATA, any(|| async { StatusCode::NO_CONTENT }))
        .route(NATIVE_CACHE_ROUTE, any(|| async { StatusCode::NO_CONTENT }))
        .route(NATIVE_CACHE_DATA, any(|| async { StatusCode::NO_CONTENT }))
        .route(NATIVE_BUS_ROUTE, any(|| async { StatusCode::NO_CONTENT }))
        .route(NATIVE_BUS_DATA, any(|| async { StatusCode::NO_CONTENT }))
        .route(TOPOLOGY_ROUTE, any(|| async { StatusCode::NO_CONTENT }))
        .route(BACKUP_ROUTE, any(|| async { StatusCode::NO_CONTENT }));
    let policy = BootstrapPolicy::from_json(policy_document).unwrap();
    let directory = tempdir().unwrap();
    let audit = AuditJournal::open(directory.path().join("audit.ndjson")).unwrap();
    with_regional_auth(router, Arc::new(policy), Arc::new(audit))
}

#[tokio::test]
async fn regional_authentication_and_scope_fail_closed() {
    let router = protected_router();
    let missing = call(
        router.clone(),
        Method::GET,
        "/experimental/v1/regional/resources/acme/payments/production/orders/stream/events/shards/0/data/records",
        None,
    )
    .await;
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
    assert!(missing.headers().contains_key("x-request-id"));

    let exact_reader = call(
        router.clone(),
        Method::GET,
        "/experimental/v1/regional/resources/acme/payments/production/orders/stream/events/shards/0/data/records",
        Some("epoch-dev-reader-v1"),
    )
    .await;
    assert_eq!(exact_reader.status(), StatusCode::NO_CONTENT);

    let bus_query = call(
        router.clone(),
        Method::POST,
        "/experimental/v1/regional/resources/acme/payments/production/orders/event-bus/events/shards/0/data/archive/replay",
        Some("epoch-dev-reader-v1"),
    )
    .await;
    assert_eq!(bus_query.status(), StatusCode::NO_CONTENT);

    let bus_mutation = call(
        router.clone(),
        Method::POST,
        "/experimental/v1/regional/resources/acme/payments/production/orders/event-bus/events/shards/0/data/mutations",
        Some("epoch-dev-reader-v1"),
    )
    .await;
    assert_eq!(bus_mutation.status(), StatusCode::FORBIDDEN);

    let cross_tenant = call(
        router.clone(),
        Method::GET,
        "/experimental/v1/regional/resources/otherco/payments/production/orders/stream/events/shards/0/data/records",
        Some("epoch-dev-reader-v1"),
    )
    .await;
    assert_eq!(cross_tenant.status(), StatusCode::FORBIDDEN);

    let invalid_scope = call(
        router.clone(),
        Method::GET,
        "/experimental/v1/regional/resources/acme/payments/production/orders%3Cunsafe%3E/stream/events/shards/0/data/records",
        Some("epoch-dev-reader-v1"),
    )
    .await;
    assert_eq!(invalid_scope.status(), StatusCode::BAD_REQUEST);

    let denied_write = call(
        router,
        Method::PUT,
        "/experimental/v1/regional/resources/acme/payments/production/orders/stream/events/shards/0/data/records",
        Some("epoch-dev-reader-v1"),
    )
    .await;
    assert_eq!(denied_write.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn regional_control_workload_can_reconcile_catalog_but_not_data() {
    let router = protected_router();
    let catalog = call(
        router.clone(),
        Method::PUT,
        "/experimental/v1/regional/catalog/resources/acme/payments/production/orders/stream/events",
        Some("epoch-dev-control-v1"),
    )
    .await;
    assert_eq!(catalog.status(), StatusCode::NO_CONTENT);

    let replacement = call(
        router.clone(),
        Method::POST,
        "/experimental/v1/regional/catalog/tablets/41/membership",
        Some("epoch-dev-control-v1"),
    )
    .await;
    assert_eq!(replacement.status(), StatusCode::NO_CONTENT);
    let reader_replacement = call(
        router.clone(),
        Method::POST,
        "/experimental/v1/regional/catalog/tablets/41/membership",
        Some("epoch-dev-reader-v1"),
    )
    .await;
    assert_eq!(reader_replacement.status(), StatusCode::FORBIDDEN);

    for (method, path) in [
        (Method::GET, "/experimental/v1/regional/control/resources"),
        (Method::PUT, "/experimental/v1/regional/control/resources"),
        (
            Method::DELETE,
            "/experimental/v1/regional/control/materializations/acme/payments/production/orders/stream/events",
        ),
    ] {
        let response = call(router.clone(), method, path, Some("epoch-dev-control-v1")).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT, "{path}");
    }
    let tenant_reader_control = call(
        router.clone(),
        Method::GET,
        "/experimental/v1/regional/control/resources",
        Some("epoch-dev-reader-v1"),
    )
    .await;
    assert_eq!(tenant_reader_control.status(), StatusCode::FORBIDDEN);

    let topology = call(
        router.clone(),
        Method::GET,
        "/experimental/v1/regional/topology",
        Some("epoch-dev-control-v1"),
    )
    .await;
    assert_eq!(topology.status(), StatusCode::NO_CONTENT);

    let backup = call(
        router.clone(),
        Method::POST,
        BACKUP_ROUTE,
        Some("epoch-dev-control-v1"),
    )
    .await;
    assert_eq!(backup.status(), StatusCode::NO_CONTENT);
    let reader_backup = call(
        router.clone(),
        Method::POST,
        BACKUP_ROUTE,
        Some("epoch-dev-reader-v1"),
    )
    .await;
    assert_eq!(reader_backup.status(), StatusCode::FORBIDDEN);

    let resource_named_data = call(
        router.clone(),
        Method::GET,
        "/experimental/v1/regional/resources/acme/payments/production/orders/stream/data/shards/0",
        Some("epoch-dev-control-v1"),
    )
    .await;
    assert_eq!(resource_named_data.status(), StatusCode::NO_CONTENT);

    let data = call(
        router,
        Method::PUT,
        "/experimental/v1/regional/resources/acme/payments/production/orders/stream/events/shards/0/data/records",
        Some("epoch-dev-control-v1"),
    )
    .await;
    assert_eq!(data.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn native_stream_v1_uses_the_same_fail_closed_scope_and_data_actions() {
    let router = protected_router();
    let route = "/v1/organizations/acme/projects/payments/environments/production/namespaces/orders/streams/events/shards/0";

    let missing = call(router.clone(), Method::GET, route, None).await;
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

    let discovery = call(
        router.clone(),
        Method::GET,
        route,
        Some("epoch-dev-reader-v1"),
    )
    .await;
    assert_eq!(discovery.status(), StatusCode::NO_CONTENT);

    let read = call(
        router.clone(),
        Method::GET,
        &format!("{route}/records"),
        Some("epoch-dev-reader-v1"),
    )
    .await;
    assert_eq!(read.status(), StatusCode::NO_CONTENT);

    let denied_write = call(
        router.clone(),
        Method::POST,
        &format!("{route}/records"),
        Some("epoch-dev-reader-v1"),
    )
    .await;
    assert_eq!(denied_write.status(), StatusCode::FORBIDDEN);

    let session_read = call(
        router.clone(),
        Method::GET,
        &format!("{route}/groups/billing/sessions"),
        Some("epoch-dev-reader-v1"),
    )
    .await;
    assert_eq!(session_read.status(), StatusCode::NO_CONTENT);

    let denied_session_join = call(
        router.clone(),
        Method::POST,
        &format!("{route}/groups/billing/sessions"),
        Some("epoch-dev-reader-v1"),
    )
    .await;
    assert_eq!(denied_session_join.status(), StatusCode::FORBIDDEN);

    let claimed_read = call(
        router.clone(),
        Method::GET,
        &format!("{route}/groups/billing/claimed-records?member_id=member-a&group_generation=3"),
        Some("epoch-dev-reader-v1"),
    )
    .await;
    assert_eq!(claimed_read.status(), StatusCode::NO_CONTENT);

    let denied_claim = call(
        router.clone(),
        Method::PUT,
        &format!("{route}/groups/billing/claim"),
        Some("epoch-dev-reader-v1"),
    )
    .await;
    assert_eq!(denied_claim.status(), StatusCode::FORBIDDEN);

    let cross_tenant = call(
        router.clone(),
        Method::GET,
        "/v1/organizations/otherco/projects/payments/environments/production/namespaces/orders/streams/events/shards/0/records",
        Some("epoch-dev-reader-v1"),
    )
    .await;
    assert_eq!(cross_tenant.status(), StatusCode::FORBIDDEN);

    let control_discovery = call(
        router.clone(),
        Method::GET,
        route,
        Some("epoch-dev-control-v1"),
    )
    .await;
    assert_eq!(control_discovery.status(), StatusCode::NO_CONTENT);
    let control_data = call(
        router,
        Method::GET,
        &format!("{route}/records"),
        Some("epoch-dev-control-v1"),
    )
    .await;
    assert_eq!(control_data.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn native_queue_v1_uses_the_same_fail_closed_scope_and_data_actions() {
    let router = protected_router();
    let route = "/v1/organizations/acme/projects/payments/environments/production/namespaces/orders/queues/jobs/shards/0";

    assert_eq!(
        call(router.clone(), Method::GET, route, None)
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            router.clone(),
            Method::GET,
            route,
            Some("epoch-dev-reader-v1")
        )
        .await
        .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(
            router.clone(),
            Method::GET,
            &format!("{route}/counts"),
            Some("epoch-dev-reader-v1")
        )
        .await
        .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(
            router.clone(),
            Method::POST,
            &format!("{route}/mutations"),
            Some("epoch-dev-reader-v1")
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            router.clone(),
            Method::GET,
            "/v1/organizations/otherco/projects/payments/environments/production/namespaces/orders/queues/jobs/shards/0/counts",
            Some("epoch-dev-reader-v1")
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            router.clone(),
            Method::GET,
            route,
            Some("epoch-dev-control-v1")
        )
        .await
        .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(
            router,
            Method::GET,
            &format!("{route}/counts"),
            Some("epoch-dev-control-v1")
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn native_cache_v1_uses_the_same_fail_closed_scope_and_data_actions() {
    let router = protected_router();
    let route = "/v1/organizations/acme/projects/payments/environments/production/namespaces/orders/caches/sessions/shards/0";

    assert_eq!(
        call(router.clone(), Method::GET, route, None)
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            router.clone(),
            Method::GET,
            route,
            Some("epoch-dev-reader-v1")
        )
        .await
        .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(
            router.clone(),
            Method::GET,
            &format!("{route}/observations?key=session-1"),
            Some("epoch-dev-reader-v1")
        )
        .await
        .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(
            router.clone(),
            Method::POST,
            &format!("{route}/mutations"),
            Some("epoch-dev-reader-v1")
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            router,
            Method::GET,
            "/v1/organizations/otherco/projects/payments/environments/production/namespaces/orders/caches/sessions/shards/0/status",
            Some("epoch-dev-reader-v1")
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn native_bus_v1_authorizes_queries_separately_from_mutations() {
    let router = protected_router();
    let route = "/v1/organizations/acme/projects/payments/environments/production/namespaces/orders/buses/events/shards/0";

    assert_eq!(
        call(router.clone(), Method::GET, route, None)
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            router.clone(),
            Method::GET,
            route,
            Some("epoch-dev-reader-v1")
        )
        .await
        .status(),
        StatusCode::NO_CONTENT
    );
    for query in ["archive/replay", "deliveries/query"] {
        assert_eq!(
            call(
                router.clone(),
                Method::POST,
                &format!("{route}/{query}"),
                Some("epoch-dev-reader-v1")
            )
            .await
            .status(),
            StatusCode::NO_CONTENT
        );
    }
    assert_eq!(
        call(
            router.clone(),
            Method::POST,
            &format!("{route}/mutations"),
            Some("epoch-dev-reader-v1")
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            router,
            Method::GET,
            "/v1/organizations/otherco/projects/payments/environments/production/namespaces/orders/buses/events/shards/0/status",
            Some("epoch-dev-reader-v1")
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn regional_audit_export_is_authorized_bounded_and_credential_free() {
    let router = protected_router();
    let data_route = "/v1/organizations/acme/projects/payments/environments/production/namespaces/orders/streams/events/shards/0/records";

    let allowed = call(
        router.clone(),
        Method::GET,
        data_route,
        Some("epoch-dev-reader-v1"),
    )
    .await;
    assert_eq!(allowed.status(), StatusCode::NO_CONTENT);

    let denied = call(
        router.clone(),
        Method::GET,
        AUDIT_ROUTE,
        Some("epoch-dev-reader-v1"),
    )
    .await;
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);

    let exported = call(
        router.clone(),
        Method::GET,
        &format!("{AUDIT_ROUTE}?after_sequence=0&limit=10"),
        Some("epoch-dev-admin-v1"),
    )
    .await;
    assert_eq!(exported.status(), StatusCode::OK);
    let body = exported.into_body().collect().await.unwrap().to_bytes();
    let document: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let records = document["records"].as_array().unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["sequence"], "1");
    assert_eq!(records[0]["event"]["action"], "data.read");
    assert_eq!(records[0]["event"]["decision"], "allow");
    assert_eq!(
        records[0]["event"]["authentication_method"],
        "bootstrap_token"
    );
    assert_eq!(records[1]["event"]["action"], "audit.read");
    assert_eq!(records[1]["event"]["decision"], "deny");
    assert_eq!(records[2]["event"]["action"], "audit.read");
    assert_eq!(records[2]["event"]["decision"], "allow");
    assert_eq!(document["next_sequence"], "3");
    assert_eq!(document["end_of_journal"], true);
    let encoded = String::from_utf8(body.to_vec()).unwrap();
    assert!(!encoded.contains("epoch-dev-reader-v1"));
    assert!(!encoded.contains("epoch-dev-admin-v1"));

    for query in [
        "after_sequence=01",
        "after_sequence=",
        "limit=0",
        "limit=",
        "limit=1&limit=2",
        "unknown=1",
    ] {
        let invalid_page = call(
            router.clone(),
            Method::GET,
            &format!("{AUDIT_ROUTE}?{query}"),
            Some("epoch-dev-admin-v1"),
        )
        .await;
        assert_eq!(
            invalid_page.status(),
            StatusCode::BAD_REQUEST,
            "query {query}"
        );
    }
}

#[tokio::test]
async fn regional_boundary_accepts_short_lived_oidc_identity_and_audits_its_method() {
    let router = protected_router_with_policy(IDENTITY_POLICY);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let token = signed_oidc_token(&json!({
        "iss":"https://identity.epoch.example",
        "aud":"epoch-api",
        "sub":"workload-orders",
        "iat":now.saturating_sub(10),
        "nbf":now.saturating_sub(10),
        "exp":now + 300,
        "jti":"regional-boundary-active-1",
        "epoch_roles":["reader","auditor"],
        "epoch_organization":"acme",
        "epoch_project":"payments",
        "epoch_environment":"production",
        "epoch_namespace":"orders"
    }));
    let data_route = "/v1/organizations/acme/projects/payments/environments/production/namespaces/orders/streams/events/shards/0/records";
    let allowed = call(router.clone(), Method::GET, data_route, Some(&token)).await;
    assert_eq!(allowed.status(), StatusCode::NO_CONTENT);
    let denied = call(
        router.clone(),
        Method::GET,
        "/v1/organizations/acme/projects/payments/environments/production/namespaces/other/streams/events/shards/0/records",
        Some(&token),
    )
    .await;
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);

    let exported = call(
        router,
        Method::GET,
        &format!("{AUDIT_ROUTE}?limit=10"),
        Some(&token),
    )
    .await;
    assert_eq!(exported.status(), StatusCode::OK);
    let body = exported.into_body().collect().await.unwrap().to_bytes();
    let document: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let records = document["records"].as_array().unwrap();
    assert_eq!(
        records.len(),
        2,
        "tenant filtering must omit other namespaces"
    );
    assert!(records.iter().all(|record| {
        record["event"]["authentication_method"] == "oidc_eddsa"
            && record["event"]["principal_id"]
                .as_str()
                .is_some_and(|principal| principal.starts_with("oidc:"))
    }));
    assert!(!String::from_utf8_lossy(&body).contains(&token));
}

#[tokio::test]
async fn regional_boundary_fails_closed_after_live_audit_tampering() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("audit.ndjson");
    let audit = Arc::new(AuditJournal::open(&path).unwrap());
    let router = Router::new().route(NATIVE_STREAM_DATA, any(|| async { StatusCode::NO_CONTENT }));
    let policy = Arc::new(BootstrapPolicy::from_json(POLICY).unwrap());
    let router = with_regional_auth(router, policy, audit);
    let data_route = "/v1/organizations/acme/projects/payments/environments/production/namespaces/orders/streams/events/shards/0/records";
    assert_eq!(
        call(
            router.clone(),
            Method::GET,
            data_route,
            Some("epoch-dev-reader-v1")
        )
        .await
        .status(),
        StatusCode::NO_CONTENT
    );
    let mut tampered = std::fs::read(&path).unwrap();
    let position = tampered
        .windows("development-reader".len())
        .position(|window| window == b"development-reader")
        .unwrap();
    tampered[position] = b'x';
    std::fs::write(&path, tampered).unwrap();

    let detected = call(
        router.clone(),
        Method::GET,
        AUDIT_ROUTE,
        Some("epoch-dev-admin-v1"),
    )
    .await;
    assert_eq!(detected.status(), StatusCode::SERVICE_UNAVAILABLE);
    let refused = call(router, Method::GET, data_route, Some("epoch-dev-reader-v1")).await;
    assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
}

fn signed_oidc_token(claims: &serde_json::Value) -> String {
    let header = json!({
        "alg":"EdDSA",
        "kid":"epoch-test-ed25519-1",
        "typ":"JWT"
    });
    let header = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).unwrap());
    let claims = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
    let signing_input = format!("{header}.{claims}");
    let key_pair = Ed25519KeyPair::from_seed_unchecked(&OIDC_SIGNING_SEED).unwrap();
    let signature = URL_SAFE_NO_PAD.encode(key_pair.sign(signing_input.as_bytes()).as_ref());
    format!("{signing_input}.{signature}")
}

async fn call(
    router: Router,
    method: Method,
    path: &str,
    token: Option<&str>,
) -> axum::response::Response {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(token) = token {
        builder = builder.header(AUTHORIZATION, format!("Bearer {token}"));
    }
    router
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

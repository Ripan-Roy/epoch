use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header::AUTHORIZATION},
    routing::any,
};
use epoch_auth::BootstrapPolicy;
use epoch_node::regional_auth::with_regional_auth;
use tower::ServiceExt;

const POLICY: &[u8] = include_bytes!("../../../spec/auth/bootstrap-policy-v1.example.json");
const CATALOG_RESOURCE: &str = "/experimental/v1/regional/catalog/resources/{organization}/{project}/{environment}/{namespace}/{kind}/{name}";
const RESOURCE_ROUTE: &str = "/experimental/v1/regional/resources/{organization}/{project}/{environment}/{namespace}/{kind}/{name}/shards/{shard}";
const DATA_ROUTE: &str = "/experimental/v1/regional/resources/{organization}/{project}/{environment}/{namespace}/{kind}/{name}/shards/{shard}/data/{*operation}";

fn protected_router() -> Router {
    let router = Router::new()
        .route(CATALOG_RESOURCE, any(|| async { StatusCode::NO_CONTENT }))
        .route(RESOURCE_ROUTE, any(|| async { StatusCode::NO_CONTENT }))
        .route(DATA_ROUTE, any(|| async { StatusCode::NO_CONTENT }));
    let policy = BootstrapPolicy::from_json(POLICY).unwrap();
    with_regional_auth(router, Arc::new(policy))
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

    let cross_tenant = call(
        router.clone(),
        Method::GET,
        "/experimental/v1/regional/resources/otherco/payments/production/orders/stream/events/shards/0/data/records",
        Some("epoch-dev-reader-v1"),
    )
    .await;
    assert_eq!(cross_tenant.status(), StatusCode::FORBIDDEN);

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

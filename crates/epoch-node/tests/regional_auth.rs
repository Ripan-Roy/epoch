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
const TOPOLOGY_ROUTE: &str = "/experimental/v1/regional/topology";
const NATIVE_STREAM_ROUTE: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/streams/{name}/shards/{shard}";
const NATIVE_STREAM_DATA: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/streams/{name}/shards/{shard}/{*operation}";
const NATIVE_QUEUE_ROUTE: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/queues/{name}/shards/{shard}";
const NATIVE_QUEUE_DATA: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/queues/{name}/shards/{shard}/{*operation}";
const NATIVE_CACHE_ROUTE: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/caches/{name}/shards/{shard}";
const NATIVE_CACHE_DATA: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/caches/{name}/shards/{shard}/{*operation}";
const NATIVE_BUS_ROUTE: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/buses/{name}/shards/{shard}";
const NATIVE_BUS_DATA: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/buses/{name}/shards/{shard}/{*operation}";

fn protected_router() -> Router {
    let router = Router::new()
        .route(CATALOG_RESOURCE, any(|| async { StatusCode::NO_CONTENT }))
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
        .route(TOPOLOGY_ROUTE, any(|| async { StatusCode::NO_CONTENT }));
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

    let topology = call(
        router.clone(),
        Method::GET,
        "/experimental/v1/regional/topology",
        Some("epoch-dev-control-v1"),
    )
    .await;
    assert_eq!(topology.status(), StatusCode::NO_CONTENT);

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

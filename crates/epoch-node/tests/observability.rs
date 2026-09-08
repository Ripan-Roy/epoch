use std::time::Duration;

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
    routing::post,
};
use epoch_node::observability::{metrics_router, with_http_observability};
use epoch_observability::{MetricsRegistry, Outcome, Stage, TenantScope};
use http_body_util::BodyExt as _;
use tower::ServiceExt as _;

#[tokio::test]
async fn request_middleware_preserves_correlation_and_records_bounded_metrics() {
    let metrics = MetricsRegistry::new("epoch-node", 8).unwrap();
    let app = with_http_observability(
        Router::new().route(
            "/v1/queues/{name}/acquire",
            post(|| async { StatusCode::NO_CONTENT }),
        ),
        metrics.clone(),
    );

    let response = app
        .oneshot(
            Request::post("/v1/queues/customer-secret/acquire")
                .header("x-request-id", "caller-123")
                .header(
                    "traceparent",
                    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(response.headers()["x-request-id"], "caller-123");
    let rendered = metrics.render_prometheus();
    assert!(rendered.contains("profile=\"queue\",operation=\"acquire\""));
    assert!(!rendered.contains("customer-secret"));
}

#[tokio::test]
async fn invalid_request_ids_are_replaced_and_metrics_are_internal_only() {
    let metrics = MetricsRegistry::new("epoch-node", 8).unwrap();
    let public = with_http_observability(
        Router::new().route("/work", post(|| async { StatusCode::OK })),
        metrics.clone(),
    );
    let response = public
        .oneshot(
            Request::post("/work")
                .header("x-request-id", "contains whitespace")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let request_id = response.headers()["x-request-id"].to_str().unwrap();
    assert!(request_id.starts_with("request-"));

    let response = metrics_router(metrics)
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-type"],
        "text/plain; version=0.0.4; charset=utf-8"
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert!(
        String::from_utf8(body.to_vec())
            .unwrap()
            .contains("epoch_http_requests_total")
    );
}

#[tokio::test]
async fn internal_diagnostic_attributes_recent_tail_latency() {
    let metrics = MetricsRegistry::new("epoch-node", 8).unwrap();
    let tenant = TenantScope::new("acme", "payments", "prod", "core").unwrap();
    metrics.record_stage(
        &tenant,
        "stream",
        Stage::Replication,
        Duration::from_millis(640),
        Outcome::Success,
    );

    let response = metrics_router(metrics)
        .oneshot(
            Request::get(
                "/v1/diagnostics/latency?organization=acme&project=payments&environment=prod&namespace=core&profile=stream",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let diagnosis: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(diagnosis["cause"], "replication");
    assert_eq!(diagnosis["observed_p99_ms"], 640);
}

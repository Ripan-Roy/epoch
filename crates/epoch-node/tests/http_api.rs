use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode},
};
use epoch_core::{DeploymentMode, ManualClock};
use epoch_engine::EpochEngine;
use epoch_node::router;
use http_body_util::BodyExt as _;
use serde_json::{Value, json};
use tower::ServiceExt as _;

fn test_app() -> Router {
    router(Arc::new(EpochEngine::new(
        DeploymentMode::Standalone,
        Arc::new(ManualClock::new(1_000)),
    )))
}

async fn call(app: &Router, method: Method, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    let request_body = if let Some(body) = body {
        builder = builder.header("content-type", "application/json");
        Body::from(serde_json::to_vec(&body).expect("test JSON serializes"))
    } else {
        Body::empty()
    };
    let response = app
        .clone()
        .oneshot(builder.body(request_body).expect("test request builds"))
        .await
        .expect("router returns a response");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("response body is readable")
        .to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("API returns JSON")
    };
    (status, value)
}

#[tokio::test]
async fn standalone_rejects_unavailable_quorum_durability() {
    let app = test_app();
    let (status, body) = call(
        &app,
        Method::POST,
        "/v1/streams/protected",
        Some(json!({
            "partitions": 1,
            "durability": "quorum_durable",
            "max_records_per_partition": null
        })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "response was {body}");
    assert_eq!(body["error"]["code"], "invalid_argument");
}

async fn create_profiles(app: &Router) {
    let resources = [
        (
            "/v1/caches/sessions",
            json!({
                "max_entries": 100,
                "default_ttl_ms": null,
                "eviction": "no_eviction",
                "durability": "volatile"
            }),
        ),
        (
            "/v1/streams/orders",
            json!({
                "partitions": 2,
                "durability": "volatile",
                "max_records_per_partition": null
            }),
        ),
        (
            "/v1/queues/jobs",
            json!({
                "durability": "volatile",
                "visibility_timeout_ms": 30_000,
                "max_messages": 100,
                "retry": {
                    "strategy": "exponential",
                    "initial_delay_ms": 100,
                    "max_delay_ms": 10_000,
                    "jitter_percent": 0,
                    "max_attempts": 3,
                    "max_age_ms": null
                },
                "dedupe_window_ms": 60_000
            }),
        ),
        (
            "/v1/buses/events",
            json!({"durability": "volatile", "archive": true}),
        ),
    ];
    for (uri, config) in resources {
        let (status, _) = call(app, Method::POST, uri, Some(config)).await;
        assert_eq!(status, StatusCode::CREATED, "failed to create {uri}");
    }
}

async fn exercise_cache(app: &Router) {
    let (status, item) = call(
        app,
        Method::PUT,
        "/v1/caches/sessions/keys/user-42",
        Some(json!({
            "value": {"kind": "string", "value": "active"},
            "ttl_ms": 5_000
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(item["version"], 1);
}

async fn exercise_stream(app: &Router) {
    let stream_event = json!({
        "envelope": {
            "id": "order-1",
            "source": "checkout",
            "type": "order.created",
            "time_ms": 1_000,
            "key": "customer-42",
            "payload": {"order_id": "1"}
        }
    });
    let (status, receipt) = call(
        app,
        Method::POST,
        "/v1/streams/orders/records",
        Some(stream_event),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(receipt["offset"], 0);
}

async fn exercise_queue(app: &Router) {
    let queue_event = json!({
        "id": "job-1",
        "source": "api",
        "type": "job.requested",
        "time_ms": 1_000,
        "payload": {"job_id": "1"}
    });
    let (status, _) = call(
        app,
        Method::POST,
        "/v1/queues/jobs/messages",
        Some(queue_event),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, deliveries) = call(
        app,
        Method::POST,
        "/v1/queues/jobs/acquire",
        Some(json!({"consumer": "worker-1", "max_messages": 1})),
    )
    .await;
    let lease_token = deliveries[0]["lease_token"]
        .as_str()
        .expect("delivery has a lease token");
    let (status, _) = call(
        app,
        Method::POST,
        "/v1/queues/jobs/settle",
        Some(json!({"action": "ack", "token": lease_token})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

async fn exercise_bus(app: &Router) {
    let (status, _) = call(
        app,
        Method::PUT,
        "/v1/buses/events/subscriptions/job-route",
        Some(json!({
            "name": "path-overrides-name",
            "filter": {"event_type_patterns": ["order.*"]},
            "target": {"kind": "queue", "resource": "jobs"}
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, outcome) = call(
        app,
        Method::POST,
        "/v1/buses/events/events",
        Some(json!({
            "id": "order-2",
            "source": "checkout",
            "type": "order.created",
            "time_ms": 1_000,
            "payload": {"order_id": "2"}
        })),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(outcome["routes"][0]["status"], "delivered");
}

#[tokio::test]
async fn native_http_api_exercises_all_four_profiles() {
    let app = test_app();
    create_profiles(&app).await;
    exercise_cache(&app).await;
    exercise_stream(&app).await;
    exercise_queue(&app).await;
    exercise_bus(&app).await;

    let (_, health) = call(&app, Method::GET, "/healthz", None).await;
    assert_eq!(health["resource_count"], 4);
    assert_eq!(health["guarantee_ceiling"], "volatile");
}

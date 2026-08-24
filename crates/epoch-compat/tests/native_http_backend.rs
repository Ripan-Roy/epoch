use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::State,
    http::{HeaderMap, Method, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
};
use epoch_compat::{
    BackendError, CacheValue, CompatibilityBackend, NativeHttpBackend, NativeHttpConfig,
    QueueMessage, StreamRecord,
};
use epoch_tablet::{StreamBatchPayload, StreamCompression, decode_stream_batch_payload};
use serde_json::{Value, json};
use tokio::{net::TcpListener, task::JoinHandle};
use url::Url;

#[derive(Debug, Clone)]
struct ObservedRequest {
    method: Method,
    path: String,
    query: Option<String>,
    authorization: Option<String>,
    generation: Option<String>,
    tablet_epoch: Option<String>,
    consistency: Option<String>,
    body: Value,
}

#[derive(Debug)]
struct MockNativeApi {
    endpoint: Url,
    observed: Arc<Mutex<Vec<ObservedRequest>>>,
    task: JoinHandle<()>,
}

impl MockNativeApi {
    async fn start() -> Self {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route("/{*path}", any(native_response))
            .with_state(Arc::clone(&observed));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            endpoint: Url::parse(&format!("http://{address}")).unwrap(),
            observed,
            task,
        }
    }
}

impl Drop for MockNativeApi {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn native_response(
    State(observed): State<Arc<Mutex<Vec<ObservedRequest>>>>,
    request: Request<Body>,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let query = request.uri().query().map(str::to_owned);
    let headers = request.headers().clone();
    let bytes = to_bytes(request.into_body(), 8 * 1024 * 1024)
        .await
        .unwrap();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    observed.lock().unwrap().push(ObservedRequest {
        method: method.clone(),
        path: path.clone(),
        query,
        authorization: header(&headers, "authorization"),
        generation: header(&headers, "x-epoch-resource-generation"),
        tablet_epoch: header(&headers, "x-epoch-tablet-epoch"),
        consistency: header(&headers, "x-epoch-read-consistency"),
        body: body.clone(),
    });

    if path.contains("/queues/oversized/") {
        return Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(vec![b'x'; 16 * 1024 * 1024 + 1]))
            .unwrap();
    }
    if path.contains("/queues/missing/") {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"code":"route_not_found", "message":"must not escape"})),
        )
            .into_response();
    }
    if is_discovery(&path) {
        let partitioning = path.contains("/streams/").then_some(json!({
            "algorithm":"fnv1a64_utf8_mod_n_v1",
            "key_encoding":"utf8",
            "missing_key_fallback":"event_id",
            "shard_count":3,
        }));
        return Json(json!({
            "resource_generation":"7",
            "tablet_epoch":"8",
            "term":"9",
            "accepts_writes":true,
            "stream_partitioning":partitioning,
        }))
        .into_response();
    }

    let document = if method == Method::GET && path.ends_with("/observations") {
        json!({"observation":{
            "revision":"11",
            "item":{
                "value":{"kind":"blob", "value":[0, 255]},
                "version":"3",
                "expires_at_ms":"60000",
            }
        }})
    } else if method == Method::POST && path.ends_with("/records/batches") {
        json!({"receipt":{"offset":"5"}})
    } else if method == Method::GET && path.ends_with("/records") {
        json!({"records":[{
            "offset":"5",
            "appended_at_ms":"1234",
            "envelope":{
                "key":STANDARD_NO_PAD.encode(b"key"),
                "payload":{
                    "value_base64":STANDARD_NO_PAD.encode(b"value"),
                    "headers":{"traceparent":STANDARD_NO_PAD.encode(b"00-test")},
                }
            }
        }]})
    } else if method == Method::GET && path.ends_with("/retention") {
        json!({"retention":{"end_offset":"6"}})
    } else if method == Method::PUT && path.ends_with("/offsets") {
        json!({"receipt":{"outcome":"applied", "committed_offset":"6"}})
    } else if method == Method::GET && path.ends_with("/lag") {
        json!({"checkpoint":{"exists":true, "committed_offset":"6"}})
    } else if method == Method::POST && path.ends_with("/mutations") {
        mutation_response(&body)
    } else {
        json!({})
    };
    Json(document).into_response()
}

fn mutation_response(body: &Value) -> Value {
    match body.pointer("/operation/kind").and_then(Value::as_str) {
        Some("increment") => {
            json!({"receipt":{"outcome":{"status":"applied", "result":{"value":"42"}}}})
        }
        Some("delete") => {
            json!({"receipt":{"outcome":{"status":"applied", "result":{"deleted":true}}}})
        }
        Some("acquire") => json!({"receipt":{"outcome":{"status":"applied", "result":{
            "deliveries":[{
                "message_id":"message-1",
                "lease_token":"lease-1",
                "attempt":"2",
                "metadata":{"correlation_id":"correlation-1", "reply_to":"replies"},
                "envelope":{
                    "headers":{"tenant":"acme"},
                    "payload":{
                        "body_base64":STANDARD_NO_PAD.encode(b"job"),
                        "content_type":"application/octet-stream",
                    }
                }
            }]
        }}}}),
        _ => json!({"receipt":{"outcome":{"status":"applied", "result":{}}}}),
    }
}

fn is_discovery(path: &str) -> bool {
    path.rsplit_once("/shards/")
        .is_some_and(|(_, suffix)| !suffix.is_empty() && !suffix.contains('/'))
}

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn backend(endpoint: Url) -> NativeHttpBackend {
    NativeHttpBackend::new(NativeHttpConfig {
        endpoints: vec![endpoint],
        token: "native-secret".into(),
        organization: "acme".into(),
        project: "shop".into(),
        environment: "dev".into(),
        namespace: "core".into(),
        timeout: Duration::from_secs(2),
    })
    .unwrap()
}

async fn prove_cache_port(backend: &NativeHttpBackend) {
    let cached = backend
        .cache_get("sessions", "profile")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cached.value, CacheValue::Blob(vec![0, 255]));
    assert_eq!(cached.version, 3);
    assert_eq!(
        backend
            .cache_increment("sessions", "visits", 2)
            .await
            .unwrap(),
        42
    );
    assert_eq!(
        backend
            .cache_delete("sessions", &["profile".into()])
            .await
            .unwrap(),
        1
    );
}

async fn prove_stream_port(backend: &NativeHttpBackend) {
    assert_eq!(backend.stream_partition_count("events").await.unwrap(), 3);
    let appended = backend
        .stream_append(
            "events",
            2,
            vec![StreamRecord {
                offset: 0,
                timestamp_ms: 1234,
                key: Some(b"key".to_vec()),
                value: Some(b"value".to_vec()),
                headers: vec![("traceparent".into(), Some(b"00-test".to_vec()))],
            }],
        )
        .await
        .unwrap();
    assert_eq!(appended, 5);
    let records = backend.stream_fetch("events", 2, 5, 10).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset, 5);
    assert_eq!(records[0].value.as_deref(), Some(b"value".as_slice()));
    assert_eq!(backend.stream_end_offset("events", 2).await.unwrap(), 6);
    backend
        .stream_commit_offset("billing", "events", 2, 6)
        .await
        .unwrap();
    assert_eq!(
        backend
            .stream_committed_offset("billing", "events", 2)
            .await
            .unwrap(),
        Some(6)
    );
}

async fn prove_queue_port(backend: &NativeHttpBackend) {
    assert!(backend.queue_exists("jobs").await.unwrap());
    assert!(!backend.queue_exists("missing").await.unwrap());
    backend
        .queue_publish(
            "jobs",
            QueueMessage {
                body: b"job".to_vec(),
                content_type: Some("application/octet-stream".into()),
                correlation_id: Some("correlation-1".into()),
                reply_to: Some("replies".into()),
                headers: BTreeMap::from([("tenant".into(), "acme".into())]),
            },
        )
        .await
        .unwrap();
    let deliveries = backend
        .queue_acquire("jobs", "worker", 1, 5_000)
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].message.body, b"job");
    assert!(deliveries[0].redelivered);
    backend
        .queue_ack("jobs", "worker", "lease-1")
        .await
        .unwrap();
    backend
        .queue_reject("jobs", "worker", "lease-2", true)
        .await
        .unwrap();
    backend
        .queue_reject("jobs", "worker", "lease-3", false)
        .await
        .unwrap();
}

fn assert_native_evidence(observed: &[ObservedRequest]) {
    assert!(observed.len() > 20);
    assert!(
        observed
            .iter()
            .all(|request| request.authorization.as_deref() == Some("Bearer native-secret"))
    );
    let operations = observed
        .iter()
        .filter(|request| !is_discovery(&request.path))
        .collect::<Vec<_>>();
    assert!(operations.iter().all(|request| {
        request.generation.as_deref() == Some("7") && request.tablet_epoch.as_deref() == Some("8")
    }));
    assert!(
        operations
            .iter()
            .filter(|request| request.method == Method::GET)
            .all(|request| request.consistency.as_deref() == Some("linearizable"))
    );
    let batch = operations.iter().find(|request| {
        request
            .path
            .ends_with("/streams/events/shards/2/records/batches")
            && request.body["partition"] == 0
            && request.body["expected_term"] == "9"
            && request.body["record_count"] == 1
            && request.body["compression"] == "gzip"
    });
    assert_native_batch_decodes(batch.copied().unwrap());
    assert!(operations.iter().any(|request| {
        request.path.ends_with("/groups/billing/lag")
            && request.query.as_deref() == Some("partition=0")
    }));
    assert!(operations.iter().any(|request| {
        request.body.pointer("/operation/kind") == Some(&Value::String("enqueue".into()))
            && request.body.pointer("/operation/correlation_id")
                == Some(&Value::String("correlation-1".into()))
    }));
}

fn assert_native_batch_decodes(request: &ObservedRequest) {
    let payload = StreamBatchPayload {
        compression: StreamCompression::Gzip,
        record_count: u16::try_from(request.body["record_count"].as_u64().unwrap()).unwrap(),
        uncompressed_bytes: u32::try_from(request.body["uncompressed_bytes"].as_u64().unwrap())
            .unwrap(),
        compressed_bytes: u32::try_from(request.body["compressed_bytes"].as_u64().unwrap())
            .unwrap(),
        payload_base64: request.body["payload_base64"].as_str().unwrap().into(),
    };
    let decoded = decode_stream_batch_payload(&payload).unwrap();
    assert_eq!(decoded.len(), 1);
    let record = &decoded[0];
    assert_eq!(record.client_sequence, 0);
    assert_eq!(
        record.envelope.key.as_deref(),
        Some(STANDARD_NO_PAD.encode(b"key").as_str())
    );
    assert_eq!(
        STANDARD_NO_PAD
            .decode(record.envelope.payload["value_base64"].as_str().unwrap())
            .unwrap(),
        b"value"
    );
    assert_eq!(
        STANDARD.decode(&payload.payload_base64).unwrap().len(),
        usize::try_from(payload.compressed_bytes).unwrap()
    );
}

#[tokio::test]
async fn translates_cache_stream_and_queue_ports_to_authenticated_fenced_native_requests() {
    let api = MockNativeApi::start().await;
    let backend = backend(api.endpoint.clone());
    prove_cache_port(&backend).await;
    prove_stream_port(&backend).await;
    prove_queue_port(&backend).await;
    assert_native_evidence(&api.observed.lock().unwrap());
}

#[tokio::test]
async fn validates_scope_and_maps_native_errors_without_leaking_backend_messages() {
    let secret_endpoint = NativeHttpConfig {
        endpoints: vec![
            Url::parse("http://alice:url-secret@127.0.0.1:1/?token=query-secret").unwrap(),
        ],
        token: "native-super-secret".into(),
        organization: "acme".into(),
        project: "shop".into(),
        environment: "dev".into(),
        namespace: "core".into(),
        timeout: Duration::from_secs(1),
    };
    let debug = format!("{secret_endpoint:?}");
    assert!(!debug.contains("native-super-secret"));
    assert!(!debug.contains("url-secret"));
    assert!(!debug.contains("query-secret"));
    assert!(debug.contains("<redacted>"));
    assert!(matches!(
        NativeHttpBackend::new(secret_endpoint),
        Err(BackendError::Invalid(_))
    ));
    assert!(matches!(
        NativeHttpBackend::new(NativeHttpConfig {
            endpoints: vec![Url::parse("http://127.0.0.1:1").unwrap()],
            token: "token".into(),
            organization: "../escape".into(),
            project: "shop".into(),
            environment: "dev".into(),
            namespace: "core".into(),
            timeout: Duration::from_secs(1),
        }),
        Err(BackendError::Invalid(_))
    ));

    let api = MockNativeApi::start().await;
    let traversal = backend(api.endpoint.clone())
        .queue_exists("..")
        .await
        .unwrap_err();
    assert!(matches!(traversal, BackendError::Invalid(_)));
    assert!(api.observed.lock().unwrap().is_empty());

    let error = backend(api.endpoint.clone())
        .queue_exists("missing")
        .await
        .unwrap();
    assert!(!error);
    let oversized = backend(api.endpoint.clone())
        .queue_exists("oversized")
        .await
        .unwrap_err();
    assert!(
        matches!(&oversized, BackendError::Unavailable(message) if message == "response exceeds limit"),
        "unexpected bounded-response error: {oversized:?}"
    );
    let observed = api.observed.lock().unwrap();
    assert_eq!(observed.len(), 2);
}

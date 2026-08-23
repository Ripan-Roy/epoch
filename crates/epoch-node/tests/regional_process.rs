use std::{
    collections::HashSet,
    fs::{File, read_to_string},
    net::{SocketAddr, TcpListener as StdTcpListener},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::State,
    http::HeaderMap,
    routing::{get, post},
};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::sync::oneshot;

const NODE_COUNT: usize = 3;
const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const ADMIN_TOKEN: &str = "epoch-dev-admin-v1";

struct NodeProcess {
    node_id: u64,
    http_address: SocketAddr,
    peer_address: SocketAddr,
    data_dir: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    webhook_signing_keys_path: PathBuf,
    child: Option<Child>,
}

impl NodeProcess {
    fn start(&mut self, peer_spec: &str) {
        let stdout = File::create(&self.stdout_path).expect("stdout log should open");
        let stderr = File::create(&self.stderr_path).expect("stderr log should open");
        let child = Command::new(env!("CARGO_BIN_EXE_epoch-node"))
            .args([
                "--http-listen",
                &self.http_address.to_string(),
                "--data-dir",
                self.data_dir.to_str().expect("data path should be UTF-8"),
                "--regional-runtime-enabled",
                "--consensus-node-id",
                &self.node_id.to_string(),
                "--consensus-group-id",
                "1",
                "--consensus-group-epoch",
                "1",
                "--consensus-listen",
                &self.peer_address.to_string(),
                "--consensus-peers",
                peer_spec,
                "--consensus-tick-ms",
                "20",
                "--regional-max-groups",
                "16",
                "--regional-epoch-target-delivery-interval-ms",
                "20",
                "--regional-managed-target-delivery-interval-ms",
                "20",
                "--regional-managed-target-allow-http-loopback",
                "--regional-source-connector-interval-ms",
                "20",
                "--regional-webhook-signing-keys-path",
                self.webhook_signing_keys_path
                    .to_str()
                    .expect("webhook key path should be UTF-8"),
                "--regional-webhook-delivery-interval-ms",
                "20",
                "--regional-webhook-allow-http-loopback",
                "--log",
                "warn",
            ])
            .env(
                "EPOCH_AUTH_POLICY_PATH",
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../spec/auth/bootstrap-policy-v1.example.json"),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("epoch-node should spawn");
        self.child = Some(child);
    }

    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn diagnostics(&self) -> String {
        format!(
            "node {} stdout:\n{}\nnode {} stderr:\n{}",
            self.node_id,
            read_to_string(&self.stdout_path).unwrap_or_default(),
            self.node_id,
            read_to_string(&self.stderr_path).unwrap_or_default()
        )
    }
}

struct ProcessCluster {
    _root: TempDir,
    peer_spec: String,
    nodes: Vec<NodeProcess>,
}

impl ProcessCluster {
    fn start() -> Self {
        let root = TempDir::new().expect("temp directory should be created");
        let webhook_signing_keys_path = root.path().join("webhook-signing-keys.json");
        std::fs::write(
            &webhook_signing_keys_path,
            r#"{"format_version":1,"keys":[{"id":"primary","secret":"0123456789abcdef0123456789abcdef"}]}"#,
        )
        .expect("webhook signing keys should be written");
        let (http_addresses, peer_addresses) = reserve_cluster_addresses();
        let peer_spec = peer_addresses
            .iter()
            .enumerate()
            .map(|(index, address)| format!("{}=http://{address}/", index + 1))
            .collect::<Vec<_>>()
            .join(",");
        let mut nodes = http_addresses
            .into_iter()
            .zip(peer_addresses)
            .enumerate()
            .map(|(index, (http_address, peer_address))| {
                let node_id = u64::try_from(index + 1).expect("node ID fits");
                let data_dir = root.path().join(format!("node-{node_id}"));
                NodeProcess {
                    node_id,
                    http_address,
                    peer_address,
                    stdout_path: root.path().join(format!("node-{node_id}.stdout.log")),
                    stderr_path: root.path().join(format!("node-{node_id}.stderr.log")),
                    webhook_signing_keys_path: webhook_signing_keys_path.clone(),
                    data_dir,
                    child: None,
                }
            })
            .collect::<Vec<_>>();
        for node in &mut nodes {
            node.start(&peer_spec);
        }
        Self {
            _root: root,
            peer_spec,
            nodes,
        }
    }

    fn restart(&mut self, index: usize) {
        self.nodes[index].start(&self.peer_spec);
    }

    fn stop_all(&mut self) {
        for node in &mut self.nodes {
            node.kill();
        }
    }

    fn restart_all(&mut self) {
        for node in &mut self.nodes {
            node.start(&self.peer_spec);
        }
    }

    fn diagnostics(&self) -> String {
        self.nodes
            .iter()
            .map(NodeProcess::diagnostics)
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl Drop for ProcessCluster {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            node.kill();
        }
    }
}

fn reserve_cluster_addresses() -> (Vec<SocketAddr>, Vec<SocketAddr>) {
    // Keep every reservation open until both address sets are known. Reserving
    // the HTTP and peer sets separately allows Linux to immediately recycle an
    // HTTP port into the peer set, making two listeners in the same cluster
    // compete for one address.
    let listeners = (0..NODE_COUNT * 2)
        .map(|_| StdTcpListener::bind("127.0.0.1:0").expect("port should reserve"))
        .collect::<Vec<_>>();
    let mut addresses = listeners
        .iter()
        .map(|listener| listener.local_addr().expect("listener should have address"))
        .collect::<Vec<_>>();
    let peer_addresses = addresses.split_off(NODE_COUNT);
    drop(listeners);
    (addresses, peer_addresses)
}

#[test]
fn cluster_address_reservations_do_not_overlap() {
    for _ in 0..32 {
        let (http_addresses, peer_addresses) = reserve_cluster_addresses();
        let unique = http_addresses
            .iter()
            .chain(&peer_addresses)
            .copied()
            .collect::<HashSet<_>>();
        assert_eq!(unique.len(), NODE_COUNT * 2);
    }
}

fn route_url(node: &NodeProcess, kind: &str, name: &str) -> String {
    format!(
        "http://{}/experimental/v1/regional/resources/acme/shop/dev/core/{kind}/{name}/shards/0",
        node.http_address,
    )
}

fn catalog_url(node: &NodeProcess) -> String {
    format!(
        "http://{}/experimental/v1/regional/catalog",
        node.http_address
    )
}

fn catalog_resource_url(node: &NodeProcess, kind: &str, name: &str) -> String {
    format!(
        "{}/resources/acme/shop/dev/core/{kind}/{name}",
        catalog_url(node),
    )
}

fn records_url(node: &NodeProcess) -> String {
    format!("{}/data/records", route_url(node, "stream", "orders"))
}

fn data_url(node: &NodeProcess, kind: &str, name: &str, operation: &str) -> String {
    format!("{}/data/{operation}", route_url(node, kind, name))
}

async fn wait_for_nodes(client: &Client, cluster: &ProcessCluster, indexes: &[usize]) {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let mut ready = true;
            for &index in indexes {
                let url = format!("http://{}/healthz", cluster.nodes[index].http_address);
                let Ok(response) = client.get(url).send().await else {
                    ready = false;
                    continue;
                };
                if response.status() != StatusCode::OK {
                    ready = false;
                }
            }
            if ready {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("nodes did not become ready\n{}", cluster.diagnostics()));
}

async fn create_resource(client: &Client, cluster: &ProcessCluster, kind: &str, name: &str) {
    let request_token = format!("process-create-{kind}-{name}-v1");
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            for node in &cluster.nodes {
                let response = client
                    .put(catalog_resource_url(node, kind, name))
                    .bearer_auth(ADMIN_TOKEN)
                    .json(&json!({
                        "request_token": request_token,
                        "expected_generation": "0",
                        "shard_count": 1,
                        "replica_count": 3
                    }))
                    .send()
                    .await;
                if response.is_ok_and(|response| response.status() == StatusCode::CREATED) {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "catalog create for {kind}/{name} did not commit\n{}",
            cluster.diagnostics()
        )
    });
}

async fn wait_for_routes(
    client: &Client,
    cluster: &ProcessCluster,
    kind: &str,
    name: &str,
    indexes: &[usize],
) -> Vec<Value> {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let mut routes = Vec::new();
            for &index in indexes {
                let response = client
                    .get(route_url(&cluster.nodes[index], kind, name))
                    .bearer_auth(ADMIN_TOKEN)
                    .send()
                    .await;
                let Ok(response) = response else {
                    routes.clear();
                    break;
                };
                if response.status() != StatusCode::OK {
                    routes.clear();
                    break;
                }
                routes.push(
                    response
                        .json::<Value>()
                        .await
                        .expect("route should be JSON"),
                );
            }
            let leader_count = routes
                .iter()
                .filter(|route| route["accepts_writes"] == true)
                .count();
            if routes.len() == indexes.len() && leader_count == 1 {
                return routes;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "routes for {kind}/{name} did not converge\n{}",
            cluster.diagnostics()
        )
    })
}

fn writable_route(routes: &[Value], indexes: &[usize]) -> (usize, u64) {
    let leaders = routes
        .iter()
        .zip(indexes)
        .filter(|(route, _)| route["accepts_writes"] == true)
        .map(|(route, index)| {
            (
                *index,
                route["term"]
                    .as_str()
                    .expect("term should be exact")
                    .parse::<u64>()
                    .expect("term should parse"),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(leaders.len(), 1, "exactly one data leader is required");
    leaders[0]
}

async fn append_record(client: &Client, node: &NodeProcess, term: u64, id: u64) {
    let response = client
        .post(records_url(node))
        .bearer_auth(ADMIN_TOKEN)
        .header("x-epoch-resource-generation", "1")
        .header("x-epoch-tablet-epoch", "1")
        .json(&json!({
            "idempotency_key": format!("process-order-{id}"),
            "expected_term": term.to_string(),
            "partition": 0,
            "envelope": {
                "id": format!("order-{id}"),
                "source": "regional-process-test",
                "type": "order.created",
                "time_ms": id.to_string(),
                "payload": {"id": id}
            }
        }))
        .send()
        .await
        .expect("append should receive a response");
    assert!(
        response.status().is_success(),
        "append failed with {}: {}",
        response.status(),
        response.text().await.unwrap_or_default()
    );
}

fn profile_write(kind: &str, term: u64) -> (&'static str, Value) {
    let term = term.to_string();
    match kind {
        "cache" => (
            "mutations",
            json!({
                "idempotency_key": "process-cache-set-1",
                "expected_term": term,
                "operation": {
                    "kind": "set",
                    "key": "session-1",
                    "value": {"kind": "string", "value": "ready"}
                }
            }),
        ),
        "queue" => (
            "mutations",
            json!({
                "idempotency_key": "process-queue-enqueue-1",
                "expected_term": term,
                "operation": {
                    "kind": "enqueue",
                    "envelope": {
                        "id": "job-1",
                        "source": "regional-process-test",
                        "type": "job.created",
                        "time_ms": "1",
                        "payload": {"id": 1}
                    }
                }
            }),
        ),
        "event-bus" => (
            "mutations",
            json!({
                "idempotency_key": "process-bus-publish-1",
                "expected_term": term,
                "operation": {
                    "kind": "publish",
                    "envelope": {
                        "id": "event-1",
                        "source": "regional-process-test",
                        "type": "order.created",
                        "time_ms": "1",
                        "payload": {"id": 1}
                    }
                }
            }),
        ),
        _ => panic!("unsupported process-test profile kind {kind}"),
    }
}

fn is_retryable_leadership_response(status: StatusCode, document: &Value) -> bool {
    let code = document["code"]
        .as_str()
        .or_else(|| document["error"]["code"].as_str());
    matches!(
        status,
        StatusCode::CONFLICT | StatusCode::SERVICE_UNAVAILABLE
    ) && matches!(code, Some("not_leader" | "stale_term"))
}

async fn write_profile(
    client: &Client,
    cluster: &ProcessCluster,
    kind: &str,
    name: &str,
    indexes: &[usize],
) {
    let result = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let routes = wait_for_routes(client, cluster, kind, name, indexes).await;
            let (leader, term) = writable_route(&routes, indexes);
            let (operation, body) = profile_write(kind, term);
            let response = client
                .post(data_url(&cluster.nodes[leader], kind, name, operation))
                .bearer_auth(ADMIN_TOKEN)
                .header("x-epoch-resource-generation", "1")
                .header("x-epoch-tablet-epoch", "1")
                .json(&body)
                .send()
                .await
                .expect("profile write should receive a response");
            let status = response.status();
            if status.is_success() {
                return Ok(());
            }
            let encoded = response.text().await.unwrap_or_default();
            let document = serde_json::from_str(&encoded).unwrap_or(Value::Null);
            if !is_retryable_leadership_response(status, &document) {
                return Err(format!(
                    "{kind}/{name} write failed with {status}: {encoded}"
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("{error}\n{}", cluster.diagnostics()),
        Err(error) => panic!(
            "{kind}/{name} write did not survive a leadership transition: {error}\n{}",
            cluster.diagnostics()
        ),
    }
}

async fn write_bus_mutation(
    client: &Client,
    cluster: &ProcessCluster,
    indexes: &[usize],
    idempotency_key: &str,
    operation: &Value,
) -> Value {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let routes = wait_for_routes(client, cluster, "event-bus", "events", indexes).await;
            let (leader, term) = writable_route(&routes, indexes);
            let response = client
                .post(data_url(
                    &cluster.nodes[leader],
                    "event-bus",
                    "events",
                    "mutations",
                ))
                .bearer_auth(ADMIN_TOKEN)
                .header("x-epoch-resource-generation", "1")
                .header("x-epoch-tablet-epoch", "1")
                .json(&json!({
                    "idempotency_key": idempotency_key,
                    "expected_term": term.to_string(),
                    "operation": operation,
                }))
                .send()
                .await
                .expect("Event Bus mutation should receive a response");
            let status = response.status();
            let document = response
                .json::<Value>()
                .await
                .expect("Event Bus mutation response should be JSON");
            if status.is_success() {
                return document;
            }
            assert!(
                is_retryable_leadership_response(status, &document),
                "Event Bus mutation failed with {status}: {document}\n{}",
                cluster.diagnostics()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "Event Bus mutation {idempotency_key} did not commit\n{}",
            cluster.diagnostics()
        )
    })
}

async fn prove_epoch_queue_and_stream_targets(
    client: &Client,
    cluster: &ProcessCluster,
    indexes: &[usize],
) {
    for (subscription, target_kind, resource) in [
        ("queue-jobs", "queue", "jobs"),
        ("stream-orders", "stream", "orders"),
    ] {
        write_bus_mutation(
            client,
            cluster,
            indexes,
            &format!("process-bus-upsert-{subscription}-v1"),
            &json!({
                "kind": "upsert_subscription",
                "subscription": {
                    "name": subscription,
                    "filter": {"event_type_patterns": ["target.*"]},
                    "target": {"kind": target_kind, "resource": resource},
                    "delivery_policy": {
                        "timeout_ms": 1000,
                        "max_in_flight": 1,
                        "retry": {
                            "strategy": "fixed",
                            "initial_delay_ms": 0,
                            "max_delay_ms": 0,
                            "jitter_percent": 0,
                            "max_attempts": 3,
                            "max_age_ms": null
                        }
                    }
                }
            }),
        )
        .await;
    }
    write_bus_mutation(
        client,
        cluster,
        indexes,
        "process-bus-publish-epoch-targets-v1",
        &json!({
            "kind": "publish",
            "envelope": {
                "id": "epoch-target-event-1",
                "source": "regional-process-test",
                "type": "target.created",
                "time_ms": "2",
                "key": "customer-42",
                "payload": {"id": 2}
            }
        }),
    )
    .await;

    wait_for_profile_apply(client, cluster, "queue", "jobs", indexes, 2).await;
    wait_for_profile_apply(client, cluster, "stream", "orders", indexes, 3).await;
    wait_for_record_count(client, cluster, indexes, 3).await;
    wait_for_profile_apply(client, cluster, "event-bus", "events", indexes, 8).await;
    wait_for_acknowledged_epoch_targets(client, cluster, indexes).await;
}

async fn wait_for_acknowledged_epoch_targets(
    client: &Client,
    cluster: &ProcessCluster,
    indexes: &[usize],
) {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let mut converged = true;
            for &index in indexes {
                for (subscription, kind, resource) in [
                    ("queue-jobs", "queue", "jobs"),
                    ("stream-orders", "stream", "orders"),
                ] {
                    let response = client
                        .post(data_url(
                            &cluster.nodes[index],
                            "event-bus",
                            "events",
                            "deliveries/query",
                        ))
                        .bearer_auth(ADMIN_TOKEN)
                        .header("x-epoch-resource-generation", "1")
                        .header("x-epoch-tablet-epoch", "1")
                        .header("x-epoch-read-consistency", "local_stale")
                        .json(&json!({
                            "subscription": subscription,
                            "state": "acknowledged",
                            "limit": 10
                        }))
                        .send()
                        .await;
                    let Ok(response) = response else {
                        converged = false;
                        break;
                    };
                    if response.status() != StatusCode::OK {
                        converged = false;
                        break;
                    }
                    let body = response
                        .json::<Value>()
                        .await
                        .expect("delivery query should be JSON");
                    let Some(record) = body["records"]
                        .as_array()
                        .and_then(|records| (records.len() == 1).then(|| &records[0]))
                    else {
                        converged = false;
                        break;
                    };
                    if record["envelope"]["id"] != "epoch-target-event-1"
                        || record["state"]["kind"] != "acknowledged"
                        || record["target"]["kind"] != kind
                        || record["destination"]["kind"] != kind
                        || record["destination"]["resource"] != resource
                        || record["destination"]["resource_generation"] != "1"
                        || record["destination"]["tablet_epoch"] != "1"
                        || record["attempts"].as_array().map(Vec::len) != Some(1)
                    {
                        converged = false;
                        break;
                    }
                }
                if !converged {
                    break;
                }
            }
            if converged {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "Epoch Queue/Stream target deliveries did not converge\n{}",
            cluster.diagnostics()
        )
    });
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CapturedWebhook {
    body: Vec<u8>,
    delivery_id: String,
    attempt: String,
    signature: String,
    key_id: String,
    cloud_event_id: String,
}

type CapturedWebhooks = Arc<Mutex<Vec<CapturedWebhook>>>;

struct TestWebhookReceiver {
    address: SocketAddr,
    captured: CapturedWebhooks,
    shutdown: oneshot::Sender<()>,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl TestWebhookReceiver {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("webhook receiver should bind");
        let address = listener
            .local_addr()
            .expect("webhook receiver address should exist");
        let captured = Arc::new(Mutex::new(Vec::new()));
        let receiver = Router::new()
            .route("/orders", post(capture_webhook))
            .with_state(Arc::clone(&captured));
        let (shutdown, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, receiver)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        Self {
            address,
            captured,
            shutdown,
            server,
        }
    }

    async fn attempts(&self) -> Result<Vec<CapturedWebhook>, tokio::time::error::Elapsed> {
        tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                let attempts = self
                    .captured
                    .lock()
                    .expect("webhook capture lock should hold")
                    .clone();
                if attempts.len() >= 2 {
                    return attempts;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
    }

    async fn stop(self) {
        let _ = self.shutdown.send(());
        self.server
            .await
            .expect("webhook receiver task should join")
            .expect("webhook receiver should stop cleanly");
    }
}

async fn capture_webhook(
    State(captured): State<CapturedWebhooks>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    };
    let mut captured = captured.lock().expect("webhook capture lock should hold");
    captured.push(CapturedWebhook {
        body: body.to_vec(),
        delivery_id: header("epoch-delivery-id"),
        attempt: header("epoch-delivery-attempt"),
        signature: header("epoch-signature"),
        key_id: header("epoch-signature-key-id"),
        cloud_event_id: header("ce-id"),
    });
    if captured.len() == 1 {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::NO_CONTENT
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CapturedManagedTarget {
    body: Value,
    delivery_id: String,
    idempotency_key: String,
    content_type: String,
}

type CapturedManagedTargets = Arc<Mutex<Vec<CapturedManagedTarget>>>;

struct TestManagedTargetReceiver {
    address: SocketAddr,
    captured: CapturedManagedTargets,
    shutdown: oneshot::Sender<()>,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl TestManagedTargetReceiver {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("managed-target receiver should bind");
        let address = listener
            .local_addr()
            .expect("managed-target receiver address should exist");
        let captured = Arc::new(Mutex::new(Vec::new()));
        let receiver = Router::new()
            .route("/orders", post(capture_managed_target))
            .with_state(Arc::clone(&captured));
        let (shutdown, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, receiver)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        Self {
            address,
            captured,
            shutdown,
            server,
        }
    }

    async fn delivery(&self) -> Result<CapturedManagedTarget, tokio::time::error::Elapsed> {
        tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                if let Some(delivery) = self
                    .captured
                    .lock()
                    .expect("managed-target capture lock should hold")
                    .first()
                    .cloned()
                {
                    return delivery;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
    }

    async fn stop(self) {
        let _ = self.shutdown.send(());
        self.server
            .await
            .expect("managed-target receiver task should join")
            .expect("managed-target receiver should stop cleanly");
    }
}

async fn capture_managed_target(
    State(captured): State<CapturedManagedTargets>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    };
    let body = serde_json::from_slice(&body).expect("managed target should receive JSON");
    captured
        .lock()
        .expect("managed-target capture lock should hold")
        .push(CapturedManagedTarget {
            body,
            delivery_id: header("epoch-delivery-id"),
            idempotency_key: header("idempotency-key"),
            content_type: header("content-type"),
        });
    StatusCode::NO_CONTENT
}

#[derive(Default)]
struct SourceConnectorState {
    positions: Mutex<Vec<String>>,
}

struct TestSourceConnector {
    address: SocketAddr,
    state: Arc<SourceConnectorState>,
    shutdown: oneshot::Sender<()>,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl TestSourceConnector {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("source connector should bind");
        let address = listener
            .local_addr()
            .expect("source connector address should exist");
        let state = Arc::new(SourceConnectorState::default());
        let router = Router::new()
            .route("/events", get(poll_source_connector))
            .with_state(Arc::clone(&state));
        let (shutdown, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        Self {
            address,
            state,
            shutdown,
            server,
        }
    }

    async fn stop(self) {
        let _ = self.shutdown.send(());
        self.server
            .await
            .expect("source connector task should join")
            .expect("source connector should stop cleanly");
    }
}

async fn poll_source_connector(
    State(state): State<Arc<SourceConnectorState>>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    let position = headers
        .get("epoch-connector-position")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    state
        .positions
        .lock()
        .expect("source positions lock should hold")
        .push(position.clone());
    if position != "cursor-10" {
        return (StatusCode::NO_CONTENT, Json(Value::Null));
    }
    (
        StatusCode::OK,
        Json(json!({
            "batch_id": "process-source-batch-11",
            "source_from": "cursor-10",
            "source_to": "cursor-11",
            "events": [{
                "id": "source-event-11",
                "source": "urn:process-source",
                "type": "source.imported",
                "time_ms": 11,
                "payload": {"order_id": 11}
            }]
        })),
    )
}

async fn prove_source_connector_ingestion(
    client: &Client,
    cluster: &ProcessCluster,
    indexes: &[usize],
) {
    let source = TestSourceConnector::start().await;
    write_bus_mutation(
        client,
        cluster,
        indexes,
        "process-source-connector-v1",
        &json!({
            "kind": "apply_integration",
            "operation": {
                "kind": "upsert_connector",
                "spec": {
                    "name": "orders-source",
                    "kind": "http",
                    "direction": "source",
                    "secret_refs": [],
                    "outbound_allowlist": ["127.0.0.1"],
                    "identity": "orders-source-reader",
                    "config": {
                        "source_url": format!("http://{}/events", source.address),
                        "start_position": "cursor-10",
                        "poll_timeout_ms": "1000"
                    }
                }
            }
        }),
    )
    .await;
    wait_for_source_checkpoint(client, cluster, indexes).await;
    let positions = source
        .state
        .positions
        .lock()
        .expect("source positions lock should hold")
        .clone();
    assert!(positions.iter().any(|position| position == "cursor-10"));
    assert!(positions.iter().any(|position| position == "cursor-11"));
    wait_for_profile_apply(client, cluster, "event-bus", "events", indexes, 21).await;
    source.stop().await;
}

async fn wait_for_source_checkpoint(client: &Client, cluster: &ProcessCluster, indexes: &[usize]) {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let mut converged = true;
            for &index in indexes {
                let response = client
                    .get(data_url(
                        &cluster.nodes[index],
                        "event-bus",
                        "events",
                        "integration/state",
                    ))
                    .bearer_auth(ADMIN_TOKEN)
                    .header("x-epoch-resource-generation", "1")
                    .header("x-epoch-tablet-epoch", "1")
                    .header("x-epoch-read-consistency", "local_stale")
                    .send()
                    .await;
                let Ok(response) = response else {
                    converged = false;
                    break;
                };
                if response.status() != StatusCode::OK {
                    converged = false;
                    break;
                }
                let body = response.json::<Value>().await.expect("state should be JSON");
                if body["state"]["connectors"]["connectors"]["orders-source"]["checkpoint"]
                    ["source_position"]
                    != "cursor-11"
                {
                    converged = false;
                    break;
                }
            }
            if converged {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "source connector checkpoint did not converge\n{}",
            cluster.diagnostics()
        )
    });
}

async fn prove_managed_api_destination(
    client: &Client,
    cluster: &ProcessCluster,
    indexes: &[usize],
) {
    let receiver = TestManagedTargetReceiver::start().await;
    write_bus_mutation(
        client,
        cluster,
        indexes,
        "process-bus-upsert-api-destination-v1",
        &json!({
            "kind": "upsert_subscription",
            "subscription": {
                "name": "api-orders",
                "filter": {"event_type_patterns": ["invoice.*"]},
                "target": {
                    "kind": "api_destination",
                    "url": format!("http://{}/orders", receiver.address),
                    "auth": {"kind": "none"},
                    "cloud_events_mode": "structured"
                },
                "delivery_policy": {
                    "timeout_ms": 1000,
                    "max_in_flight": 1,
                    "retry": {
                        "strategy": "fixed",
                        "initial_delay_ms": 0,
                        "max_delay_ms": 0,
                        "jitter_percent": 0,
                        "max_attempts": 2,
                        "max_age_ms": null
                    }
                }
            }
        }),
    )
    .await;
    write_bus_mutation(
        client,
        cluster,
        indexes,
        "process-bus-publish-api-destination-v1",
        &json!({
            "kind": "publish",
            "envelope": {
                "id": "api-destination-event-1",
                "source": "regional-process-test",
                "type": "invoice.created",
                "time_ms": "3",
                "payload": {"invoice_id": 3}
            }
        }),
    )
    .await;
    let delivery = receiver.delivery().await.unwrap_or_else(|_| {
        panic!(
            "managed API destination was not delivered\n{}",
            cluster.diagnostics()
        )
    });
    assert_eq!(delivery.body["specversion"], "1.0");
    assert_eq!(delivery.body["id"], "api-destination-event-1");
    assert_eq!(delivery.body["data"]["invoice_id"], 3);
    assert!(!delivery.delivery_id.is_empty());
    assert!(delivery.idempotency_key.starts_with("epoch-"));
    assert_eq!(delivery.content_type, "application/cloudevents+json");
    wait_for_profile_apply(client, cluster, "event-bus", "events", indexes, 18).await;
    receiver.stop().await;
}

async fn webhook_diagnostics(client: &Client, cluster: &ProcessCluster) -> String {
    let mut output = Vec::new();
    for node in &cluster.nodes {
        let topology = match client
            .get(format!(
                "http://{}/experimental/v1/regional/topology",
                node.http_address
            ))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
        {
            Ok(response) => format!(
                "{} {}",
                response.status(),
                response.text().await.unwrap_or_default()
            ),
            Err(error) => error.to_string(),
        };
        let deliveries = match client
            .post(data_url(node, "event-bus", "events", "deliveries/query"))
            .bearer_auth(ADMIN_TOKEN)
            .header("x-epoch-resource-generation", "1")
            .header("x-epoch-tablet-epoch", "1")
            .header("x-epoch-read-consistency", "local_stale")
            .json(&json!({"subscription": "webhook-orders", "limit": 10}))
            .send()
            .await
        {
            Ok(response) => format!(
                "{} {}",
                response.status(),
                response.text().await.unwrap_or_default()
            ),
            Err(error) => error.to_string(),
        };
        output.push(format!(
            "node {} topology: {topology}\nnode {} deliveries: {deliveries}",
            node.node_id, node.node_id
        ));
    }
    output.join("\n")
}

async fn prove_signed_webhook_retry(client: &Client, cluster: &ProcessCluster, indexes: &[usize]) {
    let receiver = TestWebhookReceiver::start().await;

    write_bus_mutation(
        client,
        cluster,
        indexes,
        "process-bus-upsert-webhook-v1",
        &json!({
            "kind": "upsert_subscription",
            "subscription": {
                "name": "webhook-orders",
                "filter": {"event_type_patterns": ["order.*"]},
                "target": {
                    "kind": "webhook",
                    "url": format!("http://{}/orders", receiver.address),
                    "signing_key_id": "primary"
                },
                "delivery_policy": {
                    "timeout_ms": 1000,
                    "max_in_flight": 1,
                    "retry": {
                        "strategy": "fixed",
                        "initial_delay_ms": 0,
                        "max_delay_ms": 0,
                        "jitter_percent": 0,
                        "max_attempts": 2,
                        "max_age_ms": null
                    }
                }
            }
        }),
    )
    .await;
    write_bus_mutation(
        client,
        cluster,
        indexes,
        "process-bus-publish-webhook-v1",
        &json!({
            "kind": "publish",
            "envelope": {
                "id": "webhook-event-1",
                "source": "regional-process-test",
                "type": "order.created",
                "time_ms": "2",
                "payload": {"id": 2}
            }
        }),
    )
    .await;

    let Ok(attempts) = receiver.attempts().await else {
        let webhook_diagnostics = webhook_diagnostics(client, cluster).await;
        panic!(
            "signed webhook retry was not delivered\n{webhook_diagnostics}\n{}",
            cluster.diagnostics()
        );
    };
    assert_eq!(
        attempts.len(),
        2,
        "unexpected webhook attempts: {attempts:?}"
    );
    assert_eq!(attempts[0].body, br#"{"id":2}"#);
    assert_eq!(attempts[0].delivery_id, attempts[1].delivery_id);
    assert_eq!(attempts[0].attempt, "1");
    assert_eq!(attempts[1].attempt, "2");
    assert_eq!(attempts[0].key_id, "primary");
    assert_eq!(attempts[0].cloud_event_id, "webhook-event-1");
    assert!(attempts[0].signature.starts_with("v1="));
    assert_ne!(attempts[0].signature, attempts[1].signature);

    wait_for_profile_apply(client, cluster, "event-bus", "events", indexes, 14).await;
    receiver.stop().await;
}

#[test]
fn retry_classifier_accepts_only_explicit_leadership_transitions() {
    assert!(is_retryable_leadership_response(
        StatusCode::CONFLICT,
        &json!({"code": "not_leader"})
    ));
    assert!(is_retryable_leadership_response(
        StatusCode::SERVICE_UNAVAILABLE,
        &json!({"error": {"code": "not_leader"}})
    ));
    assert!(is_retryable_leadership_response(
        StatusCode::CONFLICT,
        &json!({"error": {"code": "stale_term"}})
    ));
    assert!(!is_retryable_leadership_response(
        StatusCode::CONFLICT,
        &json!({"code": "catalog_conflict"})
    ));
    assert!(!is_retryable_leadership_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        &json!({"error": {"code": "not_leader"}})
    ));
}

fn exact_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

async fn wait_for_profile_apply(
    client: &Client,
    cluster: &ProcessCluster,
    kind: &str,
    name: &str,
    indexes: &[usize],
    expected: u64,
) {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let mut digests = Vec::new();
            for &index in indexes {
                let response = client
                    .get(data_url(&cluster.nodes[index], kind, name, "status"))
                    .bearer_auth(ADMIN_TOKEN)
                    .header("x-epoch-resource-generation", "1")
                    .header("x-epoch-tablet-epoch", "1")
                    .header("x-epoch-read-consistency", "local_stale")
                    .send()
                    .await;
                let Ok(response) = response else {
                    digests.clear();
                    break;
                };
                if response.status() != StatusCode::OK {
                    digests.clear();
                    break;
                }
                let body = response
                    .json::<Value>()
                    .await
                    .expect("status should be JSON");
                if exact_u64(&body["applied_command_count"]) != Some(expected) {
                    digests.clear();
                    break;
                }
                digests.push(body["state_digest"].clone());
            }
            if digests.len() == indexes.len()
                && digests.windows(2).all(|pair| pair.first() == pair.get(1))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "{kind}/{name} did not converge to {expected} applied commands\n{}",
            cluster.diagnostics()
        )
    });
}

async fn wait_for_acknowledged_webhook(
    client: &Client,
    cluster: &ProcessCluster,
    indexes: &[usize],
) {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let mut converged = true;
            for &index in indexes {
                let response = client
                    .post(data_url(
                        &cluster.nodes[index],
                        "event-bus",
                        "events",
                        "deliveries/query",
                    ))
                    .bearer_auth(ADMIN_TOKEN)
                    .header("x-epoch-resource-generation", "1")
                    .header("x-epoch-tablet-epoch", "1")
                    .header("x-epoch-read-consistency", "local_stale")
                    .json(&json!({
                        "subscription": "webhook-orders",
                        "state": "acknowledged",
                        "limit": 10
                    }))
                    .send()
                    .await;
                let Ok(response) = response else {
                    converged = false;
                    break;
                };
                if response.status() != StatusCode::OK {
                    converged = false;
                    break;
                }
                let body = response
                    .json::<Value>()
                    .await
                    .expect("delivery query should be JSON");
                let Some(record) = body["records"]
                    .as_array()
                    .and_then(|records| (records.len() == 1).then(|| &records[0]))
                else {
                    converged = false;
                    break;
                };
                let attempts = record["attempts"].as_array();
                if record["envelope"]["id"] != "webhook-event-1"
                    || record["state"]["kind"] != "acknowledged"
                    || attempts.map(Vec::len) != Some(2)
                    || attempts
                        .and_then(|attempts| attempts.first())
                        .and_then(|attempt| attempt["outcome"]["kind"].as_str())
                        != Some("failed")
                    || attempts
                        .and_then(|attempts| attempts.get(1))
                        .and_then(|attempt| attempt["outcome"]["kind"].as_str())
                        != Some("acknowledged")
                {
                    converged = false;
                    break;
                }
            }
            if converged {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "signed webhook acknowledgement did not converge\n{}",
            cluster.diagnostics()
        )
    });
}

async fn wait_for_acknowledged_api_destination(
    client: &Client,
    cluster: &ProcessCluster,
    indexes: &[usize],
) {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let mut converged = true;
            for &index in indexes {
                let response = client
                    .post(data_url(
                        &cluster.nodes[index],
                        "event-bus",
                        "events",
                        "deliveries/query",
                    ))
                    .bearer_auth(ADMIN_TOKEN)
                    .header("x-epoch-resource-generation", "1")
                    .header("x-epoch-tablet-epoch", "1")
                    .header("x-epoch-read-consistency", "local_stale")
                    .json(&json!({
                        "subscription": "api-orders",
                        "state": "acknowledged",
                        "limit": 10
                    }))
                    .send()
                    .await;
                let Ok(response) = response else {
                    converged = false;
                    break;
                };
                if response.status() != StatusCode::OK {
                    converged = false;
                    break;
                }
                let body = response
                    .json::<Value>()
                    .await
                    .expect("delivery query should be JSON");
                let Some(record) = body["records"]
                    .as_array()
                    .and_then(|records| (records.len() == 1).then(|| &records[0]))
                else {
                    converged = false;
                    break;
                };
                if record["envelope"]["id"] != "api-destination-event-1"
                    || record["state"]["kind"] != "acknowledged"
                    || record["attempts"].as_array().map(Vec::len) != Some(1)
                    || record["attempts"][0]["outcome"]["kind"] != "acknowledged"
                {
                    converged = false;
                    break;
                }
            }
            if converged {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "managed API destination acknowledgement did not converge\n{}",
            cluster.diagnostics()
        )
    });
}

async fn wait_for_catalog_counts(client: &Client, cluster: &ProcessCluster, expected: u64) {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let mut digests = Vec::new();
            for node in &cluster.nodes {
                let response = client
                    .get(catalog_url(node))
                    .bearer_auth(ADMIN_TOKEN)
                    .send()
                    .await;
                let Ok(response) = response else {
                    digests.clear();
                    break;
                };
                if response.status() != StatusCode::OK {
                    digests.clear();
                    break;
                }
                let body = response
                    .json::<Value>()
                    .await
                    .expect("catalog should be JSON");
                if exact_u64(&body["resource_count"]) != Some(expected)
                    || exact_u64(&body["tablet_count"]) != Some(expected)
                {
                    digests.clear();
                    break;
                }
                digests.push(body["state_digest"].clone());
            }
            if digests.len() == cluster.nodes.len()
                && digests.windows(2).all(|pair| pair.first() == pair.get(1))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "catalog did not converge to {expected} resources\n{}",
            cluster.diagnostics()
        )
    });
}

async fn wait_for_record_count(
    client: &Client,
    cluster: &ProcessCluster,
    indexes: &[usize],
    expected: usize,
) {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let mut converged = true;
            for &index in indexes {
                let response = client
                    .get(format!(
                        "{}?offset=0&limit=10",
                        records_url(&cluster.nodes[index])
                    ))
                    .bearer_auth(ADMIN_TOKEN)
                    .header("x-epoch-resource-generation", "1")
                    .header("x-epoch-tablet-epoch", "1")
                    .header("x-epoch-read-consistency", "local_stale")
                    .send()
                    .await;
                let Ok(response) = response else {
                    converged = false;
                    break;
                };
                if response.status() != StatusCode::OK {
                    converged = false;
                    break;
                }
                let body = response
                    .json::<Value>()
                    .await
                    .expect("records should be JSON");
                if body["records"].as_array().map(Vec::len) != Some(expected) {
                    converged = false;
                }
            }
            if converged {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "records did not converge to {expected}\n{}",
            cluster.diagnostics()
        )
    });
}

async fn assert_linearizable_record_read(client: &Client, node: &NodeProcess, expected: usize) {
    let response = client
        .get(format!("{}?offset=0&limit=10", records_url(node)))
        .bearer_auth(ADMIN_TOKEN)
        .header("x-epoch-resource-generation", "1")
        .header("x-epoch-tablet-epoch", "1")
        .send()
        .await
        .expect("linearizable read should receive a response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("x-epoch-read-consistency")
            .and_then(|value| value.to_str().ok()),
        Some("linearizable")
    );
    assert!(response.headers().contains_key("x-epoch-read-index"));
    let body = response
        .json::<Value>()
        .await
        .expect("linearizable records should be JSON");
    assert_eq!(body["read_consistency"], "linearizable");
    assert_eq!(body["linearizable_read_barrier"], true);
    assert!(body["read_barrier_term"].is_string());
    assert!(body["read_barrier_index"].is_string());
    assert!(body["read_barrier_applied_index"].is_string());
    assert_eq!(body["records"].as_array().map(Vec::len), Some(expected));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn regional_processes_fail_over_reopen_and_converge() {
    let mut cluster = ProcessCluster::start();
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let all = [0, 1, 2];
    wait_for_nodes(&client, &cluster, &all).await;
    create_resource(&client, &cluster, "stream", "orders").await;

    let routes = wait_for_routes(&client, &cluster, "stream", "orders", &all).await;
    let (first_leader, first_term) = writable_route(&routes, &all);
    append_record(&client, &cluster.nodes[first_leader], first_term, 1).await;
    wait_for_record_count(&client, &cluster, &all, 1).await;
    assert_linearizable_record_read(&client, &cluster.nodes[first_leader], 1).await;

    cluster.nodes[first_leader].kill();
    let survivors = all
        .into_iter()
        .filter(|index| *index != first_leader)
        .collect::<Vec<_>>();
    let routes = wait_for_routes(&client, &cluster, "stream", "orders", &survivors).await;
    let (second_leader, second_term) = writable_route(&routes, &survivors);
    append_record(&client, &cluster.nodes[second_leader], second_term, 2).await;
    wait_for_record_count(&client, &cluster, &survivors, 2).await;

    cluster.restart(first_leader);
    wait_for_nodes(&client, &cluster, &[first_leader]).await;
    wait_for_routes(&client, &cluster, "stream", "orders", &all).await;
    wait_for_record_count(&client, &cluster, &all, 2).await;

    let additional_profiles = [
        ("cache", "sessions"),
        ("queue", "jobs"),
        ("event-bus", "events"),
    ];
    for (kind, name) in additional_profiles {
        create_resource(&client, &cluster, kind, name).await;
        write_profile(&client, &cluster, kind, name, &all).await;
        wait_for_profile_apply(&client, &cluster, kind, name, &all, 1).await;
    }
    prove_epoch_queue_and_stream_targets(&client, &cluster, &all).await;
    prove_signed_webhook_retry(&client, &cluster, &all).await;
    wait_for_acknowledged_webhook(&client, &cluster, &all).await;
    prove_managed_api_destination(&client, &cluster, &all).await;
    wait_for_acknowledged_api_destination(&client, &cluster, &all).await;
    prove_source_connector_ingestion(&client, &cluster, &all).await;
    wait_for_catalog_counts(&client, &cluster, 4).await;

    cluster.stop_all();
    cluster.restart_all();
    wait_for_nodes(&client, &cluster, &all).await;
    wait_for_routes(&client, &cluster, "stream", "orders", &all).await;
    wait_for_record_count(&client, &cluster, &all, 3).await;
    for (kind, name) in additional_profiles {
        wait_for_routes(&client, &cluster, kind, name, &all).await;
        let expected = match kind {
            "event-bus" => 21,
            "queue" => 2,
            _ => 1,
        };
        wait_for_profile_apply(&client, &cluster, kind, name, &all, expected).await;
    }
    wait_for_record_count(&client, &cluster, &all, 3).await;
    wait_for_acknowledged_epoch_targets(&client, &cluster, &all).await;
    wait_for_acknowledged_webhook(&client, &cluster, &all).await;
    wait_for_acknowledged_api_destination(&client, &cluster, &all).await;
    wait_for_catalog_counts(&client, &cluster, 4).await;
}

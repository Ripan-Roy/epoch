use std::{
    fs::{File, read_to_string},
    net::{SocketAddr, TcpListener as StdTcpListener},
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tempfile::TempDir;

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
        let http_addresses = reserve_addresses(NODE_COUNT);
        let peer_addresses = reserve_addresses(NODE_COUNT);
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

fn reserve_addresses(count: usize) -> Vec<SocketAddr> {
    let listeners = (0..count)
        .map(|_| StdTcpListener::bind("127.0.0.1:0").expect("port should reserve"))
        .collect::<Vec<_>>();
    let addresses = listeners
        .iter()
        .map(|listener| listener.local_addr().expect("listener should have address"))
        .collect();
    drop(listeners);
    addresses
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
    wait_for_catalog_counts(&client, &cluster, 4).await;

    cluster.stop_all();
    cluster.restart_all();
    wait_for_nodes(&client, &cluster, &all).await;
    wait_for_routes(&client, &cluster, "stream", "orders", &all).await;
    wait_for_record_count(&client, &cluster, &all, 2).await;
    for (kind, name) in additional_profiles {
        wait_for_routes(&client, &cluster, kind, name, &all).await;
        wait_for_profile_apply(&client, &cluster, kind, name, &all, 1).await;
    }
    wait_for_catalog_counts(&client, &cluster, 4).await;
}

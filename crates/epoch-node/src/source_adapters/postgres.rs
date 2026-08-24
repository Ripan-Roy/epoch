//! Logical-replication ingestion with commit-coupled LSN feedback.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use epoch_bus::{ConnectorKind, ConnectorResource};
use epoch_core::EventEnvelope;
use pgwire_replication::{
    client::{ReplicationClient, ReplicationEvent},
    config::{ReplicationConfig, TlsConfig},
    lsn::Lsn,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::{sync::Mutex, time::timeout};
use url::Url;

use crate::{
    managed_target_delivery::{ManagedSecretStore, ManagedTargetDeliveryConfig, enforce_allowlist},
    source_adapters::{SourceBatch, SourceRecord},
};

const DEFAULT_PORT: u16 = 5432;
const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_millis(250);
const MAX_POLL_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_MAX_TRANSACTION_BYTES: usize = 4 * 1024 * 1024;
const MAX_TRANSACTION_BYTES: usize = 16 * 1024 * 1024;
const MAX_TRANSACTION_RECORDS: usize = 1_000;
const POSTGRES_EPOCH_UNIX_MS: u64 = 946_684_800_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PostgresSource {
    host: String,
    port: u16,
    database: String,
    user: String,
    slot: String,
    publication: String,
    tls: TlsConfig,
    poll_timeout: Duration,
    max_transaction_bytes: usize,
    secret_reference: String,
    source_identity: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PostgresCursor {
    version: u8,
    source: String,
    lsn: String,
}

#[derive(Debug, Clone)]
struct PendingBatch {
    batch: SourceBatch,
    durable_lsn: Lsn,
}

struct PostgresSession {
    client: ReplicationClient,
    checkpoint: String,
    assembler: TransactionAssembler,
    pending: Option<PendingBatch>,
}

impl std::fmt::Debug for PostgresSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PostgresSession")
            .field("checkpoint", &self.checkpoint)
            .field("assembler", &self.assembler)
            .field("pending", &self.pending)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(crate) struct PostgresSourceAdapter {
    secrets: Arc<ManagedSecretStore>,
    allow_http_loopback: bool,
    sessions: Mutex<BTreeMap<String, PostgresSession>>,
}

impl PostgresSourceAdapter {
    pub(crate) fn new(config: &ManagedTargetDeliveryConfig) -> Self {
        Self {
            secrets: Arc::clone(&config.secrets),
            allow_http_loopback: config.allow_http_loopback,
            sessions: Mutex::new(BTreeMap::new()),
        }
    }

    pub(crate) fn resolve(
        &self,
        name: &str,
        resource: &ConnectorResource,
    ) -> Result<Option<PostgresSource>, String> {
        if resource.spec.kind != ConnectorKind::PostgresCdc {
            return Ok(None);
        }
        let host = required(&resource.spec.config, "host", name)?;
        validate_host(name, &host, &resource.spec.outbound_allowlist)?;
        let port = optional_u16(&resource.spec.config, "port", DEFAULT_PORT, name)?;
        let database = required(&resource.spec.config, "database", name)?;
        let user = required(&resource.spec.config, "user", name)?;
        let slot = required(&resource.spec.config, "slot", name)?;
        let publication = required(&resource.spec.config, "publication", name)?;
        let secret_reference = exactly_one_secret(name, &resource.spec.secret_refs)?;
        let poll_timeout = optional_duration(
            &resource.spec.config,
            "poll_timeout_ms",
            DEFAULT_POLL_TIMEOUT,
            MAX_POLL_TIMEOUT,
            name,
        )?;
        let max_transaction_bytes = optional_usize(
            &resource.spec.config,
            "max_transaction_bytes",
            DEFAULT_MAX_TRANSACTION_BYTES,
            1,
            MAX_TRANSACTION_BYTES,
            name,
        )?;
        let tls = postgres_tls(name, &resource.spec.config, self.allow_http_loopback, &host)?;
        let source_identity = stable_hash(&[
            "postgres",
            &host,
            &port.to_string(),
            &database,
            &user,
            &slot,
            &publication,
        ]);
        Ok(Some(PostgresSource {
            host,
            port,
            database,
            user,
            slot,
            publication,
            tls,
            poll_timeout,
            max_transaction_bytes,
            secret_reference,
            source_identity,
        }))
    }

    pub(crate) async fn fetch(
        &self,
        runtime_key: &str,
        source: &PostgresSource,
        source_position: &str,
    ) -> Result<Option<SourceBatch>, String> {
        let cursor = match parse_cursor(source_position, &source.source_identity) {
            Ok(cursor) => cursor,
            Err(error) => {
                let mut sessions = self.sessions.lock().await;
                if let Some(mut session) = sessions.remove(runtime_key) {
                    session.client.abort();
                }
                return Err(error);
            }
        };
        let mut sessions = self.sessions.lock().await;
        if let Some(session) = sessions.get_mut(runtime_key) {
            reconcile_pending(session, source_position)?;
        }
        let recreate = sessions.get(runtime_key).is_none_or(|session| {
            session.checkpoint != source_position || !session.client.is_running()
        });
        if recreate {
            if let Some(mut old) = sessions.remove(runtime_key) {
                old.client.abort();
            }
            let client = self.connect(source, cursor).await?;
            sessions.insert(
                runtime_key.to_owned(),
                PostgresSession {
                    client,
                    checkpoint: source_position.to_owned(),
                    assembler: TransactionAssembler::new(source.max_transaction_bytes),
                    pending: None,
                },
            );
        }
        let session = sessions
            .get_mut(runtime_key)
            .expect("session was inserted above");
        if let Some(pending) = &session.pending {
            return Ok(Some(pending.batch.clone()));
        }

        loop {
            let received = timeout(source.poll_timeout, session.client.recv()).await;
            let event = match received {
                Err(_) => return Ok(None),
                Ok(Err(error)) => {
                    sessions.remove(runtime_key);
                    return Err(format!("PostgreSQL replication stream failed: {error}"));
                }
                Ok(Ok(None)) => {
                    sessions.remove(runtime_key);
                    return Ok(None);
                }
                Ok(Ok(Some(event))) => event,
            };
            let source_from = session.checkpoint.clone();
            if let Some(transaction) = session.assembler.observe(event)? {
                let durable_lsn = transaction.end_lsn;
                let source_to = encode_cursor(&source.source_identity, durable_lsn)?;
                let batch = transaction.into_batch(source, source_from, source_to);
                session.pending = Some(PendingBatch {
                    batch: batch.clone(),
                    durable_lsn,
                });
                return Ok(Some(batch));
            }
        }
    }

    pub(crate) async fn acknowledge(
        &self,
        runtime_key: &str,
        source_to: &str,
    ) -> Result<(), String> {
        let mut sessions = self.sessions.lock().await;
        let session = sessions.get_mut(runtime_key).ok_or_else(|| {
            "PostgreSQL replication session disappeared before acknowledgement".to_owned()
        })?;
        let pending = session
            .pending
            .as_ref()
            .ok_or_else(|| "PostgreSQL replication session has no pending batch".to_owned())?;
        if pending.batch.source_to != source_to {
            return Err("PostgreSQL acknowledgement does not match the pending LSN".into());
        }
        session.client.update_applied_lsn(pending.durable_lsn);
        source_to.clone_into(&mut session.checkpoint);
        session.pending = None;
        Ok(())
    }

    pub(crate) async fn retain_active(&self, active: &BTreeSet<String>) {
        let mut sessions = self.sessions.lock().await;
        let inactive = sessions
            .keys()
            .filter(|key| !active.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        for key in inactive {
            if let Some(mut session) = sessions.remove(&key) {
                session.client.abort();
            }
        }
    }

    async fn connect(
        &self,
        source: &PostgresSource,
        start_lsn: Lsn,
    ) -> Result<ReplicationClient, String> {
        let credentials = self
            .secrets
            .connector_credentials(&source.secret_reference)
            .map_err(|error| error.to_string())?;
        let password = credentials.get("password").ok_or_else(|| {
            "PostgreSQL connector credentials require a password property".to_owned()
        })?;
        let user = credentials.get("username").unwrap_or(&source.user);
        let config = ReplicationConfig::new(
            &source.host,
            user,
            password,
            &source.database,
            &source.slot,
            source.publication.as_str(),
        )
        .with_port(source.port)
        .with_tls(source.tls.clone())
        .with_start_lsn(start_lsn)
        .with_buffer_size(MAX_TRANSACTION_RECORDS.saturating_mul(2));
        ReplicationClient::connect(config)
            .await
            .map_err(|error| format!("PostgreSQL replication connection failed: {error}"))
    }
}

fn reconcile_pending(session: &mut PostgresSession, source_position: &str) -> Result<(), String> {
    let Some(pending) = &session.pending else {
        return Ok(());
    };
    if pending.batch.source_from == source_position {
        return Ok(());
    }
    if pending.batch.source_to == source_position {
        session.client.update_applied_lsn(pending.durable_lsn);
        source_position.clone_into(&mut session.checkpoint);
        session.pending = None;
        return Ok(());
    }
    Err("PostgreSQL checkpoint diverged from the pending transaction".into())
}

#[derive(Debug, Clone)]
struct RawChange {
    wal_start: Lsn,
    wal_end: Lsn,
    data: Vec<u8>,
}

#[derive(Debug, Clone)]
struct AssembledTransaction {
    xid: u32,
    commit_time_micros: i64,
    end_lsn: Lsn,
    changes: Vec<RawChange>,
    overflowed: bool,
}

impl AssembledTransaction {
    fn into_batch(
        self,
        source: &PostgresSource,
        source_from: String,
        source_to: String,
    ) -> SourceBatch {
        let batch_id = format!(
            "postgres-{}",
            stable_hash(&[&source.source_identity, &source_from, &source_to])
        );
        let records = if self.overflowed {
            vec![SourceRecord::Error {
                record_id: format!(
                    "postgres-overflow-{}",
                    stable_hash(&[&source.source_identity, &self.xid.to_string(), &source_to])
                ),
                reason: "postgres_transaction_too_large".into(),
            }]
        } else {
            self.changes
                .iter()
                .enumerate()
                .map(|(index, change)| {
                    SourceRecord::Event(Box::new(postgres_event(source, &self, change, index)))
                })
                .collect()
        };
        SourceBatch {
            batch_id,
            source_from,
            source_to,
            records,
        }
    }
}

#[derive(Debug, Clone)]
struct TransactionAssembler {
    max_bytes: usize,
    xid: Option<u32>,
    changes: Vec<RawChange>,
    bytes: usize,
    overflowed: bool,
}

impl TransactionAssembler {
    fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            xid: None,
            changes: Vec::new(),
            bytes: 0,
            overflowed: false,
        }
    }

    fn observe(&mut self, event: ReplicationEvent) -> Result<Option<AssembledTransaction>, String> {
        match event {
            ReplicationEvent::Begin { xid, .. } => {
                if self.xid.is_some() {
                    return Err("PostgreSQL replication emitted nested transactions".into());
                }
                self.xid = Some(xid);
                self.changes.clear();
                self.bytes = 0;
                self.overflowed = false;
                Ok(None)
            }
            ReplicationEvent::XLogData {
                wal_start,
                wal_end,
                data,
                ..
            } => {
                if self.xid.is_none() {
                    return Err("PostgreSQL WAL change arrived outside a transaction".into());
                }
                self.bytes = self.bytes.saturating_add(data.len());
                if self.bytes > self.max_bytes || self.changes.len() >= MAX_TRANSACTION_RECORDS {
                    self.overflowed = true;
                    self.changes.clear();
                } else if !self.overflowed {
                    self.changes.push(RawChange {
                        wal_start,
                        wal_end,
                        data: data.to_vec(),
                    });
                }
                Ok(None)
            }
            ReplicationEvent::Commit {
                end_lsn,
                commit_time_micros,
                ..
            } => {
                let xid = self.xid.take().ok_or_else(|| {
                    "PostgreSQL commit arrived without a transaction begin".to_owned()
                })?;
                let changes = std::mem::take(&mut self.changes);
                let overflowed = self.overflowed;
                self.bytes = 0;
                self.overflowed = false;
                if changes.is_empty() && !overflowed {
                    return Ok(None);
                }
                Ok(Some(AssembledTransaction {
                    xid,
                    commit_time_micros,
                    end_lsn,
                    changes,
                    overflowed,
                }))
            }
            ReplicationEvent::KeepAlive { .. }
            | ReplicationEvent::Message { .. }
            | ReplicationEvent::StoppedAt { .. } => Ok(None),
        }
    }
}

fn postgres_event(
    source: &PostgresSource,
    transaction: &AssembledTransaction,
    change: &RawChange,
    index: usize,
) -> EventEnvelope {
    let id = format!(
        "postgres-{}",
        stable_hash(&[
            &source.source_identity,
            &transaction.xid.to_string(),
            &change.wal_start.to_string(),
            &index.to_string(),
        ])
    );
    let commit_ms = u64::try_from(transaction.commit_time_micros.max(0))
        .unwrap_or(0)
        .saturating_div(1_000)
        .saturating_add(POSTGRES_EPOCH_UNIX_MS);
    let mut event = EventEnvelope::new(
        format!("urn:epoch:postgres:{}", source.source_identity),
        "io.epoch.postgres.pgoutput.v1",
        json!({
            "encoding": "base64",
            "data": BASE64.encode(&change.data),
            "xid": transaction.xid,
            "wal_start": change.wal_start.to_string(),
            "wal_end": change.wal_end.to_string(),
            "commit_lsn": transaction.end_lsn.to_string()
        }),
        commit_ms,
    );
    event.id = id;
    event.content_type = "application/vnd.postgresql.pgoutput+json".into();
    event.transaction_id = Some(transaction.xid.to_string());
    event
}

fn parse_cursor(position: &str, expected_source: &str) -> Result<Lsn, String> {
    if position == "0" {
        return Ok(Lsn::ZERO);
    }
    let cursor: PostgresCursor = serde_json::from_str(position)
        .map_err(|error| format!("PostgreSQL checkpoint is invalid: {error}"))?;
    if cursor.version != 1 || cursor.source != expected_source {
        return Err("PostgreSQL checkpoint belongs to a different source configuration".into());
    }
    Lsn::parse(&cursor.lsn).map_err(|error| error.to_string())
}

fn encode_cursor(source: &str, lsn: Lsn) -> Result<String, String> {
    serde_json::to_string(&PostgresCursor {
        version: 1,
        source: source.to_owned(),
        lsn: lsn.to_string(),
    })
    .map_err(|error| error.to_string())
}

fn postgres_tls(
    name: &str,
    config: &BTreeMap<String, String>,
    allow_http_loopback: bool,
    host: &str,
) -> Result<TlsConfig, String> {
    let tls = match config.get("tls_mode").map(String::as_str) {
        None | Some("verify_full") => {
            TlsConfig::verify_full(config.get("ca_pem_path").map(PathBuf::from))
        }
        Some("disable") if allow_http_loopback && is_loopback(host) => TlsConfig::disabled(),
        Some("disable") => {
            return Err(format!(
                "connector {name} may disable PostgreSQL TLS only for an explicitly enabled loopback target"
            ));
        }
        Some(other) => {
            return Err(format!(
                "connector {name} PostgreSQL tls_mode {other} is unsupported"
            ));
        }
    };
    let cert = config.get("client_cert_pem_path");
    let key = config.get("client_key_pem_path");
    match (cert, key) {
        (Some(cert), Some(key)) => Ok(tls.with_client_cert(cert, key)),
        (None, None) => Ok(tls),
        _ => Err(format!(
            "connector {name} PostgreSQL client certificate and key must be configured together"
        )),
    }
}

fn validate_host(name: &str, host: &str, allowlist: &BTreeSet<String>) -> Result<(), String> {
    if host.is_empty() || host.len() > 253 || host.contains(['/', '@', '\\']) {
        return Err(format!("connector {name} PostgreSQL host is invalid"));
    }
    let url = Url::parse(&format!("https://{host}/"))
        .map_err(|_| format!("connector {name} PostgreSQL host is invalid"))?;
    enforce_allowlist(&url, allowlist, "connector").map_err(|error| format!("{error:?}"))
}

fn is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

fn exactly_one_secret(name: &str, refs: &BTreeSet<String>) -> Result<String, String> {
    if refs.len() != 1 {
        return Err(format!(
            "connector {name} requires exactly one PostgreSQL credential reference"
        ));
    }
    Ok(refs.iter().next().expect("length checked").clone())
}

fn required(config: &BTreeMap<String, String>, key: &str, name: &str) -> Result<String, String> {
    let value = config
        .get(key)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            format!("connector {name} requires PostgreSQL configuration property {key}")
        })?;
    if value.len() > 1_024 || !value.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(format!(
            "connector {name} PostgreSQL property {key} is invalid"
        ));
    }
    Ok(value.clone())
}

fn optional_u16(
    config: &BTreeMap<String, String>,
    key: &str,
    default: u16,
    name: &str,
) -> Result<u16, String> {
    config.get(key).map_or(Ok(default), |raw| {
        raw.parse::<u16>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| format!("connector {name} PostgreSQL property {key} is invalid"))
    })
}

fn optional_duration(
    config: &BTreeMap<String, String>,
    key: &str,
    default: Duration,
    maximum: Duration,
    name: &str,
) -> Result<Duration, String> {
    config.get(key).map_or(Ok(default), |raw| {
        raw.parse::<u64>()
            .ok()
            .map(Duration::from_millis)
            .filter(|value| !value.is_zero() && *value <= maximum)
            .ok_or_else(|| format!("connector {name} PostgreSQL property {key} is invalid"))
    })
}

fn optional_usize(
    config: &BTreeMap<String, String>,
    key: &str,
    default: usize,
    minimum: usize,
    maximum: usize,
    name: &str,
) -> Result<usize, String> {
    config.get(key).map_or(Ok(default), |raw| {
        raw.parse::<usize>()
            .ok()
            .filter(|value| (*value >= minimum) && (*value <= maximum))
            .ok_or_else(|| format!("connector {name} PostgreSQL property {key} is invalid"))
    })
}

fn stable_hash(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/postgres-source/v1\0");
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    lower_hex(&hasher.finalize())
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
mod tests {
    use epoch_bus::{ConnectorDirection, ConnectorRegistry, ConnectorSpec};

    use super::*;

    fn source(max_transaction_bytes: usize) -> PostgresSource {
        PostgresSource {
            host: "localhost".into(),
            port: 5432,
            database: "orders".into(),
            user: "replicator".into(),
            slot: "epoch_orders".into(),
            publication: "epoch_orders".into(),
            tls: TlsConfig::disabled(),
            poll_timeout: Duration::from_millis(1),
            max_transaction_bytes,
            secret_reference: "postgres-creds".into(),
            source_identity: "source-a".into(),
        }
    }

    #[test]
    fn transaction_assembler_emits_only_on_commit_with_stable_cursor() {
        let source = source(1_024);
        let mut assembler = TransactionAssembler::new(source.max_transaction_bytes);
        assert!(
            assembler
                .observe(ReplicationEvent::Begin {
                    final_lsn: Lsn::from_u64(20),
                    xid: 42,
                    commit_time_micros: 1,
                })
                .unwrap()
                .is_none()
        );
        assert!(
            assembler
                .observe(ReplicationEvent::XLogData {
                    wal_start: Lsn::from_u64(10),
                    wal_end: Lsn::from_u64(20),
                    server_time_micros: 1,
                    data: Vec::from(&b"row-change"[..]).into(),
                })
                .unwrap()
                .is_none()
        );
        let transaction = assembler
            .observe(ReplicationEvent::Commit {
                lsn: Lsn::from_u64(20),
                end_lsn: Lsn::from_u64(24),
                commit_time_micros: 2_000,
            })
            .unwrap()
            .unwrap();
        let source_to = encode_cursor(&source.source_identity, transaction.end_lsn).unwrap();
        let batch = transaction.into_batch(&source, "0".into(), source_to.clone());

        assert_eq!(batch.source_from, "0");
        assert_eq!(
            parse_cursor(&source_to, "source-a").unwrap(),
            Lsn::from_u64(24)
        );
        assert_eq!(batch.records.len(), 1);
        let SourceRecord::Event(event) = &batch.records[0] else {
            panic!("expected a PostgreSQL event");
        };
        assert_eq!(event.transaction_id.as_deref(), Some("42"));
        assert_eq!(event.payload["data"], BASE64.encode(b"row-change"));
    }

    #[test]
    fn oversized_transaction_routes_one_error_and_advances_at_commit() {
        let source = source(4);
        let mut assembler = TransactionAssembler::new(source.max_transaction_bytes);
        assembler
            .observe(ReplicationEvent::Begin {
                final_lsn: Lsn::from_u64(20),
                xid: 7,
                commit_time_micros: 1,
            })
            .unwrap();
        assembler
            .observe(ReplicationEvent::XLogData {
                wal_start: Lsn::from_u64(10),
                wal_end: Lsn::from_u64(20),
                server_time_micros: 1,
                data: Vec::from(&b"too-large"[..]).into(),
            })
            .unwrap();
        let transaction = assembler
            .observe(ReplicationEvent::Commit {
                lsn: Lsn::from_u64(20),
                end_lsn: Lsn::from_u64(24),
                commit_time_micros: 2_000,
            })
            .unwrap()
            .unwrap();
        let batch = transaction.into_batch(
            &source,
            "0".into(),
            encode_cursor("source-a", Lsn::from_u64(24)).unwrap(),
        );
        assert!(
            matches!(batch.records.as_slice(), [SourceRecord::Error { reason, .. }] if reason == "postgres_transaction_too_large")
        );
    }

    #[test]
    fn resolution_requires_allowlist_credentials_and_fences_source_identity() {
        let mut registry = ConnectorRegistry::default();
        registry
            .upsert(
                ConnectorSpec {
                    name: "orders-pg".into(),
                    kind: ConnectorKind::PostgresCdc,
                    direction: ConnectorDirection::Source,
                    secret_refs: BTreeSet::from(["postgres-creds".into()]),
                    outbound_allowlist: BTreeSet::from(["localhost".into()]),
                    identity: "orders-reader".into(),
                    config: BTreeMap::from([
                        ("host".into(), "localhost".into()),
                        ("database".into(), "orders".into()),
                        ("user".into(), "replicator".into()),
                        ("slot".into(), "epoch_orders".into()),
                        ("publication".into(), "orders_publication".into()),
                        ("tls_mode".into(), "disable".into()),
                    ]),
                },
                1,
            )
            .unwrap();
        let resource = registry.connector("orders-pg").unwrap();
        let adapter = PostgresSourceAdapter {
            secrets: Arc::new(ManagedSecretStore::default()),
            allow_http_loopback: true,
            sessions: Mutex::new(BTreeMap::new()),
        };
        let resolved = adapter.resolve("orders-pg", resource).unwrap().unwrap();
        let cursor = encode_cursor(&resolved.source_identity, Lsn::from_u64(1)).unwrap();
        assert!(parse_cursor(&cursor, &resolved.source_identity).is_ok());
        assert!(parse_cursor(&cursor, "another-source").is_err());
    }

    #[tokio::test]
    #[ignore = "requires deploy/compose/docker-compose.connectors.yml"]
    async fn live_logical_replication_reads_committed_transaction_and_acknowledges_lsn() {
        let (client, connection) = tokio_postgres::connect(
            "host=127.0.0.1 port=15432 user=epoch_replication password=epoch-replication-password dbname=epoch_connectors",
            tokio_postgres::NoTls,
        )
        .await
        .unwrap();
        let connection_task = tokio::spawn(connection);
        client
            .batch_execute(
                "INSERT INTO orders (id, description) SELECT COALESCE(MAX(id), 0) + 1, 'live-connector-conformance' FROM orders",
            )
            .await
            .unwrap();
        drop(client);
        connection_task.await.unwrap().unwrap();

        let config = crate::source_adapters::test_delivery_config(&json!([{
            "kind": "connector_credentials",
            "reference": "postgres-live",
            "values": {
                "username": "epoch_replication",
                "password": "epoch-replication-password"
            }
        }]));
        let adapter = PostgresSourceAdapter::new(&config);
        let mut registry = ConnectorRegistry::default();
        registry
            .upsert(
                ConnectorSpec {
                    name: "postgres-live".into(),
                    kind: ConnectorKind::PostgresCdc,
                    direction: ConnectorDirection::Source,
                    secret_refs: BTreeSet::from(["postgres-live".into()]),
                    outbound_allowlist: BTreeSet::from(["127.0.0.1".into()]),
                    identity: "postgres-live-reader".into(),
                    config: BTreeMap::from([
                        ("host".into(), "127.0.0.1".into()),
                        ("port".into(), "15432".into()),
                        ("database".into(), "epoch_connectors".into()),
                        ("user".into(), "epoch_replication".into()),
                        ("slot".into(), "epoch_orders_slot".into()),
                        ("publication".into(), "epoch_orders".into()),
                        ("tls_mode".into(), "disable".into()),
                        ("poll_timeout_ms".into(), "1000".into()),
                    ]),
                },
                1,
            )
            .unwrap();
        let source = adapter
            .resolve(
                "postgres-live",
                registry.connector("postgres-live").unwrap(),
            )
            .unwrap()
            .unwrap();

        let mut batch = None;
        for _ in 0..10 {
            batch = adapter.fetch("live:postgres", &source, "0").await.unwrap();
            if batch.is_some() {
                break;
            }
        }
        let batch = batch.expect("PostgreSQL logical slot should contain the fixture insert");
        assert!(
            batch
                .records
                .iter()
                .any(|record| matches!(record, SourceRecord::Event(_)))
        );
        adapter
            .acknowledge("live:postgres", &batch.source_to)
            .await
            .unwrap();
        adapter.retain_active(&BTreeSet::new()).await;
        assert!(adapter.sessions.lock().await.is_empty());
    }
}

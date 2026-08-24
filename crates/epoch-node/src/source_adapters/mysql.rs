//! Row-binlog ingestion with transaction-boundary checkpoints.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use epoch_bus::{ConnectorKind, ConnectorResource};
use epoch_core::EventEnvelope;
use futures_util::StreamExt;
use mysql_async::{
    BinlogStreamRequest, ClientIdentity, Conn, OptsBuilder, SslOpts,
    binlog::{
        BinlogVersion, EventType,
        events::{Event, QueryEvent, RotateEvent},
    },
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use url::Url;

use crate::{
    managed_target_delivery::{ManagedSecretStore, ManagedTargetDeliveryConfig, enforce_allowlist},
    source_adapters::{SourceBatch, SourceRecord},
};

const DEFAULT_PORT: u16 = 3306;
const DEFAULT_POSITION: u64 = 4;
const DEFAULT_MAX_TRANSACTION_BYTES: usize = 4 * 1024 * 1024;
const MAX_TRANSACTION_BYTES: usize = 16 * 1024 * 1024;
const MAX_TRANSACTION_RECORDS: usize = 1_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MySqlSource {
    host: String,
    port: u16,
    database: String,
    user: String,
    server_id: u32,
    start_file: String,
    start_position: u64,
    tls: Option<SslOpts>,
    max_transaction_bytes: usize,
    secret_reference: String,
    source_identity: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MySqlCursor {
    version: u8,
    source: String,
    file: String,
    position: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct MySqlSourceAdapter {
    secrets: Arc<ManagedSecretStore>,
    allow_http_loopback: bool,
}

impl MySqlSourceAdapter {
    pub(crate) fn new(config: &ManagedTargetDeliveryConfig) -> Self {
        Self {
            secrets: Arc::clone(&config.secrets),
            allow_http_loopback: config.allow_http_loopback,
        }
    }

    pub(crate) fn resolve(
        &self,
        name: &str,
        resource: &ConnectorResource,
    ) -> Result<Option<MySqlSource>, String> {
        if resource.spec.kind != ConnectorKind::MySqlCdc {
            return Ok(None);
        }
        let host = required(&resource.spec.config, "host", name)?;
        validate_host(name, &host, &resource.spec.outbound_allowlist)?;
        let port = optional_integer(
            &resource.spec.config,
            "port",
            DEFAULT_PORT,
            1,
            u16::MAX,
            name,
        )?;
        let database = required(&resource.spec.config, "database", name)?;
        let user = required(&resource.spec.config, "user", name)?;
        let server_id = optional_integer(
            &resource.spec.config,
            "server_id",
            4_294_000_001_u32,
            1,
            u32::MAX,
            name,
        )?;
        let start_file = required(&resource.spec.config, "start_file", name)?;
        validate_binlog_file(&start_file)?;
        let start_position = optional_integer(
            &resource.spec.config,
            "start_binlog_position",
            DEFAULT_POSITION,
            DEFAULT_POSITION,
            u64::MAX,
            name,
        )?;
        let max_transaction_bytes = optional_integer(
            &resource.spec.config,
            "max_transaction_bytes",
            DEFAULT_MAX_TRANSACTION_BYTES,
            1,
            MAX_TRANSACTION_BYTES,
            name,
        )?;
        let secret_reference = exactly_one_secret(name, &resource.spec.secret_refs)?;
        let tls = mysql_tls(name, &resource.spec.config, self.allow_http_loopback, &host)?;
        let source_identity = stable_hash(&[
            "mysql",
            &host,
            &port.to_string(),
            &database,
            &user,
            &server_id.to_string(),
        ]);
        Ok(Some(MySqlSource {
            host,
            port,
            database,
            user,
            server_id,
            start_file,
            start_position,
            tls,
            max_transaction_bytes,
            secret_reference,
            source_identity,
        }))
    }

    pub(crate) async fn fetch(
        &self,
        source: &MySqlSource,
        source_position: &str,
    ) -> Result<Option<SourceBatch>, String> {
        let cursor = parse_cursor(source_position, source)?;
        let credentials = self
            .secrets
            .connector_credentials(&source.secret_reference)
            .map_err(|error| error.to_string())?;
        let password = credentials
            .get("password")
            .ok_or_else(|| "MySQL connector credentials require a password property".to_owned())?;
        let user = credentials.get("username").unwrap_or(&source.user);
        let opts = OptsBuilder::default()
            .ip_or_hostname(&source.host)
            .tcp_port(source.port)
            .user(Some(user.as_str()))
            .pass(Some(password.as_str()))
            .db_name(Some(source.database.as_str()))
            .prefer_socket(false)
            .ssl_opts(source.tls.clone());
        let connection = Conn::new(opts)
            .await
            .map_err(|error| format!("MySQL CDC connection failed: {error}"))?;
        let request = BinlogStreamRequest::new(source.server_id)
            .with_filename(cursor.file.as_bytes())
            .with_pos(cursor.position)
            .with_non_blocking();
        let mut stream = connection
            .get_binlog_stream(request)
            .await
            .map_err(|error| format!("MySQL binlog request failed: {error}"))?;
        let mut assembler = BinlogAssembler::new(
            cursor.file.clone(),
            cursor.position,
            source.max_transaction_bytes,
        );
        while let Some(event) = stream.next().await {
            let event = event.map_err(|error| format!("MySQL binlog stream failed: {error}"))?;
            let observation = observe_event(&event)?;
            if let Some(transaction) = assembler.observe(&observation)? {
                let source_to = encode_cursor(
                    &source.source_identity,
                    &transaction.file,
                    transaction.position,
                )?;
                return Ok(Some(transaction.into_batch(
                    source,
                    source_position.to_owned(),
                    source_to,
                )));
            }
        }
        Ok(None)
    }
}

#[derive(Debug, Clone)]
struct ObservedEvent {
    event_type: EventType,
    event_type_raw: u8,
    log_position: u64,
    timestamp_seconds: u32,
    bytes: Vec<u8>,
    query: Option<String>,
    rotation: Option<(String, u64)>,
}

fn observe_event(event: &Event) -> Result<ObservedEvent, String> {
    let header = event.header();
    let event_type = header
        .event_type()
        .map_err(|error| format!("MySQL binlog event type is unknown: {error}"))?;
    let mut bytes = Vec::new();
    event
        .write(BinlogVersion::Version4, &mut bytes)
        .map_err(|error| format!("MySQL binlog event serialization failed: {error}"))?;
    let query = if event_type == EventType::QUERY_EVENT {
        Some(
            event
                .read_event::<QueryEvent<'_>>()
                .map_err(|error| format!("MySQL query event is invalid: {error}"))?
                .query()
                .into_owned(),
        )
    } else {
        None
    };
    let rotation = if event_type == EventType::ROTATE_EVENT {
        let rotation = event
            .read_event::<RotateEvent<'_>>()
            .map_err(|error| format!("MySQL rotate event is invalid: {error}"))?;
        Some((rotation.name().into_owned(), rotation.position()))
    } else {
        None
    };
    Ok(ObservedEvent {
        event_type,
        event_type_raw: header.event_type_raw(),
        log_position: u64::from(header.log_pos()),
        timestamp_seconds: header.timestamp(),
        bytes,
        query,
        rotation,
    })
}

#[derive(Debug, Clone)]
struct BinlogTransaction {
    file: String,
    position: u64,
    events: Vec<ObservedEvent>,
    overflowed: bool,
}

impl BinlogTransaction {
    fn into_batch(
        self,
        source: &MySqlSource,
        source_from: String,
        source_to: String,
    ) -> SourceBatch {
        let batch_id = format!(
            "mysql-{}",
            stable_hash(&[&source.source_identity, &source_from, &source_to])
        );
        let records = if self.overflowed {
            vec![SourceRecord::Error {
                record_id: format!(
                    "mysql-overflow-{}",
                    stable_hash(&[
                        &source.source_identity,
                        &self.file,
                        &self.position.to_string()
                    ])
                ),
                reason: "mysql_transaction_too_large".into(),
            }]
        } else {
            self.events
                .iter()
                .enumerate()
                .map(|(index, event)| {
                    SourceRecord::Event(Box::new(mysql_event(source, &self, event, index)))
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
struct BinlogAssembler {
    file: String,
    position: u64,
    in_transaction: bool,
    events: Vec<ObservedEvent>,
    bytes: usize,
    overflowed: bool,
    max_bytes: usize,
}

impl BinlogAssembler {
    fn new(file: String, position: u64, max_bytes: usize) -> Self {
        Self {
            file,
            position,
            in_transaction: false,
            events: Vec::new(),
            bytes: 0,
            overflowed: false,
            max_bytes,
        }
    }

    fn observe(&mut self, event: &ObservedEvent) -> Result<Option<BinlogTransaction>, String> {
        if let Some((file, position)) = &event.rotation {
            validate_binlog_file(file)?;
            self.file.clone_from(file);
            self.position = *position;
            return Ok(None);
        }
        if event.log_position > 0 {
            self.position = event.log_position;
        }
        if is_ignored_control(event.event_type) {
            return Ok(None);
        }

        let query = event
            .query
            .as_deref()
            .map(str::trim)
            .map(str::to_ascii_uppercase);
        let gtid_begins = matches!(
            event.event_type,
            EventType::GTID_EVENT | EventType::ANONYMOUS_GTID_EVENT
        );
        let query_begins = query.as_deref() == Some("BEGIN");
        if gtid_begins || query_begins {
            if gtid_begins && self.in_transaction && !self.events.is_empty() {
                return Err("MySQL binlog emitted a nested transaction".into());
            }
            self.in_transaction = true;
        }

        self.bytes = self.bytes.saturating_add(event.bytes.len());
        if self.bytes > self.max_bytes || self.events.len() >= MAX_TRANSACTION_RECORDS {
            self.overflowed = true;
            self.events.clear();
        } else if !self.overflowed {
            self.events.push(event.clone());
        }

        let query_terminal = matches!(query.as_deref(), Some("COMMIT" | "ROLLBACK"));
        let xid_terminal = event.event_type == EventType::XID_EVENT;
        let autocommit_query =
            event.event_type == EventType::QUERY_EVENT && !query_begins && !self.in_transaction;
        if query_terminal || xid_terminal || autocommit_query {
            self.in_transaction = false;
            return Ok(self.finish());
        }
        Ok(None)
    }

    fn finish(&mut self) -> Option<BinlogTransaction> {
        if self.events.is_empty() && !self.overflowed {
            return None;
        }
        let transaction = BinlogTransaction {
            file: self.file.clone(),
            position: self.position,
            events: std::mem::take(&mut self.events),
            overflowed: self.overflowed,
        };
        self.bytes = 0;
        self.overflowed = false;
        Some(transaction)
    }
}

fn is_ignored_control(event_type: EventType) -> bool {
    matches!(
        event_type,
        EventType::FORMAT_DESCRIPTION_EVENT
            | EventType::ROTATE_EVENT
            | EventType::PREVIOUS_GTIDS_EVENT
            | EventType::HEARTBEAT_EVENT
            | EventType::STOP_EVENT
    )
}

fn mysql_event(
    source: &MySqlSource,
    transaction: &BinlogTransaction,
    observed: &ObservedEvent,
    index: usize,
) -> EventEnvelope {
    let mut event = EventEnvelope::new(
        format!("urn:epoch:mysql:{}", source.source_identity),
        "io.epoch.mysql.binlog.v1",
        json!({
            "encoding": "base64",
            "data": BASE64.encode(&observed.bytes),
            "event_type": observed.event_type_raw,
            "binlog_file": transaction.file,
            "binlog_position": observed.log_position
        }),
        u64::from(observed.timestamp_seconds).saturating_mul(1_000),
    );
    event.id = format!(
        "mysql-{}",
        stable_hash(&[
            &source.source_identity,
            &transaction.file,
            &observed.log_position.to_string(),
            &index.to_string(),
        ])
    );
    event.content_type = "application/vnd.mysql.binlog+json".into();
    event.transaction_id = Some(format!("{}:{}", transaction.file, transaction.position));
    event
}

fn parse_cursor(position: &str, source: &MySqlSource) -> Result<MySqlCursor, String> {
    if position == "0" {
        return Ok(MySqlCursor {
            version: 1,
            source: source.source_identity.clone(),
            file: source.start_file.clone(),
            position: source.start_position,
        });
    }
    let cursor: MySqlCursor = serde_json::from_str(position)
        .map_err(|error| format!("MySQL checkpoint is invalid: {error}"))?;
    if cursor.version != 1 || cursor.source != source.source_identity {
        return Err("MySQL checkpoint belongs to a different source configuration".into());
    }
    validate_binlog_file(&cursor.file)?;
    if cursor.position < DEFAULT_POSITION {
        return Err("MySQL checkpoint position must be at least 4".into());
    }
    Ok(cursor)
}

fn encode_cursor(source: &str, file: &str, position: u64) -> Result<String, String> {
    validate_binlog_file(file)?;
    serde_json::to_string(&MySqlCursor {
        version: 1,
        source: source.to_owned(),
        file: file.to_owned(),
        position,
    })
    .map_err(|error| error.to_string())
}

fn validate_binlog_file(file: &str) -> Result<(), String> {
    if file.is_empty()
        || file.len() > 255
        || !file
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err("MySQL binlog filename is invalid".into());
    }
    Ok(())
}

fn mysql_tls(
    name: &str,
    config: &BTreeMap<String, String>,
    allow_http_loopback: bool,
    host: &str,
) -> Result<Option<SslOpts>, String> {
    match config.get("tls_mode").map(String::as_str) {
        None | Some("verify_full") => {
            let mut options = SslOpts::default();
            if let Some(ca) = config.get("ca_pem_path") {
                options = options.with_root_certs(vec![PathBuf::from(ca).into()]);
            }
            match (
                config.get("client_cert_pem_path"),
                config.get("client_key_pem_path"),
            ) {
                (Some(cert), Some(key)) => {
                    options = options.with_client_identity(Some(ClientIdentity::new(
                        PathBuf::from(cert).into(),
                        PathBuf::from(key).into(),
                    )));
                }
                (None, None) => {}
                _ => {
                    return Err(format!(
                        "connector {name} MySQL client certificate and key must be configured together"
                    ));
                }
            }
            Ok(Some(options))
        }
        Some("disable") if allow_http_loopback && is_loopback(host) => Ok(None),
        Some("disable") => Err(format!(
            "connector {name} may disable MySQL TLS only for an explicitly enabled loopback target"
        )),
        Some(other) => Err(format!(
            "connector {name} MySQL tls_mode {other} is unsupported"
        )),
    }
}

fn validate_host(name: &str, host: &str, allowlist: &BTreeSet<String>) -> Result<(), String> {
    if host.is_empty() || host.len() > 253 || host.contains(['/', '@', '\\']) {
        return Err(format!("connector {name} MySQL host is invalid"));
    }
    let url = Url::parse(&format!("https://{host}/"))
        .map_err(|_| format!("connector {name} MySQL host is invalid"))?;
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
            "connector {name} requires exactly one MySQL credential reference"
        ));
    }
    Ok(refs.iter().next().expect("length checked").clone())
}

fn required(config: &BTreeMap<String, String>, key: &str, name: &str) -> Result<String, String> {
    let value = config
        .get(key)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("connector {name} requires MySQL configuration property {key}"))?;
    if value.len() > 1_024 || !value.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(format!("connector {name} MySQL property {key} is invalid"));
    }
    Ok(value.clone())
}

fn optional_integer<T>(
    config: &BTreeMap<String, String>,
    key: &str,
    default: T,
    minimum: T,
    maximum: T,
    name: &str,
) -> Result<T, String>
where
    T: Copy + Ord + std::str::FromStr,
{
    config.get(key).map_or(Ok(default), |raw| {
        raw.parse::<T>()
            .ok()
            .filter(|value| *value >= minimum && *value <= maximum)
            .ok_or_else(|| format!("connector {name} MySQL property {key} is invalid"))
    })
}

fn stable_hash(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/mysql-source/v1\0");
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
    use mysql_async::prelude::Queryable;

    use super::*;

    fn source(max_transaction_bytes: usize) -> MySqlSource {
        MySqlSource {
            host: "localhost".into(),
            port: 3306,
            database: "orders".into(),
            user: "replicator".into(),
            server_id: 4_294_000_001,
            start_file: "mysql-bin.000001".into(),
            start_position: 4,
            tls: None,
            max_transaction_bytes,
            secret_reference: "mysql-creds".into(),
            source_identity: "source-a".into(),
        }
    }

    fn observed(event_type: EventType, position: u64, bytes: &[u8]) -> ObservedEvent {
        ObservedEvent {
            event_type,
            event_type_raw: event_type as u8,
            log_position: position,
            timestamp_seconds: 10,
            bytes: bytes.to_vec(),
            query: None,
            rotation: None,
        }
    }

    #[test]
    fn row_transaction_emits_only_at_xid_and_resumes_exactly() {
        let source = source(1_024);
        let mut assembler = BinlogAssembler::new(source.start_file.clone(), 4, 1_024);
        assert!(
            assembler
                .observe(&observed(EventType::GTID_EVENT, 100, b"gtid"))
                .unwrap()
                .is_none()
        );
        let mut begin = observed(EventType::QUERY_EVENT, 110, b"begin");
        begin.query = Some("BEGIN".into());
        assert!(assembler.observe(&begin).unwrap().is_none());
        assert!(
            assembler
                .observe(&observed(EventType::WRITE_ROWS_EVENT, 140, b"row"))
                .unwrap()
                .is_none()
        );
        let transaction = assembler
            .observe(&observed(EventType::XID_EVENT, 160, b"xid"))
            .unwrap()
            .unwrap();
        let source_to = encode_cursor("source-a", &transaction.file, transaction.position).unwrap();
        let batch = transaction.into_batch(&source, "0".into(), source_to.clone());

        assert_eq!(batch.records.len(), 4);
        assert_eq!(parse_cursor(&source_to, &source).unwrap().position, 160);
        assert!(
            batch
                .records
                .iter()
                .all(|record| matches!(record, SourceRecord::Event(_)))
        );
    }

    #[test]
    fn rotation_and_oversized_transaction_preserve_new_file_cursor() {
        let source = source(3);
        let mut assembler = BinlogAssembler::new(source.start_file.clone(), 4, 3);
        let mut rotate = observed(EventType::ROTATE_EVENT, 0, b"");
        rotate.rotation = Some(("mysql-bin.000002".into(), 4));
        assert!(assembler.observe(&rotate).unwrap().is_none());
        assembler
            .observe(&observed(EventType::GTID_EVENT, 100, b"large"))
            .unwrap();
        let transaction = assembler
            .observe(&observed(EventType::XID_EVENT, 120, b"also-large"))
            .unwrap()
            .unwrap();
        assert_eq!(transaction.file, "mysql-bin.000002");
        let batch = transaction.into_batch(
            &source,
            "0".into(),
            encode_cursor("source-a", "mysql-bin.000002", 120).unwrap(),
        );
        assert!(
            matches!(batch.records.as_slice(), [SourceRecord::Error { reason, .. }] if reason == "mysql_transaction_too_large")
        );
    }

    #[test]
    fn resolution_requires_allowlist_and_fences_source_identity() {
        let mut registry = ConnectorRegistry::default();
        registry
            .upsert(
                ConnectorSpec {
                    name: "orders-mysql".into(),
                    kind: ConnectorKind::MySqlCdc,
                    direction: ConnectorDirection::Source,
                    secret_refs: BTreeSet::from(["mysql-creds".into()]),
                    outbound_allowlist: BTreeSet::from(["localhost".into()]),
                    identity: "orders-reader".into(),
                    config: BTreeMap::from([
                        ("host".into(), "localhost".into()),
                        ("database".into(), "orders".into()),
                        ("user".into(), "replicator".into()),
                        ("start_file".into(), "mysql-bin.000001".into()),
                        ("tls_mode".into(), "disable".into()),
                    ]),
                },
                1,
            )
            .unwrap();
        let adapter = MySqlSourceAdapter {
            secrets: Arc::new(ManagedSecretStore::default()),
            allow_http_loopback: true,
        };
        let resolved = adapter
            .resolve("orders-mysql", registry.connector("orders-mysql").unwrap())
            .unwrap()
            .unwrap();
        let cursor = encode_cursor(&resolved.source_identity, "mysql-bin.000001", 4).unwrap();
        assert!(parse_cursor(&cursor, &resolved).is_ok());
        let mut other = resolved.clone();
        other.source_identity = "other".into();
        assert!(parse_cursor(&cursor, &other).is_err());
    }

    #[tokio::test]
    #[ignore = "requires deploy/compose/docker-compose.connectors.yml"]
    async fn live_row_binlog_reads_one_complete_transaction_from_exact_position() {
        let opts = OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(13306)
            .user(Some("epoch_replication"))
            .pass(Some("epoch-replication-password"))
            .db_name(Some("epoch_connectors"))
            .prefer_socket(false);
        let mut connection = Conn::new(opts).await.unwrap();
        let status: mysql_async::Row = connection
            .query_first("SHOW BINARY LOG STATUS")
            .await
            .unwrap()
            .expect("MySQL should expose binary-log status");
        let start_file: String = status.get(0).expect("binary-log filename");
        let start_position: u64 = status.get(1).expect("binary-log position");
        connection
            .query_drop("INSERT INTO orders (description) VALUES ('live-connector-conformance')")
            .await
            .unwrap();
        connection.disconnect().await.unwrap();

        let config = crate::source_adapters::test_delivery_config(&json!([{
            "kind": "connector_credentials",
            "reference": "mysql-live",
            "values": {
                "username": "epoch_replication",
                "password": "epoch-replication-password"
            }
        }]));
        let adapter = MySqlSourceAdapter::new(&config);
        let mut registry = ConnectorRegistry::default();
        registry
            .upsert(
                ConnectorSpec {
                    name: "mysql-live".into(),
                    kind: ConnectorKind::MySqlCdc,
                    direction: ConnectorDirection::Source,
                    secret_refs: BTreeSet::from(["mysql-live".into()]),
                    outbound_allowlist: BTreeSet::from(["127.0.0.1".into()]),
                    identity: "mysql-live-reader".into(),
                    config: BTreeMap::from([
                        ("host".into(), "127.0.0.1".into()),
                        ("port".into(), "13306".into()),
                        ("database".into(), "epoch_connectors".into()),
                        ("user".into(), "epoch_replication".into()),
                        ("server_id".into(), "4294000002".into()),
                        ("start_file".into(), start_file),
                        ("start_binlog_position".into(), start_position.to_string()),
                        ("tls_mode".into(), "disable".into()),
                    ]),
                },
                1,
            )
            .unwrap();
        let source = adapter
            .resolve("mysql-live", registry.connector("mysql-live").unwrap())
            .unwrap()
            .unwrap();
        let batch = adapter.fetch(&source, "0").await.unwrap().unwrap();

        assert!(
            batch
                .records
                .iter()
                .any(|record| matches!(record, SourceRecord::Event(_)))
        );
        let cursor = parse_cursor(&batch.source_to, &source).unwrap();
        assert_eq!(cursor.file, source.start_file);
        assert!(cursor.position > source.start_position);
    }
}

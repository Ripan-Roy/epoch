use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, Mutex},
    time::Instant,
};

use amq_protocol::{
    frame::{AMQPContentHeader, AMQPFrame, ProtocolVersion, WriteContext, gen_frame, parse_frame},
    protocol::{AMQPClass, BasicProperties, basic, channel, confirm, connection, exchange, queue},
    types::{AMQPValue, FieldArray, FieldTable, LongString},
};
use anyhow::{Context, Result, bail};
use epoch_observability::{MetricsRegistry, Outcome, Protocol, ProtocolOperation};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tracing::Instrument as _;

use crate::{
    CompatibilityBackend, MAX_FRAME_BYTES, MAX_MESSAGE_BYTES, MAX_REQUEST_ITEMS,
    backend::{
        BackendError, CacheAtomicMutation, CacheEntry, CacheStorageClass, CacheValue,
        QueueDelivery, QueueMessage,
    },
    observe_protocol,
};

const AMQP_PROTOCOL_HEADER: &[u8; 8] = b"AMQP\0\0\x09\x01";
const AMQP_FRAME_END: u8 = 0xce;
const MAX_CHANNELS: u16 = 2_048;
const TOPOLOGY_KEY: &str = "__epoch:amqp:topology:v1";
const TOPOLOGY_VERSION: u16 = 1;
const MAX_TOPOLOGY_ENTRIES: usize = 4_096;
const MAX_TOPOLOGY_UPDATE_ATTEMPTS: usize = 4;
const DLX_EXCHANGE_HEADER: &str = "x-epoch-compat-dlx-exchange";
const DLX_ROUTING_KEY_HEADER: &str = "x-epoch-compat-dlx-routing-key";
const DEATH_HISTORY_HEADER: &str = "x-epoch-compat-death-history";

#[derive(Clone)]
pub struct AmqpConfig {
    pub username: String,
    pub password: String,
    pub max_connections: usize,
    pub heartbeat_seconds: u16,
    /// Replicated Cache used for durable exchange and binding metadata.
    pub topology_cache: String,
}

impl fmt::Debug for AmqpConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AmqpConfig")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("max_connections", &self.max_connections)
            .field("heartbeat_seconds", &self.heartbeat_seconds)
            .field("topology_cache", &self.topology_cache)
            .finish()
    }
}

#[derive(Debug)]
pub struct AmqpServer<B> {
    backend: Arc<B>,
    config: AmqpConfig,
    topology: Arc<Mutex<Topology>>,
    metrics: Option<MetricsRegistry>,
}

impl<B: CompatibilityBackend> AmqpServer<B> {
    pub fn new(backend: Arc<B>, config: AmqpConfig) -> Result<Self, BackendError> {
        if config.username.is_empty()
            || config.password.is_empty()
            || config.max_connections == 0
            || config.topology_cache.trim().is_empty()
        {
            return Err(BackendError::Invalid(
                "AMQP credentials, topology Cache, and positive connection limit are required"
                    .into(),
            ));
        }
        Ok(Self {
            backend,
            config,
            topology: Arc::new(Mutex::new(Topology::default())),
            metrics: None,
        })
    }

    #[must_use]
    pub fn with_observability(mut self, metrics: MetricsRegistry) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub async fn serve(self, listener: TcpListener) -> Result<(), std::io::Error> {
        let permits = Arc::new(tokio::sync::Semaphore::new(self.config.max_connections));
        loop {
            let (stream, _) = listener.accept().await?;
            let permit = Arc::clone(&permits).acquire_owned().await;
            let Ok(permit) = permit else {
                return Ok(());
            };
            let backend = Arc::clone(&self.backend);
            let config = self.config.clone();
            let topology = Arc::clone(&self.topology);
            let metrics = self.metrics.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let started = Instant::now();
                let result =
                    serve_connection(stream, backend, config, topology, metrics.as_ref()).await;
                observe_protocol(
                    metrics.as_ref(),
                    Protocol::Amqp091,
                    ProtocolOperation::Connect,
                    if result.is_ok() {
                        Outcome::Success
                    } else {
                        Outcome::ServerError
                    },
                    started.elapsed(),
                );
                if let Err(error) = result {
                    tracing::warn!(protocol = "amqp", %error, "compatibility connection closed");
                }
            });
        }
    }
}

async fn serve_connection<B: CompatibilityBackend>(
    mut stream: TcpStream,
    backend: Arc<B>,
    config: AmqpConfig,
    topology: Arc<Mutex<Topology>>,
    metrics: Option<&MetricsRegistry>,
) -> Result<()> {
    let mut protocol_header = [0_u8; 8];
    stream.read_exact(&mut protocol_header).await?;
    if &protocol_header != AMQP_PROTOCOL_HEADER {
        write_frame(
            &mut stream,
            &AMQPFrame::ProtocolHeader(ProtocolVersion::amqp_0_9_1()),
        )
        .await?;
        bail!("unsupported AMQP protocol header");
    }
    write_method(
        &mut stream,
        0,
        AMQPClass::Connection(connection::AMQPMethod::Start(connection::Start {
            version_major: 0,
            version_minor: 9,
            server_properties: FieldTable::default(),
            mechanisms: LongString::from(b"PLAIN".to_vec()),
            locales: LongString::from(b"en_US".to_vec()),
        })),
    )
    .await?;
    let start_ok = read_frame(&mut stream).await?;
    let AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::StartOk(start_ok))) =
        start_ok
    else {
        bail!("expected AMQP connection.start-ok");
    };
    authenticate(&config, &start_ok)?;
    write_method(
        &mut stream,
        0,
        AMQPClass::Connection(connection::AMQPMethod::Tune(connection::Tune {
            channel_max: MAX_CHANNELS,
            frame_max: u32::try_from(MAX_FRAME_BYTES).unwrap_or(u32::MAX),
            heartbeat: config.heartbeat_seconds,
        })),
    )
    .await?;
    let tune_ok = read_frame(&mut stream).await?;
    let AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::TuneOk(tune_ok))) =
        tune_ok
    else {
        bail!("expected AMQP connection.tune-ok");
    };
    if tune_ok.channel_max > MAX_CHANNELS
        || usize::try_from(tune_ok.frame_max).unwrap_or(usize::MAX) > MAX_FRAME_BYTES
    {
        bail!("AMQP tune exceeds server bounds");
    }
    let open = read_frame(&mut stream).await?;
    let AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::Open(open))) = open
    else {
        bail!("expected AMQP connection.open");
    };
    if open.virtual_host.as_str() != "/" {
        bail!("only AMQP virtual host / is supported");
    }
    write_method(
        &mut stream,
        0,
        AMQPClass::Connection(connection::AMQPMethod::OpenOk(connection::OpenOk::default())),
    )
    .await?;

    run_session(stream, backend, topology, config.topology_cache, metrics).await
}

async fn run_session<B: CompatibilityBackend>(
    stream: TcpStream,
    backend: Arc<B>,
    topology: Arc<Mutex<Topology>>,
    topology_cache: String,
    metrics: Option<&MetricsRegistry>,
) -> Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    let (frames_tx, mut frames_rx) = mpsc::channel(32);
    let reader_task = tokio::spawn(async move {
        loop {
            let frame = read_frame(&mut reader).await;
            let done = frame.is_err();
            if frames_tx.send(frame).await.is_err() || done {
                return;
            }
        }
    });
    let mut session = Session::with_topology_cache(backend, topology, topology_cache);
    let mut poll = tokio::time::interval(std::time::Duration::from_millis(10));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let result = async {
        loop {
            let responses = tokio::select! {
                frame = frames_rx.recv() => {
                    let frame = frame.context("AMQP reader stopped")??;
                    if matches!(
                        frame,
                        AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::Close(_)))
                    ) {
                        write_method(
                            &mut writer,
                            0,
                            AMQPClass::Connection(connection::AMQPMethod::CloseOk(
                                connection::CloseOk::default(),
                            )),
                        )
                        .await?;
                        return Ok(());
                    }
                    let operation = amqp_operation(&frame);
                    let started = Instant::now();
                    let handled = session
                        .handle(frame)
                        .instrument(tracing::info_span!(
                            "epoch.compat.request",
                            protocol = Protocol::Amqp091.as_str(),
                            operation = operation.as_str()
                        ))
                        .await;
                    observe_protocol(
                        metrics,
                        Protocol::Amqp091,
                        operation,
                        if handled.is_ok() { Outcome::Success } else { Outcome::ClientError },
                        started.elapsed(),
                    );
                    handled?
                }
                _ = poll.tick(), if session.has_consumers() => session.poll_consumers().await?,
            };
            for response in responses {
                write_frame(&mut writer, &response).await?;
            }
        }
    }
    .await;
    reader_task.abort();
    result
}

fn amqp_operation(frame: &AMQPFrame) -> ProtocolOperation {
    match frame {
        AMQPFrame::Method(_, AMQPClass::Connection(_) | AMQPClass::Channel(_)) => {
            ProtocolOperation::Connect
        }
        AMQPFrame::Method(_, AMQPClass::Queue(_) | AMQPClass::Exchange(_)) => {
            ProtocolOperation::Declare
        }
        AMQPFrame::Method(_, AMQPClass::Basic(method)) => match method {
            basic::AMQPMethod::Publish(_) => ProtocolOperation::Publish,
            basic::AMQPMethod::Get(_) | basic::AMQPMethod::Consume(_) => ProtocolOperation::Consume,
            basic::AMQPMethod::Ack(_)
            | basic::AMQPMethod::Nack(_)
            | basic::AMQPMethod::Reject(_) => ProtocolOperation::Settle,
            _ => ProtocolOperation::Other,
        },
        AMQPFrame::Header(_, _) | AMQPFrame::Body(_, _) => ProtocolOperation::Publish,
        _ => ProtocolOperation::Other,
    }
}

#[derive(Debug)]
struct PendingPublish {
    exchange: String,
    routing_key: String,
    mandatory: bool,
    properties: Option<BasicProperties>,
    expected_body_size: Option<u64>,
    body: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ExchangeKind {
    Direct,
    Fanout,
    Topic,
    Headers,
}

impl ExchangeKind {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "direct" => Some(Self::Direct),
            "fanout" => Some(Self::Fanout),
            "topic" => Some(Self::Topic),
            "headers" => Some(Self::Headers),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum HeaderMatch {
    All,
    Any,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HeaderBinding {
    mode: HeaderMatch,
    values: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    exchange: String,
    routing_key: String,
    queue: String,
    headers: Option<HeaderBinding>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExchangeDefinition {
    kind: ExchangeKind,
    durable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueueDeadLetter {
    exchange: String,
    routing_key: String,
    target_queue: String,
    durable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueueDeadLetterRequest {
    exchange: String,
    routing_key: String,
}

#[derive(Debug, Clone)]
struct Topology {
    exchanges: BTreeMap<String, ExchangeDefinition>,
    bindings: BTreeSet<Binding>,
    dead_letters: BTreeMap<String, QueueDeadLetter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedTopology {
    version: u16,
    exchanges: BTreeMap<String, ExchangeDefinition>,
    bindings: BTreeSet<Binding>,
    dead_letters: BTreeMap<String, QueueDeadLetter>,
}

impl Default for Topology {
    fn default() -> Self {
        Self {
            exchanges: BTreeMap::from([
                (
                    "amq.direct".into(),
                    ExchangeDefinition {
                        kind: ExchangeKind::Direct,
                        durable: true,
                    },
                ),
                (
                    "amq.fanout".into(),
                    ExchangeDefinition {
                        kind: ExchangeKind::Fanout,
                        durable: true,
                    },
                ),
                (
                    "amq.topic".into(),
                    ExchangeDefinition {
                        kind: ExchangeKind::Topic,
                        durable: true,
                    },
                ),
                (
                    "amq.headers".into(),
                    ExchangeDefinition {
                        kind: ExchangeKind::Headers,
                        durable: true,
                    },
                ),
            ]),
            bindings: BTreeSet::new(),
            dead_letters: BTreeMap::new(),
        }
    }
}

impl PersistedTopology {
    fn decode(entry: Option<&CacheEntry>) -> Result<Topology> {
        let Some(entry) = entry else {
            return Ok(Topology::default());
        };
        let bytes = match &entry.value {
            CacheValue::Blob(value) => value.as_slice(),
            CacheValue::String(value) => value.as_bytes(),
            _ => bail!("AMQP durable topology has an incompatible Cache value type"),
        };
        let persisted: Self =
            serde_json::from_slice(bytes).context("AMQP durable topology document is malformed")?;
        if persisted.version != TOPOLOGY_VERSION
            || persisted.exchanges.len() > MAX_TOPOLOGY_ENTRIES
            || persisted.bindings.len() > MAX_TOPOLOGY_ENTRIES
            || persisted.dead_letters.len() > MAX_TOPOLOGY_ENTRIES
            || persisted
                .exchanges
                .values()
                .any(|exchange| !exchange.durable)
            || persisted
                .dead_letters
                .values()
                .any(|declaration| !declaration.durable)
        {
            bail!("AMQP durable topology document violates its version or bounds");
        }
        let mut topology = Topology::default();
        for (name, definition) in persisted.exchanges {
            validate_topology_name(&name, "exchange")?;
            if name.starts_with("amq.") || topology.exchanges.insert(name, definition).is_some() {
                bail!("AMQP durable topology overrides a reserved exchange");
            }
        }
        for binding in persisted.bindings {
            validate_topology_name(&binding.exchange, "exchange")?;
            validate_topology_name(&binding.queue, "queue")?;
            if !topology.exchanges.contains_key(&binding.exchange) {
                bail!("AMQP durable binding references an unknown exchange");
            }
            topology.bindings.insert(binding);
        }
        for (queue, declaration) in persisted.dead_letters {
            validate_topology_name(&queue, "queue")?;
            validate_topology_name(&declaration.target_queue, "queue")?;
            if !declaration.exchange.is_empty()
                && !topology.exchanges.contains_key(&declaration.exchange)
            {
                bail!("AMQP durable dead-letter declaration references an unknown exchange");
            }
            topology.dead_letters.insert(queue, declaration);
        }
        validate_dead_letter_routes(&topology)?;
        Ok(topology)
    }

    fn from_topology(topology: &Topology) -> Self {
        let exchanges = topology
            .exchanges
            .iter()
            .filter(|(name, definition)| !name.starts_with("amq.") && definition.durable)
            .map(|(name, definition)| (name.clone(), *definition))
            .collect::<BTreeMap<_, _>>();
        let bindings = topology
            .bindings
            .iter()
            .filter(|binding| {
                topology
                    .exchanges
                    .get(&binding.exchange)
                    .is_some_and(|definition| definition.durable)
            })
            .cloned()
            .collect();
        let dead_letters = topology
            .dead_letters
            .iter()
            .filter(|(_, declaration)| declaration.durable)
            .map(|(queue, declaration)| (queue.clone(), declaration.clone()))
            .collect();
        Self {
            version: TOPOLOGY_VERSION,
            exchanges,
            bindings,
            dead_letters,
        }
    }

    fn encode(topology: &Topology) -> Result<Vec<u8>> {
        validate_dead_letter_routes(topology)?;
        let bytes = serde_json::to_vec(&Self::from_topology(topology))
            .context("encode AMQP durable topology")?;
        if bytes.len() > MAX_MESSAGE_BYTES {
            bail!("AMQP durable topology exceeds the storage limit");
        }
        Ok(bytes)
    }
}

impl Topology {
    fn merge_ephemeral(mut self, local: &Self) -> Self {
        for (name, definition) in &local.exchanges {
            if !definition.durable {
                self.exchanges.insert(name.clone(), *definition);
            }
        }
        self.bindings.extend(
            local
                .bindings
                .iter()
                .filter(|binding| {
                    local
                        .exchanges
                        .get(&binding.exchange)
                        .is_some_and(|definition| !definition.durable)
                })
                .cloned(),
        );
        self.dead_letters.extend(
            local
                .dead_letters
                .iter()
                .filter(|(_, declaration)| !declaration.durable)
                .map(|(queue, declaration)| (queue.clone(), declaration.clone())),
        );
        self
    }
}

fn validate_topology_name(value: &str, kind: &str) -> Result<()> {
    if value.is_empty() || value.len() > 255 || value.chars().any(char::is_control) {
        bail!("AMQP {kind} name is empty, too long, or contains control characters");
    }
    Ok(())
}

fn validate_dead_letter_routes(topology: &Topology) -> Result<()> {
    for declaration in topology.dead_letters.values() {
        if declaration.exchange.is_empty() {
            if declaration.routing_key != declaration.target_queue {
                bail!("AMQP default dead-letter route changed its native target");
            }
            continue;
        }
        let targets = resolve_topology_queues(
            topology,
            &declaration.exchange,
            &declaration.routing_key,
            &BTreeMap::new(),
        )?;
        if targets.as_slice() != [declaration.target_queue.as_str()] {
            bail!("AMQP named dead-letter route no longer resolves to its native target");
        }
    }
    Ok(())
}

#[derive(Debug)]
struct DeliveryLease {
    queue: String,
    consumer: String,
    lease_token: String,
}

#[derive(Debug, Clone)]
struct ConsumerState {
    queue: String,
    tag: String,
    no_ack: bool,
}

#[derive(Debug)]
struct ChannelState {
    confirms: bool,
    publish_sequence: u64,
    delivery_sequence: u64,
    prefetch: u16,
    pending_publish: Option<PendingPublish>,
    unacked: BTreeMap<u64, DeliveryLease>,
    consumers: BTreeMap<String, ConsumerState>,
}

impl Default for ChannelState {
    fn default() -> Self {
        Self {
            confirms: false,
            publish_sequence: 0,
            delivery_sequence: 0,
            prefetch: 1,
            pending_publish: None,
            unacked: BTreeMap::new(),
            consumers: BTreeMap::new(),
        }
    }
}

#[derive(Debug)]
struct Session<B> {
    backend: Arc<B>,
    channels: BTreeMap<u16, ChannelState>,
    topology: Arc<Mutex<Topology>>,
    topology_cache: String,
}

impl<B: CompatibilityBackend> Session<B> {
    #[cfg(test)]
    fn new(backend: Arc<B>) -> Self {
        Self::with_topology_cache(
            backend,
            Arc::new(Mutex::new(Topology::default())),
            "sessions".into(),
        )
    }

    #[cfg(test)]
    fn with_topology(backend: Arc<B>, topology: Arc<Mutex<Topology>>) -> Self {
        Self::with_topology_cache(backend, topology, "sessions".into())
    }

    fn with_topology_cache(
        backend: Arc<B>,
        topology: Arc<Mutex<Topology>>,
        topology_cache: String,
    ) -> Self {
        Self {
            backend,
            channels: BTreeMap::new(),
            topology,
            topology_cache,
        }
    }

    async fn current_topology(&self) -> Result<Topology> {
        let snapshot = self
            .backend
            .cache_snapshot(&self.topology_cache, &[TOPOLOGY_KEY.to_owned()])
            .await?;
        let persisted =
            PersistedTopology::decode(snapshot.entries.get(TOPOLOGY_KEY).and_then(Option::as_ref))?;
        let local = self.topology.lock().unwrap().clone();
        let merged = persisted.merge_ephemeral(&local);
        *self.topology.lock().unwrap() = merged.clone();
        Ok(merged)
    }

    async fn mutate_durable_topology<R, F>(&self, mut mutate: F) -> Result<R>
    where
        F: FnMut(&mut Topology) -> Result<R>,
    {
        for attempt in 0..MAX_TOPOLOGY_UPDATE_ATTEMPTS {
            let snapshot = self
                .backend
                .cache_snapshot(&self.topology_cache, &[TOPOLOGY_KEY.to_owned()])
                .await?;
            let persisted = PersistedTopology::decode(
                snapshot.entries.get(TOPOLOGY_KEY).and_then(Option::as_ref),
            )?;
            let local = self.topology.lock().unwrap().clone();
            let mut topology = persisted.merge_ephemeral(&local);
            let result = mutate(&mut topology)?;
            let document = PersistedTopology::encode(&topology)?;
            let mutation = CacheAtomicMutation::Put {
                key: TOPOLOGY_KEY.to_owned(),
                value: CacheValue::Blob(document),
                ttl_ms: None,
                storage_class: CacheStorageClass::Memory,
            };
            match self
                .backend
                .cache_compare_and_apply(&self.topology_cache, snapshot.revision, &[mutation])
                .await
            {
                Ok(()) => {
                    *self.topology.lock().unwrap() = topology;
                    return Ok(result);
                }
                Err(BackendError::Conflict) if attempt + 1 < MAX_TOPOLOGY_UPDATE_ATTEMPTS => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(BackendError::Conflict.into())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the exhaustive protocol state transition keeps advertised AMQP methods visible"
    )]
    async fn handle(&mut self, frame: AMQPFrame) -> Result<Vec<AMQPFrame>> {
        let channel_id = frame.channel_id();
        if channel_id == 0 {
            return match frame {
                AMQPFrame::Heartbeat => Ok(vec![AMQPFrame::Heartbeat]),
                _ => bail!("unsupported AMQP connection-level frame"),
            };
        }
        match frame {
            AMQPFrame::Method(id, AMQPClass::Channel(channel::AMQPMethod::Open(_))) => {
                if id > MAX_CHANNELS || self.channels.contains_key(&id) {
                    bail!("invalid AMQP channel open");
                }
                self.channels.insert(id, ChannelState::default());
                Ok(vec![method_frame(
                    id,
                    AMQPClass::Channel(channel::AMQPMethod::OpenOk(channel::OpenOk::default())),
                )])
            }
            AMQPFrame::Method(id, AMQPClass::Channel(channel::AMQPMethod::Close(_))) => {
                self.channels.remove(&id);
                Ok(vec![method_frame(
                    id,
                    AMQPClass::Channel(channel::AMQPMethod::CloseOk(channel::CloseOk::default())),
                )])
            }
            AMQPFrame::Method(id, AMQPClass::Queue(queue::AMQPMethod::Declare(declare))) => {
                self.require_channel(id)?;
                let name = declare.queue.to_string();
                validate_topology_name(&name, "queue")?;
                if !self.backend.queue_exists(&name).await? {
                    bail!("AMQP queue does not map to a provisioned Epoch Queue");
                }
                if !declare.passive && (declare.exclusive || declare.auto_delete) {
                    bail!("exclusive and auto-delete AMQP queues are unsupported");
                }
                let topology = self.current_topology().await?;
                let request = parse_queue_dead_letter_arguments(&declare.arguments)?;
                let requested = if let Some(request) = request {
                    let targets = if request.exchange.is_empty() {
                        vec![request.routing_key.clone()]
                    } else {
                        let definition = topology
                            .exchanges
                            .get(&request.exchange)
                            .context("AMQP dead-letter exchange does not exist")?;
                        if definition.kind == ExchangeKind::Headers {
                            bail!("headers exchanges cannot be used as an AMQP dead-letter target");
                        }
                        resolve_topology_queues(
                            &topology,
                            &request.exchange,
                            &request.routing_key,
                            &BTreeMap::new(),
                        )?
                    };
                    let [target_queue] = targets.as_slice() else {
                        bail!("AMQP dead-letter route must resolve to exactly one native Queue");
                    };
                    let configured_target = self.backend.queue_dead_letter_target(&name).await?;
                    if configured_target.as_deref() != Some(target_queue.as_str()) {
                        bail!(
                            "AMQP dead-letter arguments do not match the provisioned Epoch Queue"
                        );
                    }
                    Some(QueueDeadLetter {
                        exchange: request.exchange,
                        routing_key: request.routing_key,
                        target_queue: target_queue.clone(),
                        durable: declare.durable,
                    })
                } else {
                    None
                };
                if !declare.passive {
                    match (topology.dead_letters.get(&name), &requested) {
                        (Some(existing), Some(requested)) if existing != requested => {
                            bail!("AMQP queue redeclaration changed dead-letter arguments");
                        }
                        (Some(_), None) => {
                            bail!("AMQP queue redeclaration removed dead-letter arguments");
                        }
                        (None, Some(requested)) if requested.durable => {
                            self.mutate_durable_topology(|topology| {
                                match topology.dead_letters.get(&name) {
                                    Some(existing) if existing != requested => {
                                        bail!(
                                            "AMQP queue redeclaration changed dead-letter arguments"
                                        );
                                    }
                                    None => {
                                        if topology.dead_letters.len() >= MAX_TOPOLOGY_ENTRIES {
                                            bail!("AMQP dead-letter declaration limit exceeded");
                                        }
                                        topology
                                            .dead_letters
                                            .insert(name.clone(), requested.clone());
                                    }
                                    Some(_) => {}
                                }
                                Ok(())
                            })
                            .await?;
                        }
                        (None, Some(requested)) => {
                            let mut topology = self.topology.lock().unwrap();
                            if topology.dead_letters.len() >= MAX_TOPOLOGY_ENTRIES {
                                bail!("AMQP dead-letter declaration limit exceeded");
                            }
                            topology
                                .dead_letters
                                .insert(name.clone(), requested.clone());
                        }
                        _ => {}
                    }
                }
                if declare.nowait {
                    return Ok(Vec::new());
                }
                Ok(vec![method_frame(
                    id,
                    AMQPClass::Queue(queue::AMQPMethod::DeclareOk(queue::DeclareOk {
                        queue: declare.queue,
                        message_count: 0,
                        consumer_count: 0,
                    })),
                )])
            }
            AMQPFrame::Method(id, AMQPClass::Exchange(exchange::AMQPMethod::Declare(declare))) => {
                self.require_channel(id)?;
                let name = declare.exchange.to_string();
                validate_topology_name(&name, "exchange")?;
                if declare.passive {
                    if name.is_empty() {
                        bail!("the default AMQP exchange cannot be declared");
                    }
                    let topology = self.current_topology().await?;
                    if !topology.exchanges.contains_key(&name) {
                        bail!("AMQP exchange does not exist");
                    }
                    if declare.nowait {
                        return Ok(Vec::new());
                    }
                    return Ok(vec![method_frame(
                        id,
                        AMQPClass::Exchange(exchange::AMQPMethod::DeclareOk(
                            exchange::DeclareOk::default(),
                        )),
                    )]);
                }
                let Some(kind) = ExchangeKind::parse(declare.kind.as_str()) else {
                    bail!("unsupported AMQP exchange type");
                };
                if name.is_empty()
                    || declare.internal
                    || declare.auto_delete
                    || !declare.arguments.inner().is_empty()
                {
                    bail!("unsupported AMQP exchange declaration options");
                }
                let requested = ExchangeDefinition {
                    kind,
                    durable: declare.durable,
                };
                let topology = self.current_topology().await?;
                match topology.exchanges.get(&name) {
                    Some(existing) if *existing != requested => {
                        bail!("AMQP exchange redeclaration changed its type or durability");
                    }
                    None if declare.passive => bail!("AMQP exchange does not exist"),
                    None if name.starts_with("amq.") => {
                        bail!("AMQP reserved exchange does not exist");
                    }
                    None if declare.durable => {
                        self.mutate_durable_topology(|topology| {
                            match topology.exchanges.get(&name) {
                                Some(existing) if *existing != requested => {
                                    bail!(
                                        "AMQP exchange redeclaration changed its type or durability"
                                    );
                                }
                                None => {
                                    if topology.exchanges.len() >= MAX_TOPOLOGY_ENTRIES {
                                        bail!("AMQP exchange limit exceeded");
                                    }
                                    topology.exchanges.insert(name.clone(), requested);
                                }
                                Some(_) => {}
                            }
                            Ok(())
                        })
                        .await?;
                    }
                    None => {
                        let mut topology = self.topology.lock().unwrap();
                        if topology.exchanges.len() >= MAX_TOPOLOGY_ENTRIES {
                            bail!("AMQP exchange limit exceeded");
                        }
                        topology.exchanges.insert(name, requested);
                    }
                    Some(_) => {}
                }
                if declare.nowait {
                    return Ok(Vec::new());
                }
                Ok(vec![method_frame(
                    id,
                    AMQPClass::Exchange(exchange::AMQPMethod::DeclareOk(
                        exchange::DeclareOk::default(),
                    )),
                )])
            }
            AMQPFrame::Method(id, AMQPClass::Exchange(exchange::AMQPMethod::Delete(delete))) => {
                self.require_channel(id)?;
                let name = delete.exchange.to_string();
                if name.starts_with("amq.") || name.is_empty() {
                    bail!("built-in AMQP exchanges cannot be deleted");
                }
                let topology = self.current_topology().await?;
                let definition = topology
                    .exchanges
                    .get(&name)
                    .copied()
                    .context("AMQP exchange does not exist")?;
                let remove = |topology: &mut Topology| -> Result<()> {
                    if !topology.exchanges.contains_key(&name) {
                        bail!("AMQP exchange does not exist");
                    }
                    if delete.if_unused
                        && topology
                            .bindings
                            .iter()
                            .any(|binding| binding.exchange == name)
                    {
                        bail!("AMQP exchange is still in use");
                    }
                    topology.exchanges.remove(&name);
                    topology.bindings.retain(|binding| binding.exchange != name);
                    topology
                        .dead_letters
                        .retain(|_, declaration| declaration.exchange != name);
                    Ok(())
                };
                if definition.durable {
                    self.mutate_durable_topology(remove).await?;
                } else {
                    remove(&mut self.topology.lock().unwrap())?;
                }
                if delete.nowait {
                    return Ok(Vec::new());
                }
                Ok(vec![method_frame(
                    id,
                    AMQPClass::Exchange(exchange::AMQPMethod::DeleteOk(
                        exchange::DeleteOk::default(),
                    )),
                )])
            }
            AMQPFrame::Method(id, AMQPClass::Queue(queue::AMQPMethod::Bind(bind))) => {
                self.require_channel(id)?;
                if !self.backend.queue_exists(bind.queue.as_str()).await? {
                    bail!("AMQP queue binding targets an unknown Queue");
                }
                validate_topology_name(bind.queue.as_str(), "queue")?;
                let topology = self.current_topology().await?;
                let definition = topology
                    .exchanges
                    .get(bind.exchange.as_str())
                    .copied()
                    .context("AMQP exchange does not exist")?;
                let binding = Binding {
                    exchange: bind.exchange.to_string(),
                    routing_key: bind.routing_key.to_string(),
                    queue: bind.queue.to_string(),
                    headers: parse_binding_headers(definition.kind, &bind.arguments)?,
                };
                if definition.durable {
                    self.mutate_durable_topology(|topology| {
                        let current = topology
                            .exchanges
                            .get(&binding.exchange)
                            .context("AMQP exchange does not exist")?;
                        if *current != definition {
                            bail!("AMQP exchange changed during binding");
                        }
                        if topology.bindings.len() >= MAX_TOPOLOGY_ENTRIES
                            && !topology.bindings.contains(&binding)
                        {
                            bail!("AMQP binding limit exceeded");
                        }
                        topology.bindings.insert(binding.clone());
                        Ok(())
                    })
                    .await?;
                } else {
                    let mut topology = self.topology.lock().unwrap();
                    if topology.bindings.len() >= MAX_TOPOLOGY_ENTRIES
                        && !topology.bindings.contains(&binding)
                    {
                        bail!("AMQP binding limit exceeded");
                    }
                    topology.bindings.insert(binding);
                }
                if bind.nowait {
                    return Ok(Vec::new());
                }
                Ok(vec![method_frame(
                    id,
                    AMQPClass::Queue(queue::AMQPMethod::BindOk(queue::BindOk::default())),
                )])
            }
            AMQPFrame::Method(id, AMQPClass::Queue(queue::AMQPMethod::Unbind(unbind))) => {
                self.require_channel(id)?;
                let topology = self.current_topology().await?;
                let definition = topology
                    .exchanges
                    .get(unbind.exchange.as_str())
                    .copied()
                    .context("AMQP exchange does not exist")?;
                let binding = Binding {
                    exchange: unbind.exchange.to_string(),
                    routing_key: unbind.routing_key.to_string(),
                    queue: unbind.queue.to_string(),
                    headers: parse_binding_headers(definition.kind, &unbind.arguments)?,
                };
                if definition.durable {
                    self.mutate_durable_topology(|topology| {
                        if !topology.bindings.remove(&binding) {
                            bail!("AMQP queue binding does not exist");
                        }
                        Ok(())
                    })
                    .await?;
                } else if !self.topology.lock().unwrap().bindings.remove(&binding) {
                    bail!("AMQP queue binding does not exist");
                }
                Ok(vec![method_frame(
                    id,
                    AMQPClass::Queue(queue::AMQPMethod::UnbindOk(queue::UnbindOk::default())),
                )])
            }
            AMQPFrame::Method(id, AMQPClass::Basic(basic::AMQPMethod::Qos(qos))) => {
                let state = self.require_channel_mut(id)?;
                if qos.global {
                    bail!("unsupported AMQP QoS mode");
                }
                state.prefetch = if qos.prefetch_count == 0 {
                    u16::MAX
                } else {
                    qos.prefetch_count
                };
                Ok(vec![method_frame(
                    id,
                    AMQPClass::Basic(basic::AMQPMethod::QosOk(basic::QosOk::default())),
                )])
            }
            AMQPFrame::Method(id, AMQPClass::Basic(basic::AMQPMethod::Consume(consume))) => {
                self.require_channel(id)?;
                if consume.no_local
                    || consume.exclusive
                    || !consume.arguments.inner().is_empty()
                    || !self.backend.queue_exists(consume.queue.as_str()).await?
                {
                    bail!("unsupported AMQP consumer options or queue");
                }
                let tag = if consume.consumer_tag.as_str().is_empty() {
                    format!(
                        "epoch-{id}-{}",
                        self.require_channel(id)?.consumers.len() + 1
                    )
                } else {
                    consume.consumer_tag.to_string()
                };
                let state = self.require_channel_mut(id)?;
                if state.consumers.contains_key(&tag) {
                    bail!("duplicate AMQP consumer tag");
                }
                state.consumers.insert(
                    tag.clone(),
                    ConsumerState {
                        queue: consume.queue.to_string(),
                        tag: tag.clone(),
                        no_ack: consume.no_ack,
                    },
                );
                if consume.nowait {
                    return Ok(Vec::new());
                }
                Ok(vec![method_frame(
                    id,
                    AMQPClass::Basic(basic::AMQPMethod::ConsumeOk(basic::ConsumeOk {
                        consumer_tag: tag.into(),
                    })),
                )])
            }
            AMQPFrame::Method(id, AMQPClass::Basic(basic::AMQPMethod::Cancel(cancel))) => {
                let state = self.require_channel_mut(id)?;
                if state
                    .consumers
                    .remove(cancel.consumer_tag.as_str())
                    .is_none()
                {
                    bail!("unknown AMQP consumer tag");
                }
                if cancel.nowait {
                    return Ok(Vec::new());
                }
                Ok(vec![method_frame(
                    id,
                    AMQPClass::Basic(basic::AMQPMethod::CancelOk(basic::CancelOk {
                        consumer_tag: cancel.consumer_tag,
                    })),
                )])
            }
            AMQPFrame::Method(id, AMQPClass::Confirm(confirm::AMQPMethod::Select(select))) => {
                self.require_channel_mut(id)?.confirms = true;
                if select.nowait {
                    return Ok(Vec::new());
                }
                Ok(vec![method_frame(
                    id,
                    AMQPClass::Confirm(confirm::AMQPMethod::SelectOk(confirm::SelectOk::default())),
                )])
            }
            AMQPFrame::Method(id, AMQPClass::Basic(basic::AMQPMethod::Publish(publish))) => {
                self.validate_publish(&publish).await?;
                let state = self.require_channel_mut(id)?;
                if state.pending_publish.is_some() {
                    bail!("interleaved AMQP publishes are unsupported");
                }
                state.pending_publish = Some(PendingPublish {
                    exchange: publish.exchange.to_string(),
                    routing_key: publish.routing_key.to_string(),
                    mandatory: publish.mandatory,
                    properties: None,
                    expected_body_size: None,
                    body: Vec::new(),
                });
                Ok(Vec::new())
            }
            AMQPFrame::Header(id, header) => {
                if header.class_id != 60
                    || usize::try_from(header.body_size).unwrap_or(usize::MAX) > MAX_MESSAGE_BYTES
                {
                    bail!("invalid AMQP content header");
                }
                let pending = self
                    .require_channel_mut(id)?
                    .pending_publish
                    .as_mut()
                    .context("AMQP content header without publish")?;
                if pending.expected_body_size.is_some() {
                    bail!("duplicate AMQP content header");
                }
                pending.expected_body_size = Some(header.body_size);
                pending.properties = Some(header.properties);
                if header.body_size == 0 {
                    self.finish_publish(id).await
                } else {
                    Ok(Vec::new())
                }
            }
            AMQPFrame::Body(id, body) => {
                let pending = self
                    .require_channel_mut(id)?
                    .pending_publish
                    .as_mut()
                    .context("AMQP content body without publish")?;
                let expected = pending
                    .expected_body_size
                    .context("AMQP content body before header")?;
                if pending.body.len().saturating_add(body.len())
                    > usize::try_from(expected).unwrap_or(usize::MAX)
                {
                    bail!("AMQP body exceeds declared size");
                }
                pending.body.extend_from_slice(&body);
                if pending.body.len() == usize::try_from(expected).unwrap_or(usize::MAX) {
                    self.finish_publish(id).await
                } else {
                    Ok(Vec::new())
                }
            }
            AMQPFrame::Method(id, AMQPClass::Basic(basic::AMQPMethod::Get(get))) => {
                self.basic_get(id, get).await
            }
            AMQPFrame::Method(id, AMQPClass::Basic(basic::AMQPMethod::Ack(ack))) => {
                self.settle(id, ack.delivery_tag, ack.multiple, true)
                    .await?;
                Ok(Vec::new())
            }
            AMQPFrame::Method(id, AMQPClass::Basic(basic::AMQPMethod::Reject(reject))) => {
                self.reject(id, reject.delivery_tag, false, reject.requeue)
                    .await?;
                Ok(Vec::new())
            }
            AMQPFrame::Method(id, AMQPClass::Basic(basic::AMQPMethod::Nack(nack))) => {
                self.reject(id, nack.delivery_tag, nack.multiple, nack.requeue)
                    .await?;
                Ok(Vec::new())
            }
            AMQPFrame::Heartbeat => Ok(vec![AMQPFrame::Heartbeat]),
            _ => bail!("unsupported AMQP method; see Epoch compatibility matrix"),
        }
    }

    fn require_channel(&self, id: u16) -> Result<&ChannelState> {
        self.channels.get(&id).context("AMQP channel is not open")
    }

    fn require_channel_mut(&mut self, id: u16) -> Result<&mut ChannelState> {
        self.channels
            .get_mut(&id)
            .context("AMQP channel is not open")
    }

    fn has_consumers(&self) -> bool {
        self.channels
            .values()
            .any(|channel| !channel.consumers.is_empty())
    }

    async fn poll_consumers(&mut self) -> Result<Vec<AMQPFrame>> {
        let consumers = self
            .channels
            .iter()
            .flat_map(|(channel_id, state)| {
                state
                    .consumers
                    .values()
                    .filter(|consumer| {
                        consumer.no_ack || state.unacked.len() < usize::from(state.prefetch)
                    })
                    .map(|consumer| (*channel_id, consumer.clone()))
            })
            .collect::<Vec<_>>();
        let mut frames = Vec::new();
        for (channel_id, consumer) in consumers {
            let mut deliveries = self
                .backend
                .queue_acquire(&consumer.queue, &consumer.tag, 1, 30_000)
                .await?;
            if let Some(delivery) = deliveries.pop() {
                frames.extend(self.consumer_delivery_frames(channel_id, &consumer, delivery)?);
            }
        }
        Ok(frames)
    }

    async fn validate_publish(&self, publish: &basic::Publish) -> Result<()> {
        if publish.immediate {
            bail!("AMQP immediate publishing is unsupported");
        }
        if !publish.exchange.as_str().is_empty()
            && !self
                .current_topology()
                .await?
                .exchanges
                .contains_key(publish.exchange.as_str())
        {
            bail!("AMQP exchange does not exist");
        }
        Ok(())
    }

    async fn resolve_publish_queues(
        &self,
        exchange: &str,
        routing_key: &str,
        headers: &BTreeMap<String, String>,
    ) -> Result<Vec<String>> {
        if exchange.is_empty() {
            return if self.backend.queue_exists(routing_key).await? {
                Ok(vec![routing_key.to_owned()])
            } else {
                Ok(Vec::new())
            };
        }
        let topology = self.current_topology().await?;
        resolve_topology_queues(&topology, exchange, routing_key, headers)
    }

    async fn finish_publish(&mut self, id: u16) -> Result<Vec<AMQPFrame>> {
        let (pending, confirms) = {
            let state = self.require_channel_mut(id)?;
            (
                state
                    .pending_publish
                    .take()
                    .context("AMQP publish is missing")?,
                state.confirms,
            )
        };
        let properties = pending.properties.unwrap_or_default();
        let expiration = properties.expiration().as_ref().map(ToString::to_string);
        let ttl_ms = expiration.as_deref().map(parse_expiration).transpose()?;
        let headers = string_headers(&properties)?;
        let queues = self
            .resolve_publish_queues(&pending.exchange, &pending.routing_key, &headers)
            .await?;
        let message = QueueMessage {
            body: pending.body.clone(),
            content_type: properties.content_type().as_ref().map(ToString::to_string),
            correlation_id: properties
                .correlation_id()
                .as_ref()
                .map(ToString::to_string),
            reply_to: properties.reply_to().as_ref().map(ToString::to_string),
            headers,
            exchange: Some(pending.exchange.clone()),
            routing_key: Some(pending.routing_key.clone()),
            expiration,
            ttl_ms,
        };
        let topology = self.current_topology().await?;
        for queue in &queues {
            let mut message = message.clone();
            if let Some(dead_letter) = topology.dead_letters.get(queue) {
                message
                    .headers
                    .insert(DLX_EXCHANGE_HEADER.into(), dead_letter.exchange.clone());
                message.headers.insert(
                    DLX_ROUTING_KEY_HEADER.into(),
                    dead_letter.routing_key.clone(),
                );
            }
            self.backend.queue_publish(queue, message).await?;
        }
        let mut frames = Vec::new();
        if queues.is_empty() && pending.mandatory {
            frames.extend([
                method_frame(
                    id,
                    AMQPClass::Basic(basic::AMQPMethod::Return(basic::Return {
                        reply_code: 312,
                        reply_text: "NO_ROUTE".into(),
                        exchange: pending.exchange.into(),
                        routing_key: pending.routing_key.into(),
                    })),
                ),
                AMQPFrame::Header(
                    id,
                    AMQPContentHeader {
                        class_id: 60,
                        body_size: u64::try_from(pending.body.len()).unwrap_or(u64::MAX),
                        properties,
                    },
                ),
                AMQPFrame::Body(id, pending.body),
            ]);
        }
        if confirms {
            let state = self.require_channel_mut(id)?;
            state.publish_sequence = state.publish_sequence.saturating_add(1);
            frames.push(method_frame(
                id,
                AMQPClass::Basic(basic::AMQPMethod::Ack(basic::Ack {
                    delivery_tag: state.publish_sequence,
                    multiple: false,
                })),
            ));
        }
        Ok(frames)
    }

    async fn basic_get(&mut self, id: u16, get: basic::Get) -> Result<Vec<AMQPFrame>> {
        let consumer = format!("amqp-get-{id}");
        let deliveries = self
            .backend
            .queue_acquire(get.queue.as_str(), &consumer, 1, 30_000)
            .await?;
        let Some(delivery) = deliveries.into_iter().next() else {
            return Ok(vec![method_frame(
                id,
                AMQPClass::Basic(basic::AMQPMethod::GetEmpty(basic::GetEmpty::default())),
            )]);
        };
        if get.no_ack {
            self.backend
                .queue_ack(get.queue.as_str(), &consumer, &delivery.lease_token)
                .await?;
        }
        self.delivery_frames(id, get.queue.as_str(), consumer, delivery, get.no_ack)
    }

    fn delivery_frames(
        &mut self,
        id: u16,
        queue: &str,
        consumer: String,
        delivery: QueueDelivery,
        no_ack: bool,
    ) -> Result<Vec<AMQPFrame>> {
        let state = self.require_channel_mut(id)?;
        state.delivery_sequence = state.delivery_sequence.saturating_add(1);
        let delivery_tag = state.delivery_sequence;
        if !no_ack {
            state.unacked.insert(
                delivery_tag,
                DeliveryLease {
                    queue: queue.to_owned(),
                    consumer,
                    lease_token: delivery.lease_token,
                },
            );
        }
        let properties = delivery_properties(&delivery.message);
        let size = u64::try_from(delivery.message.body.len()).unwrap_or(u64::MAX);
        let exchange = delivery.message.exchange.clone().unwrap_or_default();
        let routing_key = delivery
            .message
            .routing_key
            .clone()
            .unwrap_or_else(|| queue.to_owned());
        Ok(vec![
            method_frame(
                id,
                AMQPClass::Basic(basic::AMQPMethod::GetOk(basic::GetOk {
                    delivery_tag,
                    redelivered: delivery.redelivered,
                    exchange: exchange.into(),
                    routing_key: routing_key.into(),
                    message_count: 0,
                })),
            ),
            AMQPFrame::Header(
                id,
                AMQPContentHeader {
                    class_id: 60,
                    body_size: size,
                    properties,
                },
            ),
            AMQPFrame::Body(id, delivery.message.body),
        ])
    }

    fn consumer_delivery_frames(
        &mut self,
        id: u16,
        consumer: &ConsumerState,
        delivery: QueueDelivery,
    ) -> Result<Vec<AMQPFrame>> {
        let state = self.require_channel_mut(id)?;
        state.delivery_sequence = state.delivery_sequence.saturating_add(1);
        let delivery_tag = state.delivery_sequence;
        if !consumer.no_ack {
            state.unacked.insert(
                delivery_tag,
                DeliveryLease {
                    queue: consumer.queue.clone(),
                    consumer: consumer.tag.clone(),
                    lease_token: delivery.lease_token,
                },
            );
        }
        let properties = delivery_properties(&delivery.message);
        let size = u64::try_from(delivery.message.body.len()).unwrap_or(u64::MAX);
        let exchange = delivery.message.exchange.clone().unwrap_or_default();
        let routing_key = delivery
            .message
            .routing_key
            .clone()
            .unwrap_or_else(|| consumer.queue.clone());
        Ok(vec![
            method_frame(
                id,
                AMQPClass::Basic(basic::AMQPMethod::Deliver(basic::Deliver {
                    consumer_tag: consumer.tag.clone().into(),
                    delivery_tag,
                    redelivered: delivery.redelivered,
                    exchange: exchange.into(),
                    routing_key: routing_key.into(),
                })),
            ),
            AMQPFrame::Header(
                id,
                AMQPContentHeader {
                    class_id: 60,
                    body_size: size,
                    properties,
                },
            ),
            AMQPFrame::Body(id, delivery.message.body),
        ])
    }

    async fn settle(&mut self, id: u16, tag: u64, multiple: bool, ack: bool) -> Result<()> {
        if !ack {
            bail!("internal settlement mode is invalid");
        }
        let leases = take_leases(self.require_channel_mut(id)?, tag, multiple)?;
        for lease in leases {
            self.backend
                .queue_ack(&lease.queue, &lease.consumer, &lease.lease_token)
                .await?;
        }
        Ok(())
    }

    async fn reject(&mut self, id: u16, tag: u64, multiple: bool, requeue: bool) -> Result<()> {
        let leases = take_leases(self.require_channel_mut(id)?, tag, multiple)?;
        for lease in leases {
            self.backend
                .queue_reject(&lease.queue, &lease.consumer, &lease.lease_token, requeue)
                .await?;
        }
        Ok(())
    }
}

fn resolve_topology_queues(
    topology: &Topology,
    exchange: &str,
    routing_key: &str,
    headers: &BTreeMap<String, String>,
) -> Result<Vec<String>> {
    let definition = topology
        .exchanges
        .get(exchange)
        .copied()
        .context("AMQP exchange does not exist")?;
    Ok(topology
        .bindings
        .iter()
        .filter(|binding| {
            binding.exchange == exchange
                && binding_matches(definition.kind, binding, routing_key, headers)
        })
        .map(|binding| binding.queue.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

fn binding_matches(
    kind: ExchangeKind,
    binding: &Binding,
    routing_key: &str,
    headers: &BTreeMap<String, String>,
) -> bool {
    match kind {
        ExchangeKind::Direct => binding.routing_key == routing_key,
        ExchangeKind::Fanout => true,
        ExchangeKind::Topic => topic_matches(&binding.routing_key, routing_key),
        ExchangeKind::Headers => {
            binding
                .headers
                .as_ref()
                .is_some_and(|binding| match binding.mode {
                    HeaderMatch::All => binding
                        .values
                        .iter()
                        .all(|(name, value)| headers.get(name) == Some(value)),
                    HeaderMatch::Any => binding
                        .values
                        .iter()
                        .any(|(name, value)| headers.get(name) == Some(value)),
                })
        }
    }
}

fn parse_binding_headers(
    kind: ExchangeKind,
    arguments: &FieldTable,
) -> Result<Option<HeaderBinding>> {
    if kind != ExchangeKind::Headers {
        if !arguments.inner().is_empty() {
            bail!("binding arguments are supported only for AMQP headers exchanges");
        }
        return Ok(None);
    }
    if arguments.inner().len() > MAX_REQUEST_ITEMS.saturating_add(1) {
        bail!("AMQP headers binding count exceeds limit");
    }
    let mut mode = None;
    let mut values = BTreeMap::new();
    for (name, value) in arguments.inner() {
        let value = amqp_string(value).context("AMQP header binding values must be strings")?;
        if name.as_str() == "x-match" {
            mode = Some(match value.as_str() {
                "all" => HeaderMatch::All,
                "any" => HeaderMatch::Any,
                _ => bail!("AMQP x-match must be all or any"),
            });
        } else {
            values.insert(name.to_string(), value);
        }
    }
    if values.is_empty() {
        bail!("AMQP headers binding requires at least one header");
    }
    Ok(Some(HeaderBinding {
        mode: mode.context("AMQP headers binding requires x-match")?,
        values,
    }))
}

fn amqp_string(value: &AMQPValue) -> Result<String> {
    match value {
        AMQPValue::ShortString(value) => Ok(value.to_string()),
        AMQPValue::LongString(value) => Ok(std::str::from_utf8(value.as_bytes())
            .context("AMQP string is not UTF-8")?
            .to_owned()),
        _ => bail!("AMQP value is not a string"),
    }
}

fn topic_matches(pattern: &str, routing_key: &str) -> bool {
    let pattern = topic_segments(pattern);
    let routing_key = topic_segments(routing_key);
    let mut reachable = vec![vec![false; routing_key.len() + 1]; pattern.len() + 1];
    reachable[0][0] = true;
    for pattern_index in 0..pattern.len() {
        for key_index in 0..=routing_key.len() {
            if !reachable[pattern_index][key_index] {
                continue;
            }
            match pattern[pattern_index] {
                "#" => {
                    reachable[pattern_index + 1][key_index] = true;
                    if key_index < routing_key.len() {
                        reachable[pattern_index][key_index + 1] = true;
                    }
                }
                "*" if key_index < routing_key.len() => {
                    reachable[pattern_index + 1][key_index + 1] = true;
                }
                literal if key_index < routing_key.len() && literal == routing_key[key_index] => {
                    reachable[pattern_index + 1][key_index + 1] = true;
                }
                _ => {}
            }
        }
    }
    reachable[pattern.len()][routing_key.len()]
}

fn topic_segments(value: &str) -> Vec<&str> {
    if value.is_empty() {
        Vec::new()
    } else {
        value.split('.').collect()
    }
}

fn parse_expiration(value: &str) -> Result<u64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("AMQP expiration must be milliseconds encoded as decimal digits");
    }
    value.parse().context("AMQP expiration exceeds u64")
}

fn parse_queue_dead_letter_arguments(
    arguments: &FieldTable,
) -> Result<Option<QueueDeadLetterRequest>> {
    if arguments.inner().is_empty() {
        return Ok(None);
    }
    if arguments.inner().len() != 2 {
        bail!("AMQP Queue supports only x-dead-letter-exchange plus x-dead-letter-routing-key");
    }
    let exchange = arguments
        .inner()
        .get("x-dead-letter-exchange")
        .context("AMQP Queue dead-letter exchange is missing")?;
    let exchange =
        amqp_string(exchange).context("AMQP Queue dead-letter exchange must be a string")?;
    if !exchange.is_empty() {
        validate_topology_name(&exchange, "dead-letter exchange")?;
    }
    let target = arguments
        .inner()
        .get("x-dead-letter-routing-key")
        .context("AMQP Queue dead-letter routing key is missing")?;
    let target =
        amqp_string(target).context("AMQP Queue dead-letter routing key must be a string")?;
    if target.is_empty() {
        bail!("AMQP Queue dead-letter routing key cannot be empty");
    }
    Ok(Some(QueueDeadLetterRequest {
        exchange,
        routing_key: target,
    }))
}

fn string_headers(properties: &BasicProperties) -> Result<BTreeMap<String, String>> {
    let Some(headers) = properties.headers() else {
        return Ok(BTreeMap::new());
    };
    if headers.inner().len() > MAX_REQUEST_ITEMS {
        bail!("AMQP header count exceeds limit");
    }
    headers
        .inner()
        .iter()
        .map(|(name, value)| {
            if name.as_str().starts_with("x-epoch-compat-") {
                bail!("reserved AMQP compatibility header");
            }
            let value = amqp_string(value).context("only AMQP string headers are supported")?;
            Ok((name.to_string(), value))
        })
        .collect()
}

fn delivery_properties(message: &QueueMessage) -> BasicProperties {
    let mut properties = BasicProperties::default();
    if let Some(content_type) = &message.content_type {
        properties = properties.with_content_type(content_type.clone().into());
    }
    if let Some(correlation_id) = &message.correlation_id {
        properties = properties.with_correlation_id(correlation_id.clone().into());
    }
    if let Some(reply_to) = &message.reply_to {
        properties = properties.with_reply_to(reply_to.clone().into());
    }
    if let Some(expiration) = &message.expiration {
        properties = properties.with_expiration(expiration.clone().into());
    }
    if !message.headers.is_empty() {
        let mut headers = FieldTable::default();
        for (name, value) in &message.headers {
            if name == DEATH_HISTORY_HEADER {
                if let Some(history) = x_death_value(value) {
                    headers.insert("x-death".into(), history);
                }
                continue;
            }
            if name.starts_with("x-epoch-compat-") {
                continue;
            }
            headers.insert(
                name.as_str().into(),
                AMQPValue::LongString(value.as_str().into()),
            );
        }
        properties = properties.with_headers(headers);
    }
    properties
}

fn x_death_value(encoded: &str) -> Option<AMQPValue> {
    let entries = serde_json::from_str::<Vec<serde_json::Value>>(encoded).ok()?;
    if entries.is_empty() || entries.len() > 32 {
        return None;
    }
    let mut deaths = Vec::with_capacity(entries.len());
    for entry in entries {
        let count = entry.get("count")?.as_u64()?;
        let count = i64::try_from(count).ok()?;
        let exchange = entry.get("exchange")?.as_str()?;
        let queue = entry.get("queue")?.as_str()?;
        let reason = entry.get("reason")?.as_str()?;
        let routing_keys = entry.get("routing_keys")?.as_array()?;
        if exchange.len() > 255
            || queue.is_empty()
            || queue.len() > 255
            || reason.is_empty()
            || reason.len() > 255
            || routing_keys.len() > 32
        {
            return None;
        }
        let routing_keys = routing_keys
            .iter()
            .map(|routing_key| {
                let routing_key = routing_key.as_str()?;
                (routing_key.len() <= 255)
                    .then(|| AMQPValue::LongString(routing_key.as_bytes().into()))
            })
            .collect::<Option<Vec<_>>>()?;
        let mut death = FieldTable::default();
        death.insert("count".into(), AMQPValue::LongLongInt(count));
        death.insert(
            "exchange".into(),
            AMQPValue::LongString(exchange.as_bytes().into()),
        );
        death.insert(
            "queue".into(),
            AMQPValue::LongString(queue.as_bytes().into()),
        );
        death.insert(
            "reason".into(),
            AMQPValue::LongString(reason.as_bytes().into()),
        );
        death.insert(
            "routing-keys".into(),
            AMQPValue::FieldArray(FieldArray::from(routing_keys)),
        );
        deaths.push(AMQPValue::FieldTable(death));
    }
    Some(AMQPValue::FieldArray(FieldArray::from(deaths)))
}

fn take_leases(state: &mut ChannelState, tag: u64, multiple: bool) -> Result<Vec<DeliveryLease>> {
    let tags = if multiple {
        state
            .unacked
            .range(..=tag)
            .map(|(tag, _)| *tag)
            .collect::<Vec<_>>()
    } else {
        vec![tag]
    };
    let leases = tags
        .into_iter()
        .map(|tag| {
            state
                .unacked
                .remove(&tag)
                .context("unknown AMQP delivery tag")
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(leases)
}

fn authenticate(config: &AmqpConfig, start_ok: &connection::StartOk) -> Result<()> {
    if start_ok.mechanism.as_str() != "PLAIN" || start_ok.locale.as_str() != "en_US" {
        bail!("unsupported AMQP authentication mechanism or locale");
    }
    let response = start_ok.response.as_bytes();
    let parts = response.split(|byte| *byte == 0).collect::<Vec<_>>();
    if parts.len() != 3
        || !parts[0].is_empty()
        || !constant_time_equal(config.username.as_bytes(), parts[1])
        || !constant_time_equal(config.password.as_bytes(), parts[2])
    {
        bail!("AMQP authentication failed");
    }
    Ok(())
}

async fn read_frame<R: AsyncRead + Unpin>(stream: &mut R) -> Result<AMQPFrame> {
    let mut header = [0_u8; 7];
    stream.read_exact(&mut header).await?;
    let size = u32::from_be_bytes(header[3..7].try_into().unwrap());
    let size = usize::try_from(size)
        .ok()
        .filter(|size| *size <= MAX_FRAME_BYTES)
        .context("AMQP frame exceeds limit")?;
    let mut bytes = Vec::with_capacity(size + 8);
    bytes.extend_from_slice(&header);
    bytes.resize(size + 8, 0);
    stream.read_exact(&mut bytes[7..]).await?;
    if bytes.last() != Some(&AMQP_FRAME_END) {
        bail!("AMQP frame terminator is invalid");
    }
    let (remaining, frame) =
        parse_frame(bytes.as_slice()).map_err(|_| anyhow::anyhow!("invalid AMQP frame"))?;
    if !remaining.is_empty() {
        bail!("AMQP frame has trailing bytes");
    }
    Ok(frame)
}

async fn write_method<W: AsyncWrite + Unpin>(
    stream: &mut W,
    channel_id: u16,
    method: AMQPClass,
) -> Result<()> {
    write_frame(stream, &method_frame(channel_id, method)).await
}

async fn write_frame<W: AsyncWrite + Unpin>(stream: &mut W, frame: &AMQPFrame) -> Result<()> {
    let generated = gen_frame::<Vec<u8>>(frame)(WriteContext::from(Vec::new()))
        .map_err(|_| anyhow::anyhow!("AMQP frame encoding failed"))?
        .into_inner()
        .0;
    if generated.len() > MAX_FRAME_BYTES + 8 {
        bail!("AMQP response frame exceeds limit");
    }
    stream.write_all(&generated).await?;
    Ok(())
}

fn method_frame(channel_id: u16, method: AMQPClass) -> AMQPFrame {
    AMQPFrame::Method(channel_id, method)
}

fn constant_time_equal(expected: &[u8], actual: &[u8]) -> bool {
    let mut difference = expected.len() ^ actual.len();
    for index in 0..expected.len().max(actual.len()) {
        difference |= usize::from(
            expected.get(index).copied().unwrap_or(0) ^ actual.get(index).copied().unwrap_or(0),
        );
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::MemoryBackend;

    #[test]
    fn configuration_debug_never_exposes_the_amqp_password() {
        let config = AmqpConfig {
            username: "epoch".into(),
            password: "amqp-super-secret".into(),
            max_connections: 8,
            heartbeat_seconds: 10,
            topology_cache: "sessions".into(),
        };
        let debug = format!("{config:?}");
        assert!(!debug.contains("amqp-super-secret"));
        assert!(debug.contains("<redacted>"));
    }

    fn method(channel_id: u16, method: basic::AMQPMethod) -> AMQPFrame {
        method_frame(channel_id, AMQPClass::Basic(method))
    }

    async fn open_channel(session: &mut Session<MemoryBackend>) {
        let response = session
            .handle(method_frame(
                1,
                AMQPClass::Channel(channel::AMQPMethod::Open(channel::Open::default())),
            ))
            .await
            .unwrap();
        assert!(matches!(
            response.as_slice(),
            [AMQPFrame::Method(
                1,
                AMQPClass::Channel(channel::AMQPMethod::OpenOk(_))
            )]
        ));
    }

    async fn publish(session: &mut Session<MemoryBackend>, body: &[u8]) -> Vec<AMQPFrame> {
        publish_to(
            session,
            "",
            "jobs",
            false,
            BasicProperties::default()
                .with_content_type("application/octet-stream".into())
                .with_correlation_id("correlation-1".into())
                .with_reply_to("replies".into()),
            body,
        )
        .await
    }

    async fn publish_to(
        session: &mut Session<MemoryBackend>,
        exchange: &str,
        routing_key: &str,
        mandatory: bool,
        properties: BasicProperties,
        body: &[u8],
    ) -> Vec<AMQPFrame> {
        assert!(
            session
                .handle(method(
                    1,
                    basic::AMQPMethod::Publish(basic::Publish {
                        exchange: exchange.into(),
                        routing_key: routing_key.into(),
                        mandatory,
                        ..basic::Publish::default()
                    }),
                ))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            session
                .handle(AMQPFrame::Header(
                    1,
                    AMQPContentHeader {
                        class_id: 60,
                        body_size: u64::try_from(body.len()).unwrap(),
                        properties,
                    },
                ))
                .await
                .unwrap()
                .is_empty()
        );
        session
            .handle(AMQPFrame::Body(1, body.to_vec()))
            .await
            .unwrap()
    }

    async fn get(session: &mut Session<MemoryBackend>) -> Vec<AMQPFrame> {
        get_from(session, "jobs", false).await
    }

    async fn get_from(
        session: &mut Session<MemoryBackend>,
        queue: &str,
        no_ack: bool,
    ) -> Vec<AMQPFrame> {
        session
            .handle(method(
                1,
                basic::AMQPMethod::Get(basic::Get {
                    queue: queue.into(),
                    no_ack,
                }),
            ))
            .await
            .unwrap()
    }

    #[test]
    fn topic_patterns_match_amqp_star_and_hash_semantics() {
        assert!(topic_matches("orders.*", "orders.created"));
        assert!(!topic_matches("orders.*", "orders.eu.created"));
        assert!(topic_matches("orders.#", "orders"));
        assert!(topic_matches("orders.#", "orders.eu.created"));
        assert!(topic_matches("#.critical", "orders.eu.critical"));
        assert!(!topic_matches("#.critical", "orders.eu.created"));
        assert!(topic_matches("#", ""));
        assert!(topic_matches("", ""));
    }

    #[test]
    fn headers_bindings_require_bounded_string_criteria_and_valid_match_mode() {
        let mut valid = FieldTable::default();
        valid.insert(
            "x-match".into(),
            AMQPValue::LongString("all".as_bytes().into()),
        );
        valid.insert(
            "tenant".into(),
            AMQPValue::LongString("acme".as_bytes().into()),
        );
        assert_eq!(
            parse_binding_headers(ExchangeKind::Headers, &valid).unwrap(),
            Some(HeaderBinding {
                mode: HeaderMatch::All,
                values: BTreeMap::from([("tenant".into(), "acme".into())]),
            })
        );
        assert!(parse_binding_headers(ExchangeKind::Direct, &valid).is_err());

        let mut missing_mode = FieldTable::default();
        missing_mode.insert(
            "tenant".into(),
            AMQPValue::LongString("acme".as_bytes().into()),
        );
        assert!(parse_binding_headers(ExchangeKind::Headers, &missing_mode).is_err());

        let mut invalid_mode = missing_mode.clone();
        invalid_mode.insert(
            "x-match".into(),
            AMQPValue::LongString("none".as_bytes().into()),
        );
        assert!(parse_binding_headers(ExchangeKind::Headers, &invalid_mode).is_err());

        let mut non_string = FieldTable::default();
        non_string.insert(
            "x-match".into(),
            AMQPValue::LongString("any".as_bytes().into()),
        );
        non_string.insert("priority".into(), AMQPValue::LongInt(10));
        assert!(parse_binding_headers(ExchangeKind::Headers, &non_string).is_err());
    }

    #[test]
    fn queue_dead_letter_arguments_accept_default_and_named_exchange_routes() {
        assert_eq!(
            parse_queue_dead_letter_arguments(&FieldTable::default()).unwrap(),
            None
        );
        let mut valid = FieldTable::default();
        valid.insert(
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString(Vec::new().into()),
        );
        valid.insert(
            "x-dead-letter-routing-key".into(),
            AMQPValue::LongString("failed-jobs".as_bytes().into()),
        );
        assert_eq!(
            parse_queue_dead_letter_arguments(&valid).unwrap(),
            Some(QueueDeadLetterRequest {
                exchange: String::new(),
                routing_key: "failed-jobs".into(),
            })
        );

        let mut named_exchange = valid.clone();
        named_exchange.insert(
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString("events.failed".as_bytes().into()),
        );
        assert_eq!(
            parse_queue_dead_letter_arguments(&named_exchange).unwrap(),
            Some(QueueDeadLetterRequest {
                exchange: "events.failed".into(),
                routing_key: "failed-jobs".into(),
            })
        );

        let mut unknown = valid.clone();
        unknown.insert(
            "x-message-ttl".into(),
            AMQPValue::LongString("1000".as_bytes().into()),
        );
        assert!(parse_queue_dead_letter_arguments(&unknown).is_err());
    }

    #[test]
    fn death_history_becomes_a_bounded_rabbitmq_x_death_table_array() {
        let history = serde_json::json!([{
            "count":2,
            "exchange":"orders",
            "queue":"jobs",
            "reason":"rejected",
            "routing_keys":["orders.created"]
        }]);
        let AMQPValue::FieldArray(deaths) = x_death_value(&history.to_string()).unwrap() else {
            panic!("x-death must be a field array");
        };
        let [AMQPValue::FieldTable(death)] = deaths.as_slice() else {
            panic!("x-death must contain one table");
        };
        assert_eq!(death.inner().get("count"), Some(&AMQPValue::LongLongInt(2)));
        assert!(matches!(
            death.inner().get("queue"),
            Some(AMQPValue::LongString(value)) if value.as_bytes() == b"jobs"
        ));
    }

    async fn bind_headers(
        session: &mut Session<MemoryBackend>,
        queue_name: &str,
        mode: &str,
        criteria: &[(&str, &str)],
    ) {
        let mut arguments = FieldTable::default();
        arguments.insert(
            "x-match".into(),
            AMQPValue::LongString(mode.as_bytes().into()),
        );
        for (name, value) in criteria {
            arguments.insert(
                (*name).into(),
                AMQPValue::LongString(value.as_bytes().into()),
            );
        }
        session
            .handle(method_frame(
                1,
                AMQPClass::Queue(queue::AMQPMethod::Bind(queue::Bind {
                    queue: queue_name.into(),
                    exchange: "events.headers".into(),
                    arguments,
                    ..queue::Bind::default()
                })),
            ))
            .await
            .unwrap();
    }

    async fn declare_headers_exchange(session: &mut Session<MemoryBackend>) {
        session
            .handle(method_frame(
                1,
                AMQPClass::Exchange(exchange::AMQPMethod::Declare(exchange::Declare {
                    exchange: "events.headers".into(),
                    kind: "headers".into(),
                    ..exchange::Declare::default()
                })),
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn routes_headers_exchange_with_all_and_any_semantics() {
        let backend = Arc::new(MemoryBackend::with_resources(
            "sessions", "events", 2, "jobs",
        ));
        backend.add_queue("audit");
        let mut session = Session::new(backend);
        open_channel(&mut session).await;
        declare_headers_exchange(&mut session).await;

        bind_headers(
            &mut session,
            "jobs",
            "all",
            &[("tenant", "acme"), ("format", "json")],
        )
        .await;
        bind_headers(
            &mut session,
            "audit",
            "any",
            &[("tenant", "acme"), ("priority", "high")],
        )
        .await;

        let mut matching = FieldTable::default();
        matching.insert(
            "tenant".into(),
            AMQPValue::LongString("acme".as_bytes().into()),
        );
        matching.insert(
            "format".into(),
            AMQPValue::LongString("json".as_bytes().into()),
        );
        assert!(
            publish_to(
                &mut session,
                "events.headers",
                "routing-key-is-ignored",
                true,
                BasicProperties::default().with_headers(matching),
                b"both",
            )
            .await
            .is_empty()
        );
        for queue in ["jobs", "audit"] {
            assert!(matches!(
                get_from(&mut session, queue, true).await.as_slice(),
                [.., AMQPFrame::Body(1, body)] if body == b"both"
            ));
        }

        let mut any_only = FieldTable::default();
        any_only.insert(
            "priority".into(),
            AMQPValue::LongString("high".as_bytes().into()),
        );
        assert!(
            publish_to(
                &mut session,
                "events.headers",
                "",
                true,
                BasicProperties::default().with_headers(any_only),
                b"audit-only",
            )
            .await
            .is_empty()
        );
        assert!(matches!(
            get_from(&mut session, "jobs", true).await.as_slice(),
            [AMQPFrame::Method(
                1,
                AMQPClass::Basic(basic::AMQPMethod::GetEmpty(_))
            )]
        ));
        assert!(matches!(
            get_from(&mut session, "audit", true).await.as_slice(),
            [.., AMQPFrame::Body(1, body)] if body == b"audit-only"
        ));

        let returned = publish_to(
            &mut session,
            "events.headers",
            "",
            true,
            BasicProperties::default(),
            b"no-match",
        )
        .await;
        assert!(matches!(
            returned.as_slice(),
            [
                AMQPFrame::Method(1, AMQPClass::Basic(basic::AMQPMethod::Return(value))),
                AMQPFrame::Header(1, _),
                AMQPFrame::Body(1, body),
            ] if value.reply_code == 312 && body == b"no-match"
        ));
    }

    #[tokio::test]
    async fn durable_exchange_and_binding_survive_independent_gateway_state() {
        let backend = Arc::new(MemoryBackend::with_resources(
            "sessions", "events", 2, "jobs",
        ));
        let mut setup = Session::new(Arc::clone(&backend));
        open_channel(&mut setup).await;
        setup
            .handle(method_frame(
                1,
                AMQPClass::Exchange(exchange::AMQPMethod::Declare(exchange::Declare {
                    exchange: "events.durable".into(),
                    kind: "topic".into(),
                    durable: true,
                    ..exchange::Declare::default()
                })),
            ))
            .await
            .unwrap();
        setup
            .handle(method_frame(
                1,
                AMQPClass::Queue(queue::AMQPMethod::Bind(queue::Bind {
                    queue: "jobs".into(),
                    exchange: "events.durable".into(),
                    routing_key: "orders.#".into(),
                    ..queue::Bind::default()
                })),
            ))
            .await
            .unwrap();
        assert!(matches!(
            backend.cache_get("sessions", TOPOLOGY_KEY).await.unwrap(),
            Some(CacheEntry {
                value: CacheValue::Blob(_),
                ..
            })
        ));

        // A distinct local topology simulates another gateway process or a
        // restart and must load the replicated document before routing.
        let mut recovered = Session::new(Arc::clone(&backend));
        open_channel(&mut recovered).await;
        assert!(
            publish_to(
                &mut recovered,
                "events.durable",
                "orders.created",
                true,
                BasicProperties::default(),
                b"survived",
            )
            .await
            .is_empty()
        );
        assert!(matches!(
            get_from(&mut recovered, "jobs", true).await.as_slice(),
            [.., AMQPFrame::Body(1, body)] if body == b"survived"
        ));
        assert!(matches!(
            recovered
                .handle(method_frame(
                    1,
                    AMQPClass::Exchange(exchange::AMQPMethod::Declare(exchange::Declare {
                        exchange: "events.durable".into(),
                        passive: true,
                        ..exchange::Declare::default()
                    })),
                ))
                .await
                .unwrap()
                .as_slice(),
            [AMQPFrame::Method(
                1,
                AMQPClass::Exchange(exchange::AMQPMethod::DeclareOk(_))
            )]
        ));

        recovered
            .handle(method_frame(
                1,
                AMQPClass::Exchange(exchange::AMQPMethod::Delete(exchange::Delete {
                    exchange: "events.durable".into(),
                    ..exchange::Delete::default()
                })),
            ))
            .await
            .unwrap();
        let mut after_delete = Session::new(backend);
        open_channel(&mut after_delete).await;
        assert!(
            after_delete
                .handle(method_frame(
                    1,
                    AMQPClass::Basic(basic::AMQPMethod::Publish(basic::Publish {
                        exchange: "events.durable".into(),
                        routing_key: "orders.created".into(),
                        ..basic::Publish::default()
                    })),
                ))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one scenario proves routing, metadata, fanout, shared topology, returns, and confirms together"
    )]
    async fn routes_topic_and_fanout_publishes_and_returns_mandatory_misses() {
        let backend = Arc::new(MemoryBackend::with_resources(
            "sessions", "events", 2, "jobs",
        ));
        backend.add_queue("audit");
        let topology = Arc::new(Mutex::new(Topology::default()));
        let mut setup = Session::with_topology(Arc::clone(&backend), Arc::clone(&topology));
        open_channel(&mut setup).await;
        for (name, kind) in [("events.topic", "topic"), ("events.all", "fanout")] {
            let declared = setup
                .handle(method_frame(
                    1,
                    AMQPClass::Exchange(exchange::AMQPMethod::Declare(exchange::Declare {
                        exchange: name.into(),
                        kind: kind.into(),
                        ..exchange::Declare::default()
                    })),
                ))
                .await
                .unwrap();
            assert!(matches!(
                declared.as_slice(),
                [AMQPFrame::Method(
                    1,
                    AMQPClass::Exchange(exchange::AMQPMethod::DeclareOk(_))
                )]
            ));
        }
        for (exchange, queue, key) in [
            ("events.topic", "jobs", "orders.*"),
            ("events.topic", "audit", "orders.#"),
            ("events.all", "jobs", "ignored.one"),
            ("events.all", "audit", "ignored.two"),
        ] {
            setup
                .handle(method_frame(
                    1,
                    AMQPClass::Queue(queue::AMQPMethod::Bind(queue::Bind {
                        queue: queue.into(),
                        exchange: exchange.into(),
                        routing_key: key.into(),
                        ..queue::Bind::default()
                    })),
                ))
                .await
                .unwrap();
        }

        let mut session = Session::with_topology(backend, topology);
        open_channel(&mut session).await;
        session
            .handle(method_frame(
                1,
                AMQPClass::Confirm(confirm::AMQPMethod::Select(confirm::Select::default())),
            ))
            .await
            .unwrap();
        let mut headers = FieldTable::default();
        headers.insert(
            "tenant".into(),
            AMQPValue::LongString("acme".as_bytes().into()),
        );
        let confirmed = publish_to(
            &mut session,
            "events.topic",
            "orders.created",
            true,
            BasicProperties::default()
                .with_headers(headers)
                .with_expiration("5000".into()),
            b"created",
        )
        .await;
        assert!(matches!(
            confirmed.as_slice(),
            [AMQPFrame::Method(
                1,
                AMQPClass::Basic(basic::AMQPMethod::Ack(_))
            )]
        ));
        for queue in ["jobs", "audit"] {
            let delivery = get_from(&mut session, queue, true).await;
            assert!(matches!(
                delivery.as_slice(),
                [
                    AMQPFrame::Method(1, AMQPClass::Basic(basic::AMQPMethod::GetOk(get))),
                    AMQPFrame::Header(1, header),
                    AMQPFrame::Body(1, body),
                ] if get.exchange.as_str() == "events.topic"
                    && get.routing_key.as_str() == "orders.created"
                    && body == b"created"
                    && header.properties.expiration().as_ref().map(ToString::to_string)
                        == Some("5000".into())
                    && header.properties.headers().as_ref().is_some_and(|headers| {
                        headers.inner().get("tenant").and_then(AMQPValue::as_long_string)
                            .is_some_and(|value| value.as_bytes() == b"acme")
                    })
            ));
        }

        let fanout = publish_to(
            &mut session,
            "events.all",
            "anything",
            false,
            BasicProperties::default(),
            b"broadcast",
        )
        .await;
        assert!(matches!(fanout.as_slice(), [AMQPFrame::Method(..)]));
        for queue in ["jobs", "audit"] {
            assert!(matches!(
                get_from(&mut session, queue, true).await.as_slice(),
                [.., AMQPFrame::Body(1, body)] if body == b"broadcast"
            ));
        }

        let returned = publish_to(
            &mut session,
            "events.topic",
            "payments.created",
            true,
            BasicProperties::default(),
            b"unroutable",
        )
        .await;
        assert!(matches!(
            returned.as_slice(),
            [
                AMQPFrame::Method(1, AMQPClass::Basic(basic::AMQPMethod::Return(value))),
                AMQPFrame::Header(1, _),
                AMQPFrame::Body(1, body),
                AMQPFrame::Method(1, AMQPClass::Basic(basic::AMQPMethod::Ack(_))),
            ] if value.reply_code == 312 && value.reply_text.as_str() == "NO_ROUTE"
                && body == b"unroutable"
        ));
    }

    #[test]
    fn authenticates_plain_credentials_without_prefix_or_length_confusion() {
        let config = AmqpConfig {
            username: "epoch".into(),
            password: "secret".into(),
            max_connections: 1,
            heartbeat_seconds: 30,
            topology_cache: "sessions".into(),
        };
        let valid = connection::StartOk {
            mechanism: "PLAIN".into(),
            locale: "en_US".into(),
            response: LongString::from(b"\0epoch\0secret".to_vec()),
            ..connection::StartOk::default()
        };
        assert!(authenticate(&config, &valid).is_ok());
        let invalid = connection::StartOk {
            mechanism: "PLAIN".into(),
            locale: "en_US".into(),
            response: LongString::from(b"\0epoch\0secret-extra".to_vec()),
            ..connection::StartOk::default()
        };
        assert!(authenticate(&config, &invalid).is_err());
    }

    #[tokio::test]
    async fn deterministic_frame_mutation_corpus_never_panics_or_bypasses_size_bounds() {
        let oversized = u32::try_from(MAX_FRAME_BYTES + 1).unwrap();
        let mut oversized_header = vec![1, 0, 1];
        oversized_header.extend_from_slice(&oversized.to_be_bytes());
        let mut oversized_input = oversized_header.as_slice();
        assert!(read_frame(&mut oversized_input).await.is_err());

        let mut state = 0xa076_1d64_78bd_642f_u64;
        for iteration in 0..512_u16 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let declared = u32::from(state.to_le_bytes()[0] % 65);
            let mut input = vec![state.to_le_bytes()[1], 0, iteration.to_le_bytes()[0]];
            input.extend_from_slice(&declared.to_be_bytes());
            let available = usize::from(state.to_le_bytes()[2] % 80);
            for _ in 0..available {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                input.push(state.to_le_bytes()[0]);
            }
            let mut input = input.as_slice();
            let _ = read_frame(&mut input).await;
        }
    }

    #[tokio::test]
    async fn translates_publish_confirm_get_and_ack_with_binary_body() {
        let backend = Arc::new(MemoryBackend::with_resources(
            "sessions", "events", 2, "jobs",
        ));
        let mut session = Session::new(backend);
        open_channel(&mut session).await;
        let declared = session
            .handle(method_frame(
                1,
                AMQPClass::Queue(queue::AMQPMethod::Declare(queue::Declare {
                    queue: "jobs".into(),
                    ..queue::Declare::default()
                })),
            ))
            .await
            .unwrap();
        assert!(matches!(
            declared.as_slice(),
            [AMQPFrame::Method(
                1,
                AMQPClass::Queue(queue::AMQPMethod::DeclareOk(_))
            )]
        ));
        session
            .handle(method_frame(
                1,
                AMQPClass::Confirm(confirm::AMQPMethod::Select(confirm::Select::default())),
            ))
            .await
            .unwrap();
        let confirmation = publish(&mut session, b"binary\0message").await;
        assert!(matches!(
            confirmation.as_slice(),
            [AMQPFrame::Method(1, AMQPClass::Basic(basic::AMQPMethod::Ack(ack)))]
                if ack.delivery_tag == 1
        ));
        let delivery = get(&mut session).await;
        assert!(matches!(
            delivery.as_slice(),
            [
                AMQPFrame::Method(1, AMQPClass::Basic(basic::AMQPMethod::GetOk(get))),
                AMQPFrame::Header(1, header),
                AMQPFrame::Body(1, body),
            ] if get.delivery_tag == 1
                && body == b"binary\0message"
                && header.properties.correlation_id().as_ref().map(ToString::to_string)
                    == Some("correlation-1".into())
                && header.properties.reply_to().as_ref().map(ToString::to_string)
                    == Some("replies".into())
        ));
        session
            .handle(method(
                1,
                basic::AMQPMethod::Ack(basic::Ack {
                    delivery_tag: 1,
                    multiple: false,
                }),
            ))
            .await
            .unwrap();
        assert!(matches!(
            get(&mut session).await.as_slice(),
            [AMQPFrame::Method(
                1,
                AMQPClass::Basic(basic::AMQPMethod::GetEmpty(_))
            )]
        ));
    }

    #[tokio::test]
    async fn nack_requeue_returns_the_delivery_instead_of_acknowledging_it() {
        let backend = Arc::new(MemoryBackend::with_resources(
            "sessions", "events", 2, "jobs",
        ));
        let mut session = Session::new(backend);
        open_channel(&mut session).await;
        assert!(publish(&mut session, b"retry").await.is_empty());
        assert!(matches!(
            get(&mut session).await.as_slice(),
            [
                AMQPFrame::Method(1, AMQPClass::Basic(basic::AMQPMethod::GetOk(_))),
                ..
            ]
        ));
        session
            .handle(method(
                1,
                basic::AMQPMethod::Nack(basic::Nack {
                    delivery_tag: 1,
                    multiple: false,
                    requeue: true,
                }),
            ))
            .await
            .unwrap();
        assert!(matches!(
            get(&mut session).await.as_slice(),
            [.., AMQPFrame::Body(1, body)] if body == b"retry"
        ));
    }

    #[tokio::test]
    async fn reject_without_requeue_uses_the_provisioned_native_dead_letter_target() {
        let backend = Arc::new(MemoryBackend::with_resources(
            "sessions", "events", 2, "jobs",
        ));
        backend.add_queue("failed-jobs");
        backend.configure_queue_dead_letter("jobs", "failed-jobs");
        let mut session = Session::new(backend);
        open_channel(&mut session).await;

        let mut arguments = FieldTable::default();
        arguments.insert(
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString(Vec::new().into()),
        );
        arguments.insert(
            "x-dead-letter-routing-key".into(),
            AMQPValue::LongString("failed-jobs".as_bytes().into()),
        );
        assert!(
            session
                .handle(method_frame(
                    1,
                    AMQPClass::Queue(queue::AMQPMethod::Declare(queue::Declare {
                        queue: "jobs".into(),
                        arguments: arguments.clone(),
                        ..queue::Declare::default()
                    })),
                ))
                .await
                .is_ok()
        );

        let mut wrong = arguments;
        wrong.insert(
            "x-dead-letter-routing-key".into(),
            AMQPValue::LongString("other".as_bytes().into()),
        );
        assert!(
            session
                .handle(method_frame(
                    1,
                    AMQPClass::Queue(queue::AMQPMethod::Declare(queue::Declare {
                        queue: "jobs".into(),
                        arguments: wrong,
                        ..queue::Declare::default()
                    })),
                ))
                .await
                .is_err()
        );

        assert!(publish(&mut session, b"poison").await.is_empty());
        assert!(matches!(
            get(&mut session).await.as_slice(),
            [
                AMQPFrame::Method(1, AMQPClass::Basic(basic::AMQPMethod::GetOk(_))),
                ..
            ]
        ));
        session
            .handle(method(
                1,
                basic::AMQPMethod::Reject(basic::Reject {
                    delivery_tag: 1,
                    requeue: false,
                }),
            ))
            .await
            .unwrap();
        assert!(matches!(
            get_from(&mut session, "failed-jobs", true)
                .await
                .as_slice(),
            [
                AMQPFrame::Method(1, AMQPClass::Basic(basic::AMQPMethod::GetOk(_))),
                AMQPFrame::Header(1, header),
                AMQPFrame::Body(1, body),
            ] if body == b"poison"
                && header.properties.headers().as_ref().is_some_and(|headers| {
                    matches!(headers.inner().get("x-death"), Some(AMQPValue::FieldArray(values)) if values.as_slice().len() == 1)
                })
        ));
        assert!(matches!(
            get(&mut session).await.as_slice(),
            [AMQPFrame::Method(
                1,
                AMQPClass::Basic(basic::AMQPMethod::GetEmpty(_))
            )]
        ));
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one scenario proves named DLX declaration, durable recovery, native target validation, retargeting, and x-death metadata"
    )]
    async fn named_dead_letter_exchange_routes_to_the_provisioned_native_target() {
        let backend = Arc::new(MemoryBackend::with_resources(
            "sessions", "events", 2, "jobs",
        ));
        backend.add_queue("failed-jobs");
        backend.configure_queue_dead_letter("jobs", "failed-jobs");
        let mut setup = Session::new(Arc::clone(&backend));
        open_channel(&mut setup).await;
        setup
            .handle(method_frame(
                1,
                AMQPClass::Exchange(exchange::AMQPMethod::Declare(exchange::Declare {
                    exchange: "dead.events".into(),
                    kind: "topic".into(),
                    durable: true,
                    ..exchange::Declare::default()
                })),
            ))
            .await
            .unwrap();
        setup
            .handle(method_frame(
                1,
                AMQPClass::Queue(queue::AMQPMethod::Bind(queue::Bind {
                    queue: "failed-jobs".into(),
                    exchange: "dead.events".into(),
                    routing_key: "failed.#".into(),
                    ..queue::Bind::default()
                })),
            ))
            .await
            .unwrap();
        let mut arguments = FieldTable::default();
        arguments.insert(
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString("dead.events".as_bytes().into()),
        );
        arguments.insert(
            "x-dead-letter-routing-key".into(),
            AMQPValue::LongString("failed.jobs".as_bytes().into()),
        );
        setup
            .handle(method_frame(
                1,
                AMQPClass::Queue(queue::AMQPMethod::Declare(queue::Declare {
                    queue: "jobs".into(),
                    durable: true,
                    arguments,
                    ..queue::Declare::default()
                })),
            ))
            .await
            .unwrap();

        let mut recovered = Session::new(backend);
        open_channel(&mut recovered).await;
        assert!(publish(&mut recovered, b"named-poison").await.is_empty());
        assert!(matches!(
            get(&mut recovered).await.as_slice(),
            [
                AMQPFrame::Method(1, AMQPClass::Basic(basic::AMQPMethod::GetOk(_))),
                ..
            ]
        ));
        recovered
            .handle(method(
                1,
                basic::AMQPMethod::Reject(basic::Reject {
                    delivery_tag: 1,
                    requeue: false,
                }),
            ))
            .await
            .unwrap();
        assert!(matches!(
            get_from(&mut recovered, "failed-jobs", true).await.as_slice(),
            [
                AMQPFrame::Method(1, AMQPClass::Basic(basic::AMQPMethod::GetOk(get))),
                AMQPFrame::Header(1, header),
                AMQPFrame::Body(1, body),
            ] if get.exchange.as_str() == "dead.events"
                && get.routing_key.as_str() == "failed.jobs"
                && body == b"named-poison"
                && header.properties.headers().as_ref().is_some_and(|headers| {
                    matches!(headers.inner().get("x-death"), Some(AMQPValue::FieldArray(values)) if values.as_slice().len() == 1)
                })
        ));
    }

    #[tokio::test]
    async fn push_consumer_obeys_prefetch_until_the_delivery_is_acknowledged() {
        let backend = Arc::new(MemoryBackend::with_resources(
            "sessions", "events", 2, "jobs",
        ));
        let mut session = Session::new(backend);
        open_channel(&mut session).await;
        assert!(publish(&mut session, b"first").await.is_empty());
        assert!(publish(&mut session, b"second").await.is_empty());
        let consume_ok = session
            .handle(method(
                1,
                basic::AMQPMethod::Consume(basic::Consume {
                    queue: "jobs".into(),
                    consumer_tag: "worker-1".into(),
                    ..basic::Consume::default()
                }),
            ))
            .await
            .unwrap();
        assert!(matches!(
            consume_ok.as_slice(),
            [AMQPFrame::Method(1, AMQPClass::Basic(basic::AMQPMethod::ConsumeOk(ok)))]
                if ok.consumer_tag.as_str() == "worker-1"
        ));
        let first = session.poll_consumers().await.unwrap();
        assert!(matches!(
            first.as_slice(),
            [
                AMQPFrame::Method(1, AMQPClass::Basic(basic::AMQPMethod::Deliver(delivery))),
                AMQPFrame::Header(1, _),
                AMQPFrame::Body(1, body),
            ] if delivery.delivery_tag == 1 && body == b"first"
        ));
        assert!(session.poll_consumers().await.unwrap().is_empty());
        session
            .handle(method(
                1,
                basic::AMQPMethod::Ack(basic::Ack {
                    delivery_tag: 1,
                    multiple: false,
                }),
            ))
            .await
            .unwrap();
        assert!(matches!(
            session.poll_consumers().await.unwrap().as_slice(),
            [.., AMQPFrame::Body(1, body)] if body == b"second"
        ));
    }
}

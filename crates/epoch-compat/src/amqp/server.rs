use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, Mutex},
};

use amq_protocol::{
    frame::{AMQPContentHeader, AMQPFrame, ProtocolVersion, WriteContext, gen_frame, parse_frame},
    protocol::{AMQPClass, BasicProperties, basic, channel, confirm, connection, exchange, queue},
    types::{AMQPValue, FieldTable, LongString},
};
use anyhow::{Context, Result, bail};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};

use crate::{
    CompatibilityBackend, MAX_FRAME_BYTES, MAX_MESSAGE_BYTES, MAX_REQUEST_ITEMS,
    backend::{BackendError, QueueDelivery, QueueMessage},
};

const AMQP_PROTOCOL_HEADER: &[u8; 8] = b"AMQP\0\0\x09\x01";
const AMQP_FRAME_END: u8 = 0xce;
const MAX_CHANNELS: u16 = 2_048;

#[derive(Clone)]
pub struct AmqpConfig {
    pub username: String,
    pub password: String,
    pub max_connections: usize,
    pub heartbeat_seconds: u16,
}

impl fmt::Debug for AmqpConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AmqpConfig")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("max_connections", &self.max_connections)
            .field("heartbeat_seconds", &self.heartbeat_seconds)
            .finish()
    }
}

#[derive(Debug)]
pub struct AmqpServer<B> {
    backend: Arc<B>,
    config: AmqpConfig,
    topology: Arc<Mutex<Topology>>,
}

impl<B: CompatibilityBackend> AmqpServer<B> {
    pub fn new(backend: Arc<B>, config: AmqpConfig) -> Result<Self, BackendError> {
        if config.username.is_empty() || config.password.is_empty() || config.max_connections == 0 {
            return Err(BackendError::Invalid(
                "AMQP credentials and positive connection limit are required".into(),
            ));
        }
        Ok(Self {
            backend,
            config,
            topology: Arc::new(Mutex::new(Topology::default())),
        })
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
            tokio::spawn(async move {
                let _permit = permit;
                if let Err(error) = serve_connection(stream, backend, config, topology).await {
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

    run_session(stream, backend, topology).await
}

async fn run_session<B: CompatibilityBackend>(
    stream: TcpStream,
    backend: Arc<B>,
    topology: Arc<Mutex<Topology>>,
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
    let mut session = Session::with_topology(backend, topology);
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
                    session.handle(frame).await?
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

#[derive(Debug)]
struct PendingPublish {
    queues: Vec<String>,
    exchange: String,
    routing_key: String,
    mandatory: bool,
    properties: Option<BasicProperties>,
    expected_body_size: Option<u64>,
    body: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExchangeKind {
    Direct,
    Fanout,
    Topic,
}

impl ExchangeKind {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "direct" => Some(Self::Direct),
            "fanout" => Some(Self::Fanout),
            "topic" => Some(Self::Topic),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Binding {
    exchange: String,
    routing_key: String,
    queue: String,
}

#[derive(Debug)]
struct Topology {
    exchanges: BTreeMap<String, ExchangeKind>,
    bindings: BTreeSet<Binding>,
}

impl Default for Topology {
    fn default() -> Self {
        Self {
            exchanges: BTreeMap::from([
                ("amq.direct".into(), ExchangeKind::Direct),
                ("amq.fanout".into(), ExchangeKind::Fanout),
                ("amq.topic".into(), ExchangeKind::Topic),
            ]),
            bindings: BTreeSet::new(),
        }
    }
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
}

impl<B: CompatibilityBackend> Session<B> {
    #[cfg(test)]
    fn new(backend: Arc<B>) -> Self {
        Self::with_topology(backend, Arc::new(Mutex::new(Topology::default())))
    }

    fn with_topology(backend: Arc<B>, topology: Arc<Mutex<Topology>>) -> Self {
        Self {
            backend,
            channels: BTreeMap::new(),
            topology,
        }
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
                let name = declare.queue.as_str();
                if name.is_empty() || !declare.arguments.inner().is_empty() {
                    bail!("server-named queues and queue arguments are unsupported");
                }
                if !self.backend.queue_exists(name).await? {
                    bail!("AMQP queue does not map to a provisioned Epoch Queue");
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
                let Some(kind) = ExchangeKind::parse(declare.kind.as_str()) else {
                    bail!("unsupported AMQP exchange type");
                };
                if name.is_empty()
                    || declare.internal
                    || declare.auto_delete
                    || declare.durable
                    || !declare.arguments.inner().is_empty()
                {
                    bail!("unsupported AMQP exchange declaration options");
                }
                {
                    let mut topology = self.topology.lock().unwrap();
                    match topology.exchanges.get(&name) {
                        Some(existing) if *existing != kind => {
                            bail!("AMQP exchange redeclaration changed its type");
                        }
                        None if declare.passive => bail!("AMQP exchange does not exist"),
                        None => {
                            topology.exchanges.insert(name, kind);
                        }
                        Some(_) => {}
                    }
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
                let mut topology = self.topology.lock().unwrap();
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
                drop(topology);
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
                if !bind.arguments.inner().is_empty()
                    || !self.backend.queue_exists(bind.queue.as_str()).await?
                {
                    bail!("unsupported AMQP queue binding");
                }
                let mut topology = self.topology.lock().unwrap();
                if !topology.exchanges.contains_key(bind.exchange.as_str()) {
                    bail!("AMQP exchange does not exist");
                }
                topology.bindings.insert(Binding {
                    exchange: bind.exchange.to_string(),
                    routing_key: bind.routing_key.to_string(),
                    queue: bind.queue.to_string(),
                });
                drop(topology);
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
                if !unbind.arguments.inner().is_empty() {
                    bail!("AMQP queue-unbind arguments are unsupported");
                }
                let binding = Binding {
                    exchange: unbind.exchange.to_string(),
                    routing_key: unbind.routing_key.to_string(),
                    queue: unbind.queue.to_string(),
                };
                if !self.topology.lock().unwrap().bindings.remove(&binding) {
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
                let queues = self.resolve_publish_queues(&publish).await?;
                let state = self.require_channel_mut(id)?;
                if state.pending_publish.is_some() {
                    bail!("interleaved AMQP publishes are unsupported");
                }
                state.pending_publish = Some(PendingPublish {
                    queues,
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

    async fn resolve_publish_queues(&self, publish: &basic::Publish) -> Result<Vec<String>> {
        if publish.immediate {
            bail!("AMQP immediate publishing is unsupported");
        }
        if publish.exchange.as_str().is_empty() {
            return if self
                .backend
                .queue_exists(publish.routing_key.as_str())
                .await?
            {
                Ok(vec![publish.routing_key.to_string()])
            } else {
                Ok(Vec::new())
            };
        }
        let topology = self.topology.lock().unwrap();
        let kind = topology
            .exchanges
            .get(publish.exchange.as_str())
            .copied()
            .context("AMQP exchange does not exist")?;
        Ok(topology
            .bindings
            .iter()
            .filter(|binding| {
                binding.exchange == publish.exchange.as_str()
                    && binding_matches(kind, &binding.routing_key, publish.routing_key.as_str())
            })
            .map(|binding| binding.queue.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
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
        let message = QueueMessage {
            body: pending.body.clone(),
            content_type: properties.content_type().as_ref().map(ToString::to_string),
            correlation_id: properties
                .correlation_id()
                .as_ref()
                .map(ToString::to_string),
            reply_to: properties.reply_to().as_ref().map(ToString::to_string),
            headers: string_headers(&properties)?,
            exchange: Some(pending.exchange.clone()),
            routing_key: Some(pending.routing_key.clone()),
            expiration,
            ttl_ms,
        };
        for queue in &pending.queues {
            self.backend.queue_publish(queue, message.clone()).await?;
        }
        let mut frames = Vec::new();
        if pending.queues.is_empty() && pending.mandatory {
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

fn binding_matches(kind: ExchangeKind, binding_key: &str, routing_key: &str) -> bool {
    match kind {
        ExchangeKind::Direct => binding_key == routing_key,
        ExchangeKind::Fanout => true,
        ExchangeKind::Topic => topic_matches(binding_key, routing_key),
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
            let value = match value {
                AMQPValue::ShortString(value) => value.to_string(),
                AMQPValue::LongString(value) => std::str::from_utf8(value.as_bytes())
                    .context("AMQP string header is not UTF-8")?
                    .to_owned(),
                _ => bail!("only AMQP string headers are supported"),
            };
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
            headers.insert(
                name.as_str().into(),
                AMQPValue::LongString(value.as_str().into()),
            );
        }
        properties = properties.with_headers(headers);
    }
    properties
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

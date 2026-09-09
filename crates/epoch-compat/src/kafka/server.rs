use std::{
    io::{Cursor, Read as _},
    sync::Arc,
    time::Instant,
};

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use epoch_observability::{MetricsRegistry, Outcome, Protocol, ProtocolOperation};
use flate2::read::MultiGzDecoder;
use kafka_protocol::{
    ResponseError,
    messages::{
        ApiKey, ApiVersionsResponse, FetchResponse, FindCoordinatorResponse, HeartbeatResponse,
        JoinGroupResponse, LeaveGroupResponse, ListOffsetsResponse, MetadataResponse,
        OffsetCommitResponse, OffsetFetchResponse, ProduceResponse, RequestKind, ResponseHeader,
        ResponseKind, SyncGroupResponse, TopicName,
        api_versions_response::ApiVersion,
        fetch_response::{FetchableTopicResponse, PartitionData},
        find_coordinator_response::Coordinator,
        join_group_response::JoinGroupResponseMember,
        leave_group_response::MemberResponse,
        list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
        metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        },
        offset_commit_response::{OffsetCommitResponsePartition, OffsetCommitResponseTopic},
        offset_fetch_response::{OffsetFetchResponsePartition, OffsetFetchResponseTopic},
        produce_response::{PartitionProduceResponse, TopicProduceResponse},
    },
    protocol::{Encodable, StrBytes, decode_request_header_from_buffer},
    records::{BatchDecodeInfo, Compression, RecordBatchDecoder},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::Instrument as _;

use crate::{
    CompatibilityBackend, MAX_FRAME_BYTES, MAX_MESSAGE_BYTES,
    backend::{BackendError, StreamGroupIdentity, StreamGroupRejection, StreamGroupSessionResult},
    observe_protocol,
};

use super::records::{decode_records, encode_records};

const MAX_KAFKA_RECORDS_PER_PRODUCE_PARTITION: usize = 1_000;
const KAFKA_SNAPPY_MAGIC_HEADER: &[u8; 16] = b"\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01";
const ZSTD_MAX_WINDOW_LOG: u32 = 23;

pub const SUPPORTED_APIS: &[(ApiKey, i16, i16)] = &[
    (ApiKey::Produce, 3, 9),
    (ApiKey::Fetch, 4, 12),
    (ApiKey::ListOffsets, 1, 7),
    (ApiKey::Metadata, 1, 12),
    (ApiKey::OffsetCommit, 2, 9),
    (ApiKey::OffsetFetch, 1, 7),
    (ApiKey::FindCoordinator, 0, 4),
    (ApiKey::JoinGroup, 0, 9),
    (ApiKey::SyncGroup, 0, 5),
    (ApiKey::Heartbeat, 0, 4),
    (ApiKey::LeaveGroup, 0, 5),
    (ApiKey::ApiVersions, 0, 4),
];

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub advertised_host: String,
    pub port: u16,
    pub node_id: i32,
    pub max_connections: usize,
}

#[derive(Debug)]
pub struct KafkaServer<B> {
    backend: Arc<B>,
    config: KafkaConfig,
    metrics: Option<MetricsRegistry>,
}

impl<B: CompatibilityBackend> KafkaServer<B> {
    pub fn new(backend: Arc<B>, config: KafkaConfig) -> Result<Self, BackendError> {
        if config.advertised_host.trim().is_empty()
            || config.port == 0
            || config.node_id < 0
            || config.max_connections == 0
        {
            return Err(BackendError::Invalid(
                "Kafka host, port, non-negative node ID, and connection limit are required".into(),
            ));
        }
        Ok(Self {
            backend,
            config,
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
            let metrics = self.metrics.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let started = Instant::now();
                let result = serve_connection(stream, backend, config, metrics.as_ref()).await;
                observe_protocol(
                    metrics.as_ref(),
                    Protocol::Kafka,
                    ProtocolOperation::Connect,
                    if result.is_ok() {
                        Outcome::Success
                    } else {
                        Outcome::ServerError
                    },
                    started.elapsed(),
                );
                if let Err(error) = result {
                    tracing::warn!(protocol = "kafka", %error, "compatibility connection closed");
                }
            });
        }
    }
}

async fn serve_connection<B: CompatibilityBackend>(
    mut stream: TcpStream,
    backend: Arc<B>,
    config: KafkaConfig,
    metrics: Option<&MetricsRegistry>,
) -> Result<()> {
    loop {
        let length = match stream.read_i32().await {
            Ok(length) => length,
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let length = usize::try_from(length)
            .ok()
            .filter(|length| *length >= 8 && *length <= MAX_FRAME_BYTES)
            .context("invalid Kafka frame length")?;
        let mut frame = vec![0_u8; length];
        stream.read_exact(&mut frame).await?;
        let operation = kafka_operation(&frame);
        let started = Instant::now();
        let handled = handle_frame(Bytes::from(frame), backend.as_ref(), &config)
            .instrument(tracing::info_span!(
                "epoch.compat.request",
                protocol = Protocol::Kafka.as_str(),
                operation = operation.as_str()
            ))
            .await;
        observe_protocol(
            metrics,
            Protocol::Kafka,
            operation,
            if handled.is_ok() {
                Outcome::Success
            } else {
                Outcome::ClientError
            },
            started.elapsed(),
        );
        if let Some(response) = handled? {
            stream.write_all(&response).await?;
        }
    }
}

fn kafka_operation(frame: &[u8]) -> ProtocolOperation {
    let Some(api_key) = frame
        .get(..2)
        .map(|bytes| i16::from_be_bytes([bytes[0], bytes[1]]))
    else {
        return ProtocolOperation::Other;
    };
    match api_key {
        0 => ProtocolOperation::Produce,
        1 | 2 => ProtocolOperation::Fetch,
        8..=14 => ProtocolOperation::Group,
        _ => ProtocolOperation::Other,
    }
}

async fn handle_frame<B: CompatibilityBackend>(
    mut frame: Bytes,
    backend: &B,
    config: &KafkaConfig,
) -> Result<Option<Vec<u8>>> {
    let header = decode_request_header_from_buffer(&mut frame).context("invalid Kafka header")?;
    let api_key = ApiKey::try_from(header.request_api_key)
        .map_err(|()| anyhow::anyhow!("unknown Kafka API key"))?;
    let version = header.request_api_version;
    if !supported(api_key, version) {
        bail!("unsupported Kafka API {api_key:?} version {version}");
    }
    let request =
        RequestKind::decode(api_key, &mut frame, version).context("invalid Kafka request")?;
    if frame.has_remaining() {
        bail!("Kafka request has trailing bytes");
    }
    let (response, emit) = dispatch(request, backend, config, version).await?;
    if !emit {
        return Ok(None);
    }
    let mut payload = BytesMut::new();
    ResponseHeader::default()
        .with_correlation_id(header.correlation_id)
        .encode(&mut payload, api_key.response_header_version(version))?;
    response.encode(&mut payload, version)?;
    let response_length = i32::try_from(payload.len()).context("Kafka response exceeds i32")?;
    let mut framed = Vec::with_capacity(payload.len() + 4);
    framed.put_i32(response_length);
    framed.extend_from_slice(&payload);
    Ok(Some(framed))
}

async fn dispatch<B: CompatibilityBackend>(
    request: RequestKind,
    backend: &B,
    config: &KafkaConfig,
    version: i16,
) -> Result<(ResponseKind, bool)> {
    match request {
        RequestKind::ApiVersions(_) => Ok((
            ResponseKind::ApiVersions(
                ApiVersionsResponse::default().with_api_keys(
                    SUPPORTED_APIS
                        .iter()
                        .map(|(key, min, max)| {
                            ApiVersion::default()
                                .with_api_key(*key as i16)
                                .with_min_version(*min)
                                .with_max_version(*max)
                        })
                        .collect(),
                ),
            ),
            true,
        )),
        RequestKind::Metadata(request) => Ok((
            ResponseKind::Metadata(metadata_response(request, backend, config).await),
            true,
        )),
        RequestKind::Produce(request) => {
            let emit = request.acks != 0;
            Ok((
                ResponseKind::Produce(produce_response(request, backend).await),
                emit,
            ))
        }
        RequestKind::Fetch(request) => Ok((
            ResponseKind::Fetch(fetch_response(request, backend).await),
            true,
        )),
        RequestKind::ListOffsets(request) => Ok((
            ResponseKind::ListOffsets(list_offsets_response(request, backend).await),
            true,
        )),
        RequestKind::FindCoordinator(request) => Ok((
            ResponseKind::FindCoordinator(find_coordinator_response(request, config, version)),
            true,
        )),
        RequestKind::JoinGroup(request) => Ok((
            ResponseKind::JoinGroup(join_group_response(request, backend, version).await),
            true,
        )),
        RequestKind::SyncGroup(request) => Ok((
            ResponseKind::SyncGroup(sync_group_response(request, backend, version).await),
            true,
        )),
        RequestKind::Heartbeat(request) => Ok((
            ResponseKind::Heartbeat(heartbeat_response(request, backend).await),
            true,
        )),
        RequestKind::LeaveGroup(request) => Ok((
            ResponseKind::LeaveGroup(leave_group_response(request, backend, version).await),
            true,
        )),
        RequestKind::OffsetCommit(request) => Ok((
            ResponseKind::OffsetCommit(offset_commit_response(request, backend).await),
            true,
        )),
        RequestKind::OffsetFetch(request) => Ok((
            ResponseKind::OffsetFetch(offset_fetch_response(request, backend).await),
            true,
        )),
        _ => bail!("Kafka API was advertised without a dispatcher"),
    }
}

fn find_coordinator_response(
    request: kafka_protocol::messages::FindCoordinatorRequest,
    config: &KafkaConfig,
    version: i16,
) -> FindCoordinatorResponse {
    let host = StrBytes::from_string(config.advertised_host.clone());
    let unsupported_key_type = request.key_type != 0;
    let coordinator = |key: StrBytes| {
        let mut coordinator = Coordinator::default()
            .with_key(key)
            .with_node_id(config.node_id.into())
            .with_host(host.clone())
            .with_port(i32::from(config.port));
        if unsupported_key_type {
            coordinator.error_code = ResponseError::InvalidRequest.code();
            coordinator.error_message = Some(StrBytes::from_static_str(
                "Epoch supports Kafka group coordinators only",
            ));
        }
        coordinator
    };

    if version >= 4 {
        return FindCoordinatorResponse::default().with_coordinators(
            request
                .coordinator_keys
                .into_iter()
                .map(coordinator)
                .collect(),
        );
    }

    let mut response = FindCoordinatorResponse::default()
        .with_node_id(config.node_id.into())
        .with_host(host)
        .with_port(i32::from(config.port));
    if unsupported_key_type {
        response.error_code = ResponseError::InvalidRequest.code();
        response.error_message = Some(StrBytes::from_static_str(
            "Epoch supports Kafka group coordinators only",
        ));
    }
    response
}

async fn join_group_response<B: CompatibilityBackend>(
    request: kafka_protocol::messages::JoinGroupRequest,
    backend: &B,
    version: i16,
) -> JoinGroupResponse {
    let group = request.group_id.to_string();
    let requested_member = request.member_id.to_string();
    let group_instance_id = request.group_instance_id.as_ref().map(StrBytes::as_str);
    if request.protocol_type.as_str() != "consumer" || request.protocols.is_empty() {
        return join_group_failure(ResponseError::InconsistentGroupProtocol, &requested_member);
    }
    let Some(protocol) = request
        .protocols
        .iter()
        .find(|protocol| protocol.name.as_str() == "range")
    else {
        return join_group_failure(ResponseError::InconsistentGroupProtocol, &requested_member);
    };
    let Ok(stream) = decode_consumer_subscription(&protocol.metadata) else {
        return join_group_failure(ResponseError::InvalidRequest, &requested_member);
    };
    let member_id = if requested_member.is_empty() {
        match kafka_member_id(&stream, group_instance_id) {
            Ok(member_id) => member_id,
            Err(_) => return join_group_failure(ResponseError::InvalidRequest, ""),
        }
    } else {
        let Ok(identity) = member_identity(&requested_member) else {
            return join_group_failure(ResponseError::UnknownMemberId, &requested_member);
        };
        if identity.stream != stream {
            return join_group_failure(ResponseError::InconsistentGroupProtocol, &requested_member);
        }
        if identity.group_instance_id.as_deref() != group_instance_id {
            return join_group_failure(ResponseError::FencedInstanceId, &requested_member);
        }
        requested_member
    };
    if version >= 4 && request.member_id.is_empty() {
        return join_group_failure(ResponseError::MemberIdRequired, &member_id);
    }
    let timeout = u64::try_from(request.session_timeout_ms)
        .ok()
        .filter(|timeout| (1_000..=300_000).contains(timeout));
    let Some(timeout) = timeout else {
        return join_group_failure(ResponseError::InvalidSessionTimeout, &member_id);
    };
    match backend
        .stream_group_join(&stream, &group, &member_id, timeout)
        .await
    {
        Ok(result) => joined_group_response(&result, &stream, &member_id),
        Err(error) => join_group_failure(group_backend_error(&error), &member_id),
    }
}

fn joined_group_response(
    result: &StreamGroupSessionResult,
    stream: &str,
    member_id: &str,
) -> JoinGroupResponse {
    if let Some(rejection) = result.rejection {
        return join_group_failure(group_rejection_error(rejection), member_id);
    }
    let Ok(generation) = i32::try_from(result.session.generation) else {
        return join_group_failure(ResponseError::UnknownServerError, member_id);
    };
    let Some(leader) = result
        .session
        .members
        .iter()
        .map(|member| member.member_id.as_str())
        .min()
    else {
        return join_group_failure(ResponseError::UnknownServerError, member_id);
    };
    let members = if member_id == leader {
        result
            .session
            .members
            .iter()
            .map(|member| {
                let group_instance_id = member_identity(&member.member_id)
                    .ok()
                    .and_then(|identity| identity.group_instance_id)
                    .map(StrBytes::from_string);
                JoinGroupResponseMember::default()
                    .with_member_id(StrBytes::from_string(member.member_id.clone()))
                    .with_group_instance_id(group_instance_id)
                    .with_metadata(encode_consumer_subscription(stream))
            })
            .collect()
    } else {
        Vec::new()
    };
    JoinGroupResponse::default()
        .with_error_code(0)
        .with_generation_id(generation)
        .with_protocol_type(Some(StrBytes::from_static_str("consumer")))
        .with_protocol_name(Some(StrBytes::from_static_str("range")))
        .with_leader(StrBytes::from_string(leader.to_owned()))
        .with_member_id(StrBytes::from_string(member_id.to_owned()))
        .with_members(members)
}

fn join_group_failure(error: ResponseError, member_id: &str) -> JoinGroupResponse {
    JoinGroupResponse::default()
        .with_error_code(error.code())
        .with_generation_id(-1)
        .with_protocol_type(Some(StrBytes::from_static_str("consumer")))
        .with_protocol_name(Some(StrBytes::from_static_str("range")))
        .with_member_id(StrBytes::from_string(member_id.to_owned()))
}

async fn sync_group_response<B: CompatibilityBackend>(
    request: kafka_protocol::messages::SyncGroupRequest,
    backend: &B,
    version: i16,
) -> SyncGroupResponse {
    if version >= 5
        && (request.protocol_type.as_ref().map(StrBytes::as_str) != Some("consumer")
            || request.protocol_name.as_ref().map(StrBytes::as_str) != Some("range"))
    {
        return sync_group_failure(ResponseError::InconsistentGroupProtocol);
    }
    let member_id = request.member_id.to_string();
    let Ok(identity) = member_identity(&member_id) else {
        return sync_group_failure(ResponseError::UnknownMemberId);
    };
    if identity.group_instance_id.as_deref()
        != request.group_instance_id.as_ref().map(StrBytes::as_str)
    {
        return sync_group_failure(ResponseError::FencedInstanceId);
    }
    let stream = identity.stream;
    let Some(generation) = u64::try_from(request.generation_id)
        .ok()
        .filter(|generation| *generation > 0)
    else {
        return sync_group_failure(ResponseError::IllegalGeneration);
    };
    let group = request.group_id.to_string();
    let result = match backend
        .stream_group_heartbeat(&stream, &group, &member_id, generation)
        .await
    {
        Ok(result) => result,
        Err(error) => return sync_group_failure(group_backend_error(&error)),
    };
    if let Some(rejection) = result.rejection {
        return sync_group_failure(group_rejection_error(rejection));
    }
    if let Err(error) = backend
        .stream_group_claim(
            &stream,
            &group,
            &member_id,
            generation,
            &result.session.assigned_partitions,
        )
        .await
    {
        return sync_group_failure(group_backend_error(&error));
    }
    let Ok(assignment) = encode_consumer_assignment(&stream, &result.session.assigned_partitions)
    else {
        return sync_group_failure(ResponseError::InvalidRequest);
    };
    SyncGroupResponse::default()
        .with_error_code(0)
        .with_protocol_type(Some(StrBytes::from_static_str("consumer")))
        .with_protocol_name(Some(StrBytes::from_static_str("range")))
        .with_assignment(assignment)
}

fn sync_group_failure(error: ResponseError) -> SyncGroupResponse {
    SyncGroupResponse::default()
        .with_error_code(error.code())
        .with_protocol_type(Some(StrBytes::from_static_str("consumer")))
        .with_protocol_name(Some(StrBytes::from_static_str("range")))
}

async fn heartbeat_response<B: CompatibilityBackend>(
    request: kafka_protocol::messages::HeartbeatRequest,
    backend: &B,
) -> HeartbeatResponse {
    let member_id = request.member_id.to_string();
    let Ok(identity) = member_identity(&member_id) else {
        return HeartbeatResponse::default().with_error_code(ResponseError::UnknownMemberId.code());
    };
    if identity.group_instance_id.as_deref()
        != request.group_instance_id.as_ref().map(StrBytes::as_str)
    {
        return HeartbeatResponse::default()
            .with_error_code(ResponseError::FencedInstanceId.code());
    }
    let stream = identity.stream;
    let Some(generation) = u64::try_from(request.generation_id)
        .ok()
        .filter(|generation| *generation > 0)
    else {
        return HeartbeatResponse::default()
            .with_error_code(ResponseError::IllegalGeneration.code());
    };
    let error = match backend
        .stream_group_heartbeat(
            &stream,
            &request.group_id.to_string(),
            &member_id,
            generation,
        )
        .await
    {
        Ok(result) => result.rejection.map(group_rejection_error),
        Err(error) => Some(group_backend_error(&error)),
    };
    HeartbeatResponse::default().with_error_code(error.as_ref().map_or(0, ResponseError::code))
}

async fn leave_group_response<B: CompatibilityBackend>(
    request: kafka_protocol::messages::LeaveGroupRequest,
    backend: &B,
    version: i16,
) -> LeaveGroupResponse {
    let group = request.group_id.to_string();
    if version <= 2 {
        let error = leave_group_member(backend, &group, request.member_id.as_str(), None).await;
        return LeaveGroupResponse::default()
            .with_error_code(error.as_ref().map_or(0, ResponseError::code));
    }
    let mut members = Vec::with_capacity(request.members.len());
    for member in request.members {
        let error = leave_group_member(
            backend,
            &group,
            member.member_id.as_str(),
            member.group_instance_id.as_ref().map(StrBytes::as_str),
        )
        .await;
        members.push(
            MemberResponse::default()
                .with_member_id(member.member_id)
                .with_group_instance_id(member.group_instance_id)
                .with_error_code(error.as_ref().map_or(0, ResponseError::code)),
        );
    }
    LeaveGroupResponse::default()
        .with_error_code(0)
        .with_members(members)
}

async fn leave_group_member<B: CompatibilityBackend>(
    backend: &B,
    group: &str,
    member_id: &str,
    group_instance_id: Option<&str>,
) -> Option<ResponseError> {
    let Ok(identity) = member_identity(member_id) else {
        return Some(ResponseError::UnknownMemberId);
    };
    if identity.group_instance_id.as_deref() != group_instance_id {
        return Some(ResponseError::FencedInstanceId);
    }
    let stream = identity.stream;
    let session = match backend
        .stream_group_observe(&stream, group, member_id)
        .await
    {
        Ok(Some(session)) => session,
        Ok(None) => return Some(ResponseError::UnknownMemberId),
        Err(error) => return Some(group_backend_error(&error)),
    };
    if !session
        .members
        .iter()
        .any(|member| member.member_id == member_id)
    {
        return Some(ResponseError::UnknownMemberId);
    }
    match backend
        .stream_group_leave(&stream, group, member_id, session.generation)
        .await
    {
        Ok(result) => result.rejection.map(group_rejection_error),
        Err(error) => Some(group_backend_error(&error)),
    }
}

const KAFKA_MEMBER_PREFIX: &str = "epoch";
const KAFKA_STATIC_MEMBER_MARKER: &str = "static";
const MAX_CONSUMER_PROTOCOL_ITEMS: usize = 1_024;
const MAX_GROUP_INSTANCE_ID_BYTES: usize = 128;

fn kafka_member_id(stream: &str, group_instance_id: Option<&str>) -> Result<String> {
    let encoded_stream = URL_SAFE_NO_PAD.encode(stream.as_bytes());
    let member_id = if let Some(group_instance_id) = group_instance_id {
        if group_instance_id.is_empty() || group_instance_id.len() > MAX_GROUP_INSTANCE_ID_BYTES {
            bail!("Kafka group instance ID is invalid");
        }
        let encoded_instance = URL_SAFE_NO_PAD.encode(group_instance_id.as_bytes());
        format!(
            "{KAFKA_MEMBER_PREFIX}.{encoded_stream}.{KAFKA_STATIC_MEMBER_MARKER}.{encoded_instance}"
        )
    } else {
        format!(
            "{KAFKA_MEMBER_PREFIX}.{encoded_stream}.{}",
            uuid::Uuid::now_v7().simple()
        )
    };
    if member_id.len() > 256 {
        bail!("Kafka member ID exceeds the native limit");
    }
    Ok(member_id)
}

#[derive(Debug, PartialEq, Eq)]
struct KafkaMemberIdentity {
    stream: String,
    group_instance_id: Option<String>,
}

fn member_identity(member_id: &str) -> Result<KafkaMemberIdentity> {
    let mut parts = member_id.split('.');
    let prefix = parts.next();
    let stream = parts.next();
    let identity = parts.next();
    if prefix != Some(KAFKA_MEMBER_PREFIX) || stream.is_none() || identity.is_none() {
        bail!("invalid Epoch Kafka member ID");
    }
    let group_instance_id = if identity == Some(KAFKA_STATIC_MEMBER_MARKER) {
        let encoded_instance = parts.next().context("static member instance is missing")?;
        if parts.next().is_some() {
            bail!("invalid Epoch Kafka member ID");
        }
        let instance = URL_SAFE_NO_PAD
            .decode(encoded_instance)
            .context("invalid group instance encoding")?;
        let instance = String::from_utf8(instance).context("group instance is not UTF-8")?;
        if instance.is_empty() || instance.len() > MAX_GROUP_INSTANCE_ID_BYTES {
            bail!("Kafka group instance ID is invalid");
        }
        Some(instance)
    } else {
        if parts.next().is_some() || uuid::Uuid::parse_str(identity.unwrap_or_default()).is_err() {
            bail!("invalid Epoch Kafka member ID");
        }
        None
    };
    let stream = URL_SAFE_NO_PAD
        .decode(stream.unwrap_or_default())
        .context("invalid stream encoding")?;
    let stream = String::from_utf8(stream).context("stream is not UTF-8")?;
    if stream.is_empty() {
        bail!("stream is empty");
    }
    Ok(KafkaMemberIdentity {
        stream,
        group_instance_id,
    })
}

fn decode_consumer_subscription(metadata: &Bytes) -> Result<String> {
    let mut input = metadata.clone();
    let version = read_i16(&mut input)?;
    if !(0..=3).contains(&version) {
        bail!("unsupported consumer subscription version");
    }
    let topic_count = read_count(&mut input)?;
    if topic_count != 1 {
        bail!("Epoch Kafka groups require exactly one topic");
    }
    let stream = read_string(&mut input)?;
    skip_nullable_bytes(&mut input)?;
    if version >= 1 {
        for _ in 0..read_count(&mut input)? {
            let _topic = read_string(&mut input)?;
            for _ in 0..read_count(&mut input)? {
                let _partition = read_i32(&mut input)?;
            }
        }
    }
    if version >= 2 {
        let _generation = read_i32(&mut input)?;
    }
    if version >= 3 {
        let _rack = read_nullable_string(&mut input)?;
    }
    if input.has_remaining() {
        bail!("consumer subscription has trailing bytes");
    }
    Ok(stream)
}

fn encode_consumer_subscription(stream: &str) -> Bytes {
    let mut output = BytesMut::new();
    output.put_i16(0);
    output.put_i32(1);
    put_string(&mut output, stream);
    output.put_i32(-1);
    output.freeze()
}

fn encode_consumer_assignment(stream: &str, partitions: &[u32]) -> Result<Bytes> {
    let mut output = BytesMut::new();
    output.put_i16(0);
    output.put_i32(1);
    put_string(&mut output, stream);
    output.put_i32(i32::try_from(partitions.len()).context("too many assigned partitions")?);
    for partition in partitions {
        output.put_i32(i32::try_from(*partition).context("partition exceeds i32")?);
    }
    output.put_i32(-1);
    Ok(output.freeze())
}

fn put_string(output: &mut BytesMut, value: &str) {
    output.put_i16(i16::try_from(value.len()).unwrap_or(i16::MAX));
    output.extend_from_slice(value.as_bytes());
}

fn read_i16(input: &mut Bytes) -> Result<i16> {
    if input.remaining() < 2 {
        bail!("consumer protocol is truncated");
    }
    Ok(input.get_i16())
}

fn read_i32(input: &mut Bytes) -> Result<i32> {
    if input.remaining() < 4 {
        bail!("consumer protocol is truncated");
    }
    Ok(input.get_i32())
}

fn read_count(input: &mut Bytes) -> Result<usize> {
    usize::try_from(read_i32(input)?)
        .ok()
        .filter(|count| *count <= MAX_CONSUMER_PROTOCOL_ITEMS)
        .context("consumer protocol item count is invalid")
}

fn read_string(input: &mut Bytes) -> Result<String> {
    let length = usize::try_from(read_i16(input)?)
        .ok()
        .filter(|length| *length <= input.remaining())
        .context("consumer protocol string length is invalid")?;
    String::from_utf8(input.copy_to_bytes(length).to_vec())
        .context("consumer protocol is not UTF-8")
}

fn read_nullable_string(input: &mut Bytes) -> Result<Option<String>> {
    let length = read_i16(input)?;
    if length == -1 {
        return Ok(None);
    }
    let length = usize::try_from(length)
        .ok()
        .filter(|length| *length <= input.remaining())
        .context("consumer protocol nullable string length is invalid")?;
    String::from_utf8(input.copy_to_bytes(length).to_vec())
        .map(Some)
        .context("consumer protocol is not UTF-8")
}

fn skip_nullable_bytes(input: &mut Bytes) -> Result<()> {
    let length = read_i32(input)?;
    if length == -1 {
        return Ok(());
    }
    let length = usize::try_from(length)
        .ok()
        .filter(|length| *length <= input.remaining())
        .context("consumer protocol byte length is invalid")?;
    input.advance(length);
    Ok(())
}

const fn group_rejection_error(rejection: StreamGroupRejection) -> ResponseError {
    match rejection {
        StreamGroupRejection::UnknownGroup | StreamGroupRejection::UnknownMember => {
            ResponseError::UnknownMemberId
        }
        StreamGroupRejection::StaleGeneration => ResponseError::IllegalGeneration,
        StreamGroupRejection::CapacityReached => ResponseError::GroupMaxSizeReached,
        StreamGroupRejection::Invalid => ResponseError::InvalidRequest,
    }
}

fn group_backend_error(error: &BackendError) -> ResponseError {
    match error {
        BackendError::NotFound => ResponseError::UnknownTopicOrPartition,
        BackendError::Conflict => ResponseError::RebalanceInProgress,
        BackendError::Unavailable(_) => ResponseError::CoordinatorNotAvailable,
        BackendError::WrongType | BackendError::Invalid(_) => ResponseError::InvalidRequest,
    }
}

async fn offset_commit_response<B: CompatibilityBackend>(
    request: kafka_protocol::messages::OffsetCommitRequest,
    backend: &B,
) -> OffsetCommitResponse {
    let group = request.group_id.to_string();
    let member_id = request.member_id.to_string();
    let requested_instance = request.group_instance_id.as_ref().map(StrBytes::as_str);
    let identity = if request.generation_id_or_member_epoch < 0 && member_id.is_empty() {
        Ok(None)
    } else if let Some(generation) = u64::try_from(request.generation_id_or_member_epoch)
        .ok()
        .filter(|generation| *generation > 0)
    {
        member_identity(&member_id).and_then(|decoded| {
            if decoded.group_instance_id.as_deref() != requested_instance {
                bail!("Kafka group instance ID does not match member ID");
            }
            Ok(Some((
                decoded.stream,
                StreamGroupIdentity {
                    member_id: member_id.clone(),
                    generation,
                },
            )))
        })
    } else {
        Err(anyhow::anyhow!("invalid Kafka group identity"))
    };
    let mut topics = Vec::with_capacity(request.topics.len());
    for topic in request.topics {
        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for partition in topic.partitions {
            let result: Result<(), ResponseError> = match (
                u32::try_from(partition.partition_index),
                u64::try_from(partition.committed_offset),
            ) {
                (Ok(partition_id), Ok(offset)) => {
                    let commit_identity = match &identity {
                        Ok(None) => Ok(None),
                        Ok(Some((stream, identity))) if stream == topic.name.as_str() => {
                            Ok(Some(identity))
                        }
                        Ok(Some(_)) => Err(ResponseError::UnknownMemberId),
                        Err(_) if requested_instance.is_some() => {
                            Err(ResponseError::FencedInstanceId)
                        }
                        Err(_) => Err(ResponseError::IllegalGeneration),
                    };
                    match commit_identity {
                        Ok(identity) => backend
                            .stream_commit_offset(
                                &group,
                                topic.name.as_str(),
                                partition_id,
                                offset,
                                identity,
                            )
                            .await
                            .map_err(|error| {
                                if identity.is_some() && matches!(error, BackendError::Conflict) {
                                    ResponseError::IllegalGeneration
                                } else {
                                    kafka_error(&error)
                                }
                            }),
                        Err(error) => Err(error),
                    }
                }
                _ => Err(ResponseError::InvalidRequest),
            };
            partitions.push(
                OffsetCommitResponsePartition::default()
                    .with_partition_index(partition.partition_index)
                    .with_error_code(result.err().as_ref().map_or(0, ResponseError::code)),
            );
        }
        topics.push(
            OffsetCommitResponseTopic::default()
                .with_name(topic.name)
                .with_partitions(partitions),
        );
    }
    OffsetCommitResponse::default().with_topics(topics)
}

async fn offset_fetch_response<B: CompatibilityBackend>(
    request: kafka_protocol::messages::OffsetFetchRequest,
    backend: &B,
) -> OffsetFetchResponse {
    let group = request.group_id.to_string();
    let mut topics = Vec::new();
    for topic in request.topics.unwrap_or_default() {
        let mut partitions = Vec::with_capacity(topic.partition_indexes.len());
        for partition in topic.partition_indexes {
            let result = match u32::try_from(partition) {
                Ok(partition_id) => {
                    backend
                        .stream_committed_offset(&group, topic.name.as_str(), partition_id)
                        .await
                }
                Err(_) => Err(BackendError::Invalid("negative Kafka partition".into())),
            };
            partitions.push(match result {
                Ok(offset) => OffsetFetchResponsePartition::default()
                    .with_partition_index(partition)
                    .with_committed_offset(
                        offset
                            .and_then(|value| i64::try_from(value).ok())
                            .unwrap_or(-1),
                    )
                    .with_committed_leader_epoch(-1),
                Err(error) => OffsetFetchResponsePartition::default()
                    .with_partition_index(partition)
                    .with_committed_offset(-1)
                    .with_committed_leader_epoch(-1)
                    .with_error_code(kafka_error(&error).code()),
            });
        }
        topics.push(
            OffsetFetchResponseTopic::default()
                .with_name(topic.name)
                .with_partitions(partitions),
        );
    }
    OffsetFetchResponse::default().with_topics(topics)
}

async fn metadata_response<B: CompatibilityBackend>(
    request: kafka_protocol::messages::MetadataRequest,
    backend: &B,
    config: &KafkaConfig,
) -> MetadataResponse {
    let broker = MetadataResponseBroker::default()
        .with_node_id(config.node_id.into())
        .with_host(StrBytes::from_string(config.advertised_host.clone()))
        .with_port(i32::from(config.port));
    let topics = request
        .topics
        .unwrap_or_default()
        .into_iter()
        .filter_map(|topic| topic.name)
        .map(|name| async {
            let result = backend.stream_partition_count(name.as_str()).await;
            metadata_topic(name, result, config.node_id)
        });
    MetadataResponse::default()
        .with_brokers(vec![broker])
        .with_cluster_id(Some(StrBytes::from_static_str("epoch-compat")))
        .with_controller_id(config.node_id.into())
        .with_topics(futures_util::future::join_all(topics).await)
}

fn metadata_topic(
    name: TopicName,
    result: Result<u32, BackendError>,
    node_id: i32,
) -> MetadataResponseTopic {
    match result {
        Ok(count) => MetadataResponseTopic::default()
            .with_name(Some(name))
            .with_partitions(
                (0..count)
                    .map(|partition| {
                        MetadataResponsePartition::default()
                            .with_partition_index(i32::try_from(partition).unwrap_or(i32::MAX))
                            .with_leader_id(node_id.into())
                            .with_leader_epoch(0)
                            .with_replica_nodes(vec![node_id.into()])
                            .with_isr_nodes(vec![node_id.into()])
                    })
                    .collect(),
            ),
        Err(error) => MetadataResponseTopic::default()
            .with_name(Some(name))
            .with_error_code(kafka_error(&error).code()),
    }
}

async fn produce_response<B: CompatibilityBackend>(
    request: kafka_protocol::messages::ProduceRequest,
    backend: &B,
) -> ProduceResponse {
    let mut responses = Vec::with_capacity(request.topic_data.len());
    for topic in request.topic_data {
        let mut partition_responses = Vec::with_capacity(topic.partition_data.len());
        for partition in topic.partition_data {
            let result = produce_partition(
                backend,
                topic.name.as_str(),
                partition.index,
                partition.records,
            )
            .await;
            partition_responses.push(match result {
                Ok(base_offset) => PartitionProduceResponse::default()
                    .with_index(partition.index)
                    .with_base_offset(i64::try_from(base_offset).unwrap_or(i64::MAX)),
                Err(error) => PartitionProduceResponse::default()
                    .with_index(partition.index)
                    .with_error_code(kafka_error(&error).code()),
            });
        }
        responses.push(
            TopicProduceResponse::default()
                .with_name(topic.name)
                .with_partition_responses(partition_responses),
        );
    }
    ProduceResponse::default().with_responses(responses)
}

async fn produce_partition<B: CompatibilityBackend>(
    backend: &B,
    stream: &str,
    partition: i32,
    records: Option<Bytes>,
) -> Result<u64, BackendError> {
    let partition = u32::try_from(partition)
        .map_err(|_| BackendError::Invalid("negative Kafka partition".into()))?;
    let mut bytes = records.ok_or_else(|| BackendError::Invalid("empty Kafka batch".into()))?;
    if bytes.len() > MAX_MESSAGE_BYTES {
        return Err(BackendError::Invalid("Kafka batch exceeds limit".into()));
    }
    let batches = validate_kafka_batch_headers(&bytes)?;
    let mut total = 0_usize;
    let mut translated = Vec::new();
    for batch in batches {
        // Metadata decoding already checked each complete magic-2 frame and CRC.
        bytes.advance(8);
        let length = usize::try_from(bytes.get_i32())
            .map_err(|_| BackendError::Invalid("negative Kafka batch length".into()))?;
        let mut frame = bytes.split_to(length);
        frame.advance(49);
        let decoded = (|| -> Result<_> {
            let decompressed = decompress_kafka_records_bounded(&mut frame, batch.compression)?;
            total = total
                .checked_add(decompressed.len())
                .context("Kafka expansion overflow")?;
            anyhow::ensure!(total <= MAX_MESSAGE_BYTES, "Kafka expansion exceeds limit");
            decode_records(decompressed, &batch)
        })()
        .map_err(|_| BackendError::Invalid("malformed or oversized Kafka record batch".into()))?;
        translated.extend(decoded);
    }
    backend.stream_append(stream, partition, translated).await
}

fn validate_kafka_batch_headers(records: &Bytes) -> Result<Vec<BatchDecodeInfo>, BackendError> {
    let mut headers = records.clone();
    let batches = RecordBatchDecoder::decode_batch_info(&mut headers)
        .map_err(|_| BackendError::Invalid("malformed Kafka record batch".into()))?;
    if batches.is_empty() || headers.has_remaining() {
        return Err(BackendError::Invalid(
            "unsupported or malformed Kafka record batch".into(),
        ));
    }
    let mut record_count = 0_usize;
    for batch in &batches {
        if batch.transactional || batch.control {
            return Err(BackendError::Invalid(
                "Kafka transactional/control batches are unsupported".into(),
            ));
        }
        if batch.producer_id != -1 || batch.producer_epoch != -1 {
            return Err(BackendError::Invalid(
                "Kafka idempotent producer identity is unsupported".into(),
            ));
        }
        let batch_count = usize::try_from(batch.record_count)
            .map_err(|_| BackendError::Invalid("negative Kafka record count".into()))?;
        record_count = record_count
            .checked_add(batch_count)
            .ok_or_else(|| BackendError::Invalid("Kafka record count overflow".into()))?;
        if record_count > MAX_KAFKA_RECORDS_PER_PRODUCE_PARTITION {
            return Err(BackendError::Invalid(format!(
                "Kafka record count exceeds limit of {MAX_KAFKA_RECORDS_PER_PRODUCE_PARTITION}"
            )));
        }
    }
    if record_count == 0 {
        return Err(BackendError::Invalid("empty Kafka batch".into()));
    }
    Ok(batches)
}

fn decompress_kafka_records_bounded(
    compressed: &mut Bytes,
    compression: Compression,
) -> Result<Bytes> {
    match compression {
        Compression::None => {
            if compressed.len() > MAX_MESSAGE_BYTES {
                bail!("Kafka record data exceeds {MAX_MESSAGE_BYTES} bytes");
            }
            Ok(compressed.copy_to_bytes(compressed.remaining()))
        }
        Compression::Gzip => {
            let input = compressed.copy_to_bytes(compressed.remaining());
            read_kafka_records_bounded(MultiGzDecoder::new(Cursor::new(input)))
        }
        Compression::Snappy => decompress_kafka_snappy_bounded(compressed),
        Compression::Lz4 => {
            let input = compressed.copy_to_bytes(compressed.remaining());
            let decoder = lz4::Decoder::new(Cursor::new(input))
                .context("failed to initialize Kafka LZ4 decoder")?;
            read_kafka_records_bounded(decoder)
        }
        Compression::Zstd => {
            let input = compressed.copy_to_bytes(compressed.remaining());
            let mut decoder = zstd::stream::read::Decoder::new(Cursor::new(input))
                .context("failed to initialize Kafka Zstandard decoder")?;
            decoder
                .window_log_max(ZSTD_MAX_WINDOW_LOG)
                .context("failed to bound Kafka Zstandard window")?;
            read_kafka_records_bounded(decoder)
        }
    }
}

fn read_kafka_records_bounded(reader: impl std::io::Read) -> Result<Bytes> {
    let limit =
        u64::try_from(MAX_MESSAGE_BYTES).context("Kafka message limit does not fit u64")? + 1;
    let mut output = Vec::new();
    reader
        .take(limit)
        .read_to_end(&mut output)
        .context("failed to decompress Kafka record data")?;
    if output.len() > MAX_MESSAGE_BYTES {
        bail!("Kafka decompressed record data exceeds {MAX_MESSAGE_BYTES} bytes");
    }
    Ok(output.into())
}

fn decompress_kafka_snappy_bounded(compressed: &mut Bytes) -> Result<Bytes> {
    if !compressed.starts_with(KAFKA_SNAPPY_MAGIC_HEADER) {
        let block = compressed.copy_to_bytes(compressed.remaining());
        return decompress_kafka_snappy_block(&block, Vec::new()).map(Into::into);
    }
    compressed.advance(KAFKA_SNAPPY_MAGIC_HEADER.len());
    let mut output = Vec::new();
    while compressed.has_remaining() {
        if compressed.remaining() < std::mem::size_of::<u32>() {
            bail!("truncated Kafka Snappy block length");
        }
        let compressed_length = usize::try_from(compressed.get_u32())
            .context("Kafka Snappy block length does not fit usize")?;
        if compressed_length == 0 || compressed_length > compressed.remaining() {
            bail!("invalid Kafka Snappy block length");
        }
        let block = compressed.copy_to_bytes(compressed_length);
        output = decompress_kafka_snappy_block(&block, output)?;
    }
    if output.is_empty() {
        bail!("empty Kafka Snappy stream");
    }
    Ok(output.into())
}

fn decompress_kafka_snappy_block(block: &[u8], mut output: Vec<u8>) -> Result<Vec<u8>> {
    let uncompressed_length =
        snap::raw::decompress_len(block).context("invalid Kafka Snappy block header")?;
    let next_length = output
        .len()
        .checked_add(uncompressed_length)
        .context("Kafka Snappy output length overflow")?;
    if next_length > MAX_MESSAGE_BYTES {
        bail!("Kafka Snappy data exceeds {MAX_MESSAGE_BYTES} decompressed bytes");
    }
    let start = output.len();
    output.resize(next_length, 0);
    let actual = snap::raw::Decoder::new()
        .decompress(block, &mut output[start..])
        .context("failed to decompress Kafka Snappy block")?;
    if actual != uncompressed_length {
        bail!("Kafka Snappy block length mismatch");
    }
    Ok(output)
}

async fn fetch_response<B: CompatibilityBackend>(
    request: kafka_protocol::messages::FetchRequest,
    backend: &B,
) -> FetchResponse {
    let mut topics = Vec::with_capacity(request.topics.len());
    for topic in request.topics {
        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for partition in topic.partitions {
            let result = fetch_partition(
                backend,
                topic.topic.as_str(),
                partition.partition,
                partition.fetch_offset,
            )
            .await;
            partitions.push(match result {
                Ok((high_watermark, records)) => PartitionData::default()
                    .with_partition_index(partition.partition)
                    .with_high_watermark(i64::try_from(high_watermark).unwrap_or(i64::MAX))
                    .with_last_stable_offset(i64::try_from(high_watermark).unwrap_or(i64::MAX))
                    .with_records(Some(records)),
                Err(error) => PartitionData::default()
                    .with_partition_index(partition.partition)
                    .with_error_code(kafka_error(&error).code()),
            });
        }
        topics.push(
            FetchableTopicResponse::default()
                .with_topic(topic.topic)
                .with_partitions(partitions),
        );
    }
    FetchResponse::default().with_responses(topics)
}

async fn fetch_partition<B: CompatibilityBackend>(
    backend: &B,
    stream: &str,
    partition: i32,
    offset: i64,
) -> Result<(u64, Bytes), BackendError> {
    let partition = u32::try_from(partition)
        .map_err(|_| BackendError::Invalid("negative Kafka partition".into()))?;
    let offset =
        u64::try_from(offset).map_err(|_| BackendError::Invalid("negative Kafka offset".into()))?;
    let records = backend
        .stream_fetch(stream, partition, offset, 1_000)
        .await?;
    let high_watermark = backend.stream_end_offset(stream, partition).await?;
    let encoded = encode_records(&records)
        .map_err(|_| BackendError::Unavailable("Kafka response encoding failed".into()))?;
    Ok((high_watermark, encoded))
}

async fn list_offsets_response<B: CompatibilityBackend>(
    request: kafka_protocol::messages::ListOffsetsRequest,
    backend: &B,
) -> ListOffsetsResponse {
    let mut topics = Vec::with_capacity(request.topics.len());
    for topic in request.topics {
        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for partition in topic.partitions {
            let partition_id = partition.partition_index;
            let result = match (u32::try_from(partition_id), partition.timestamp) {
                (Ok(_partition_id), -2) => Ok(0),
                (Ok(partition_id), -1) => {
                    backend
                        .stream_end_offset(topic.name.as_str(), partition_id)
                        .await
                }
                (Ok(_), _) => Err(BackendError::Invalid(
                    "timestamp-based Kafka offset lookup is unsupported".into(),
                )),
                (Err(_), _) => Err(BackendError::Invalid("negative Kafka partition".into())),
            };
            partitions.push(match result {
                Ok(offset) => ListOffsetsPartitionResponse::default()
                    .with_partition_index(partition_id)
                    .with_offset(i64::try_from(offset).unwrap_or(i64::MAX))
                    .with_timestamp(partition.timestamp),
                Err(error) => ListOffsetsPartitionResponse::default()
                    .with_partition_index(partition_id)
                    .with_error_code(kafka_error(&error).code()),
            });
        }
        topics.push(
            ListOffsetsTopicResponse::default()
                .with_name(topic.name)
                .with_partitions(partitions),
        );
    }
    ListOffsetsResponse::default().with_topics(topics)
}

fn supported(key: ApiKey, version: i16) -> bool {
    SUPPORTED_APIS
        .iter()
        .any(|(candidate, min, max)| *candidate == key && (*min..=*max).contains(&version))
}

fn kafka_error(error: &BackendError) -> ResponseError {
    match error {
        BackendError::NotFound => ResponseError::UnknownTopicOrPartition,
        BackendError::Conflict => ResponseError::NotLeaderOrFollower,
        BackendError::Invalid(detail) if detail.contains("exceeds limit") => {
            ResponseError::MessageTooLarge
        }
        BackendError::WrongType | BackendError::Invalid(_) => ResponseError::InvalidRequest,
        BackendError::Unavailable(_) => ResponseError::BrokerNotAvailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::MemoryBackend;
    use kafka_protocol::{
        messages::{
            ApiVersionsRequest, FindCoordinatorRequest, GroupId, HeartbeatRequest,
            JoinGroupRequest, LeaveGroupRequest, OffsetCommitRequest, OffsetFetchRequest,
            RequestHeader, SyncGroupRequest,
            join_group_request::JoinGroupRequestProtocol,
            leave_group_request::MemberIdentity,
            offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
            offset_fetch_request::OffsetFetchRequestTopic,
        },
        protocol::Decodable,
        records::{Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType},
    };

    fn config() -> KafkaConfig {
        KafkaConfig {
            advertised_host: "broker.test".into(),
            port: 9_092,
            node_id: 7,
            max_connections: 8,
        }
    }

    fn encode_request<T: Encodable>(api_key: ApiKey, version: i16, request: &T) -> Bytes {
        let mut bytes = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(api_key as i16)
            .with_request_api_version(version)
            .with_correlation_id(42)
            .with_client_id(Some(StrBytes::from_static_str("epoch-test")))
            .encode(&mut bytes, api_key.request_header_version(version))
            .unwrap();
        request.encode(&mut bytes, version).unwrap();
        bytes.freeze()
    }

    #[tokio::test]
    async fn api_versions_wire_response_advertises_only_dispatched_versions() {
        let backend = MemoryBackend::with_resources("sessions", "events", 2, "jobs");
        let request = ApiVersionsRequest::default()
            .with_client_software_name(StrBytes::from_static_str("epoch-test"))
            .with_client_software_version(StrBytes::from_static_str("1"));
        let response = handle_frame(
            encode_request(ApiKey::ApiVersions, 4, &request),
            &backend,
            &config(),
        )
        .await
        .unwrap()
        .unwrap();
        let mut bytes = Bytes::from(response);
        let payload_length = usize::try_from(bytes.get_i32()).unwrap();
        assert_eq!(payload_length, bytes.remaining());
        let header =
            ResponseHeader::decode(&mut bytes, ApiKey::ApiVersions.response_header_version(4))
                .unwrap();
        assert_eq!(header.correlation_id, 42);
        let response = ApiVersionsResponse::decode(&mut bytes, 4).unwrap();
        let actual = response
            .api_keys
            .iter()
            .map(|api| (api.api_key, api.min_version, api.max_version))
            .collect::<Vec<_>>();
        let expected = SUPPORTED_APIS
            .iter()
            .map(|(key, min, max)| (*key as i16, *min, *max))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        assert!(!bytes.has_remaining());
    }

    #[tokio::test]
    async fn translates_all_advertised_record_compressions_and_preserves_data() {
        let backend = MemoryBackend::with_resources("sessions", "events", 2, "jobs");
        for (index, compression) in [
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ]
        .into_iter()
        .enumerate()
        {
            let value = format!("value-{index}");
            let record = Record {
                transactional: false,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: 0,
                producer_id: -1,
                producer_epoch: -1,
                timestamp_type: TimestampType::Creation,
                offset: 0,
                sequence: -1,
                timestamp: 1_700_000_000_000 + i64::try_from(index).unwrap(),
                key: Some(Bytes::from(format!("key-{index}"))),
                value: Some(Bytes::from(value.clone())),
                headers: [(
                    StrBytes::from_static_str("traceparent"),
                    Some(Bytes::from_static(b"00-test")),
                )]
                .into_iter()
                .collect(),
            };
            let mut batch = BytesMut::new();
            RecordBatchEncoder::encode(
                &mut batch,
                &[record],
                &RecordEncodeOptions {
                    version: 2,
                    compression,
                },
            )
            .unwrap();
            let offset = produce_partition(&backend, "events", 1, Some(batch.freeze()))
                .await
                .unwrap();
            assert_eq!(offset, u64::try_from(index).unwrap());
            let (high_watermark, mut fetched) =
                fetch_partition(&backend, "events", 1, i64::try_from(index).unwrap())
                    .await
                    .unwrap();
            assert_eq!(high_watermark, u64::try_from(index + 1).unwrap());
            let decoded = RecordBatchDecoder::decode_all(&mut fetched).unwrap();
            assert_eq!(decoded.len(), 1);
            assert_eq!(decoded[0].records.len(), 1);
            assert_eq!(
                decoded[0].records[0].value.as_deref(),
                Some(value.as_bytes())
            );
            assert_eq!(decoded[0].records[0].offset, i64::try_from(index).unwrap());
        }
    }

    #[tokio::test]
    async fn rejects_compressed_record_data_that_expands_past_the_hard_limit() {
        let backend = MemoryBackend::with_resources("sessions", "events", 2, "jobs");
        let record = test_record(Bytes::from(vec![b'x'; MAX_MESSAGE_BYTES + 1]), 0);
        let mut batch = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut batch,
            &[record],
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::Gzip,
            },
        )
        .unwrap();
        assert!(batch.len() < MAX_MESSAGE_BYTES);

        let error = produce_partition(&backend, "events", 0, Some(batch.freeze()))
            .await
            .unwrap_err();
        assert!(matches!(error, BackendError::Invalid(message) if message.contains("oversized")));
    }

    #[tokio::test]
    async fn rejects_record_counts_before_the_decoder_can_reserve_untrusted_capacity() {
        let backend = MemoryBackend::with_resources("sessions", "events", 2, "jobs");
        let records = (0..=MAX_KAFKA_RECORDS_PER_PRODUCE_PARTITION)
            .map(|index| test_record(Bytes::from_static(b"x"), index))
            .collect::<Vec<_>>();
        let mut batch = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut batch,
            &records,
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::Gzip,
            },
        )
        .unwrap();

        let error = produce_partition(&backend, "events", 0, Some(batch.freeze()))
            .await
            .unwrap_err();
        assert!(
            matches!(error, BackendError::Invalid(message) if message.contains("record count"))
        );
    }

    #[tokio::test]
    async fn rejects_unimplemented_idempotent_producer_identity() {
        let backend = MemoryBackend::with_resources("sessions", "events", 2, "jobs");
        let mut record = test_record(Bytes::from_static(b"uncommitted"), 0);
        record.producer_id = 42;
        record.producer_epoch = 1;
        let mut batch = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut batch,
            &[record],
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .unwrap();
        assert!(matches!(
            produce_partition(&backend, "events", 0, Some(batch.freeze())).await,
            Err(BackendError::Invalid(_))
        ));
        assert_eq!(backend.stream_end_offset("events", 0).await.unwrap(), 0);
    }

    fn test_record(value: Bytes, sequence: usize) -> Record {
        Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: i64::try_from(sequence).unwrap(),
            sequence: i32::try_from(sequence).unwrap(),
            timestamp: 1_700_000_000_000,
            key: None,
            value: Some(value),
            headers: [].into_iter().collect(),
        }
    }

    #[tokio::test]
    async fn metadata_maps_every_native_partition_to_the_advertised_broker() {
        let backend = MemoryBackend::with_resources("sessions", "events", 3, "jobs");
        let topic = metadata_topic(
            TopicName(StrBytes::from_static_str("events")),
            backend.stream_partition_count("events").await,
            config().node_id,
        );
        assert_eq!(topic.error_code, 0);
        assert_eq!(topic.partitions.len(), 3);
        assert!(topic.partitions.iter().all(|partition| {
            partition.leader_id == config().node_id
                && partition.replica_nodes.len() == 1
                && partition.replica_nodes[0] == 7
        }));
    }

    #[test]
    fn coordinator_response_encodes_for_every_advertised_version() {
        for version in 0..=4 {
            let key = StrBytes::from_static_str("billing");
            let request = if version >= 4 {
                FindCoordinatorRequest::default().with_coordinator_keys(vec![key])
            } else {
                FindCoordinatorRequest::default().with_key(key)
            };
            let response = find_coordinator_response(request, &config(), version);
            let mut encoded = BytesMut::new();
            response.encode(&mut encoded, version).unwrap();
            let decoded = FindCoordinatorResponse::decode(&mut encoded.freeze(), version).unwrap();
            if version >= 4 {
                assert_eq!(decoded.coordinators.len(), 1);
                assert_eq!(decoded.coordinators[0].node_id, config().node_id);
            } else {
                assert_eq!(decoded.node_id, config().node_id);
            }
        }
    }

    #[tokio::test]
    async fn commits_and_fetches_manual_consumer_offsets_through_the_native_checkpoint_port() {
        let backend = MemoryBackend::with_resources("sessions", "events", 3, "jobs");
        let commit = OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("billing")))
            .with_topics(vec![
                OffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from_static_str("events")))
                    .with_partitions(vec![
                        OffsetCommitRequestPartition::default()
                            .with_partition_index(2)
                            .with_committed_offset(73),
                    ]),
            ]);
        let committed = offset_commit_response(commit, &backend).await;
        assert_eq!(committed.topics[0].partitions[0].error_code, 0);

        let fetch = OffsetFetchRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("billing")))
            .with_topics(Some(vec![
                OffsetFetchRequestTopic::default()
                    .with_name(TopicName(StrBytes::from_static_str("events")))
                    .with_partition_indexes(vec![2, 1]),
            ]));
        let fetched = offset_fetch_response(fetch, &backend).await;
        assert_eq!(fetched.topics[0].partitions[0].committed_offset, 73);
        assert_eq!(fetched.topics[0].partitions[1].committed_offset, -1);
    }

    #[tokio::test]
    async fn classic_consumer_group_uses_native_generation_fences_and_assigns_every_partition() {
        let backend = MemoryBackend::with_resources("sessions", "events", 7, "jobs");
        let initial = join_request("", "events");
        let identified = join_group_response(initial, &backend, 9).await;
        assert_eq!(
            identified.error_code,
            ResponseError::MemberIdRequired.code()
        );
        let first_member = identified.member_id.to_string();

        let first_join =
            join_group_response(join_request(&first_member, "events"), &backend, 9).await;
        assert_eq!(first_join.error_code, 0);
        assert_eq!(first_join.generation_id, 1);

        let second_identity = join_group_response(join_request("", "events"), &backend, 9).await;
        let second_member = second_identity.member_id.to_string();
        let second_join =
            join_group_response(join_request(&second_member, "events"), &backend, 9).await;
        assert_eq!(second_join.error_code, 0);
        assert_eq!(second_join.generation_id, 2);

        let first_rejoin =
            join_group_response(join_request(&first_member, "events"), &backend, 9).await;
        assert_eq!(first_rejoin.error_code, 0);
        assert_eq!(first_rejoin.generation_id, 2);

        let first_sync = sync_group_response(sync_request(&first_member, 2), &backend, 5).await;
        let second_sync = sync_group_response(sync_request(&second_member, 2), &backend, 5).await;
        assert_eq!(first_sync.error_code, 0);
        assert_eq!(second_sync.error_code, 0);
        let (first_stream, first_partitions) = decode_assignment(first_sync.assignment).unwrap();
        let (second_stream, second_partitions) = decode_assignment(second_sync.assignment).unwrap();
        assert_eq!(first_stream, "events");
        assert_eq!(second_stream, "events");
        assert!(
            first_partitions
                .iter()
                .all(|item| !second_partitions.contains(item))
        );
        let first_owned = first_partitions[0];
        let committed = offset_commit_response(
            group_offset_commit(&first_member, 2, first_owned, 0),
            &backend,
        )
        .await;
        assert_eq!(committed.topics[0].partitions[0].error_code, 0);
        let stale_commit = offset_commit_response(
            group_offset_commit(&first_member, 1, first_owned, 0),
            &backend,
        )
        .await;
        assert_eq!(
            stale_commit.topics[0].partitions[0].error_code,
            ResponseError::IllegalGeneration.code()
        );
        let mut assigned = first_partitions;
        assigned.extend(second_partitions);
        assigned.sort_unstable();
        assert_eq!(assigned, (0..7).collect::<Vec<_>>());

        let stale = heartbeat_response(heartbeat_request(&first_member, 1), &backend).await;
        assert_eq!(stale.error_code, ResponseError::IllegalGeneration.code());
        let current = heartbeat_response(heartbeat_request(&first_member, 2), &backend).await;
        assert_eq!(current.error_code, 0);

        let leave = LeaveGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("billing")))
            .with_members(vec![
                MemberIdentity::default()
                    .with_member_id(StrBytes::from_string(second_member.clone())),
            ]);
        let left = leave_group_response(leave, &backend, 5).await;
        assert_eq!(left.error_code, 0);
        assert_eq!(left.members[0].error_code, 0);
        let departed = heartbeat_response(heartbeat_request(&second_member, 3), &backend).await;
        assert_eq!(departed.error_code, ResponseError::UnknownMemberId.code());
    }

    async fn join_static_member(backend: &MemoryBackend) -> (String, StrBytes) {
        let instance = StrBytes::from_static_str("billing-worker-a");
        let initial = join_request("", "events").with_group_instance_id(Some(instance.clone()));
        let identified = join_group_response(initial, backend, 9).await;
        assert_eq!(
            identified.error_code,
            ResponseError::MemberIdRequired.code()
        );
        let member = identified.member_id.to_string();
        let joined = join_group_response(
            join_request(&member, "events").with_group_instance_id(Some(instance.clone())),
            backend,
            9,
        )
        .await;
        assert_eq!(joined.error_code, 0);
        assert_eq!(joined.generation_id, 1);
        assert_eq!(
            joined.members[0].group_instance_id.as_ref(),
            Some(&instance)
        );
        (member, instance)
    }

    #[tokio::test]
    async fn static_consumer_members_reuse_their_bounded_identity() {
        let backend = MemoryBackend::with_resources("sessions", "events", 3, "jobs");
        for invalid in [String::new(), "x".repeat(MAX_GROUP_INSTANCE_ID_BYTES + 1)] {
            let response = join_group_response(
                join_request("", "events")
                    .with_group_instance_id(Some(StrBytes::from_string(invalid))),
                &backend,
                9,
            )
            .await;
            assert_eq!(response.error_code, ResponseError::InvalidRequest.code());
        }
        let (member, instance) = join_static_member(&backend).await;

        let rejoined = join_group_response(
            join_request(&member, "events").with_group_instance_id(Some(instance.clone())),
            &backend,
            9,
        )
        .await;
        assert_eq!(rejoined.error_code, 0);
        assert_eq!(rejoined.generation_id, 1);

        let mismatched = join_group_response(
            join_request(&member, "events")
                .with_group_instance_id(Some(StrBytes::from_static_str("billing-worker-b"))),
            &backend,
            9,
        )
        .await;
        assert_eq!(
            mismatched.error_code,
            ResponseError::FencedInstanceId.code()
        );
    }

    #[tokio::test]
    async fn static_consumer_group_operations_fence_instance_mismatches() {
        let backend = MemoryBackend::with_resources("sessions", "events", 3, "jobs");
        let (member, instance) = join_static_member(&backend).await;
        let sync = sync_group_response(
            sync_request(&member, 1).with_group_instance_id(Some(instance.clone())),
            &backend,
            5,
        )
        .await;
        assert_eq!(sync.error_code, 0);
        assert_eq!(
            sync_group_response(sync_request(&member, 1), &backend, 5)
                .await
                .error_code,
            ResponseError::FencedInstanceId.code()
        );
        let heartbeat = heartbeat_response(
            heartbeat_request(&member, 1).with_group_instance_id(Some(instance.clone())),
            &backend,
        )
        .await;
        assert_eq!(heartbeat.error_code, 0);
        assert_eq!(
            heartbeat_response(
                heartbeat_request(&member, 1)
                    .with_group_instance_id(Some(StrBytes::from_static_str("billing-worker-b"),)),
                &backend,
            )
            .await
            .error_code,
            ResponseError::FencedInstanceId.code()
        );

        let mut commit = group_offset_commit(&member, 1, 0, 4);
        commit.group_instance_id = Some(instance.clone());
        assert_eq!(
            offset_commit_response(commit, &backend).await.topics[0].partitions[0].error_code,
            0
        );
        let mut mismatched_commit = group_offset_commit(&member, 1, 0, 5);
        mismatched_commit.group_instance_id = Some(StrBytes::from_static_str("billing-worker-b"));
        assert_eq!(
            offset_commit_response(mismatched_commit, &backend)
                .await
                .topics[0]
                .partitions[0]
                .error_code,
            ResponseError::FencedInstanceId.code()
        );

        let fenced_leave = LeaveGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("billing")))
            .with_members(vec![
                MemberIdentity::default()
                    .with_member_id(StrBytes::from_string(member.clone()))
                    .with_group_instance_id(Some(StrBytes::from_static_str("billing-worker-b"))),
            ]);
        let fenced_leave = leave_group_response(fenced_leave, &backend, 5).await;
        assert_eq!(fenced_leave.error_code, 0);
        assert_eq!(
            fenced_leave.members[0].error_code,
            ResponseError::FencedInstanceId.code()
        );
        assert_eq!(
            heartbeat_response(
                heartbeat_request(&member, 1).with_group_instance_id(Some(instance.clone())),
                &backend,
            )
            .await
            .error_code,
            0
        );

        let leave = LeaveGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("billing")))
            .with_members(vec![
                MemberIdentity::default()
                    .with_member_id(StrBytes::from_string(member.clone()))
                    .with_group_instance_id(Some(instance)),
            ]);
        let left = leave_group_response(leave, &backend, 5).await;
        assert_eq!(left.error_code, 0);
        assert_eq!(left.members[0].error_code, 0);
    }

    #[tokio::test]
    async fn consumer_groups_reject_multi_topic_and_non_range_subscriptions_without_mutation() {
        let backend = MemoryBackend::with_resources("sessions", "events", 2, "jobs");
        let mut multiple = encode_consumer_subscription("events").to_vec();
        multiple[2..6].copy_from_slice(&2_i32.to_be_bytes());
        let invalid = JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("billing")))
            .with_session_timeout_ms(30_000)
            .with_rebalance_timeout_ms(30_000)
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from_static_str("range"))
                    .with_metadata(Bytes::from(multiple)),
            ]);
        assert_eq!(
            join_group_response(invalid, &backend, 9).await.error_code,
            ResponseError::InvalidRequest.code()
        );

        let unsupported = JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("billing")))
            .with_session_timeout_ms(30_000)
            .with_rebalance_timeout_ms(30_000)
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from_static_str("cooperative-sticky"))
                    .with_metadata(encode_consumer_subscription("events")),
            ]);
        assert_eq!(
            join_group_response(unsupported, &backend, 9)
                .await
                .error_code,
            ResponseError::InconsistentGroupProtocol.code()
        );
        assert!(
            backend
                .stream_group_observe("events", "billing", "missing")
                .await
                .unwrap()
                .is_none()
        );
    }

    fn join_request(member_id: &str, stream: &str) -> JoinGroupRequest {
        JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("billing")))
            .with_session_timeout_ms(30_000)
            .with_rebalance_timeout_ms(30_000)
            .with_member_id(StrBytes::from_string(member_id.to_owned()))
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from_static_str("range"))
                    .with_metadata(encode_consumer_subscription(stream)),
            ])
    }

    fn sync_request(member_id: &str, generation: i32) -> SyncGroupRequest {
        SyncGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("billing")))
            .with_generation_id(generation)
            .with_member_id(StrBytes::from_string(member_id.to_owned()))
            .with_protocol_type(Some(StrBytes::from_static_str("consumer")))
            .with_protocol_name(Some(StrBytes::from_static_str("range")))
    }

    fn heartbeat_request(member_id: &str, generation: i32) -> HeartbeatRequest {
        HeartbeatRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("billing")))
            .with_generation_id(generation)
            .with_member_id(StrBytes::from_string(member_id.to_owned()))
    }

    fn group_offset_commit(
        member_id: &str,
        generation: i32,
        partition: u32,
        offset: i64,
    ) -> OffsetCommitRequest {
        OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("billing")))
            .with_generation_id_or_member_epoch(generation)
            .with_member_id(StrBytes::from_string(member_id.to_owned()))
            .with_topics(vec![
                OffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from_static_str("events")))
                    .with_partitions(vec![
                        OffsetCommitRequestPartition::default()
                            .with_partition_index(i32::try_from(partition).unwrap())
                            .with_committed_offset(offset),
                    ]),
            ])
    }

    fn decode_assignment(mut assignment: Bytes) -> Result<(String, Vec<u32>)> {
        if read_i16(&mut assignment)? != 0 || read_count(&mut assignment)? != 1 {
            bail!("unexpected assignment envelope");
        }
        let stream = read_string(&mut assignment)?;
        let partitions = (0..read_count(&mut assignment)?)
            .map(|_| {
                u32::try_from(read_i32(&mut assignment)?).context("negative assigned partition")
            })
            .collect::<Result<Vec<_>>>()?;
        skip_nullable_bytes(&mut assignment)?;
        if assignment.has_remaining() {
            bail!("assignment has trailing bytes");
        }
        Ok((stream, partitions))
    }
}

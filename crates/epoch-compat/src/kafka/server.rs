use std::{
    cell::Cell,
    io::{Cursor, Read as _},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use flate2::read::MultiGzDecoder;
use kafka_protocol::{
    ResponseError,
    messages::{
        ApiKey, ApiVersionsResponse, FetchResponse, FindCoordinatorResponse, ListOffsetsResponse,
        MetadataResponse, OffsetCommitResponse, OffsetFetchResponse, ProduceResponse, RequestKind,
        ResponseHeader, ResponseKind, TopicName,
        api_versions_response::ApiVersion,
        fetch_response::{FetchableTopicResponse, PartitionData},
        find_coordinator_response::Coordinator,
        list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
        metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        },
        offset_commit_response::{OffsetCommitResponsePartition, OffsetCommitResponseTopic},
        offset_fetch_response::{OffsetFetchResponsePartition, OffsetFetchResponseTopic},
        produce_response::{PartitionProduceResponse, TopicProduceResponse},
    },
    protocol::{Encodable, StrBytes, decode_request_header_from_buffer},
    records::{
        Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
        TimestampType,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{
    CompatibilityBackend, MAX_FRAME_BYTES, MAX_MESSAGE_BYTES,
    backend::{BackendError, StreamRecord},
};

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
        Ok(Self { backend, config })
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
            tokio::spawn(async move {
                let _permit = permit;
                if let Err(error) = serve_connection(stream, backend, config).await {
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
        if let Some(response) = handle_frame(Bytes::from(frame), backend.as_ref(), &config).await? {
            stream.write_all(&response).await?;
        }
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

async fn offset_commit_response<B: CompatibilityBackend>(
    request: kafka_protocol::messages::OffsetCommitRequest,
    backend: &B,
) -> OffsetCommitResponse {
    let group = request.group_id.to_string();
    let mut topics = Vec::with_capacity(request.topics.len());
    for topic in request.topics {
        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for partition in topic.partitions {
            let result = match (
                u32::try_from(partition.partition_index),
                u64::try_from(partition.committed_offset),
            ) {
                (Ok(partition_id), Ok(offset)) => {
                    backend
                        .stream_commit_offset(&group, topic.name.as_str(), partition_id, offset)
                        .await
                }
                _ => Err(BackendError::Invalid(
                    "negative Kafka partition or committed offset".into(),
                )),
            };
            partitions.push(
                OffsetCommitResponsePartition::default()
                    .with_partition_index(partition.partition_index)
                    .with_error_code(
                        result
                            .err()
                            .as_ref()
                            .map_or(0, |error| kafka_error(error).code()),
                    ),
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
    validate_kafka_batch_headers(&bytes)?;

    let translated = {
        let decompressed_bytes = Cell::new(0_usize);
        let decompressor = |compressed: &mut Bytes, compression: Compression| {
            let decompressed = decompress_kafka_records_bounded(compressed, compression)?;
            let total = decompressed_bytes
                .get()
                .checked_add(decompressed.len())
                .context("Kafka decompressed batch size overflow")?;
            if total > MAX_MESSAGE_BYTES {
                bail!("Kafka decompressed batches exceed {MAX_MESSAGE_BYTES} bytes");
            }
            decompressed_bytes.set(total);
            Ok(decompressed)
        };
        let mut translated = Vec::with_capacity(MAX_KAFKA_RECORDS_PER_PRODUCE_PARTITION);
        while bytes.has_remaining() {
            let batch =
                RecordBatchDecoder::decode_with_custom_compression(&mut bytes, Some(&decompressor))
                    .map_err(|_| {
                        BackendError::Invalid("malformed or oversized Kafka record batch".into())
                    })?;
            for record in batch.records {
                if record.transactional || record.control {
                    return Err(BackendError::Invalid(
                        "Kafka transactional/control batches are unsupported".into(),
                    ));
                }
                translated.push(StreamRecord {
                    offset: 0,
                    timestamp_ms: u64::try_from(record.timestamp).unwrap_or(0),
                    key: record.key.map(|value| value.to_vec()),
                    value: record.value.map(|value| value.to_vec()),
                    headers: record
                        .headers
                        .into_iter()
                        .map(|(name, value)| (name.to_string(), value.map(|value| value.to_vec())))
                        .collect(),
                });
            }
        }
        translated
    };
    backend.stream_append(stream, partition, translated).await
}

fn validate_kafka_batch_headers(records: &Bytes) -> Result<(), BackendError> {
    let mut headers = records.clone();
    let batches = RecordBatchDecoder::decode_batch_info(&mut headers)
        .map_err(|_| BackendError::Invalid("malformed Kafka record batch".into()))?;
    if batches.is_empty() || headers.has_remaining() {
        return Err(BackendError::Invalid(
            "unsupported or malformed Kafka record batch".into(),
        ));
    }
    let mut record_count = 0_usize;
    for batch in batches {
        if batch.transactional || batch.control {
            return Err(BackendError::Invalid(
                "Kafka transactional/control batches are unsupported".into(),
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
    Ok(())
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
    let records = records
        .into_iter()
        .map(|record| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: i64::try_from(record.offset).unwrap_or(i64::MAX),
            sequence: -1,
            timestamp: i64::try_from(record.timestamp_ms).unwrap_or(i64::MAX),
            key: record.key.map(Bytes::from),
            value: record.value.map(Bytes::from),
            headers: record
                .headers
                .into_iter()
                .map(|(name, value)| (StrBytes::from_string(name), value.map(Bytes::from)))
                .collect(),
        })
        .collect::<Vec<_>>();
    let mut encoded = BytesMut::new();
    if !records.is_empty() {
        RecordBatchEncoder::encode(
            &mut encoded,
            &records,
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .map_err(|_| BackendError::Unavailable("Kafka response encoding failed".into()))?;
    }
    Ok((high_watermark, encoded.freeze()))
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
        BackendError::Invalid(_) => ResponseError::InvalidRequest,
        BackendError::Unavailable(_) => ResponseError::BrokerNotAvailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::MemoryBackend;
    use kafka_protocol::{
        messages::{
            ApiVersionsRequest, FindCoordinatorRequest, GroupId, OffsetCommitRequest,
            OffsetFetchRequest, RequestHeader,
            offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
            offset_fetch_request::OffsetFetchRequestTopic,
        },
        protocol::Decodable,
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
}

package epoch

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	regionalAuthorizationHeader = "authorization"
	regionalGenerationHeader    = "x-epoch-resource-generation"
	regionalTabletEpochHeader   = "x-epoch-tablet-epoch"
	regionalReadHeader          = "x-epoch-read-consistency"
	maxRegionalFetchRecords     = 1_000
	maxStreamRetentionRecords   = 100_000
	maxStreamRetentionBytes     = 3 * 1024 * 1024
	maxStreamRetentionAgeMS     = 10 * 365 * 24 * 60 * 60 * 1_000
	minStreamSessionTimeout     = time.Second
	maxStreamSessionTimeout     = 5 * time.Minute
	maxStreamBatchRecords       = 1_000
	maxStreamBatchCompressed    = 360 * 1024
	maxStreamBatchUncompressed  = 4 * 1024 * 1024
	maxStreamClaimTransitions   = 4_096
	minStreamCaptureInterval    = time.Second
	maxStreamCaptureInterval    = 31 * 24 * time.Hour
	streamPartitioner           = "fnv1a64_utf8_mod_n_v1"
)

// StreamCompression names one wire-compatible Stream batch frame encoding.
type StreamCompression string

const (
	StreamCompressionNone   StreamCompression = "none"
	StreamCompressionGzip   StreamCompression = "gzip"
	StreamCompressionLZ4    StreamCompression = "lz4"
	StreamCompressionSnappy StreamCompression = "snappy"
	StreamCompressionZstd   StreamCompression = "zstd"
)

// StreamBatchRecord correlates one batch record with its caller sequence.
type StreamBatchRecord struct {
	ClientSequence uint32
	Envelope       EventEnvelope
}

// StreamBatchFrame is one bounded, client-framed atomic Stream batch.
// Use EncodeStreamBatch for none/gzip, or NewStreamBatchFrame for an LZ4,
// Snappy, or Zstd frame produced by the caller's preferred codec library.
type StreamBatchFrame struct {
	Compression       StreamCompression
	RecordCount       uint16
	UncompressedBytes uint32
	CompressedBytes   uint32
	PayloadBase64     string
}

// NewStreamBatchFrame validates bounded metadata and wraps exact compressed bytes.
func NewStreamBatchFrame(compression StreamCompression, recordCount uint16, uncompressedBytes uint32, compressed []byte) (StreamBatchFrame, error) {
	if err := validateStreamCompression(compression); err != nil {
		return StreamBatchFrame{}, err
	}
	if recordCount == 0 || recordCount > maxStreamBatchRecords {
		return StreamBatchFrame{}, fmt.Errorf("epoch: Stream batch record count must be between 1 and %d", maxStreamBatchRecords)
	}
	if uncompressedBytes == 0 || uncompressedBytes > maxStreamBatchUncompressed {
		return StreamBatchFrame{}, fmt.Errorf("epoch: Stream batch uncompressed bytes must be between 1 and %d", maxStreamBatchUncompressed)
	}
	if len(compressed) == 0 || len(compressed) > maxStreamBatchCompressed {
		return StreamBatchFrame{}, fmt.Errorf("epoch: Stream batch compressed bytes must be between 1 and %d", maxStreamBatchCompressed)
	}
	if compression == StreamCompressionNone && uint32(len(compressed)) != uncompressedBytes {
		return StreamBatchFrame{}, fmt.Errorf("epoch: uncompressed Stream batch frame size must match uncompressed bytes")
	}
	return StreamBatchFrame{
		Compression:       compression,
		RecordCount:       recordCount,
		UncompressedBytes: uncompressedBytes,
		CompressedBytes:   uint32(len(compressed)),
		PayloadBase64:     base64.StdEncoding.EncodeToString(compressed),
	}, nil
}

// EncodeStreamBatch validates records and produces canonical none or gzip bytes.
// The other advertised codecs remain available through NewStreamBatchFrame.
func EncodeStreamBatch(records []StreamBatchRecord, compression StreamCompression) (StreamBatchFrame, error) {
	if len(records) == 0 || len(records) > maxStreamBatchRecords {
		return StreamBatchFrame{}, fmt.Errorf("epoch: Stream batch must contain between 1 and %d records", maxStreamBatchRecords)
	}
	seen := make(map[uint32]struct{}, len(records))
	canonical := make([]canonicalStreamBatchRecord, 0, len(records))
	for _, record := range records {
		if _, duplicate := seen[record.ClientSequence]; duplicate {
			return StreamBatchFrame{}, fmt.Errorf("epoch: duplicate Stream batch client sequence %d", record.ClientSequence)
		}
		seen[record.ClientSequence] = struct{}{}
		event, err := record.Envelope.normalized()
		if err != nil {
			return StreamBatchFrame{}, err
		}
		canonical = append(canonical, canonicalStreamBatchRecord{
			ClientSequence: record.ClientSequence,
			Envelope:       canonicalStreamEnvelopeFrom(event),
		})
	}
	plain, err := marshalCanonicalStreamBatch(canonical)
	if err != nil {
		return StreamBatchFrame{}, fmt.Errorf("epoch: encode Stream batch: %w", err)
	}
	if len(plain) > maxStreamBatchUncompressed {
		return StreamBatchFrame{}, fmt.Errorf("epoch: Stream batch uncompressed bytes must be between 1 and %d", maxStreamBatchUncompressed)
	}
	var compressed []byte
	switch compression {
	case StreamCompressionNone:
		compressed = plain
	case StreamCompressionGzip:
		var output bytes.Buffer
		writer := gzip.NewWriter(&output)
		writer.Header.ModTime = time.Unix(0, 0)
		writer.Header.OS = 255
		if _, err := writer.Write(plain); err != nil {
			return StreamBatchFrame{}, fmt.Errorf("epoch: gzip Stream batch: %w", err)
		}
		if err := writer.Close(); err != nil {
			return StreamBatchFrame{}, fmt.Errorf("epoch: finish gzip Stream batch: %w", err)
		}
		compressed = output.Bytes()
	case StreamCompressionLZ4, StreamCompressionSnappy, StreamCompressionZstd:
		return StreamBatchFrame{}, fmt.Errorf("epoch: %s Stream batches require a caller-supplied standard frame", compression)
	default:
		return StreamBatchFrame{}, validateStreamCompression(compression)
	}
	return NewStreamBatchFrame(compression, uint16(len(records)), uint32(len(plain)), compressed)
}

type canonicalStreamBatchRecord struct {
	ClientSequence uint32                  `json:"client_sequence"`
	Envelope       canonicalStreamEnvelope `json:"envelope"`
}

type canonicalStreamEnvelope struct {
	ID            string            `json:"id"`
	Source        string            `json:"source"`
	Type          string            `json:"type"`
	Subject       string            `json:"subject,omitempty"`
	TimeMS        uint64            `json:"time_ms"`
	Key           string            `json:"key,omitempty"`
	Headers       map[string]string `json:"headers"`
	ContentType   string            `json:"content_type"`
	SchemaRef     string            `json:"schema_ref,omitempty"`
	Traceparent   string            `json:"traceparent,omitempty"`
	Payload       any               `json:"payload"`
	DeliverAtMS   *uint64           `json:"deliver_at_ms,omitempty"`
	TTLMS         *uint64           `json:"ttl_ms,omitempty"`
	Priority      uint8             `json:"priority"`
	DedupeID      string            `json:"dedupe_id,omitempty"`
	TransactionID string            `json:"transaction_id,omitempty"`
	Extensions    map[string]any    `json:"extensions"`
}

func canonicalStreamEnvelopeFrom(event EventEnvelope) canonicalStreamEnvelope {
	return canonicalStreamEnvelope{
		ID: event.ID, Source: event.Source, Type: event.Type, Subject: event.Subject,
		TimeMS: event.TimeMS, Key: event.Key, Headers: event.Headers, ContentType: event.ContentType,
		SchemaRef: event.SchemaRef, Traceparent: event.Traceparent, Payload: event.Payload,
		DeliverAtMS: event.DeliverAtMS, TTLMS: event.TTLMS, Priority: event.Priority,
		DedupeID: event.DedupeID, TransactionID: event.TransactionID, Extensions: event.Extensions,
	}
}

func marshalCanonicalStreamBatch(records []canonicalStreamBatchRecord) ([]byte, error) {
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(records); err != nil {
		return nil, err
	}
	encoded := bytes.TrimSuffix(output.Bytes(), []byte{'\n'})
	encoded = bytes.ReplaceAll(encoded, []byte(`\u2028`), []byte("\u2028"))
	encoded = bytes.ReplaceAll(encoded, []byte(`\u2029`), []byte("\u2029"))
	return encoded, nil
}

func validateStreamCompression(compression StreamCompression) error {
	switch compression {
	case StreamCompressionNone, StreamCompressionGzip, StreamCompressionLZ4, StreamCompressionSnappy, StreamCompressionZstd:
		return nil
	default:
		return fmt.Errorf("epoch: unsupported Stream batch compression %q", compression)
	}
}

func (frame StreamBatchFrame) validate() error {
	if err := validateStreamCompression(frame.Compression); err != nil {
		return err
	}
	if frame.RecordCount == 0 || frame.RecordCount > maxStreamBatchRecords {
		return fmt.Errorf("epoch: Stream batch record count must be between 1 and %d", maxStreamBatchRecords)
	}
	if frame.UncompressedBytes == 0 || frame.UncompressedBytes > maxStreamBatchUncompressed {
		return fmt.Errorf("epoch: Stream batch uncompressed bytes must be between 1 and %d", maxStreamBatchUncompressed)
	}
	if frame.CompressedBytes == 0 || frame.CompressedBytes > maxStreamBatchCompressed {
		return fmt.Errorf("epoch: Stream batch compressed bytes must be between 1 and %d", maxStreamBatchCompressed)
	}
	compressed, err := base64.StdEncoding.DecodeString(frame.PayloadBase64)
	if err != nil || base64.StdEncoding.EncodeToString(compressed) != frame.PayloadBase64 {
		return fmt.Errorf("epoch: Stream batch payload must be canonical standard base64")
	}
	if uint32(len(compressed)) != frame.CompressedBytes {
		return fmt.Errorf("epoch: Stream batch compressed byte declaration does not match payload")
	}
	if frame.Compression == StreamCompressionNone && frame.CompressedBytes != frame.UncompressedBytes {
		return fmt.Errorf("epoch: uncompressed Stream batch frame sizes must match")
	}
	return nil
}

// RegionalScope identifies one fully-qualified Epoch namespace.
type RegionalScope struct {
	Organization string
	Project      string
	Environment  string
	Namespace    string
}

// RegionalStreamClient routes authenticated Stream calls across regional nodes.
// It discovers the current leader before each operation and retries only with
// the caller's unchanged idempotency key.
type RegionalStreamClient struct {
	regional *regionalClient
}

type regionalClient struct {
	transports []Transport
	token      string
	scopePath  string
}

type regionalRoute struct {
	ResourceGeneration string                      `json:"resource_generation"`
	TabletEpoch        string                      `json:"tablet_epoch"`
	Term               string                      `json:"term"`
	AcceptsWrites      bool                        `json:"accepts_writes"`
	StreamPartitioning *regionalStreamPartitioning `json:"stream_partitioning,omitempty"`
}

type regionalStreamPartitioning struct {
	Algorithm          string `json:"algorithm"`
	KeyEncoding        string `json:"key_encoding"`
	MissingKeyFallback string `json:"missing_key_fallback"`
	ShardCount         uint32 `json:"shard_count"`
}

// StreamRetentionPolicy bounds retained records independently by count, canonical bytes,
// and age. A zero field disables that bound.
type StreamRetentionPolicy struct {
	MaxRecordsPerPartition uint64
	MaxBytesPerPartition   uint64
	MaxAgeMS               uint64
}

// StreamReadIsolation controls whether pending transactional records are visible.
type StreamReadIsolation string

const (
	StreamReadCommitted   StreamReadIsolation = "read_committed"
	StreamReadUncommitted StreamReadIsolation = "read_uncommitted"
)

// StreamConsumerMode selects a shared push or independently identified long-poll lane.
type StreamConsumerMode string

const (
	StreamConsumerPush      StreamConsumerMode = "push"
	StreamConsumerDedicated StreamConsumerMode = "dedicated"
)

// StreamCaptureFormat selects one portable capture encoding.
type StreamCaptureFormat string

const (
	StreamCaptureJSONLines StreamCaptureFormat = "json_lines"
	StreamCaptureJSONArray StreamCaptureFormat = "json_array"
)

// StreamOffsetCommit is atomically committed with a Stream transaction.
type StreamOffsetCommit struct {
	Group      string
	Partition  uint32
	NextOffset uint64
}

// StreamReplicationRecord retains the source position and loop-prevention path.
type StreamReplicationRecord struct {
	SourceOffset      uint64
	Envelope          EventEnvelope
	TraversedClusters []string
}

// StreamReplicationBatch is one contiguous, checkpointed cross-cluster batch.
type StreamReplicationBatch struct {
	SourceCluster     string
	SourceStream      string
	SourcePartition   uint32
	FirstSourceOffset uint64
	Records           []StreamReplicationRecord
}

// StreamSuperstreamMember names one physical Stream shard and starting offset.
type StreamSuperstreamMember struct {
	Name   string
	Stream string
	Shard  uint32
	Offset uint64
}

func (policy StreamRetentionPolicy) validate() error {
	if policy.MaxRecordsPerPartition > maxStreamRetentionRecords {
		return fmt.Errorf("epoch: Stream retention max records must be between 1 and %d when set", maxStreamRetentionRecords)
	}
	if policy.MaxBytesPerPartition > maxStreamRetentionBytes {
		return fmt.Errorf("epoch: Stream retention max bytes must be between 1 and %d when set", maxStreamRetentionBytes)
	}
	if policy.MaxAgeMS > maxStreamRetentionAgeMS {
		return fmt.Errorf("epoch: Stream retention max age must be between 1 and %d milliseconds when set", maxStreamRetentionAgeMS)
	}
	return nil
}

// NewRegionalStreamClient builds a regional client over one or more HTTP endpoints.
func NewRegionalStreamClient(endpoints []string, token string, scope RegionalScope, timeout time.Duration) (*RegionalStreamClient, error) {
	regional, err := newRegionalClient(endpoints, token, scope, timeout)
	if err != nil {
		return nil, err
	}
	return &RegionalStreamClient{regional: regional}, nil
}

// NewRegionalStreamClientWithTransports injects endpoint transports for tests or custom networking.
func NewRegionalStreamClientWithTransports(transports []Transport, token string, scope RegionalScope) (*RegionalStreamClient, error) {
	regional, err := newRegionalClientWithTransports(transports, token, scope)
	if err != nil {
		return nil, err
	}
	return &RegionalStreamClient{regional: regional}, nil
}

func newRegionalClient(endpoints []string, token string, scope RegionalScope, timeout time.Duration) (*regionalClient, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("epoch: at least one regional endpoint is required")
	}
	transports := make([]Transport, 0, len(endpoints))
	for _, endpoint := range endpoints {
		transport, err := NewHTTPTransport(endpoint, timeout)
		if err != nil {
			return nil, err
		}
		transports = append(transports, transport)
	}
	return newRegionalClientWithTransports(transports, token, scope)
}

func newRegionalClientWithTransports(transports []Transport, token string, scope RegionalScope) (*regionalClient, error) {
	if len(transports) == 0 {
		return nil, fmt.Errorf("epoch: at least one regional transport is required")
	}
	for _, transport := range transports {
		if transport == nil {
			return nil, fmt.Errorf("epoch: regional transports cannot contain nil")
		}
	}
	token = strings.TrimSpace(token)
	if token == "" || strings.ContainsAny(token, "\r\n") {
		return nil, fmt.Errorf("epoch: bearer token is required and must fit one HTTP header")
	}
	scopePath, err := regionalScopePath(scope)
	if err != nil {
		return nil, err
	}
	return &regionalClient{transports: append([]Transport(nil), transports...), token: token, scopePath: scopePath}, nil
}

// Append appends one record after discovering the current leader and route fences.
func (client *RegionalStreamClient) Append(ctx context.Context, stream string, shard uint32, idempotencyKey string, event EventEnvelope) (Document, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, fmt.Errorf("epoch: idempotency key is required")
	}
	event, err := event.normalized()
	if err != nil {
		return nil, err
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(route regionalRoute) Request {
		return Request{
			Method: "POST",
			Path:   "/records",
			Body: struct {
				IdempotencyKey string        `json:"idempotency_key"`
				ExpectedTerm   string        `json:"expected_term"`
				Partition      uint32        `json:"partition"`
				Envelope       EventEnvelope `json:"envelope"`
			}{idempotencyKey, route.Term, 0, event},
		}
	})
}

// AppendBatch atomically appends one caller-framed batch to a single Stream shard.
// The exact frame and idempotency key are retained across one bounded rediscovery.
func (client *RegionalStreamClient) AppendBatch(ctx context.Context, stream string, shard uint32, idempotencyKey string, frame StreamBatchFrame) (Document, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, fmt.Errorf("epoch: idempotency key is required")
	}
	if err := frame.validate(); err != nil {
		return nil, err
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(route regionalRoute) Request {
		return Request{
			Method: "POST",
			Path:   "/records/batches",
			Body: struct {
				IdempotencyKey string            `json:"idempotency_key"`
				ExpectedTerm   string            `json:"expected_term"`
				Partition      uint32            `json:"partition"`
				Compression    StreamCompression `json:"compression"`
				RecordCount    uint16            `json:"record_count"`
				Uncompressed   uint32            `json:"uncompressed_bytes"`
				Compressed     uint32            `json:"compressed_bytes"`
				PayloadBase64  string            `json:"payload_base64"`
			}{
				idempotencyKey, route.Term, 0, frame.Compression, frame.RecordCount,
				frame.UncompressedBytes, frame.CompressedBytes, frame.PayloadBase64,
			},
		}
	})
}

// AppendKeyed discovers the current Stream partitioning contract, selects a shard from
// the event key (or event ID when the key is empty), and pins the write to that routing
// generation so a concurrent expansion cannot silently remap an uncertain mutation.
func (client *RegionalStreamClient) AppendKeyed(ctx context.Context, stream, idempotencyKey string, event EventEnvelope) (Document, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, fmt.Errorf("epoch: idempotency key is required")
	}
	event, err := event.normalized()
	if err != nil {
		return nil, err
	}
	regional := client.regionalClient()
	if regional == nil {
		return nil, fmt.Errorf("epoch: regional stream client is not configured")
	}
	bootstrapPath, err := regional.resourceShardPath("streams", "stream", stream, 0)
	if err != nil {
		return nil, err
	}
	route, err := regional.discoverRoute(ctx, bootstrapPath)
	if err != nil {
		return nil, err
	}
	partitioning, err := validatedStreamPartitioning(route)
	if err != nil {
		return nil, err
	}
	partitionValue := event.Key
	if partitionValue == "" {
		partitionValue = event.ID
	}
	shard, err := StreamShardFor(partitionValue, partitioning.ShardCount)
	if err != nil {
		return nil, err
	}
	return regionalCallAtGeneration[Document](ctx, regional, "streams", "stream", stream, shard, route.ResourceGeneration, func(target regionalRoute) Request {
		return Request{
			Method: "POST",
			Path:   "/records",
			Body: struct {
				IdempotencyKey string        `json:"idempotency_key"`
				ExpectedTerm   string        `json:"expected_term"`
				Partition      uint32        `json:"partition"`
				Envelope       EventEnvelope `json:"envelope"`
			}{idempotencyKey, target.Term, 0, event},
		}
	})
}

// StreamShardFor implements the advertised unsigned FNV-1a UTF-8 partitioner.
func StreamShardFor(partitionValue string, shardCount uint32) (uint32, error) {
	if shardCount == 0 {
		return 0, fmt.Errorf("epoch: Stream shard count must be greater than zero")
	}
	hash := uint64(0xcbf29ce484222325)
	for _, value := range []byte(partitionValue) {
		hash = (hash ^ uint64(value)) * 0x100000001b3
	}
	return uint32(hash % uint64(shardCount)), nil
}

// Fetch performs a linearizable bounded read from one Stream shard.
func (client *RegionalStreamClient) Fetch(ctx context.Context, stream string, shard uint32, offset uint64, limit uint32) (Document, error) {
	return client.FetchWithIsolation(ctx, stream, shard, offset, limit, StreamReadCommitted)
}

// FetchWithIsolation performs a linearizable bounded read with explicit transaction visibility.
func (client *RegionalStreamClient) FetchWithIsolation(ctx context.Context, stream string, shard uint32, offset uint64, limit uint32, isolation StreamReadIsolation) (Document, error) {
	if limit == 0 || limit > maxRegionalFetchRecords {
		return nil, fmt.Errorf("epoch: fetch limit must be between 1 and %d", maxRegionalFetchRecords)
	}
	if isolation != StreamReadCommitted && isolation != StreamReadUncommitted {
		return nil, fmt.Errorf("epoch: unsupported Stream read isolation %q", isolation)
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(_ regionalRoute) Request {
		return Request{Method: "GET", Path: "/records", Query: url.Values{
			"offset":    {strconv.FormatUint(offset, 10)},
			"limit":     {strconv.FormatUint(uint64(limit), 10)},
			"isolation": {string(isolation)},
		}, Headers: map[string]string{regionalReadHeader: "linearizable"}}
	})
}

// ConsumeLongPoll waits for visible records in a push or dedicated consumer lane.
func (client *RegionalStreamClient) ConsumeLongPoll(ctx context.Context, stream string, shard uint32, offset uint64, limit uint32, isolation StreamReadIsolation, mode StreamConsumerMode, consumerID string, wait time.Duration) (Document, error) {
	if limit == 0 || limit > maxRegionalFetchRecords {
		return nil, fmt.Errorf("epoch: fetch limit must be between 1 and %d", maxRegionalFetchRecords)
	}
	if isolation != StreamReadCommitted && isolation != StreamReadUncommitted {
		return nil, fmt.Errorf("epoch: unsupported Stream read isolation %q", isolation)
	}
	waitMS := wait.Milliseconds()
	if waitMS < 1 || waitMS > 30_000 {
		return nil, fmt.Errorf("epoch: consumer wait must be between 1ms and 30s")
	}
	if mode == StreamConsumerDedicated {
		if strings.TrimSpace(consumerID) == "" {
			return nil, fmt.Errorf("epoch: dedicated consumer ID is required")
		}
	} else if mode != StreamConsumerPush || consumerID != "" {
		return nil, fmt.Errorf("epoch: push mode requires an empty consumer ID")
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(_ regionalRoute) Request {
		query := url.Values{
			"offset":    {strconv.FormatUint(offset, 10)},
			"limit":     {strconv.FormatUint(uint64(limit), 10)},
			"isolation": {string(isolation)},
			"mode":      {string(mode)},
			"wait_ms":   {strconv.FormatInt(waitMS, 10)},
		}
		if consumerID != "" {
			query.Set("consumer_id", consumerID)
		}
		return Request{Method: "GET", Path: "/records/consume", Query: query, Headers: map[string]string{regionalReadHeader: "linearizable"}}
	})
}

// AppendIdempotent appends one producer-epoch/sequence-fenced record.
func (client *RegionalStreamClient) AppendIdempotent(ctx context.Context, stream string, shard uint32, idempotencyKey, producerID string, producerEpoch, sequence uint64, event EventEnvelope) (Document, error) {
	event, err := event.normalized()
	if err != nil {
		return nil, err
	}
	if err = validateStreamProducer(producerID, producerEpoch); err != nil {
		return nil, err
	}
	operation := struct {
		Action        string        `json:"action"`
		ProducerID    string        `json:"producer_id"`
		ProducerEpoch string        `json:"producer_epoch"`
		Sequence      string        `json:"sequence"`
		Partition     uint32        `json:"partition"`
		Envelope      EventEnvelope `json:"envelope"`
	}{"append_idempotent", producerID, strconv.FormatUint(producerEpoch, 10), strconv.FormatUint(sequence, 10), 0, event}
	return client.mutateState(ctx, stream, shard, idempotencyKey, operation)
}

// BeginTransaction opens or exactly replays a producer-fenced transaction.
func (client *RegionalStreamClient) BeginTransaction(ctx context.Context, stream string, shard uint32, idempotencyKey, transactionID, producerID string, producerEpoch uint64) (Document, error) {
	if err := validateStreamTransaction(transactionID); err != nil {
		return nil, err
	}
	if err := validateStreamProducer(producerID, producerEpoch); err != nil {
		return nil, err
	}
	operation := struct {
		Action        string `json:"action"`
		TransactionID string `json:"transaction_id"`
		ProducerID    string `json:"producer_id"`
		ProducerEpoch string `json:"producer_epoch"`
	}{"begin_transaction", transactionID, producerID, strconv.FormatUint(producerEpoch, 10)}
	return client.mutateState(ctx, stream, shard, idempotencyKey, operation)
}

// AppendTransaction atomically appends a bounded sequence inside an open transaction.
func (client *RegionalStreamClient) AppendTransaction(ctx context.Context, stream string, shard uint32, idempotencyKey, transactionID, producerID string, producerEpoch, sequence uint64, events []EventEnvelope) (Document, error) {
	if err := validateStreamTransaction(transactionID); err != nil {
		return nil, err
	}
	if err := validateStreamProducer(producerID, producerEpoch); err != nil {
		return nil, err
	}
	if len(events) == 0 || len(events) > 128 {
		return nil, fmt.Errorf("epoch: Stream transaction append must contain between 1 and 128 records")
	}
	normalized := make([]EventEnvelope, 0, len(events))
	for _, event := range events {
		event, err := event.normalized()
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, event)
	}
	operation := struct {
		Action        string          `json:"action"`
		TransactionID string          `json:"transaction_id"`
		ProducerID    string          `json:"producer_id"`
		ProducerEpoch string          `json:"producer_epoch"`
		Sequence      string          `json:"sequence"`
		Partition     uint32          `json:"partition"`
		Envelopes     []EventEnvelope `json:"envelopes"`
	}{"append_transaction", transactionID, producerID, strconv.FormatUint(producerEpoch, 10), strconv.FormatUint(sequence, 10), 0, normalized}
	return client.mutateState(ctx, stream, shard, idempotencyKey, operation)
}

// CommitTransaction makes its records visible and optionally advances one group offset atomically.
func (client *RegionalStreamClient) CommitTransaction(ctx context.Context, stream string, shard uint32, idempotencyKey, transactionID string, offsetCommit *StreamOffsetCommit) (Document, error) {
	if err := validateStreamTransaction(transactionID); err != nil {
		return nil, err
	}
	var commit any
	if offsetCommit != nil {
		if strings.TrimSpace(offsetCommit.Group) == "" || offsetCommit.Partition != shard {
			return nil, fmt.Errorf("epoch: Stream transaction offset commit must name a group on shard %d", shard)
		}
		commit = struct {
			Group      string `json:"group"`
			Partition  uint32 `json:"partition"`
			NextOffset string `json:"next_offset"`
		}{offsetCommit.Group, 0, strconv.FormatUint(offsetCommit.NextOffset, 10)}
	}
	operation := struct {
		Action        string `json:"action"`
		TransactionID string `json:"transaction_id"`
		OffsetCommit  any    `json:"offset_commit,omitempty"`
	}{"commit_transaction", transactionID, commit}
	return client.mutateState(ctx, stream, shard, idempotencyKey, operation)
}

// AbortTransaction permanently hides the transaction's records from read-committed consumers.
func (client *RegionalStreamClient) AbortTransaction(ctx context.Context, stream string, shard uint32, idempotencyKey, transactionID string) (Document, error) {
	if err := validateStreamTransaction(transactionID); err != nil {
		return nil, err
	}
	operation := struct {
		Action        string `json:"action"`
		TransactionID string `json:"transaction_id"`
	}{"abort_transaction", transactionID}
	return client.mutateState(ctx, stream, shard, idempotencyKey, operation)
}

// Compact retains the latest committed record per key and expires old tombstones.
func (client *RegionalStreamClient) Compact(ctx context.Context, stream string, shard uint32, idempotencyKey string, tombstoneRetention time.Duration) (Document, error) {
	if tombstoneRetention <= 0 {
		return nil, fmt.Errorf("epoch: tombstone retention must be positive")
	}
	operation := struct {
		Action               string `json:"action"`
		Partition            uint32 `json:"partition"`
		TombstoneRetentionMS string `json:"tombstone_retention_ms"`
	}{"compact", 0, strconv.FormatInt(tombstoneRetention.Milliseconds(), 10)}
	return client.mutateState(ctx, stream, shard, idempotencyKey, operation)
}

// TierPrefix moves a committed hot prefix into one immutable, checksum-verified object.
func (client *RegionalStreamClient) TierPrefix(ctx context.Context, stream string, shard uint32, idempotencyKey string, beforeOffset uint64, maxRecords uint32) (Document, error) {
	if maxRecords == 0 || maxRecords > 1_024 {
		return nil, fmt.Errorf("epoch: tier max records must be between 1 and 1024")
	}
	operation := struct {
		Action       string `json:"action"`
		Partition    uint32 `json:"partition"`
		BeforeOffset string `json:"before_offset"`
		MaxRecords   uint32 `json:"max_records"`
	}{"tier_prefix", 0, strconv.FormatUint(beforeOffset, 10), maxRecords}
	return client.mutateState(ctx, stream, shard, idempotencyKey, operation)
}

// Capture writes one bounded committed offset range in a portable open format.
func (client *RegionalStreamClient) Capture(ctx context.Context, stream string, shard uint32, idempotencyKey, captureID string, firstOffset, endOffset uint64, format StreamCaptureFormat) (Document, error) {
	if strings.TrimSpace(captureID) == "" || firstOffset > endOffset {
		return nil, fmt.Errorf("epoch: capture ID is required and its offset range must be ordered")
	}
	if format != StreamCaptureJSONLines && format != StreamCaptureJSONArray {
		return nil, fmt.Errorf("epoch: unsupported Stream capture format %q", format)
	}
	operation := struct {
		Action      string              `json:"action"`
		CaptureID   string              `json:"capture_id"`
		Partition   uint32              `json:"partition"`
		FirstOffset string              `json:"first_offset"`
		EndOffset   string              `json:"end_offset"`
		Format      StreamCaptureFormat `json:"format"`
	}{"capture", captureID, 0, strconv.FormatUint(firstOffset, 10), strconv.FormatUint(endOffset, 10), format}
	return client.mutateState(ctx, stream, shard, idempotencyKey, operation)
}

// Replicate applies one contiguous source batch with checkpoint and loop fencing.
func (client *RegionalStreamClient) Replicate(ctx context.Context, stream string, shard uint32, idempotencyKey string, batch StreamReplicationBatch) (Document, error) {
	if strings.TrimSpace(batch.SourceCluster) == "" || strings.TrimSpace(batch.SourceStream) == "" || len(batch.Records) == 0 || len(batch.Records) > 128 {
		return nil, fmt.Errorf("epoch: replication requires source identity and between 1 and 128 records")
	}
	type wireRecord struct {
		SourceOffset      string        `json:"source_offset"`
		Envelope          EventEnvelope `json:"envelope"`
		TraversedClusters []string      `json:"traversed_clusters"`
	}
	records := make([]wireRecord, 0, len(batch.Records))
	for _, record := range batch.Records {
		event, err := record.Envelope.normalized()
		if err != nil {
			return nil, err
		}
		records = append(records, wireRecord{strconv.FormatUint(record.SourceOffset, 10), event, append([]string(nil), record.TraversedClusters...)})
	}
	operation := struct {
		Action         string `json:"action"`
		LocalPartition uint32 `json:"local_partition"`
		Batch          any    `json:"batch"`
	}{"replicate", 0, struct {
		SourceCluster     string       `json:"source_cluster"`
		SourceStream      string       `json:"source_stream"`
		SourcePartition   uint32       `json:"source_partition"`
		FirstSourceOffset string       `json:"first_source_offset"`
		Records           []wireRecord `json:"records"`
	}{batch.SourceCluster, batch.SourceStream, batch.SourcePartition, strconv.FormatUint(batch.FirstSourceOffset, 10), records}}
	return client.mutateState(ctx, stream, shard, idempotencyKey, operation)
}

// Transaction returns one linearizable transaction observation.
func (client *RegionalStreamClient) Transaction(ctx context.Context, stream string, shard uint32, transactionID string) (Document, error) {
	segment, err := segment(transactionID, "transaction")
	if err != nil {
		return nil, err
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(_ regionalRoute) Request {
		return Request{Method: "GET", Path: "/transactions/" + segment, Headers: map[string]string{regionalReadHeader: "linearizable"}}
	})
}

// TierObjects lists immutable tier manifests for one shard.
func (client *RegionalStreamClient) TierObjects(ctx context.Context, stream string, shard uint32) (Document, error) {
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(_ regionalRoute) Request {
		return Request{Method: "GET", Path: "/tier/objects", Headers: map[string]string{regionalReadHeader: "linearizable"}}
	})
}

// CaptureArtifact returns one retained capture artifact and checksum.
func (client *RegionalStreamClient) CaptureArtifact(ctx context.Context, stream string, shard uint32, captureID string) (Document, error) {
	segment, err := segment(captureID, "capture")
	if err != nil {
		return nil, err
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(_ regionalRoute) Request {
		return Request{Method: "GET", Path: "/captures/" + segment, Headers: map[string]string{regionalReadHeader: "linearizable"}}
	})
}

// ConfigureCaptureSchedule enables leader-driven periodic open-format capture.
func (client *RegionalStreamClient) ConfigureCaptureSchedule(ctx context.Context, stream string, shard uint32, idempotencyKey, scheduleID string, interval time.Duration, format StreamCaptureFormat) (Document, error) {
	if _, err := segment(scheduleID, "capture schedule"); err != nil {
		return nil, err
	}
	if interval < minStreamCaptureInterval || interval > maxStreamCaptureInterval || interval%time.Millisecond != 0 {
		return nil, fmt.Errorf("epoch: capture interval must be between %s and %s in whole milliseconds", minStreamCaptureInterval, maxStreamCaptureInterval)
	}
	if format != StreamCaptureJSONLines && format != StreamCaptureJSONArray {
		return nil, fmt.Errorf("epoch: unsupported Stream capture format %q", format)
	}
	return client.mutateState(ctx, stream, shard, idempotencyKey, struct {
		Action     string              `json:"action"`
		ScheduleID string              `json:"schedule_id"`
		Partition  uint32              `json:"partition"`
		IntervalMS string              `json:"interval_ms"`
		Format     StreamCaptureFormat `json:"format"`
	}{"configure_capture_schedule", scheduleID, 0, strconv.FormatInt(interval.Milliseconds(), 10), format})
}

// CaptureSchedule returns the replicated schedule checkpoint and next deadline.
func (client *RegionalStreamClient) CaptureSchedule(ctx context.Context, stream string, shard uint32, scheduleID string) (Document, error) {
	segment, err := segment(scheduleID, "capture schedule")
	if err != nil {
		return nil, err
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(_ regionalRoute) Request {
		return Request{Method: "GET", Path: "/capture-schedules/" + segment, Headers: map[string]string{regionalReadHeader: "linearizable"}}
	})
}

// PartitionAdvice estimates an online expand-only partition target.
func (client *RegionalStreamClient) PartitionAdvice(ctx context.Context, stream string, targetRecordsPerPartition, targetBytesPerPartition uint64) (Document, error) {
	if targetRecordsPerPartition == 0 || targetBytesPerPartition == 0 {
		return nil, fmt.Errorf("epoch: partition advice targets must be positive")
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, 0, func(_ regionalRoute) Request {
		return Request{Method: "GET", Path: "/partitions/advice", Query: url.Values{
			"target_records_per_partition": {strconv.FormatUint(targetRecordsPerPartition, 10)},
			"target_bytes_per_partition":   {strconv.FormatUint(targetBytesPerPartition, 10)},
		}, Headers: map[string]string{regionalReadHeader: "linearizable"}}
	})
}

// FetchSuperstream creates a deterministic logical merge over independently linearizable shards.
// The result is not an atomic cross-shard snapshot; each record retains its member identity.
func (client *RegionalStreamClient) FetchSuperstream(ctx context.Context, members []StreamSuperstreamMember, limit uint32, isolation StreamReadIsolation) (Document, error) {
	if len(members) == 0 || len(members) > 128 {
		return nil, fmt.Errorf("epoch: superstream must contain between 1 and 128 members")
	}
	if limit == 0 || limit > maxRegionalFetchRecords {
		return nil, fmt.Errorf("epoch: fetch limit must be between 1 and %d", maxRegionalFetchRecords)
	}
	type mergedRecord struct {
		appendedAt uint64
		member     string
		partition  uint64
		offset     uint64
		document   Document
	}
	merged := make([]mergedRecord, 0)
	seen := make(map[string]struct{}, len(members))
	for _, member := range members {
		if strings.TrimSpace(member.Name) == "" || strings.TrimSpace(member.Stream) == "" {
			return nil, fmt.Errorf("epoch: superstream member name and Stream are required")
		}
		if _, duplicate := seen[member.Name]; duplicate {
			return nil, fmt.Errorf("epoch: duplicate superstream member %q", member.Name)
		}
		seen[member.Name] = struct{}{}
	}
	for _, member := range members {
		response, err := client.FetchWithIsolation(ctx, member.Stream, member.Shard, member.Offset, limit, isolation)
		if err != nil {
			return nil, err
		}
		records, ok := response["records"].([]any)
		if !ok {
			return nil, fmt.Errorf("epoch: superstream member %q response omitted records", member.Name)
		}
		for _, value := range records {
			record, ok := value.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("epoch: superstream member %q returned an invalid record", member.Name)
			}
			appendedAt, err := streamResponseU64(record["appended_at_ms"], "appended_at_ms")
			if err != nil {
				return nil, err
			}
			partition, err := streamResponseU64(record["partition"], "partition")
			if err != nil {
				return nil, err
			}
			offset, err := streamResponseU64(record["offset"], "offset")
			if err != nil {
				return nil, err
			}
			copy := Document{"member": member.Name}
			for key, item := range record {
				copy[key] = item
			}
			merged = append(merged, mergedRecord{appendedAt, member.Name, partition, offset, copy})
		}
	}
	sort.Slice(merged, func(left, right int) bool {
		a, b := merged[left], merged[right]
		if a.appendedAt != b.appendedAt {
			return a.appendedAt < b.appendedAt
		}
		if a.member != b.member {
			return a.member < b.member
		}
		if a.partition != b.partition {
			return a.partition < b.partition
		}
		return a.offset < b.offset
	})
	if len(merged) > int(limit) {
		merged = merged[:limit]
	}
	records := make([]any, len(merged))
	for index, record := range merged {
		records[index] = record.document
	}
	return Document{
		"records":        records,
		"member_count":   len(members),
		"ordering":       "appended_at_member_partition_offset",
		"snapshot_scope": "independently_linearizable_members",
	}, nil
}

func streamResponseU64(value any, field string) (uint64, error) {
	switch value := value.(type) {
	case string:
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("epoch: Stream response %s is invalid: %w", field, err)
		}
		return parsed, nil
	case float64:
		const maxSafeJSONInteger = float64(9_007_199_254_740_991)
		if value < 0 || value > maxSafeJSONInteger || value != float64(uint64(value)) {
			return 0, fmt.Errorf("epoch: Stream response %s is invalid", field)
		}
		return uint64(value), nil
	default:
		return 0, fmt.Errorf("epoch: Stream response omitted %s", field)
	}
}

func (client *RegionalStreamClient) mutateState(ctx context.Context, stream string, shard uint32, idempotencyKey string, operation any) (Document, error) {
	if err := validateRegionalIdempotencyKey(idempotencyKey); err != nil {
		return nil, err
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(route regionalRoute) Request {
		return Request{Method: "POST", Path: "/state", Body: struct {
			IdempotencyKey string `json:"idempotency_key"`
			ExpectedTerm   string `json:"expected_term"`
			Operation      any    `json:"operation"`
		}{idempotencyKey, route.Term, operation}}
	})
}

func validateStreamProducer(producerID string, producerEpoch uint64) error {
	if strings.TrimSpace(producerID) == "" || producerEpoch == 0 {
		return fmt.Errorf("epoch: producer ID and non-zero epoch are required")
	}
	return nil
}

func validateStreamTransaction(transactionID string) error {
	if strings.TrimSpace(transactionID) == "" {
		return fmt.Errorf("epoch: transaction ID is required")
	}
	return nil
}

// CommitOffset commits or explicitly resets a generation-fenced next offset.
func (client *RegionalStreamClient) CommitOffset(ctx context.Context, stream string, shard uint32, group, member string, generation, nextOffset uint64, reset bool, idempotencyKey string) (Document, error) {
	groupSegment, err := segment(group, "consumer group")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(member) == "" {
		return nil, fmt.Errorf("epoch: consumer member is required")
	}
	if generation == 0 {
		return nil, fmt.Errorf("epoch: consumer group generation must be non-zero")
	}
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, fmt.Errorf("epoch: idempotency key is required")
	}
	mode := "commit"
	if reset {
		mode = "reset"
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(route regionalRoute) Request {
		return Request{Method: "PUT", Path: "/groups/" + groupSegment + "/offsets", Body: struct {
			IdempotencyKey string `json:"idempotency_key"`
			ExpectedTerm   string `json:"expected_term"`
			MemberID       string `json:"member_id"`
			Generation     string `json:"group_generation"`
			Partition      uint32 `json:"partition"`
			NextOffset     string `json:"next_offset"`
			Mode           string `json:"mode"`
		}{idempotencyKey, route.Term, member, strconv.FormatUint(generation, 10), 0, strconv.FormatUint(nextOffset, 10), mode}}
	})
}

// Lag returns the linearizable checkpoint and lag observation for a group.
func (client *RegionalStreamClient) Lag(ctx context.Context, stream string, shard uint32, group string) (Document, error) {
	return client.lagAtGeneration(ctx, stream, shard, group, "")
}

func (client *RegionalStreamClient) lagAtGeneration(ctx context.Context, stream string, shard uint32, group, resourceGeneration string) (Document, error) {
	groupSegment, err := segment(group, "consumer group")
	if err != nil {
		return nil, err
	}
	return regionalCallAtGeneration[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, resourceGeneration, func(_ regionalRoute) Request {
		return Request{Method: "GET", Path: "/groups/" + groupSegment + "/lag", Headers: map[string]string{regionalReadHeader: "linearizable"}}
	})
}

// FetchGroup performs a linearizable fetch beginning at the durable group checkpoint.
func (client *RegionalStreamClient) FetchGroup(ctx context.Context, stream string, shard uint32, group string, limit uint32) (Document, error) {
	if limit == 0 || limit > maxRegionalFetchRecords {
		return nil, fmt.Errorf("epoch: fetch limit must be between 1 and %d", maxRegionalFetchRecords)
	}
	groupSegment, err := segment(group, "consumer group")
	if err != nil {
		return nil, err
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(_ regionalRoute) Request {
		return Request{Method: "GET", Path: "/groups/" + groupSegment + "/records", Query: url.Values{"limit": {strconv.FormatUint(uint64(limit), 10)}}, Headers: map[string]string{regionalReadHeader: "linearizable"}}
	})
}

// ClaimGroup installs a coordinated-session generation as the durable owner fence on one shard.
// The transition preserves the group's committed next offset.
func (client *RegionalStreamClient) ClaimGroup(ctx context.Context, stream string, shard uint32, group, member string, generation uint64, idempotencyKey string) (Document, error) {
	return client.claimGroupAtGeneration(ctx, stream, shard, group, member, generation, idempotencyKey, "")
}

func (client *RegionalStreamClient) claimGroupAtGeneration(ctx context.Context, stream string, shard uint32, group, member string, generation uint64, idempotencyKey, resourceGeneration string) (Document, error) {
	groupSegment, _, err := consumerSessionSegments(group, member)
	if err != nil {
		return nil, err
	}
	if generation == 0 {
		return nil, fmt.Errorf("epoch: consumer group generation must be non-zero")
	}
	if err := validateRegionalIdempotencyKey(idempotencyKey); err != nil {
		return nil, err
	}
	return regionalCallAtGeneration[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, resourceGeneration, func(route regionalRoute) Request {
		return Request{Method: "PUT", Path: "/groups/" + groupSegment + "/claim", Body: struct {
			IdempotencyKey string `json:"idempotency_key"`
			ExpectedTerm   string `json:"expected_term"`
			MemberID       string `json:"member_id"`
			Generation     string `json:"group_generation"`
			Partition      uint32 `json:"partition"`
		}{idempotencyKey, route.Term, member, strconv.FormatUint(generation, 10), 0}}
	})
}

// FetchClaimedGroup performs a bounded linearizable fetch only when the exact
// member and coordinated-session generation own this shard's checkpoint fence.
func (client *RegionalStreamClient) FetchClaimedGroup(ctx context.Context, stream string, shard uint32, group, member string, generation uint64, limit uint32) (Document, error) {
	if limit == 0 || limit > maxRegionalFetchRecords {
		return nil, fmt.Errorf("epoch: fetch limit must be between 1 and %d", maxRegionalFetchRecords)
	}
	groupSegment, _, err := consumerSessionSegments(group, member)
	if err != nil {
		return nil, err
	}
	if generation == 0 {
		return nil, fmt.Errorf("epoch: consumer group generation must be non-zero")
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(_ regionalRoute) Request {
		return Request{Method: "GET", Path: "/groups/" + groupSegment + "/claimed-records", Query: url.Values{
			"member_id":        {member},
			"group_generation": {strconv.FormatUint(generation, 10)},
			"limit":            {strconv.FormatUint(uint64(limit), 10)},
		}, Headers: map[string]string{regionalReadHeader: "linearizable"}}
	})
}

// ClaimConsumerSession copies one stable shard-zero assignment into every
// assigned shard's checkpoint fence. It revalidates the coordinator after all
// claims and returns no assignment if a concurrent rebalance occurred.
func (client *RegionalStreamClient) ClaimConsumerSession(ctx context.Context, stream, group, member string, generation uint64, idempotencyKeyPrefix string) ([]uint32, error) {
	if _, _, err := consumerSessionSegments(group, member); err != nil {
		return nil, err
	}
	if generation == 0 {
		return nil, fmt.Errorf("epoch: consumer group generation must be non-zero")
	}
	if err := validateRegionalIdempotencyKey(idempotencyKeyPrefix); err != nil {
		return nil, err
	}
	regional := client.regionalClient()
	if regional == nil {
		return nil, fmt.Errorf("epoch: regional stream client is not configured")
	}
	coordinatorPath, err := regional.resourceShardPath("streams", "stream", stream, 0)
	if err != nil {
		return nil, err
	}
	coordinatorRoute, err := regional.discoverRoute(ctx, coordinatorPath)
	if err != nil {
		return nil, err
	}
	resourceGeneration := coordinatorRoute.ResourceGeneration
	before, err := client.consumerSessionAtGeneration(ctx, stream, group, resourceGeneration)
	if err != nil {
		return nil, err
	}
	assigned, err := coordinatedAssignment(before, group, member, generation)
	if err != nil {
		return nil, err
	}
	type plannedClaim struct {
		shard      uint32
		generation uint64
		key        string
	}
	claims := make([]plannedClaim, 0, len(assigned))
	for _, shard := range assigned {
		lag, lagErr := client.lagAtGeneration(ctx, stream, shard, group, resourceGeneration)
		if lagErr != nil {
			return nil, lagErr
		}
		generations, planErr := claimGenerations(lag, generation)
		if planErr != nil {
			return nil, fmt.Errorf("epoch: shard %d claim plan: %w", shard, planErr)
		}
		for _, claimGeneration := range generations {
			key := idempotencyKeyPrefix + "-shard-" + strconv.FormatUint(uint64(shard), 10) + "-generation-" + strconv.FormatUint(claimGeneration, 10)
			if len([]byte(key)) > 128 {
				return nil, fmt.Errorf("epoch: derived consumer claim idempotency key exceeds 128 bytes")
			}
			claims = append(claims, plannedClaim{shard, claimGeneration, key})
		}
	}
	for _, claim := range claims {
		receipt, claimErr := client.claimGroupAtGeneration(ctx, stream, claim.shard, group, member, claim.generation, claim.key, resourceGeneration)
		if claimErr != nil {
			return nil, claimErr
		}
		if err := requireAppliedGroupClaim(receipt, claim.shard); err != nil {
			return nil, err
		}
	}
	after, err := client.consumerSessionAtGeneration(ctx, stream, group, resourceGeneration)
	if err != nil {
		return nil, err
	}
	revalidated, err := coordinatedAssignment(after, group, member, generation)
	if err != nil || !sameShards(assigned, revalidated) {
		return nil, fmt.Errorf("epoch: consumer session rebalanced while shard claims were being installed")
	}
	return assigned, nil
}

func claimGenerations(document Document, target uint64) ([]uint64, error) {
	checkpoint, ok := document["checkpoint"].(map[string]any)
	if !ok {
		if typed, typedOK := document["checkpoint"].(Document); typedOK {
			checkpoint = map[string]any(typed)
		} else {
			return nil, fmt.Errorf("checkpoint observation is missing")
		}
	}
	current := uint64(0)
	if checkpoint["exists"] == true {
		raw, ok := checkpoint["group_generation"].(string)
		if !ok {
			return nil, fmt.Errorf("checkpoint generation is missing")
		}
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || parsed == 0 || strconv.FormatUint(parsed, 10) != raw {
			return nil, fmt.Errorf("checkpoint generation is invalid")
		}
		current = parsed
	}
	if current > target {
		return nil, fmt.Errorf("checkpoint generation %d is ahead of session generation %d", current, target)
	}
	start := uint64(1)
	if current > 0 {
		start = current
		if current < target {
			start++
		}
	}
	count := target - start + 1
	if count > maxStreamClaimTransitions {
		return nil, fmt.Errorf("claim requires %d transitions; maximum is %d", count, maxStreamClaimTransitions)
	}
	generations := make([]uint64, 0, count)
	for value := start; ; value++ {
		generations = append(generations, value)
		if value == target {
			break
		}
	}
	return generations, nil
}

func coordinatedAssignment(document Document, group, member string, generation uint64) ([]uint32, error) {
	session, ok := document["session"].(map[string]any)
	if !ok {
		if typed, typedOK := document["session"].(Document); typedOK {
			session = map[string]any(typed)
		} else {
			return nil, fmt.Errorf("epoch: consumer session response omitted session state")
		}
	}
	if session["exists"] != true || session["group"] != group || session["group_generation"] != strconv.FormatUint(generation, 10) {
		return nil, fmt.Errorf("epoch: consumer session generation is absent or fenced")
	}
	members, ok := session["members"].([]any)
	if !ok {
		return nil, fmt.Errorf("epoch: consumer session response omitted members")
	}
	for _, rawMember := range members {
		entry, ok := rawMember.(map[string]any)
		if !ok || entry["member_id"] != member {
			continue
		}
		rawShards, ok := entry["assigned_shards"].([]any)
		if !ok || len(rawShards) == 0 {
			return nil, fmt.Errorf("epoch: consumer member has no assigned shards")
		}
		assigned := make([]uint32, 0, len(rawShards))
		var previous uint32
		for _, rawShard := range rawShards {
			value, ok := rawShard.(float64)
			if !ok || value < 0 || value > float64(^uint32(0)) || value != float64(uint32(value)) {
				return nil, fmt.Errorf("epoch: consumer session returned an invalid shard assignment")
			}
			shard := uint32(value)
			if len(assigned) > 0 && shard <= previous {
				return nil, fmt.Errorf("epoch: consumer session returned an invalid shard assignment")
			}
			assigned = append(assigned, shard)
			previous = shard
		}
		return assigned, nil
	}
	return nil, fmt.Errorf("epoch: consumer member is not active in the requested session generation")
}

func requireAppliedGroupClaim(document Document, shard uint32) error {
	receipt, ok := document["receipt"].(map[string]any)
	if !ok {
		if typed, typedOK := document["receipt"].(Document); typedOK {
			receipt = map[string]any(typed)
		} else {
			return fmt.Errorf("epoch: shard %d claim response omitted its receipt", shard)
		}
	}
	if receipt["outcome"] != "applied" || receipt["session_fenced"] != true {
		return fmt.Errorf("epoch: shard %d rejected the coordinated consumer claim", shard)
	}
	return nil
}

func sameShards(left, right []uint32) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

// JoinConsumerSession creates or renews a member and returns its generation-fenced shard assignment.
func (client *RegionalStreamClient) JoinConsumerSession(ctx context.Context, stream, group, member string, sessionTimeout time.Duration, idempotencyKey string) (Document, error) {
	groupSegment, _, err := consumerSessionSegments(group, member)
	if err != nil {
		return nil, err
	}
	if sessionTimeout < minStreamSessionTimeout || sessionTimeout > maxStreamSessionTimeout || sessionTimeout%time.Millisecond != 0 {
		return nil, fmt.Errorf("epoch: consumer session timeout must be a whole millisecond between %s and %s", minStreamSessionTimeout, maxStreamSessionTimeout)
	}
	if err := validateRegionalIdempotencyKey(idempotencyKey); err != nil {
		return nil, err
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, 0, func(route regionalRoute) Request {
		return Request{Method: "POST", Path: "/groups/" + groupSegment + "/sessions", Body: struct {
			IdempotencyKey string `json:"idempotency_key"`
			ExpectedTerm   string `json:"expected_term"`
			MemberID       string `json:"member_id"`
			SessionTimeout string `json:"session_timeout_ms"`
		}{idempotencyKey, route.Term, member, strconv.FormatInt(sessionTimeout.Milliseconds(), 10)}}
	})
}

// HeartbeatConsumerSession renews one member using the current group generation fence.
func (client *RegionalStreamClient) HeartbeatConsumerSession(ctx context.Context, stream, group, member string, generation uint64, idempotencyKey string) (Document, error) {
	return client.mutateConsumerSession(ctx, "PUT", stream, group, member, generation, idempotencyKey, "/heartbeat")
}

// LeaveConsumerSession revokes a member and deterministically reassigns its shards.
func (client *RegionalStreamClient) LeaveConsumerSession(ctx context.Context, stream, group, member string, generation uint64, idempotencyKey string) (Document, error) {
	return client.mutateConsumerSession(ctx, "DELETE", stream, group, member, generation, idempotencyKey, "")
}

// MaintainConsumerSession expires members whose inclusive deadlines have passed.
func (client *RegionalStreamClient) MaintainConsumerSession(ctx context.Context, stream, group, idempotencyKey string) (Document, error) {
	groupSegment, err := segment(group, "consumer group")
	if err != nil {
		return nil, err
	}
	if err := validateRegionalIdempotencyKey(idempotencyKey); err != nil {
		return nil, err
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, 0, func(route regionalRoute) Request {
		return Request{Method: "POST", Path: "/groups/" + groupSegment + "/sessions/maintenance", Body: struct {
			IdempotencyKey string `json:"idempotency_key"`
			ExpectedTerm   string `json:"expected_term"`
		}{idempotencyKey, route.Term}}
	})
}

// ConsumerSession returns the linearizable membership, generation, deadlines, and assignments.
func (client *RegionalStreamClient) ConsumerSession(ctx context.Context, stream, group string) (Document, error) {
	return client.consumerSessionAtGeneration(ctx, stream, group, "")
}

func (client *RegionalStreamClient) consumerSessionAtGeneration(ctx context.Context, stream, group, resourceGeneration string) (Document, error) {
	groupSegment, err := segment(group, "consumer group")
	if err != nil {
		return nil, err
	}
	return regionalCallAtGeneration[Document](ctx, client.regionalClient(), "streams", "stream", stream, 0, resourceGeneration, func(_ regionalRoute) Request {
		return Request{Method: "GET", Path: "/groups/" + groupSegment + "/sessions", Headers: map[string]string{regionalReadHeader: "linearizable"}}
	})
}

func consumerSessionSegments(group, member string) (string, string, error) {
	groupSegment, err := segment(group, "consumer group")
	if err != nil {
		return "", "", err
	}
	memberSegment, err := segment(member, "consumer member")
	if err != nil {
		return "", "", err
	}
	return groupSegment, memberSegment, nil
}

func (client *RegionalStreamClient) mutateConsumerSession(ctx context.Context, method, stream, group, member string, generation uint64, idempotencyKey, suffix string) (Document, error) {
	groupSegment, memberSegment, err := consumerSessionSegments(group, member)
	if err != nil {
		return nil, err
	}
	if generation == 0 {
		return nil, fmt.Errorf("epoch: consumer group generation must be non-zero")
	}
	if err := validateRegionalIdempotencyKey(idempotencyKey); err != nil {
		return nil, err
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, 0, func(route regionalRoute) Request {
		return Request{Method: method, Path: "/groups/" + groupSegment + "/sessions/" + memberSegment + suffix, Body: struct {
			IdempotencyKey string `json:"idempotency_key"`
			ExpectedTerm   string `json:"expected_term"`
			Generation     string `json:"group_generation"`
		}{idempotencyKey, route.Term, strconv.FormatUint(generation, 10)}}
	})
}

func validateRegionalIdempotencyKey(idempotencyKey string) error {
	if strings.TrimSpace(idempotencyKey) == "" {
		return fmt.Errorf("epoch: idempotency key is required")
	}
	return nil
}

// ConfigureRetention commits a replacement time/size/count policy and immediately applies it.
func (client *RegionalStreamClient) ConfigureRetention(ctx context.Context, stream string, shard uint32, idempotencyKey string, policy StreamRetentionPolicy) (Document, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, fmt.Errorf("epoch: idempotency key is required")
	}
	if err := policy.validate(); err != nil {
		return nil, err
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(route regionalRoute) Request {
		body := struct {
			IdempotencyKey         string `json:"idempotency_key"`
			ExpectedTerm           string `json:"expected_term"`
			MaxRecordsPerPartition uint64 `json:"max_records_per_partition,omitempty"`
			MaxBytesPerPartition   string `json:"max_bytes_per_partition,omitempty"`
			MaxAgeMS               string `json:"max_age_ms,omitempty"`
		}{
			IdempotencyKey:         idempotencyKey,
			ExpectedTerm:           route.Term,
			MaxRecordsPerPartition: policy.MaxRecordsPerPartition,
		}
		if policy.MaxBytesPerPartition != 0 {
			body.MaxBytesPerPartition = strconv.FormatUint(policy.MaxBytesPerPartition, 10)
		}
		if policy.MaxAgeMS != 0 {
			body.MaxAgeMS = strconv.FormatUint(policy.MaxAgeMS, 10)
		}
		return Request{Method: "PUT", Path: "/retention", Body: body}
	})
}

// MaintainRetention commits an idle-stream age sweep using the current leader time.
func (client *RegionalStreamClient) MaintainRetention(ctx context.Context, stream string, shard uint32, idempotencyKey string) (Document, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, fmt.Errorf("epoch: idempotency key is required")
	}
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(route regionalRoute) Request {
		return Request{Method: "POST", Path: "/retention/maintenance", Body: struct {
			IdempotencyKey string `json:"idempotency_key"`
			ExpectedTerm   string `json:"expected_term"`
		}{idempotencyKey, route.Term}}
	})
}

// Retention returns the linearizable policy, watermark, retained boundary, and byte count.
func (client *RegionalStreamClient) Retention(ctx context.Context, stream string, shard uint32) (Document, error) {
	return regionalCall[Document](ctx, client.regionalClient(), "streams", "stream", stream, shard, func(_ regionalRoute) Request {
		return Request{Method: "GET", Path: "/retention", Headers: map[string]string{regionalReadHeader: "linearizable"}}
	})
}

func (client *RegionalStreamClient) regionalClient() *regionalClient {
	if client == nil {
		return nil
	}
	return client.regional
}

func regionalCall[T any](ctx context.Context, client *regionalClient, collection, resourceLabel, resource string, shard uint32, requestFor func(regionalRoute) Request) (T, error) {
	return regionalCallAtGeneration[T](ctx, client, collection, resourceLabel, resource, shard, "", requestFor)
}

func regionalCallAtGeneration[T any](ctx context.Context, client *regionalClient, collection, resourceLabel, resource string, shard uint32, expectedGeneration string, requestFor func(regionalRoute) Request) (T, error) {
	var zero T
	if client == nil {
		return zero, fmt.Errorf("epoch: regional %s client is not configured", resourceLabel)
	}
	basePath, err := client.resourceShardPath(collection, resourceLabel, resource, shard)
	if err != nil {
		return zero, err
	}
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		transport, route, discoverErr := client.discoverLeader(ctx, basePath)
		if discoverErr != nil {
			lastErr = discoverErr
			if !regionalRediscoveryError(discoverErr) {
				return zero, discoverErr
			}
			continue
		}
		if expectedGeneration != "" && route.ResourceGeneration != expectedGeneration {
			return zero, fmt.Errorf(
				"epoch: Stream routing generation changed from %s to %s before the operation; no request was attempted",
				expectedGeneration,
				route.ResourceGeneration,
			)
		}
		request := requestFor(route)
		request.Path = basePath + request.Path
		request.Headers = mergedRegionalHeaders(request.Headers, client.token, route)
		var result T
		if callErr := transport.Do(ctx, request, &result); callErr == nil {
			return result, nil
		} else {
			lastErr = callErr
			if !regionalRediscoveryError(callErr) {
				return zero, callErr
			}
		}
	}
	return zero, fmt.Errorf("epoch: regional %s operation could not reach a current leader: %w", resourceLabel, lastErr)
}

func (client *regionalClient) discoverRoute(ctx context.Context, path string) (regionalRoute, error) {
	var lastErr error
	for _, transport := range client.transports {
		var route regionalRoute
		err := transport.Do(ctx, Request{Method: "GET", Path: path, Headers: map[string]string{regionalAuthorizationHeader: "Bearer " + client.token}}, &route)
		if err != nil {
			if !regionalRediscoveryError(err) {
				return regionalRoute{}, err
			}
			lastErr = err
			continue
		}
		if !validRegionalRoute(route) {
			lastErr = fmt.Errorf("epoch: regional route response is incomplete")
			continue
		}
		return route, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("epoch: no configured endpoint reported Stream routing metadata")
	}
	return regionalRoute{}, lastErr
}

func validatedStreamPartitioning(route regionalRoute) (regionalStreamPartitioning, error) {
	if route.StreamPartitioning == nil {
		return regionalStreamPartitioning{}, fmt.Errorf("epoch: regional Stream route omitted partitioning metadata")
	}
	partitioning := *route.StreamPartitioning
	if partitioning.Algorithm != streamPartitioner || partitioning.KeyEncoding != "utf8" ||
		partitioning.MissingKeyFallback != "event_id" || partitioning.ShardCount == 0 {
		return regionalStreamPartitioning{}, fmt.Errorf("epoch: regional Stream partitioning metadata is unsupported or incomplete")
	}
	return partitioning, nil
}

func (client *regionalClient) discoverLeader(ctx context.Context, path string) (Transport, regionalRoute, error) {
	var lastErr error
	for _, transport := range client.transports {
		var route regionalRoute
		err := transport.Do(ctx, Request{Method: "GET", Path: path, Headers: map[string]string{regionalAuthorizationHeader: "Bearer " + client.token}}, &route)
		if err != nil {
			if !regionalRediscoveryError(err) {
				return nil, regionalRoute{}, err
			}
			lastErr = err
			continue
		}
		if !validRegionalRoute(route) {
			lastErr = fmt.Errorf("epoch: regional route response is incomplete")
			continue
		}
		if route.AcceptsWrites {
			return transport, route, nil
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("epoch: no configured endpoint reported the current leader")
	}
	return nil, regionalRoute{}, lastErr
}

func validRegionalRoute(route regionalRoute) bool {
	for _, value := range []string{route.ResourceGeneration, route.TabletEpoch, route.Term} {
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil || parsed == 0 || value != strconv.FormatUint(parsed, 10) {
			return false
		}
	}
	return true
}

func mergedRegionalHeaders(headers map[string]string, token string, route regionalRoute) map[string]string {
	merged := make(map[string]string, len(headers)+3)
	for name, value := range headers {
		merged[name] = value
	}
	merged[regionalAuthorizationHeader] = "Bearer " + token
	merged[regionalGenerationHeader] = route.ResourceGeneration
	merged[regionalTabletEpochHeader] = route.TabletEpoch
	return merged
}

func regionalRediscoveryError(err error) bool {
	var failure *APIError
	if !errors.As(err, &failure) {
		return false
	}
	if failure.Retryable() {
		return true
	}
	if failure.Code == "fenced" {
		var routeFence struct {
			Retryable bool `json:"retryable"`
		}
		return json.Unmarshal(failure.Body, &routeFence) == nil && routeFence.Retryable
	}
	switch failure.Code {
	case "not_leader", "route_not_found", "route_unavailable", "read_barrier_timeout":
		return true
	default:
		return false
	}
}

func regionalScopePath(scope RegionalScope) (string, error) {
	organization, err := segment(scope.Organization, "organization")
	if err != nil {
		return "", err
	}
	project, err := segment(scope.Project, "project")
	if err != nil {
		return "", err
	}
	environment, err := segment(scope.Environment, "environment")
	if err != nil {
		return "", err
	}
	namespace, err := segment(scope.Namespace, "namespace")
	if err != nil {
		return "", err
	}
	return "/v1/organizations/" + organization + "/projects/" + project + "/environments/" + environment + "/namespaces/" + namespace, nil
}

func (client *regionalClient) resourceShardPath(collection, resourceLabel, resource string, shard uint32) (string, error) {
	resourceName, err := segment(resource, resourceLabel)
	if err != nil {
		return "", err
	}
	return client.scopePath + "/" + collection + "/" + resourceName + "/shards/" + strconv.FormatUint(uint64(shard), 10), nil
}

package io.epoch.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Authenticated, leader- and fence-aware client for regional Stream shards. */
public final class RegionalStreamClient {
  private static final int MAX_FETCH_RECORDS = 1_000;
  private static final Duration MIN_SESSION_TIMEOUT = Duration.ofSeconds(1);
  private static final Duration MAX_SESSION_TIMEOUT = Duration.ofMinutes(5);
  private static final int MAX_CLAIM_TRANSITIONS = 4_096;
  private static final Duration MIN_CAPTURE_INTERVAL = Duration.ofSeconds(1);
  private static final Duration MAX_CAPTURE_INTERVAL = Duration.ofDays(31);

  private final RegionalClientCore regional;

  /** Constructs a client over one or more regional node endpoints. */
  public RegionalStreamClient(
      List<URI> endpoints, String token, RegionalScope scope, Duration timeout) {
    this.regional = RegionalClientCore.forEndpoints(endpoints, token, scope, timeout);
  }

  private RegionalStreamClient(List<Transport> transports, String token, RegionalScope scope) {
    this.regional = RegionalClientCore.forTransports(transports, token, scope);
  }

  /** Constructs with injected transports for tests or custom networking. */
  public static RegionalStreamClient withTransports(
      List<Transport> transports, String token, RegionalScope scope) {
    return new RegionalStreamClient(transports, token, scope);
  }

  /** Appends one record using an explicit idempotency key. */
  public JsonNode append(String stream, int shard, String idempotencyKey, EventEnvelope event)
      throws IOException, InterruptedException {
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    Objects.requireNonNull(event, "event");
    return call(
        stream,
        shard,
        route -> {
          ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          body.put("partition", 0);
          body.set("envelope", event.toJson());
          return new RegionalClientCore.RequestSpec("POST", "/records", body, Map.of(), Map.of());
        });
  }

  /** Atomically appends one caller-framed batch to a single Stream shard. */
  public JsonNode appendBatch(
      String stream, int shard, String idempotencyKey, StreamBatchFrame frame)
      throws IOException, InterruptedException {
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    Objects.requireNonNull(frame, "frame");
    return call(
        stream,
        shard,
        route ->
            new RegionalClientCore.RequestSpec(
                "POST",
                "/records/batches",
                frame.toRequest(idempotencyKey, route.term()),
                Map.of(),
                Map.of()));
  }

  /**
   * Discovers the Stream partitioning contract and appends by event key, falling back to event ID.
   * The target generation is pinned so an expansion race cannot silently remap an uncertain write.
   */
  public JsonNode appendKeyed(String stream, String idempotencyKey, EventEnvelope event)
      throws IOException, InterruptedException {
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    Objects.requireNonNull(event, "event");
    RegionalClientCore.Route routing = regional.discoverRoute("streams", "Stream", stream, 0);
    RegionalClientCore.StreamPartitioning partitioning = routing.streamPartitioning();
    if (partitioning == null
        || !StreamPartitioner.ALGORITHM.equals(partitioning.algorithm())
        || !"utf8".equals(partitioning.keyEncoding())
        || !"event_id".equals(partitioning.missingKeyFallback())
        || partitioning.shardCount() <= 0) {
      throw new IOException("regional Stream partitioning metadata is unsupported or incomplete");
    }
    String partitionValue = event.key();
    if (partitionValue == null || partitionValue.isEmpty()) {
      partitionValue = event.id();
    }
    int shard = StreamPartitioner.shardFor(partitionValue, partitioning.shardCount());
    return regional.callAtGeneration(
        "streams",
        "Stream",
        stream,
        shard,
        routing.resourceGeneration(),
        route -> {
          ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          body.put("partition", 0);
          body.set("envelope", event.toJson());
          return new RegionalClientCore.RequestSpec("POST", "/records", body, Map.of(), Map.of());
        });
  }

  /** Performs a linearizable bounded fetch. */
  public JsonNode fetch(String stream, int shard, long offset, int limit)
      throws IOException, InterruptedException {
    return fetch(stream, shard, BigInteger.valueOf(offset), limit);
  }

  /** Performs a linearizable bounded fetch over the complete unsigned 64-bit offset range. */
  public JsonNode fetch(String stream, int shard, BigInteger offset, int limit)
      throws IOException, InterruptedException {
    RegionalClientCore.nonNegativeU64(offset, "offset");
    fetchLimit(limit);
    return call(
        stream,
        shard,
        route ->
            new RegionalClientCore.RequestSpec(
                "GET", "/records", null, Map.of("offset", offset, "limit", limit), linearizable()));
  }

  /** Performs a linearizable bounded fetch with explicit transaction visibility. */
  public JsonNode fetchWithIsolation(
      String stream, int shard, BigInteger offset, int limit, StreamReadIsolation isolation)
      throws IOException, InterruptedException {
    RegionalClientCore.nonNegativeU64(offset, "offset");
    fetchLimit(limit);
    Objects.requireNonNull(isolation, "isolation");
    return call(
        stream,
        shard,
        route ->
            new RegionalClientCore.RequestSpec(
                "GET",
                "/records",
                null,
                Map.of("offset", offset, "limit", limit, "isolation", isolation.wireValue()),
                linearizable()));
  }

  /**
   * Deterministically merges named shards. Each member read is independently linearizable; the
   * result is deliberately not advertised as an atomic cross-shard snapshot.
   */
  public JsonNode fetchSuperstream(
      List<StreamSuperstreamMember> members, int limit, StreamReadIsolation isolation)
      throws IOException, InterruptedException {
    Objects.requireNonNull(members, "members");
    Objects.requireNonNull(isolation, "isolation");
    if (members.isEmpty() || members.size() > 128) {
      throw new IllegalArgumentException("superstream must contain between 1 and 128 members");
    }
    fetchLimit(limit);
    Set<String> names = new HashSet<>();
    for (StreamSuperstreamMember member : members) {
      Objects.requireNonNull(member, "superstream member");
      if (!names.add(member.name())) {
        throw new IllegalArgumentException("duplicate superstream member: " + member.name());
      }
    }

    List<MergedStreamRecord> merged = new ArrayList<>();
    for (StreamSuperstreamMember member : members) {
      JsonNode response =
          fetchWithIsolation(member.stream(), member.shard(), member.offset(), limit, isolation);
      JsonNode records = response.path("records");
      if (!records.isArray()) {
        throw new IOException("superstream member " + member.name() + " response omitted records");
      }
      for (JsonNode record : records) {
        if (!record.isObject()) {
          throw new IOException(
              "superstream member " + member.name() + " returned an invalid record");
        }
        ObjectNode decorated = ((ObjectNode) record).deepCopy();
        decorated.put("member", member.name());
        merged.add(
            new MergedStreamRecord(
                responseU64(record.get("appended_at_ms"), "appended_at_ms"),
                member.name(),
                responseU64(record.get("partition"), "partition"),
                responseU64(record.get("offset"), "offset"),
                decorated));
      }
    }
    merged.sort(
        java.util.Comparator.comparing(MergedStreamRecord::appendedAt)
            .thenComparing(MergedStreamRecord::member)
            .thenComparing(MergedStreamRecord::partition)
            .thenComparing(MergedStreamRecord::offset));
    ArrayNode records = RegionalClientCore.MAPPER.createArrayNode();
    merged.stream().limit(limit).map(MergedStreamRecord::document).forEach(records::add);
    ObjectNode result = RegionalClientCore.MAPPER.createObjectNode();
    result.set("records", records);
    result.put("member_count", members.size());
    result.put("ordering", "appended_at_member_partition_offset");
    result.put("snapshot_scope", "independently_linearizable_members");
    return result;
  }

  /** Waits for visible records in a shared push or dedicated consumer lane. */
  public JsonNode consumeLongPoll(
      String stream,
      int shard,
      BigInteger offset,
      int limit,
      StreamReadIsolation isolation,
      StreamConsumerMode mode,
      String consumerId,
      Duration wait)
      throws IOException, InterruptedException {
    RegionalClientCore.nonNegativeU64(offset, "offset");
    fetchLimit(limit);
    Objects.requireNonNull(isolation, "isolation");
    Objects.requireNonNull(mode, "mode");
    Objects.requireNonNull(wait, "wait");
    long waitMs = wait.toMillis();
    if (waitMs < 1 || waitMs > 30_000) {
      throw new IllegalArgumentException("consumer wait must be between 1ms and 30s");
    }
    if (mode == StreamConsumerMode.DEDICATED) {
      RegionalClientCore.required(consumerId, "dedicated consumer ID");
    } else if (consumerId != null) {
      throw new IllegalArgumentException("push mode does not accept a consumer ID");
    }
    return call(
        stream,
        shard,
        route -> {
          Map<String, Object> query = new java.util.LinkedHashMap<>();
          query.put("offset", offset);
          query.put("limit", limit);
          query.put("isolation", isolation.wireValue());
          query.put("mode", mode.wireValue());
          query.put("wait_ms", waitMs);
          if (consumerId != null) {
            query.put("consumer_id", consumerId);
          }
          return new RegionalClientCore.RequestSpec(
              "GET", "/records/consume", null, query, linearizable());
        });
  }

  /** Appends one producer-epoch/sequence-fenced record. */
  public JsonNode appendIdempotent(
      String stream,
      int shard,
      String idempotencyKey,
      String producerId,
      BigInteger producerEpoch,
      BigInteger sequence,
      EventEnvelope event)
      throws IOException, InterruptedException {
    streamProducer(producerId, producerEpoch);
    RegionalClientCore.nonNegativeU64(sequence, "producer sequence");
    Objects.requireNonNull(event, "event");
    ObjectNode operation = RegionalClientCore.MAPPER.createObjectNode();
    operation.put("action", "append_idempotent");
    operation.put("producer_id", producerId);
    operation.put("producer_epoch", producerEpoch.toString());
    operation.put("sequence", sequence.toString());
    operation.put("partition", 0);
    operation.set("envelope", event.toJson());
    return mutateState(stream, shard, idempotencyKey, operation);
  }

  /** Opens or exactly replays a producer-fenced transaction. */
  public JsonNode beginTransaction(
      String stream,
      int shard,
      String idempotencyKey,
      String transactionId,
      String producerId,
      BigInteger producerEpoch)
      throws IOException, InterruptedException {
    RegionalClientCore.required(transactionId, "transaction ID");
    streamProducer(producerId, producerEpoch);
    ObjectNode operation = RegionalClientCore.MAPPER.createObjectNode();
    operation.put("action", "begin_transaction");
    operation.put("transaction_id", transactionId);
    operation.put("producer_id", producerId);
    operation.put("producer_epoch", producerEpoch.toString());
    return mutateState(stream, shard, idempotencyKey, operation);
  }

  /** Atomically appends a bounded sequence inside an open transaction. */
  public JsonNode appendTransaction(
      String stream,
      int shard,
      String idempotencyKey,
      String transactionId,
      String producerId,
      BigInteger producerEpoch,
      BigInteger sequence,
      List<EventEnvelope> events)
      throws IOException, InterruptedException {
    RegionalClientCore.required(transactionId, "transaction ID");
    streamProducer(producerId, producerEpoch);
    RegionalClientCore.nonNegativeU64(sequence, "producer sequence");
    Objects.requireNonNull(events, "events");
    if (events.isEmpty() || events.size() > 128 || events.stream().anyMatch(Objects::isNull)) {
      throw new IllegalArgumentException(
          "transaction append must contain between 1 and 128 non-null records");
    }
    ObjectNode operation = RegionalClientCore.MAPPER.createObjectNode();
    operation.put("action", "append_transaction");
    operation.put("transaction_id", transactionId);
    operation.put("producer_id", producerId);
    operation.put("producer_epoch", producerEpoch.toString());
    operation.put("sequence", sequence.toString());
    operation.put("partition", 0);
    var envelopes = operation.putArray("envelopes");
    events.forEach(event -> envelopes.add(event.toJson()));
    return mutateState(stream, shard, idempotencyKey, operation);
  }

  /** Makes transaction records visible and optionally advances one offset atomically. */
  public JsonNode commitTransaction(
      String stream,
      int shard,
      String idempotencyKey,
      String transactionId,
      StreamOffsetCommit offsetCommit)
      throws IOException, InterruptedException {
    RegionalClientCore.required(transactionId, "transaction ID");
    ObjectNode operation = RegionalClientCore.MAPPER.createObjectNode();
    operation.put("action", "commit_transaction");
    operation.put("transaction_id", transactionId);
    if (offsetCommit != null) {
      if (offsetCommit.partition() != shard) {
        throw new IllegalArgumentException(
            "transaction offset commit must target the transaction shard");
      }
      ObjectNode offset = operation.putObject("offset_commit");
      offset.put("group", offsetCommit.group());
      offset.put("partition", 0);
      offset.put("next_offset", offsetCommit.nextOffset().toString());
    }
    return mutateState(stream, shard, idempotencyKey, operation);
  }

  /** Permanently hides transaction records from read-committed consumers. */
  public JsonNode abortTransaction(
      String stream, int shard, String idempotencyKey, String transactionId)
      throws IOException, InterruptedException {
    RegionalClientCore.required(transactionId, "transaction ID");
    ObjectNode operation = RegionalClientCore.MAPPER.createObjectNode();
    operation.put("action", "abort_transaction");
    operation.put("transaction_id", transactionId);
    return mutateState(stream, shard, idempotencyKey, operation);
  }

  /** Retains the latest committed record per key and expires old tombstones. */
  public JsonNode compact(
      String stream, int shard, String idempotencyKey, Duration tombstoneRetention)
      throws IOException, InterruptedException {
    Objects.requireNonNull(tombstoneRetention, "tombstoneRetention");
    long milliseconds = tombstoneRetention.toMillis();
    if (milliseconds <= 0) {
      throw new IllegalArgumentException("tombstone retention must be positive");
    }
    ObjectNode operation = RegionalClientCore.MAPPER.createObjectNode();
    operation.put("action", "compact");
    operation.put("partition", 0);
    operation.put("tombstone_retention_ms", Long.toString(milliseconds));
    return mutateState(stream, shard, idempotencyKey, operation);
  }

  /** Moves a committed hot prefix into an immutable checksum-verified object. */
  public JsonNode tierPrefix(
      String stream, int shard, String idempotencyKey, BigInteger beforeOffset, int maxRecords)
      throws IOException, InterruptedException {
    RegionalClientCore.nonNegativeU64(beforeOffset, "tier before offset");
    if (maxRecords < 1 || maxRecords > 1_024) {
      throw new IllegalArgumentException("tier max records must be between 1 and 1024");
    }
    ObjectNode operation = RegionalClientCore.MAPPER.createObjectNode();
    operation.put("action", "tier_prefix");
    operation.put("partition", 0);
    operation.put("before_offset", beforeOffset.toString());
    operation.put("max_records", maxRecords);
    return mutateState(stream, shard, idempotencyKey, operation);
  }

  /** Captures one committed offset range in a portable open format. */
  public JsonNode capture(
      String stream,
      int shard,
      String idempotencyKey,
      String captureId,
      BigInteger firstOffset,
      BigInteger endOffset,
      StreamCaptureFormat format)
      throws IOException, InterruptedException {
    RegionalClientCore.required(captureId, "capture ID");
    RegionalClientCore.nonNegativeU64(firstOffset, "capture first offset");
    RegionalClientCore.nonNegativeU64(endOffset, "capture end offset");
    if (firstOffset.compareTo(endOffset) > 0) {
      throw new IllegalArgumentException("capture offset range must be ordered");
    }
    Objects.requireNonNull(format, "format");
    ObjectNode operation = RegionalClientCore.MAPPER.createObjectNode();
    operation.put("action", "capture");
    operation.put("capture_id", captureId);
    operation.put("partition", 0);
    operation.put("first_offset", firstOffset.toString());
    operation.put("end_offset", endOffset.toString());
    operation.put("format", format.wireValue());
    return mutateState(stream, shard, idempotencyKey, operation);
  }

  /** Enables leader-driven periodic capture with a replicated offset checkpoint. */
  public JsonNode configureCaptureSchedule(
      String stream,
      int shard,
      String idempotencyKey,
      String scheduleId,
      Duration interval,
      StreamCaptureFormat format)
      throws IOException, InterruptedException {
    RegionalClientCore.required(scheduleId, "capture schedule ID");
    Objects.requireNonNull(interval, "interval");
    Objects.requireNonNull(format, "format");
    long intervalMs = interval.toMillis();
    if (interval.compareTo(MIN_CAPTURE_INTERVAL) < 0
        || interval.compareTo(MAX_CAPTURE_INTERVAL) > 0
        || !interval.equals(Duration.ofMillis(intervalMs))) {
      throw new IllegalArgumentException(
          "capture interval must be between 1 second and 31 days in whole milliseconds");
    }
    ObjectNode operation = RegionalClientCore.MAPPER.createObjectNode();
    operation.put("action", "configure_capture_schedule");
    operation.put("schedule_id", scheduleId);
    operation.put("partition", 0);
    operation.put("interval_ms", Long.toString(intervalMs));
    operation.put("format", format.wireValue());
    return mutateState(stream, shard, idempotencyKey, operation);
  }

  /** Applies one contiguous source batch with checkpoint and loop fencing. */
  public JsonNode replicate(
      String stream, int shard, String idempotencyKey, StreamReplicationBatch replication)
      throws IOException, InterruptedException {
    Objects.requireNonNull(replication, "replication");
    ObjectNode operation = RegionalClientCore.MAPPER.createObjectNode();
    operation.put("action", "replicate");
    operation.put("local_partition", 0);
    ObjectNode batch = operation.putObject("batch");
    batch.put("source_cluster", replication.sourceCluster());
    batch.put("source_stream", replication.sourceStream());
    batch.put("source_partition", replication.sourcePartition());
    batch.put("first_source_offset", replication.firstSourceOffset().toString());
    var records = batch.putArray("records");
    for (StreamReplicationRecord record : replication.records()) {
      ObjectNode encoded = records.addObject();
      encoded.put("source_offset", record.sourceOffset().toString());
      encoded.set("envelope", record.envelope().toJson());
      encoded.set(
          "traversed_clusters", RegionalClientCore.MAPPER.valueToTree(record.traversedClusters()));
    }
    return mutateState(stream, shard, idempotencyKey, operation);
  }

  /** Returns one linearizable transaction observation. */
  public JsonNode transaction(String stream, int shard, String transactionId)
      throws IOException, InterruptedException {
    String transaction = RegionalClientCore.segment(transactionId, "transaction");
    return linearizableRead(stream, shard, "/transactions/" + transaction, Map.of());
  }

  /** Lists immutable tier manifests for one shard. */
  public JsonNode tierObjects(String stream, int shard) throws IOException, InterruptedException {
    return linearizableRead(stream, shard, "/tier/objects", Map.of());
  }

  /** Returns one retained capture artifact and checksum. */
  public JsonNode captureArtifact(String stream, int shard, String captureId)
      throws IOException, InterruptedException {
    String capture = RegionalClientCore.segment(captureId, "capture");
    return linearizableRead(stream, shard, "/captures/" + capture, Map.of());
  }

  /** Returns a replicated automatic-capture checkpoint and next deadline. */
  public JsonNode captureSchedule(String stream, int shard, String scheduleId)
      throws IOException, InterruptedException {
    String schedule = RegionalClientCore.segment(scheduleId, "capture schedule");
    return linearizableRead(stream, shard, "/capture-schedules/" + schedule, Map.of());
  }

  /** Estimates an online expand-only partition target. */
  public JsonNode partitionAdvice(
      String stream, BigInteger targetRecordsPerPartition, BigInteger targetBytesPerPartition)
      throws IOException, InterruptedException {
    RegionalClientCore.positiveU64(targetRecordsPerPartition, "target records per partition");
    RegionalClientCore.positiveU64(targetBytesPerPartition, "target bytes per partition");
    return linearizableRead(
        stream,
        0,
        "/partitions/advice",
        Map.of(
            "target_records_per_partition",
            targetRecordsPerPartition,
            "target_bytes_per_partition",
            targetBytesPerPartition));
  }

  /** Commits or explicitly resets a generation-fenced next offset. */
  public JsonNode commitOffset(
      String stream,
      int shard,
      String group,
      String memberId,
      long generation,
      long nextOffset,
      boolean reset,
      String idempotencyKey)
      throws IOException, InterruptedException {
    return commitOffset(
        stream,
        shard,
        group,
        memberId,
        BigInteger.valueOf(generation),
        BigInteger.valueOf(nextOffset),
        reset,
        idempotencyKey);
  }

  /** Commits or resets a checkpoint over the complete unsigned 64-bit position range. */
  public JsonNode commitOffset(
      String stream,
      int shard,
      String group,
      String memberId,
      BigInteger generation,
      BigInteger nextOffset,
      boolean reset,
      String idempotencyKey)
      throws IOException, InterruptedException {
    String groupSegment = RegionalClientCore.segment(group, "consumer group");
    RegionalClientCore.required(memberId, "consumer member");
    RegionalClientCore.positiveU64(generation, "consumer group generation");
    RegionalClientCore.nonNegativeU64(nextOffset, "next offset");
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    return call(
        stream,
        shard,
        route -> {
          ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          body.put("member_id", memberId);
          body.put("group_generation", generation.toString());
          body.put("partition", 0);
          body.put("next_offset", nextOffset.toString());
          body.put("mode", reset ? "reset" : "commit");
          return new RegionalClientCore.RequestSpec(
              "PUT", "/groups/" + groupSegment + "/offsets", body, Map.of(), Map.of());
        });
  }

  /** Returns a linearizable checkpoint and lag observation. */
  public JsonNode lag(String stream, int shard, String group)
      throws IOException, InterruptedException {
    return lagAtGeneration(stream, shard, group, null);
  }

  private JsonNode lagAtGeneration(
      String stream, int shard, String group, String resourceGeneration)
      throws IOException, InterruptedException {
    String groupSegment = RegionalClientCore.segment(group, "consumer group");
    return regional.callAtGeneration(
        "streams",
        "Stream",
        stream,
        shard,
        resourceGeneration,
        route ->
            new RegionalClientCore.RequestSpec(
                "GET", "/groups/" + groupSegment + "/lag", null, Map.of(), linearizable()));
  }

  /** Fetches records beginning at the durable group checkpoint. */
  public JsonNode fetchGroup(String stream, int shard, String group, int limit)
      throws IOException, InterruptedException {
    fetchLimit(limit);
    String groupSegment = RegionalClientCore.segment(group, "consumer group");
    return call(
        stream,
        shard,
        route ->
            new RegionalClientCore.RequestSpec(
                "GET",
                "/groups/" + groupSegment + "/records",
                null,
                Map.of("limit", limit),
                linearizable()));
  }

  /** Installs a coordinated-session generation as the durable owner fence on one shard. */
  public JsonNode claimGroup(
      String stream,
      int shard,
      String group,
      String memberId,
      long generation,
      String idempotencyKey)
      throws IOException, InterruptedException {
    return claimGroupAtGeneration(
        stream, shard, group, memberId, BigInteger.valueOf(generation), idempotencyKey, null);
  }

  private JsonNode claimGroupAtGeneration(
      String stream,
      int shard,
      String group,
      String memberId,
      BigInteger generation,
      String idempotencyKey,
      String resourceGeneration)
      throws IOException, InterruptedException {
    String groupSegment = RegionalClientCore.segment(group, "consumer group");
    RegionalClientCore.required(memberId, "consumer member");
    RegionalClientCore.positiveU64(generation, "consumer group generation");
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    return regional.callAtGeneration(
        "streams",
        "Stream",
        stream,
        shard,
        resourceGeneration,
        route -> {
          ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          body.put("member_id", memberId);
          body.put("group_generation", generation.toString());
          body.put("partition", 0);
          return new RegionalClientCore.RequestSpec(
              "PUT", "/groups/" + groupSegment + "/claim", body, Map.of(), Map.of());
        });
  }

  /** Installs a shard checkpoint fence over the complete unsigned generation range. */
  public JsonNode claimGroup(
      String stream,
      int shard,
      String group,
      String memberId,
      BigInteger generation,
      String idempotencyKey)
      throws IOException, InterruptedException {
    return claimGroupAtGeneration(stream, shard, group, memberId, generation, idempotencyKey, null);
  }

  /** Fetches only while the exact member and session generation own this shard. */
  public JsonNode fetchClaimedGroup(
      String stream, int shard, String group, String memberId, long generation, int limit)
      throws IOException, InterruptedException {
    return fetchClaimedGroup(stream, shard, group, memberId, BigInteger.valueOf(generation), limit);
  }

  /** Fetches with an unsigned session-generation fence. */
  public JsonNode fetchClaimedGroup(
      String stream, int shard, String group, String memberId, BigInteger generation, int limit)
      throws IOException, InterruptedException {
    fetchLimit(limit);
    String groupSegment = RegionalClientCore.segment(group, "consumer group");
    RegionalClientCore.required(memberId, "consumer member");
    RegionalClientCore.positiveU64(generation, "consumer group generation");
    return call(
        stream,
        shard,
        route ->
            new RegionalClientCore.RequestSpec(
                "GET",
                "/groups/" + groupSegment + "/claimed-records",
                null,
                Map.of(
                    "member_id", memberId,
                    "group_generation", generation.toString(),
                    "limit", limit),
                linearizable()));
  }

  /** Claims every assigned shard and rejects a concurrent coordinator rebalance. */
  public List<Integer> claimConsumerSession(
      String stream, String group, String memberId, long generation, String idempotencyKeyPrefix)
      throws IOException, InterruptedException {
    return claimConsumerSession(
        stream, group, memberId, BigInteger.valueOf(generation), idempotencyKeyPrefix);
  }

  /** Claims every assigned shard over the complete unsigned generation range. */
  public List<Integer> claimConsumerSession(
      String stream,
      String group,
      String memberId,
      BigInteger generation,
      String idempotencyKeyPrefix)
      throws IOException, InterruptedException {
    RegionalClientCore.segment(group, "consumer group");
    RegionalClientCore.required(memberId, "consumer member");
    RegionalClientCore.positiveU64(generation, "consumer group generation");
    RegionalClientCore.required(idempotencyKeyPrefix, "idempotency key prefix");
    String resourceGeneration =
        regional.discoverRoute("streams", "Stream", stream, 0).resourceGeneration();
    List<Integer> assigned =
        coordinatedAssignment(
            consumerSessionAtGeneration(stream, group, resourceGeneration),
            group,
            memberId,
            generation);
    List<PlannedClaim> claims = new ArrayList<>();
    for (int shard : assigned) {
      JsonNode lag = lagAtGeneration(stream, shard, group, resourceGeneration);
      for (BigInteger claimGeneration : claimGenerations(lag, generation)) {
        String key = idempotencyKeyPrefix + "-shard-" + shard + "-generation-" + claimGeneration;
        if (key.getBytes(StandardCharsets.UTF_8).length > 128) {
          throw new IllegalArgumentException(
              "derived consumer claim idempotency key exceeds 128 bytes");
        }
        claims.add(new PlannedClaim(shard, claimGeneration, key));
      }
    }
    for (PlannedClaim claim : claims) {
      JsonNode result =
          claimGroupAtGeneration(
              stream,
              claim.shard(),
              group,
              memberId,
              claim.generation(),
              claim.key(),
              resourceGeneration);
      JsonNode receipt = result.path("receipt");
      if (!"applied".equals(receipt.path("outcome").asText())
          || !receipt.path("session_fenced").asBoolean(false)) {
        throw new IOException(
            "shard " + claim.shard() + " rejected the coordinated consumer claim");
      }
    }
    List<Integer> revalidated =
        coordinatedAssignment(
            consumerSessionAtGeneration(stream, group, resourceGeneration),
            group,
            memberId,
            generation);
    if (!assigned.equals(revalidated)) {
      throw new IOException("consumer session rebalanced while shard claims were being installed");
    }
    return List.copyOf(assigned);
  }

  /** Joins or renews a coordinated consumer member and returns its shard assignment. */
  public JsonNode joinConsumerSession(
      String stream, String group, String memberId, Duration sessionTimeout, String idempotencyKey)
      throws IOException, InterruptedException {
    String groupSegment = RegionalClientCore.segment(group, "consumer group");
    RegionalClientCore.required(memberId, "consumer member");
    Objects.requireNonNull(sessionTimeout, "sessionTimeout");
    if (sessionTimeout.compareTo(MIN_SESSION_TIMEOUT) < 0
        || sessionTimeout.compareTo(MAX_SESSION_TIMEOUT) > 0
        || sessionTimeout.getNano() % 1_000_000 != 0) {
      throw new IllegalArgumentException(
          "consumer session timeout must be a whole millisecond between 1 second and 5 minutes");
    }
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    return call(
        stream,
        0,
        route -> {
          ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          body.put("member_id", memberId);
          body.put("session_timeout_ms", sessionTimeout.toMillis());
          return new RegionalClientCore.RequestSpec(
              "POST", "/groups/" + groupSegment + "/sessions", body, Map.of(), Map.of());
        });
  }

  /** Renews one coordinated member using the current group-generation fence. */
  public JsonNode heartbeatConsumerSession(
      String stream, String group, String memberId, long generation, String idempotencyKey)
      throws IOException, InterruptedException {
    return heartbeatConsumerSession(
        stream, group, memberId, BigInteger.valueOf(generation), idempotencyKey);
  }

  /** Renews one coordinated member over the complete unsigned generation range. */
  public JsonNode heartbeatConsumerSession(
      String stream, String group, String memberId, BigInteger generation, String idempotencyKey)
      throws IOException, InterruptedException {
    return mutateConsumerSession(
        "PUT", stream, group, memberId, generation, idempotencyKey, "/heartbeat");
  }

  /** Leaves a coordinated group and deterministically reassigns the member's shards. */
  public JsonNode leaveConsumerSession(
      String stream, String group, String memberId, long generation, String idempotencyKey)
      throws IOException, InterruptedException {
    return leaveConsumerSession(
        stream, group, memberId, BigInteger.valueOf(generation), idempotencyKey);
  }

  /** Leaves a coordinated group over the complete unsigned generation range. */
  public JsonNode leaveConsumerSession(
      String stream, String group, String memberId, BigInteger generation, String idempotencyKey)
      throws IOException, InterruptedException {
    return mutateConsumerSession("DELETE", stream, group, memberId, generation, idempotencyKey, "");
  }

  /** Commits an inclusive-deadline expiry sweep on the shard-zero coordinator. */
  public JsonNode maintainConsumerSession(String stream, String group, String idempotencyKey)
      throws IOException, InterruptedException {
    String groupSegment = RegionalClientCore.segment(group, "consumer group");
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    return call(
        stream,
        0,
        route -> {
          ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          return new RegionalClientCore.RequestSpec(
              "POST",
              "/groups/" + groupSegment + "/sessions/maintenance",
              body,
              Map.of(),
              Map.of());
        });
  }

  /** Returns linearizable coordinated membership, deadlines, generation, and assignments. */
  public JsonNode consumerSession(String stream, String group)
      throws IOException, InterruptedException {
    return consumerSessionAtGeneration(stream, group, null);
  }

  private JsonNode consumerSessionAtGeneration(
      String stream, String group, String resourceGeneration)
      throws IOException, InterruptedException {
    String groupSegment = RegionalClientCore.segment(group, "consumer group");
    return regional.callAtGeneration(
        "streams",
        "Stream",
        stream,
        0,
        resourceGeneration,
        route ->
            new RegionalClientCore.RequestSpec(
                "GET", "/groups/" + groupSegment + "/sessions", null, Map.of(), linearizable()));
  }

  private JsonNode mutateConsumerSession(
      String method,
      String stream,
      String group,
      String memberId,
      BigInteger generation,
      String idempotencyKey,
      String suffix)
      throws IOException, InterruptedException {
    String groupSegment = RegionalClientCore.segment(group, "consumer group");
    String memberSegment = RegionalClientCore.segment(memberId, "consumer member");
    RegionalClientCore.positiveU64(generation, "consumer group generation");
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    return call(
        stream,
        0,
        route -> {
          ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          body.put("group_generation", generation.toString());
          return new RegionalClientCore.RequestSpec(
              method,
              "/groups/" + groupSegment + "/sessions/" + memberSegment + suffix,
              body,
              Map.of(),
              Map.of());
        });
  }

  private static List<Integer> coordinatedAssignment(
      JsonNode document, String group, String memberId, BigInteger generation) throws IOException {
    JsonNode session = document.path("session");
    if (!session.isObject()) {
      throw new IOException("consumer session response omitted session state");
    }
    if (!session.path("exists").asBoolean(false)
        || !group.equals(session.path("group").asText())
        || !generation.toString().equals(session.path("group_generation").asText())) {
      throw new IOException("consumer session generation is absent or fenced");
    }
    JsonNode members = session.path("members");
    if (!members.isArray()) {
      throw new IOException("consumer session response omitted members");
    }
    for (JsonNode member : members) {
      if (!memberId.equals(member.path("member_id").asText())) {
        continue;
      }
      JsonNode shards = member.path("assigned_shards");
      if (!shards.isArray() || shards.isEmpty()) {
        throw new IOException("consumer member has no assigned shards");
      }
      List<Integer> assigned = new ArrayList<>(shards.size());
      int previous = -1;
      for (JsonNode shard : shards) {
        if (!shard.canConvertToInt() || shard.intValue() < 0 || shard.intValue() <= previous) {
          throw new IOException("consumer session returned an invalid shard assignment");
        }
        previous = shard.intValue();
        assigned.add(previous);
      }
      return assigned;
    }
    throw new IOException("consumer member is not active in the requested session generation");
  }

  private static List<BigInteger> claimGenerations(JsonNode document, BigInteger target)
      throws IOException {
    JsonNode checkpoint = document.path("checkpoint");
    if (!checkpoint.isObject()) {
      throw new IOException("checkpoint observation is missing");
    }
    BigInteger current = BigInteger.ZERO;
    if (checkpoint.path("exists").asBoolean(false)) {
      String raw = checkpoint.path("group_generation").asText();
      try {
        current = new BigInteger(raw);
        RegionalClientCore.positiveU64(current, "checkpoint generation");
        if (!current.toString().equals(raw)) {
          throw new NumberFormatException("non-canonical decimal");
        }
      } catch (IllegalArgumentException error) {
        throw new IOException("checkpoint generation is invalid", error);
      }
    }
    if (current.compareTo(target) > 0) {
      throw new IOException(
          "checkpoint generation " + current + " is ahead of session generation " + target);
    }
    BigInteger start = current.equals(target) ? current : current.add(BigInteger.ONE);
    BigInteger count = target.subtract(start).add(BigInteger.ONE);
    if (count.compareTo(BigInteger.valueOf(MAX_CLAIM_TRANSITIONS)) > 0) {
      throw new IOException(
          "claim requires " + count + " transitions; maximum is " + MAX_CLAIM_TRANSITIONS);
    }
    List<BigInteger> generations = new ArrayList<>(count.intValueExact());
    for (BigInteger value = start;
        value.compareTo(target) <= 0;
        value = value.add(BigInteger.ONE)) {
      generations.add(value);
    }
    return generations;
  }

  private record PlannedClaim(int shard, BigInteger generation, String key) {}

  /** Commits a replacement time/size/count policy and immediately applies it. */
  public JsonNode configureRetention(
      String stream, int shard, String idempotencyKey, StreamRetentionPolicy policy)
      throws IOException, InterruptedException {
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    Objects.requireNonNull(policy, "policy");
    return call(
        stream,
        shard,
        route -> {
          ObjectNode body = policy.toJson();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          return new RegionalClientCore.RequestSpec("PUT", "/retention", body, Map.of(), Map.of());
        });
  }

  /** Commits an idle-stream age sweep using the current leader time. */
  public JsonNode maintainRetention(String stream, int shard, String idempotencyKey)
      throws IOException, InterruptedException {
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    return call(
        stream,
        shard,
        route -> {
          ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          return new RegionalClientCore.RequestSpec(
              "POST", "/retention/maintenance", body, Map.of(), Map.of());
        });
  }

  /** Returns a linearizable policy, watermark, retained boundary, and byte count. */
  public JsonNode retention(String stream, int shard) throws IOException, InterruptedException {
    return call(
        stream,
        shard,
        route ->
            new RegionalClientCore.RequestSpec(
                "GET", "/retention", null, Map.of(), linearizable()));
  }

  private JsonNode mutateState(
      String stream, int shard, String idempotencyKey, ObjectNode operation)
      throws IOException, InterruptedException {
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    return call(
        stream,
        shard,
        route -> {
          ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          body.set("operation", operation);
          return new RegionalClientCore.RequestSpec("POST", "/state", body, Map.of(), Map.of());
        });
  }

  private JsonNode linearizableRead(
      String stream, int shard, String path, Map<String, Object> query)
      throws IOException, InterruptedException {
    return call(
        stream,
        shard,
        route -> new RegionalClientCore.RequestSpec("GET", path, null, query, linearizable()));
  }

  private static void streamProducer(String producerId, BigInteger producerEpoch) {
    RegionalClientCore.required(producerId, "producer ID");
    RegionalClientCore.positiveU64(producerEpoch, "producer epoch");
  }

  private static BigInteger responseU64(JsonNode value, String field) throws IOException {
    BigInteger parsed;
    if (value != null && value.isTextual()) {
      String text = value.textValue();
      if (!text.matches("0|[1-9][0-9]*")) {
        throw new IOException("Stream response " + field + " is invalid");
      }
      parsed = new BigInteger(text);
    } else if (value != null && value.isIntegralNumber()) {
      parsed = value.bigIntegerValue();
    } else {
      throw new IOException("Stream response omitted " + field);
    }
    try {
      RegionalClientCore.nonNegativeU64(parsed, "Stream response " + field);
    } catch (IllegalArgumentException error) {
      throw new IOException(error.getMessage(), error);
    }
    return parsed;
  }

  private record MergedStreamRecord(
      BigInteger appendedAt,
      String member,
      BigInteger partition,
      BigInteger offset,
      ObjectNode document) {}

  private JsonNode call(String stream, int shard, RegionalClientCore.RequestFactory requestFactory)
      throws IOException, InterruptedException {
    return regional.call("streams", "Stream", stream, shard, requestFactory);
  }

  private static Map<String, String> linearizable() {
    return Map.of("x-epoch-read-consistency", "linearizable");
  }

  private static void fetchLimit(int limit) {
    if (limit < 1 || limit > MAX_FETCH_RECORDS) {
      throw new IllegalArgumentException("fetch limit must be between 1 and " + MAX_FETCH_RECORDS);
    }
  }
}

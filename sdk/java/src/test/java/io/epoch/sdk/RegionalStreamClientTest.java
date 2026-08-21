package io.epoch.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.Test;

final class RegionalStreamClientTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void keyedAppendUsesThePublishedUtf8Partitioner() throws Exception {
    RecordingRegionalTransport leader =
        new RecordingRegionalTransport(
            MAPPER.readTree(
                """
                {
                  "resource_generation":"5",
                  "tablet_epoch":"3",
                  "term":"8",
                  "accepts_writes":true,
                  "stream_partitioning":{
                    "algorithm":"fnv1a64_utf8_mod_n_v1",
                    "key_encoding":"utf8",
                    "missing_key_fallback":"event_id",
                    "shard_count":16
                  }
                }
                """));
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(leader), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));
    EventEnvelope event =
        EventEnvelope.builder("checkout", "order.created", Map.of("id", "42"))
            .id("order-1")
            .key("customer-42")
            .timeMs(42)
            .build();

    assertEquals(14, StreamPartitioner.shardFor("customer-42", 16));
    assertEquals(13, StreamPartitioner.shardFor("order-1", 16));
    assertEquals(9, StreamPartitioner.shardFor("café", 16));
    assertEquals(15, StreamPartitioner.shardFor("東京", 16));
    client.appendKeyed("orders", "append-42", event);

    assertEquals(3, leader.requests.size());
    assertEquals(true, leader.requests.get(0).path().endsWith("/shards/0"));
    assertEquals(true, leader.requests.get(1).path().endsWith("/shards/14"));
    assertEquals(true, leader.requests.get(2).path().endsWith("/shards/14/records"));
  }

  @Test
  void keyedAppendFailsClosedWhenRoutingGenerationChanges() throws Exception {
    JsonNode bootstrap =
        MAPPER.readTree(
            """
            {"resource_generation":"5","tablet_epoch":"3","term":"8","accepts_writes":true,
             "stream_partitioning":{"algorithm":"fnv1a64_utf8_mod_n_v1","key_encoding":"utf8",
             "missing_key_fallback":"event_id","shard_count":16}}
            """);
    JsonNode target =
        MAPPER.readTree(
            """
            {"resource_generation":"6","tablet_epoch":"3","term":"9","accepts_writes":true,
             "stream_partitioning":{"algorithm":"fnv1a64_utf8_mod_n_v1","key_encoding":"utf8",
             "missing_key_fallback":"event_id","shard_count":16}}
            """);
    RecordingRegionalTransport transport =
        new RecordingRegionalTransport(Map.of("0", bootstrap, "14", target));
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(transport), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));
    EventEnvelope event =
        EventEnvelope.builder("checkout", "order.created", Map.of())
            .id("order-1")
            .key("customer-42")
            .build();

    IOException failure =
        assertThrows(IOException.class, () -> client.appendKeyed("orders", "append-42", event));

    assertEquals(true, failure.getMessage().contains("generation changed"));
    assertEquals(2, transport.requests.size());
  }

  @Test
  void appendDiscoversLeaderAndCarriesAuthenticationFencesAndTerm() throws Exception {
    RecordingRegionalTransport follower =
        new RecordingRegionalTransport(
            MAPPER.readTree(
                """
                {"resource_generation":"5","tablet_epoch":"3","term":"8","accepts_writes":false}
                """));
    RecordingRegionalTransport leader =
        new RecordingRegionalTransport(
            MAPPER.readTree(
                """
                {"resource_generation":"5","tablet_epoch":"3","term":"8","accepts_writes":true}
                """));
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(follower, leader),
            "secret-token",
            new RegionalScope("acme", "shop", "dev", "core"));
    EventEnvelope event =
        EventEnvelope.builder("checkout", "order.created", Map.of("id", "42"))
            .id("order-42")
            .timeMs(42)
            .build();

    JsonNode response = client.append("orders/eu", 0, "append-42", event);

    assertEquals("committed", response.path("state").asText());
    assertEquals(1, follower.requests.size());
    assertEquals(2, leader.requests.size());
    String path =
        "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core"
            + "/streams/orders%2Feu/shards/0";
    assertEquals(path, follower.requests.get(0).path());
    Request write = leader.requests.get(1);
    assertEquals("POST", write.method());
    assertEquals(path + "/records", write.path());
    assertEquals("Bearer secret-token", write.headers().get("authorization"));
    assertEquals("5", write.headers().get("x-epoch-resource-generation"));
    assertEquals("3", write.headers().get("x-epoch-tablet-epoch"));
    assertEquals("append-42", write.body().path("idempotency_key").asText());
    assertEquals("8", write.body().path("expected_term").asText());
  }

  @Test
  void groupAndLinearizableFetchContractsAreExplicit() throws Exception {
    RecordingRegionalTransport leader =
        new RecordingRegionalTransport(
            MAPPER.readTree(
                """
                {"resource_generation":"2","tablet_epoch":"4","term":"9","accepts_writes":true}
                """));
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(leader), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));

    client.commitOffset("orders", 0, "billing/eu", "member-a", 3, 11, false, "commit-11");
    client.fetch("orders", 0, 11, 25);

    Request commit = leader.requests.get(1);
    assertEquals("PUT", commit.method());
    assertEquals(true, commit.path().endsWith("/groups/billing%2Feu/offsets"));
    assertEquals("commit", commit.body().path("mode").asText());
    Request read = leader.requests.get(3);
    assertEquals("linearizable", read.headers().get("x-epoch-read-consistency"));
    assertEquals(BigInteger.valueOf(11), read.query().get("offset"));
    assertEquals(25, read.query().get("limit"));
  }

  @Test
  void coordinatedConsumerSessionContractsUseShardZero() throws Exception {
    RecordingRegionalTransport leader =
        new RecordingRegionalTransport(
            MAPPER.readTree(
                """
                {"resource_generation":"2","tablet_epoch":"4","term":"9","accepts_writes":true}
                """));
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(leader), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));

    client.joinConsumerSession(
        "orders", "billing/eu", "member-a", Duration.ofSeconds(30), "join-a");
    client.heartbeatConsumerSession("orders", "billing/eu", "member-a", 3, "heartbeat-a");
    client.consumerSession("orders", "billing/eu");
    client.maintainConsumerSession("orders", "billing/eu", "maintain-a");
    client.leaveConsumerSession("orders", "billing/eu", "member-a", 3, "leave-a");

    assertEquals(true, leader.requests.get(1).path().endsWith("/groups/billing%2Feu/sessions"));
    assertEquals("POST", leader.requests.get(1).method());
    assertEquals(
        true,
        leader.requests.get(3).path().endsWith("/groups/billing%2Feu/sessions/member-a/heartbeat"));
    assertEquals("PUT", leader.requests.get(3).method());
    assertEquals("linearizable", leader.requests.get(5).headers().get("x-epoch-read-consistency"));
    assertEquals(
        true, leader.requests.get(7).path().endsWith("/groups/billing%2Feu/sessions/maintenance"));
    assertEquals("DELETE", leader.requests.get(9).method());
    assertEquals(
        true, leader.requests.get(9).path().endsWith("/groups/billing%2Feu/sessions/member-a"));
    for (int index : List.of(0, 2, 4, 6, 8)) {
      assertEquals(true, leader.requests.get(index).path().endsWith("/shards/0"));
    }
  }

  @Test
  void claimsAndRevalidatesAssignedShardsBeforeFencedFetch() throws Exception {
    JsonNode session =
        MAPPER.readTree(
            """
            {"session":{"exists":true,"group":"billing/eu","shard_count":3,
             "group_generation":"1","members":[
               {"member_id":"member-a","assigned_shards":[0,2]}]}}
            """);
    JsonNode claim =
        MAPPER.readTree("{\"receipt\":{\"outcome\":\"applied\",\"session_fenced\":true}}");
    JsonNode unclaimed = MAPPER.readTree("{\"checkpoint\":{\"exists\":false}}");
    RecordingRegionalTransport leader =
        new RecordingRegionalTransport(
            MAPPER.readTree(
                """
                {"resource_generation":"2","tablet_epoch":"4","term":"9","accepts_writes":true}
                """),
            List.of(
                session,
                unclaimed,
                unclaimed,
                claim,
                claim,
                session,
                MAPPER.readTree("{\"records\":[]}")));
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(leader), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));

    assertEquals(
        List.of(0, 2),
        client.claimConsumerSession("orders", "billing/eu", "member-a", 1, "claim-cycle-a"));
    client.fetchClaimedGroup("orders", 2, "billing/eu", "member-a", 1, 25);

    List<Request> requests = leader.requests;
    assertEquals(true, requests.get(2).path().endsWith("/groups/billing%2Feu/sessions"));
    assertEquals(true, requests.get(4).path().endsWith("/groups/billing%2Feu/lag"));
    assertEquals(true, requests.get(6).path().endsWith("/groups/billing%2Feu/lag"));
    assertEquals(true, requests.get(8).path().endsWith("/groups/billing%2Feu/claim"));
    assertEquals(true, requests.get(10).path().endsWith("/groups/billing%2Feu/claim"));
    assertEquals(true, requests.get(12).path().endsWith("/groups/billing%2Feu/sessions"));
    assertEquals(true, requests.get(14).path().endsWith("/groups/billing%2Feu/claimed-records"));
    assertEquals(
        "claim-cycle-a-shard-0-generation-1",
        requests.get(8).body().path("idempotency_key").asText());
    assertEquals("member-a", requests.get(14).query().get("member_id"));
    assertEquals("1", requests.get(14).query().get("group_generation"));
    assertEquals(25, requests.get(14).query().get("limit"));
  }

  @Test
  void claimConsumerSessionBridgesAnOlderCheckpointGeneration() throws Exception {
    JsonNode session =
        MAPPER.readTree(
            """
            {"session":{"exists":true,"group":"billing","shard_count":1,
             "group_generation":"3","members":[
               {"member_id":"member-a","assigned_shards":[0]}]}}
            """);
    JsonNode lag =
        MAPPER.readTree(
            """
            {"checkpoint":{"exists":true,"group_generation":"1"}}
            """);
    JsonNode claim =
        MAPPER.readTree("{\"receipt\":{\"outcome\":\"applied\",\"session_fenced\":true}}");
    RecordingRegionalTransport leader =
        new RecordingRegionalTransport(
            MAPPER.readTree(
                """
                {"resource_generation":"2","tablet_epoch":"4","term":"9","accepts_writes":true}
                """),
            List.of(session, lag, claim, claim, session));
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(leader), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));

    assertEquals(
        List.of(0), client.claimConsumerSession("orders", "billing", "member-a", 3, "bridge-a"));
    assertEquals("2", leader.requests.get(6).body().path("group_generation").asText());
    assertEquals("3", leader.requests.get(8).body().path("group_generation").asText());
    assertEquals(
        "bridge-a-shard-0-generation-2",
        leader.requests.get(6).body().path("idempotency_key").asText());
    assertEquals(
        "bridge-a-shard-0-generation-3",
        leader.requests.get(8).body().path("idempotency_key").asText());
  }

  @Test
  void consumerFenceIsPreservedWithoutRoutingRediscovery() throws Exception {
    EpochApiException fenced =
        new EpochApiException(
            409,
            "fenced",
            "consumer member or session generation is fenced",
            MAPPER.readTree(
                """
                {"error":{"code":"fenced","outcome_certainty":"definite_not_committed"}}
                """));
    RecordingRegionalTransport leader =
        new RecordingRegionalTransport(
            MAPPER.readTree(
                """
                {"resource_generation":"2","tablet_epoch":"4","term":"9","accepts_writes":true}
                """),
            fenced);
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(leader), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));

    EpochApiException failure =
        assertThrows(
            EpochApiException.class,
            () -> client.fetchClaimedGroup("orders", 0, "billing", "member-old", 2, 1));

    assertEquals(fenced, failure);
    assertEquals(2, leader.requests.size());
  }

  @Test
  void canonicalGzipBatchKeepsExactFrameAndIdentityAcrossRetry() throws Exception {
    RecordingRegionalTransport leader =
        new RecordingRegionalTransport(
            MAPPER.readTree(
                """
                {"resource_generation":"2","tablet_epoch":"4","term":"9","accepts_writes":true}
                """),
            new EpochApiException(
                409, "not_leader", "leadership changed", MAPPER.createObjectNode()));
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(leader), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));
    List<StreamBatchRecord> records =
        List.of(
            new StreamBatchRecord(7, batchEvent("order-7", "customer-7")),
            new StreamBatchRecord(8, batchEvent("order-8", "customer-8")));
    StreamBatchFrame frame = StreamBatchFrame.encode(records, StreamCompression.GZIP);

    client.appendBatch("orders", 2, "batch-7", frame);

    assertEquals(4, leader.requests.size());
    Request first = leader.requests.get(1);
    Request second = leader.requests.get(3);
    assertEquals(true, first.path().endsWith("/records/batches"));
    assertEquals(first.body(), second.body());
    assertEquals("batch-7", first.body().path("idempotency_key").asText());
    assertEquals("gzip", first.body().path("compression").asText());
    assertEquals(2, first.body().path("record_count").asInt());
    byte[] compressed = Base64.getDecoder().decode(first.body().path("payload_base64").asText());
    String plain;
    try (GZIPInputStream input = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
      plain = new String(input.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    }
    String expected =
        "[{\"client_sequence\":7,\"envelope\":{"
            + "\"id\":\"order-7\",\"source\":\"checkout\","
            + "\"type\":\"order.created\",\"time_ms\":42,"
            + "\"key\":\"customer-7\",\"headers\":{\"a\":\"first\",\"z\":\"last\"},"
            + "\"content_type\":\"application/json\","
            + "\"payload\":{\"a\":7,\"z\":[{\"a\":1,\"y\":2}]},"
            + "\"priority\":0,\"extensions\":{\"a\":true,"
            + "\"z\":{\"a\":1,\"b\":2}}}},{\"client_sequence\":8,\"envelope\":{"
            + "\"id\":\"order-8\",\"source\":\"checkout\","
            + "\"type\":\"order.created\",\"time_ms\":42,"
            + "\"key\":\"customer-8\",\"headers\":{\"a\":\"first\",\"z\":\"last\"},"
            + "\"content_type\":\"application/json\","
            + "\"payload\":{\"a\":7,\"z\":[{\"a\":1,\"y\":2}]},"
            + "\"priority\":0,\"extensions\":{\"a\":true,"
            + "\"z\":{\"a\":1,\"b\":2}}}}]";
    assertEquals(expected, plain);
  }

  @Test
  void duplicateSequencesAndUnsupportedFramesFailBeforeNetwork() {
    EventEnvelope event = batchEvent("order-7", "customer-7");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StreamBatchFrame.encode(
                List.of(new StreamBatchRecord(7, event), new StreamBatchRecord(7, event)),
                StreamCompression.NONE));
    for (StreamCompression compression : StreamCompression.values()) {
      StreamBatchFrame.compressed(compression, 1, 1, new byte[] {1});
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> StreamBatchFrame.compressed(StreamCompression.NONE, 0, 2, new byte[] {1}));
  }

  @Test
  void canonicalBatchJsonKeepsSerdeCompatibleUnicode() throws Exception {
    EventEnvelope event =
        EventEnvelope.builder("checkout", "order.created", Map.of("message", "<paid>&\u2029"))
            .id("订单\u2028七")
            .key("東京")
            .timeMs(42)
            .build();
    StreamBatchFrame frame =
        StreamBatchFrame.encode(List.of(new StreamBatchRecord(1, event)), StreamCompression.NONE);
    RecordingRegionalTransport leader =
        new RecordingRegionalTransport(
            MAPPER.readTree(
                """
                {"resource_generation":"2","tablet_epoch":"4","term":"9","accepts_writes":true}
                """));
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(leader), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));
    client.appendBatch("orders", 0, "unicode-batch", frame);
    byte[] plain =
        Base64.getDecoder().decode(leader.requests.get(1).body().path("payload_base64").asText());
    String document = new String(plain, java.nio.charset.StandardCharsets.UTF_8);
    assertEquals(false, document.contains("\\u2028"));
    assertEquals(false, document.contains("\\u2029"));
    assertEquals(true, document.contains("订单\u2028七"));
    assertEquals(true, document.contains("<paid>&\u2029"));
  }

  private static EventEnvelope batchEvent(String id, String key) {
    return EventEnvelope.builder(
            "checkout", "order.created", Map.of("z", List.of(Map.of("y", 2, "a", 1)), "a", 7))
        .id(id)
        .key(key)
        .timeMs(42)
        .headers(Map.of("z", "last", "a", "first"))
        .extensions(Map.of("z", Map.of("b", 2, "a", 1), "a", true))
        .build();
  }

  @Test
  void retryableLeaderRaceRediscoversWithoutChangingMutationIdentity() throws Exception {
    RecordingRegionalTransport leader =
        new RecordingRegionalTransport(
            MAPPER.readTree(
                """
                {"resource_generation":"5","tablet_epoch":"3","term":"8","accepts_writes":true}
                """),
            new EpochApiException(
                409, "not_leader", "leadership changed", MAPPER.createObjectNode()));
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(leader), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));
    EventEnvelope event =
        EventEnvelope.builder("checkout", "order.created", Map.of("id", "42"))
            .id("order-42")
            .timeMs(42)
            .build();

    client.append("orders", 0, "append-42", event);

    assertEquals(4, leader.requests.size());
    assertEquals("append-42", leader.requests.get(1).body().path("idempotency_key").asText());
    assertEquals("append-42", leader.requests.get(3).body().path("idempotency_key").asText());
  }

  @Test
  void definitiveDiscoveryFailureIsPreserved() {
    DiscoveryErrorTransport denied =
        new DiscoveryErrorTransport(
            new EpochApiException(403, "forbidden", "scope denied", MAPPER.createObjectNode()));
    RecordingRegionalTransport unused =
        new RecordingRegionalTransport(
            MAPPER
                .createObjectNode()
                .put("resource_generation", "5")
                .put("tablet_epoch", "3")
                .put("term", "8")
                .put("accepts_writes", true));
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(denied, unused),
            "secret-token",
            new RegionalScope("acme", "shop", "dev", "core"));

    EpochApiException failure =
        assertThrows(EpochApiException.class, () -> client.fetch("orders", 0, 0, 1));

    assertEquals(403, failure.status());
    assertEquals("forbidden", failure.code());
    assertEquals(1, denied.requests.size());
    assertEquals(0, unused.requests.size());
  }

  @Test
  void offsetsPreserveTheCompleteUnsigned64BitJsonContract() throws Exception {
    RecordingRegionalTransport leader =
        new RecordingRegionalTransport(
            MAPPER.readTree(
                """
                {"resource_generation":"2","tablet_epoch":"4","term":"9","accepts_writes":true}
                """));
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(leader), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));
    BigInteger maximum = new BigInteger("18446744073709551615");

    client.fetch("orders", 0, maximum, 1);
    client.commitOffset(
        "orders", 0, "billing", "member-a", BigInteger.ONE, maximum, false, "commit-max");

    assertEquals(maximum, leader.requests.get(1).query().get("offset"));
    assertEquals(maximum.toString(), leader.requests.get(3).body().path("next_offset").asText());
  }

  @Test
  void retentionMutationsAndLinearizableObservationAreExplicit() throws Exception {
    RecordingRegionalTransport leader =
        new RecordingRegionalTransport(
            MAPPER.readTree(
                """
                {"resource_generation":"2","tablet_epoch":"4","term":"9","accepts_writes":true}
                """));
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(leader), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));
    StreamRetentionPolicy policy =
        new StreamRetentionPolicy(
            100, BigInteger.valueOf(1_048_576), BigInteger.valueOf(86_400_000));

    client.configureRetention("orders", 0, "retention-1", policy);
    client.maintainRetention("orders", 0, "retention-sweep-1");
    client.retention("orders", 0);

    Request configure = leader.requests.get(1);
    assertEquals("PUT", configure.method());
    assertEquals(true, configure.path().endsWith("/retention"));
    assertEquals("retention-1", configure.body().path("idempotency_key").asText());
    assertEquals("9", configure.body().path("expected_term").asText());
    assertEquals("1048576", configure.body().path("max_bytes_per_partition").asText());
    assertEquals("86400000", configure.body().path("max_age_ms").asText());
    Request maintenance = leader.requests.get(3);
    assertEquals("POST", maintenance.method());
    assertEquals(true, maintenance.path().endsWith("/retention/maintenance"));
    Request read = leader.requests.get(5);
    assertEquals("linearizable", read.headers().get("x-epoch-read-consistency"));
  }

  @Test
  void invalidRetentionPolicyFailsBeforeNetwork() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StreamRetentionPolicy(
                null, BigInteger.valueOf(3L * 1024 * 1024 + 1), BigInteger.ONE));
  }

  @Test
  void advancedStateContractsAreFencedBrowserSafeAndIsolationAware() throws Exception {
    RecordingRegionalTransport leader =
        new RecordingRegionalTransport(
            MAPPER.readTree(
                """
                {"resource_generation":"2","tablet_epoch":"4","term":"9","accepts_writes":true}
                """));
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(leader), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));
    BigInteger maximum = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    EventEnvelope event =
        EventEnvelope.builder("checkout", "order.created", Map.of("id", "42"))
            .id("order-42")
            .timeMs(42)
            .build();

    client.appendIdempotent("orders", 2, "producer-1", "checkout", maximum, maximum, event);
    client.beginTransaction("orders", 2, "tx-begin", "tx-1", "checkout", BigInteger.valueOf(7));
    client.commitTransaction(
        "orders", 2, "tx-commit", "tx-1", new StreamOffsetCommit("workers", 2, maximum));
    client.fetchWithIsolation("orders", 2, maximum, 10, StreamReadIsolation.READ_UNCOMMITTED);
    client.partitionAdvice("orders", BigInteger.valueOf(1_000), BigInteger.valueOf(1_048_576));
    client.consumeLongPoll(
        "orders",
        2,
        BigInteger.ZERO,
        10,
        StreamReadIsolation.READ_COMMITTED,
        StreamConsumerMode.DEDICATED,
        "analytics-a",
        Duration.ofSeconds(1));
    client.configureCaptureSchedule(
        "orders",
        2,
        "capture-schedule",
        "analytics",
        Duration.ofMinutes(1),
        StreamCaptureFormat.JSON_LINES);
    client.captureSchedule("orders", 2, "analytics");

    Request producer = leader.requests.get(1);
    assertEquals(true, producer.path().endsWith("/state"));
    assertEquals("9", producer.body().path("expected_term").asText());
    assertEquals(
        maximum.toString(), producer.body().path("operation").path("producer_epoch").asText());
    assertEquals(maximum.toString(), producer.body().path("operation").path("sequence").asText());

    JsonNode commit = leader.requests.get(5).body().path("operation").path("offset_commit");
    assertEquals(0, commit.path("partition").asInt());
    assertEquals(maximum.toString(), commit.path("next_offset").asText());
    Request isolated = leader.requests.get(7);
    assertEquals("read_uncommitted", isolated.query().get("isolation"));
    Request advice = leader.requests.get(9);
    assertEquals(true, advice.path().endsWith("/partitions/advice"));
    assertEquals("linearizable", advice.headers().get("x-epoch-read-consistency"));
    Request consume = leader.requests.get(11);
    assertEquals(true, consume.path().endsWith("/records/consume"));
    assertEquals("dedicated", consume.query().get("mode"));
    assertEquals("analytics-a", consume.query().get("consumer_id"));
    JsonNode schedule = leader.requests.get(13).body().path("operation");
    assertEquals("configure_capture_schedule", schedule.path("action").asText());
    assertEquals("60000", schedule.path("interval_ms").asText());
    assertEquals(true, leader.requests.get(15).path().endsWith("/capture-schedules/analytics"));
  }

  @Test
  void scopeAndMutationInputsFailBeforeNetwork() {
    assertThrows(
        IllegalArgumentException.class, () -> new RegionalScope("", "shop", "dev", "core"));
  }

  @Test
  void superstreamMergesIndependentShardsDeterministically() throws Exception {
    JsonNode route =
        MAPPER.readTree(
            """
            {"resource_generation":"2","tablet_epoch":"4","term":"9","accepts_writes":true}
            """);
    RecordingRegionalTransport leader =
        new RecordingRegionalTransport(
            route,
            List.of(
                MAPPER.readTree(
                    """
                    {"records":[
                      {"appended_at_ms":"20","partition":0,"offset":"4","value":"a"},
                      {"appended_at_ms":"30","partition":0,"offset":"5","value":"c"}
                    ]}
                    """),
                MAPPER.readTree(
                    """
                    {"records":[
                      {"appended_at_ms":"10","partition":1,"offset":"9","value":"b"}
                    ]}
                    """)));
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(leader), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));

    JsonNode merged =
        client.fetchSuperstream(
            List.of(
                new StreamSuperstreamMember("orders", "orders", 0, 4),
                new StreamSuperstreamMember("audit", "audit", 1, 9)),
            2,
            StreamReadIsolation.READ_COMMITTED);

    assertEquals("b", merged.path("records").get(0).path("value").asText());
    assertEquals("audit", merged.path("records").get(0).path("member").asText());
    assertEquals("a", merged.path("records").get(1).path("value").asText());
    assertEquals(2, merged.path("member_count").asInt());
    assertEquals("independently_linearizable_members", merged.path("snapshot_scope").asText());
    assertEquals("read_committed", leader.requests.get(1).query().get("isolation"));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            client.fetchSuperstream(
                List.of(
                    new StreamSuperstreamMember("same", "orders", 0, 0),
                    new StreamSuperstreamMember("same", "audit", 0, 0)),
                10,
                StreamReadIsolation.READ_COMMITTED));
  }

  private record Request(
      String method,
      String path,
      JsonNode body,
      Map<String, ?> query,
      Map<String, String> headers) {}

  private static final class RecordingRegionalTransport implements Transport {
    private final JsonNode route;
    private final Map<String, JsonNode> routes;
    private final List<Request> requests = new ArrayList<>();
    private IOException nextOperationError;
    private final ArrayDeque<JsonNode> operationResponses;

    private RecordingRegionalTransport(JsonNode route) {
      this(route, (IOException) null);
    }

    private RecordingRegionalTransport(Map<String, JsonNode> routes) {
      this.route = MAPPER.nullNode();
      this.routes = Map.copyOf(routes);
      this.operationResponses = new ArrayDeque<>();
    }

    private RecordingRegionalTransport(JsonNode route, IOException nextOperationError) {
      this.route = route;
      this.routes = Map.of();
      this.nextOperationError = nextOperationError;
      this.operationResponses = new ArrayDeque<>();
    }

    private RecordingRegionalTransport(JsonNode route, List<JsonNode> operationResponses) {
      this.route = route;
      this.routes = Map.of();
      this.operationResponses = new ArrayDeque<>(operationResponses);
    }

    @Override
    public JsonNode request(String method, String path, JsonNode body, Map<String, ?> query) {
      throw new AssertionError("regional calls must use the header-aware transport contract");
    }

    @Override
    public JsonNode request(
        String method,
        String path,
        JsonNode body,
        Map<String, ?> query,
        Map<String, String> headers)
        throws IOException {
      requests.add(new Request(method, path, body, query, headers));
      if ("GET".equals(method) && path.matches(".*/shards/[0-9]+")) {
        String shard = path.substring(path.lastIndexOf('/') + 1);
        return routes.getOrDefault(shard, route);
      }
      if (nextOperationError != null) {
        IOException error = nextOperationError;
        nextOperationError = null;
        throw error;
      }
      if (!operationResponses.isEmpty()) {
        return operationResponses.removeFirst();
      }
      return MAPPER
          .createObjectNode()
          .put("state", "committed")
          .put("outcome_certainty", "committed");
    }
  }

  private static final class DiscoveryErrorTransport implements Transport {
    private final EpochApiException error;
    private final List<Request> requests = new ArrayList<>();

    private DiscoveryErrorTransport(EpochApiException error) {
      this.error = error;
    }

    @Override
    public JsonNode request(String method, String path, JsonNode body, Map<String, ?> query) {
      throw new AssertionError("regional calls must use the header-aware transport contract");
    }

    @Override
    public JsonNode request(
        String method,
        String path,
        JsonNode body,
        Map<String, ?> query,
        Map<String, String> headers)
        throws IOException {
      requests.add(new Request(method, path, body, query, headers));
      throw error;
    }
  }
}

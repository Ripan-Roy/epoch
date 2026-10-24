package io.epoch.sdk;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.MessageLite;
import io.epoch.sdk.gen.epoch.v1.ApplyResourceRequest;
import io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesRequest;
import io.epoch.sdk.gen.epoch.v1.DeleteResourceRequest;
import io.epoch.sdk.gen.epoch.v1.GetOperationRequest;
import io.epoch.sdk.gen.epoch.v1.GetResourceRequest;
import io.epoch.sdk.gen.epoch.v1.ListResourcesRequest;
import io.epoch.sdk.gen.epoch.v1.WatchResourceChangesRequest;
import io.grpc.Context;
import io.grpc.Metadata;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Public Java SDK probe for owned loopback Catalog tests, not a production gateway. */
public final class ManagementCatalogProbe {
  private static final ObjectMapper JSON =
      new ObjectMapper(
          JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build());

  private ManagementCatalogProbe() {}

  static void durableJson(Path path, JsonNode value) throws IOException {
    Path parent = path.toAbsolutePath().getParent();
    Path temporary =
        Files.createTempFile(
            parent,
            ".sdk-checkpoint-",
            "",
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
    try {
      try (FileChannel file = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
        ByteBuffer buffer = ByteBuffer.wrap(JSON.writeValueAsBytes(value));
        while (buffer.hasRemaining()) {
          file.write(buffer);
        }
        file.force(true);
      }
      Files.move(
          temporary, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      try (FileChannel directory = FileChannel.open(parent, StandardOpenOption.READ)) {
        directory.force(true);
      }
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  static ObjectNode invoke(ManagementClient client, ManagementContext context, JsonNode action)
      throws Exception {
    String method = action.path("method").asText();
    byte[] request = Base64.getDecoder().decode(action.path("request_proto").asText());
    ObjectNode proof =
        JSON.createObjectNode()
            .put("method", method)
            .put("request_proto", action.path("request_proto").asText())
            .put("grpc_code", 0)
            .put("attempts", 0)
            .put("budget", 0)
            .put("outcome_may_be_unknown", false);
    try {
      if (method.equals("WatchResourceChanges") || method.equals("WatchReconnect")) {
        watch(client, context, action, request, proof);
      } else {
        ManagementResult<? extends MessageLite> result =
            switch (method) {
              case "ApplyResource" ->
                  client.applyResource(context, ApplyResourceRequest.parseFrom(request));
              case "BatchApplyResources" ->
                  client.batchApplyResources(
                      context, BatchApplyResourcesRequest.parseFrom(request));
              case "DeleteResource" ->
                  client.deleteResource(context, DeleteResourceRequest.parseFrom(request));
              case "GetResource" ->
                  client.getResource(context, GetResourceRequest.parseFrom(request));
              case "GetOperation" ->
                  client.getOperation(context, GetOperationRequest.parseFrom(request));
              case "ListResources" ->
                  client.listResources(context, ListResourcesRequest.parseFrom(request));
              default -> throw new IllegalArgumentException("unplanned public management method");
            };
        proof.put(
            "response_proto", Base64.getEncoder().encodeToString(result.response().toByteArray()));
        info(proof, result.info());
      }
    } catch (ManagementRPCException error) {
      proof.put("grpc_code", error.status().getCode().value());
      info(proof, error.info());
    }
    return proof;
  }

  private static void info(ObjectNode proof, ManagementCallInfo value) {
    proof
        .put("attempts", value.attempts())
        .put("budget", value.budget())
        .put("outcome_may_be_unknown", value.outcomeMayBeUnknown());
  }

  private static void watch(
      ManagementClient client,
      ManagementContext context,
      JsonNode action,
      byte[] bytes,
      ObjectNode proof)
      throws Exception {
    String method = action.path("method").asText();
    var pages = proof.putArray("pages_proto");
    long target = 0;
    try (var stream =
        client.watchResourceChanges(context, WatchResourceChangesRequest.parseFrom(bytes))) {
      for (int index = 0; index < 512; index++) {
        var page = stream.receive();
        if (index == 0) {
          target = page.getLatestCursor();
        }
        String checkpoint = Long.toUnsignedString(page.getNextCursor());
        // Persist application progress before ACK, including empty filtered pages.
        durableJson(
            Path.of(action.path("checkpoint_path").asText()),
            JSON.createObjectNode()
                .put("schema", "epoch.sdk.management.checkpoint/v1")
                .put("language", "java")
                .put("next_cursor", checkpoint));
        stream.acknowledge(page.getNextCursor());
        pages.add(Base64.getEncoder().encodeToString(page.toByteArray()));
        proof.put("checkpoint", Long.toUnsignedString(stream.checkpoint()));
        info(proof, stream.info());
        if (method.equals("WatchReconnect") && index == 0) {
          durableJson(Path.of(action.path("ready_path").asText()), proof);
          Path release = Path.of(action.path("release_path").asText());
          long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(240);
          while (!Files.exists(release)) {
            if (context.context().isCancelled() || System.nanoTime() >= deadline) {
              throw new IOException("owned watch release was not observed");
            }
            Thread.sleep(10);
          }
        }
        if (method.equals("WatchReconnect") && stream.info().attempts() > 1
            || method.equals("WatchResourceChanges")
                && Long.compareUnsigned(page.getNextCursor(), target) >= 0) {
          return;
        }
      }
    }
    throw new IOException("watch exceeded its bounded page budget");
  }

  private static void execute(Path path, ObjectNode proof) throws Exception {
    JsonNode plan;
    try (var file = Files.newInputStream(path)) {
      byte[] data = file.readNBytes((8 << 20) + 1);
      if (data.length > 8 << 20) {
        throw new IOException("SDK plan exceeds its byte budget");
      }
      plan = JSON.readTree(data);
    }
    if (!plan.isObject()
        || plan.size() != 5
        || !plan.path("schema").asText().equals("epoch.sdk.management.plan/v1")
        || !plan.path("phase").isTextual()
        || plan.path("phase").asText().isEmpty()
        || !plan.path("endpoints").isArray()
        || !plan.path("actions").isArray()
        || plan.path("actions").isEmpty()
        || plan.path("actions").size() > 512
        || !plan.path("timeout_seconds").isInt()
        || plan.path("timeout_seconds").asInt() < 1
        || plan.path("timeout_seconds").asInt() > 120) {
      throw new IOException("invalid bounded public SDK plan");
    }
    proof.put("phase", plan.path("phase").asText());
    var endpoints = new ArrayList<String>();
    for (JsonNode endpoint : plan.path("endpoints")) {
      if (!endpoint.isTextual()) {
        throw new IOException("invalid planned authority");
      }
      endpoints.add(endpoint.asText());
    }
    try (var scheduler = Executors.newSingleThreadScheduledExecutor();
        var parent = Context.ROOT.withDeadlineAfter(300, TimeUnit.SECONDS, scheduler);
        var client =
            new ManagementClient(
                new ManagementConfig(
                    endpoints,
                    System.getenv("EPOCH_CONTROL_HA_GRPC_ADMIN_TOKEN"),
                    Duration.ofSeconds(plan.path("timeout_seconds").asInt()),
                    null,
                    true))) {
      var context = new ManagementContext(parent, new Metadata());
      for (JsonNode action : plan.path("actions")) {
        if (!action.isObject()
            || !action.path("expected_code").isInt()
            || action.path("expected_code").asInt() < 0
            || action.path("expected_code").asInt() > 16
            || action.has("expected_unknown") && !action.path("expected_unknown").isBoolean()) {
          throw new IOException("invalid planned public SDK action");
        }
        ObjectNode result = invoke(client, context, action);
        proof.withArray("actions").add(result);
        if (result.path("grpc_code").asInt() != action.path("expected_code").asInt()
            || action.has("expected_unknown")
                && result.path("outcome_may_be_unknown").asBoolean()
                    != action.path("expected_unknown").asBoolean()) {
          throw new IOException("public SDK witness differs from the planned code/unknown outcome");
        }
      }
    }
    proof.put("passed", true);
  }

  public static void main(String[] args) throws IOException {
    ObjectNode proof =
        JSON.createObjectNode()
            .put("schema", "epoch.sdk.management.probe/v1")
            .put("language", "java")
            .put("phase", "")
            .put("passed", false);
    proof.putArray("actions");
    boolean failed = false;
    try {
      if (args.length != 2 || !args[0].equals("--plan")) {
        throw new IOException("explicit owned plan path required");
      }
      execute(Path.of(args[1]), proof);
    } catch (Exception error) {
      failed = true;
      System.err.println("public Java SDK probe failed (" + error.getClass().getSimpleName() + ")");
    }
    System.out.println(JSON.writeValueAsString(proof));
    if (failed) {
      System.exit(1);
    }
  }
}

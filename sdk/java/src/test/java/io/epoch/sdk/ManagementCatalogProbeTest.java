package io.epoch.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.epoch.sdk.gen.epoch.v1.ApplyResourceResponse;
import io.epoch.sdk.gen.epoch.v1.BatchApplyResource;
import io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesRequest;
import io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesResponse;
import io.epoch.sdk.gen.epoch.v1.RegionalAdminServiceGrpc;
import io.epoch.sdk.gen.epoch.v1.Resource;
import io.epoch.sdk.gen.epoch.v1.ResourceKind;
import io.epoch.sdk.gen.epoch.v1.ResourceName;
import io.epoch.sdk.gen.epoch.v1.ResourceSpec;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class ManagementCatalogProbeTest {
  @Test
  void publicBatchKeepsItsOriginalTokenPresenceAndReceipt() throws Exception {
    var seen = new AtomicReference<BatchApplyResourcesRequest>();
    Server server =
        NettyServerBuilder.forPort(0)
            .addService(
                new RegionalAdminServiceGrpc.RegionalAdminServiceImplBase() {
                  @Override
                  public void batchApplyResources(
                      BatchApplyResourcesRequest request,
                      StreamObserver<BatchApplyResourcesResponse> response) {
                    seen.set(request);
                    var item = request.getResources(0);
                    response.onNext(
                        BatchApplyResourcesResponse.newBuilder()
                            .addResults(
                                ApplyResourceResponse.newBuilder()
                                    .setResource(
                                        Resource.newBuilder()
                                            .setName(item.getName())
                                            .setSpec(item.getSpec())
                                            .setGeneration(1))
                                    .setCreated(true)
                                    .setChanged(true))
                            .build());
                    response.onCompleted();
                  }
                })
            .build()
            .start();
    try (var client =
        new ManagementClient(
            new ManagementConfig(
                List.of("127.0.0.1:" + server.getPort()),
                "fixture-admin",
                Duration.ofSeconds(2),
                null,
                true))) {
      var request =
          BatchApplyResourcesRequest.newBuilder()
              .setRequestToken("original-token")
              .addResources(
                  BatchApplyResource.newBuilder()
                      .setName(
                          ResourceName.newBuilder()
                              .setOrganization("acme")
                              .setProject("shop")
                              .setEnvironment("dev")
                              .setNamespace("core")
                              .setKind(ResourceKind.RESOURCE_KIND_CACHE)
                              .setName("owned"))
                      .setSpec(ResourceSpec.newBuilder().setReplicas(3))
                      .setExpectedGeneration(0))
              .build();
      var action =
          new ObjectMapper()
              .createObjectNode()
              .put("method", "BatchApplyResources")
              .put("request_proto", Base64.getEncoder().encodeToString(request.toByteArray()))
              .put("expected_code", 0);
      var result = ManagementCatalogProbe.invoke(client, ManagementContext.current(), action);
      assertEquals(0, result.path("grpc_code").asInt());
      assertEquals(1, result.path("attempts").asInt());
      assertFalse(result.path("outcome_may_be_unknown").asBoolean());
      assertEquals(request, seen.get());
      assertTrue(seen.get().getResources(0).hasExpectedGeneration());
      assertFalse(result.path("response_proto").asText().isEmpty());
    } finally {
      server.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
    }
  }

  @Test
  void durableCheckpointPreservesUnsignedCursorAndAtomicReplacement(
      @TempDir java.nio.file.Path directory) throws Exception {
    var path = directory.resolve("checkpoint.json");
    var value = new ObjectMapper().createObjectNode().put("next_cursor", Long.toUnsignedString(-1));
    ManagementCatalogProbe.durableJson(path, value);
    assertEquals(
        "18446744073709551615",
        new ObjectMapper().readTree(Files.readAllBytes(path)).path("next_cursor").asText());
    ManagementCatalogProbe.durableJson(path, value.deepCopy().put("next_cursor", "0"));
    assertEquals(
        "0", new ObjectMapper().readTree(Files.readAllBytes(path)).path("next_cursor").asText());
    try (var contents = Files.list(directory)) {
      assertEquals(1, contents.count());
    }
  }

  @Test
  void malformedMethodAndWireAreNotConvertedIntoNetworkRequests() throws Exception {
    try (var client =
        new ManagementClient(
            new ManagementConfig(
                List.of("127.0.0.1:12345"), "fixture-admin", Duration.ofSeconds(1), null, true))) {
      for (String method : List.of("UnplannedMethod", "BatchApplyResources")) {
        var action =
            new ObjectMapper()
                .createObjectNode()
                .put("method", method)
                .put("request_proto", "/w==")
                .put("expected_code", 0);
        assertThrows(
            Exception.class,
            () -> ManagementCatalogProbe.invoke(client, ManagementContext.current(), action));
      }
    }
  }
}

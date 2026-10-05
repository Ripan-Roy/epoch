package io.epoch.examples;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.epoch.sdk.ManagementClient;
import io.epoch.sdk.ManagementConfig;
import io.epoch.sdk.ManagementContext;
import io.epoch.sdk.ManagementRPCException;
import io.epoch.sdk.TlsConfig;
import io.epoch.sdk.gen.epoch.v1.ApplyResourceRequest;
import io.epoch.sdk.gen.epoch.v1.BatchApplyResource;
import io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesRequest;
import io.epoch.sdk.gen.epoch.v1.DataClassification;
import io.epoch.sdk.gen.epoch.v1.DeleteResourceRequest;
import io.epoch.sdk.gen.epoch.v1.DurabilityProfile;
import io.epoch.sdk.gen.epoch.v1.GetOperationRequest;
import io.epoch.sdk.gen.epoch.v1.GetResourceRequest;
import io.epoch.sdk.gen.epoch.v1.ListResourcesRequest;
import io.epoch.sdk.gen.epoch.v1.PlacementPolicy;
import io.epoch.sdk.gen.epoch.v1.ResourceGovernance;
import io.epoch.sdk.gen.epoch.v1.ResourceKind;
import io.epoch.sdk.gen.epoch.v1.ResourceName;
import io.epoch.sdk.gen.epoch.v1.ResourceSpec;
import io.epoch.sdk.gen.epoch.v1.WatchResourceChangesRequest;
import io.epoch.sdk.gen.epoch.v1.WatchResourceChangesResponse;
import io.epoch.sdk.gen.epoch.v1.WorkloadProfile;
import io.grpc.Context;
import io.grpc.Metadata;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Compile-only reference. Use a dedicated test resource, never a production resource. */
public final class ManagementExample {
  private ManagementExample() {}

  /** The callback must process the page and durably persist its scanned cursor before returning. */
  public static void demonstrate(Consumer<WatchResourceChangesResponse> processAndCheckpoint)
      throws Exception {
    String token = required("EPOCH_EXAMPLE_REQUEST_TOKEN");
    if (!token.equals(token.strip()) || token.getBytes(StandardCharsets.UTF_8).length > 128) {
      throw new IllegalArgumentException("supply a canonical stable token of at most 128 bytes");
    }
    var config =
        new ManagementConfig(
            List.of(System.getenv().getOrDefault("EPOCH_CONTROL_GRPC_AUTHORITY", "localhost:8081")),
            required("EPOCH_BEARER_TOKEN"),
            Duration.ofSeconds(5),
            new TlsConfig(
                Path.of(required("EPOCH_TLS_CA_PATH")),
                Path.of(required("EPOCH_TLS_KEYSTORE_PATH")),
                required("EPOCH_TLS_KEYSTORE_PASSWORD").toCharArray()),
            false);
    var name =
        ResourceName.newBuilder()
            .setOrganization("acme")
            .setProject("payments")
            .setEnvironment("production")
            .setNamespace("orders")
            .setKind(ResourceKind.RESOURCE_KIND_CACHE)
            .setName("sdk-management-example")
            .build();
    var spec =
        ResourceSpec.newBuilder()
            .setWorkloadProfile(WorkloadProfile.WORKLOAD_PROFILE_CACHE)
            .setDurability(DurabilityProfile.DURABILITY_PROFILE_QUORUM_DURABLE)
            .setReplicas(3)
            .setGovernance(
                ResourceGovernance.newBuilder()
                    .setOwner("team:sdk-example")
                    .setCostCenter("cc-sdk")
                    .setClassification(DataClassification.DATA_CLASSIFICATION_INTERNAL))
            .setPlacement(
                PlacementPolicy.newBuilder()
                    .addAllowedRegions("ap-south")
                    .setMinimumZones(3)
                    .setRequiredNodeClass("general-purpose"))
            .setConfiguration(
                Struct.newBuilder()
                    .putFields("shard_count", Value.newBuilder().setNumberValue(1).build()))
            .build();
    var operationRequest =
        GetOperationRequest.newBuilder().setRequestToken(token).addAffectedResources(name).build();
    try (var scheduler = Executors.newSingleThreadScheduledExecutor();
        var parent = Context.ROOT.withDeadlineAfter(30, TimeUnit.SECONDS, scheduler);
        var client = new ManagementClient(config)) {
      var context = new ManagementContext(parent, new Metadata());
      try {
        client.batchApplyResources(
            context,
            BatchApplyResourcesRequest.newBuilder()
                .setRequestToken(token)
                .addResources(
                    BatchApplyResource.newBuilder()
                        .setName(name)
                        .setSpec(spec)
                        .setExpectedGeneration(0))
                .build());
      } catch (ManagementRPCException error) {
        System.out.println(
            "attempts="
                + error.info().attempts()
                + "; outcome unknown="
                + error.info().outcomeMayBeUnknown());
        if (error.info().outcomeMayBeUnknown()) {
          // Resolve the ORIGINAL token and scope, even if the old caller deadline expired.
          // NotFound is not non-commit. Never invent a new mutation/token here.
          client.getOperation(ManagementContext.current(), operationRequest);
        }
        throw error;
      }
      var operation = client.getOperation(context, operationRequest).response();
      var current =
          client
              .getResource(context, GetResourceRequest.newBuilder().setName(name).build())
              .response()
              .getResource();
      client.listResources(
          context,
          ListResourcesRequest.newBuilder()
              .setOrganization(name.getOrganization())
              .setProject(name.getProject())
              .setEnvironment(name.getEnvironment())
              .setNamespace(name.getNamespace())
              .setPageSize(50)
              .build()); // One bounded page, not a complete inventory.
      client.applyResource(
          context,
          ApplyResourceRequest.newBuilder()
              .setRequestToken(token + "-apply")
              .setName(name)
              .setSpec(spec)
              .setExpectedGeneration(current.getGeneration())
              .build());
      try (var watch =
          client.watchResourceChanges(
              context,
              WatchResourceChangesRequest.newBuilder()
                  .setAfterCursor(operation.getLastChangeCursor())
                  .setBatchSize(2)
                  .setOrganization(name.getOrganization())
                  .setProject(name.getProject())
                  .setEnvironment(name.getEnvironment())
                  .setNamespace(name.getNamespace())
                  .build())) {
        var page = watch.receive();
        processAndCheckpoint.accept(
            page); // Includes empty filtered pages; application owns durability.
        watch.acknowledge(page.getNextCursor());
      }
      // Cleanup is restricted to the dedicated example-owned generation.
      client.deleteResource(
          context,
          DeleteResourceRequest.newBuilder()
              .setRequestToken(token + "-delete")
              .setName(name)
              .setExpectedGeneration(current.getGeneration())
              .build());
    }
  }

  private static String required(String key) {
    String value = System.getenv(key);
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException("missing example configuration: " + key);
    }
    return value;
  }
}

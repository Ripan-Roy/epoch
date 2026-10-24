package io.epoch.sdk;

import io.epoch.sdk.gen.epoch.v1.ApplyResourceRequest;
import io.epoch.sdk.gen.epoch.v1.BatchApplyResource;
import io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesRequest;
import io.epoch.sdk.gen.epoch.v1.DeleteResourceRequest;
import io.epoch.sdk.gen.epoch.v1.DurabilityProfile;
import io.epoch.sdk.gen.epoch.v1.GetOperationRequest;
import io.epoch.sdk.gen.epoch.v1.GetResourceRequest;
import io.epoch.sdk.gen.epoch.v1.ListResourcesRequest;
import io.epoch.sdk.gen.epoch.v1.ResourceKind;
import io.epoch.sdk.gen.epoch.v1.ResourceName;
import io.epoch.sdk.gen.epoch.v1.ResourceSpec;
import io.epoch.sdk.gen.epoch.v1.WatchResourceChangesRequest;
import io.epoch.sdk.gen.epoch.v1.WorkloadProfile;
import io.grpc.Status;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/** Executed by the Go TLS fixture in CI, not a live Catalog quickstart. */
public final class ManagementTLSProbe {
  private ManagementTLSProbe() {}

  public static void main(String[] args) throws Exception {
    check(args.length == 5);
    String endpoint = args[0];
    TlsConfig tls =
        new TlsConfig(Path.of(args[1]), Path.of(args[2]), "epoch-wire-test".toCharArray());
    var config =
        new ManagementConfig(List.of(endpoint), "sdk-secret", Duration.ofSeconds(2), tls, false);
    var name =
        ResourceName.newBuilder()
            .setOrganization("acme")
            .setProject("shop")
            .setEnvironment("qa")
            .setNamespace("core")
            .setKind(ResourceKind.RESOURCE_KIND_CACHE)
            .setName("orders")
            .build();
    var spec =
        ResourceSpec.newBuilder()
            .setWorkloadProfile(WorkloadProfile.WORKLOAD_PROFILE_CACHE)
            .setDurability(DurabilityProfile.DURABILITY_PROFILE_QUORUM_DURABLE)
            .setReplicas(3)
            .build();
    var context = ManagementContext.current();
    var get = GetResourceRequest.newBuilder().setName(name).build();
    try (ManagementClient client = new ManagementClient(config)) {
      var applied =
          client.applyResource(
              context,
              ApplyResourceRequest.newBuilder()
                  .setName(name)
                  .setSpec(spec)
                  .setRequestToken("java-wire-apply")
                  .setExpectedGeneration(0)
                  .build());
      check(applied.response().getResource().getName().equals(name));
      check(applied.info().attempts() == 1 && !applied.info().outcomeMayBeUnknown());
      check(client.getResource(context, get).response().getResource().getName().equals(name));
      var listed =
          client.listResources(
              context,
              ListResourcesRequest.newBuilder()
                  .setOrganization("acme")
                  .setPageSize(50)
                  .putTags("test", "java-wire")
                  .build());
      check(listed.response().getResourcesCount() == 1);
      // This fixture token is serialization evidence, not real-server pagination.
      check(listed.response().getNextPageToken().equals("opaque-next-page"));
      check(
          client
                  .deleteResource(
                      context,
                      DeleteResourceRequest.newBuilder()
                          .setName(name)
                          .setRequestToken("java-wire-delete")
                          .setExpectedGeneration(0)
                          .build())
                  .response()
                  .getGeneration()
              == -1L);
      check(
          client
              .batchApplyResources(
                  context,
                  BatchApplyResourcesRequest.newBuilder()
                      .setRequestToken("java-wire-batch")
                      .addResources(
                          BatchApplyResource.newBuilder()
                              .setName(name)
                              .setSpec(spec)
                              .setExpectedGeneration(0))
                      .build())
              .response()
              .getReplayed());
      var operation =
          client
              .getOperation(
                  context,
                  GetOperationRequest.newBuilder()
                      .setRequestToken("java-wire-delete")
                      .addAffectedResources(name)
                      .build())
              .response();
      check(
          operation.getProposalId() == -1L
              && operation.hasExpectedGeneration()
              && operation.getExpectedGeneration() == 0);
      try (ManagementWatch watch =
          client.watchResourceChanges(
              context,
              WatchResourceChangesRequest.newBuilder()
                  .setOrganization("acme")
                  .setAfterCursor(10)
                  .setBatchSize(2)
                  .build())) {
        check(watch.receive().getNextCursor() == 13);
        watch.acknowledge(13);
        check(watch.checkpoint() == 13);
      }
    }
    assertDenied(
        new ManagementConfig(List.of(endpoint), "wrong-secret", config.timeout(), tls, false),
        get,
        Status.Code.UNAUTHENTICATED);
    assertDenied(
        new ManagementConfig(
            List.of(endpoint),
            "sdk-secret",
            config.timeout(),
            new TlsConfig(Path.of(args[1])),
            false),
        get,
        Status.Code.UNAVAILABLE);
    assertDenied(
        new ManagementConfig(
            List.of(endpoint),
            "sdk-secret",
            config.timeout(),
            new TlsConfig(Path.of(args[3]), Path.of(args[2]), "epoch-wire-test".toCharArray()),
            false),
        get,
        Status.Code.UNAVAILABLE);
    assertDenied(
        new ManagementConfig(List.of(args[4]), "sdk-secret", config.timeout(), tls, false),
        get,
        Status.Code.UNAVAILABLE);
    System.out.println(
        "Java management: seven TLS 1.3 RPCs, mTLS/bearer/foreign CA/TLS 1.2 denial,"
            + " uint64/OCC, remote close passed");
  }

  private static void assertDenied(
      ManagementConfig config, GetResourceRequest request, Status.Code code) throws Exception {
    try (ManagementClient client = new ManagementClient(config)) {
      try {
        client.getResource(ManagementContext.current(), request);
      } catch (ManagementRPCException error) {
        check(error.status().getCode() == code && error.info().attempts() == 1);
        check(!error.toString().contains("secret"));
        return;
      }
    }
    throw new IllegalStateException("Java management accepted an invalid trust/identity boundary");
  }

  private static void check(boolean condition) {
    if (!condition) {
      throw new IllegalStateException("Java management generated-wire contract failed");
    }
  }
}

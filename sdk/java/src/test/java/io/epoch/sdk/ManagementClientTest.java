package io.epoch.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.epoch.sdk.gen.epoch.v1.ApplyResourceRequest;
import io.epoch.sdk.gen.epoch.v1.ApplyResourceResponse;
import io.epoch.sdk.gen.epoch.v1.BatchApplyResource;
import io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesRequest;
import io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesResponse;
import io.epoch.sdk.gen.epoch.v1.DeleteResourceRequest;
import io.epoch.sdk.gen.epoch.v1.DeleteResourceResponse;
import io.epoch.sdk.gen.epoch.v1.GetOperationRequest;
import io.epoch.sdk.gen.epoch.v1.GetOperationResponse;
import io.epoch.sdk.gen.epoch.v1.GetResourceRequest;
import io.epoch.sdk.gen.epoch.v1.GetResourceResponse;
import io.epoch.sdk.gen.epoch.v1.ListResourcesRequest;
import io.epoch.sdk.gen.epoch.v1.ListResourcesResponse;
import io.epoch.sdk.gen.epoch.v1.OperationState;
import io.epoch.sdk.gen.epoch.v1.RegionalAdminServiceGrpc;
import io.epoch.sdk.gen.epoch.v1.Resource;
import io.epoch.sdk.gen.epoch.v1.ResourceChange;
import io.epoch.sdk.gen.epoch.v1.ResourceChangeKind;
import io.epoch.sdk.gen.epoch.v1.ResourceKind;
import io.epoch.sdk.gen.epoch.v1.ResourceName;
import io.epoch.sdk.gen.epoch.v1.ResourceSpec;
import io.epoch.sdk.gen.epoch.v1.WatchResourceChangesRequest;
import io.epoch.sdk.gen.epoch.v1.WatchResourceChangesResponse;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class ManagementClientTest {
  static final ResourceName NAME =
      ResourceName.newBuilder()
          .setOrganization("acme")
          .setProject("payments")
          .setEnvironment("test")
          .setNamespace("orders")
          .setKind(ResourceKind.RESOURCE_KIND_CACHE)
          .setName("management-test")
          .build();
  static final ResourceSpec SPEC = ResourceSpec.getDefaultInstance();
  static final Resource RESOURCE =
      Resource.newBuilder().setName(NAME).setSpec(SPEC).setGeneration(-1L).build();

  @Test
  void implementsAllSevenMethodsWithUnsignedPresenceAwareMessages() throws Exception {
    Fixture service = new Fixture();
    try (RunningServer server = new RunningServer(service);
        ManagementClient client = client(server.endpoint())) {
      ManagementContext context = ManagementContext.current();
      ApplyResourceRequest apply =
          ApplyResourceRequest.newBuilder()
              .setRequestToken("stable-apply")
              .setName(NAME)
              .setSpec(SPEC)
              .setExpectedGeneration(0)
              .build();
      assertEquals(
          -1L, client.applyResource(context, apply).response().getResource().getGeneration());
      assertEquals(1, client.getResource(context, get()).info().attempts());
      assertEquals(
          1,
          client
              .listResources(context, ListResourcesRequest.getDefaultInstance())
              .response()
              .getResourcesCount());
      DeleteResourceRequest delete =
          DeleteResourceRequest.newBuilder()
              .setRequestToken("stable-delete")
              .setName(NAME)
              .setExpectedGeneration(-1L)
              .build();
      assertEquals(-1L, client.deleteResource(context, delete).response().getGeneration());
      BatchApplyResourcesRequest batch =
          BatchApplyResourcesRequest.newBuilder()
              .setRequestToken("stable-batch")
              .addResources(
                  BatchApplyResource.newBuilder()
                      .setName(NAME)
                      .setSpec(SPEC)
                      .setExpectedGeneration(0))
              .build();
      assertEquals(1, client.batchApplyResources(context, batch).response().getResultsCount());
      GetOperationRequest operation =
          GetOperationRequest.newBuilder()
              .setRequestToken("stable-delete")
              .addAffectedResources(NAME)
              .build();
      GetOperationResponse receipt = client.getOperation(context, operation).response();
      assertEquals(-1L, receipt.getProposalId());
      assertTrue(receipt.hasExpectedGeneration());
      assertEquals(0, receipt.getExpectedGeneration());
      try (ManagementWatch watch = client.watchResourceChanges(context, watchRequest(0))) {
        WatchResourceChangesResponse page = watch.receive();
        assertEquals(2, page.getNextCursor());
        assertEquals(0, watch.checkpoint());
        assertThrows(ManagementRPCException.class, watch::receive);
        assertThrows(ManagementRPCException.class, () -> watch.acknowledge(1));
        watch.acknowledge(2);
        assertEquals(2, watch.checkpoint());
      }
      assertTrue(((ApplyResourceRequest) service.requests.get(0)).hasExpectedGeneration());
      assertEquals(0, ((ApplyResourceRequest) service.requests.get(0)).getExpectedGeneration());
      assertEquals(-1L, ((DeleteResourceRequest) service.requests.get(3)).getExpectedGeneration());
      assertEquals(7, service.requests.size());
    }
  }

  @Test
  void onlyUnavailableFailsOverAndPreservesTheOriginalMutation() throws Exception {
    Fixture first = new Fixture();
    first.failure = Status.UNAVAILABLE.withDescription("backend-private-text");
    Fixture second = new Fixture();
    try (RunningServer a = new RunningServer(first);
        RunningServer b = new RunningServer(second);
        ManagementClient client = client(a.endpoint(), b.endpoint())) {
      DeleteResourceRequest request =
          DeleteResourceRequest.newBuilder()
              .setName(NAME)
              .setRequestToken("original")
              .setExpectedGeneration(0)
              .build();
      ManagementResult<DeleteResourceResponse> result =
          client.deleteResource(ManagementContext.current(), request);
      assertEquals(new ManagementCallInfo(2, 2, false), result.info());
      assertEquals(request, first.requests.getFirst());
      assertEquals(request, second.requests.getFirst());
      first.failure = Status.PERMISSION_DENIED.withDescription("backend-private-text");
      ManagementRPCException failure =
          assertThrows(
              ManagementRPCException.class,
              () -> client.deleteResource(ManagementContext.current(), request));
      assertEquals(Status.Code.PERMISSION_DENIED, failure.status().getCode());
      assertEquals(new ManagementCallInfo(1, 2, true), failure.info());
      assertFalse(failure.toString().contains("backend-private-text"));
      assertEquals("backend-private-text", failure.status().getDescription());
      assertEquals(1, second.requests.size());
    }
  }

  @Test
  void rejectsMalformedReceiptsWithoutSemanticRetry() throws Exception {
    Fixture first = new Fixture();
    first.resource = RESOURCE.toBuilder().setName(NAME.toBuilder().setName("foreign")).build();
    Fixture second = new Fixture();
    try (RunningServer a = new RunningServer(first);
        RunningServer b = new RunningServer(second);
        ManagementClient client = client(a.endpoint(), b.endpoint())) {
      ManagementRPCException failure =
          assertThrows(
              ManagementRPCException.class,
              () -> client.getResource(ManagementContext.current(), get()));
      assertEquals(Status.Code.DATA_LOSS, failure.status().getCode());
      assertEquals(1, failure.info().attempts());
      assertTrue(second.requests.isEmpty());
    }
  }

  @Test
  void validatesInputsAndPreDispatchCancellationWithoutNetworkActivity() throws Exception {
    Fixture service = new Fixture();
    try (RunningServer server = new RunningServer(service);
        ManagementClient client = client(server.endpoint());
        Context.CancellableContext canceled = Context.ROOT.withCancellation()) {
      canceled.cancel(null);
      ManagementRPCException failure =
          assertThrows(
              ManagementRPCException.class,
              () -> client.getResource(new ManagementContext(canceled, new Metadata()), get()));
      assertEquals(Status.Code.CANCELLED, failure.status().getCode());
      assertEquals(0, failure.info().attempts());
      assertFalse(failure.info().outcomeMayBeUnknown());
      assertThrows(
          ManagementRPCException.class,
          () ->
              client.applyResource(
                  ManagementContext.current(),
                  ApplyResourceRequest.newBuilder()
                      .setName(NAME)
                      .setRequestToken("stable")
                      .build()));
      assertThrows(
          ManagementRPCException.class,
          () ->
              client.batchApplyResources(
                  ManagementContext.current(),
                  BatchApplyResourcesRequest.newBuilder().setRequestToken("stable").build()));
      assertTrue(service.requests.isEmpty());
    }
  }

  @Test
  void validatesTheWholeEndpointAllowlistAndHidesCredentials() {
    ManagementConfig config =
        new ManagementConfig(
            List.of("127.0.0.1:1"), "secret-bearer", Duration.ofSeconds(1), null, true);
    assertFalse(config.toString().contains("secret-bearer"));
    for (String target :
        List.of(
            "https://localhost:1",
            "localhost:01",
            "localhost:0",
            "localhost:65536",
            "dns:///localhost:1",
            "localhost:1/path",
            "example.com:1")) {
      assertThrows(
          ManagementRPCException.class,
          () ->
              new ManagementClient(
                  new ManagementConfig(
                      List.of("localhost:1", target),
                      "secret",
                      Duration.ofSeconds(1),
                      null,
                      true)));
    }
  }

  static GetResourceRequest get() {
    return GetResourceRequest.newBuilder().setName(NAME).build();
  }

  static WatchResourceChangesRequest watchRequest(long after) {
    return WatchResourceChangesRequest.newBuilder()
        .setAfterCursor(after)
        .setOrganization(NAME.getOrganization())
        .setKind(NAME.getKind())
        .build();
  }

  static ManagementClient client(String... endpoints) throws Exception {
    return new ManagementClient(
        new ManagementConfig(List.of(endpoints), "test-bearer", Duration.ofSeconds(3), null, true));
  }

  static final class RunningServer implements AutoCloseable {
    final Server server;

    RunningServer(Fixture service) throws Exception {
      server =
          NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
              .addService(service)
              .build()
              .start();
    }

    String endpoint() {
      return "127.0.0.1:" + server.getPort();
    }

    @Override
    public void close() {
      server.shutdownNow();
      try {
        assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        throw new AssertionError(error);
      }
    }
  }

  static class Fixture extends RegionalAdminServiceGrpc.RegionalAdminServiceImplBase {
    final List<Object> requests = Collections.synchronizedList(new ArrayList<>());
    volatile Status failure;
    volatile Resource resource = RESOURCE;

    <T> void reply(Object request, T response, StreamObserver<T> observer) {
      requests.add(request);
      if (failure != null) {
        observer.onError(failure.asRuntimeException());
      } else {
        observer.onNext(response);
        observer.onCompleted();
      }
    }

    @Override
    public void applyResource(
        ApplyResourceRequest request, StreamObserver<ApplyResourceResponse> observer) {
      reply(request, ApplyResourceResponse.newBuilder().setResource(resource).build(), observer);
    }

    @Override
    public void getResource(
        GetResourceRequest request, StreamObserver<GetResourceResponse> observer) {
      reply(request, GetResourceResponse.newBuilder().setResource(resource).build(), observer);
    }

    @Override
    public void listResources(
        ListResourcesRequest request, StreamObserver<ListResourcesResponse> observer) {
      reply(request, ListResourcesResponse.newBuilder().addResources(resource).build(), observer);
    }

    @Override
    public void deleteResource(
        DeleteResourceRequest request, StreamObserver<DeleteResourceResponse> observer) {
      reply(
          request,
          DeleteResourceResponse.newBuilder()
              .setName(request.getName())
              .setGeneration(-1L)
              .setDeleted(true)
              .build(),
          observer);
    }

    @Override
    public void batchApplyResources(
        BatchApplyResourcesRequest request, StreamObserver<BatchApplyResourcesResponse> observer) {
      BatchApplyResourcesResponse.Builder response = BatchApplyResourcesResponse.newBuilder();
      for (BatchApplyResource item : request.getResourcesList()) {
        response.addResults(
            ApplyResourceResponse.newBuilder()
                .setResource(resource.toBuilder().setName(item.getName())));
      }
      reply(request, response.build(), observer);
    }

    @Override
    public void getOperation(
        GetOperationRequest request, StreamObserver<GetOperationResponse> observer) {
      reply(
          request,
          GetOperationResponse.newBuilder()
              .setRequestToken(request.getRequestToken())
              .setProposalId(-1L)
              .setState(OperationState.OPERATION_STATE_SUCCEEDED)
              .setCommandKind("delete_desired")
              .setExpectedGeneration(0)
              .addAllAffectedResources(request.getAffectedResourcesList())
              .build(),
          observer);
    }

    @Override
    public void watchResourceChanges(
        WatchResourceChangesRequest request,
        StreamObserver<WatchResourceChangesResponse> observer) {
      requests.add(request);
      observer.onNext(
          WatchResourceChangesResponse.newBuilder()
              .setEarliestCursor(1)
              .setLatestCursor(3)
              .setNextCursor(2)
              .addChanges(
                  ResourceChange.newBuilder()
                      .setCursor(1)
                      .setGeneration(-1L)
                      .setName(NAME)
                      .setKind(ResourceChangeKind.RESOURCE_CHANGE_KIND_DESIRED_APPLIED))
              .build());
      observer.onCompleted();
    }
  }
}

package io.epoch.sdk;

import static io.epoch.sdk.ManagementClientTest.NAME;
import static io.epoch.sdk.ManagementClientTest.RESOURCE;
import static io.epoch.sdk.ManagementClientTest.SPEC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.epoch.sdk.ManagementClientTest.Fixture;
import io.epoch.sdk.ManagementClientTest.RunningServer;
import io.epoch.sdk.gen.epoch.v1.ApplyResourceRequest;
import io.epoch.sdk.gen.epoch.v1.ApplyResourceResponse;
import io.epoch.sdk.gen.epoch.v1.BatchApplyResource;
import io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesRequest;
import io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesResponse;
import io.epoch.sdk.gen.epoch.v1.DataClassification;
import io.epoch.sdk.gen.epoch.v1.DeleteResourceRequest;
import io.epoch.sdk.gen.epoch.v1.GetOperationRequest;
import io.epoch.sdk.gen.epoch.v1.GetOperationResponse;
import io.epoch.sdk.gen.epoch.v1.ListResourcesRequest;
import io.epoch.sdk.gen.epoch.v1.ListResourcesResponse;
import io.epoch.sdk.gen.epoch.v1.OperationState;
import io.epoch.sdk.gen.epoch.v1.ResourceChange;
import io.epoch.sdk.gen.epoch.v1.ResourceChangeKind;
import io.epoch.sdk.gen.epoch.v1.ResourceGovernance;
import io.epoch.sdk.gen.epoch.v1.WatchResourceChangesRequest;
import io.epoch.sdk.gen.epoch.v1.WatchResourceChangesResponse;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

final class ManagementSafetyTest {
  @Test
  void roundTripsTheSharedOmittedZeroAndMaximumGenerationWireVectors() throws Exception {
    var fixture =
        new ObjectMapper()
            .readTree(Path.of("../../spec/fixtures/sdk-management-wire-v1.json").toFile());
    var name = fixture.get("name");
    var identity =
        NAME.toBuilder()
            .setOrganization(name.get("organization").textValue())
            .setProject(name.get("project").textValue())
            .setEnvironment(name.get("environment").textValue())
            .setNamespace(name.get("namespace").textValue())
            .setKindValue(name.get("kind").intValue())
            .setName(name.get("name").textValue())
            .build();
    for (var vector : fixture.get("vectors")) {
      var request =
          DeleteResourceRequest.newBuilder()
              .setName(identity)
              .setRequestToken(fixture.get("request_token").textValue());
      if (vector.get("expected_presence").booleanValue()) {
        request.setExpectedGeneration(
            vector.get("expected_generation").bigIntegerValue().longValue());
      }
      byte[] encoded = request.build().toByteArray();
      assertEquals(vector.get("protobuf_hex").textValue(), HexFormat.of().formatHex(encoded));
      assertEquals(request.build(), DeleteResourceRequest.parseFrom(encoded));
      assertEquals(
          vector.get("expected_presence").booleanValue(),
          DeleteResourceRequest.parseFrom(encoded).hasExpectedGeneration());
    }
  }

  @Test
  void metadataIsFrozenAndCallerAuthorizationIsReplacedExactlyOnce() throws Exception {
    Metadata.Key<String> auth = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<String> trace = Metadata.Key.of("traceparent", Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<byte[]> binary = Metadata.Key.of("proof-bin", Metadata.BINARY_BYTE_MARSHALLER);
    Metadata source = new Metadata();
    source.put(auth, "caller-private");
    source.put(auth, "caller-duplicate");
    source.put(trace, "original-trace");
    byte[] originalBinary = {1, 2, 3};
    source.put(binary, originalBinary);
    ManagementContext caller = new ManagementContext(Context.ROOT, source);
    source.discardAll(trace);
    originalBinary[0] = 9;
    AtomicReference<Metadata> observed = new AtomicReference<>();
    var server =
        NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
            .intercept(
                new ServerInterceptor() {
                  @Override
                  public <Q, R> ServerCall.Listener<Q> interceptCall(
                      ServerCall<Q, R> call, Metadata headers, ServerCallHandler<Q, R> next) {
                    observed.set(headers);
                    return next.startCall(call, headers);
                  }
                })
            .addService(new Fixture())
            .build()
            .start();
    try (ManagementClient client = ManagementClientTest.client("127.0.0.1:" + server.getPort())) {
      client.getResource(caller, ManagementClientTest.get());
      assertEquals(
          List.of("Bearer test-bearer"),
          java.util.stream.StreamSupport.stream(observed.get().getAll(auth).spliterator(), false)
              .toList());
      assertEquals("original-trace", observed.get().get(trace));
      assertEquals("010203", HexFormat.of().formatHex(observed.get().get(binary)));
      assertEquals("caller-duplicate", caller.metadata().get(auth));
      assertFalse(caller.toString().contains("caller-private"));
    } finally {
      server.shutdownNow();
      assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void cancellationAfterDispatchUnblocksTheCallerAndRetainsUnknownMutationOutcome()
      throws Exception {
    CountDownLatch dispatched = new CountDownLatch(1);
    Fixture fixture =
        new Fixture() {
          @Override
          public void applyResource(
              ApplyResourceRequest request, StreamObserver<ApplyResourceResponse> observer) {
            dispatched.countDown();
          }
        };
    try (RunningServer server = new RunningServer(fixture);
        ManagementClient client = ManagementClientTest.client(server.endpoint());
        Context.CancellableContext caller = Context.ROOT.withCancellation();
        var executor = Executors.newSingleThreadExecutor()) {
      var result =
          executor.submit(
              () ->
                  assertThrows(
                      ManagementRPCException.class,
                      () ->
                          client.applyResource(
                              new ManagementContext(caller, new Metadata()), apply())));
      assertTrue(dispatched.await(2, TimeUnit.SECONDS));
      caller.cancel(null);
      ManagementRPCException failure = result.get(2, TimeUnit.SECONDS);
      assertEquals(Status.Code.CANCELLED, failure.status().getCode());
      assertEquals(new ManagementCallInfo(1, 1, true), failure.info());
    }
  }

  @Test
  void failoverSharesOneAbsoluteDeadlineAndNeverRetriesDeadlineExceeded() throws Exception {
    AtomicReference<Deadline> firstDeadline = new AtomicReference<>();
    AtomicReference<Deadline> secondDeadline = new AtomicReference<>();
    Fixture first =
        new Fixture() {
          @Override
          public void applyResource(
              ApplyResourceRequest request, StreamObserver<ApplyResourceResponse> observer) {
            firstDeadline.set(Context.current().getDeadline());
            try {
              new CountDownLatch(1).await(125, TimeUnit.MILLISECONDS);
            } catch (InterruptedException error) {
              Thread.currentThread().interrupt();
            }
            observer.onError(Status.UNAVAILABLE.asRuntimeException());
          }
        };
    Fixture second =
        new Fixture() {
          @Override
          public void applyResource(
              ApplyResourceRequest request, StreamObserver<ApplyResourceResponse> observer) {
            secondDeadline.set(Context.current().getDeadline());
          }
        };
    try (RunningServer a = new RunningServer(first);
        RunningServer b = new RunningServer(second);
        ManagementClient client =
            new ManagementClient(
                new ManagementConfig(
                    List.of(a.endpoint(), b.endpoint()),
                    "bearer",
                    Duration.ofMillis(500),
                    null,
                    true))) {
      var failure =
          assertThrows(
              ManagementRPCException.class,
              () -> client.applyResource(ManagementContext.current(), apply()));
      assertEquals(Status.Code.DEADLINE_EXCEEDED, failure.status().getCode());
      assertEquals(new ManagementCallInfo(2, 2, true), failure.info());
      long delta =
          firstDeadline.get().timeRemaining(TimeUnit.MILLISECONDS)
              - secondDeadline.get().timeRemaining(TimeUnit.MILLISECONDS);
      assertTrue(Math.abs(delta) < 75, "failover widened the original deadline");
    }
  }

  @Test
  void completeListGovernanceFiltersAndPresenceOfEmptyTagValuesAreEnforced() throws Exception {
    Fixture service = new Fixture();
    service.resource =
        RESOURCE.toBuilder()
            .setSpec(
                SPEC.toBuilder()
                    .setGovernance(
                        ResourceGovernance.newBuilder()
                            .setOwner("team")
                            .setCostCenter("cost")
                            .setClassification(DataClassification.DATA_CLASSIFICATION_INTERNAL)
                            .putTags("empty", "")))
            .build();
    var correct =
        ListResourcesRequest.newBuilder()
            .setOrganization(NAME.getOrganization())
            .setProject(NAME.getProject())
            .setEnvironment(NAME.getEnvironment())
            .setNamespace(NAME.getNamespace())
            .setKind(NAME.getKind())
            .setOwner("team")
            .setCostCenter("cost")
            .setClassification(DataClassification.DATA_CLASSIFICATION_INTERNAL)
            .putTags("empty", "");
    try (RunningServer server = new RunningServer(service);
        ManagementClient client = ManagementClientTest.client(server.endpoint())) {
      assertEquals(
          1,
          client
              .listResources(ManagementContext.current(), correct.build())
              .response()
              .getResourcesCount());
      for (ListResourcesRequest request :
          List.of(
              correct.clone().setOrganization("foreign").build(),
              correct.clone().setProject("foreign").build(),
              correct.clone().setEnvironment("foreign").build(),
              correct.clone().setNamespace("foreign").build(),
              correct.clone().setKindValue(2).build(),
              correct.clone().setOwner("foreign").build(),
              correct.clone().setCostCenter("foreign").build(),
              correct.clone().setClassificationValue(1).build(),
              correct.clone().putTags("absent", "").build())) {
        assertDataLoss(() -> client.listResources(ManagementContext.current(), request));
      }
    }
  }

  @Test
  void duplicateOversizedInventoryAndBatchReceiptsFailClosed() {
    var list =
        ListResourcesResponse.newBuilder().addResources(RESOURCE).addResources(RESOURCE).build();
    assertEquals(
        Status.Code.DATA_LOSS,
        Status.fromThrowable(
                assertThrows(
                    RuntimeException.class,
                    () ->
                        ManagementContracts.list(ListResourcesRequest.getDefaultInstance(), list)))
            .getCode());
    var batch =
        BatchApplyResourcesRequest.newBuilder()
            .setRequestToken("batch")
            .addResources(BatchApplyResource.newBuilder().setName(NAME).setSpec(SPEC))
            .build();
    for (var response :
        List.of(
            BatchApplyResourcesResponse.getDefaultInstance(),
            BatchApplyResourcesResponse.newBuilder()
                .setReplayed(true)
                .addResults(ApplyResourceResponse.newBuilder().setResource(RESOURCE))
                .build(),
            BatchApplyResourcesResponse.newBuilder()
                .addResults(
                    ApplyResourceResponse.newBuilder()
                        .setResource(RESOURCE.toBuilder().setGeneration(0)))
                .build())) {
      assertThrows(RuntimeException.class, () -> ManagementContracts.batch(batch, response));
    }
    assertThrows(
        RuntimeException.class,
        () ->
            ManagementContracts.list(
                ListResourcesRequest.newBuilder().setPageSize(1).build(), list));
  }

  @Test
  void exactOperationScopesOutcomesAndUnsignedCursorRangesAreRequired() {
    var request =
        GetOperationRequest.newBuilder()
            .setRequestToken("original")
            .addAffectedResources(NAME)
            .build();
    var valid =
        GetOperationResponse.newBuilder()
            .setRequestToken("original")
            .setProposalId(-1L)
            .setCommandKind("apply_desired")
            .setState(OperationState.OPERATION_STATE_SUCCEEDED)
            .addAffectedResources(NAME)
            .setFirstChangeCursor(Long.MIN_VALUE)
            .setLastChangeCursor(-1L);
    ManagementContracts.operation(request, valid.build());
    for (var response :
        List.of(
            valid.clone().setRequestToken("foreign").build(),
            valid.clone().setProposalId(0).build(),
            valid.clone().setStateValue(99).build(),
            valid.clone().setCommandKind("").build(),
            valid.clone().clearAffectedResources().build(),
            valid.clone().addAffectedResources(NAME).build(),
            valid.clone().setFirstChangeCursor(-1L).setLastChangeCursor(Long.MIN_VALUE).build(),
            valid.clone().setFirstChangeCursor(0).build(),
            valid.clone().setState(OperationState.OPERATION_STATE_FAILED).build(),
            valid.clone().setFailureMessage("unexpected").build())) {
      assertThrows(RuntimeException.class, () -> ManagementContracts.operation(request, response));
    }
  }

  @Test
  void watchResumesOnlyFromTheAcknowledgedScannedCursorWithOriginalFilters() throws Exception {
    Fixture first = watchFixture(page(13, 20), Status.UNAVAILABLE);
    Fixture second = watchFixture(page(-1L, -1L), Status.OK);
    var filter =
        ManagementClientTest.watchRequest(10).toBuilder()
            .setProject(NAME.getProject())
            .setEnvironment(NAME.getEnvironment())
            .setNamespace(NAME.getNamespace())
            .setBatchSize(2)
            .build();
    try (RunningServer a = new RunningServer(first);
        RunningServer b = new RunningServer(second);
        ManagementClient client = ManagementClientTest.client(a.endpoint(), b.endpoint());
        ManagementWatch watch = client.watchResourceChanges(ManagementContext.current(), filter)) {
      assertEquals(13, watch.receive().getNextCursor());
      assertEquals(10, watch.checkpoint());
      watch.acknowledge(13);
      assertEquals(-1L, watch.receive().getNextCursor());
      assertEquals(filter.toBuilder().setAfterCursor(13).build(), second.requests.getFirst());
      watch.acknowledge(-1L);
      assertEquals(-1L, watch.checkpoint());
      var exhausted = assertThrows(ManagementRPCException.class, watch::receive);
      assertEquals(Status.Code.UNAVAILABLE, exhausted.status().getCode());
      assertEquals(new ManagementCallInfo(2, 2, false), exhausted.info());
    }
  }

  @Test
  void staleOrMalformedWatchNeverReconnectsOrAdvancesItsCheckpoint() throws Exception {
    for (Status status : List.of(Status.ABORTED, Status.PERMISSION_DENIED)) {
      Fixture first = watchFixture(null, status);
      Fixture second = new Fixture();
      try (RunningServer a = new RunningServer(first);
          RunningServer b = new RunningServer(second);
          ManagementClient client = ManagementClientTest.client(a.endpoint(), b.endpoint());
          ManagementWatch watch =
              client.watchResourceChanges(
                  ManagementContext.current(), ManagementClientTest.watchRequest(10))) {
        var failure = assertThrows(ManagementRPCException.class, watch::receive);
        assertEquals(status.getCode(), failure.status().getCode());
        assertEquals(10, watch.checkpoint());
        assertTrue(second.requests.isEmpty());
      }
    }
    for (var invalid :
        List.of(
            page(13, 20).toBuilder().setEarliestCursor(12).build(),
            page(9, 20),
            page(21, 20),
            page(13, 20).toBuilder().setEarliestCursor(0).build(),
            page(13, 20).toBuilder()
                .addChanges(
                    ResourceChange.newBuilder()
                        .setCursor(11)
                        .setName(NAME)
                        .setGeneration(0)
                        .setKindValue(1))
                .build())) {
      Fixture fixture = watchFixture(invalid, Status.OK);
      try (RunningServer server = new RunningServer(fixture);
          ManagementClient client = ManagementClientTest.client(server.endpoint());
          ManagementWatch watch =
              client.watchResourceChanges(
                  ManagementContext.current(), ManagementClientTest.watchRequest(10))) {
        var failure = assertThrows(ManagementRPCException.class, watch::receive);
        assertEquals(Status.Code.DATA_LOSS, failure.status().getCode());
        assertEquals(10, watch.checkpoint());
      }
    }
  }

  @Test
  void maximumUnsignedWatchPagesAndEmptyCatalogAreValid() {
    ManagementContracts.watch(
        ManagementClientTest.watchRequest(Long.MAX_VALUE),
        Long.MAX_VALUE,
        page(-1L, -1L).toBuilder()
            .addChanges(
                ResourceChange.newBuilder()
                    .setCursor(Long.MIN_VALUE)
                    .setGeneration(-1L)
                    .setName(NAME)
                    .setKind(ResourceChangeKind.RESOURCE_CHANGE_KIND_DESIRED_APPLIED))
            .build());
    ManagementContracts.watch(
        ManagementClientTest.watchRequest(0),
        0,
        WatchResourceChangesResponse.newBuilder().setEarliestCursor(1).build());
  }

  @Test
  void watchPersistsBeyondUnaryTimeoutAndCloseUnblocksRemoteWithoutCancelingParent()
      throws Exception {
    CountDownLatch opened = new CountDownLatch(1);
    CountDownLatch remoteClosed = new CountDownLatch(1);
    Fixture fixture =
        new Fixture() {
          @Override
          public void watchResourceChanges(
              WatchResourceChangesRequest request,
              StreamObserver<WatchResourceChangesResponse> observer) {
            ((ServerCallStreamObserver<WatchResourceChangesResponse>) observer)
                .setOnCancelHandler(remoteClosed::countDown);
            opened.countDown();
          }
        };
    try (RunningServer server = new RunningServer(fixture);
        ManagementClient client =
            new ManagementClient(
                new ManagementConfig(
                    List.of(server.endpoint()), "bearer", Duration.ofMillis(25), null, true));
        Context.CancellableContext parent = Context.ROOT.withCancellation();
        ManagementWatch watch =
            client.watchResourceChanges(
                new ManagementContext(parent, new Metadata()),
                ManagementClientTest.watchRequest(0));
        var executor = Executors.newSingleThreadExecutor()) {
      var result =
          executor.submit(() -> assertThrows(ManagementRPCException.class, watch::receive));
      assertTrue(opened.await(2, TimeUnit.SECONDS));
      assertFalse(remoteClosed.await(100, TimeUnit.MILLISECONDS));
      assertFalse(result.isDone());
      closeWatch(watch);
      assertEquals(Status.Code.CANCELLED, result.get(2, TimeUnit.SECONDS).status().getCode());
      assertTrue(remoteClosed.await(2, TimeUnit.SECONDS));
      assertFalse(parent.isCancelled());
    }
  }

  @Test
  void printedAndSerializedErrorsDoNotLeakBackendTextOrTrailers() throws Exception {
    Metadata metadata = new Metadata();
    metadata.put(Metadata.Key.of("private", Metadata.ASCII_STRING_MARSHALLER), "private-payload");
    var error =
        new ManagementRPCException(
            Status.ABORTED.withDescription("private-payload"),
            metadata,
            new IllegalStateException("private-payload"),
            new ManagementCallInfo(1, 2, true));
    StringWriter output = new StringWriter();
    error.printStackTrace(new PrintWriter(output));
    assertFalse(output.toString().contains("private-payload"));
    assertNull(error.getCause());
    assertEquals("private-payload", error.rpcCause().getMessage());
    try (ObjectOutputStream stream = new ObjectOutputStream(new ByteArrayOutputStream())) {
      assertThrows(NotSerializableException.class, () -> stream.writeObject(error));
    }
  }

  private static ApplyResourceRequest apply() {
    return ApplyResourceRequest.newBuilder()
        .setRequestToken("original")
        .setName(NAME)
        .setSpec(SPEC)
        .setExpectedGeneration(0)
        .build();
  }

  private static void closeWatch(ManagementWatch watch) {
    watch.close();
  }

  private static WatchResourceChangesResponse page(long next, long latest) {
    return WatchResourceChangesResponse.newBuilder()
        .setEarliestCursor(1)
        .setLatestCursor(latest)
        .setNextCursor(next)
        .build();
  }

  private static Fixture watchFixture(WatchResourceChangesResponse page, Status status) {
    return new Fixture() {
      @Override
      public void watchResourceChanges(
          WatchResourceChangesRequest request,
          StreamObserver<WatchResourceChangesResponse> observer) {
        requests.add(request);
        if (page != null) {
          observer.onNext(page);
        }
        if (status.isOk()) {
          observer.onCompleted();
        } else {
          observer.onError(status.asRuntimeException());
        }
      }
    };
  }

  private static void assertDataLoss(org.junit.jupiter.api.function.Executable call) {
    assertEquals(
        Status.Code.DATA_LOSS, assertThrows(ManagementRPCException.class, call).status().getCode());
  }
}

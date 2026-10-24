package io.epoch.sdk;

import com.google.protobuf.MessageLite;
import io.epoch.sdk.gen.epoch.v1.ApplyResourceRequest;
import io.epoch.sdk.gen.epoch.v1.ApplyResourceResponse;
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
import io.epoch.sdk.gen.epoch.v1.RegionalAdminServiceGrpc;
import io.epoch.sdk.gen.epoch.v1.WatchResourceChangesRequest;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.grpc.stub.MetadataUtils;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import javax.net.ssl.KeyManagerFactory;

/** Thread-safe generated management client. Only UNAVAILABLE advances the explicit allowlist. */
public final class ManagementClient implements AutoCloseable {
  static final int MAX_MESSAGE_BYTES = 5 << 20;
  private static final Metadata.Key<String> AUTHORIZATION =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
  private final List<ManagedChannel> channels = new ArrayList<>();
  private final String token;
  private final long timeoutNanos;
  private final AtomicBoolean closed = new AtomicBoolean();

  public ManagementClient(ManagementConfig config) throws IOException {
    ManagementContracts.require(
        config != null
            && !config.endpoints().isEmpty()
            && !config.timeout().isNegative()
            && !config.timeout().isZero());
    long duration;
    try {
      duration = config.timeout().toNanos();
    } catch (ArithmeticException error) {
      throw ManagementRPCException.local(Status.INVALID_ARGUMENT);
    }
    ManagementContracts.require(
        validBearer(config.bearerToken())
            && ((config.tls() != null) != config.allowInsecureLoopback()));
    Set<String> seen = new HashSet<>();
    for (String endpoint : config.endpoints()) {
      validateEndpoint(endpoint, config.allowInsecureLoopback(), seen);
    }
    SslContext trust = config.tls() == null ? null : loadTls(config.tls());
    this.timeoutNanos = duration;
    this.token = config.bearerToken();
    try {
      for (String endpoint : config.endpoints()) {
        NettyChannelBuilder builder =
            NettyChannelBuilder.forTarget("dns:///" + endpoint)
                .disableRetry()
                .disableServiceConfigLookUp()
                .proxyDetector(address -> null)
                .maxInboundMessageSize(MAX_MESSAGE_BYTES)
                .userAgent(HttpTransport.USER_AGENT);
        if (trust == null) {
          builder.usePlaintext();
        } else {
          builder.sslContext(trust);
        }
        channels.add(builder.build());
      }
    } catch (RuntimeException error) {
      close();
      throw error;
    }
  }

  private static boolean validBearer(String value) {
    return !value.isEmpty()
        && value.length() <= 4096
        && value.chars().allMatch(character -> character >= 33 && character <= 126);
  }

  private static void validateEndpoint(String endpoint, boolean loopback, Set<String> seen)
      throws IOException {
    ManagementContracts.require(
        !endpoint.isEmpty()
            && endpoint
                .chars()
                .noneMatch(
                    character ->
                        character <= 32 || character >= 127 || "/\\@?#".indexOf(character) >= 0));
    int separator = endpoint.lastIndexOf(':');
    ManagementContracts.require(separator > 0 && separator + 1 < endpoint.length());
    String host = endpoint.substring(0, separator);
    String port = endpoint.substring(separator + 1);
    int number;
    try {
      number = Integer.parseInt(port);
    } catch (NumberFormatException error) {
      throw ManagementRPCException.local(Status.INVALID_ARGUMENT);
    }
    ManagementContracts.require(
        number >= 1 && number <= 65535 && Integer.toString(number).equals(port));
    boolean literalV6 = host.startsWith("[") && host.endsWith("]");
    String bareHost = literalV6 ? host.substring(1, host.length() - 1) : host;
    if (literalV6) {
      ManagementContracts.require(
          bareHost.contains(":") && !bareHost.contains("%") && bareHost.matches("[0-9a-fA-F:.]+"));
      try {
        ManagementContracts.require(InetAddress.getByName(bareHost).getAddress().length == 16);
      } catch (IOException error) {
        throw ManagementRPCException.local(Status.INVALID_ARGUMENT);
      }
    } else {
      ManagementContracts.require(host.matches("[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?"));
    }
    ManagementContracts.require(seen.add(endpoint.toLowerCase(Locale.ROOT)));
    if (loopback) {
      boolean allowed = "localhost".equals(bareHost);
      if (literalV6 || bareHost.matches("[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+")) {
        try {
          allowed = InetAddress.getByName(bareHost).isLoopbackAddress();
        } catch (IOException error) {
          allowed = false;
        }
      }
      ManagementContracts.require(allowed);
    }
  }

  private static SslContext loadTls(TlsConfig config) throws IOException {
    SslContextBuilder builder =
        GrpcSslContexts.configure(SslContextBuilder.forClient(), SslProvider.JDK)
            .protocols("TLSv1.3")
            .trustManager(config.trustManagerFactory());
    KeyManagerFactory identity = config.keyManagerFactory();
    if (identity != null) {
      builder.keyManager(identity);
    }
    return builder.build();
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      channels.forEach(ManagedChannel::shutdownNow);
    }
  }

  void usable() {
    if (closed.get()) {
      throw Status.UNAVAILABLE.asRuntimeException();
    }
  }

  int budget() {
    return channels.size();
  }

  ManagedChannel channel(int index) {
    return channels.get(index);
  }

  Metadata authenticated(ManagementContext caller) {
    Metadata metadata = caller.metadata();
    metadata.discardAll(AUTHORIZATION);
    metadata.put(AUTHORIZATION, "Bearer " + token);
    return metadata;
  }

  private <Q extends MessageLite, R> ManagementResult<R> unary(
      ManagementContext caller,
      Q request,
      boolean mutation,
      BiFunction<RegionalAdminServiceGrpc.RegionalAdminServiceBlockingStub, Q, R> invoke,
      BiConsumer<Q, R> validate) {
    ManagementContracts.require(caller != null);
    int attempts = 0;
    Deadline deadline = Deadline.after(timeoutNanos, TimeUnit.NANOSECONDS);
    if (caller.context().getDeadline() != null) {
      deadline = deadline.minimum(caller.context().getDeadline());
    }
    Context previous = caller.context().attach();
    try {
      for (ManagedChannel channel : channels) {
        usable();
        if (caller.context().isCancelled()) {
          throw Contexts.statusFromCancelled(caller.context()).asRuntimeException();
        }
        if (deadline.isExpired()) {
          throw Status.DEADLINE_EXCEEDED.asRuntimeException();
        }
        ManagementContracts.require(request.getSerializedSize() <= MAX_MESSAGE_BYTES);
        attempts++;
        try {
          var stub =
              RegionalAdminServiceGrpc.newBlockingStub(channel)
                  .withDeadline(deadline)
                  .withInterceptors(
                      MetadataUtils.newAttachHeadersInterceptor(authenticated(caller)));
          R response = invoke.apply(stub, request);
          validate.accept(request, response);
          return new ManagementResult<>(
              response, new ManagementCallInfo(attempts, budget(), false));
        } catch (RuntimeException error) {
          if (Status.fromThrowable(error).getCode() != Status.Code.UNAVAILABLE
              || attempts == budget()) {
            throw error;
          }
        }
      }
      throw Status.UNAVAILABLE.asRuntimeException();
    } catch (RuntimeException error) {
      if (error instanceof ManagementRPCException failure && attempts == 0) {
        throw failure;
      }
      throw new ManagementRPCException(
          Status.fromThrowable(error),
          Status.trailersFromThrowable(error),
          error,
          new ManagementCallInfo(attempts, budget(), mutation && attempts > 0));
    } finally {
      caller.context().detach(previous);
    }
  }

  /** Accept desired state; the receipt does not imply physical readiness. */
  public ManagementResult<ApplyResourceResponse> applyResource(
      ManagementContext context, ApplyResourceRequest request) {
    ManagementContracts.require(request != null && request.hasSpec());
    ManagementContracts.token(request.getRequestToken());
    ManagementContracts.name(request.getName());
    return unary(
        context,
        request,
        true,
        RegionalAdminServiceGrpc.RegionalAdminServiceBlockingStub::applyResource,
        (original, response) -> ManagementContracts.apply(original.getName(), response));
  }

  public ManagementResult<GetResourceResponse> getResource(
      ManagementContext context, GetResourceRequest request) {
    ManagementContracts.require(request != null);
    ManagementContracts.name(request.getName());
    return unary(
        context,
        request,
        false,
        RegionalAdminServiceGrpc.RegionalAdminServiceBlockingStub::getResource,
        (original, response) -> {
          ManagementContracts.receipt(response.hasResource());
          ManagementContracts.resource(response.getResource());
          ManagementContracts.receipt(original.getName().equals(response.getResource().getName()));
        });
  }

  /** Bounded inventory. The current server rejects nonempty page tokens. */
  public ManagementResult<ListResourcesResponse> listResources(
      ManagementContext context, ListResourcesRequest request) {
    ManagementContracts.listRequest(request);
    return unary(
        context,
        request,
        false,
        RegionalAdminServiceGrpc.RegionalAdminServiceBlockingStub::listResources,
        ManagementContracts::list);
  }

  public ManagementResult<DeleteResourceResponse> deleteResource(
      ManagementContext context, DeleteResourceRequest request) {
    ManagementContracts.require(request != null);
    ManagementContracts.token(request.getRequestToken());
    ManagementContracts.name(request.getName());
    return unary(
        context,
        request,
        true,
        RegionalAdminServiceGrpc.RegionalAdminServiceBlockingStub::deleteResource,
        ManagementContracts::delete);
  }

  /** Atomic desired-state acceptance, not atomic physical materialization. */
  public ManagementResult<BatchApplyResourcesResponse> batchApplyResources(
      ManagementContext context, BatchApplyResourcesRequest request) {
    ManagementContracts.require(request != null);
    ManagementContracts.token(request.getRequestToken());
    ManagementContracts.names(
        request.getResourcesList().stream().map(item -> item.getName()).toList());
    request.getResourcesList().forEach(item -> ManagementContracts.require(item.hasSpec()));
    return unary(
        context,
        request,
        true,
        RegionalAdminServiceGrpc.RegionalAdminServiceBlockingStub::batchApplyResources,
        ManagementContracts::batch);
  }

  /**
   * Resolve the original token for its exact authorized resource set. NotFound is not non-commit.
   */
  public ManagementResult<GetOperationResponse> getOperation(
      ManagementContext context, GetOperationRequest request) {
    ManagementContracts.require(request != null);
    ManagementContracts.token(request.getRequestToken());
    ManagementContracts.names(request.getAffectedResourcesList());
    return unary(
        context,
        request,
        false,
        RegionalAdminServiceGrpc.RegionalAdminServiceBlockingStub::getOperation,
        ManagementContracts::operation);
  }

  /** Lazy, single-consumer watch. Unary timeout never caps the persistent stream. */
  public ManagementWatch watchResourceChanges(
      ManagementContext context, WatchResourceChangesRequest request) {
    ManagementContracts.require(context != null);
    ManagementContracts.watchRequest(request);
    try {
      usable();
    } catch (RuntimeException error) {
      throw ManagementRPCException.local(Status.fromThrowable(error));
    }
    return new ManagementWatch(this, context, request);
  }
}

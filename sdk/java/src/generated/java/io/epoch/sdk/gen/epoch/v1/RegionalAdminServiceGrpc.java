package io.epoch.sdk.gen.epoch.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * RegionalAdmin is the versioned boundary between managed Go reconciliation
 * and a regional Rust control surface. It carries desired metadata only.
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class RegionalAdminServiceGrpc {

  private RegionalAdminServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "epoch.v1.RegionalAdminService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.ApplyResourceRequest,
      io.epoch.sdk.gen.epoch.v1.ApplyResourceResponse> getApplyResourceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ApplyResource",
      requestType = io.epoch.sdk.gen.epoch.v1.ApplyResourceRequest.class,
      responseType = io.epoch.sdk.gen.epoch.v1.ApplyResourceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.ApplyResourceRequest,
      io.epoch.sdk.gen.epoch.v1.ApplyResourceResponse> getApplyResourceMethod() {
    io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.ApplyResourceRequest, io.epoch.sdk.gen.epoch.v1.ApplyResourceResponse> getApplyResourceMethod;
    if ((getApplyResourceMethod = RegionalAdminServiceGrpc.getApplyResourceMethod) == null) {
      synchronized (RegionalAdminServiceGrpc.class) {
        if ((getApplyResourceMethod = RegionalAdminServiceGrpc.getApplyResourceMethod) == null) {
          RegionalAdminServiceGrpc.getApplyResourceMethod = getApplyResourceMethod =
              io.grpc.MethodDescriptor.<io.epoch.sdk.gen.epoch.v1.ApplyResourceRequest, io.epoch.sdk.gen.epoch.v1.ApplyResourceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ApplyResource"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.epoch.sdk.gen.epoch.v1.ApplyResourceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.epoch.sdk.gen.epoch.v1.ApplyResourceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RegionalAdminServiceMethodDescriptorSupplier("ApplyResource"))
              .build();
        }
      }
    }
    return getApplyResourceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.GetResourceRequest,
      io.epoch.sdk.gen.epoch.v1.GetResourceResponse> getGetResourceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetResource",
      requestType = io.epoch.sdk.gen.epoch.v1.GetResourceRequest.class,
      responseType = io.epoch.sdk.gen.epoch.v1.GetResourceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.GetResourceRequest,
      io.epoch.sdk.gen.epoch.v1.GetResourceResponse> getGetResourceMethod() {
    io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.GetResourceRequest, io.epoch.sdk.gen.epoch.v1.GetResourceResponse> getGetResourceMethod;
    if ((getGetResourceMethod = RegionalAdminServiceGrpc.getGetResourceMethod) == null) {
      synchronized (RegionalAdminServiceGrpc.class) {
        if ((getGetResourceMethod = RegionalAdminServiceGrpc.getGetResourceMethod) == null) {
          RegionalAdminServiceGrpc.getGetResourceMethod = getGetResourceMethod =
              io.grpc.MethodDescriptor.<io.epoch.sdk.gen.epoch.v1.GetResourceRequest, io.epoch.sdk.gen.epoch.v1.GetResourceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetResource"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.epoch.sdk.gen.epoch.v1.GetResourceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.epoch.sdk.gen.epoch.v1.GetResourceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RegionalAdminServiceMethodDescriptorSupplier("GetResource"))
              .build();
        }
      }
    }
    return getGetResourceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.ListResourcesRequest,
      io.epoch.sdk.gen.epoch.v1.ListResourcesResponse> getListResourcesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListResources",
      requestType = io.epoch.sdk.gen.epoch.v1.ListResourcesRequest.class,
      responseType = io.epoch.sdk.gen.epoch.v1.ListResourcesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.ListResourcesRequest,
      io.epoch.sdk.gen.epoch.v1.ListResourcesResponse> getListResourcesMethod() {
    io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.ListResourcesRequest, io.epoch.sdk.gen.epoch.v1.ListResourcesResponse> getListResourcesMethod;
    if ((getListResourcesMethod = RegionalAdminServiceGrpc.getListResourcesMethod) == null) {
      synchronized (RegionalAdminServiceGrpc.class) {
        if ((getListResourcesMethod = RegionalAdminServiceGrpc.getListResourcesMethod) == null) {
          RegionalAdminServiceGrpc.getListResourcesMethod = getListResourcesMethod =
              io.grpc.MethodDescriptor.<io.epoch.sdk.gen.epoch.v1.ListResourcesRequest, io.epoch.sdk.gen.epoch.v1.ListResourcesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListResources"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.epoch.sdk.gen.epoch.v1.ListResourcesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.epoch.sdk.gen.epoch.v1.ListResourcesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RegionalAdminServiceMethodDescriptorSupplier("ListResources"))
              .build();
        }
      }
    }
    return getListResourcesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.DeleteResourceRequest,
      io.epoch.sdk.gen.epoch.v1.DeleteResourceResponse> getDeleteResourceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteResource",
      requestType = io.epoch.sdk.gen.epoch.v1.DeleteResourceRequest.class,
      responseType = io.epoch.sdk.gen.epoch.v1.DeleteResourceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.DeleteResourceRequest,
      io.epoch.sdk.gen.epoch.v1.DeleteResourceResponse> getDeleteResourceMethod() {
    io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.DeleteResourceRequest, io.epoch.sdk.gen.epoch.v1.DeleteResourceResponse> getDeleteResourceMethod;
    if ((getDeleteResourceMethod = RegionalAdminServiceGrpc.getDeleteResourceMethod) == null) {
      synchronized (RegionalAdminServiceGrpc.class) {
        if ((getDeleteResourceMethod = RegionalAdminServiceGrpc.getDeleteResourceMethod) == null) {
          RegionalAdminServiceGrpc.getDeleteResourceMethod = getDeleteResourceMethod =
              io.grpc.MethodDescriptor.<io.epoch.sdk.gen.epoch.v1.DeleteResourceRequest, io.epoch.sdk.gen.epoch.v1.DeleteResourceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteResource"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.epoch.sdk.gen.epoch.v1.DeleteResourceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.epoch.sdk.gen.epoch.v1.DeleteResourceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RegionalAdminServiceMethodDescriptorSupplier("DeleteResource"))
              .build();
        }
      }
    }
    return getDeleteResourceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesRequest,
      io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesResponse> getBatchApplyResourcesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "BatchApplyResources",
      requestType = io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesRequest.class,
      responseType = io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesRequest,
      io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesResponse> getBatchApplyResourcesMethod() {
    io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesRequest, io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesResponse> getBatchApplyResourcesMethod;
    if ((getBatchApplyResourcesMethod = RegionalAdminServiceGrpc.getBatchApplyResourcesMethod) == null) {
      synchronized (RegionalAdminServiceGrpc.class) {
        if ((getBatchApplyResourcesMethod = RegionalAdminServiceGrpc.getBatchApplyResourcesMethod) == null) {
          RegionalAdminServiceGrpc.getBatchApplyResourcesMethod = getBatchApplyResourcesMethod =
              io.grpc.MethodDescriptor.<io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesRequest, io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "BatchApplyResources"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RegionalAdminServiceMethodDescriptorSupplier("BatchApplyResources"))
              .build();
        }
      }
    }
    return getBatchApplyResourcesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.GetOperationRequest,
      io.epoch.sdk.gen.epoch.v1.GetOperationResponse> getGetOperationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetOperation",
      requestType = io.epoch.sdk.gen.epoch.v1.GetOperationRequest.class,
      responseType = io.epoch.sdk.gen.epoch.v1.GetOperationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.GetOperationRequest,
      io.epoch.sdk.gen.epoch.v1.GetOperationResponse> getGetOperationMethod() {
    io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.GetOperationRequest, io.epoch.sdk.gen.epoch.v1.GetOperationResponse> getGetOperationMethod;
    if ((getGetOperationMethod = RegionalAdminServiceGrpc.getGetOperationMethod) == null) {
      synchronized (RegionalAdminServiceGrpc.class) {
        if ((getGetOperationMethod = RegionalAdminServiceGrpc.getGetOperationMethod) == null) {
          RegionalAdminServiceGrpc.getGetOperationMethod = getGetOperationMethod =
              io.grpc.MethodDescriptor.<io.epoch.sdk.gen.epoch.v1.GetOperationRequest, io.epoch.sdk.gen.epoch.v1.GetOperationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetOperation"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.epoch.sdk.gen.epoch.v1.GetOperationRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.epoch.sdk.gen.epoch.v1.GetOperationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RegionalAdminServiceMethodDescriptorSupplier("GetOperation"))
              .build();
        }
      }
    }
    return getGetOperationMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.WatchResourceChangesRequest,
      io.epoch.sdk.gen.epoch.v1.WatchResourceChangesResponse> getWatchResourceChangesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "WatchResourceChanges",
      requestType = io.epoch.sdk.gen.epoch.v1.WatchResourceChangesRequest.class,
      responseType = io.epoch.sdk.gen.epoch.v1.WatchResourceChangesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.WatchResourceChangesRequest,
      io.epoch.sdk.gen.epoch.v1.WatchResourceChangesResponse> getWatchResourceChangesMethod() {
    io.grpc.MethodDescriptor<io.epoch.sdk.gen.epoch.v1.WatchResourceChangesRequest, io.epoch.sdk.gen.epoch.v1.WatchResourceChangesResponse> getWatchResourceChangesMethod;
    if ((getWatchResourceChangesMethod = RegionalAdminServiceGrpc.getWatchResourceChangesMethod) == null) {
      synchronized (RegionalAdminServiceGrpc.class) {
        if ((getWatchResourceChangesMethod = RegionalAdminServiceGrpc.getWatchResourceChangesMethod) == null) {
          RegionalAdminServiceGrpc.getWatchResourceChangesMethod = getWatchResourceChangesMethod =
              io.grpc.MethodDescriptor.<io.epoch.sdk.gen.epoch.v1.WatchResourceChangesRequest, io.epoch.sdk.gen.epoch.v1.WatchResourceChangesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "WatchResourceChanges"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.epoch.sdk.gen.epoch.v1.WatchResourceChangesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.epoch.sdk.gen.epoch.v1.WatchResourceChangesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RegionalAdminServiceMethodDescriptorSupplier("WatchResourceChanges"))
              .build();
        }
      }
    }
    return getWatchResourceChangesMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RegionalAdminServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RegionalAdminServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RegionalAdminServiceStub>() {
        @java.lang.Override
        public RegionalAdminServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RegionalAdminServiceStub(channel, callOptions);
        }
      };
    return RegionalAdminServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static RegionalAdminServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RegionalAdminServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RegionalAdminServiceBlockingV2Stub>() {
        @java.lang.Override
        public RegionalAdminServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RegionalAdminServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return RegionalAdminServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RegionalAdminServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RegionalAdminServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RegionalAdminServiceBlockingStub>() {
        @java.lang.Override
        public RegionalAdminServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RegionalAdminServiceBlockingStub(channel, callOptions);
        }
      };
    return RegionalAdminServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static RegionalAdminServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RegionalAdminServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RegionalAdminServiceFutureStub>() {
        @java.lang.Override
        public RegionalAdminServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RegionalAdminServiceFutureStub(channel, callOptions);
        }
      };
    return RegionalAdminServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * RegionalAdmin is the versioned boundary between managed Go reconciliation
   * and a regional Rust control surface. It carries desired metadata only.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * ApplyResource idempotently creates or updates desired resource state.
     * </pre>
     */
    default void applyResource(io.epoch.sdk.gen.epoch.v1.ApplyResourceRequest request,
        io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.ApplyResourceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getApplyResourceMethod(), responseObserver);
    }

    /**
     * <pre>
     * GetResource returns one resource by fully qualified name.
     * </pre>
     */
    default void getResource(io.epoch.sdk.gen.epoch.v1.GetResourceRequest request,
        io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.GetResourceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetResourceMethod(), responseObserver);
    }

    /**
     * <pre>
     * ListResources returns a stable page of resources.
     * </pre>
     */
    default void listResources(io.epoch.sdk.gen.epoch.v1.ListResourcesRequest request,
        io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.ListResourcesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListResourcesMethod(), responseObserver);
    }

    /**
     * <pre>
     * DeleteResource removes desired resource state idempotently.
     * </pre>
     */
    default void deleteResource(io.epoch.sdk.gen.epoch.v1.DeleteResourceRequest request,
        io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.DeleteResourceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteResourceMethod(), responseObserver);
    }

    /**
     * <pre>
     * BatchApplyResources atomically commits 1-128 desired resource changes.
     * </pre>
     */
    default void batchApplyResources(io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesRequest request,
        io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getBatchApplyResourcesMethod(), responseObserver);
    }

    /**
     * <pre>
     * GetOperation returns one durable request-token outcome.
     * </pre>
     */
    default void getOperation(io.epoch.sdk.gen.epoch.v1.GetOperationRequest request,
        io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.GetOperationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetOperationMethod(), responseObserver);
    }

    /**
     * <pre>
     * WatchResourceChanges streams resumable, bounded change checkpoints.
     * </pre>
     */
    default void watchResourceChanges(io.epoch.sdk.gen.epoch.v1.WatchResourceChangesRequest request,
        io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.WatchResourceChangesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getWatchResourceChangesMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service RegionalAdminService.
   * <pre>
   * RegionalAdmin is the versioned boundary between managed Go reconciliation
   * and a regional Rust control surface. It carries desired metadata only.
   * </pre>
   */
  public static abstract class RegionalAdminServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return RegionalAdminServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service RegionalAdminService.
   * <pre>
   * RegionalAdmin is the versioned boundary between managed Go reconciliation
   * and a regional Rust control surface. It carries desired metadata only.
   * </pre>
   */
  public static final class RegionalAdminServiceStub
      extends io.grpc.stub.AbstractAsyncStub<RegionalAdminServiceStub> {
    private RegionalAdminServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RegionalAdminServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RegionalAdminServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * ApplyResource idempotently creates or updates desired resource state.
     * </pre>
     */
    public void applyResource(io.epoch.sdk.gen.epoch.v1.ApplyResourceRequest request,
        io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.ApplyResourceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getApplyResourceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * GetResource returns one resource by fully qualified name.
     * </pre>
     */
    public void getResource(io.epoch.sdk.gen.epoch.v1.GetResourceRequest request,
        io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.GetResourceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetResourceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ListResources returns a stable page of resources.
     * </pre>
     */
    public void listResources(io.epoch.sdk.gen.epoch.v1.ListResourcesRequest request,
        io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.ListResourcesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListResourcesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * DeleteResource removes desired resource state idempotently.
     * </pre>
     */
    public void deleteResource(io.epoch.sdk.gen.epoch.v1.DeleteResourceRequest request,
        io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.DeleteResourceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteResourceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * BatchApplyResources atomically commits 1-128 desired resource changes.
     * </pre>
     */
    public void batchApplyResources(io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesRequest request,
        io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getBatchApplyResourcesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * GetOperation returns one durable request-token outcome.
     * </pre>
     */
    public void getOperation(io.epoch.sdk.gen.epoch.v1.GetOperationRequest request,
        io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.GetOperationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetOperationMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * WatchResourceChanges streams resumable, bounded change checkpoints.
     * </pre>
     */
    public void watchResourceChanges(io.epoch.sdk.gen.epoch.v1.WatchResourceChangesRequest request,
        io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.WatchResourceChangesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getWatchResourceChangesMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service RegionalAdminService.
   * <pre>
   * RegionalAdmin is the versioned boundary between managed Go reconciliation
   * and a regional Rust control surface. It carries desired metadata only.
   * </pre>
   */
  public static final class RegionalAdminServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<RegionalAdminServiceBlockingV2Stub> {
    private RegionalAdminServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RegionalAdminServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RegionalAdminServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * ApplyResource idempotently creates or updates desired resource state.
     * </pre>
     */
    public io.epoch.sdk.gen.epoch.v1.ApplyResourceResponse applyResource(io.epoch.sdk.gen.epoch.v1.ApplyResourceRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getApplyResourceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * GetResource returns one resource by fully qualified name.
     * </pre>
     */
    public io.epoch.sdk.gen.epoch.v1.GetResourceResponse getResource(io.epoch.sdk.gen.epoch.v1.GetResourceRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetResourceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ListResources returns a stable page of resources.
     * </pre>
     */
    public io.epoch.sdk.gen.epoch.v1.ListResourcesResponse listResources(io.epoch.sdk.gen.epoch.v1.ListResourcesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListResourcesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * DeleteResource removes desired resource state idempotently.
     * </pre>
     */
    public io.epoch.sdk.gen.epoch.v1.DeleteResourceResponse deleteResource(io.epoch.sdk.gen.epoch.v1.DeleteResourceRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDeleteResourceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * BatchApplyResources atomically commits 1-128 desired resource changes.
     * </pre>
     */
    public io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesResponse batchApplyResources(io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getBatchApplyResourcesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * GetOperation returns one durable request-token outcome.
     * </pre>
     */
    public io.epoch.sdk.gen.epoch.v1.GetOperationResponse getOperation(io.epoch.sdk.gen.epoch.v1.GetOperationRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetOperationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * WatchResourceChanges streams resumable, bounded change checkpoints.
     * </pre>
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<?, io.epoch.sdk.gen.epoch.v1.WatchResourceChangesResponse>
        watchResourceChanges(io.epoch.sdk.gen.epoch.v1.WatchResourceChangesRequest request) {
      return io.grpc.stub.ClientCalls.blockingV2ServerStreamingCall(
          getChannel(), getWatchResourceChangesMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service RegionalAdminService.
   * <pre>
   * RegionalAdmin is the versioned boundary between managed Go reconciliation
   * and a regional Rust control surface. It carries desired metadata only.
   * </pre>
   */
  public static final class RegionalAdminServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<RegionalAdminServiceBlockingStub> {
    private RegionalAdminServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RegionalAdminServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RegionalAdminServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * ApplyResource idempotently creates or updates desired resource state.
     * </pre>
     */
    public io.epoch.sdk.gen.epoch.v1.ApplyResourceResponse applyResource(io.epoch.sdk.gen.epoch.v1.ApplyResourceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getApplyResourceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * GetResource returns one resource by fully qualified name.
     * </pre>
     */
    public io.epoch.sdk.gen.epoch.v1.GetResourceResponse getResource(io.epoch.sdk.gen.epoch.v1.GetResourceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetResourceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ListResources returns a stable page of resources.
     * </pre>
     */
    public io.epoch.sdk.gen.epoch.v1.ListResourcesResponse listResources(io.epoch.sdk.gen.epoch.v1.ListResourcesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListResourcesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * DeleteResource removes desired resource state idempotently.
     * </pre>
     */
    public io.epoch.sdk.gen.epoch.v1.DeleteResourceResponse deleteResource(io.epoch.sdk.gen.epoch.v1.DeleteResourceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteResourceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * BatchApplyResources atomically commits 1-128 desired resource changes.
     * </pre>
     */
    public io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesResponse batchApplyResources(io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getBatchApplyResourcesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * GetOperation returns one durable request-token outcome.
     * </pre>
     */
    public io.epoch.sdk.gen.epoch.v1.GetOperationResponse getOperation(io.epoch.sdk.gen.epoch.v1.GetOperationRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetOperationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * WatchResourceChanges streams resumable, bounded change checkpoints.
     * </pre>
     */
    public java.util.Iterator<io.epoch.sdk.gen.epoch.v1.WatchResourceChangesResponse> watchResourceChanges(
        io.epoch.sdk.gen.epoch.v1.WatchResourceChangesRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getWatchResourceChangesMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service RegionalAdminService.
   * <pre>
   * RegionalAdmin is the versioned boundary between managed Go reconciliation
   * and a regional Rust control surface. It carries desired metadata only.
   * </pre>
   */
  public static final class RegionalAdminServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<RegionalAdminServiceFutureStub> {
    private RegionalAdminServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RegionalAdminServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RegionalAdminServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * ApplyResource idempotently creates or updates desired resource state.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.epoch.sdk.gen.epoch.v1.ApplyResourceResponse> applyResource(
        io.epoch.sdk.gen.epoch.v1.ApplyResourceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getApplyResourceMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * GetResource returns one resource by fully qualified name.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.epoch.sdk.gen.epoch.v1.GetResourceResponse> getResource(
        io.epoch.sdk.gen.epoch.v1.GetResourceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetResourceMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ListResources returns a stable page of resources.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.epoch.sdk.gen.epoch.v1.ListResourcesResponse> listResources(
        io.epoch.sdk.gen.epoch.v1.ListResourcesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListResourcesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * DeleteResource removes desired resource state idempotently.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.epoch.sdk.gen.epoch.v1.DeleteResourceResponse> deleteResource(
        io.epoch.sdk.gen.epoch.v1.DeleteResourceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteResourceMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * BatchApplyResources atomically commits 1-128 desired resource changes.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesResponse> batchApplyResources(
        io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getBatchApplyResourcesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * GetOperation returns one durable request-token outcome.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.epoch.sdk.gen.epoch.v1.GetOperationResponse> getOperation(
        io.epoch.sdk.gen.epoch.v1.GetOperationRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetOperationMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_APPLY_RESOURCE = 0;
  private static final int METHODID_GET_RESOURCE = 1;
  private static final int METHODID_LIST_RESOURCES = 2;
  private static final int METHODID_DELETE_RESOURCE = 3;
  private static final int METHODID_BATCH_APPLY_RESOURCES = 4;
  private static final int METHODID_GET_OPERATION = 5;
  private static final int METHODID_WATCH_RESOURCE_CHANGES = 6;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_APPLY_RESOURCE:
          serviceImpl.applyResource((io.epoch.sdk.gen.epoch.v1.ApplyResourceRequest) request,
              (io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.ApplyResourceResponse>) responseObserver);
          break;
        case METHODID_GET_RESOURCE:
          serviceImpl.getResource((io.epoch.sdk.gen.epoch.v1.GetResourceRequest) request,
              (io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.GetResourceResponse>) responseObserver);
          break;
        case METHODID_LIST_RESOURCES:
          serviceImpl.listResources((io.epoch.sdk.gen.epoch.v1.ListResourcesRequest) request,
              (io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.ListResourcesResponse>) responseObserver);
          break;
        case METHODID_DELETE_RESOURCE:
          serviceImpl.deleteResource((io.epoch.sdk.gen.epoch.v1.DeleteResourceRequest) request,
              (io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.DeleteResourceResponse>) responseObserver);
          break;
        case METHODID_BATCH_APPLY_RESOURCES:
          serviceImpl.batchApplyResources((io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesRequest) request,
              (io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesResponse>) responseObserver);
          break;
        case METHODID_GET_OPERATION:
          serviceImpl.getOperation((io.epoch.sdk.gen.epoch.v1.GetOperationRequest) request,
              (io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.GetOperationResponse>) responseObserver);
          break;
        case METHODID_WATCH_RESOURCE_CHANGES:
          serviceImpl.watchResourceChanges((io.epoch.sdk.gen.epoch.v1.WatchResourceChangesRequest) request,
              (io.grpc.stub.StreamObserver<io.epoch.sdk.gen.epoch.v1.WatchResourceChangesResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getApplyResourceMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.epoch.sdk.gen.epoch.v1.ApplyResourceRequest,
              io.epoch.sdk.gen.epoch.v1.ApplyResourceResponse>(
                service, METHODID_APPLY_RESOURCE)))
        .addMethod(
          getGetResourceMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.epoch.sdk.gen.epoch.v1.GetResourceRequest,
              io.epoch.sdk.gen.epoch.v1.GetResourceResponse>(
                service, METHODID_GET_RESOURCE)))
        .addMethod(
          getListResourcesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.epoch.sdk.gen.epoch.v1.ListResourcesRequest,
              io.epoch.sdk.gen.epoch.v1.ListResourcesResponse>(
                service, METHODID_LIST_RESOURCES)))
        .addMethod(
          getDeleteResourceMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.epoch.sdk.gen.epoch.v1.DeleteResourceRequest,
              io.epoch.sdk.gen.epoch.v1.DeleteResourceResponse>(
                service, METHODID_DELETE_RESOURCE)))
        .addMethod(
          getBatchApplyResourcesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesRequest,
              io.epoch.sdk.gen.epoch.v1.BatchApplyResourcesResponse>(
                service, METHODID_BATCH_APPLY_RESOURCES)))
        .addMethod(
          getGetOperationMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.epoch.sdk.gen.epoch.v1.GetOperationRequest,
              io.epoch.sdk.gen.epoch.v1.GetOperationResponse>(
                service, METHODID_GET_OPERATION)))
        .addMethod(
          getWatchResourceChangesMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              io.epoch.sdk.gen.epoch.v1.WatchResourceChangesRequest,
              io.epoch.sdk.gen.epoch.v1.WatchResourceChangesResponse>(
                service, METHODID_WATCH_RESOURCE_CHANGES)))
        .build();
  }

  private static abstract class RegionalAdminServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    RegionalAdminServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.epoch.sdk.gen.epoch.v1.RegionalAdminProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("RegionalAdminService");
    }
  }

  private static final class RegionalAdminServiceFileDescriptorSupplier
      extends RegionalAdminServiceBaseDescriptorSupplier {
    RegionalAdminServiceFileDescriptorSupplier() {}
  }

  private static final class RegionalAdminServiceMethodDescriptorSupplier
      extends RegionalAdminServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    RegionalAdminServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (RegionalAdminServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new RegionalAdminServiceFileDescriptorSupplier())
              .addMethod(getApplyResourceMethod())
              .addMethod(getGetResourceMethod())
              .addMethod(getListResourcesMethod())
              .addMethod(getDeleteResourceMethod())
              .addMethod(getBatchApplyResourcesMethod())
              .addMethod(getGetOperationMethod())
              .addMethod(getWatchResourceChangesMethod())
              .build();
        }
      }
    }
    return result;
  }
}

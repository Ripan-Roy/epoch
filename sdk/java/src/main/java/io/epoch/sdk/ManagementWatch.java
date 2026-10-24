package io.epoch.sdk;

import io.epoch.sdk.gen.epoch.v1.RegionalAdminServiceGrpc;
import io.epoch.sdk.gen.epoch.v1.WatchResourceChangesRequest;
import io.epoch.sdk.gen.epoch.v1.WatchResourceChangesResponse;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single-consumer persistent watch with explicit scanned-cursor acknowledgements. Process and
 * durably checkpoint each page before acknowledging its exact next cursor. Only UNAVAILABLE/EOF
 * advances once per endpoint; stale cursors never reset to zero.
 */
public final class ManagementWatch implements AutoCloseable {
  private final ManagementClient client;
  private final ManagementContext caller;
  private final Context.CancellableContext context;
  private final WatchResourceChangesRequest filter;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicBoolean receiving = new AtomicBoolean();
  private volatile WireStream stream;
  private volatile long checkpoint;
  private volatile Long pending;
  private volatile int attempts;

  ManagementWatch(
      ManagementClient client, ManagementContext caller, WatchResourceChangesRequest filter) {
    this.client = client;
    this.caller = caller;
    this.context = caller.context().withCancellation();
    this.filter = filter;
    this.checkpoint = filter.getAfterCursor();
  }

  public long checkpoint() {
    return checkpoint;
  }

  public ManagementCallInfo info() {
    return new ManagementCallInfo(attempts, client.budget(), false);
  }

  public void acknowledge(long cursor) {
    if (closed.get() || context.isCancelled()) {
      throw ManagementRPCException.local(Status.CANCELLED);
    }
    if (pending == null) {
      throw ManagementRPCException.local(Status.FAILED_PRECONDITION);
    }
    if (cursor != pending.longValue()) {
      throw ManagementRPCException.local(Status.INVALID_ARGUMENT);
    }
    checkpoint = cursor;
    pending = null;
  }

  public WatchResourceChangesResponse receive() {
    if (!receiving.compareAndSet(false, true)) {
      throw ManagementRPCException.local(Status.FAILED_PRECONDITION);
    }
    try {
      if (closed.get()) {
        throw ManagementRPCException.local(Status.CANCELLED);
      }
      if (pending != null) {
        throw ManagementRPCException.local(Status.FAILED_PRECONDITION);
      }
      return receiveNext();
    } finally {
      receiving.set(false);
    }
  }

  private WatchResourceChangesResponse receiveNext() {
    try {
      while (true) {
        client.usable();
        if (context.isCancelled()) {
          throw Contexts.statusFromCancelled(context).asRuntimeException();
        }
        WireStream current = stream;
        if (current == null) {
          if (attempts == client.budget()) {
            throw Status.UNAVAILABLE.asRuntimeException();
          }
          current = open(attempts++);
          stream = current;
          if (closed.get()) {
            current.cancel();
            throw Status.CANCELLED.asRuntimeException();
          }
        }
        // Exactly one inbound page is requested. No prefetch happens before ACK.
        current.call.request(1);
        Event event = current.events.take();
        if (closed.get()) {
          throw Status.CANCELLED.asRuntimeException();
        }
        if (event instanceof Page page) {
          ManagementContracts.watch(filter, checkpoint, page.value());
          pending = page.value().getNextCursor();
          return page.value();
        }
        End end = (End) event;
        stream = null;
        current.cancel();
        if (end.status().isOk() || end.status().getCode() == Status.Code.UNAVAILABLE) {
          if (attempts < client.budget()) {
            continue;
          }
          Status terminal = end.status().isOk() ? Status.UNAVAILABLE : end.status();
          throw terminal.asRuntimeException(end.trailers());
        }
        throw end.status().asRuntimeException(end.trailers());
      }
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw fail(Status.CANCELLED.withCause(error), null, error);
    } catch (RuntimeException error) {
      throw fail(Status.fromThrowable(error), Status.trailersFromThrowable(error), error);
    }
  }

  private WireStream open(int index) {
    Context previous = context.attach();
    try {
      CallOptions options = CallOptions.DEFAULT;
      if (context.getDeadline() != null) {
        options = options.withDeadline(context.getDeadline());
      }
      ClientCall<WatchResourceChangesRequest, WatchResourceChangesResponse> call =
          client
              .channel(index)
              .newCall(RegionalAdminServiceGrpc.getWatchResourceChangesMethod(), options);
      WireStream wire = new WireStream(call);
      call.start(
          new ClientCall.Listener<>() {
            @Override
            public void onMessage(WatchResourceChangesResponse value) {
              if (!wire.events.offer(new Page(value))) {
                wire.events.clear();
                wire.events.offer(new End(Status.DATA_LOSS, new Metadata()));
                call.cancel("Epoch watch inbound flow contract violated", null);
              }
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
              wire.events.offer(new End(status, trailers));
            }
          },
          client.authenticated(caller));
      call.sendMessage(filter.toBuilder().setAfterCursor(checkpoint).build());
      call.halfClose();
      return wire;
    } finally {
      context.detach(previous);
    }
  }

  private ManagementRPCException fail(Status status, Metadata trailers, Throwable cause) {
    close();
    return new ManagementRPCException(status, trailers, cause, info());
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      context.cancel(null);
      WireStream active = stream;
      if (active != null) {
        active.cancel();
        active.events.clear();
        active.events.offer(new End(Status.CANCELLED, new Metadata()));
      }
    }
  }

  private sealed interface Event permits Page, End {}

  private record Page(WatchResourceChangesResponse value) implements Event {}

  private record End(Status status, Metadata trailers) implements Event {}

  private static final class WireStream {
    final ClientCall<WatchResourceChangesRequest, WatchResourceChangesResponse> call;
    final BlockingQueue<Event> events = new ArrayBlockingQueue<>(2);

    WireStream(ClientCall<WatchResourceChangesRequest, WatchResourceChangesResponse> call) {
      this.call = call;
    }

    void cancel() {
      call.cancel("Epoch management watch closed", null);
    }
  }
}

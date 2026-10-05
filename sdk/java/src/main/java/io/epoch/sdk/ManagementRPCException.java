package io.epoch.sdk;

import io.grpc.Metadata;
import io.grpc.Status;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;

/** Fixed-text failure; backend status, trailers, and cause require deliberate access. */
public final class ManagementRPCException extends RuntimeException {
  private static final long serialVersionUID = 1L;
  private final transient Status status;
  private final transient Metadata trailers;
  private final transient Throwable rpcCause;
  private final transient ManagementCallInfo info;

  ManagementRPCException(
      Status status, Metadata trailers, Throwable cause, ManagementCallInfo info) {
    super(
        "Epoch management RPC failed after "
            + info.attempts()
            + " attempts ("
            + status.getCode()
            + ")",
        null);
    this.status = status;
    this.trailers = trailers == null ? new Metadata() : ManagementContext.copy(trailers);
    this.rpcCause = cause;
    this.info = info;
  }

  public Status status() {
    return status;
  }

  public Metadata trailers() {
    return ManagementContext.copy(trailers);
  }

  public Throwable rpcCause() {
    return rpcCause;
  }

  public ManagementCallInfo info() {
    return info;
  }

  static ManagementRPCException local(Status status) {
    return new ManagementRPCException(status, null, null, new ManagementCallInfo(0, 0, false));
  }

  @Serial
  private void writeObject(ObjectOutputStream output) throws IOException {
    throw new NotSerializableException("Epoch management failures cannot be serialized");
  }

  @Serial
  private void readObject(ObjectInputStream input) throws IOException {
    throw new NotSerializableException("Epoch management failures cannot be deserialized");
  }
}

package io.epoch.sdk;

import io.grpc.Context;
import io.grpc.Metadata;
import java.util.Objects;

/** Caller-owned gRPC deadline/cancellation and a defensive outgoing metadata snapshot. */
public record ManagementContext(Context context, Metadata metadata) {
  public ManagementContext {
    Objects.requireNonNull(context, "context");
    metadata = copy(Objects.requireNonNull(metadata, "metadata"));
  }

  public static ManagementContext current() {
    return new ManagementContext(Context.current(), new Metadata());
  }

  @Override
  public Metadata metadata() {
    return copy(metadata);
  }

  @Override
  public String toString() {
    return "ManagementContext[context=<caller-owned>, metadata=<redacted>]";
  }

  static Metadata copy(Metadata original) {
    Metadata result = new Metadata();
    for (String name : original.keys()) {
      if (name.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
        Metadata.Key<byte[]> key = Metadata.Key.of(name, Metadata.BINARY_BYTE_MARSHALLER);
        for (byte[] value : original.getAll(key)) {
          result.put(key, value.clone());
        }
      } else {
        Metadata.Key<String> key = Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER);
        for (String value : original.getAll(key)) {
          result.put(key, value);
        }
      }
    }
    return result;
  }
}

package io.epoch.sdk;

import java.util.Objects;

/** Immutable generated receipt plus explicit attempt/unknown-outcome information. */
public record ManagementResult<T>(T response, ManagementCallInfo info) {
  public ManagementResult {
    Objects.requireNonNull(response, "response");
    Objects.requireNonNull(info, "info");
  }
}

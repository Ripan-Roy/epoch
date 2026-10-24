package io.epoch.sdk;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/** Explicit controller allowlist, trust policy, and whole-unary-call timeout. */
public record ManagementConfig(
    List<String> endpoints,
    String bearerToken,
    Duration timeout,
    TlsConfig tls,
    boolean allowInsecureLoopback) {
  public ManagementConfig {
    endpoints = List.copyOf(Objects.requireNonNull(endpoints, "endpoints"));
    Objects.requireNonNull(bearerToken, "bearerToken");
    Objects.requireNonNull(timeout, "timeout");
  }

  @Override
  public String toString() {
    return "ManagementConfig[endpoints="
        + endpoints
        + ", bearerToken=<redacted>, timeout="
        + timeout
        + ", tls="
        + (tls != null)
        + ", allowInsecureLoopback="
        + allowInsecureLoopback
        + "]";
  }
}

package io.epoch.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Set;

/** Typed HTTP or transport failure returned by the Epoch Java SDK. */
public final class EpochApiException extends IOException {
  private static final long serialVersionUID = 1L;
  private static final Set<Integer> RETRYABLE_STATUSES = Set.of(408, 425, 429);
  private static final Set<String> RETRYABLE_CODES =
      Set.of("deadline_exceeded", "overloaded", "rate_limited", "transport_error", "unavailable");

  private final int status;
  private final String code;
  private final String detail;
  private final transient JsonNode body;

  public EpochApiException(int status, String code, String detail, JsonNode body) {
    super("Epoch API error %d (%s): %s".formatted(status, code, detail));
    this.status = status;
    this.code = code;
    this.detail = detail;
    this.body = body;
  }

  public EpochApiException(int status, String code, String detail, JsonNode body, Throwable cause) {
    super("Epoch API error %d (%s): %s".formatted(status, code, detail), cause);
    this.status = status;
    this.code = code;
    this.detail = detail;
    this.body = body;
  }

  public int status() {
    return status;
  }

  public String code() {
    return code;
  }

  public String detail() {
    return detail;
  }

  public JsonNode body() {
    return body;
  }

  /** Whether a generic transport retry may be considered for an idempotent operation. */
  public boolean retryable() {
    return status == 0
        || status >= 500
        || RETRYABLE_STATUSES.contains(status)
        || RETRYABLE_CODES.contains(code);
  }
}

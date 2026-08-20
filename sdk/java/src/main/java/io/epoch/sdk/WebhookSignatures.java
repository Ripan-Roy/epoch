package io.epoch.sdk;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Objects;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Verification helpers for requests emitted by Epoch's signed webhook executor. */
public final class WebhookSignatures {
  private static final HexFormat HEX = HexFormat.of();

  private WebhookSignatures() {}

  /** Authenticated identity to persist before applying webhook side effects. */
  public record Verification(String deliveryId, long attempt, Instant signedAt) {}

  /** Verifies the exact request body, replay identity, and bounded v1 timestamp. */
  public static Verification verify(
      byte[] secret,
      byte[] body,
      String deliveryId,
      String attemptHeader,
      String timestampHeader,
      String signatureHeader,
      Instant now,
      Duration tolerance) {
    Objects.requireNonNull(secret, "secret");
    Objects.requireNonNull(body, "body");
    Objects.requireNonNull(now, "now");
    Objects.requireNonNull(tolerance, "tolerance");
    if (secret.length == 0) {
      throw new IllegalArgumentException("webhook secret is required");
    }
    if (deliveryId == null || deliveryId.isBlank()) {
      throw new IllegalArgumentException("webhook delivery ID is required");
    }
    long attempt = canonicalNonNegativeLong(attemptHeader, "attempt");
    if (attempt == 0 || attempt > 4_294_967_295L) {
      throw new IllegalArgumentException("webhook attempt must be between 1 and 4294967295");
    }
    long timestamp = canonicalNonNegativeLong(timestampHeader, "timestamp");
    if (tolerance.isZero() || tolerance.isNegative()) {
      throw new IllegalArgumentException("webhook timestamp tolerance must be positive");
    }
    Instant signedAt;
    try {
      signedAt = Instant.ofEpochSecond(timestamp);
      if (signedAt.isBefore(now.minus(tolerance)) || signedAt.isAfter(now.plus(tolerance))) {
        throw new IllegalArgumentException("webhook timestamp is outside the allowed tolerance");
      }
    } catch (DateTimeException | ArithmeticException error) {
      throw new IllegalArgumentException("webhook timestamp is invalid", error);
    }
    if (signatureHeader == null
        || signatureHeader.length() != 67
        || !signatureHeader.startsWith("v1=")
        || !signatureHeader
            .substring(3)
            .equals(signatureHeader.substring(3).toLowerCase(Locale.ROOT))) {
      throw new IllegalArgumentException("webhook signature must use v1 lowercase hexadecimal");
    }
    byte[] provided;
    try {
      provided = HEX.parseHex(signatureHeader.substring(3));
    } catch (IllegalArgumentException error) {
      throw new IllegalArgumentException(
          "webhook signature must use v1 lowercase hexadecimal", error);
    }
    byte[] expected = mac(secret, timestamp, deliveryId, attempt, body);
    if (!MessageDigest.isEqual(provided, expected)) {
      throw new IllegalArgumentException("webhook signature is invalid");
    }
    return new Verification(deliveryId, attempt, signedAt);
  }

  private static long canonicalNonNegativeLong(String value, String name) {
    if (value == null || value.isEmpty() || !value.chars().allMatch(Character::isDigit)) {
      throw new IllegalArgumentException("webhook " + name + " must be a non-negative integer");
    }
    try {
      long parsed = Long.parseLong(value);
      if (parsed < 0 || !Long.toString(parsed).equals(value)) {
        throw new IllegalArgumentException(
            "webhook " + name + " must use canonical decimal encoding");
      }
      return parsed;
    } catch (NumberFormatException error) {
      throw new IllegalArgumentException(
          "webhook " + name + " must be a non-negative integer", error);
    }
  }

  private static byte[] mac(
      byte[] secret, long timestamp, String deliveryId, long attempt, byte[] body) {
    try {
      MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
      String bodyDigest = HEX.formatHex(sha256.digest(body));
      String canonical =
          "v1\n" + timestamp + "\n" + deliveryId + "\n" + attempt + "\n" + bodyDigest;
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(secret, "HmacSHA256"));
      return mac.doFinal(canonical.getBytes(StandardCharsets.UTF_8));
    } catch (GeneralSecurityException error) {
      throw new IllegalStateException("required webhook cryptography is unavailable", error);
    }
  }
}

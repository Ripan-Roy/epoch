package io.epoch.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

final class WebhookSignaturesTest {
  private static final byte[] SECRET =
      "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8);
  private static final String DELIVERY_ID = "epoch.bus.delivery.v1.1.orders";
  private static final String SIGNATURE =
      "v1=866b035f5c00f59cc64a7caea8a4d16be04dd41966774cdfc336e7cf341d18d9";

  @Test
  void verifiesCrossLanguageVectorAndReturnsReplayIdentity() {
    WebhookSignatures.Verification verified =
        WebhookSignatures.verify(
            SECRET,
            "{\"order_id\":\"one\"}".getBytes(StandardCharsets.UTF_8),
            DELIVERY_ID,
            "2",
            "1700000000",
            SIGNATURE,
            Instant.ofEpochSecond(1_700_000_010L),
            Duration.ofSeconds(30));

    assertEquals(DELIVERY_ID, verified.deliveryId());
    assertEquals(2L, verified.attempt());
  }

  @Test
  void changedOrStaleRequestsFailClosed() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WebhookSignatures.verify(
                SECRET,
                "{\"order_id\":\"changed\"}".getBytes(StandardCharsets.UTF_8),
                DELIVERY_ID,
                "2",
                "1700000000",
                SIGNATURE,
                Instant.ofEpochSecond(1_700_000_010L),
                Duration.ofSeconds(30)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WebhookSignatures.verify(
                SECRET,
                "{\"order_id\":\"one\"}".getBytes(StandardCharsets.UTF_8),
                DELIVERY_ID,
                "2",
                "1700000000",
                SIGNATURE,
                Instant.ofEpochSecond(1_700_000_031L),
                Duration.ofSeconds(30)));
  }

  @Test
  void nonCanonicalReplayHeadersFailClosed() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WebhookSignatures.verify(
                SECRET,
                "{\"order_id\":\"one\"}".getBytes(StandardCharsets.UTF_8),
                DELIVERY_ID,
                "02",
                "1700000000",
                SIGNATURE,
                Instant.ofEpochSecond(1_700_000_010L),
                Duration.ofSeconds(30)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WebhookSignatures.verify(
                SECRET,
                "{\"order_id\":\"one\"}".getBytes(StandardCharsets.UTF_8),
                DELIVERY_ID,
                "2",
                "01700000000",
                SIGNATURE,
                Instant.ofEpochSecond(1_700_000_010L),
                Duration.ofSeconds(30)));
  }
}

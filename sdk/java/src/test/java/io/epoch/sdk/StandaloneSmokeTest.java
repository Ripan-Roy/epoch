package io.epoch.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

final class StandaloneSmokeTest {
  @Test
  @EnabledIfEnvironmentVariable(named = "EPOCH_JAVA_INTEGRATION_URL", matches = "https?://.+")
  void exercisesARealStandaloneNode() throws Exception {
    EpochClient client =
        new EpochClient(
            URI.create(System.getenv("EPOCH_JAVA_INTEGRATION_URL")), Duration.ofSeconds(5));

    assertEquals("local_durable", client.health().get("guarantee_ceiling").textValue());
    client.createStream("java-sdk-smoke", StreamConfig.defaults());
    client.appendStream(
        "java-sdk-smoke",
        EventEnvelope.builder("java-sdk", "smoke.created", Map.of("ok", true))
            .id("java-smoke-1")
            .timeMs(1_000)
            .build());

    assertEquals(
        "java-smoke-1",
        client
            .fetchStream("java-sdk-smoke", 0, 0, 10)
            .get(0)
            .get("envelope")
            .get("id")
            .textValue());
  }
}

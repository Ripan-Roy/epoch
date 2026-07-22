package io.epoch.releaseprobe;

import io.epoch.sdk.DurabilityProfile;
import io.epoch.sdk.EventEnvelope;
import java.util.Map;

/** Minimal external consumer used to validate the locally built SDK artifact. */
public final class ArtifactConsumer {
  private ArtifactConsumer() {}

  public static void main(String[] args) {
    EventEnvelope envelope =
        EventEnvelope.builder("release-gate", "artifact.probed", Map.of("ok", true))
            .id("artifact-probe")
            .timeMs(1L)
            .build();

    if (!"artifact.probed".equals(envelope.toJson().path("type").asText())) {
      throw new IllegalStateException("the packaged EventEnvelope API returned an invalid type");
    }
    if (!"local_durable".equals(DurabilityProfile.LOCAL_DURABLE.wireName())) {
      throw new IllegalStateException("the packaged durability API returned an invalid wire name");
    }
  }
}

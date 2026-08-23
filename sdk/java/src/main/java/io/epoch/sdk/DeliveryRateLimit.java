package io.epoch.sdk;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Per-subscription committed delivery rate and burst. */
public record DeliveryRateLimit(int deliveriesPerSecond, int burst) {
  public DeliveryRateLimit {
    if (deliveriesPerSecond < 1
        || deliveriesPerSecond > 1_000_000
        || burst < 1
        || burst > 1_000_000) {
      throw new IllegalArgumentException("delivery rate and burst must be between 1 and 1000000");
    }
  }

  ObjectNode toJson() {
    return JsonNodeFactory.instance
        .objectNode()
        .put("deliveries_per_second", deliveriesPerSecond)
        .put("burst", burst);
  }
}

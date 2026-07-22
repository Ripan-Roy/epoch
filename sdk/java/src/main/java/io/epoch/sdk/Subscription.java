package io.epoch.sdk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Objects;

/** Typed Event Bus subscription definition. */
public record Subscription(
    String name, EventFilter filter, SubscriptionTarget target, EventTransform transform) {

  public Subscription {
    if (Objects.requireNonNull(name, "name").isBlank()) {
      throw new IllegalArgumentException("name is required");
    }
    Objects.requireNonNull(filter, "filter");
    Objects.requireNonNull(target, "target");
    Objects.requireNonNull(transform, "transform");
  }

  public Subscription(String name, SubscriptionTarget target) {
    this(name, EventFilter.empty(), target, EventTransform.empty());
  }

  ObjectNode toJson(ObjectMapper mapper) {
    ObjectNode value = mapper.createObjectNode().put("name", name);
    value.set("filter", filter.toJson(mapper));
    value.set("target", target.toJson());
    value.set("transform", transform.toJson(mapper));
    return value;
  }
}

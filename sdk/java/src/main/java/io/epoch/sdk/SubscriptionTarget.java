package io.epoch.sdk;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Objects;

/** Typed Event Bus delivery target. */
public final class SubscriptionTarget {
  private final String kind;
  private final String resource;
  private final String url;

  private SubscriptionTarget(String kind, String resource, String url) {
    this.kind = kind;
    this.resource = resource;
    this.url = url;
  }

  public static SubscriptionTarget pull() {
    return new SubscriptionTarget("pull", null, null);
  }

  public static SubscriptionTarget queue(String resource) {
    return resource("queue", resource);
  }

  public static SubscriptionTarget stream(String resource) {
    return resource("stream", resource);
  }

  public static SubscriptionTarget webhook(String url) {
    return url("webhook", url);
  }

  public static SubscriptionTarget http(String url) {
    return url("http", url);
  }

  private static SubscriptionTarget resource(String kind, String resource) {
    return new SubscriptionTarget(kind, required(resource, "resource"), null);
  }

  private static SubscriptionTarget url(String kind, String url) {
    return new SubscriptionTarget(kind, null, required(url, "url"));
  }

  private static String required(String value, String name) {
    if (Objects.requireNonNull(value, name).isBlank()) {
      throw new IllegalArgumentException(name + " is required");
    }
    return value;
  }

  ObjectNode toJson() {
    ObjectNode value = JsonNodeFactory.instance.objectNode().put("kind", kind);
    if (resource != null) {
      value.put("resource", resource);
    }
    if (url != null) {
      value.put("url", url);
    }
    return value;
  }
}

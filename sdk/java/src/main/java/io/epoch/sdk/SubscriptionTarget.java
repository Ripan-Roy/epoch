package io.epoch.sdk;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Objects;

/** Typed Event Bus delivery target. */
public final class SubscriptionTarget {
  private final String kind;
  private final String resource;
  private final String url;
  private final String signingKeyId;
  private final String pool;
  private final DestinationAuth auth;
  private final String cloudEventsMode;

  private SubscriptionTarget(
      String kind,
      String resource,
      String url,
      String signingKeyId,
      String pool,
      DestinationAuth auth,
      String cloudEventsMode) {
    this.kind = kind;
    this.resource = resource;
    this.url = url;
    this.signingKeyId = signingKeyId;
    this.pool = pool;
    this.auth = auth;
    this.cloudEventsMode = cloudEventsMode;
  }

  public static SubscriptionTarget pull() {
    return new SubscriptionTarget("pull", null, null, null, null, null, null);
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

  public static SubscriptionTarget signedWebhook(String url, String signingKeyId) {
    return signedUrl("webhook", url, signingKeyId);
  }

  public static SubscriptionTarget http(String url) {
    return url("http", url);
  }

  public static SubscriptionTarget signedHttp(String url, String signingKeyId) {
    return signedUrl("http", url, signingKeyId);
  }

  public static SubscriptionTarget apiDestination(
      String url, DestinationAuth auth, String cloudEventsMode) {
    return new SubscriptionTarget(
        "api_destination",
        null,
        required(url, "url"),
        null,
        null,
        Objects.requireNonNull(auth, "auth"),
        cloudEventsMode(cloudEventsMode));
  }

  public static SubscriptionTarget endpointPool(
      String pool, DestinationAuth auth, String cloudEventsMode) {
    return new SubscriptionTarget(
        "endpoint_pool",
        null,
        null,
        null,
        resourceName(pool, "endpoint pool"),
        Objects.requireNonNull(auth, "auth"),
        cloudEventsMode(cloudEventsMode));
  }

  public static SubscriptionTarget function(String resource) {
    return resource("function", resource);
  }

  public static SubscriptionTarget connector(String resource) {
    return resource("connector", resource);
  }

  private static SubscriptionTarget resource(String kind, String resource) {
    return new SubscriptionTarget(
        kind, required(resource, "resource"), null, null, null, null, null);
  }

  private static SubscriptionTarget url(String kind, String url) {
    return new SubscriptionTarget(kind, null, required(url, "url"), null, null, null, null);
  }

  private static SubscriptionTarget signedUrl(String kind, String url, String signingKeyId) {
    return new SubscriptionTarget(
        kind,
        null,
        required(url, "url"),
        resourceName(signingKeyId, "signing key ID"),
        null,
        null,
        null);
  }

  private static String cloudEventsMode(String value) {
    if (!"binary".equals(value) && !"structured".equals(value)) {
      throw new IllegalArgumentException("CloudEvents mode must be binary or structured");
    }
    return value;
  }

  private static String resourceName(String value, String name) {
    String required = required(value, name);
    if (required.length() > 128
        || required.chars().anyMatch(character -> !isResourceNameCharacter(character))) {
      throw new IllegalArgumentException(name + " must be a 1-128 byte resource name");
    }
    return required;
  }

  private static boolean isResourceNameCharacter(int character) {
    return character >= 'a' && character <= 'z'
        || character >= 'A' && character <= 'Z'
        || character >= '0' && character <= '9'
        || character == '-'
        || character == '_'
        || character == '.';
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
    if (signingKeyId != null) {
      value.put("signing_key_id", signingKeyId);
    }
    if (pool != null) {
      value.put("pool", pool);
    }
    if (auth != null) {
      value.set("auth", auth.toJson());
    }
    if (cloudEventsMode != null) {
      value.put("cloud_events_mode", cloudEventsMode);
    }
    return value;
  }
}

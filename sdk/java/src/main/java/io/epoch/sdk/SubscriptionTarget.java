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

  private SubscriptionTarget(String kind, String resource, String url, String signingKeyId) {
    this.kind = kind;
    this.resource = resource;
    this.url = url;
    this.signingKeyId = signingKeyId;
  }

  public static SubscriptionTarget pull() {
    return new SubscriptionTarget("pull", null, null, null);
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

  private static SubscriptionTarget resource(String kind, String resource) {
    return new SubscriptionTarget(kind, required(resource, "resource"), null, null);
  }

  private static SubscriptionTarget url(String kind, String url) {
    return new SubscriptionTarget(kind, null, required(url, "url"), null);
  }

  private static SubscriptionTarget signedUrl(String kind, String url, String signingKeyId) {
    return new SubscriptionTarget(
        kind, null, required(url, "url"), resourceName(signingKeyId, "signing key ID"));
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
    return value;
  }
}

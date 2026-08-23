package io.epoch.sdk;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.util.List;
import java.util.Objects;

/** Rotatable API destination credential reference; never a secret value. */
public final class DestinationAuth {
  private final String kind;
  private final String secretRef;
  private final String header;
  private final String tokenUrl;
  private final List<String> scopes;

  private DestinationAuth(
      String kind, String secretRef, String header, String tokenUrl, List<String> scopes) {
    this.kind = kind;
    this.secretRef = secretRef;
    this.header = header;
    this.tokenUrl = tokenUrl;
    this.scopes = List.copyOf(scopes);
  }

  public static DestinationAuth none() {
    return new DestinationAuth("none", null, null, null, List.of());
  }

  public static DestinationAuth apiKey(String secretRef, String header) {
    return new DestinationAuth(
        "api_key",
        resourceName(secretRef, "secret reference"),
        bounded(required(header, "header"), "header", 256),
        null,
        List.of());
  }

  public static DestinationAuth oauth2(String secretRef, String tokenUrl, List<String> scopes) {
    List<String> checkedScopes = List.copyOf(Objects.requireNonNull(scopes, "scopes"));
    if (checkedScopes.size() > 64) {
      throw new IllegalArgumentException("OAuth2 scopes cannot exceed 64 entries");
    }
    checkedScopes.forEach(scope -> bounded(required(scope, "OAuth2 scope"), "OAuth2 scope", 4096));
    return new DestinationAuth(
        "oauth2",
        resourceName(secretRef, "secret reference"),
        null,
        httpUrl(tokenUrl, "token URL"),
        checkedScopes);
  }

  ObjectNode toJson() {
    ObjectNode value = JsonNodeFactory.instance.objectNode().put("kind", kind);
    if (secretRef != null) {
      value.put("secret_ref", secretRef);
    }
    if (header != null) {
      value.put("header", header);
    }
    if (tokenUrl != null) {
      value.put("token_url", tokenUrl);
    }
    if (!scopes.isEmpty()) {
      value.set(
          "scopes",
          JsonNodeFactory.instance
              .arrayNode()
              .addAll(scopes.stream().map(JsonNodeFactory.instance::textNode).toList()));
    }
    return value;
  }

  private static String required(String value, String name) {
    if (Objects.requireNonNull(value, name).isBlank()) {
      throw new IllegalArgumentException(name + " is required");
    }
    return value;
  }

  private static String bounded(String value, String name, int maximumBytes) {
    if (value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > maximumBytes) {
      throw new IllegalArgumentException(name + " exceeds " + maximumBytes + " bytes");
    }
    return value;
  }

  private static String resourceName(String value, String name) {
    String checked = required(value, name);
    if (checked.length() > 128
        || checked.chars().anyMatch(character -> !isResourceNameCharacter(character))) {
      throw new IllegalArgumentException(name + " must be a 1-128 byte resource name");
    }
    return checked;
  }

  private static boolean isResourceNameCharacter(int character) {
    return character >= 'a' && character <= 'z'
        || character >= 'A' && character <= 'Z'
        || character >= '0' && character <= '9'
        || character == '-'
        || character == '_'
        || character == '.';
  }

  private static String httpUrl(String value, String name) {
    String checked = required(value, name);
    URI uri;
    try {
      uri = URI.create(checked);
    } catch (IllegalArgumentException error) {
      throw new IllegalArgumentException(name + " must be an absolute HTTP(S) URL", error);
    }
    if (!("http".equals(uri.getScheme()) || "https".equals(uri.getScheme()))
        || uri.getHost() == null
        || uri.getUserInfo() != null
        || uri.getFragment() != null) {
      throw new IllegalArgumentException(name + " must be an absolute HTTP(S) URL");
    }
    return checked;
  }
}

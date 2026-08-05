package io.epoch.sdk;

import java.util.Objects;

/** Fully-qualified organization/project/environment/namespace scope. */
public record RegionalScope(
    String organization, String project, String environment, String namespace) {
  public RegionalScope {
    organization = required(organization, "organization");
    project = required(project, "project");
    environment = required(environment, "environment");
    namespace = required(namespace, "namespace");
  }

  private static String required(String value, String label) {
    if (Objects.requireNonNull(value, label).isBlank()) {
      throw new IllegalArgumentException(label + " is required");
    }
    return value;
  }
}

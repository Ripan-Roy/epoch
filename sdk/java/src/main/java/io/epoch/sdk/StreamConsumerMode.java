package io.epoch.sdk;

/** Shared push or independently identified long-poll consumer lane. */
public enum StreamConsumerMode {
  PUSH("push"),
  DEDICATED("dedicated");

  private final String wireValue;

  StreamConsumerMode(String wireValue) {
    this.wireValue = wireValue;
  }

  String wireValue() {
    return wireValue;
  }
}

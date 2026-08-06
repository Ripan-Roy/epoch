package io.epoch.sdk;

/** Deterministic Event Bus delivery retry strategy. */
public enum DeliveryBackoffStrategy {
  EXPONENTIAL("exponential"),
  FIXED("fixed");

  private final String wireValue;

  DeliveryBackoffStrategy(String wireValue) {
    this.wireValue = wireValue;
  }

  String wireValue() {
    return wireValue;
  }
}

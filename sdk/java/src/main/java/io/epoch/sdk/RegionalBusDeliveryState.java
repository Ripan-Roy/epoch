package io.epoch.sdk;

/** Replicated Event Bus delivery-ledger state. */
public enum RegionalBusDeliveryState {
  PENDING("pending"),
  IN_FLIGHT("in_flight"),
  ACKNOWLEDGED("acknowledged"),
  DEAD_LETTERED("dead_lettered");

  private final String wireValue;

  RegionalBusDeliveryState(String wireValue) {
    this.wireValue = wireValue;
  }

  String wireValue() {
    return wireValue;
  }
}

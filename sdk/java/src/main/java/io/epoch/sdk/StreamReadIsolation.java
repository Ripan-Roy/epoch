package io.epoch.sdk;

/** Transaction visibility for Stream reads. */
public enum StreamReadIsolation {
  READ_COMMITTED("read_committed"),
  READ_UNCOMMITTED("read_uncommitted");

  private final String wireValue;

  StreamReadIsolation(String wireValue) {
    this.wireValue = wireValue;
  }

  String wireValue() {
    return wireValue;
  }
}

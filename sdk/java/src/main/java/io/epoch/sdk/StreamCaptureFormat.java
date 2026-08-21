package io.epoch.sdk;

/** Portable open format for a bounded Stream capture. */
public enum StreamCaptureFormat {
  JSON_LINES("json_lines"),
  JSON_ARRAY("json_array");

  private final String wireValue;

  StreamCaptureFormat(String wireValue) {
    this.wireValue = wireValue;
  }

  String wireValue() {
    return wireValue;
  }
}

package io.epoch.sdk;

/** Standard frame encodings accepted by the atomic Stream batch protocol. */
public enum StreamCompression {
  NONE("none"),
  GZIP("gzip"),
  LZ4("lz4"),
  SNAPPY("snappy"),
  ZSTD("zstd");

  private final String wireName;

  StreamCompression(String wireName) {
    this.wireName = wireName;
  }

  /** Returns the exact protocol identifier. */
  public String wireName() {
    return wireName;
  }
}

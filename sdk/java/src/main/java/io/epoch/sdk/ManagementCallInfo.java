package io.epoch.sdk;

/** SDK RPC attempts (not physical frames), endpoint budget, and ambiguous mutation outcome. */
public record ManagementCallInfo(int attempts, int budget, boolean outcomeMayBeUnknown) {}

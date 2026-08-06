from __future__ import annotations

import json
import os
import time
from typing import Any

from epoch_sdk import (
    RegionalCacheClient,
    RegionalCacheExpectation,
    RegionalCacheLockGuard,
    RegionalCacheMutation,
    RegionalCacheValue,
    RegionalScope,
)


def result(document: dict[str, Any]) -> dict[str, Any]:
    operation = document["receipt"]["outcome"]["result"]
    assert isinstance(operation, dict)
    return operation


client = RegionalCacheClient(
    os.getenv(
        "EPOCH_REGIONAL_ENDPOINTS",
        "http://127.0.0.1:18661,http://127.0.0.1:18662,http://127.0.0.1:18663",
    ).split(","),
    token=os.getenv("EPOCH_TOKEN", "epoch-dev-admin-v1"),
    scope=RegionalScope("acme", "shop", "dev", "core"),
    timeout=3.0,
)

written = client.set(
    "sessions",
    0,
    "docs-python-cache-set-v1",
    "profile",
    RegionalCacheValue.string("alice"),
)
replayed = client.set(
    "sessions",
    0,
    "docs-python-cache-set-v1",
    "profile",
    RegionalCacheValue.string("alice"),
)
version = int(result(written)["item"]["version"])
compared = client.compare_and_set(
    "sessions",
    0,
    "docs-python-cache-cas-v1",
    "profile",
    RegionalCacheExpectation.version(version),
    RegionalCacheValue.hash({"name": "alice", "role": "admin"}),
)
observation = client.observe("sessions", 0, "profile")
revision = int(observation["observation"]["shard_revision"])
transaction = client.transaction(
    "sessions",
    0,
    "docs-python-cache-transaction-v1",
    revision,
    [
        RegionalCacheMutation.set("visits", RegionalCacheValue.counter(1)),
        RegionalCacheMutation.set("recent", RegionalCacheValue.list(["home", "checkout"])),
        RegionalCacheMutation.set("roles", RegionalCacheValue.set(["admin", "buyer"])),
        RegionalCacheMutation.set(
            "rank", RegionalCacheValue.sorted_set({"alice": 9.5})
        ),
        RegionalCacheMutation.set("avatar", RegionalCacheValue.blob(b"epoch")),
    ],
)
acquired = client.acquire_lock(
    "sessions", 0, "docs-python-cache-lock-v1", "profile-lock", "docs-python", 1, 60_000
)
lease_token = result(acquired)["lease_token"]
guard = RegionalCacheLockGuard("profile-lock", "docs-python", 1, lease_token)
guarded = client.increment(
    "sessions",
    0,
    "docs-python-cache-guarded-increment-v1",
    "visits",
    1,
    lock_guard=guard,
)
released = client.release_lock(
    "sessions",
    0,
    "docs-python-cache-release-v1",
    "profile-lock",
    "docs-python",
    1,
    lease_token,
)
ephemeral = client.set(
    "sessions",
    0,
    "docs-python-cache-ttl-v1",
    "flash",
    RegionalCacheValue.string("short"),
    ttl_ms=1,
)
time.sleep(0.01)
maintained = client.maintain(
    "sessions", 0, "docs-python-cache-maintain-v1", max_expirations=100
)

print(
    json.dumps(
        {
            "set": written,
            "exact_retry": replayed,
            "cas": compared,
            "transaction": transaction,
            "guarded_increment": guarded,
            "release": released,
            "ttl": ephemeral,
            "maintain": maintained,
            "profile": client.observe("sessions", 0, "profile"),
            "status": client.status("sessions", 0),
        },
        indent=2,
    )
)

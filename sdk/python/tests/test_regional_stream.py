from __future__ import annotations

import base64
import gzip
import unittest
from typing import Any

from epoch_sdk import (
    EpochAPIError,
    EventEnvelope,
    RegionalScope,
    RegionalStreamClient,
    StreamBatchFrame,
    StreamBatchRecord,
    StreamRetentionPolicy,
    stream_shard_for,
)
from epoch_sdk.regional import _claim_generations


class RegionalFakeTransport:
    def __init__(
        self,
        route: dict[str, Any],
        operation_errors: list[EpochAPIError] | None = None,
        routes: dict[str, dict[str, Any]] | None = None,
        operation_responses: list[dict[str, Any]] | None = None,
    ) -> None:
        self.route = route
        self.routes = dict(routes or {})
        self.requests: list[dict[str, Any]] = []
        self.operation_errors = list(operation_errors or [])
        self.operation_responses = list(operation_responses or [])

    def request(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        query: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        self.requests.append(
            {"method": method, "path": path, "body": body, "query": query, "headers": headers}
        )
        shard_suffix = path.rsplit("/shards/", maxsplit=1)[-1]
        if method == "GET" and shard_suffix.isascii() and shard_suffix.isdigit():
            return self.routes.get(shard_suffix, self.route)
        if self.operation_errors:
            raise self.operation_errors.pop(0)
        if self.operation_responses:
            return self.operation_responses.pop(0)
        return {"state": "committed", "outcome_certainty": "committed"}


class DiscoveryErrorTransport:
    def __init__(self, error: EpochAPIError) -> None:
        self.error = error
        self.requests: list[dict[str, Any]] = []

    def request(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        query: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        self.requests.append(
            {"method": method, "path": path, "body": body, "query": query, "headers": headers}
        )
        raise self.error


class RegionalStreamClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.follower = RegionalFakeTransport(
            {
                "resource_generation": "5",
                "tablet_epoch": "3",
                "term": "8",
                "accepts_writes": False,
            }
        )
        self.leader = RegionalFakeTransport(
            {
                "resource_generation": "5",
                "tablet_epoch": "3",
                "term": "8",
                "accepts_writes": True,
            }
        )
        self.client = RegionalStreamClient.with_transports(
            [self.follower, self.leader],
            token="secret-token",
            scope=RegionalScope("acme", "shop", "dev", "core"),
        )

    def test_keyed_append_uses_the_published_utf8_partitioner(self) -> None:
        leader = RegionalFakeTransport(
            {
                "resource_generation": "5",
                "tablet_epoch": "3",
                "term": "8",
                "accepts_writes": True,
                "stream_partitioning": {
                    "algorithm": "fnv1a64_utf8_mod_n_v1",
                    "key_encoding": "utf8",
                    "missing_key_fallback": "event_id",
                    "shard_count": 16,
                },
            }
        )
        client = RegionalStreamClient.with_transports(
            [leader],
            token="secret-token",
            scope=RegionalScope("acme", "shop", "dev", "core"),
        )
        event = EventEnvelope(
            id="order-1",
            key="customer-42",
            source="checkout",
            event_type="order.created",
            payload={"id": "42"},
            time_ms=42,
        )

        self.assertEqual(stream_shard_for("customer-42", 16), 14)
        self.assertEqual(stream_shard_for("order-1", 16), 13)
        self.assertEqual(stream_shard_for("café", 16), 9)
        self.assertEqual(stream_shard_for("東京", 16), 15)
        client.append_keyed("orders", "append-42", event)

        self.assertEqual(len(leader.requests), 3)
        self.assertTrue(leader.requests[0]["path"].endswith("/shards/0"))
        self.assertTrue(leader.requests[1]["path"].endswith("/shards/14"))
        self.assertTrue(leader.requests[2]["path"].endswith("/shards/14/records"))

    def test_keyed_append_fails_closed_when_routing_generation_changes(self) -> None:
        partitioning = {
            "algorithm": "fnv1a64_utf8_mod_n_v1",
            "key_encoding": "utf8",
            "missing_key_fallback": "event_id",
            "shard_count": 16,
        }
        bootstrap = {
            "resource_generation": "5",
            "tablet_epoch": "3",
            "term": "8",
            "accepts_writes": True,
            "stream_partitioning": partitioning,
        }
        target = {
            **bootstrap,
            "resource_generation": "6",
            "term": "9",
        }
        transport = RegionalFakeTransport(bootstrap, routes={"0": bootstrap, "14": target})
        client = RegionalStreamClient.with_transports(
            [transport],
            token="secret-token",
            scope=RegionalScope("acme", "shop", "dev", "core"),
        )
        event = EventEnvelope(
            id="order-1",
            key="customer-42",
            source="checkout",
            event_type="order.created",
            payload={},
        )

        with self.assertRaisesRegex(ValueError, "generation changed"):
            client.append_keyed("orders", "append-42", event)

        self.assertEqual(len(transport.requests), 2)

    def test_append_discovers_leader_and_carries_auth_fences_and_term(self) -> None:
        event = EventEnvelope(
            id="order-42",
            source="checkout",
            event_type="order.created",
            payload={"id": "42"},
            time_ms=42,
        )
        response = self.client.append("orders/eu", 0, "append-42", event)

        self.assertEqual(response["state"], "committed")
        self.assertEqual(len(self.follower.requests), 1)
        self.assertEqual(len(self.leader.requests), 2)
        path = (
            "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/"
            "streams/orders%2Feu/shards/0"
        )
        self.assertEqual(self.follower.requests[0]["path"], path)
        write = self.leader.requests[1]
        self.assertEqual((write["method"], write["path"]), ("POST", f"{path}/records"))
        self.assertEqual(write["headers"]["authorization"], "Bearer secret-token")
        self.assertEqual(write["headers"]["x-epoch-resource-generation"], "5")
        self.assertEqual(write["headers"]["x-epoch-tablet-epoch"], "3")
        self.assertEqual(write["body"]["idempotency_key"], "append-42")
        self.assertEqual(write["body"]["expected_term"], "8")

    def test_group_and_linearizable_fetch_contracts(self) -> None:
        self.client.commit_offset(
            "orders", 0, "billing/eu", "member-a", 3, 11, idempotency_key="commit-11"
        )
        self.client.fetch("orders", 0, 11, limit=25)

        commit = self.leader.requests[1]
        self.assertTrue(commit["path"].endswith("/groups/billing%2Feu/offsets"))
        self.assertEqual(commit["body"]["mode"], "commit")
        read = self.leader.requests[3]
        self.assertEqual(read["headers"]["x-epoch-read-consistency"], "linearizable")
        self.assertEqual(read["query"], {"offset": 11, "limit": 25})

    def test_coordinated_consumer_session_contracts_use_shard_zero(self) -> None:
        self.client.join_consumer_session(
            "orders", "billing/eu", "member-a", 30_000, idempotency_key="join-a"
        )
        self.client.heartbeat_consumer_session(
            "orders", "billing/eu", "member-a", 3, idempotency_key="heartbeat-a"
        )
        self.client.consumer_session("orders", "billing/eu")
        self.client.maintain_consumer_session("orders", "billing/eu", idempotency_key="maintain-a")
        self.client.leave_consumer_session(
            "orders", "billing/eu", "member-a", 3, idempotency_key="leave-a"
        )

        expected = [
            (1, "POST", "/groups/billing%2Feu/sessions"),
            (3, "PUT", "/groups/billing%2Feu/sessions/member-a/heartbeat"),
            (5, "GET", "/groups/billing%2Feu/sessions"),
            (7, "POST", "/groups/billing%2Feu/sessions/maintenance"),
            (9, "DELETE", "/groups/billing%2Feu/sessions/member-a"),
        ]
        for index, method, suffix in expected:
            request = self.leader.requests[index]
            self.assertEqual(request["method"], method)
            self.assertTrue(request["path"].endswith(suffix))
        self.assertEqual(
            self.leader.requests[5]["headers"]["x-epoch-read-consistency"], "linearizable"
        )
        for index in (0, 2, 4, 6, 8):
            self.assertTrue(self.leader.requests[index]["path"].endswith("/shards/0"))

    def test_claims_and_revalidates_assigned_shards_before_fenced_fetch(self) -> None:
        session = {
            "session": {
                "exists": True,
                "group": "billing/eu",
                "shard_count": 3,
                "group_generation": "1",
                "members": [{"member_id": "member-a", "assigned_shards": [0, 2]}],
            }
        }
        claim = {"receipt": {"outcome": "applied", "session_fenced": True}}
        unclaimed = {"checkpoint": {"exists": False}}
        leader = RegionalFakeTransport(
            {
                "resource_generation": "2",
                "tablet_epoch": "4",
                "term": "9",
                "accepts_writes": True,
            },
            operation_responses=[
                session,
                unclaimed,
                unclaimed,
                claim,
                claim,
                session,
                {"records": []},
            ],
        )
        client = RegionalStreamClient.with_transports(
            [leader],
            token="secret-token",
            scope=RegionalScope("acme", "shop", "dev", "core"),
        )

        self.assertEqual(
            client.claim_consumer_session(
                "orders", "billing/eu", "member-a", 1, idempotency_key_prefix="claim-cycle-a"
            ),
            (0, 2),
        )
        client.fetch_claimed_group("orders", 2, "billing/eu", "member-a", 1, limit=25)

        expected = [
            (2, "GET", "/groups/billing%2Feu/sessions"),
            (4, "GET", "/groups/billing%2Feu/lag"),
            (6, "GET", "/groups/billing%2Feu/lag"),
            (8, "PUT", "/groups/billing%2Feu/claim"),
            (10, "PUT", "/groups/billing%2Feu/claim"),
            (12, "GET", "/groups/billing%2Feu/sessions"),
            (14, "GET", "/groups/billing%2Feu/claimed-records"),
        ]
        for index, method, suffix in expected:
            request = leader.requests[index]
            self.assertEqual(request["method"], method)
            self.assertTrue(request["path"].endswith(suffix))
        self.assertEqual(
            leader.requests[8]["body"]["idempotency_key"],
            "claim-cycle-a-shard-0-generation-1",
        )
        self.assertEqual(
            leader.requests[14]["query"],
            {"member_id": "member-a", "group_generation": "1", "limit": 25},
        )

    def test_consumer_claim_planner_bridges_only_bounded_monotonic_generations(self) -> None:
        self.assertEqual(
            _claim_generations({"checkpoint": {"exists": True, "group_generation": "1"}}, 3),
            (2, 3),
        )
        self.assertEqual(
            _claim_generations({"checkpoint": {"exists": True, "group_generation": "3"}}, 3),
            (3,),
        )
        with self.assertRaisesRegex(ValueError, "ahead"):
            _claim_generations({"checkpoint": {"exists": True, "group_generation": "4"}}, 3)
        with self.assertRaisesRegex(ValueError, "maximum"):
            _claim_generations({"checkpoint": {"exists": False}}, 4_097)

    def test_consumer_fence_is_preserved_without_routing_rediscovery(self) -> None:
        fenced = EpochAPIError(
            409,
            "fenced",
            "consumer member or session generation is fenced",
            {
                "error": {
                    "code": "fenced",
                    "outcome_certainty": "definite_not_committed",
                }
            },
        )
        leader = RegionalFakeTransport(
            {
                "resource_generation": "2",
                "tablet_epoch": "4",
                "term": "9",
                "accepts_writes": True,
            },
            [fenced],
        )
        client = RegionalStreamClient.with_transports(
            [leader],
            token="secret-token",
            scope=RegionalScope("acme", "shop", "dev", "core"),
        )

        with self.assertRaises(EpochAPIError) as caught:
            client.fetch_claimed_group("orders", 0, "billing", "member-old", 2, limit=1)

        self.assertIs(caught.exception, fenced)
        self.assertEqual((caught.exception.status, caught.exception.code), (409, "fenced"))
        self.assertEqual(len(leader.requests), 2)

    def test_canonical_gzip_batch_keeps_exact_frame_and_identity_across_retry(self) -> None:
        leader = RegionalFakeTransport(
            {
                "resource_generation": "2",
                "tablet_epoch": "4",
                "term": "9",
                "accepts_writes": True,
            },
            [EpochAPIError(409, "not_leader", "leadership changed")],
        )
        client = RegionalStreamClient.with_transports(
            [leader],
            token="secret-token",
            scope=RegionalScope("acme", "shop", "dev", "core"),
        )
        records = [
            StreamBatchRecord(7, self._batch_event("order-7", "customer-7")),
            StreamBatchRecord(8, self._batch_event("order-8", "customer-8")),
        ]
        frame = StreamBatchFrame.encode(records, "gzip")

        client.append_batch("orders", 2, "batch-7", frame)

        self.assertEqual(len(leader.requests), 4)
        first = leader.requests[1]
        second = leader.requests[3]
        self.assertTrue(first["path"].endswith("/records/batches"))
        self.assertEqual(first["body"], second["body"])
        self.assertEqual(first["body"]["idempotency_key"], "batch-7")
        self.assertEqual(first["body"]["compression"], "gzip")
        self.assertEqual(first["body"]["record_count"], 2)
        plain = gzip.decompress(base64.b64decode(first["body"]["payload_base64"], validate=True))
        expected = (
            b'[{"client_sequence":7,"envelope":{"id":"order-7","source":"checkout",'
            b'"type":"order.created","time_ms":42,"key":"customer-7","headers":'
            b'{"a":"first","z":"last"},"content_type":"application/json","payload":'
            b'{"a":7,"z":[{"a":1,"y":2}]},"priority":0,"extensions":{"a":true,'
            b'"z":{"a":1,"b":2}}}},{"client_sequence":8,"envelope":{"id":"order-8",'
            b'"source":"checkout","type":"order.created","time_ms":42,'
            b'"key":"customer-8","headers":{"a":"first","z":"last"},'
            b'"content_type":"application/json","payload":{"a":7,"z":[{"a":1,'
            b'"y":2}]},"priority":0,"extensions":{"a":true,"z":{"a":1,"b":2}}}}]'
        )
        self.assertEqual(plain, expected)

    def test_duplicate_sequences_and_invalid_frames_fail_before_network(self) -> None:
        event = self._batch_event("order-7", "customer-7")
        with self.assertRaisesRegex(ValueError, "duplicate"):
            StreamBatchFrame.encode(
                [StreamBatchRecord(7, event), StreamBatchRecord(7, event)], "none"
            )
        for compression in ("none", "gzip", "lz4", "snappy", "zstd"):
            StreamBatchFrame.from_compressed(compression, 1, 1, b"x")
        with self.assertRaisesRegex(ValueError, "compression"):
            StreamBatchFrame.from_compressed("brotli", 1, 2, b"x")
        self.assertEqual(len(self.leader.requests), 0)

    def test_canonical_batch_json_keeps_serde_compatible_unicode(self) -> None:
        frame = StreamBatchFrame.encode(
            [
                StreamBatchRecord(
                    1,
                    EventEnvelope(
                        id="订单\u2028七",
                        key="東京",
                        source="checkout",
                        event_type="order.created",
                        payload={"message": "<paid>&\u2029"},
                        time_ms=42,
                    ),
                )
            ],
            "none",
        )
        self.assertNotIn(b"\\u2028", frame.compressed)
        self.assertNotIn(b"\\u2029", frame.compressed)
        self.assertIn("订单\u2028七".encode(), frame.compressed)
        self.assertIn("<paid>&\u2029".encode(), frame.compressed)

    @staticmethod
    def _batch_event(event_id: str, key: str) -> EventEnvelope:
        return EventEnvelope(
            id=event_id,
            key=key,
            source="checkout",
            event_type="order.created",
            payload={"z": [{"y": 2, "a": 1}], "a": 7},
            time_ms=42,
            headers={"z": "last", "a": "first"},
            extensions={"z": {"b": 2, "a": 1}, "a": True},
        )

    def test_scope_and_mutation_inputs_fail_before_network(self) -> None:
        with self.assertRaisesRegex(ValueError, "organization"):
            RegionalScope("", "shop", "dev", "core")
        with self.assertRaisesRegex(ValueError, "idempotency"):
            self.client.append(
                "orders",
                0,
                "",
                EventEnvelope(source="checkout", event_type="created", payload={}),
            )

    def test_retention_mutations_and_linearizable_observation(self) -> None:
        self.client.configure_retention(
            "orders",
            0,
            "retention-1",
            StreamRetentionPolicy(
                max_records_per_partition=100,
                max_bytes_per_partition=1_048_576,
                max_age_ms=86_400_000,
            ),
        )
        self.client.maintain_retention("orders", 0, "retention-sweep-1")
        self.client.retention("orders", 0)

        configure = self.leader.requests[1]
        self.assertEqual((configure["method"], configure["path"][-10:]), ("PUT", "/retention"))
        self.assertEqual(
            configure["body"],
            {
                "idempotency_key": "retention-1",
                "expected_term": "8",
                "max_records_per_partition": 100,
                "max_bytes_per_partition": "1048576",
                "max_age_ms": "86400000",
            },
        )
        maintenance = self.leader.requests[3]
        self.assertEqual(
            (maintenance["method"], maintenance["path"][-22:]),
            ("POST", "/retention/maintenance"),
        )
        read = self.leader.requests[5]
        self.assertEqual(read["headers"]["x-epoch-read-consistency"], "linearizable")

    def test_invalid_retention_policy_fails_before_network(self) -> None:
        with self.assertRaisesRegex(ValueError, "max bytes"):
            StreamRetentionPolicy(max_bytes_per_partition=3 * 1024 * 1024 + 1)
        self.assertEqual(len(self.leader.requests), 0)

    def test_retryable_leader_race_rediscovers_without_changing_mutation_identity(self) -> None:
        leader = RegionalFakeTransport(
            {
                "resource_generation": "5",
                "tablet_epoch": "3",
                "term": "8",
                "accepts_writes": True,
            },
            [EpochAPIError(409, "not_leader", "leadership changed")],
        )
        client = RegionalStreamClient.with_transports(
            [leader],
            token="secret-token",
            scope=RegionalScope("acme", "shop", "dev", "core"),
        )
        event = EventEnvelope(
            id="order-42",
            source="checkout",
            event_type="order.created",
            payload={"id": "42"},
            time_ms=42,
        )

        client.append("orders", 0, "append-42", event)

        self.assertEqual(len(leader.requests), 4)
        self.assertEqual(leader.requests[1]["body"]["idempotency_key"], "append-42")
        self.assertEqual(leader.requests[3]["body"]["idempotency_key"], "append-42")

    def test_definitive_discovery_failure_is_preserved(self) -> None:
        denied = DiscoveryErrorTransport(EpochAPIError(403, "forbidden", "scope denied"))
        client = RegionalStreamClient.with_transports(
            [denied, self.leader],
            token="secret-token",
            scope=RegionalScope("acme", "shop", "dev", "core"),
        )

        with self.assertRaises(EpochAPIError) as caught:
            client.fetch("orders", 0, 0, limit=1)

        self.assertEqual((caught.exception.status, caught.exception.code), (403, "forbidden"))
        self.assertEqual(len(denied.requests), 1)
        self.assertEqual(len(self.leader.requests), 0)


if __name__ == "__main__":
    unittest.main()

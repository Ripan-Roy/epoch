from __future__ import annotations

import unittest
from typing import Any

from epoch_sdk import EpochAPIError, EventEnvelope, RegionalScope, RegionalStreamClient


class RegionalFakeTransport:
    def __init__(
        self, route: dict[str, Any], operation_errors: list[EpochAPIError] | None = None
    ) -> None:
        self.route = route
        self.requests: list[dict[str, Any]] = []
        self.operation_errors = list(operation_errors or [])

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
        if method == "GET" and path.endswith("/shards/0"):
            return self.route
        if self.operation_errors:
            raise self.operation_errors.pop(0)
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

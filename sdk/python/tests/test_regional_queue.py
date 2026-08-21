from __future__ import annotations

import unittest
from typing import Any

from epoch_sdk import EventEnvelope, RegionalQueueClient, RegionalScope


class RecordingTransport:
    def __init__(self) -> None:
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
        if method == "GET" and path.endswith("/shards/0"):
            return {
                "resource_generation": "6",
                "tablet_epoch": "4",
                "term": "11",
                "accepts_writes": True,
            }
        return {"state": "committed", "outcome_certainty": "committed"}


class RegionalQueueClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.transport = RecordingTransport()
        self.client = RegionalQueueClient.with_transports(
            [self.transport],
            token="secret-token",
            scope=RegionalScope("acme", "shop", "dev", "core"),
        )

    def test_complete_mutation_and_linearizable_read_contract(self) -> None:
        event = EventEnvelope(
            id="job-42",
            source="checkout",
            event_type="job.created",
            payload={"id": "42"},
            time_ms=42,
        )
        self.client.enqueue(
            "jobs/eu",
            0,
            "enqueue-42",
            event,
            session_id="account-7",
            correlation_id="correlation-42",
            reply_to="reply-temporary",
        )
        self.client.acquire(
            "jobs/eu",
            0,
            "acquire-42",
            consumer="worker-a",
            consumer_epoch=7,
            max_messages=4,
            max_in_flight=2,
            visibility_timeout_ms=5_000,
            session_id="account-7",
        )
        self.client.renew_session_lock(
            "jobs/eu", 0, "renew-session", "worker-a", 7, "session-42", 1_000
        )
        self.client.release_session_lock(
            "jobs/eu", 0, "release-session", "worker-a", 7, "session-42"
        )
        self.client.defer("jobs/eu", 0, "defer-42", "worker-a", 7, "lease-42", "dependency")
        self.client.receive_deferred("jobs/eu", 0, "receive-deferred", "job-42", "worker-a", 7)
        self.client.acknowledge("jobs/eu", 0, "ack-42", "worker-a", 7, "lease-42")
        self.client.extend_lease("jobs/eu", 0, "extend-42", "worker-a", 7, "lease-42", 1_000)
        self.client.release("jobs/eu", 0, "release-42", "worker-a", 7, "lease-42", 50, "retry")
        self.client.nack("jobs/eu", 0, "nack-42", "worker-a", 7, "lease-42", "retry")
        self.client.reject("jobs/eu", 0, "reject-42", "worker-a", 7, "lease-42", "invalid")
        self.client.redrive("jobs/eu", 0, "redrive-42", "job-42", 9)
        self.client.maintain("jobs/eu", 0, "maintain-42")
        self.client.mutation("jobs/eu", 0, 12)
        self.client.counts("jobs/eu", 0)
        self.client.dead_letters("jobs/eu", 0, limit=25)
        self.client.redrives("jobs/eu", 0, limit=25)
        self.client.consumer_flow("jobs/eu", 0, "worker/a")
        self.client.advanced_status("jobs/eu", 0)
        self.client.correlation("jobs/eu", 0, "correlation/42")
        self.client.dead_letter_forwards("jobs/eu", 0, limit=25)
        self.client.status("jobs/eu", 0)

        base = (
            "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/"
            "queues/jobs%2Feu/shards/0"
        )
        operations = self.transport.requests[1::2]
        self.assertEqual(len(operations), 22)
        self.assertEqual(operations[0]["path"], f"{base}/mutations")
        self.assertEqual(operations[0]["body"]["operation"]["session_id"], "account-7")
        self.assertEqual(operations[1]["body"]["operation"]["kind"], "acquire")
        self.assertEqual(operations[1]["body"]["operation"]["consumer_epoch"], "7")
        self.assertEqual(operations[1]["body"]["operation"]["session_id"], "account-7")
        self.assertEqual(operations[13]["path"], f"{base}/mutations/12")
        self.assertEqual(operations[17]["path"], f"{base}/consumers/worker%2Fa/flow")
        self.assertEqual(operations[19]["path"], f"{base}/correlations/correlation%2F42")
        self.assertEqual(operations[20]["path"], f"{base}/dead-letter-forwards")
        for request in operations[13:]:
            self.assertEqual(request["headers"]["x-epoch-read-consistency"], "linearizable")

    def test_invalid_credit_fails_before_network(self) -> None:
        with self.assertRaisesRegex(ValueError, "max messages"):
            self.client.acquire(
                "jobs", 0, "acquire", consumer="worker", consumer_epoch=1, max_messages=0
            )

        with self.assertRaisesRegex(ValueError, "unsigned 64-bit"):
            self.client.acquire(
                "jobs",
                0,
                "acquire-overflow",
                consumer="worker-a",
                consumer_epoch=1 << 64,
                max_messages=1,
            )
        self.assertEqual(self.transport.requests, [])


if __name__ == "__main__":
    unittest.main()

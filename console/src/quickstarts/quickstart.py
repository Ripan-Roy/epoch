import os
import sys

from epoch_sdk import EpochClient, EventEnvelope

STREAM = "orders"
QUEUE = "jobs"


def seed(client: EpochClient) -> None:
    client.create_stream(STREAM, partitions=1, durability="local_durable")
    client.create_queue(QUEUE, durability="local_durable")

    receipt = client.append_stream(
        STREAM,
        EventEnvelope(
            id="order-o-1001",
            source="quickstart",
            event_type="order.created",
            payload={"order_id": "o-1001"},
        ),
    )
    if receipt["acknowledgement"]["durability"] != "local_durable":
        raise RuntimeError(f"unexpected durability receipt: {receipt}")
    records = client.fetch_stream(STREAM, partition=0, offset=0, limit=10)
    print(f"stream records before restart: {len(records)}")

    for job_id in ("job-1001", "job-1002"):
        client.send(
            QUEUE,
            EventEnvelope(
                id=job_id,
                source="quickstart",
                event_type="job.requested",
                payload={"job_id": job_id},
            ),
        )

    deliveries = client.receive(QUEUE, consumer="worker-a", max_messages=1)
    if len(deliveries) != 1:
        raise RuntimeError(f"expected one delivery, got {len(deliveries)}")
    if deliveries[0]["message"]["id"] != "job-1001":
        raise RuntimeError(f"expected job-1001 first: {deliveries}")
    client.acknowledge(QUEUE, deliveries[0]["lease_token"])
    print("acked one job; restart the node now")


def verify(client: EpochClient) -> None:
    records = client.fetch_stream(STREAM, partition=0, offset=0, limit=10)
    if len(records) != 1 or records[0]["envelope"]["id"] != "order-o-1001":
        raise RuntimeError(f"unexpected recovered stream records: {records}")

    counts = client.queue_counts(QUEUE)
    if counts["acknowledged"] != 1:
        raise RuntimeError(f"expected one recovered acknowledgement: {counts}")

    deliveries = client.receive(QUEUE, consumer="worker-b", max_messages=10)
    if len(deliveries) != 1:
        raise RuntimeError(
            f"expected only the unacked job after restart, got {len(deliveries)}"
        )
    if deliveries[0]["message"]["id"] != "job-1002":
        raise RuntimeError(f"expected job-1002 after restart: {deliveries}")
    client.acknowledge(QUEUE, deliveries[0]["lease_token"])
    print("restart verified: stream record and queue settlement survived")


if len(sys.argv) != 2 or sys.argv[1] not in {"seed", "verify"}:
    raise SystemExit("usage: python quickstart.py [seed|verify]")

client = EpochClient(os.environ.get("EPOCH_URL", "http://127.0.0.1:7601"))
seed(client) if sys.argv[1] == "seed" else verify(client)

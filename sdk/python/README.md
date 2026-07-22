# Epoch Python SDK

This is the initial typed Python client for Epoch's provisional native HTTP
surface. It covers health and resource discovery plus every Cache, Stream,
Queue, and Event Bus route currently exposed by the standalone node.

```python
from epoch_sdk import EpochClient, EventEnvelope

client = EpochClient("http://127.0.0.1:7601")
client.create_stream("orders", partitions=4)
receipt = client.append_stream(
    "orders",
    EventEnvelope(source="checkout", event_type="order.created", payload={"id": "1001"}),
)
print(receipt)
```

The runnable node currently accepts only volatile resources. Event Bus filters,
transforms, and targets use typed models; the transport is injectable for fast
contract tests. This package is pre-alpha, is not published, and does not yet
provide native gRPC streaming, automatic retries, or the full Go/Java/Python
DX-001 contract matrix.

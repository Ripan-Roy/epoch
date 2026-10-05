"""Executed by the Go TLS-1.3-only fixture in CI; not a live Catalog quickstart."""

from __future__ import annotations

import os
import sys
from dataclasses import replace

import grpc

from epoch_sdk.management import (
    ManagementClient,
    ManagementConfig,
    ManagementRPCError,
    common,
    messages,
)
from epoch_sdk.transport import TLSConfig


def run(endpoint: str, ca: str, certificate: str, private_key: str, untrusted_ca: str) -> None:
    if not __debug__:
        raise RuntimeError("wire proof requires active Python assertions")
    # These fake environment proxies must not carry the SDK's credential or
    # expand the explicitly configured controller allowlist.
    os.environ.update(
        {
            "https_proxy": "http://127.0.0.1:9",
            "http_proxy": "http://127.0.0.1:9",
            "grpc_proxy": "http://127.0.0.1:9",
        }
    )
    config = ManagementConfig(
        endpoints=(endpoint,),
        bearer_token="sdk-secret",
        timeout=2,
        tls=TLSConfig(root_ca=ca, certificate=certificate, private_key=private_key),
    )
    name = common.ResourceName(
        organization="acme",
        project="shop",
        environment="qa",
        namespace="core",
        kind=common.RESOURCE_KIND_CACHE,
        name="orders",
    )
    spec = common.ResourceSpec(
        workload_profile=common.WORKLOAD_PROFILE_CACHE,
        replicas=3,
        durability=common.DURABILITY_PROFILE_QUORUM_DURABLE,
    )
    with ManagementClient(config) as client:
        applied = client.apply_resource(
            messages.ApplyResourceRequest(
                request_token="python-wire-apply",
                name=name,
                spec=spec,
                expected_generation=0,
            )
        )
        assert applied.response.resource.name == name
        assert applied.info.attempts == 1 and not applied.info.outcome_may_be_unknown
        assert (
            client.get_resource(messages.GetResourceRequest(name=name)).response.resource.name
            == name
        )
        inventory = client.list_resources(
            messages.ListResourcesRequest(
                organization="acme",
                page_size=50,
                tags={"test": "python-wire"},
            )
        )
        assert len(inventory.response.resources) == 1
        # This fixture round-trips an opaque response token. It is serialization
        # evidence, not a claim that the real controller implements pagination.
        assert inventory.response.next_page_token == "opaque-next-page"
        deleted = client.delete_resource(
            messages.DeleteResourceRequest(
                request_token="python-wire-delete",
                name=name,
                expected_generation=0,
            )
        )
        assert deleted.response.generation == (1 << 64) - 1
        batch = client.batch_apply_resources(
            messages.BatchApplyResourcesRequest(
                request_token="python-wire-batch",
                resources=[
                    messages.BatchApplyResource(
                        name=name,
                        spec=spec,
                        expected_generation=0,
                    )
                ],
            )
        )
        assert batch.response.replayed
        operation = client.get_operation(
            messages.GetOperationRequest(
                request_token="python-wire-delete",
                affected_resources=[name],
            )
        ).response
        assert operation.proposal_id == (1 << 64) - 1
        assert operation.HasField("expected_generation") and operation.expected_generation == 0
        with client.watch_resource_changes(
            messages.WatchResourceChangesRequest(
                organization="acme",
                after_cursor=10,
                batch_size=2,
            )
        ) as watch:
            assert watch.recv().next_cursor == 13
            watch.acknowledge(13)
            assert watch.checkpoint == 13
    with ManagementClient(replace(config, bearer_token="wrong-secret")) as client:
        try:
            client.get_resource(messages.GetResourceRequest(name=name))
        except ManagementRPCError as error:
            assert error.code() == grpc.StatusCode.UNAUTHENTICATED
            assert error.info.attempts == 1
            assert "secret" not in str(error)
        else:
            raise AssertionError("wrong bearer identity was accepted")
    with ManagementClient(replace(config, tls=TLSConfig(root_ca=ca))) as client:
        try:
            client.get_resource(messages.GetResourceRequest(name=name))
        except ManagementRPCError as error:
            assert error.code() == grpc.StatusCode.UNAVAILABLE
        else:
            raise AssertionError("mTLS accepted an anonymous client")
    with ManagementClient(
        replace(
            config,
            tls=TLSConfig(
                root_ca=untrusted_ca,
                certificate=certificate,
                private_key=private_key,
            ),
        )
    ) as client:
        try:
            client.get_resource(messages.GetResourceRequest(name=name))
        except ManagementRPCError as error:
            assert error.code() == grpc.StatusCode.UNAVAILABLE
        else:
            raise AssertionError("explicit foreign CA trusted the server identity")
    print(
        "Python management: seven TLS-1.3-only RPCs, mTLS/bearer denial, "
        "uint64/OCC, remote close passed"
    )


if __name__ == "__main__":
    run(*sys.argv[1:])

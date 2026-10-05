"""Compile-only management reference; use a dedicated test resource, never production.

Pass an application-owned callback that processes a page and durably persists
its scanned next_cursor before returning. The SDK cannot make external effects
transactional. Controllers must retain Epoch's enforced TLS 1.3 server policy.
"""

import os
from collections.abc import Callable

from epoch_sdk.management import (
    ManagementClient,
    ManagementConfig,
    ManagementContext,
    ManagementRPCError,
    common,
    messages,
)
from epoch_sdk.transport import TLSConfig


def demonstrate(
    process_and_checkpoint: Callable[[messages.WatchResourceChangesResponse], None],
) -> None:
    token = os.environ["EPOCH_EXAMPLE_REQUEST_TOKEN"]
    if not token or token.strip() != token or len(token.encode("utf-8")) > 128:
        raise ValueError("supply a canonical stable example token of at most 128 bytes")
    configuration = ManagementConfig(
        endpoints=(os.environ.get("EPOCH_CONTROL_GRPC_AUTHORITY", "localhost:8081"),),
        bearer_token=os.environ["EPOCH_BEARER_TOKEN"],
        timeout=5,
        tls=TLSConfig(
            root_ca=os.environ["EPOCH_TLS_CA_PATH"],
            certificate=os.environ["EPOCH_TLS_CERT_PATH"],
            private_key=os.environ["EPOCH_TLS_KEY_PATH"],
        ),
    )
    identity = common.ResourceName(
        organization="acme",
        project="payments",
        environment="production",
        namespace="orders",
        kind=common.RESOURCE_KIND_CACHE,
        name="sdk-management-example",
    )
    spec = common.ResourceSpec(
        workload_profile=common.WORKLOAD_PROFILE_CACHE,
        durability=common.DURABILITY_PROFILE_QUORUM_DURABLE,
        replicas=3,
        governance=common.ResourceGovernance(
            owner="team:sdk-example",
            cost_center="cc-sdk",
            classification=common.DATA_CLASSIFICATION_INTERNAL,
        ),
        placement=common.PlacementPolicy(
            allowed_regions=["ap-south"],
            minimum_zones=3,
            required_node_class="general-purpose",
        ),
    )
    spec.configuration.update({"shard_count": 1})
    context = ManagementContext(timeout=30)
    with ManagementClient(configuration) as client:
        try:
            client.batch_apply_resources(
                messages.BatchApplyResourcesRequest(
                    request_token=token,
                    resources=[
                        messages.BatchApplyResource(
                            name=identity,
                            spec=spec,
                            expected_generation=0,
                        )
                    ],
                ),
                context=context,
            )
        except ManagementRPCError as error:
            print(
                f"attempts={error.info.attempts}; "
                f"outcome unknown={error.info.outcome_may_be_unknown}"
            )
            if error.info.outcome_may_be_unknown:
                # Resolve the original token and complete scope. NotFound does
                # not prove non-commit; no new write/token is invented here.
                client.get_operation(
                    messages.GetOperationRequest(
                        request_token=token,
                        affected_resources=[identity],
                    ),
                    context=ManagementContext(timeout=5),
                )
            raise
        operation = client.get_operation(
            messages.GetOperationRequest(
                request_token=token,
                affected_resources=[identity],
            ),
            context=context,
        ).response
        current = client.get_resource(
            messages.GetResourceRequest(name=identity), context=context
        ).response.resource
        client.list_resources(
            messages.ListResourcesRequest(
                organization=identity.organization,
                project=identity.project,
                environment=identity.environment,
                namespace=identity.namespace,
                page_size=50,
            ),
            context=context,
        )  # One bounded page, not a complete inventory.
        client.apply_resource(
            messages.ApplyResourceRequest(
                request_token=token + "-apply",
                name=identity,
                spec=spec,
                expected_generation=current.generation,
            ),
            context=context,
        )
        with client.watch_resource_changes(
            messages.WatchResourceChangesRequest(
                after_cursor=operation.last_change_cursor,
                batch_size=2,
                organization=identity.organization,
                project=identity.project,
                environment=identity.environment,
                namespace=identity.namespace,
            ),
            context=context,
        ) as watch:
            page = watch.recv()
            scanned = page.next_cursor
            process_and_checkpoint(page)  # Includes empty filtered pages.
            watch.acknowledge(scanned)
        # Cleanup is restricted to the dedicated example-owned generation.
        client.delete_resource(
            messages.DeleteResourceRequest(
                request_token=token + "-delete",
                name=identity,
                expected_generation=current.generation,
            ),
            context=context,
        )

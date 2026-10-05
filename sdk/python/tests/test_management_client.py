"""Public management SDK over real generated gRPC, not Catalog certification."""

import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event, Lock, Thread
from time import sleep
from unittest.mock import patch

import grpc

from epoch_sdk._generated.epoch.v1 import regional_admin_pb2_grpc as service
from epoch_sdk.management import (
    ManagementClient,
    ManagementConfig,
    ManagementContext,
    ManagementRPCError,
    common,
    messages,
)


def name(value="orders"):
    return common.ResourceName(
        organization="acme",
        project="payments",
        environment="test",
        namespace="team",
        kind=common.RESOURCE_KIND_CACHE,
        name=value,
    )


def resource(identity=None):
    return common.Resource(
        name=identity or name(),
        generation=1,
        spec=common.ResourceSpec(
            governance=common.ResourceGovernance(
                owner="sdk-team",
                cost_center="qa",
                classification=common.DATA_CLASSIFICATION_INTERNAL,
                tags={"empty": "", "test": "management"},
            )
        ),
    )


def apply_request():
    return messages.ApplyResourceRequest(
        request_token="original-apply",
        name=name(),
        spec=resource().spec,
        expected_generation=0,
    )


def page(after=10, next_cursor=13):
    return messages.WatchResourceChangesResponse(
        earliest_cursor=1,
        latest_cursor=20,
        next_cursor=next_cursor,
        changes=[
            messages.ResourceChange(
                cursor=after + 1,
                name=name(),
                generation=1,
                kind=messages.RESOURCE_CHANGE_KIND_DESIRED_APPLIED,
            )
        ],
    )


class Fixture(service.RegionalAdminServiceServicer):
    def __init__(self):
        self.handlers = {}
        self.calls = []
        self.lock = Lock()
        self.started = Event()
        self.cancelled = Event()

    def dispatch(self, method, request, context, default):
        with self.lock:
            self.calls.append((method, request, tuple(context.invocation_metadata())))
        if [value for key, value in context.invocation_metadata() if key == "authorization"] != [
            "Bearer sdk-secret"
        ]:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "secret-backend-detail")
        handler = self.handlers.get(method)
        return handler(request, context) if handler else default(request)

    def ApplyResource(self, request, context):
        return self.dispatch(
            "apply",
            request,
            context,
            lambda request: messages.ApplyResourceResponse(
                resource=resource(request.name), created=True, changed=True
            ),
        )

    def GetResource(self, request, context):
        return self.dispatch(
            "get",
            request,
            context,
            lambda request: messages.GetResourceResponse(resource=resource(request.name)),
        )

    def ListResources(self, request, context):
        return self.dispatch(
            "list",
            request,
            context,
            lambda request: messages.ListResourcesResponse(resources=[resource()]),
        )

    def DeleteResource(self, request, context):
        return self.dispatch(
            "delete",
            request,
            context,
            lambda request: messages.DeleteResourceResponse(
                name=request.name, generation=(1 << 64) - 1, deleted=True
            ),
        )

    def BatchApplyResources(self, request, context):
        return self.dispatch(
            "batch",
            request,
            context,
            lambda request: messages.BatchApplyResourcesResponse(
                results=[
                    messages.ApplyResourceResponse(
                        resource=resource(item.name),
                        created=True,
                        changed=True,
                        replayed=True,
                    )
                    for item in reversed(request.resources)
                ],
                replayed=True,
            ),
        )

    def GetOperation(self, request, context):
        return self.dispatch(
            "operation",
            request,
            context,
            lambda request: messages.GetOperationResponse(
                request_token=request.request_token,
                proposal_id=(1 << 64) - 1,
                state=messages.OPERATION_STATE_SUCCEEDED,
                affected_resources=request.affected_resources,
                command_kind="delete_managed",
                expected_generation=0,
            ),
        )

    def WatchResourceChanges(self, request, context):
        return self.dispatch(
            "watch", request, context, lambda request: iter([page(request.after_cursor)])
        )


@contextmanager
def server(fixture):
    executor = ThreadPoolExecutor(max_workers=8)
    instance = grpc.server(executor)
    service.add_RegionalAdminServiceServicer_to_server(fixture, instance)
    port = instance.add_insecure_port("127.0.0.1:0")
    instance.start()
    try:
        yield f"127.0.0.1:{port}"
    finally:
        instance.stop(0).wait(2)
        executor.shutdown(wait=True)


def config(*endpoints, timeout=2):
    return ManagementConfig(
        endpoints=tuple(endpoints),
        bearer_token="sdk-secret",
        timeout=timeout,
        allow_insecure_loopback=True,
    )


class ManagementClientTest(unittest.TestCase):
    def test_all_seven_generated_methods_and_uint64_presence(self):
        fixture = Fixture()
        with server(fixture) as endpoint, ManagementClient(config(endpoint)) as client:
            original = apply_request()
            result = client.apply_resource(original)
            self.assertEqual(result.response.resource.name, original.name)
            self.assertEqual((result.info.attempts, result.info.budget), (1, 1))
            self.assertFalse(result.info.outcome_may_be_unknown)
            self.assertEqual(
                client.get_resource(
                    messages.GetResourceRequest(name=name())
                ).response.resource.name,
                name(),
            )
            inventory = client.list_resources(
                messages.ListResourcesRequest(
                    organization="acme",
                    owner="sdk-team",
                    cost_center="qa",
                    classification=common.DATA_CLASSIFICATION_INTERNAL,
                    tags={"empty": ""},
                )
            )
            self.assertEqual(len(inventory.response.resources), 1)
            for expected in (None, 0, (1 << 64) - 1):
                request = messages.DeleteResourceRequest(
                    request_token="original-delete", name=name()
                )
                if expected is not None:
                    request.expected_generation = expected
                self.assertEqual(client.delete_resource(request).response.generation, (1 << 64) - 1)
                actual = fixture.calls[-1][1]
                self.assertEqual(actual.HasField("expected_generation"), expected is not None)
                self.assertEqual(actual, request)
            batch = messages.BatchApplyResourcesRequest(
                request_token="original-batch",
                resources=[
                    messages.BatchApplyResource(
                        name=name("a"), spec=resource().spec, expected_generation=0
                    ),
                    messages.BatchApplyResource(name=name("b"), spec=resource().spec),
                ],
            )
            self.assertTrue(client.batch_apply_resources(batch).response.replayed)
            operation = client.get_operation(
                messages.GetOperationRequest(
                    request_token="original-delete",
                    affected_resources=[name()],
                )
            ).response
            self.assertEqual(operation.proposal_id, (1 << 64) - 1)
            self.assertTrue(operation.HasField("expected_generation"))
            self.assertEqual(operation.expected_generation, 0)
            with client.watch_resource_changes(
                messages.WatchResourceChangesRequest(
                    after_cursor=10,
                    organization="acme",
                    batch_size=2,
                )
            ) as watch:
                self.assertEqual(watch.recv().next_cursor, 13)
                watch.acknowledge(13)
                self.assertEqual(watch.checkpoint, 13)
            self.assertEqual(
                {call[0] for call in fixture.calls},
                {
                    "apply",
                    "get",
                    "list",
                    "delete",
                    "batch",
                    "operation",
                    "watch",
                },
            )

    def test_unavailable_failover_retains_original_request_and_metadata(self):
        first, second = Fixture(), Fixture()

        def unavailable(request, context):
            request.request_token = "transport-mutation"
            request.ClearField("expected_generation")
            context.abort(grpc.StatusCode.UNAVAILABLE, "sdk-secret secret-backend-detail")

        first.handlers["apply"] = unavailable
        with server(first) as a, server(second) as b, ManagementClient(config(a, b)) as client:
            original = apply_request()
            frozen = original.SerializeToString()
            result = client.apply_resource(
                original,
                metadata=(
                    ("authorization", "Bearer conflicting-secret"),
                    ("x-request-id", "trace-one"),
                ),
            )
            self.assertEqual(original.SerializeToString(), frozen)
            self.assertEqual(second.calls[0][1], original)
            self.assertEqual(result.info.attempts, 2)
            self.assertFalse(result.info.outcome_may_be_unknown)
            for fixture in (first, second):
                metadata = fixture.calls[0][2]
                self.assertEqual(
                    [value for key, value in metadata if key == "authorization"],
                    ["Bearer sdk-secret"],
                )
                self.assertIn(("x-request-id", "trace-one"), metadata)

    def test_semantic_failures_are_not_retried_and_mutation_outcome_is_unknown(self):
        first, second = Fixture(), Fixture()
        with server(first) as a, server(second) as b, ManagementClient(config(a, b)) as client:
            for status in (
                grpc.StatusCode.ABORTED,
                grpc.StatusCode.PERMISSION_DENIED,
                grpc.StatusCode.INVALID_ARGUMENT,
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                grpc.StatusCode.UNAUTHENTICATED,
            ):
                with self.subTest(status=status):

                    def reject(request, context, status=status):
                        context.abort(status, "sdk-secret secret-backend-detail")

                    first.handlers["apply"] = reject
                    with self.assertRaises(ManagementRPCError) as caught:
                        client.apply_resource(apply_request())
                    error = caught.exception
                    self.assertEqual(error.code(), status)
                    self.assertTrue(error.info.outcome_may_be_unknown)
                    self.assertEqual((error.info.attempts, error.info.budget), (1, 2))
                    self.assertIsInstance(error.cause, grpc.RpcError)
                    self.assertNotIn("secret", str(error))
                    self.assertNotIn("secret", repr(error))
            self.assertEqual(second.calls, [])

    def test_one_deadline_covers_whole_failover_sequence(self):
        first, second = Fixture(), Fixture()
        observed = []

        def delayed(request, context):
            observed.append(context.time_remaining())
            sleep(0.15)
            context.abort(grpc.StatusCode.UNAVAILABLE, "lost response")

        def finish(request, context):
            observed.append(context.time_remaining())
            return messages.ApplyResourceResponse(resource=resource(request.name))

        first.handlers["apply"], second.handlers["apply"] = delayed, finish
        with (
            server(first) as a,
            server(second) as b,
            ManagementClient(config(a, b, timeout=1)) as client,
        ):
            client.apply_resource(apply_request(), context=ManagementContext(timeout=0.7))
        self.assertEqual(len(observed), 2)
        self.assertLess(observed[1], observed[0] - 0.09)
        self.assertLessEqual(observed[0], 0.72)

    def test_expired_deadline_is_local_but_dispatched_deadline_is_unknown(self):
        first, second = Fixture(), Fixture()

        def block(request, context):
            ended = Event()
            context.add_callback(ended.set)
            first.started.set()
            ended.wait(2)
            context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, "deadline")

        first.handlers["apply"] = block
        with server(first) as a, server(second) as b, ManagementClient(config(a, b)) as client:
            with self.assertRaises(ManagementRPCError) as caught:
                client.apply_resource(apply_request(), context=ManagementContext(timeout=0))
            self.assertEqual(caught.exception.code(), grpc.StatusCode.DEADLINE_EXCEEDED)
            self.assertEqual(caught.exception.info.attempts, 0)
            self.assertFalse(caught.exception.info.outcome_may_be_unknown)
            self.assertEqual(first.calls, [])
            with self.assertRaises(ManagementRPCError) as caught:
                client.apply_resource(apply_request(), context=ManagementContext(timeout=0.1))
            self.assertEqual(caught.exception.code(), grpc.StatusCode.DEADLINE_EXCEEDED)
            self.assertEqual(caught.exception.info.attempts, 1)
            self.assertTrue(caught.exception.info.outcome_may_be_unknown)
            self.assertTrue(first.started.is_set())
            self.assertEqual(second.calls, [])

    def test_endpoint_exhaustion_is_bounded_and_metadata_rejection_is_local(self):
        first, second = Fixture(), Fixture()

        def unavailable(request, context):
            context.abort(grpc.StatusCode.UNAVAILABLE, "backend-secret")

        first.handlers["apply"] = second.handlers["apply"] = unavailable
        with server(first) as a, server(second) as b, ManagementClient(config(a, b)) as client:
            for metadata in (
                (("bad key", "value"),),
                (("safe", "secret\n"),),
                (("safe-bin", "not-bytes"),),
            ):
                with (
                    self.subTest(metadata=metadata),
                    self.assertRaises(ManagementRPCError) as caught,
                ):
                    client.apply_resource(apply_request(), metadata=metadata)
                self.assertEqual(caught.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)
                self.assertFalse(caught.exception.info.outcome_may_be_unknown)
            self.assertEqual(first.calls + second.calls, [])
            with self.assertRaises(ManagementRPCError) as caught:
                client.apply_resource(apply_request(), metadata=(("trace-bin", b"\x00\xff"),))
            self.assertEqual(caught.exception.code(), grpc.StatusCode.UNAVAILABLE)
            self.assertEqual((caught.exception.info.attempts, caught.exception.info.budget), (2, 2))
            self.assertTrue(caught.exception.info.outcome_may_be_unknown)
            self.assertEqual(len(first.calls), 1)
            self.assertEqual(len(second.calls), 1)
            self.assertEqual(first.calls[0][1], second.calls[0][1])
            self.assertIn(("trace-bin", b"\x00\xff"), first.calls[0][2])

    def test_pre_dispatch_cancellation_is_known_and_post_dispatch_cancellation_is_unknown(self):
        fixture = Fixture()

        def block(request, context):
            context.add_callback(fixture.cancelled.set)
            fixture.started.set()
            if not fixture.cancelled.wait(2):
                context.abort(grpc.StatusCode.INTERNAL, "test cancellation did not arrive")
            context.abort(grpc.StatusCode.CANCELLED, "cancelled")

        fixture.handlers["apply"] = block
        with server(fixture) as endpoint, ManagementClient(config(endpoint)) as client:
            cancelled = ManagementContext()
            cancelled.cancel()
            with self.assertRaises(ManagementRPCError) as caught:
                client.apply_resource(apply_request(), context=cancelled)
            self.assertEqual(caught.exception.code(), grpc.StatusCode.CANCELLED)
            self.assertEqual(caught.exception.info.attempts, 0)
            self.assertFalse(caught.exception.info.outcome_may_be_unknown)
            self.assertEqual(fixture.calls, [])
            active = ManagementContext()
            failures = []

            def call():
                try:
                    client.apply_resource(apply_request(), context=active)
                except ManagementRPCError as failure:
                    failures.append(failure)

            thread = Thread(target=call)
            thread.start()
            self.assertTrue(fixture.started.wait(2))
            active.cancel()
            thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertTrue(fixture.cancelled.wait(2))
            self.assertEqual(len(failures), 1)
            self.assertEqual(failures[0].code(), grpc.StatusCode.CANCELLED)
            self.assertTrue(failures[0].info.outcome_may_be_unknown)

    def test_request_validation_rejects_before_dispatch(self):
        fixture = Fixture()
        with server(fixture) as endpoint, ManagementClient(config(endpoint)) as client:
            cases = [
                (client.apply_resource, messages.ApplyResourceRequest()),
                (
                    client.apply_resource,
                    messages.ApplyResourceRequest(
                        request_token=" t ", name=name(), spec=resource().spec
                    ),
                ),
                (client.get_resource, messages.GetResourceRequest(name=name("a/b"))),
                (client.list_resources, messages.ListResourcesRequest(page_size=101)),
                (client.list_resources, messages.ListResourcesRequest(kind=999)),
                (client.list_resources, messages.ListResourcesRequest(classification=999)),
                (
                    client.batch_apply_resources,
                    messages.BatchApplyResourcesRequest(request_token="t"),
                ),
                (
                    client.batch_apply_resources,
                    messages.BatchApplyResourcesRequest(
                        request_token="t",
                        resources=[
                            messages.BatchApplyResource(name=name(), spec=resource().spec),
                            messages.BatchApplyResource(name=name(), spec=resource().spec),
                        ],
                    ),
                ),
                (client.get_operation, messages.GetOperationRequest(request_token="t")),
                (
                    client.delete_resource,
                    messages.DeleteResourceRequest(request_token="", name=name()),
                ),
                (
                    client.watch_resource_changes,
                    messages.WatchResourceChangesRequest(batch_size=1001),
                ),
            ]
            for call, request in cases:
                with self.subTest(call=call.__name__, request=request):
                    with self.assertRaises(ManagementRPCError) as caught:
                        call(request)
                    self.assertEqual(caught.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)
                    self.assertEqual(caught.exception.info.attempts, 0)
            self.assertEqual(fixture.calls, [])

    def test_bad_receipts_never_trigger_another_mutation_attempt(self):
        first, second = Fixture(), Fixture()
        with server(first) as a, server(second) as b, ManagementClient(config(a, b)) as client:
            responses = [
                messages.ApplyResourceResponse(),
                messages.ApplyResourceResponse(resource=resource(name("foreign"))),
                messages.ApplyResourceResponse(
                    resource=common.Resource(name=name(), spec=resource().spec)
                ),
                messages.ApplyResourceResponse(resource=common.Resource(name=name(), generation=1)),
            ]
            for response in responses:
                first.handlers["apply"] = lambda request, context, response=response: response
                with (
                    self.subTest(response=response),
                    self.assertRaises(ManagementRPCError) as caught,
                ):
                    client.apply_resource(apply_request())
                self.assertEqual(caught.exception.code(), grpc.StatusCode.DATA_LOSS)
                self.assertTrue(caught.exception.info.outcome_may_be_unknown)
            self.assertEqual(second.calls, [])

    def test_inventory_rejects_every_foreign_filter_duplicate_and_excess(self):
        fixture = Fixture()
        request = messages.ListResourcesRequest(
            organization="acme",
            project="payments",
            environment="test",
            namespace="team",
            kind=common.RESOURCE_KIND_CACHE,
            page_size=1,
            owner="sdk-team",
            cost_center="qa",
            classification=common.DATA_CLASSIFICATION_INTERNAL,
            tags={"empty": ""},
        )
        invalid = []
        for field in ("organization", "project", "environment", "namespace", "kind"):
            item = resource()
            setattr(item.name, field, common.RESOURCE_KIND_QUEUE if field == "kind" else "foreign")
            invalid.append([item])
        for field in ("owner", "cost_center", "classification"):
            item = resource()
            setattr(
                item.spec.governance,
                field,
                common.DATA_CLASSIFICATION_PUBLIC if field == "classification" else "foreign",
            )
            invalid.append([item])
        item = resource()
        del item.spec.governance.tags["empty"]
        invalid.append([item])
        invalid.extend([[resource(), resource()], [resource(name("a")), resource(name("b"))]])
        with server(fixture) as endpoint, ManagementClient(config(endpoint)) as client:
            for items in invalid:
                fixture.handlers["list"] = lambda request, context, items=items: (
                    messages.ListResourcesResponse(resources=items)
                )
                with self.subTest(items=items), self.assertRaises(ManagementRPCError) as caught:
                    client.list_resources(request)
                self.assertEqual(caught.exception.code(), grpc.StatusCode.DATA_LOSS)
                self.assertFalse(caught.exception.info.outcome_may_be_unknown)
            request.page_size = 2
            fixture.handlers["list"] = lambda request, context: messages.ListResourcesResponse(
                resources=[resource(), resource()]
            )
            with self.assertRaises(ManagementRPCError):
                client.list_resources(request)

    def test_batch_and_operation_require_exact_scopes_and_outcomes(self):
        fixture = Fixture()
        batch = messages.BatchApplyResourcesRequest(
            request_token="t",
            resources=[
                messages.BatchApplyResource(name=name("a"), spec=resource().spec),
                messages.BatchApplyResource(name=name("b"), spec=resource().spec),
            ],
        )
        operation = messages.GetOperationRequest(
            request_token="t", affected_resources=[name("a"), name("b")]
        )
        with server(fixture) as endpoint, ManagementClient(config(endpoint)) as client:
            bad_batches = [
                messages.BatchApplyResourcesResponse(),
                messages.BatchApplyResourcesResponse(
                    results=[messages.ApplyResourceResponse(resource=resource(name("a")))] * 2
                ),
                messages.BatchApplyResourcesResponse(
                    results=[
                        messages.ApplyResourceResponse(resource=resource(item.name))
                        for item in batch.resources
                    ],
                    replayed=True,
                ),
            ]
            for response in bad_batches:
                fixture.handlers["batch"] = lambda request, context, response=response: response
                with self.assertRaises(ManagementRPCError) as caught:
                    client.batch_apply_resources(batch)
                self.assertEqual(caught.exception.code(), grpc.StatusCode.DATA_LOSS)
            valid = messages.GetOperationResponse(
                request_token="t",
                proposal_id=7,
                state=messages.OPERATION_STATE_SUCCEEDED,
                affected_resources=operation.affected_resources,
                command_kind="batch_apply_desired",
                first_change_cursor=1,
                last_change_cursor=2,
            )
            bad_operations = []
            for field, value in (
                ("request_token", "foreign"),
                ("proposal_id", 0),
                ("state", 999),
                ("command_kind", ""),
                ("last_change_cursor", 0),
                ("first_change_cursor", 0),
                ("failure_code", "foreign"),
            ):
                invalid = messages.GetOperationResponse()
                invalid.CopyFrom(valid)
                setattr(invalid, field, value)
                bad_operations.append(invalid)
            invalid = messages.GetOperationResponse()
            invalid.CopyFrom(valid)
            invalid.affected_resources[1].CopyFrom(name("foreign"))
            bad_operations.append(invalid)
            invalid = messages.GetOperationResponse()
            invalid.CopyFrom(valid)
            invalid.state = messages.OPERATION_STATE_FAILED
            bad_operations.append(invalid)
            for response in bad_operations:
                fixture.handlers["operation"] = lambda request, context, response=response: response
                with (
                    self.subTest(response=response),
                    self.assertRaises(ManagementRPCError) as caught,
                ):
                    client.get_operation(operation)
                self.assertEqual(caught.exception.code(), grpc.StatusCode.DATA_LOSS)

    def test_configuration_validates_entire_allowlist_before_creating_channels(self):
        invalid = [
            (),
            ("remote.test:8081",),
            ("http://127.0.0.1:1",),
            ("127.0.0.1:01",),
            ("127.0.0.1:0",),
            ("127.0.0.1:65536",),
            ("127.0.0.1:1/path",),
            ("127.0.0.1:1", "127.0.0.1:1"),
            ("127.0.0.1:1", "remote.test:2"),
        ]
        with patch("grpc.insecure_channel") as create:
            for endpoints in invalid:
                with self.subTest(endpoints=endpoints), self.assertRaises(ValueError):
                    ManagementClient(config(*endpoints))
            for timeout in (0, -1, float("nan"), float("inf"), True):
                with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                    ManagementClient(config("127.0.0.1:1", timeout=timeout))
            for token in ("", " ", "secret\n", "x" * 4097, "☃"):
                with self.subTest(token=token[:20]), self.assertRaises(ValueError) as caught:
                    ManagementClient(
                        ManagementConfig(
                            endpoints=("127.0.0.1:1",),
                            bearer_token=token,
                            timeout=1,
                            allow_insecure_loopback=True,
                        )
                    )
                self.assertNotIn("secret", str(caught.exception))
            create.assert_not_called()

    def test_close_is_idempotent_and_client_rejects_further_dispatch(self):
        fixture = Fixture()
        with server(fixture) as endpoint:
            client = ManagementClient(config(endpoint))
            client.close()
            client.close()
            with self.assertRaises(ManagementRPCError) as caught:
                client.apply_resource(apply_request())
            self.assertEqual(caught.exception.info.attempts, 0)
            self.assertFalse(caught.exception.info.outcome_may_be_unknown)
            self.assertEqual(fixture.calls, [])


class ManagementWatchTest(unittest.TestCase):
    def test_unavailable_reconnect_preserves_filters_and_does_not_reset_cursor(self):
        first, second = Fixture(), Fixture()

        def unavailable(request, context):
            context.abort(grpc.StatusCode.UNAVAILABLE, "lost stream")

        first.handlers["watch"] = unavailable
        with server(first) as a, server(second) as b, ManagementClient(config(a, b)) as client:
            request = messages.WatchResourceChangesRequest(
                after_cursor=10,
                organization="acme",
                project="payments",
                environment="test",
                namespace="team",
                kind=common.RESOURCE_KIND_CACHE,
            )
            with client.watch_resource_changes(request) as watch:
                self.assertEqual(watch.recv().next_cursor, 13)
                self.assertEqual(watch.info.attempts, 2)
                self.assertEqual(watch.checkpoint, 10)
                self.assertEqual(first.calls[0][1], request)
                self.assertEqual(second.calls[0][1], request)

    def test_empty_catalog_and_uint64_extreme_pages_preserve_exact_checkpoint(self):
        fixture = Fixture()
        maximum = (1 << 64) - 1
        with server(fixture) as endpoint, ManagementClient(config(endpoint)) as client:
            for after, response in (
                (
                    0,
                    messages.WatchResourceChangesResponse(
                        earliest_cursor=1,
                        latest_cursor=0,
                        next_cursor=0,
                    ),
                ),
                (
                    maximum - 1,
                    messages.WatchResourceChangesResponse(
                        earliest_cursor=maximum,
                        latest_cursor=maximum,
                        next_cursor=maximum,
                        changes=[
                            messages.ResourceChange(
                                cursor=maximum,
                                name=name(),
                                generation=maximum,
                                kind=messages.RESOURCE_CHANGE_KIND_DESIRED_APPLIED,
                            )
                        ],
                    ),
                ),
            ):
                fixture.handlers["watch"] = lambda request, context, response=response: iter(
                    [response]
                )
                with client.watch_resource_changes(
                    messages.WatchResourceChangesRequest(after_cursor=after)
                ) as watch:
                    received = watch.recv()
                    self.assertEqual(received, response)
                    watch.acknowledge(received.next_cursor)
                    self.assertEqual(watch.checkpoint, response.next_cursor)

    def test_parent_cancellation_and_deadline_before_stream_dispatch(self):
        fixture = Fixture()
        with server(fixture) as endpoint, ManagementClient(config(endpoint)) as client:
            for context, expected in (
                (ManagementContext(timeout=0), grpc.StatusCode.DEADLINE_EXCEEDED),
                (ManagementContext(), grpc.StatusCode.CANCELLED),
            ):
                if expected == grpc.StatusCode.CANCELLED:
                    context.cancel()
                with client.watch_resource_changes(
                    messages.WatchResourceChangesRequest(), context=context
                ) as watch:
                    with self.assertRaises(ManagementRPCError) as caught:
                        watch.recv()
                    self.assertEqual(caught.exception.code(), expected)
                    self.assertEqual(watch.info.attempts, 0)
            self.assertEqual(fixture.calls, [])

    def test_acknowledged_scanned_cursor_drives_reconnect_not_visible_or_latest_cursor(self):
        first, second = Fixture(), Fixture()
        second.handlers["watch"] = lambda request, context: iter(
            [
                messages.WatchResourceChangesResponse(
                    earliest_cursor=1, latest_cursor=20, next_cursor=17
                )
            ]
        )
        request = messages.WatchResourceChangesRequest(
            after_cursor=10, organization="acme", batch_size=2
        )
        with server(first) as a, server(second) as b, ManagementClient(config(a, b)) as client:
            with client.watch_resource_changes(request) as watch:
                self.assertEqual(first.calls, [])
                result = watch.recv()
                self.assertEqual(watch.checkpoint, 10)
                self.assertEqual(watch.info.attempts, 1)
                with self.assertRaises(ManagementRPCError) as caught:
                    watch.recv()
                self.assertEqual(caught.exception.code(), grpc.StatusCode.FAILED_PRECONDITION)
                for invalid in (11, 20, True):
                    with self.assertRaises(ManagementRPCError):
                        watch.acknowledge(invalid)
                result.next_cursor = 99
                watch.acknowledge(13)
                self.assertEqual(watch.checkpoint, 13)
                self.assertEqual(watch.recv().next_cursor, 17)
                self.assertEqual(second.calls[0][1].after_cursor, 13)
                self.assertEqual(second.calls[0][1].organization, "acme")
                self.assertEqual(watch.info.attempts, 2)
                watch.acknowledge(17)
                with self.assertRaises(ManagementRPCError) as caught:
                    watch.recv()
                self.assertEqual(caught.exception.code(), grpc.StatusCode.UNAVAILABLE)
                self.assertEqual(watch.checkpoint, 17)
                self.assertEqual(watch.info.attempts, 2)
            self.assertEqual(request.after_cursor, 10)

    def test_empty_filtered_page_still_needs_scanned_checkpoint(self):
        fixture = Fixture()
        fixture.handlers["watch"] = lambda request, context: iter(
            [
                messages.WatchResourceChangesResponse(
                    earliest_cursor=1, latest_cursor=100, next_cursor=50
                )
            ]
        )
        with (
            server(fixture) as endpoint,
            ManagementClient(config(endpoint)) as client,
            client.watch_resource_changes(messages.WatchResourceChangesRequest()) as watch,
        ):
            self.assertEqual(len(watch.recv().changes), 0)
            self.assertEqual(watch.checkpoint, 0)
            watch.acknowledge(50)
            self.assertEqual(watch.checkpoint, 50)

    def test_bad_pages_do_not_advance_checkpoint_or_reconnect(self):
        first, second = Fixture(), Fixture()
        invalid = []
        for field, value in (
            ("earliest_cursor", 0),
            ("earliest_cursor", 12),
            ("earliest_cursor", 30),
            ("next_cursor", 9),
            ("next_cursor", 21),
        ):
            item = page()
            setattr(item, field, value)
            invalid.append(item)
        for field, value in (("cursor", 10), ("cursor", 14), ("generation", 0), ("kind", 999)):
            item = page()
            setattr(item.changes[0], field, value)
            invalid.append(item)
        item = page()
        item.changes[0].name.organization = "foreign"
        invalid.append(item)
        item = page()
        item.changes.append(item.changes[0])
        invalid.append(item)
        with server(first) as a, server(second) as b, ManagementClient(config(a, b)) as client:
            for item in invalid:
                first.handlers["watch"] = lambda request, context, item=item: iter([item])
                with client.watch_resource_changes(
                    messages.WatchResourceChangesRequest(after_cursor=10, organization="acme")
                ) as watch:
                    with self.subTest(item=item), self.assertRaises(ManagementRPCError) as caught:
                        watch.recv()
                    self.assertEqual(caught.exception.code(), grpc.StatusCode.DATA_LOSS)
                    self.assertEqual(watch.checkpoint, 10)
                    self.assertEqual(watch.info.attempts, 1)
            self.assertEqual(second.calls, [])

    def test_stale_cursor_is_not_retried_or_reset(self):
        first, second = Fixture(), Fixture()

        def stale(request, context):
            context.abort(grpc.StatusCode.ABORTED, "retention floor advanced")

        first.handlers["watch"] = stale
        with server(first) as a, server(second) as b, ManagementClient(config(a, b)) as client:
            with client.watch_resource_changes(
                messages.WatchResourceChangesRequest(after_cursor=10)
            ) as watch:
                with self.assertRaises(ManagementRPCError) as caught:
                    watch.recv()
                self.assertEqual(caught.exception.code(), grpc.StatusCode.ABORTED)
                self.assertEqual(watch.checkpoint, 10)
            self.assertEqual(second.calls, [])

    def test_close_cancels_remote_stream_without_cancelling_shared_context(self):
        fixture = Fixture()

        def persistent(request, context):
            context.add_callback(fixture.cancelled.set)
            yield page()
            fixture.started.set()
            fixture.cancelled.wait(2)

        fixture.handlers["watch"] = persistent
        with (
            server(fixture) as endpoint,
            ManagementClient(config(endpoint, timeout=0.05)) as client,
        ):
            context = ManagementContext()
            watch = client.watch_resource_changes(
                messages.WatchResourceChangesRequest(after_cursor=10), context=context
            )
            watch.recv()
            watch.acknowledge(13)
            self.assertTrue(fixture.started.wait(2))
            failures = []

            def receive():
                try:
                    watch.recv()
                except ManagementRPCError as failure:
                    failures.append(failure)

            thread = Thread(target=receive)
            thread.start()
            sleep(0.08)
            self.assertTrue(thread.is_alive(), "unary timeout truncated persistent stream")
            watch.close()
            thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertTrue(fixture.cancelled.wait(2))
            self.assertFalse(context.cancelled)
            self.assertEqual(len(failures), 1)
            self.assertEqual(failures[0].code(), grpc.StatusCode.CANCELLED)


if __name__ == "__main__":
    unittest.main()

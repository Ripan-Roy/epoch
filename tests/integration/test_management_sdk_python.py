"""Public SDK fault-probe contracts; fixture tests are not Catalog certification."""

import base64
import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

import grpc

import management_sdk_python as probe
from epoch_sdk.management import ManagementClient, ManagementConfig, common, messages
from epoch_sdk._generated.epoch.v1 import regional_admin_pb2_grpc as service


class Backend(service.RegionalAdminServiceServicer):
    def __init__(self):
        self.requests = []

    def BatchApplyResources(self, request, context):
        self.requests.append(request)
        item = request.resources[0]
        return messages.BatchApplyResourcesResponse(
            results=[
                messages.ApplyResourceResponse(
                    resource=common.Resource(
                        name=item.name, spec=item.spec, generation=1
                    ),
                    created=True,
                    changed=True,
                )
            ]
        )


class PythonProbeTest(unittest.TestCase):
    def test_public_batch_keeps_exact_token_zero_presence_and_receipt(self):
        server = grpc.server(ThreadPoolExecutor(max_workers=2))
        backend = Backend()
        service.add_RegionalAdminServiceServicer_to_server(backend, server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        self.addCleanup(lambda: server.stop(0).wait())
        request = messages.BatchApplyResourcesRequest(
            request_token="original-token",
            resources=[
                messages.BatchApplyResource(
                    name=common.ResourceName(
                        organization="acme",
                        project="shop",
                        environment="dev",
                        namespace="core",
                        kind=common.RESOURCE_KIND_CACHE,
                        name="owned",
                    ),
                    spec=common.ResourceSpec(replicas=3),
                    expected_generation=0,
                )
            ],
        )
        encoded = base64.b64encode(
            request.SerializeToString(deterministic=True)
        ).decode("ascii")
        with ManagementClient(
            ManagementConfig(
                endpoints=(f"127.0.0.1:{port}",),
                bearer_token="fixture-admin",
                timeout=2,
                allow_insecure_loopback=True,
            )
        ) as client:
            witness = probe.invoke(
                client,
                {
                    "method": "BatchApplyResources",
                    "request_proto": encoded,
                    "expected_code": 0,
                },
            )
        self.assertEqual(0, witness["grpc_code"])
        self.assertEqual(1, witness["attempts"])
        self.assertFalse(witness["outcome_may_be_unknown"])
        self.assertEqual(request, backend.requests[0])
        self.assertTrue(
            backend.requests[0].resources[0].HasField("expected_generation")
        )
        self.assertTrue(witness["response_proto"])

    def test_checkpoint_is_durable_canonical_uint64_and_failed_replace_preserves_old(
        self,
    ):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "checkpoint.json"
            probe.durable_json(path, {"next_cursor": str(2**64 - 1)})
            previous = path.read_bytes()
            self.assertEqual(str(2**64 - 1), json.loads(previous)["next_cursor"])
            self.assertEqual(0o600, path.stat().st_mode & 0o777)
            with mock.patch.object(
                probe.os, "replace", side_effect=OSError("injected rename failure")
            ):
                with self.assertRaises(OSError):
                    probe.durable_json(path, {"next_cursor": "0"})
            self.assertEqual(previous, path.read_bytes())
            self.assertEqual([path], list(Path(temporary).iterdir()))

    def test_malformed_probe_method_and_wire_fail_before_network(self):
        with ManagementClient(
            ManagementConfig(
                endpoints=("127.0.0.1:12345",),
                bearer_token="fixture-admin",
                timeout=1,
                allow_insecure_loopback=True,
            )
        ) as client:
            for item in (
                {"method": "UnplannedMethod", "request_proto": "", "expected_code": 0},
                {
                    "method": "BatchApplyResources",
                    "request_proto": "/w==",
                    "expected_code": 0,
                },
            ):
                with self.assertRaises(ValueError):
                    probe.invoke(client, item)


if __name__ == "__main__":
    unittest.main()

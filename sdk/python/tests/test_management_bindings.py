"""Pinned generated contracts; public-client behavior has separate wire tests."""

import json
import os
import pickle
import re
import runpy
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import grpc

from epoch_sdk._generated.epoch.v1 import common_pb2 as common
from epoch_sdk._generated.epoch.v1 import regional_admin_pb2 as messages
from epoch_sdk._generated.epoch.v1 import regional_admin_pb2_grpc as service

ROOT = Path(__file__).resolve().parents[3]


class ManagementBindingTest(unittest.TestCase):
    def test_service_descriptor_matches_all_committed_rpc_methods(self):
        contract = (ROOT / "spec/proto/epoch/v1/regional_admin.proto").read_text()
        names = re.findall(r"\brpc\s+(\w+)\(", contract)
        descriptor = messages.DESCRIPTOR.services_by_name["RegionalAdminService"]
        self.assertEqual([method.name for method in descriptor.methods], names)
        self.assertEqual(len(names), 7)
        self.assertEqual(messages.DESCRIPTOR.name, "epoch/v1/regional_admin.proto")
        self.assertEqual(descriptor.full_name, "epoch.v1.RegionalAdminService")
        self.assertEqual(
            [method.server_streaming for method in descriptor.methods], [False] * 6 + [True]
        )
        self.assertTrue(all(not method.client_streaming for method in descriptor.methods))

    def test_generated_stub_has_every_exact_method_without_connecting(self):
        with grpc.insecure_channel("127.0.0.1:9") as channel:
            stub = service.RegionalAdminServiceStub(channel)
            for method in messages.DESCRIPTOR.services_by_name["RegionalAdminService"].methods:
                self.assertTrue(callable(getattr(stub, method.name)))

    def test_shared_go_wire_vectors_preserve_uint64_and_generation_presence(self):
        fixture = json.loads((ROOT / "spec/fixtures/sdk-management-wire-v1.json").read_text())
        self.assertEqual(fixture["schema"], "epoch.sdk.management-wire/v1")
        self.assertEqual(len(fixture["vectors"]), 3)
        for vector in fixture["vectors"]:
            with self.subTest(generation=vector["expected_generation"]):
                request = messages.DeleteResourceRequest(
                    request_token=fixture["request_token"],
                    name=common.ResourceName(**fixture["name"]),
                )
                if vector["expected_presence"]:
                    request.expected_generation = vector["expected_generation"]
                self.assertEqual(request.SerializeToString().hex(), vector["protobuf_hex"])
                decoded = messages.DeleteResourceRequest.FromString(
                    bytes.fromhex(vector["protobuf_hex"])
                )
                self.assertEqual(decoded, request)
                self.assertEqual(
                    decoded.HasField("expected_generation"), vector["expected_presence"]
                )

    def test_operation_and_watch_cursors_preserve_unsigned_extrema(self):
        maximum = (1 << 64) - 1
        operation = messages.GetOperationResponse(
            request_token="original-token",
            proposal_id=maximum,
            state=messages.OPERATION_STATE_SUCCEEDED,
            command_kind="delete_managed",
            first_change_cursor=maximum,
            last_change_cursor=maximum,
            expected_generation=0,
        )
        decoded = messages.GetOperationResponse.FromString(operation.SerializeToString())
        self.assertEqual(decoded, operation)
        self.assertEqual(decoded.proposal_id, maximum)
        self.assertTrue(decoded.HasField("expected_generation"))
        page = messages.WatchResourceChangesResponse(
            earliest_cursor=maximum,
            latest_cursor=maximum,
            next_cursor=maximum,
        )
        self.assertEqual(
            messages.WatchResourceChangesResponse.FromString(page.SerializeToString()), page
        )

    def test_pickling_resolves_only_the_sdk_generated_namespace(self):
        name = common.ResourceName(
            organization="acme", kind=common.RESOURCE_KIND_CACHE, name="demo"
        )
        self.assertEqual(type(name).__module__, "epoch_sdk._generated.epoch.v1.common_pb2")
        self.assertEqual(pickle.loads(pickle.dumps(name)), name)
        request = messages.GetResourceRequest(name=name)
        self.assertEqual(
            type(request).__module__, "epoch_sdk._generated.epoch.v1.regional_admin_pb2"
        )
        self.assertEqual(pickle.loads(pickle.dumps(request)), request)

    def test_regeneration_is_exact_and_uses_the_pinned_compiler(self):
        completed = subprocess.run(
            [sys.executable, str(ROOT / "scripts/generate-python-management.py"), "--check"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("bindings match the pinned contract", completed.stdout)

    def test_regeneration_rejects_unexpected_generated_source(self):
        generator = runpy.run_path(str(ROOT / "scripts/generate-python-management.py"))
        with tempfile.TemporaryDirectory(prefix="epoch-binding-inventory-") as directory:
            output = Path(directory)
            for relative, content in generator["generate"]().items():
                destination = output / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_text(content)
            (output / "foreign_pb2.py").write_text("# stale or foreign generated source\n")
            with (
                patch.dict(generator["main"].__globals__, {"OUTPUT": output}),
                patch.object(sys, "argv", ["generate-python-management.py", "--check"]),
                self.assertRaisesRegex(SystemExit, "unexpected.*foreign_pb2.py"),
            ):
                generator["main"]()

    def test_make_generation_check_rejects_stale_python_before_rewriting(self):
        makefile = (ROOT / "Makefile").read_text()
        generation_targets = makefile[
            makefile.index("generate: ##") : makefile.index("release-check: ##")
        ]
        with tempfile.TemporaryDirectory(prefix="epoch-make-generate-") as directory:
            fixture = Path(directory)
            (fixture / "Makefile").write_text(generation_targets)
            (fixture / "spec/proto").mkdir(parents=True)
            (fixture / "spec/proto/fixture.proto").write_text('syntax = "proto3";\n')
            (fixture / "sdk/go/gen").mkdir(parents=True)
            (fixture / "scripts").mkdir()
            (fixture / "state").write_text("stale")
            (fixture / "scripts/generate-python-management.py").write_text(
                "import sys\nfrom pathlib import Path\nstate = Path('state')\n"
                "if '--check' in sys.argv:\n"
                "    sys.exit(0 if state.read_text() == 'current' else 7)\n"
                "state.write_text('current')\n"
            )
            tools = fixture / "bin"
            tools.mkdir()
            buf = tools / "buf"
            buf.write_text("#!/bin/sh\nexit 0\n")
            buf.chmod(0o700)
            completed = subprocess.run(
                ["make", "generate-check"],
                cwd=fixture,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
                env={**os.environ, "PATH": f"{tools}:{os.environ['PATH']}"},
            )
            self.assertNotEqual(
                completed.returncode, 0, "generation check repaired stale source before testing it"
            )
            self.assertEqual((fixture / "state").read_text(), "stale")

            (fixture / "state").write_text("current")
            buf.write_text("#!/bin/sh\nexit 9\n")
            completed = subprocess.run(
                ["make", "generate-check"],
                cwd=fixture,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
                env={**os.environ, "PATH": f"{tools}:{os.environ['PATH']}"},
            )
            self.assertNotEqual(
                completed.returncode, 0, "generation check masked a failed Go generator"
            )


if __name__ == "__main__":
    unittest.main()

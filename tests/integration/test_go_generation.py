"""Local Go generators retain exact bytes, source pins, and strict freshness."""

from __future__ import annotations

import json
import runpy
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


class GoGenerationTests(unittest.TestCase):
    def setUp(self):
        self.generator = runpy.run_path(str(ROOT / "scripts/generate-go-protobuf.py"))
        self.installer = runpy.run_path(
            str(ROOT / "scripts/install-go-protobuf-tools.py")
        )

    def test_template_uses_only_local_generators_without_changing_wire_options(self):
        template = (ROOT / "buf.gen.yaml").read_text()
        self.assertNotIn("remote:", template)
        for required in (
            "clean: true",
            "local: protoc-gen-go\n",
            "local: protoc-gen-go-grpc\n",
            "paths=source_relative",
            "epoch.local/epoch/sdk/go/gen",
        ):
            self.assertIn(required, template)

    def test_actual_local_tools_match_every_committed_generated_byte(self):
        result = subprocess.run(
            ["python3", str(ROOT / "scripts/generate-go-protobuf.py"), "--check"],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("match pinned local generators", result.stdout)

    def test_wrong_buf_version_fails_before_generation(self):
        with (
            patch("subprocess.run") as execute,
            self.assertRaisesRegex(SystemExit, "Buf 1.72.0"),
        ):
            execute.return_value.stdout = "1.73.0\n"
            self.generator["require_buf"]()

    def test_missing_and_stale_outputs_fail_without_rewriting(self):
        with tempfile.TemporaryDirectory(prefix="epoch-go-freshness-") as directory:
            output = Path(directory)
            path = output / "epoch/v1/contract.pb.go"
            for existing in (None, b"stale"):
                with self.subTest(existing=existing):
                    if existing is not None:
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_bytes(existing)
                    with self.assertRaisesRegex(SystemExit, "stale Go bindings"):
                        self.generator["synchronize"](
                            output,
                            {Path("epoch/v1/contract.pb.go"): b"current"},
                            check=True,
                        )
                    self.assertEqual(
                        path.read_bytes() if path.exists() else None, existing
                    )

    def test_foreign_source_and_symlinks_are_never_deleted_or_overwritten(self):
        with tempfile.TemporaryDirectory(prefix="epoch-go-freshness-") as directory:
            output = Path(directory)
            foreign = output / "foreign.go"
            foreign.write_bytes(b"user source")
            for check in (False, True):
                with (
                    self.subTest(check=check),
                    self.assertRaisesRegex(
                        SystemExit, "unexpected Go generated source"
                    ),
                ):
                    self.generator["synchronize"](
                        output, {Path("current.go"): b"current"}, check=check
                    )
            self.assertEqual(foreign.read_bytes(), b"user source")
            self.assertFalse((output / "current.go").exists())
            foreign.unlink()
            foreign.symlink_to(ROOT / "buf.gen.yaml")
            with self.assertRaisesRegex(SystemExit, "symlink"):
                self.generator["synchronize"](
                    output, {Path("current.go"): b"current"}, check=False
                )

    def test_invalid_generated_paths_are_rejected_before_writes(self):
        with tempfile.TemporaryDirectory(prefix="epoch-go-freshness-") as directory:
            output = Path(directory)
            for generated in (
                {},
                {Path("../foreign.go"): b"bad"},
                {Path("/foreign.go"): b"bad"},
            ):
                with (
                    self.subTest(generated=generated),
                    self.assertRaisesRegex(
                        SystemExit, "invalid Go generated inventory"
                    ),
                ):
                    self.generator["synchronize"](output, generated, check=False)
            self.assertEqual(list(output.iterdir()), [])

    def test_installer_preserves_existing_directory_before_any_tool_execution(self):
        with tempfile.TemporaryDirectory(prefix="epoch-go-tools-") as directory:
            destination = Path(directory) / "tools"
            destination.mkdir()
            sentinel = destination / "user-data"
            sentinel.write_bytes(b"keep")
            with (
                patch("subprocess.run") as execute,
                self.assertRaisesRegex(SystemExit, "already exists"),
            ):
                self.installer["install"](destination)
            execute.assert_not_called()
            self.assertEqual(sentinel.read_bytes(), b"keep")

    def test_source_checksum_failure_keeps_sumdb_enabled_and_never_installs(self):
        with tempfile.TemporaryDirectory(prefix="epoch-go-tools-") as directory:
            destination = Path(directory) / "tools"
            with patch("subprocess.run") as execute:
                execute.side_effect = [
                    subprocess.CompletedProcess(
                        [], 0, stdout="go version go1.26.5 linux/amd64\n"
                    ),
                    subprocess.CompletedProcess(
                        [],
                        0,
                        stdout=json.dumps(
                            {"Path": "wrong source", "Sum": "wrong checksum"}
                        ),
                    ),
                ]
                with self.assertRaisesRegex(SystemExit, "checksum mismatch"):
                    self.installer["install"](destination)
                self.assertEqual(execute.call_count, 2)
                download = execute.call_args_list[1]
                self.assertEqual(download.args[0][:3], ["go", "mod", "download"])
                environment = download.kwargs["env"]
                self.assertEqual(environment["GOSUMDB"], "sum.golang.org")
                self.assertEqual(environment["GOPROXY"], "https://proxy.golang.org")
                self.assertEqual(environment["GONOSUMDB"], "")
                self.assertEqual(environment["GOTOOLCHAIN"], "local")
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_installer_ignores_inherited_source_build_and_temporary_caches(self):
        with tempfile.TemporaryDirectory(prefix="epoch-go-tools-") as directory:
            destination = Path(directory) / "tools"
            with (
                patch.dict(
                    "os.environ",
                    {
                        "GOMODCACHE": "/untrusted/source-cache",
                        "GOCACHE": "/untrusted/build-cache",
                        "GOTMPDIR": "/untrusted/temporary-cache",
                        "CGO_ENABLED": "1",
                        "GOFLAGS": "-toolexec=untrusted",
                    },
                ),
                patch("subprocess.run") as execute,
            ):
                execute.side_effect = [
                    subprocess.CompletedProcess(
                        [], 0, stdout="go version go1.26.5 linux/amd64\n"
                    ),
                    subprocess.CompletedProcess(
                        [], 0, stdout=json.dumps({"Path": "wrong source"})
                    ),
                ]
                with self.assertRaisesRegex(SystemExit, "checksum mismatch"):
                    self.installer["install"](destination)
                environment = execute.call_args_list[1].kwargs["env"]
                caches = [
                    Path(environment[name])
                    for name in ("GOMODCACHE", "GOCACHE", "GOTMPDIR")
                ]
                self.assertEqual(len(set(caches)), 3)
                self.assertEqual(len({path.parent for path in caches}), 1)
                for path in caches:
                    self.assertEqual(path.parent.parent, Path(directory))
                    self.assertTrue(path.parent.name.startswith(".epoch-go-protobuf-"))
                self.assertEqual(environment["CGO_ENABLED"], "0")
                self.assertEqual(
                    environment["GOFLAGS"],
                    "-trimpath -tags=epoch_codegen_clean_cache_v1",
                )
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_legacy_or_wrong_compiler_build_metadata_is_rejected_before_execution(self):
        with tempfile.TemporaryDirectory(prefix="epoch-go-tools-") as directory:
            root = Path(directory)
            binary = root / "bin/protoc-gen-go"
            binary.parent.mkdir()
            binary.write_bytes(b"not executed")
            binary.chmod(0o755)
            spec = self.installer["TOOLS"]["protoc-gen-go"]
            for compiler, tag in (
                ("go1.26.5", None),
                ("go1.26.6", "epoch_codegen_clean_cache_v1"),
                ("go1.26.5", "wrong-source-build"),
            ):
                metadata = (
                    f"{binary}: {compiler}\n"
                    f"\tmod\t{spec['module']}\t{spec['version']}\t{spec['sum']}\n"
                    "\tbuild\t-trimpath=true\n\tbuild\tCGO_ENABLED=0\n"
                )
                if tag is not None:
                    metadata += f"\tbuild\t-tags={tag}\n"
                with (
                    self.subTest(compiler=compiler, tag=tag),
                    patch("subprocess.run") as execute,
                    self.assertRaisesRegex(SystemExit, "clean source build"),
                ):
                    execute.return_value.stdout = metadata
                    self.installer["verify_tools"](root)
                self.assertEqual(execute.call_count, 1)
                self.assertEqual(execute.call_args.args[0][:3], ["go", "version", "-m"])

    def test_wrong_compiler_and_missing_generators_fail_closed(self):
        with tempfile.TemporaryDirectory(prefix="epoch-go-tools-") as directory:
            with patch("subprocess.run") as execute:
                execute.return_value.stdout = "go version go1.26.6 linux/amd64\n"
                with self.assertRaisesRegex(SystemExit, "go1.26.5"):
                    self.installer["install"](Path(directory) / "tools")
                self.assertEqual(execute.call_count, 1)
            with (
                patch("subprocess.run") as execute,
                self.assertRaisesRegex(SystemExit, "missing regular"),
            ):
                self.installer["verify_tools"](Path(directory))
            execute.assert_not_called()

    def test_tool_build_metadata_is_checked_before_execution(self):
        with tempfile.TemporaryDirectory(prefix="epoch-go-tools-") as directory:
            root = Path(directory)
            binary = root / "bin/protoc-gen-go"
            binary.parent.mkdir()
            binary.write_bytes(b"untrusted file")
            binary.chmod(0o755)
            with (
                patch("subprocess.run") as execute,
                self.assertRaisesRegex(SystemExit, "build metadata"),
            ):
                execute.return_value.stdout = "\tmod\tuntrusted\tv1.36.11\twrong\n"
                self.installer["verify_tools"](root)
            self.assertEqual(execute.call_count, 1)
            self.assertEqual(execute.call_args.args[0][:3], ["go", "version", "-m"])


if __name__ == "__main__":
    unittest.main()

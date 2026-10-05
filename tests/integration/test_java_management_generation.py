"""Pin freshness and inventory checks for externally generated Java contracts."""

from __future__ import annotations

import hashlib
import runpy
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/generate-java-management.py"


class JavaGenerationTest(unittest.TestCase):
    def test_java_generation_has_no_remote_plugin_dependency(self):
        template = (ROOT / "buf.gen.java.yaml").read_text()
        self.assertNotIn("remote:", template)
        self.assertIn("protoc_builtin: java", template)
        self.assertIn("protoc_path: protoc", template)
        self.assertIn("local: protoc-gen-grpc-java", template)

    def test_pinned_local_generation_is_exact(self):
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--check"],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("bindings match the pinned contract", result.stdout)

    def test_plugin_checksum_is_checked_before_any_execution(self):
        module = runpy.run_path(str(SCRIPT))
        with tempfile.TemporaryDirectory(prefix="epoch-java-plugin-test-") as directory:
            binary = Path(directory) / "protoc-gen-grpc-java"
            binary.write_bytes(b"untrusted executable")
            binary.chmod(0o755)
            with self.assertRaisesRegex(SystemExit, "checksum"):
                module["verify_plugin"](binary)
            binary.unlink()
            binary.symlink_to(SCRIPT)
            with self.assertRaisesRegex(SystemExit, "regular file"):
                module["verify_plugin"](binary)

    def test_verified_plugin_and_unsupported_platform_are_fail_closed(self):
        module = runpy.run_path(str(SCRIPT))
        with tempfile.TemporaryDirectory(prefix="epoch-java-plugin-test-") as directory:
            binary = Path(directory) / "protoc-gen-grpc-java"
            content = b"test compiler bytes"
            binary.write_bytes(content)
            binary.chmod(0o755)
            with patch.dict(
                module["verify_plugin"].__globals__,
                {"plugin_spec": lambda: ("test", hashlib.sha256(content).hexdigest())},
            ):
                self.assertEqual(module["verify_plugin"](binary), binary.resolve())
                binary.chmod(0o644)
                with self.assertRaisesRegex(SystemExit, "executable"):
                    module["verify_plugin"](binary)
        with (
            patch("platform.system", return_value="Unsupported"),
            self.assertRaisesRegex(SystemExit, "unsupported"),
        ):
            module["plugin_spec"]()

    def test_installer_preserves_existing_paths_and_rejects_corrupt_downloads(self):
        module = runpy.run_path(str(SCRIPT))
        with tempfile.TemporaryDirectory(
            prefix="epoch-java-install-test-"
        ) as directory:
            destination = Path(directory) / "toolchain"
            destination.mkdir()
            sentinel = destination / "user-data"
            sentinel.write_bytes(b"keep")
            with patch("urllib.request.urlopen") as download:
                with self.assertRaisesRegex(SystemExit, "already exists"):
                    module["install_plugin"](destination)
                download.assert_not_called()
            self.assertEqual(sentinel.read_bytes(), b"keep")
            missing = Path(directory) / "new-toolchain"
            with patch("urllib.request.urlopen") as download:
                response = download.return_value.__enter__.return_value
                classifier, _ = module["plugin_spec"]()
                response.geturl.return_value = (
                    f"{module['PLUGIN_REPOSITORY']}/1.84.0/"
                    f"protoc-gen-grpc-java-1.84.0-{classifier}.exe"
                )
                response.read.return_value = b"corrupt download"
                with self.assertRaisesRegex(SystemExit, "checksum"):
                    module["install_plugin"](missing)
            self.assertFalse(missing.exists())
            self.assertEqual(sorted(Path(directory).iterdir()), [destination])

    def test_java_ci_installs_pinned_local_compilers_before_freshness_check(self):
        text = (ROOT / ".github/workflows/ci.yml").read_text()
        job = text[text.index("  java:\n") : text.index("  web:\n")]
        self.assertIn("scripts/install-protoc.sh", job)
        self.assertIn("scripts/generate-java-management.py --install-plugin", job)
        self.assertLess(
            job.index("--install-plugin"),
            job.index("test_java_management_generation.py"),
        )

    def test_stale_and_missing_outputs_are_rejected_without_rewriting(self):
        module = runpy.run_path(str(SCRIPT))
        relative = Path("io/epoch/sdk/gen/epoch/v1/RegionalAdminServiceGrpc.java")
        with tempfile.TemporaryDirectory(
            prefix="epoch-java-generation-test-"
        ) as directory:
            output = Path(directory)
            target = output / relative
            target.parent.mkdir(parents=True)
            for content in (None, b"stale"):
                if content is not None:
                    target.write_bytes(content)
                with (
                    patch.dict(
                        module["main"].__globals__,
                        {
                            "OUTPUT": output,
                            "ROOT": output,
                            "generate": lambda: {relative: b"current"},
                        },
                    ),
                    patch.object(sys, "argv", [str(SCRIPT), "--check"]),
                    self.assertRaisesRegex(
                        SystemExit, "stale Java management bindings"
                    ),
                ):
                    module["main"]()
                if content is None:
                    self.assertFalse(target.exists())
                else:
                    self.assertEqual(target.read_bytes(), content)

    def test_unexpected_generated_inventory_is_not_deleted_or_accepted(self):
        module = runpy.run_path(str(SCRIPT))
        with tempfile.TemporaryDirectory(
            prefix="epoch-java-inventory-test-"
        ) as directory:
            output = Path(directory)
            foreign = output / "Foreign.java"
            foreign.write_bytes(b"not owned by this generator")
            for args in ([str(SCRIPT)], [str(SCRIPT), "--check"]):
                with (
                    patch.dict(
                        module["main"].__globals__,
                        {
                            "OUTPUT": output,
                            "generate": lambda: {Path("Current.java"): b"current"},
                        },
                    ),
                    patch.object(sys, "argv", args),
                    self.assertRaisesRegex(SystemExit, "unexpected.*Foreign.java"),
                ):
                    module["main"]()
                self.assertEqual(foreign.read_bytes(), b"not owned by this generator")
                self.assertFalse((output / "Current.java").exists())

    def test_make_checks_java_before_any_generator_can_rewrite_it(self):
        text = (ROOT / "Makefile").read_text()
        targets = text[
            text.index("generate-check: ##") : text.index("release-check: ##")
        ]
        self.assertIn("scripts/generate-java-management.py --check", targets)
        self.assertLess(
            targets.index("scripts/generate-java-management.py --check"),
            targets.index("$(MAKE) generate"),
        )


if __name__ == "__main__":
    unittest.main()

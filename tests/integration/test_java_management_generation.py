"""Pin freshness and inventory checks for externally generated Java contracts."""

from __future__ import annotations

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
    def test_pinned_remote_generation_is_exact(self):
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--check"],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("bindings match the pinned contract", result.stdout)

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

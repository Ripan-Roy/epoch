from __future__ import annotations

import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_DIRECTORY = REPOSITORY_ROOT / "scripts" / "release"


class PackageScriptTests(unittest.TestCase):
    def test_makefile_exposes_the_complete_package_shape_gate(self) -> None:
        makefile = (REPOSITORY_ROOT / "Makefile").read_text(encoding="utf-8")

        self.assertIn("package-shape:", makefile)
        self.assertIn("python3 -m unittest discover -s tests/release", makefile)
        self.assertIn("check-package-policy.py --scope versions", makefile)
        self.assertIn("scripts/release/check-rust.sh", makefile)
        self.assertIn("scripts/release/check-go.sh", makefile)
        self.assertIn("scripts/release/check-java.sh", makefile)
        self.assertIn("scripts/release/check-python.sh", makefile)
        self.assertIn("scripts/release/check-typescript.sh", makefile)

    def test_shape_scripts_never_invoke_a_registry_publish(self) -> None:
        scripts = {
            "check-rust.sh": "cargo metadata",
            "check-go.sh": "go mod tidy -diff",
            "check-java.sh": "clean install",
            "check-python.sh": "-m twine check --strict",
            "check-typescript.sh": "npm pack --dry-run",
        }
        forbidden_commands = (
            "cargo publish",
            "npm publish",
            "pnpm publish",
            "twine upload",
            "mvn deploy",
        )

        for name, expected_command in scripts.items():
            with self.subTest(script=name):
                contents = (SCRIPT_DIRECTORY / name).read_text(encoding="utf-8")
                self.assertIn("set -eu", contents)
                self.assertIn(expected_command, contents)
                for forbidden_command in forbidden_commands:
                    self.assertNotIn(forbidden_command, contents)

    def test_package_workflow_has_no_publication_authority(self) -> None:
        workflow = (
            REPOSITORY_ROOT / ".github" / "workflows" / "package-readiness.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("permissions: {}", workflow)
        self.assertNotIn("id-token: write", workflow)
        self.assertNotIn("secrets.", workflow)
        for script in (
            "check-package-policy.py",
            "check-rust.sh",
            "check-go.sh",
            "check-java.sh",
            "check-python.sh",
            "check-typescript.sh",
        ):
            self.assertIn(script, workflow)
        self.assertIn("--scope versions", workflow)
        for forbidden_command in (
            "cargo publish",
            "npm publish",
            "pnpm publish",
            "twine upload",
            "mvn deploy",
        ):
            self.assertNotIn(forbidden_command, workflow)

    def test_manual_release_gate_cannot_publish(self) -> None:
        workflow = (
            REPOSITORY_ROOT / ".github" / "workflows" / "release-gate.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("workflow_dispatch:", workflow)
        self.assertNotIn("pull_request:", workflow)
        self.assertNotIn("push:", workflow)
        self.assertIn("permissions: {}", workflow)
        self.assertIn("--mode release", workflow)
        self.assertNotIn("id-token: write", workflow)
        self.assertNotIn("secrets.", workflow)

    def test_docs_surface_the_machine_readable_package_boundary(self) -> None:
        page = (REPOSITORY_ROOT / "console" / "src" / "DocsPage.tsx").read_text(
            encoding="utf-8"
        )
        pages_workflow = (
            REPOSITORY_ROOT / ".github" / "workflows" / "pages.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("package-readiness.json", page)
        self.assertIn("Package publication status", page)
        self.assertIn("NO REGISTRY UPLOAD", page)
        self.assertIn("Package publication status", pages_workflow)


if __name__ == "__main__":
    unittest.main()

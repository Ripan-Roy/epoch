"""Cached OCI compilation must not replace exact-source runtime verification."""

from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BUILD_ACTION = "docker/build-push-action@53b7df96c91f9c12dcc8a07bcb9ccacbed38856a"
BUILDX_ACTION = "docker/setup-buildx-action@37fe631027851001ddb9b187196cc803df7f5f0e"


def job(workflow: str, name: str) -> str:
    match = re.search(
        rf"^  {re.escape(name)}:\n(.*?)(?=^  [a-z][a-z0-9-]*:|\Z)",
        workflow,
        re.MULTILINE | re.DOTALL,
    )
    if match is None:
        raise AssertionError(f"missing CI job {name}")
    return match[1]


def build_steps(section: str) -> list[str]:
    return [
        step
        for step in re.split(r"^      - ", section, flags=re.MULTILINE)
        if f"uses: {BUILD_ACTION}" in step
    ]


class ContainerCacheContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = (ROOT / ".github/workflows/ci.yml").read_text()

    def test_native_arm64_build_caches_compilation_not_its_proof(self) -> None:
        section = job(self.workflow, "container-arm64")
        self.assertIn(BUILDX_ACTION, section)
        self.assertIn('DOCKER_BUILD_RECORD_UPLOAD: "false"', section)
        self.assertIn('test "$(uname -m)" = aarch64', section)
        builds = build_steps(section)
        self.assertEqual(len(builds), 1)
        self.assert_cached_build(builds[0], "node", "arm64", "epoch/node:ci-arm64")
        for proof in (
            "./scripts/inspect-oci-image.sh",
            "docker save --output",
            "sha256sum node.tar",
            "control-ha-node-arm64-${{ github.run_attempt }}",
        ):
            self.assertIn(proof, section)

    def test_all_five_x86_images_keep_distinct_caches_and_runtime_checks(self) -> None:
        section = job(self.workflow, "container")
        self.assertIn(BUILDX_ACTION, section)
        self.assertIn('DOCKER_BUILD_RECORD_UPLOAD: "false"', section)
        builds = build_steps(section)
        self.assertEqual(len(builds), 5)
        for component in ("node", "control", "operator", "cli", "compat"):
            matching = [
                step
                for step in builds
                if f"file: deploy/docker/Dockerfile.{component}\n" in step
            ]
            self.assertEqual(len(matching), 1, component)
            self.assert_cached_build(
                matching[0], component, "amd64", f"epoch/{component}:ci"
            )
            self.assertIn(
                f"./scripts/inspect-oci-image.sh epoch/{component}:ci", section
            )
            self.assertIn(f"image: epoch/{component}:ci", section)
        self.assertIn("tests/integration/oci-image-inspection.sh", section)
        self.assertIn("make test-protocol-regional", section)
        for profile in ("stream", "queue", "cache", "bus"):
            self.assertIn(f"tests/integration/{profile}-tablet.sh", section)
        self.assertIn("branches: [main]", self.workflow)
        self.assertNotIn("packages: write", self.workflow)

    def assert_cached_build(
        self, step: str, component: str, architecture: str, tag: str
    ) -> None:
        for required in (
            "context: .",
            "load: true",
            "push: false",
            f"file: deploy/docker/Dockerfile.{component}",
            f"platforms: linux/{architecture}",
            f"tags: {tag}",
            "EPOCH_VERSION=${{ steps.image-version.outputs.value }}",
            "VCS_REF=${{ github.sha }}",
            f"cache-from: type=gha,version=2,scope=ci-{component}-{architecture}",
            f"cache-to: type=gha,version=2,scope=ci-{component}-{architecture},mode=max,timeout=2m,ignore-error=true",
        ):
            self.assertIn(required, step)
        self.assertNotIn("if:", step)
        self.assertNotIn("continue-on-error:", step)
        self.assertNotIn("secrets.", step)


if __name__ == "__main__":
    unittest.main()

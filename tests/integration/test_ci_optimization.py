"""CI reuse must remove repeated compilation, never verification or proof."""

from __future__ import annotations

import re
import unittest
from pathlib import Path


WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/ci.yml"
CACHE_ACTION = "actions/cache@55cc8345863c7cc4c66a329aec7e433d2d1c52a9"
COMPILE_JOBS = {
    "rust": "Rust format, lint, test, build",
    "integration": "Cross-language standalone and docs smoke",
    "connector-conformance": "Source connector conformance",
    "protocol-conformance": "Redis, Kafka, and RabbitMQ client conformance",
}


def job_block(workflow: str, name: str) -> str:
    match = re.search(
        rf"^  {re.escape(name)}:\n(.*?)(?=^  [a-z][a-z0-9-]*:\n|\Z)",
        workflow,
        re.MULTILINE | re.DOTALL,
    )
    if match is None:
        raise AssertionError(f"Missing job {name}")
    return match.group(1)


def steps(job: str) -> list[str]:
    return re.findall(r"^      - .*?(?=^      - |\Z)", job, re.MULTILINE | re.DOTALL)


def named_step(job: str, name: str) -> str:
    matches = [
        step for step in steps(job) if step.startswith(f"      - name: {name}\n")
    ]
    if len(matches) != 1:
        raise AssertionError(
            f"Expected exactly one {name!r} step; found {len(matches)}"
        )
    return matches[0]


class CIOptimizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workflow = WORKFLOW.read_text()

    def test_independent_integration_jobs_start_without_quality_barriers(self) -> None:
        for name in COMPILE_JOBS:
            with self.subTest(job=name):
                job = job_block(self.workflow, name)
                self.assertNotRegex(job, re.compile(r"^    needs:", re.MULTILINE))
                self.assertIn(f"    name: {COMPILE_JOBS[name]}\n", job)

    def test_registry_cache_is_pinned_and_contains_only_dependency_sources(
        self,
    ) -> None:
        expected_paths = {
            "~/.cargo/registry/index",
            "~/.cargo/registry/cache",
            "~/.cargo/registry/src",
            "~/.cargo/git/db",
            "~/.cargo/git/checkouts",
        }
        for name in COMPILE_JOBS:
            with self.subTest(job=name):
                job = job_block(self.workflow, name)
                cache = named_step(job, "Restore Cargo registry and Git dependencies")
                self.assertIn(f"uses: {CACHE_ACTION}", cache)
                paths = set(re.findall(r"^            (~/.+)$", cache, re.MULTILINE))
                self.assertEqual(paths, expected_paths)
                self.assertIn("cargo-registry-v1-", cache)
                for binding in (
                    "runner.os",
                    "runner.arch",
                    "steps.runner-image.outputs.version",
                    "rust-1.97.1",
                    "hashFiles('Cargo.lock')",
                ):
                    self.assertIn(binding, cache)
                self.assertIn("restore-keys: |", cache)
                self.assertNotIn("if:", cache)

    def test_compiler_cache_is_job_image_config_and_source_scoped(self) -> None:
        self.assertIn('  CARGO_INCREMENTAL: "0"', self.workflow)
        for name in COMPILE_JOBS:
            with self.subTest(job=name):
                job = job_block(self.workflow, name)
                image = named_step(job, "Identify the hosted runner image")
                self.assertIn("id: runner-image", image)
                self.assertIn("${ImageOS:?}", image)
                self.assertIn("${ImageVersion:?}", image)
                cache = named_step(job, "Restore job-specific Rust compiler artifacts")
                self.assertIn(f"uses: {CACHE_ACTION}", cache)
                self.assertIn(f"cargo-target-v1-{name}-", cache)
                for binding in (
                    "runner.os",
                    "runner.arch",
                    "steps.runner-image.outputs.version",
                    "rust-1.97.1",
                    "'Cargo.lock'",
                    "'Cargo.toml'",
                    "'**/Cargo.toml'",
                    "'rust-toolchain.toml'",
                    "'**/build.rs'",
                    "'.cargo/config*'",
                    "'**/.cargo/config*'",
                    "'spec/proto/**'",
                    "'buf*.yaml'",
                    "'.github/workflows/ci.yml'",
                    "github.sha",
                ):
                    self.assertIn(binding, cache)
                lines = cache.splitlines()
                key = next(
                    line.strip()[5:]
                    for line in lines
                    if line.startswith("          key: ")
                )
                fallback = lines[lines.index("          restore-keys: |") + 1].strip()
                self.assertEqual(key, fallback + "${{ github.sha }}")
                self.assertIn("            target/debug\n", cache)
                self.assertIn("            !target/debug/incremental\n", cache)
                self.assertNotIn("target/doc", cache)
                self.assertNotIn("evidence", cache)
                self.assertNotIn("if:", cache)
                self.assertLess(job.index(image), job.index(cache))
                first_compile = {
                    "rust": "cargo fmt --all --check",
                    "integration": "bash tests/integration/smoke.sh",
                    "connector-conformance": "cargo test --locked -p epoch-node",
                    "protocol-conformance": "bash tests/integration/protocol-compatibility.sh",
                }[name]
                self.assertLess(job.index(cache), job.index(first_compile))

    def test_compilation_tests_and_real_runtime_probes_never_skip_on_cache_hits(
        self,
    ) -> None:
        required_commands = {
            "rust": (
                "cargo fmt --all --check",
                "cargo clippy --locked --workspace --all-targets --all-features -- -D warnings",
                "cargo test --locked --workspace --all-targets --all-features",
                "make test-consensus-process",
                "cargo build --locked --workspace --all-targets --all-features",
                "cargo doc --locked --workspace --all-features --no-deps",
            ),
            "integration": (
                "bash tests/integration/smoke.sh",
                "TestPublicProbeMatrixKeepsTransportLossUnknown",
                "bash tests/integration/docs-quickstarts.sh",
            ),
            "connector-conformance": (
                "up --detach --wait object-store postgres mysql kafka",
                "cargo test --locked -p epoch-node source_adapters --lib --",
                "--ignored --nocapture --test-threads=1",
            ),
            "protocol-conformance": (
                "bash tests/integration/protocol-compatibility.sh",
            ),
        }
        for name, commands in required_commands.items():
            job = job_block(self.workflow, name)
            for command in commands:
                with self.subTest(job=name, command=command):
                    matches = [step for step in steps(job) if command in step]
                    self.assertEqual(len(matches), 1)
                    self.assertNotRegex(matches[0], r"(?m)^        if:")
            self.assertNotIn("continue-on-error:", job)

    def test_optimization_contract_is_executed_in_ci(self) -> None:
        job = job_block(self.workflow, "go-contracts")
        self.assertIn("make test-ci-optimization", job)


if __name__ == "__main__":
    unittest.main()

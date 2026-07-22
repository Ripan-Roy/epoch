from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CHECKER = REPOSITORY_ROOT / "scripts" / "release" / "check-package-policy.py"
POLICY = REPOSITORY_ROOT / "release" / "package-readiness.json"
FIXTURES = Path(__file__).resolve().parent / "fixtures"
VERSION_SURFACE_FILES = (
    Path("Cargo.toml"),
    Path("package.json"),
    Path("console/package.json"),
    Path("sdk/go/epoch/transport.go"),
    Path("sdk/java/pom.xml"),
    Path("sdk/java/src/main/java/io/epoch/sdk/HttpTransport.java"),
    Path("sdk/python/pyproject.toml"),
    Path("sdk/python/src/epoch_sdk/transport.py"),
)


class PackagePolicyTests(unittest.TestCase):
    maxDiff = None

    def run_checker(
        self,
        *,
        repo: Path = REPOSITORY_ROOT,
        policy: Path = POLICY,
        scope: str = "policy",
        mode: str = "shape",
        cargo_metadata: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        command = [
            sys.executable,
            str(CHECKER),
            "--repo",
            str(repo),
            "--policy",
            str(policy),
            "--scope",
            scope,
            "--mode",
            mode,
        ]
        if cargo_metadata is not None:
            command.extend(["--cargo-metadata", str(cargo_metadata)])
        return subprocess.run(command, capture_output=True, check=False, text=True)

    def assert_failed_with(
        self, result: subprocess.CompletedProcess[str], expected: str
    ) -> None:
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(expected, result.stderr)

    def load_policy(self) -> dict[str, object]:
        return json.loads(POLICY.read_text(encoding="utf-8"))

    def write_policy(self, directory: Path, policy: dict[str, object]) -> Path:
        path = directory / "package-readiness.json"
        path.write_text(json.dumps(policy), encoding="utf-8")
        return path

    def copy_version_surface(self, destination: Path) -> None:
        for relative_path in VERSION_SURFACE_FILES:
            target = destination / relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(REPOSITORY_ROOT / relative_path, target)

    def test_repository_policy_is_valid_for_shape_checks(self) -> None:
        result = self.run_checker()

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("package policy: valid", result.stdout)

    def test_unknown_blocker_reference_is_rejected(self) -> None:
        policy = self.load_policy()
        packages = policy["packages"]
        self.assertIsInstance(packages, dict)
        packages["go"]["blockerIds"].append("not-a-real-blocker")

        with tempfile.TemporaryDirectory() as temp_directory:
            policy_path = self.write_policy(Path(temp_directory), policy)
            result = self.run_checker(policy=policy_path)

        self.assert_failed_with(result, "unknown blocker 'not-a-real-blocker'")

    def test_python_version_must_match_the_alpha_release(self) -> None:
        policy = self.load_policy()
        packages = policy["packages"]
        self.assertIsInstance(packages, dict)
        packages["python"]["version"] = "0.1.0a1"

        with tempfile.TemporaryDirectory() as temp_directory:
            policy_path = self.write_policy(Path(temp_directory), policy)
            result = self.run_checker(policy=policy_path)

        self.assert_failed_with(result, "Python version must be '0.1.0a2'")

    def test_release_mode_rejects_the_source_only_channel(self) -> None:
        result = self.run_checker(mode="release")

        self.assert_failed_with(result, "release channel is source-only")

    def test_release_mode_can_accept_a_fully_approved_candidate(self) -> None:
        policy = self.load_policy()
        release = policy["release"]
        packages = policy["packages"]
        self.assertIsInstance(release, dict)
        self.assertIsInstance(packages, dict)
        release["channel"] = "registry-candidate"
        policy["blockers"] = []
        approved_coordinates = {
            "rust": "crates.io/epoch",
            "go": "github.com/Ripan-Roy/epoch",
            "java": "io.epoch:epoch-sdk",
            "python": "epoch-sdk",
            "typescript": "@epoch/sdk",
        }
        for ecosystem, package in packages.items():
            package["publicationStatus"] = "ready"
            package["stage"] = "package-ready"
            package["coordinate"] = approved_coordinates[ecosystem]
            package["blockerIds"] = []

        with tempfile.TemporaryDirectory() as temp_directory:
            policy_path = self.write_policy(Path(temp_directory), policy)
            result = self.run_checker(policy=policy_path, mode="release")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("package policy: valid", result.stdout)

    def test_repository_versions_match_the_release_mapping(self) -> None:
        result = self.run_checker(scope="versions")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("repository version mapping: valid", result.stdout)

    def test_repository_version_drift_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_directory:
            repo = Path(temp_directory)
            self.copy_version_surface(repo)
            console_manifest = json.loads(
                (repo / "console" / "package.json").read_text(encoding="utf-8")
            )
            console_manifest["version"] = "0.1.0-alpha.1"
            (repo / "console" / "package.json").write_text(
                json.dumps(console_manifest), encoding="utf-8"
            )
            result = self.run_checker(repo=repo, scope="versions")

        self.assert_failed_with(
            result, "console/package.json version must be '0.1.0-alpha.2'"
        )

    def test_rust_scope_accepts_an_explicitly_private_workspace(self) -> None:
        result = self.run_checker(
            scope="rust", cargo_metadata=FIXTURES / "cargo-metadata-private.json"
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Rust package boundary: valid", result.stdout)

    def test_rust_scope_rejects_implicit_publishability(self) -> None:
        metadata = json.loads(
            (FIXTURES / "cargo-metadata-private.json").read_text(encoding="utf-8")
        )
        metadata["packages"][0]["publish"] = None

        with tempfile.TemporaryDirectory() as temp_directory:
            metadata_path = Path(temp_directory) / "cargo-metadata.json"
            metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
            result = self.run_checker(scope="rust", cargo_metadata=metadata_path)

        self.assert_failed_with(result, "must declare publish = false")

    def test_rust_scope_rejects_an_unclassified_workspace_member(self) -> None:
        metadata = json.loads(
            (FIXTURES / "cargo-metadata-private.json").read_text(encoding="utf-8")
        )
        package = {
            **metadata["packages"][0],
            "id": "path+file:///repo/crates/surprise#0.1.0-alpha.2",
            "name": "surprise",
            "manifest_path": "/repo/crates/surprise/Cargo.toml",
        }
        metadata["packages"].append(package)
        metadata["workspace_members"].append(package["id"])

        with tempfile.TemporaryDirectory() as temp_directory:
            metadata_path = Path(temp_directory) / "cargo-metadata.json"
            metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
            result = self.run_checker(scope="rust", cargo_metadata=metadata_path)

        self.assert_failed_with(
            result, "Rust package policy does not match workspace members"
        )

    def test_go_scope_accepts_the_documented_provisional_module(self) -> None:
        with tempfile.TemporaryDirectory() as temp_directory:
            repo = Path(temp_directory)
            shutil.copyfile(FIXTURES / "go.mod.provisional", repo / "go.mod")
            result = self.run_checker(repo=repo, scope="go")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Go module boundary: valid", result.stdout)

    def test_go_scope_rejects_provisional_module_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temp_directory:
            repo = Path(temp_directory)
            shutil.copyfile(FIXTURES / "go.mod.wrong", repo / "go.mod")
            result = self.run_checker(repo=repo, scope="go")

        self.assert_failed_with(result, "Go module must remain 'epoch.local/epoch'")

    def test_typescript_scope_accepts_an_explicitly_planned_absence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_directory:
            repo = Path(temp_directory)
            shutil.copyfile(
                FIXTURES / "pnpm-workspace.yaml", repo / "pnpm-workspace.yaml"
            )
            result = self.run_checker(repo=repo, scope="typescript")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("TypeScript package boundary: planned", result.stdout)

    def test_typescript_scope_rejects_an_unreviewed_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_directory:
            repo = Path(temp_directory)
            shutil.copyfile(
                FIXTURES / "pnpm-workspace.yaml", repo / "pnpm-workspace.yaml"
            )
            package_directory = repo / "sdk" / "typescript"
            package_directory.mkdir(parents=True)
            shutil.copyfile(
                FIXTURES / "typescript-package.json", package_directory / "package.json"
            )
            result = self.run_checker(repo=repo, scope="typescript")

        self.assert_failed_with(result, "exists while its policy stage is 'planned'")

    def test_typescript_scope_requires_a_reserved_workspace_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_directory:
            repo = Path(temp_directory)
            (repo / "pnpm-workspace.yaml").write_text(
                "packages:\n  - console\n", encoding="utf-8"
            )
            result = self.run_checker(repo=repo, scope="typescript")

        self.assert_failed_with(result, "must reserve 'sdk/typescript'")


if __name__ == "__main__":
    unittest.main()

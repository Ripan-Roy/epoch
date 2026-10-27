"""Keep the complete compression dependency graph above its security floors."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import io
import json
import re
import shutil
import tarfile
import tempfile
import tomllib
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
SECURITY_FLOORS = {
    "zstd": (0, 14, 3),
    "zstd-safe": (8, 2, 0),
    "zstd-sys": (2, 1, 2),
}
VERIFIER_SPEC = importlib.util.spec_from_file_location(
    "verify_vendored_dependencies", ROOT / "scripts/verify-vendored-dependencies.py"
)
assert VERIFIER_SPEC is not None and VERIFIER_SPEC.loader is not None
VERIFIER = importlib.util.module_from_spec(VERIFIER_SPEC)
VERIFIER_SPEC.loader.exec_module(VERIFIER)


def stable_version(value: str) -> tuple[int, int, int]:
    match = re.fullmatch(
        r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?",
        value,
    )
    if match is None:
        raise ValueError(f"expected stable major.minor.patch version, got {value!r}")
    return tuple(int(part) for part in match.groups())


def verify_security_graph(manifest: dict, lockfile: dict) -> None:
    dependencies = manifest["workspace"]["dependencies"]
    requirement = dependencies.get("zstd")
    requirement = (
        requirement.get("version") if isinstance(requirement, dict) else requirement
    )
    if not isinstance(requirement, str):
        raise ValueError("missing workspace zstd security floor")
    match = re.fullmatch(r"[=^]?([0-9]+\.[0-9]+(?:\.[0-9]+)?)", requirement)
    if match is None:
        raise ValueError(f"unsupported workspace zstd requirement {requirement!r}")
    minimum = match[1]
    if minimum.count(".") == 1:
        minimum += ".0"
    if stable_version(minimum) < SECURITY_FLOORS["zstd"]:
        raise ValueError("workspace zstd requirement is below 0.14.3")
    if "zstd" not in dependencies["kafka-protocol"].get("features", []):
        raise ValueError("Kafka Zstandard compression must remain enabled")
    if "binlog" not in dependencies["mysql_async"].get("features", []):
        raise ValueError("MySQL CDC binlog support must remain enabled")
    packages = lockfile.get("package", [])
    for name, floor in SECURITY_FLOORS.items():
        versions = [
            package["version"] for package in packages if package["name"] == name
        ]
        if len(versions) != 1:
            raise ValueError(f"expected exactly one locked {name}, got {versions}")
        if stable_version(versions[0]) < floor:
            raise ValueError(
                f"locked {name} {versions[0]} is below security floor {floor}"
            )
    for package, required in (
        ("kafka-protocol", "zstd"),
        ("mysql_async", "mysql_common"),
        ("mysql_common", "zstd"),
        ("zstd", "zstd-safe"),
        ("zstd-safe", "zstd-sys"),
    ):
        entries = [entry for entry in packages if entry["name"] == package]
        if len(entries) != 1:
            raise ValueError(f"expected exactly one locked {package}")
        edges = {value.split(" ")[0] for value in entries[0].get("dependencies", [])}
        if required not in edges:
            raise ValueError(
                f"required locked compression edge {package} -> {required} is missing"
            )


class DependencySecurityGraphTests(unittest.TestCase):
    def test_repository_compression_graph_has_no_vulnerable_parallel_versions(
        self,
    ) -> None:
        manifest = tomllib.loads((ROOT / "Cargo.toml").read_text())
        lockfile = tomllib.loads((ROOT / "Cargo.lock").read_text())
        verify_security_graph(manifest, lockfile)

    def test_stable_version_accepts_native_library_build_metadata(self) -> None:
        self.assertEqual(stable_version("2.1.2+zstd.1.5.7"), (2, 1, 2))
        for value in (
            "0.14",
            "0.14.3-rc.1",
            "0.14.3 trailing",
            "01.14.3",
            "2.1.2+zstd..7",
        ):
            with self.subTest(version=value), self.assertRaises(ValueError):
                stable_version(value)

    def test_repository_sources_are_the_reviewed_pin_and_narrow_mysql_patch(
        self,
    ) -> None:
        manifest = tomllib.loads((ROOT / "Cargo.toml").read_text())
        kafka = manifest["workspace"]["dependencies"]["kafka-protocol"]
        self.assertEqual(
            kafka["git"], "https://github.com/kafka-protocol-rs/kafka-protocol-rs.git"
        )
        self.assertEqual(kafka["rev"], "7faf833aeb152d3b92f9bd50074a764c674f17b5")
        self.assertEqual(
            manifest["patch"]["crates-io"]["mysql_common"],
            {"path": "third_party/mysql_common-0.37.3"},
        )

    def test_absent_old_and_parallel_locked_packages_are_rejected(self) -> None:
        manifest, lockfile = self.safe_graph()
        for name in SECURITY_FLOORS:
            with self.subTest(package=name, case="absent"):
                missing = {
                    "package": [p for p in lockfile["package"] if p["name"] != name]
                }
                with self.assertRaisesRegex(ValueError, "exactly one"):
                    verify_security_graph(manifest, missing)
            for parallel in (False, True):
                with self.subTest(package=name, parallel=parallel):
                    changed = copy.deepcopy(lockfile)
                    if parallel:
                        changed["package"].append({"name": name, "version": "0.0.1"})
                    else:
                        next(p for p in changed["package"] if p["name"] == name)[
                            "version"
                        ] = "0.0.1"
                    with self.assertRaises(ValueError):
                        verify_security_graph(manifest, changed)

    def test_manifest_floor_and_advertised_features_cannot_be_removed(self) -> None:
        for requirement in ("0.13", "0.14.2", "^0.14.0"):
            manifest, lockfile = self.safe_graph()
            manifest["workspace"]["dependencies"]["zstd"]["version"] = requirement
            with (
                self.subTest(requirement=requirement),
                self.assertRaisesRegex(ValueError, "below"),
            ):
                verify_security_graph(manifest, lockfile)
        manifest, lockfile = self.safe_graph()
        del manifest["workspace"]["dependencies"]["zstd"]
        with self.assertRaisesRegex(ValueError, "missing"):
            verify_security_graph(manifest, lockfile)
        for package in ("kafka-protocol", "mysql_async"):
            manifest, lockfile = self.safe_graph()
            manifest["workspace"]["dependencies"][package]["features"] = []
            with (
                self.subTest(package=package),
                self.assertRaisesRegex(ValueError, "remain enabled"),
            ):
                verify_security_graph(manifest, lockfile)
        manifest, lockfile = self.safe_graph()
        verify_security_graph(manifest, lockfile)

    def test_locked_edges_cannot_hide_disabled_compression(self) -> None:
        _, safe = self.safe_graph()
        for entry in safe["package"]:
            if not entry.get("dependencies"):
                continue
            manifest, lockfile = self.safe_graph()
            next(p for p in lockfile["package"] if p["name"] == entry["name"])[
                "dependencies"
            ] = []
            with (
                self.subTest(package=entry["name"]),
                self.assertRaisesRegex(ValueError, "edge"),
            ):
                verify_security_graph(manifest, lockfile)

    @staticmethod
    def safe_graph() -> tuple[dict, dict]:
        return (
            {
                "workspace": {
                    "dependencies": {
                        "zstd": {"version": "0.14.3"},
                        "kafka-protocol": {"features": ["zstd"]},
                        "mysql_async": {"features": ["binlog"]},
                    }
                }
            },
            {
                "package": [
                    {
                        "name": "kafka-protocol",
                        "version": "0.18.0",
                        "dependencies": ["zstd"],
                    },
                    {
                        "name": "mysql_async",
                        "version": "0.37.1",
                        "dependencies": ["mysql_common"],
                    },
                    {
                        "name": "mysql_common",
                        "version": "0.37.3",
                        "dependencies": ["zstd"],
                    },
                    {
                        "name": "zstd",
                        "version": "0.14.3",
                        "dependencies": ["zstd-safe"],
                    },
                    {
                        "name": "zstd-safe",
                        "version": "8.2.0",
                        "dependencies": ["zstd-sys"],
                    },
                    {"name": "zstd-sys", "version": "2.1.2+zstd.1.5.7"},
                ]
            },
        )


class VendoredDependencyProvenanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name) / VERIFIER.PREFIX
        shutil.copytree(VERIFIER.VENDOR, self.directory)
        manifest = self.directory / "Cargo.toml"
        # Keep isolated fixtures usable during the initial RED-first migration.
        manifest.write_bytes(
            manifest.read_bytes().replace(
                VERIFIER.OLD_REQUIREMENT, VERIFIER.NEW_REQUIREMENT, 1
            )
        )

    def test_repository_vendor_matches_the_approved_source_and_only_patch(self) -> None:
        VERIFIER.verify_vendor()

    def test_pristine_isolated_copy_is_valid(self) -> None:
        VERIFIER.verify_vendor(self.directory)

    def test_source_and_inventory_tampering_cannot_authorize_each_other(self) -> None:
        source = self.directory / "src/lib.rs"
        source.write_bytes(source.read_bytes() + b"\n// unapproved change\n")
        with self.assertRaisesRegex(VERIFIER.VerificationError, "content checksum"):
            VERIFIER.verify_vendor(self.directory)
        record_path = self.directory / VERIFIER.RECORD
        record = json.loads(record_path.read_text())
        record["files"]["src/lib.rs"] = VERIFIER.sha256(source.read_bytes())
        record_path.write_text(json.dumps(record))
        with self.assertRaisesRegex(
            VERIFIER.VerificationError, "inventory does not match"
        ):
            VERIFIER.verify_vendor(self.directory)

    def test_original_manifest_and_licenses_are_immutable(self) -> None:
        names = ["Cargo.toml.orig"] + [
            path.name
            for path in self.directory.iterdir()
            if path.name.startswith("LICENSE")
        ]
        self.assertGreater(len(names), 1)
        for name in names:
            path = self.directory / name
            original = path.read_bytes()
            path.write_bytes(original + b"\nunapproved\n")
            with (
                self.subTest(file=name),
                self.assertRaisesRegex(VERIFIER.VerificationError, "content checksum"),
            ):
                VERIFIER.verify_vendor(self.directory)
            path.write_bytes(original)

    def test_manifest_rollback_or_unrelated_patch_is_rejected(self) -> None:
        path = self.directory / "Cargo.toml"
        approved = path.read_bytes()
        for contents in (
            approved.replace(VERIFIER.NEW_REQUIREMENT, VERIFIER.OLD_REQUIREMENT, 1),
            approved + b"\n# unrelated modification\n",
        ):
            path.write_bytes(contents)
            with self.assertRaisesRegex(VERIFIER.VerificationError, "Cargo.toml"):
                VERIFIER.verify_vendor(self.directory)

    def test_missing_and_extra_files_are_rejected(self) -> None:
        source = self.directory / "src/lib.rs"
        original = source.read_bytes()
        source.unlink()
        with self.assertRaisesRegex(VERIFIER.VerificationError, "inventory mismatch"):
            VERIFIER.verify_vendor(self.directory)
        source.write_bytes(original)
        (self.directory / "Cargo.lock").write_text("unapproved lockfile")
        with self.assertRaisesRegex(VERIFIER.VerificationError, "inventory mismatch"):
            VERIFIER.verify_vendor(self.directory)

    def test_file_directory_and_root_symlinks_are_rejected(self) -> None:
        source = self.directory / "src/lib.rs"
        target = Path(self.temporary.name) / "outside.rs"
        target.write_bytes(source.read_bytes())
        source.unlink()
        source.symlink_to(target)
        with self.assertRaisesRegex(VERIFIER.VerificationError, "symlink"):
            VERIFIER.verify_vendor(self.directory)
        source.unlink()
        source.write_bytes(target.read_bytes())
        (self.directory / "linked-directory").symlink_to(
            self.directory / "src", target_is_directory=True
        )
        with self.assertRaisesRegex(VERIFIER.VerificationError, "symlink"):
            VERIFIER.verify_vendor(self.directory)
        root_link = Path(self.temporary.name) / "linked-root"
        root_link.symlink_to(self.directory, target_is_directory=True)
        with self.assertRaisesRegex(VERIFIER.VerificationError, "real directory"):
            VERIFIER.verify_vendor(root_link)

    def test_provenance_identity_and_inventory_are_fixed(self) -> None:
        path = self.directory / VERIFIER.RECORD
        original = json.loads(path.read_text())
        for key, value in (
            ("package", "other_package"),
            ("version", "0.38.2"),
            ("source", "https://example.invalid/other.crate"),
            ("archive_sha256", "0" * 64),
            ("schema", 2),
            ("patched_manifest_sha256", "0" * 64),
        ):
            record = copy.deepcopy(original)
            record[key] = value
            path.write_text(json.dumps(record))
            with self.subTest(field=key), self.assertRaises(VERIFIER.VerificationError):
                VERIFIER.verify_vendor(self.directory)
        record = copy.deepcopy(original)
        record["files"]["../outside.rs"] = "0" * 64
        path.write_text(json.dumps(record))
        with self.assertRaisesRegex(VERIFIER.VerificationError, "unsafe"):
            VERIFIER.verify_vendor(self.directory)

    def test_archive_checksum_is_checked_before_tar_parsing(self) -> None:
        archive = Path(self.temporary.name) / "bad.crate"
        archive.write_bytes(b"not an official archive")
        with mock.patch.object(VERIFIER.tarfile, "open") as parser:
            with self.assertRaisesRegex(VERIFIER.VerificationError, "archive checksum"):
                VERIFIER.read_upstream_archive(archive)
            parser.assert_not_called()

    def test_archive_paths_and_symlinks_are_rejected_without_extraction(self) -> None:
        for name, symlink in (
            ("../escape", False),
            ("other_package/file", False),
            (f"{VERIFIER.PREFIX}/../escape", False),
            (f"{VERIFIER.PREFIX}/linked", True),
        ):
            payload = io.BytesIO()
            with tarfile.open(fileobj=payload, mode="w:gz") as archive:
                member = tarfile.TarInfo(name)
                if symlink:
                    member.type = tarfile.SYMTYPE
                    member.linkname = "../../escape"
                    archive.addfile(member)
                else:
                    member.size = 1
                    archive.addfile(member, io.BytesIO(b"x"))
            archive_path = Path(self.temporary.name) / "unsafe.crate"
            archive_path.write_bytes(payload.getvalue())
            digest = hashlib.sha256(payload.getvalue()).hexdigest()
            with (
                self.subTest(member=name),
                mock.patch.object(VERIFIER, "ARCHIVE_SHA256", digest),
                self.assertRaises(VERIFIER.VerificationError),
            ):
                VERIFIER.read_upstream_archive(archive_path)


if __name__ == "__main__":
    unittest.main()

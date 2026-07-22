#!/usr/bin/env python3
"""Validate Epoch's non-publishing package boundary.

The policy deliberately distinguishes a mechanically testable package shape from
permission to publish. It uses only the Python standard library so the same
checks run locally, in CI, and in small negative-test fixtures.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
import xml.etree.ElementTree as ElementTree
from pathlib import Path
from typing import Any


PACKAGE_KEYS = frozenset({"rust", "go", "java", "python", "typescript"})
PUBLICATION_STATUSES = frozenset({"blocked", "planned", "ready", "published"})
PACKAGE_STAGES = frozenset({"planned", "repository-only", "package-ready", "published"})
BLOCKER_ID_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
ALPHA_VERSION_PATTERN = re.compile(
    r"^(?P<major>0|[1-9][0-9]*)\."
    r"(?P<minor>0|[1-9][0-9]*)\."
    r"(?P<patch>0|[1-9][0-9]*)-alpha\."
    r"(?P<alpha>0|[1-9][0-9]*)$"
)


class PolicyError(ValueError):
    """Raised when package policy or repository state violates a boundary."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise PolicyError(message)


def require_mapping(value: object, context: str) -> dict[str, Any]:
    require(isinstance(value, dict), f"{context} must be a JSON object")
    return value


def require_list(value: object, context: str) -> list[Any]:
    require(isinstance(value, list), f"{context} must be a JSON array")
    return value


def require_non_empty_string(value: object, context: str) -> str:
    require(
        isinstance(value, str) and bool(value.strip()),
        f"{context} must be a non-empty string",
    )
    return value


def load_json(path: Path, context: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise PolicyError(f"{context} does not exist: {path}") from error
    except json.JSONDecodeError as error:
        raise PolicyError(f"{context} is not valid JSON: {error}") from error
    return require_mapping(value, context)


def load_text(path: Path, context: str) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise PolicyError(f"{context} does not exist: {path}") from error


def load_toml(path: Path, context: str) -> dict[str, Any]:
    try:
        value = tomllib.loads(load_text(path, context))
    except tomllib.TOMLDecodeError as error:
        raise PolicyError(f"{context} is not valid TOML: {error}") from error
    return require_mapping(value, context)


def require_string_list(value: object, context: str) -> list[str]:
    entries = require_list(value, context)
    require(
        all(isinstance(entry, str) and bool(entry) for entry in entries),
        f"{context} must contain only non-empty strings",
    )
    strings = list(entries)
    require(len(strings) == len(set(strings)), f"{context} must not contain duplicates")
    return strings


def package_policy(policy: dict[str, Any], ecosystem: str) -> dict[str, Any]:
    packages = require_mapping(policy.get("packages"), "packages")
    return require_mapping(packages.get(ecosystem), f"packages.{ecosystem}")


def validate_blockers(policy: dict[str, Any]) -> set[str]:
    blockers = require_list(policy.get("blockers"), "blockers")

    blocker_ids: set[str] = set()
    for index, raw_blocker in enumerate(blockers):
        blocker = require_mapping(raw_blocker, f"blockers[{index}]")
        blocker_id = require_non_empty_string(
            blocker.get("id"), f"blockers[{index}].id"
        )
        require(
            BLOCKER_ID_PATTERN.fullmatch(blocker_id) is not None,
            f"blocker id '{blocker_id}' must use lowercase kebab-case",
        )
        require(blocker_id not in blocker_ids, f"duplicate blocker id '{blocker_id}'")
        require_non_empty_string(blocker.get("label"), f"blocker '{blocker_id}' label")
        require_non_empty_string(
            blocker.get("summary"), f"blocker '{blocker_id}' summary"
        )
        blocker_ids.add(blocker_id)
    return blocker_ids


def validate_common_package_fields(
    ecosystem: str, package: dict[str, Any], blocker_ids: set[str]
) -> set[str]:
    context = f"packages.{ecosystem}"
    require_non_empty_string(package.get("label"), f"{context}.label")
    require_non_empty_string(package.get("version"), f"{context}.version")

    publication_status = package.get("publicationStatus")
    require(
        publication_status in PUBLICATION_STATUSES,
        f"{context}.publicationStatus must be one of {sorted(PUBLICATION_STATUSES)}",
    )
    stage = package.get("stage")
    require(
        stage in PACKAGE_STAGES,
        f"{context}.stage must be one of {sorted(PACKAGE_STAGES)}",
    )

    for coordinate_field in ("coordinate", "provisionalCoordinate"):
        coordinate = package.get(coordinate_field)
        require(
            coordinate is None
            or (isinstance(coordinate, str) and bool(coordinate.strip())),
            f"{context}.{coordinate_field} must be null or a non-empty string",
        )

    referenced_blockers = require_string_list(
        package.get("blockerIds"), f"{context}.blockerIds"
    )
    for blocker_id in referenced_blockers:
        require(
            blocker_id in blocker_ids,
            f"{context} references unknown blocker '{blocker_id}'",
        )

    if publication_status in {"ready", "published"}:
        require(
            not referenced_blockers, f"{context} cannot be ready while blockers remain"
        )
        require(
            package.get("coordinate") is not None,
            f"{context} needs a permanent coordinate",
        )
    else:
        require(
            bool(referenced_blockers),
            f"{context} must explain why publication is not ready",
        )

    return set(referenced_blockers)


def expected_python_version(release_version: str) -> str:
    match = ALPHA_VERSION_PATTERN.fullmatch(release_version)
    require(match is not None, "release.version must use MAJOR.MINOR.PATCH-alpha.N")
    return (
        f"{match.group('major')}.{match.group('minor')}.{match.group('patch')}"
        f"a{match.group('alpha')}"
    )


def validate_policy(policy: dict[str, Any], mode: str) -> None:
    require(policy.get("schemaVersion") == 1, "schemaVersion must be 1")
    release = require_mapping(policy.get("release"), "release")
    release_version = require_non_empty_string(
        release.get("version"), "release.version"
    )
    expected_python = expected_python_version(release_version)

    channel = release.get("channel")
    require(
        channel in {"source-only", "registry-candidate"},
        "release.channel is not supported",
    )
    require(
        release.get("goTag") == f"v{release_version}",
        "release.goTag must be v<release.version>",
    )

    blocker_ids = validate_blockers(policy)
    packages = require_mapping(policy.get("packages"), "packages")
    require(
        set(packages) == PACKAGE_KEYS,
        f"packages must contain exactly {sorted(PACKAGE_KEYS)}",
    )

    used_blockers: set[str] = set()
    for ecosystem in sorted(PACKAGE_KEYS):
        package = require_mapping(packages[ecosystem], f"packages.{ecosystem}")
        used_blockers.update(
            validate_common_package_fields(ecosystem, package, blocker_ids)
        )

        expected_version = expected_python if ecosystem == "python" else release_version
        if ecosystem == "python":
            require(
                package.get("version") == expected_version,
                f"Python version must be '{expected_version}' for release '{release_version}'",
            )
        else:
            require(
                package.get("version") == expected_version,
                f"packages.{ecosystem}.version must match release.version",
            )

    require(
        used_blockers == blocker_ids,
        "every declared blocker must be referenced; "
        f"unused={sorted(blocker_ids - used_blockers)}, undefined={sorted(used_blockers - blocker_ids)}",
    )

    rust = package_policy(policy, "rust")
    public_crates = require_string_list(
        rust.get("publicCrates"), "packages.rust.publicCrates"
    )
    private_crates = require_string_list(
        rust.get("privateCrates"), "packages.rust.privateCrates"
    )
    require(
        public_crates == sorted(public_crates),
        "packages.rust.publicCrates must be sorted",
    )
    require(
        private_crates == sorted(private_crates),
        "packages.rust.privateCrates must be sorted",
    )
    require(
        not set(public_crates).intersection(private_crates),
        "Rust public and private crate lists must be disjoint",
    )

    if channel == "source-only":
        require(
            not public_crates, "a source-only release cannot declare public Rust crates"
        )
        for ecosystem in sorted(PACKAGE_KEYS):
            package = package_policy(policy, ecosystem)
            require(
                package.get("coordinate") is None,
                f"source-only packages.{ecosystem}.coordinate must remain null",
            )
            require(
                package.get("publicationStatus") not in {"ready", "published"},
                f"source-only packages.{ecosystem} cannot be publication ready",
            )

    if mode == "release":
        require(channel != "source-only", "release channel is source-only")
        blocked = [
            ecosystem
            for ecosystem in sorted(PACKAGE_KEYS)
            if package_policy(policy, ecosystem).get("publicationStatus") != "ready"
        ]
        require(
            not blocked,
            f"package publication remains blocked for: {', '.join(blocked)}",
        )


def validate_rust(policy: dict[str, Any], metadata_path: Path | None) -> None:
    require(
        metadata_path is not None, "--cargo-metadata is required for the Rust scope"
    )
    metadata = load_json(metadata_path, "Cargo metadata")
    package_records = require_list(metadata.get("packages"), "Cargo metadata packages")
    workspace_members = set(
        require_string_list(
            metadata.get("workspace_members"), "Cargo metadata workspace_members"
        )
    )

    workspace_packages: dict[str, dict[str, Any]] = {}
    for index, raw_package in enumerate(package_records):
        package = require_mapping(raw_package, f"Cargo metadata packages[{index}]")
        package_id = package.get("id")
        if package_id not in workspace_members:
            continue
        name = require_non_empty_string(
            package.get("name"), f"Cargo package {package_id} name"
        )
        require(
            name not in workspace_packages,
            f"duplicate Rust workspace package name '{name}'",
        )
        workspace_packages[name] = package

    require(
        len(workspace_packages) == len(workspace_members),
        "Cargo metadata workspace members must all resolve to package records",
    )

    rust = package_policy(policy, "rust")
    public_crates = set(
        require_string_list(rust.get("publicCrates"), "packages.rust.publicCrates")
    )
    private_crates = set(
        require_string_list(rust.get("privateCrates"), "packages.rust.privateCrates")
    )
    expected_crates = public_crates | private_crates
    actual_crates = set(workspace_packages)
    require(
        expected_crates == actual_crates,
        "Rust package policy does not match workspace members: "
        f"missing={sorted(actual_crates - expected_crates)}, stale={sorted(expected_crates - actual_crates)}",
    )

    expected_version = require_non_empty_string(
        rust.get("version"), "packages.rust.version"
    )
    for name in sorted(actual_crates):
        package = workspace_packages[name]
        require(
            package.get("version") == expected_version,
            f"Rust package '{name}' must use version {expected_version}",
        )

        publish = package.get("publish")
        if name in private_crates:
            require(
                publish == [],
                f"private Rust package '{name}' must declare publish = false",
            )
            continue

        require(
            publish == ["crates-io"],
            f"public Rust package '{name}' must declare publish = [\"crates-io\"]",
        )
        validate_public_rust_package(name, package, public_crates, expected_version)


def validate_public_rust_package(
    name: str,
    package: dict[str, Any],
    public_crates: set[str],
    expected_version: str,
) -> None:
    require_non_empty_string(
        package.get("description"), f"public Rust package '{name}' description"
    )
    require(
        bool(package.get("license") or package.get("license_file")),
        f"public Rust package '{name}' needs a selected license or license-file",
    )
    require_non_empty_string(
        package.get("repository"), f"public Rust package '{name}' repository"
    )
    require(
        bool(package.get("readme") or package.get("documentation")),
        f"public Rust package '{name}' needs readme or documentation metadata",
    )

    dependencies = require_list(
        package.get("dependencies"), f"Rust package '{name}' dependencies"
    )
    for index, raw_dependency in enumerate(dependencies):
        dependency = require_mapping(
            raw_dependency, f"Rust package '{name}' dependency {index}"
        )
        if dependency.get("kind") == "dev":
            continue
        source = dependency.get("source")
        require(
            not (isinstance(source, str) and source.startswith("git+")),
            f"public Rust package '{name}' cannot rely on git dependency '{dependency.get('name')}'",
        )
        dependency_name = dependency.get("name")
        if dependency.get("path") is None or not isinstance(dependency_name, str):
            continue
        require(
            dependency_name in public_crates,
            f"public Rust package '{name}' depends on private workspace package '{dependency_name}'",
        )
        require(
            dependency.get("req") == f"={expected_version}",
            f"public Rust dependency '{name} -> {dependency_name}' must use ={expected_version}",
        )


def read_go_module(go_mod: Path) -> str:
    try:
        contents = go_mod.read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise PolicyError(f"Go module file does not exist: {go_mod}") from error
    match = re.search(r"^\s*module\s+(\S+)\s*$", contents, flags=re.MULTILINE)
    require(match is not None, f"Go module file has no module directive: {go_mod}")
    return match.group(1)


def validate_go(policy: dict[str, Any], repo: Path, mode: str) -> None:
    go_policy = package_policy(policy, "go")
    actual_module = read_go_module(repo / "go.mod")

    if mode == "shape":
        expected_module = go_policy.get("provisionalCoordinate")
        require(
            isinstance(expected_module, str),
            "shape-mode Go policy needs a provisionalCoordinate",
        )
        require(
            actual_module == expected_module,
            f"Go module must remain '{expected_module}' until a permanent path is selected; "
            f"found '{actual_module}'",
        )
    else:
        expected_module = go_policy.get("coordinate")
        require(
            isinstance(expected_module, str),
            "release-mode Go policy needs a permanent coordinate",
        )
        require(
            actual_module == expected_module, f"Go module must be '{expected_module}'"
        )
        require(
            not actual_module.startswith("epoch.local/"),
            "release Go module cannot be provisional",
        )


def require_manifest_version(
    manifest_path: str, actual_version: object, expected_version: str
) -> None:
    require(
        actual_version == expected_version,
        f"{manifest_path} version must be '{expected_version}'; found {actual_version!r}",
    )


def captured_value(contents: str, pattern: str, context: str) -> str:
    match = re.search(pattern, contents)
    require(match is not None, f"{context} is missing")
    return match.group(1)


def validate_repository_versions(policy: dict[str, Any], repo: Path) -> None:
    release_version = require_non_empty_string(
        require_mapping(policy.get("release"), "release").get("version"),
        "release.version",
    )
    python_version = expected_python_version(release_version)

    cargo_manifest = load_toml(repo / "Cargo.toml", "Cargo.toml")
    workspace = require_mapping(cargo_manifest.get("workspace"), "Cargo.toml workspace")
    workspace_package = require_mapping(
        workspace.get("package"), "Cargo.toml workspace.package"
    )
    require_manifest_version(
        "Cargo.toml workspace.package",
        workspace_package.get("version"),
        release_version,
    )

    for relative_path in ("package.json", "console/package.json"):
        manifest = load_json(repo / relative_path, relative_path)
        require_manifest_version(
            relative_path, manifest.get("version"), release_version
        )
        require(
            manifest.get("private") is True,
            f"{relative_path} must remain private",
        )

    java_path = repo / "sdk" / "java" / "pom.xml"
    try:
        java_root = ElementTree.fromstring(load_text(java_path, "sdk/java/pom.xml"))
    except ElementTree.ParseError as error:
        raise PolicyError(f"sdk/java/pom.xml is not valid XML: {error}") from error
    namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
    java_version = java_root.findtext("m:version", namespaces=namespace)
    java_group = java_root.findtext("m:groupId", namespaces=namespace)
    java_artifact = java_root.findtext("m:artifactId", namespaces=namespace)
    require_manifest_version("sdk/java/pom.xml", java_version, release_version)
    expected_java_coordinate = package_policy(policy, "java").get(
        "coordinate"
    ) or package_policy(policy, "java").get("provisionalCoordinate")
    require(
        isinstance(expected_java_coordinate, str),
        "Java package policy needs a coordinate",
    )
    require(
        f"{java_group}:{java_artifact}" == expected_java_coordinate,
        f"sdk/java/pom.xml coordinate must be '{expected_java_coordinate}'",
    )

    python_manifest = load_toml(
        repo / "sdk" / "python" / "pyproject.toml", "sdk/python/pyproject.toml"
    )
    python_project = require_mapping(
        python_manifest.get("project"), "sdk/python/pyproject.toml project"
    )
    require_manifest_version(
        "sdk/python/pyproject.toml", python_project.get("version"), python_version
    )
    expected_python_coordinate = package_policy(policy, "python").get(
        "coordinate"
    ) or package_policy(policy, "python").get("provisionalCoordinate")
    require(
        python_project.get("name") == expected_python_coordinate,
        "sdk/python/pyproject.toml name must match the package policy coordinate",
    )

    user_agents = (
        (
            "sdk/go/epoch/transport.go",
            r'userAgent\s*=\s*"([^"]+)"',
            f"epoch-go/{release_version}",
        ),
        (
            "sdk/java/src/main/java/io/epoch/sdk/HttpTransport.java",
            r'USER_AGENT\s*=\s*"([^"]+)"',
            f"epoch-java/{release_version}",
        ),
        (
            "sdk/python/src/epoch_sdk/transport.py",
            r'"user-agent"\s*:\s*"([^"]+)"',
            f"epoch-python/{python_version}",
        ),
    )
    for relative_path, pattern, expected_user_agent in user_agents:
        actual_user_agent = captured_value(
            load_text(repo / relative_path, relative_path),
            pattern,
            f"{relative_path} user agent",
        )
        require(
            actual_user_agent == expected_user_agent,
            f"{relative_path} user agent must be '{expected_user_agent}'; "
            f"found '{actual_user_agent}'",
        )


def workspace_reserves_typescript(workspace_path: Path) -> bool:
    try:
        contents = workspace_path.read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise PolicyError(
            f"pnpm workspace file does not exist: {workspace_path}"
        ) from error
    return any(
        re.fullmatch(r"\s*-\s*['\"]?sdk/typescript['\"]?\s*", line) is not None
        for line in contents.splitlines()
    )


def validate_typescript(policy: dict[str, Any], repo: Path) -> None:
    typescript = package_policy(policy, "typescript")
    require(
        workspace_reserves_typescript(repo / "pnpm-workspace.yaml"),
        "pnpm-workspace.yaml must reserve 'sdk/typescript'",
    )

    manifest_path = repo / "sdk" / "typescript" / "package.json"
    if not manifest_path.exists():
        require(
            typescript.get("stage") == "planned",
            "TypeScript package is absent but its policy stage is not 'planned'",
        )
        return

    require(
        typescript.get("stage") != "planned",
        f"{manifest_path} exists while its policy stage is 'planned'",
    )
    manifest = load_json(manifest_path, "TypeScript package manifest")
    require(
        manifest.get("private") is True,
        "TypeScript SDK must remain private until publication approval",
    )
    require(
        manifest.get("version") == typescript.get("version"),
        "TypeScript SDK version must match package policy",
    )
    selected_name = typescript.get("coordinate") or typescript.get(
        "provisionalCoordinate"
    )
    require(
        isinstance(selected_name, str),
        "TypeScript package policy needs a reviewed package name",
    )
    require(
        manifest.get("name") == selected_name,
        "TypeScript SDK name must match package policy",
    )

    required_fields = {
        "type",
        "files",
        "exports",
        "types",
        "sideEffects",
        "engines",
        "repository",
    }
    missing_fields = sorted(field for field in required_fields if field not in manifest)
    require(
        not missing_fields,
        f"TypeScript SDK manifest is missing fields: {missing_fields}",
    )
    scripts = require_mapping(manifest.get("scripts"), "TypeScript SDK scripts")
    missing_scripts = sorted({"build", "lint", "test", "typecheck"} - set(scripts))
    require(
        not missing_scripts,
        f"TypeScript SDK manifest is missing scripts: {missing_scripts}",
    )


def parse_arguments() -> argparse.Namespace:
    script_root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo", type=Path, default=script_root, help="repository root"
    )
    parser.add_argument(
        "--policy",
        type=Path,
        default=script_root / "release" / "package-readiness.json",
        help="package-readiness policy JSON",
    )
    parser.add_argument(
        "--scope",
        choices=("policy", "versions", "rust", "go", "typescript"),
        default="policy",
        help="repository boundary to validate",
    )
    parser.add_argument(
        "--mode",
        choices=("shape", "release"),
        default="shape",
        help="shape checks accept documented blockers; release checks reject them",
    )
    parser.add_argument(
        "--cargo-metadata", type=Path, help="cargo metadata JSON for Rust checks"
    )
    return parser.parse_args()


def main() -> int:
    arguments = parse_arguments()
    try:
        policy = load_json(arguments.policy.resolve(), "package policy")
        validate_policy(policy, arguments.mode)
        print("package policy: valid")

        repo = arguments.repo.resolve()
        if arguments.scope == "versions":
            validate_repository_versions(policy, repo)
            print("repository version mapping: valid")
        elif arguments.scope == "rust":
            validate_rust(policy, arguments.cargo_metadata)
            print("Rust package boundary: valid")
        elif arguments.scope == "go":
            validate_go(policy, repo, arguments.mode)
            print("Go module boundary: valid")
        elif arguments.scope == "typescript":
            validate_typescript(policy, repo)
            stage = package_policy(policy, "typescript")["stage"]
            print(f"TypeScript package boundary: {stage}")
    except PolicyError as error:
        print(f"package policy error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

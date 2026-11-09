"""Install exact Go Protobuf generators into a new, private tool directory.

Public module downloads retain SumDB verification and additionally match the
committed source checksums. No repository dependencies or global binaries are
changed. Installed tools are reused offline after their versions/build metadata
have been checked; generation never calls the Buf remote plugin service.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOOL_DIRECTORY = ROOT / ".cache/go-protobuf-1.36.11-1.5.1-source-v1"
GO_VERSION = "go1.26.5"
SOURCE_BUILD_TAG = "epoch_codegen_clean_cache_v1"
TOOLS = {
    "protoc-gen-go": {
        "module": "google.golang.org/protobuf",
        "package": "google.golang.org/protobuf/cmd/protoc-gen-go",
        "version": "v1.36.11",
        "sum": "h1:fV6ZwhNocDyBLK0dj+fg8ektcVegBBuEolpbTQyBNVE=",
        "mod_sum": "h1:HTf+CrKn2C3g5S8VImy6tdcUvCska2kB7j23XfzDpco=",
        "banner": "protoc-gen-go v1.36.11",
    },
    "protoc-gen-go-grpc": {
        "module": "google.golang.org/grpc/cmd/protoc-gen-go-grpc",
        "package": "google.golang.org/grpc/cmd/protoc-gen-go-grpc",
        "version": "v1.5.1",
        "sum": "h1:F29+wU6Ee6qgu9TddPgooOdaqsxTMunOoj8KA5yuS5A=",
        "mod_sum": "h1:5KF+wpkbTSbGcR9zteSqZV6fqFOWBl4Yde8En8MryZA=",
        "banner": "protoc-gen-go-grpc 1.5.1",
    },
}


def verify_tools(directory: Path) -> Path:
    """Verify regular executables, pinned source build metadata, and banners."""
    if not directory.is_absolute() or directory.is_symlink():
        raise SystemExit(
            "Go Protobuf tool directory must be an absolute regular directory"
        )
    for name, spec in TOOLS.items():
        binary = directory / "bin" / name
        if binary.is_symlink() or not binary.is_file():
            raise SystemExit(
                f"missing regular {name}; install pinned Go Protobuf tools first"
            )
        if not binary.stat().st_mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH):
            raise SystemExit(f"{name} must be executable")
        metadata = subprocess.run(
            ["go", "version", "-m", str(binary)],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        module = re.search(
            r"^\s*mod\s+(\S+)\s+(\S+)\s+(\S+)\s*$", metadata, re.MULTILINE
        )
        if module is None or module.groups() != (
            spec["module"],
            spec["version"],
            spec["sum"],
        ):
            raise SystemExit(f"{name} does not match pinned Go module build metadata")
        settings = dict(
            re.findall(r"^\s*build\s+(\S+?)=(\S+)\s*$", metadata, re.MULTILINE)
        )
        if (
            metadata.splitlines()[0] != f"{binary}: {GO_VERSION}"
            or settings.get("-tags") != SOURCE_BUILD_TAG
            or settings.get("-trimpath") != "true"
            or settings.get("CGO_ENABLED") != "0"
        ):
            raise SystemExit(f"{name} does not match the pinned clean source build")
        banner = subprocess.run(
            [str(binary), "--version"], capture_output=True, text=True, check=True
        ).stdout.strip()
        if banner != spec["banner"]:
            raise SystemExit(f"{name} does not match its pinned version")
    return directory / "bin"


def install(directory: Path) -> None:
    if not directory.is_absolute() or directory.name in ("", ".", ".."):
        raise SystemExit("tool destination must be an absolute new directory")
    if directory.exists() or directory.is_symlink():
        raise SystemExit("tool destination already exists")
    if not directory.parent.is_dir():
        raise SystemExit("tool destination parent must exist")
    version = subprocess.run(
        ["go", "version"], capture_output=True, text=True, check=True
    ).stdout
    if not version.startswith(f"go version {GO_VERSION} "):
        raise SystemExit(f"Go Protobuf installation requires {GO_VERSION}")
    with tempfile.TemporaryDirectory(
        prefix=".epoch-go-protobuf-", dir=directory.parent
    ) as staging:
        temporary = Path(staging)
        # Go's download metadata authenticates the archive, but an inherited
        # extracted module cache may have been modified after download. Never
        # compile this provenance-pinned tool from inherited source/build caches.
        caches = {
            "GOMODCACHE": temporary / "module-cache",
            "GOCACHE": temporary / "build-cache",
            "GOTMPDIR": temporary / "temporary-cache",
        }
        for cache in caches.values():
            cache.mkdir()
        environment = {
            **os.environ,
            **{name: str(path) for name, path in caches.items()},
            "GOBIN": str(temporary / "bin"),
            "GOENV": "off",
            "GOFLAGS": f"-trimpath -tags={SOURCE_BUILD_TAG}",
            "CGO_ENABLED": "0",
            "GOTOOLCHAIN": "local",
            "GOWORK": "off",
            "GOPROXY": "https://proxy.golang.org",
            "GOSUMDB": "sum.golang.org",
            "GONOSUMDB": "",
            "GONOPROXY": "",
            "GOPRIVATE": "",
        }
        for spec in TOOLS.values():
            downloaded = json.loads(
                subprocess.run(
                    [
                        "go",
                        "mod",
                        "download",
                        "-json",
                        f"{spec['module']}@{spec['version']}",
                    ],
                    cwd=temporary,
                    env=environment,
                    capture_output=True,
                    text=True,
                    check=True,
                ).stdout
            )
            if any(
                downloaded.get(key) != spec[expected]
                for key, expected in (
                    ("Path", "module"),
                    ("Version", "version"),
                    ("Sum", "sum"),
                    ("GoModSum", "mod_sum"),
                )
            ):
                raise SystemExit("Go Protobuf module source checksum mismatch")
            subprocess.run(
                ["go", "install", f"{spec['package']}@{spec['version']}"],
                cwd=temporary,
                env=environment,
                check=True,
            )
        verify_tools(temporary)
        directory.mkdir()
        try:
            (temporary / "bin").rename(directory / "bin")
        except BaseException:
            directory.rmdir()
            raise
    print(f"installed exact source-verified Go Protobuf tools at {directory}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        verify_tools(args.destination)
        print("Go Protobuf tools match pinned versions and source metadata")
    else:
        install(args.destination)


if __name__ == "__main__":
    main()

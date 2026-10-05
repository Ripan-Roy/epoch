"""Generate pinned Java management contracts, or verify without changing files."""

from __future__ import annotations

import argparse
import hashlib
import os
import platform
import stat
import subprocess
import tempfile
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "sdk/java/src/generated/java"
PLUGIN_VERSION = "1.84.0"
PLUGIN_DIRECTORY = ROOT / ".cache" / f"grpc-java-{PLUGIN_VERSION}"
PLUGIN_REPOSITORY = "https://repo.maven.apache.org/maven2/io/grpc/protoc-gen-grpc-java"
MAX_PLUGIN_BYTES = 32 * 1024 * 1024
PLUGIN_DIGESTS = {
    ("Linux", "x86_64"): (
        "linux-x86_64",
        "907c2d4efc2bae9b21cada75c31b36f3821e15b7be96311e214a6be65bcd20aa",
    ),
    ("Linux", "aarch64"): (
        "linux-aarch_64",
        "d9357761b4e69ddc7abec3d3a80e1096b5eda3e25d6ed6d0b1ead07066600a864",
    ),
    ("Darwin", "arm64"): (
        "osx-aarch_64",
        "49aab9e56a30a8484e9c32d36be12a351d5783e00bdaafe9ec2c2fd8c9029504",
    ),
    ("Darwin", "x86_64"): (
        "osx-x86_64",
        "49aab9e56a30a8484e9c32d36be12a351d5783e00bdaafe9ec2c2fd8c9029504",
    ),
}


def plugin_spec() -> tuple[str, str]:
    system, machine = platform.system(), platform.machine().lower()
    machine = (
        {"amd64": "x86_64", "arm64": "aarch64"}.get(machine, machine)
        if system == "Linux"
        else machine
    )
    spec = PLUGIN_DIGESTS.get((system, machine))
    if spec is None:
        raise SystemExit("unsupported platform for the pinned gRPC-Java compiler")
    return spec


def verify_plugin(path: Path) -> Path:
    if path.is_symlink() or not path.is_file():
        raise SystemExit(
            "gRPC-Java compiler must be an installed regular file; "
            "run scripts/generate-java-management.py --install-plugin "
            "with an absolute new directory"
        )
    attributes = path.stat()
    if not 0 < attributes.st_size <= MAX_PLUGIN_BYTES:
        raise SystemExit("invalid gRPC-Java compiler size")
    if not attributes.st_mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH):
        raise SystemExit("gRPC-Java compiler must be executable")
    _, expected = plugin_spec()
    if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise SystemExit("gRPC-Java compiler checksum mismatch")
    return path.resolve()


def install_plugin(destination: Path) -> None:
    """Install only into a new directory; never replace a user's compiler."""
    if not destination.is_absolute() or destination.name in ("", ".", ".."):
        raise SystemExit("compiler destination must be an absolute new directory")
    if destination.exists() or destination.is_symlink():
        raise SystemExit("compiler destination already exists")
    if not destination.parent.is_dir():
        raise SystemExit("compiler destination parent must exist")
    classifier, expected = plugin_spec()
    artifact = f"protoc-gen-grpc-java-{PLUGIN_VERSION}-{classifier}.exe"
    url = f"{PLUGIN_REPOSITORY}/{PLUGIN_VERSION}/{artifact}"
    with tempfile.TemporaryDirectory(
        prefix=".epoch-grpc-java-", dir=destination.parent
    ) as directory:
        temporary = Path(directory)
        with urllib.request.urlopen(url, timeout=30) as response:
            if response.geturl() != url:
                raise SystemExit("unexpected gRPC-Java compiler download redirect")
            content = response.read(MAX_PLUGIN_BYTES + 1)
        if not 0 < len(content) <= MAX_PLUGIN_BYTES:
            raise SystemExit("invalid gRPC-Java compiler download size")
        if hashlib.sha256(content).hexdigest() != expected:
            raise SystemExit("gRPC-Java compiler download checksum mismatch")
        staging = temporary / "toolchain"
        binary = staging / "bin" / "protoc-gen-grpc-java"
        binary.parent.mkdir(parents=True)
        binary.write_bytes(content)
        binary.chmod(0o755)
        verify_plugin(binary)
        # mkdir is exclusive, including against a concurrent installer. Rename
        # only our own bin directory into it; do not overwrite an existing path.
        destination.mkdir()
        try:
            binary.parent.rename(destination / "bin")
        except BaseException:
            destination.rmdir()
            raise
    print(f"installed checksum-pinned gRPC-Java {PLUGIN_VERSION}")


def generate() -> dict[Path, bytes]:
    version = subprocess.run(
        ["buf", "--version"], capture_output=True, text=True, check=True
    ).stdout.strip()
    if version != "1.72.0":
        raise SystemExit("Java generation requires Buf 1.72.0")
    protoc_version = subprocess.run(
        ["protoc", "--version"], capture_output=True, text=True, check=True
    ).stdout.strip()
    if protoc_version != "libprotoc 35.1":
        raise SystemExit("Java generation requires protoc 35.1")
    plugin = verify_plugin(
        Path(
            os.environ.get(
                "EPOCH_GRPC_JAVA_PLUGIN", PLUGIN_DIRECTORY / "bin/protoc-gen-grpc-java"
            )
        )
    )
    environment = os.environ.copy()
    environment["PATH"] = str(plugin.parent) + os.pathsep + os.environ.get("PATH", "")
    with tempfile.TemporaryDirectory(prefix="epoch-java-generate-") as directory:
        temporary = Path(directory)
        subprocess.run(
            [
                "buf",
                "generate",
                str(ROOT / "spec/proto"),
                "--template",
                str(ROOT / "buf.gen.java.yaml"),
                "--output",
                str(temporary),
            ],
            cwd=ROOT,
            env=environment,
            check=True,
        )
        files = {
            path.relative_to(temporary): path.read_bytes()
            for path in temporary.rglob("*.java")
        }
        if not files or not any(
            path.name == "RegionalAdminServiceGrpc.java" for path in files
        ):
            raise SystemExit("Java generator did not produce the management service")
        return files


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--install-plugin", type=Path, metavar="NEW_DIRECTORY")
    args = parser.parse_args()
    if args.install_plugin is not None:
        install_plugin(args.install_plugin)
        return
    files = generate()
    actual = {path.relative_to(OUTPUT) for path in OUTPUT.rglob("*") if path.is_file()}
    unexpected = sorted(actual - files.keys())
    if unexpected:
        raise SystemExit(
            "unexpected Java generated source: " + ", ".join(map(str, unexpected))
        )
    stale = []
    for path, content in files.items():
        destination = OUTPUT / path
        if args.check:
            if not destination.exists() or destination.read_bytes() != content:
                stale.append(str(destination.relative_to(ROOT)))
        else:
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(content)
    if stale:
        raise SystemExit("stale Java management bindings: " + ", ".join(stale))
    print(
        "Java management bindings match the pinned contract"
        if args.check
        else "generated Java management bindings"
    )


if __name__ == "__main__":
    main()

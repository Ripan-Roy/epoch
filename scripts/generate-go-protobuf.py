"""Generate or verify Go contracts with exact, local, source-verified plugins.

Checks stage generation outside the SDK and never rewrite missing/stale files.
Unexpected generated inventory is never deleted or silently accepted.
"""

from __future__ import annotations

import argparse
import os
import runpy
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "sdk/go/gen"


def require_buf() -> None:
    version = subprocess.run(
        ["buf", "--version"], capture_output=True, text=True, check=True
    ).stdout.strip()
    if version != "1.72.0":
        raise SystemExit("Go generation requires Buf 1.72.0")


def generate(source_paths: tuple[str, ...] = ()) -> dict[Path, bytes]:
    require_buf()
    installer = runpy.run_path(str(ROOT / "scripts/install-go-protobuf-tools.py"))
    tools = Path(os.environ.get("EPOCH_PROTOBUF_GO_TOOLS", installer["TOOL_DIRECTORY"]))
    binaries = installer["verify_tools"](tools)
    environment = {
        **os.environ,
        "PATH": str(binaries) + os.pathsep + os.environ.get("PATH", ""),
    }
    with tempfile.TemporaryDirectory(prefix="epoch-go-generate-") as directory:
        temporary = Path(directory)
        subprocess.run(
            [
                "buf",
                "generate",
                str(ROOT / "spec/proto"),
                *[argument for path in source_paths for argument in ("--path", path)],
                "--template",
                str(ROOT / "buf.gen.yaml"),
                "--output",
                str(temporary),
            ],
            cwd=ROOT,
            env=environment,
            check=True,
        )
        output = temporary / "sdk/go/gen"
        files = {
            path.relative_to(output): path.read_bytes() for path in output.rglob("*.go")
        }
        if not files or not any(path.name.endswith("_grpc.pb.go") for path in files):
            raise SystemExit("Go generator did not produce a service")
        return files


def synchronize(output: Path, generated: dict[Path, bytes], *, check: bool) -> None:
    if not generated or any(
        path.is_absolute() or ".." in path.parts for path in generated
    ):
        raise SystemExit("invalid Go generated inventory")
    if output.is_symlink():
        raise SystemExit("Go generated output cannot be a symlink")
    actual = set()
    for path in output.rglob("*"):
        if path.is_symlink():
            raise SystemExit("Go generated output cannot contain a symlink")
        if path.is_file():
            actual.add(path.relative_to(output))
    unexpected = sorted(actual - generated.keys())
    if unexpected:
        raise SystemExit(
            "unexpected Go generated source: " + ", ".join(map(str, unexpected))
        )
    stale = []
    for path, content in generated.items():
        target = output / path
        if check:
            if not target.is_file() or target.read_bytes() != content:
                stale.append(str(path))
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
    if stale:
        raise SystemExit("stale Go bindings: " + ", ".join(stale))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    synchronize(OUTPUT, generate(), check=args.check)
    print(
        "Go bindings match pinned local generators"
        if args.check
        else "generated Go bindings"
    )


if __name__ == "__main__":
    main()

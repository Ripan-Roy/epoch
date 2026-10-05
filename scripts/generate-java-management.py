"""Generate pinned Java management contracts, or verify without changing files."""

from __future__ import annotations

import argparse
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "sdk/java/src/generated/java"


def generate() -> dict[Path, bytes]:
    version = subprocess.run(
        ["buf", "--version"], capture_output=True, text=True, check=True
    ).stdout.strip()
    if version != "1.72.0":
        raise SystemExit("Java generation requires Buf 1.72.0")
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
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
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

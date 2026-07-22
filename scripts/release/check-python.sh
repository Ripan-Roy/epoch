#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
python_source="$repo_root/sdk/python"
requirements="$repo_root/tools/release/python-build-requirements.txt"
python_command=${PYTHON:-python3}
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/epoch-python-artifacts.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT
export PIP_CACHE_DIR="$work_dir/pip-cache"
export PIP_DISABLE_PIP_VERSION_CHECK=1
export PIP_NO_INPUT=1

build_environment="$work_dir/build-environment"
wheel_environment="$work_dir/wheel-consumer"
sdist_environment="$work_dir/sdist-consumer"
dist_dir="$work_dir/dist"
project_dir="$work_dir/project"

mkdir -p "$project_dir/src" "$project_dir/tests"
cp "$python_source/README.md" "$python_source/pyproject.toml" "$project_dir/"
cp -R "$python_source/src/epoch_sdk" "$project_dir/src/"
cp -R "$python_source/tests/." "$project_dir/tests/"

"$python_command" -m venv "$build_environment"
build_python="$build_environment/bin/python"
"$build_python" -m pip install \
  --disable-pip-version-check \
  --requirement "$requirements"

SOURCE_DATE_EPOCH=1784678400 \
  "$build_python" -m build \
    --no-isolation \
    --sdist \
    --wheel \
    --outdir "$dist_dir" \
    "$project_dir"

shopt -s nullglob
wheels=("$dist_dir"/*.whl)
sdists=("$dist_dir"/*.tar.gz)
shopt -u nullglob
if [[ ${#wheels[@]} -ne 1 || ${#sdists[@]} -ne 1 ]]; then
  echo "expected exactly one wheel and one source distribution" >&2
  exit 1
fi
wheel=${wheels[0]}
sdist=${sdists[0]}

"$build_python" -m twine check --strict "$wheel" "$sdist"

version=$(
  "$python_command" -c \
    'import pathlib, sys, tomllib; print(tomllib.loads(pathlib.Path(sys.argv[1]).read_text())["project"]["version"])' \
    "$project_dir/pyproject.toml"
)

"$build_python" - "$wheel" "$sdist" "$version" <<'PY'
from __future__ import annotations

import base64
import csv
import hashlib
import io
import pathlib
import sys
import tarfile
import zipfile
from email import policy
from email.parser import BytesParser

wheel_path = pathlib.Path(sys.argv[1])
sdist_path = pathlib.Path(sys.argv[2])
expected_version = sys.argv[3]
required_modules = {
    "epoch_sdk/__init__.py",
    "epoch_sdk/client.py",
    "epoch_sdk/errors.py",
    "epoch_sdk/models.py",
    "epoch_sdk/transport.py",
    "epoch_sdk/py.typed",
}


def metadata_from(data: bytes, source: str):
    metadata = BytesParser(policy=policy.default).parsebytes(data)
    if metadata["Name"] != "epoch-sdk":
        raise SystemExit(f"{source} has unexpected Name metadata: {metadata['Name']!r}")
    if metadata["Version"] != expected_version:
        raise SystemExit(f"{source} has unexpected Version metadata: {metadata['Version']!r}")
    if metadata["Requires-Python"] != ">=3.11":
        raise SystemExit(
            f"{source} has unexpected Requires-Python metadata: {metadata['Requires-Python']!r}"
        )
    return metadata


with zipfile.ZipFile(wheel_path) as archive:
    names = {name for name in archive.namelist() if not name.endswith("/")}
    if not required_modules <= names:
        raise SystemExit(f"wheel is missing package files: {sorted(required_modules - names)}")
    forbidden = [
        name
        for name in names
        if "/tests/" in f"/{name}/"
        or "__pycache__" in name
        or name.endswith((".pyc", ".pyo"))
    ]
    if forbidden:
        raise SystemExit(f"wheel contains development files: {sorted(forbidden)}")

    dist_info = f"epoch_sdk-{expected_version}.dist-info"
    metadata_name = f"{dist_info}/METADATA"
    wheel_name = f"{dist_info}/WHEEL"
    record_name = f"{dist_info}/RECORD"
    for required in (metadata_name, wheel_name, record_name):
        if required not in names:
            raise SystemExit(f"wheel is missing {required}")

    metadata_from(archive.read(metadata_name), "wheel")
    wheel_metadata = archive.read(wheel_name).decode("utf-8")
    if "Root-Is-Purelib: true" not in wheel_metadata or "Tag: py3-none-any" not in wheel_metadata:
        raise SystemExit("wheel must be a platform-independent pure-Python artifact")

    records = {
        row[0]: (row[1], row[2])
        for row in csv.reader(io.StringIO(archive.read(record_name).decode("utf-8")))
    }
    if set(records) != names:
        raise SystemExit("wheel RECORD does not enumerate exactly the archived files")
    for name in sorted(names - {record_name}):
        digest_field, size_field = records[name]
        if not digest_field.startswith("sha256="):
            raise SystemExit(f"wheel RECORD lacks a SHA-256 digest for {name}")
        encoded_digest = digest_field.removeprefix("sha256=")
        expected_digest = base64.urlsafe_b64encode(
            hashlib.sha256(archive.read(name)).digest()
        ).rstrip(b"=").decode("ascii")
        if encoded_digest != expected_digest:
            raise SystemExit(f"wheel RECORD digest mismatch for {name}")
        if size_field != str(len(archive.read(name))):
            raise SystemExit(f"wheel RECORD size mismatch for {name}")

with tarfile.open(sdist_path, mode="r:gz") as archive:
    names = {member.name for member in archive.getmembers() if member.isfile()}
    roots = {name.split("/", 1)[0] for name in names}
    if len(roots) != 1:
        raise SystemExit(f"sdist must have exactly one archive root, found {sorted(roots)}")
    root = roots.pop()
    required_sdist = {
        f"{root}/PKG-INFO",
        f"{root}/README.md",
        f"{root}/pyproject.toml",
        *(f"{root}/src/{name}" for name in required_modules),
    }
    if not required_sdist <= names:
        raise SystemExit(f"sdist is missing package files: {sorted(required_sdist - names)}")
    forbidden = [
        name
        for name in names
        if "__pycache__" in name
        or name.endswith((".pyc", ".pyo", ".pem", ".key", ".p12"))
        or "/.git/" in f"/{name}/"
        or "/.venv/" in f"/{name}/"
    ]
    if forbidden:
        raise SystemExit(f"sdist contains forbidden local files: {sorted(forbidden)}")
    package_info = archive.extractfile(f"{root}/PKG-INFO")
    if package_info is None:
        raise SystemExit("sdist PKG-INFO is unreadable")
    metadata_from(package_info.read(), "sdist")
PY

consumer_probe='from importlib.metadata import version; from epoch_sdk import EventEnvelope; assert version("epoch-sdk") == "'"$version"'"; assert EventEnvelope("release-gate", "artifact.probed", {"ok": True}, id="artifact-probe", time_ms=1).to_dict()["type"] == "artifact.probed"'

"$python_command" -m venv "$wheel_environment"
wheel_python="$wheel_environment/bin/python"
"$wheel_python" -m pip install \
  --disable-pip-version-check \
  --no-deps \
  --no-index \
  "$wheel"
"$wheel_python" -c "$consumer_probe"

"$python_command" -m venv "$sdist_environment"
sdist_python="$sdist_environment/bin/python"
"$sdist_python" -m pip install \
  --disable-pip-version-check \
  --requirement "$requirements"
"$sdist_python" -m pip install \
  --disable-pip-version-check \
  --no-build-isolation \
  --no-deps \
  --no-index \
  "$sdist"
"$sdist_python" -c "$consumer_probe"

printf '%s\n' \
  "Python artifact shape verified for epoch-sdk $version." \
  "The wheel and sdist passed strict metadata checks and fresh-environment imports." \
  "No registry upload was attempted."

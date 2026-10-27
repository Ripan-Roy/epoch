#!/usr/bin/env python3
"""Verify the narrowly patched, offline MySQL dependency against its provenance.

The official crate is retained byte-for-byte except for its normalized manifest's
Zstandard requirement. This is not a general-purpose dependency vendor tool.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import stat
import sys
import tarfile
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = "mysql_common"
VERSION = "0.37.3"
PREFIX = f"{PACKAGE}-{VERSION}"
VENDOR = ROOT / "third_party" / PREFIX
RECORD = "EPOCH_SOURCE.json"
SOURCE = "https://static.crates.io/crates/mysql_common/mysql_common-0.37.3.crate"
ARCHIVE_SHA256 = "0f27695f286b461da077b8c2f72f47feaa04ce3c3f9c0976257410e90e21208a"
EXCLUDED = [".gitignore", "Cargo.lock"]
INVENTORY_SHA256 = "c5a54f578a8113bd3a8967b788e079cc5bdc3dd4fed625f2eff7c1a8fe6a7ea3"
PATCHED_MANIFEST_SHA256 = (
    "07da2805f89fcc56df17940eb888f2bf88932b2dab1b83f1c5555491c5cb445b"
)
OLD_REQUIREMENT = b'[dependencies.zstd]\nversion = "0.13"'
NEW_REQUIREMENT = b'[dependencies.zstd]\nversion = "0.14.3"'


class VerificationError(ValueError):
    """The committed dependency does not match the approved source and patch."""


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def inventory_digest(files: dict[str, str]) -> str:
    return sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode())


def safe_relative_path(value: str) -> str:
    path = PurePosixPath(value)
    if (
        not value
        or "\\" in value
        or path.is_absolute()
        or any(part in ("", ".", "..") for part in value.split("/"))
        or path.as_posix() != value
    ):
        raise VerificationError(f"unsafe dependency path: {value!r}")
    return value


def read_upstream_archive(archive: Path) -> tuple[dict[str, str], str]:
    """Read metadata only, after authenticating the entire official archive."""
    data = archive.read_bytes()
    if sha256(data) != ARCHIVE_SHA256:
        raise VerificationError("upstream archive checksum mismatch")
    files: dict[str, str] = {}
    manifest = None
    seen: set[str] = set()
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as source:
        for member in source.getmembers():
            name = member.name.rstrip("/") if member.isdir() else member.name
            safe_relative_path(name)
            if name in seen:
                raise VerificationError(f"duplicate archive member: {name}")
            seen.add(name)
            if name == PREFIX and member.isdir():
                continue
            if not name.startswith(PREFIX + "/"):
                raise VerificationError(f"unexpected archive package: {name}")
            relative = safe_relative_path(name[len(PREFIX) + 1 :])
            if member.isdir():
                continue
            if not member.isfile():
                raise VerificationError(f"non-regular archive member: {name}")
            stream = source.extractfile(member)
            if stream is None:
                raise VerificationError(f"unreadable archive member: {name}")
            payload = stream.read()
            if relative in EXCLUDED:
                continue
            if relative == RECORD:
                raise VerificationError(
                    "upstream archive contains reserved provenance file"
                )
            files[relative] = sha256(payload)
            if relative == "Cargo.toml":
                manifest = payload
    if manifest is None or manifest.count(OLD_REQUIREMENT) != 1:
        raise VerificationError(
            "upstream manifest does not have the exact approved Zstd requirement"
        )
    patched = manifest.replace(OLD_REQUIREMENT, NEW_REQUIREMENT, 1)
    return files, sha256(patched)


def provenance(files: dict[str, str], patched_manifest: str) -> dict:
    return {
        "schema": 1,
        "package": PACKAGE,
        "version": VERSION,
        "source": SOURCE,
        "archive_sha256": ARCHIVE_SHA256,
        "excluded_files": EXCLUDED,
        "files": files,
        "patched_manifest_sha256": patched_manifest,
    }


def validate_provenance(record: dict) -> dict[str, str]:
    if not isinstance(record, dict) or set(record) != set(provenance({}, "")):
        raise VerificationError("unexpected provenance schema or fields")
    if type(record["schema"]) is not int:
        raise VerificationError("provenance schema must be an integer")
    expected = provenance({}, PATCHED_MANIFEST_SHA256)
    for key, value in expected.items():
        if key != "files" and record[key] != value:
            raise VerificationError(f"unexpected provenance {key}")
    files = record["files"]
    if not isinstance(files, dict) or not files:
        raise VerificationError("missing upstream file inventory")
    for name, digest in files.items():
        safe_relative_path(name)
        if name in EXCLUDED or name == RECORD:
            raise VerificationError(f"unexpected inventory file: {name}")
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
        ):
            raise VerificationError(f"invalid inventory digest: {name}")
    if inventory_digest(files) != INVENTORY_SHA256:
        raise VerificationError(
            "upstream inventory does not match the approved archive"
        )
    return files


def verify_vendor(directory: Path = VENDOR) -> None:
    if directory.is_symlink() or not directory.is_dir():
        raise VerificationError("vendor root must be a real directory")
    record_path = directory / RECORD
    if record_path.is_symlink() or not record_path.is_file():
        raise VerificationError("missing regular provenance file")
    try:
        record = json.loads(record_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise VerificationError(f"unreadable provenance: {error}") from error
    expected = dict(validate_provenance(record))
    expected["Cargo.toml"] = PATCHED_MANIFEST_SHA256
    actual: dict[str, str] = {}
    for path in directory.rglob("*"):
        relative = safe_relative_path(path.relative_to(directory).as_posix())
        mode = path.lstat().st_mode
        if stat.S_ISLNK(mode):
            raise VerificationError(f"symlink in vendor directory: {relative}")
        if stat.S_ISDIR(mode):
            continue
        if not stat.S_ISREG(mode):
            raise VerificationError(f"non-regular vendor file: {relative}")
        if relative != RECORD:
            actual[relative] = sha256(path.read_bytes())
    if set(actual) != set(expected):
        missing = sorted(set(expected) - set(actual))
        extra = sorted(set(actual) - set(expected))
        raise VerificationError(
            f"vendor file inventory mismatch; missing={missing}, extra={extra}"
        )
    for name, digest in expected.items():
        if actual[name] != digest:
            raise VerificationError(f"vendor content checksum mismatch: {name}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--record-upstream",
        type=Path,
        metavar="ARCHIVE",
        help="record the authenticated official archive inventory; never extract files",
    )
    args = parser.parse_args()
    try:
        if args.record_upstream is not None:
            files, manifest = read_upstream_archive(args.record_upstream)
            record = provenance(files, manifest)
            validate_provenance(record)
            if VENDOR.is_symlink() or not VENDOR.is_dir():
                raise VerificationError("vendor root must already be a real directory")
            target = VENDOR / RECORD
            if target.is_symlink():
                raise VerificationError(
                    "refusing to overwrite a symlink provenance file"
                )
            target.write_text(
                json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            print(f"Recorded authenticated {PREFIX} provenance")
        else:
            verify_vendor()
            print(
                f"Verified {PREFIX}: only the approved Zstandard manifest patch differs"
            )
    except (OSError, ValueError, tarfile.TarError) as error:
        parser.exit(1, f"vendored dependency verification failed: {error}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())

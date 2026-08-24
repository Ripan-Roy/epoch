#!/usr/bin/env python3
"""Resumable Epoch load/fault/soak evidence runner."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import stat
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
REGIONAL_DRIVER = REPO_ROOT / "tests/integration/regional-runtime.py"
STATE_SCHEMA = "epoch.soak.state/v1"
MANIFEST_SCHEMA = "epoch.soak.evidence/v1"
REGIONAL_SCHEMA = "epoch.regional-runtime.evidence/v1"
EVENT_LOG_NAME = "events.jsonl"
MANIFEST_NAME = "evidence.json"
SIGNATURE_NAME = "evidence.sig"
PUBLIC_KEY_NAME = "evidence-public.pem"
THIRTY_DAYS_MS = 30 * 24 * 60 * 60 * 1000
REQUIRED_PROFILES = ("cache", "stream", "queue", "event-bus")
REQUIRED_FAULTS = (
    "control_sigkill",
    "stream_leader_sigkill",
    "queue_leader_sigkill",
    "advanced_queue_leader_sigkill",
    "cache_leader_sigkill",
    "event_bus_leader_sigkill",
    "all_voter_sigkill_reopen",
)
REQUIRED_INVARIANTS = (
    "catalog_digest_preserved",
    "profile_state_converged",
    "managed_intent_replayed",
    "leadership_terms_advanced",
    "idempotent_retries_preserved",
    "automatic_checkpoints_reopened",
)


class EvidenceError(RuntimeError):
    """Raised when campaign evidence is incomplete or invalid."""


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def canonical_bytes(document: object) -> bytes:
    return (
        json.dumps(
            document,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )


def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    document: dict[str, Any] = {}
    for key, value in pairs:
        if key in document:
            raise EvidenceError(f"duplicate JSON key: {key}")
        document[key] = value
    return document


def load_json(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
    except OSError as error:
        raise EvidenceError(f"cannot read {path}: {error}") from error
    try:
        document = json.loads(raw, object_pairs_hook=reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise EvidenceError(f"invalid JSON in {path}: {error}") from error
    if not isinstance(document, dict):
        raise EvidenceError(f"{path} must contain one JSON object")
    return document


def atomic_write(path: Path, content: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def sha256_bytes(content: bytes) -> str:
    return f"sha256:{hashlib.sha256(content).hexdigest()}"


def file_receipt(path: Path, root: Path) -> dict[str, object]:
    resolved_root = root.resolve()
    resolved = path.resolve(strict=True)
    if resolved == resolved_root or resolved_root not in resolved.parents:
        raise EvidenceError(f"artifact escapes evidence directory: {path}")
    if path.is_symlink():
        raise EvidenceError(f"artifact symlinks are not accepted: {path}")
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return {
        "path": resolved.relative_to(resolved_root).as_posix(),
        "sha256": f"sha256:{digest.hexdigest()}",
        "size_bytes": size,
    }


def collect_artifacts(directory: Path, root: Path) -> list[dict[str, object]]:
    if not directory.exists():
        return []
    artifacts: list[dict[str, object]] = []
    for candidate in sorted(directory.rglob("*")):
        if candidate.is_symlink():
            raise EvidenceError(f"artifact symlinks are not accepted: {candidate}")
        if candidate.is_file():
            artifacts.append(file_receipt(candidate, root))
    return artifacts


def command_output(arguments: Sequence[str], *, required: bool = True) -> str:
    try:
        result = subprocess.run(
            list(arguments),
            cwd=REPO_ROOT,
            check=required,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        if required:
            raise EvidenceError(f"command failed: {' '.join(arguments)}") from error
        return ""
    return result.stdout.strip()


def source_identity() -> dict[str, object]:
    revision = command_output(("git", "rev-parse", "HEAD"))
    status = subprocess.run(
        ("git", "status", "--porcelain=v1", "-z", "--untracked-files=all"),
        cwd=REPO_ROOT,
        check=True,
        stdout=subprocess.PIPE,
    ).stdout
    tracked_diff = subprocess.run(
        ("git", "diff", "--binary", "HEAD", "--"),
        cwd=REPO_ROOT,
        check=True,
        stdout=subprocess.PIPE,
    ).stdout
    untracked = subprocess.run(
        ("git", "ls-files", "--others", "--exclude-standard", "-z"),
        cwd=REPO_ROOT,
        check=True,
        stdout=subprocess.PIPE,
    ).stdout.split(b"\0")
    tree_hash = hashlib.sha256()
    tree_hash.update(b"status\0")
    tree_hash.update(status)
    tree_hash.update(b"diff\0")
    tree_hash.update(tracked_diff)
    for encoded_path in sorted(path for path in untracked if path):
        relative = os.fsdecode(encoded_path)
        candidate = REPO_ROOT / relative
        if not candidate.is_file() or candidate.is_symlink():
            raise EvidenceError(f"unsupported untracked source entry: {relative}")
        tree_hash.update(b"untracked\0")
        tree_hash.update(encoded_path)
        tree_hash.update(b"\0")
        with candidate.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                tree_hash.update(chunk)
    version = (REPO_ROOT / "VERSION").read_text(encoding="utf-8").strip()
    return {
        "git_revision": revision,
        "version": version,
        "worktree_clean": not status,
        "source_tree_sha256": f"sha256:{tree_hash.hexdigest()}",
    }


def runtime_identity(environment: Mapping[str, str]) -> dict[str, object]:
    image = environment.get("EPOCH_REGIONAL_IMAGE", "epoch/node:regional")
    inspection = command_output(
        (
            "docker",
            "image",
            "inspect",
            image,
            "--format",
            "{{json .}}",
        ),
        required=False,
    )
    image_document: dict[str, Any] = {}
    if inspection:
        try:
            parsed = json.loads(inspection)
            if isinstance(parsed, dict):
                image_document = parsed
        except json.JSONDecodeError:
            image_document = {}
    config = image_document.get("Config")
    labels = config.get("Labels", {}) if isinstance(config, dict) else {}
    if not isinstance(labels, dict):
        labels = {}
    return {
        "driver": "tests/integration/regional-runtime.py",
        "image": image,
        "image_id": image_document.get("Id", "unresolved"),
        "image_repo_digests": image_document.get("RepoDigests") or [],
        "image_revision": labels.get("org.opencontainers.image.revision", "unknown"),
        "image_version": labels.get("org.opencontainers.image.version", "unknown"),
        "machine": platform.machine(),
        "operating_system": platform.system(),
        "python": platform.python_version(),
    }


@dataclass(frozen=True)
class CampaignPlan:
    name: str
    target_rounds: int
    target_active_ms: int

    def as_document(self) -> dict[str, object]:
        return {
            "name": self.name,
            "target_active_ms": self.target_active_ms,
            "target_rounds": self.target_rounds,
            "workload": "regional-four-profile-fault-campaign-v1",
            "load_shape": {
                "mode": "bounded_mixed_profile_correctness",
                "saturation_claimed": False,
            },
            "profiles": list(REQUIRED_PROFILES),
            "faults": list(REQUIRED_FAULTS),
            "invariants": list(REQUIRED_INVARIANTS),
        }

    @property
    def digest(self) -> str:
        return sha256_bytes(canonical_bytes(self.as_document()))

    def is_complete(self, state: Mapping[str, Any]) -> bool:
        passed = [
            attempt
            for attempt in state.get("attempts", [])
            if isinstance(attempt, dict) and attempt.get("status") == "passed"
        ]
        active_ms = sum(
            int(attempt.get("duration_ms", 0))
            for attempt in passed
            if isinstance(attempt.get("duration_ms"), int)
        )
        return len(passed) >= self.target_rounds and active_ms >= self.target_active_ms


PLANS = {
    "accelerated": CampaignPlan("accelerated", target_rounds=1, target_active_ms=0),
    "thirty-day": CampaignPlan(
        "thirty-day", target_rounds=1, target_active_ms=THIRTY_DAYS_MS
    ),
}


RoundDriver = Callable[[Path, int], Path]


def validate_regional_result(path: Path) -> dict[str, Any]:
    result = load_json(path)
    if result.get("schema") != REGIONAL_SCHEMA:
        raise EvidenceError("regional driver returned an unsupported evidence schema")
    if result.get("status") != "passed":
        raise EvidenceError("regional driver did not report a passed campaign")
    profiles = result.get("profiles")
    if not isinstance(profiles, list) or set(profiles) != set(REQUIRED_PROFILES):
        raise EvidenceError("regional driver did not cover every required profile")
    faults = result.get("faults")
    if not isinstance(faults, list) or not set(REQUIRED_FAULTS).issubset(faults):
        raise EvidenceError("regional driver did not cover every required fault")
    invariants = result.get("invariants")
    if not isinstance(invariants, dict):
        raise EvidenceError("regional driver omitted invariant results")
    failed = [name for name in REQUIRED_INVARIANTS if invariants.get(name) is not True]
    if failed:
        raise EvidenceError(f"regional invariants did not pass: {', '.join(failed)}")
    return result


def percentile(values: Sequence[int], percentage: int) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    index = max(0, ((len(ordered) * percentage + 99) // 100) - 1)
    return ordered[min(index, len(ordered) - 1)]


class Campaign:
    def __init__(
        self,
        state_dir: Path,
        plan: CampaignPlan,
        signing_key: Path,
        *,
        identity: dict[str, object] | None = None,
        environment: Mapping[str, str] | None = None,
        driver: RoundDriver | None = None,
        round_timeout_seconds: int = 1800,
    ) -> None:
        self.state_dir = state_dir.resolve()
        self.state_path = self.state_dir / "state.json"
        self.event_path = self.state_dir / EVENT_LOG_NAME
        self.manifest_path = self.state_dir / MANIFEST_NAME
        self.signature_path = self.state_dir / SIGNATURE_NAME
        self.public_key_path = self.state_dir / PUBLIC_KEY_NAME
        self.plan = plan
        self.signing_key = signing_key.resolve()
        if (
            self.signing_key == self.state_dir
            or self.state_dir in self.signing_key.parents
        ):
            raise EvidenceError(
                "the private signing key must stay outside evidence state"
            )
        if signing_key.is_symlink() or not self.signing_key.is_file():
            raise EvidenceError("the soak signing key must be a regular file")
        if stat.S_IMODE(self.signing_key.stat().st_mode) & 0o077:
            raise EvidenceError(
                "the soak signing key must not be group/world accessible"
            )
        self.environment = dict(os.environ if environment is None else environment)
        self.identity = identity or {
            "source": source_identity(),
            "runtime": runtime_identity(self.environment),
        }
        self.driver = driver or self._regional_driver
        self.round_timeout_seconds = round_timeout_seconds

    def _append_event(self, kind: str, **details: object) -> None:
        event = {"at": utc_now(), "kind": kind, **details}
        self.event_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor = os.open(
            self.event_path,
            os.O_WRONLY | os.O_APPEND | os.O_CREAT,
            0o600,
        )
        with os.fdopen(descriptor, "ab") as output:
            output.write(canonical_bytes(event))
            output.flush()
            os.fsync(output.fileno())

    def _new_state(self) -> dict[str, Any]:
        return {
            "schema": STATE_SCHEMA,
            "run_id": str(uuid.uuid4()),
            "created_at": utc_now(),
            "updated_at": utc_now(),
            "plan": self.plan.as_document(),
            "plan_sha256": self.plan.digest,
            "identity": self.identity,
            "attempts": [],
        }

    def _write_state(self, state: dict[str, Any]) -> None:
        state["updated_at"] = utc_now()
        atomic_write(self.state_path, canonical_bytes(state))

    def _load_state(self) -> dict[str, Any]:
        self.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.state_dir, stat.S_IRWXU)
        if not self.state_path.exists():
            state = self._new_state()
            self._write_state(state)
            self._append_event(
                "campaign_started",
                run_id=state["run_id"],
                plan=self.plan.name,
                plan_sha256=self.plan.digest,
            )
            return state
        state = load_json(self.state_path)
        if state.get("schema") != STATE_SCHEMA:
            raise EvidenceError("unsupported soak state schema")
        if state.get("plan_sha256") != self.plan.digest:
            raise EvidenceError("cannot resume with a different campaign plan")
        if state.get("identity") != self.identity:
            raise EvidenceError(
                "cannot resume after source or runtime identity changed"
            )
        attempts = state.get("attempts")
        if not isinstance(attempts, list):
            raise EvidenceError("soak state attempts must be an array")
        changed = False
        for attempt in attempts:
            if isinstance(attempt, dict) and attempt.get("status") == "running":
                attempt["status"] = "interrupted"
                attempt["completed_at"] = utc_now()
                directory = self.state_dir / str(attempt.get("directory", ""))
                attempt["artifacts"] = collect_artifacts(directory, self.state_dir)
                changed = True
                self._append_event(
                    "round_interrupted",
                    round=attempt.get("round"),
                    attempt=attempt.get("attempt"),
                )
        if changed:
            self._write_state(state)
        self._append_event("campaign_resumed", run_id=state.get("run_id"))
        return state

    def _regional_driver(self, round_dir: Path, round_number: int) -> Path:
        result_path = round_dir / "regional-result.json"
        log_path = round_dir / "regional-runtime.log"
        runtime_artifacts = round_dir / "runtime-artifacts"
        environment = self.environment.copy()
        environment.update(
            {
                "EPOCH_REGIONAL_ARTIFACT_DIR": str(runtime_artifacts),
                "EPOCH_REGIONAL_RESULT_PATH": str(result_path),
                "EPOCH_REGIONAL_PROJECT_NAME": (
                    f"epoch-soak-{os.getpid()}-{round_number}"
                ),
            }
        )
        with log_path.open("wb") as output:
            try:
                subprocess.run(
                    (sys.executable, str(REGIONAL_DRIVER)),
                    cwd=REPO_ROOT,
                    env=environment,
                    check=True,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    timeout=self.round_timeout_seconds,
                )
            except subprocess.TimeoutExpired as error:
                raise EvidenceError(
                    f"regional round exceeded {self.round_timeout_seconds} seconds"
                ) from error
            except subprocess.CalledProcessError as error:
                raise EvidenceError(
                    f"regional round failed with exit code {error.returncode}"
                ) from error
        return result_path

    def _next_coordinates(self, state: Mapping[str, Any]) -> tuple[int, int]:
        attempts = state.get("attempts", [])
        passed_rounds = {
            int(attempt["round"])
            for attempt in attempts
            if isinstance(attempt, dict)
            and attempt.get("status") == "passed"
            and isinstance(attempt.get("round"), int)
        }
        round_number = len(passed_rounds) + 1
        attempt_number = (
            sum(
                1
                for attempt in attempts
                if isinstance(attempt, dict) and attempt.get("round") == round_number
            )
            + 1
        )
        return round_number, attempt_number

    def _run_one(self, state: dict[str, Any]) -> None:
        round_number, attempt_number = self._next_coordinates(state)
        relative_directory = Path(
            f"round-{round_number:06d}-attempt-{attempt_number:03d}"
        )
        round_dir = self.state_dir / relative_directory
        round_dir.mkdir(parents=False, exist_ok=False, mode=0o700)
        attempt: dict[str, Any] = {
            "round": round_number,
            "attempt": attempt_number,
            "directory": relative_directory.as_posix(),
            "status": "running",
            "started_at": utc_now(),
        }
        state["attempts"].append(attempt)
        self._write_state(state)
        self._append_event("round_started", round=round_number, attempt=attempt_number)
        started = time.monotonic_ns()
        try:
            result_path = self.driver(round_dir, round_number)
            result = validate_regional_result(result_path)
            duration_ms = max(1, (time.monotonic_ns() - started) // 1_000_000)
            attempt.update(
                {
                    "status": "passed",
                    "completed_at": utc_now(),
                    "duration_ms": duration_ms,
                    "result": result,
                    "artifacts": collect_artifacts(round_dir, self.state_dir),
                }
            )
            self._write_state(state)
            self._append_event(
                "round_passed",
                round=round_number,
                attempt=attempt_number,
                duration_ms=duration_ms,
            )
        except BaseException as error:
            duration_ms = max(1, (time.monotonic_ns() - started) // 1_000_000)
            attempt.update(
                {
                    "status": "failed",
                    "completed_at": utc_now(),
                    "duration_ms": duration_ms,
                    "error_type": type(error).__name__,
                    "artifacts": collect_artifacts(round_dir, self.state_dir),
                }
            )
            self._write_state(state)
            self._append_event(
                "round_failed",
                round=round_number,
                attempt=attempt_number,
                duration_ms=duration_ms,
                error_type=type(error).__name__,
            )
            raise

    def _finalize(self, state: dict[str, Any]) -> Path:
        attempts = state["attempts"]
        passed = [attempt for attempt in attempts if attempt.get("status") == "passed"]
        durations = [int(attempt["duration_ms"]) for attempt in passed]
        self._append_event("campaign_completed", run_id=state["run_id"])
        event_receipt = file_receipt(self.event_path, self.state_dir)
        manifest = {
            "schema": MANIFEST_SCHEMA,
            "status": "passed",
            "run_id": state["run_id"],
            "created_at": state["created_at"],
            "completed_at": utc_now(),
            "plan": state["plan"],
            "plan_sha256": state["plan_sha256"],
            "identity": state["identity"],
            "claim": {
                "accelerated_harness_only": self.plan.name == "accelerated",
                "managed_service_slo_claimed": False,
                "production_certification_claimed": False,
                "throughput_or_latency_slo_claimed": False,
            },
            "summary": {
                "attempts": len(attempts),
                "failed_or_interrupted_attempts": len(attempts) - len(passed),
                "passed_rounds": len(passed),
                "active_ms": sum(durations),
                "campaign_runtime_ms": {
                    "minimum": min(durations),
                    "p50": percentile(durations, 50),
                    "p95": percentile(durations, 95),
                    "p99": percentile(durations, 99),
                    "maximum": max(durations),
                },
            },
            "event_log": event_receipt,
            "attempts": attempts,
            "signature": {
                "algorithm": "Ed25519",
                "public_key": PUBLIC_KEY_NAME,
                "signature": SIGNATURE_NAME,
            },
        }
        self._write_public_key()
        public_der = subprocess.run(
            (
                "openssl",
                "pkey",
                "-pubin",
                "-in",
                str(self.public_key_path),
                "-outform",
                "DER",
            ),
            check=True,
            stdout=subprocess.PIPE,
        ).stdout
        manifest["signature"]["public_key_sha256"] = sha256_bytes(public_der)
        pending_manifest = self.manifest_path.with_name(
            f".{self.manifest_path.name}.{os.getpid()}.pending"
        )
        pending_signature = self.signature_path.with_name(
            f".{self.signature_path.name}.{os.getpid()}.pending"
        )
        try:
            atomic_write(pending_manifest, canonical_bytes(manifest), mode=0o644)
            subprocess.run(
                (
                    "openssl",
                    "pkeyutl",
                    "-sign",
                    "-rawin",
                    "-inkey",
                    str(self.signing_key),
                    "-in",
                    str(pending_manifest),
                    "-out",
                    str(pending_signature),
                ),
                check=True,
            )
            verification = subprocess.run(
                (
                    "openssl",
                    "pkeyutl",
                    "-verify",
                    "-pubin",
                    "-inkey",
                    str(self.public_key_path),
                    "-rawin",
                    "-in",
                    str(pending_manifest),
                    "-sigfile",
                    str(pending_signature),
                ),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if verification.returncode != 0:
                raise EvidenceError("new evidence signature did not verify")
            os.chmod(pending_signature, 0o644)
            os.replace(pending_signature, self.signature_path)
            os.replace(pending_manifest, self.manifest_path)
            directory = os.open(self.state_dir, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except BaseException:
            pending_manifest.unlink(missing_ok=True)
            pending_signature.unlink(missing_ok=True)
            raise
        verify_manifest(self.manifest_path, self.public_key_path)
        return self.manifest_path

    def _write_public_key(self) -> None:
        key_details = command_output(
            ("openssl", "pkey", "-in", str(self.signing_key), "-text", "-noout")
        )
        if "ED25519" not in key_details.upper():
            raise EvidenceError("the soak signing key must be Ed25519")
        temporary = self.public_key_path.with_name(
            f".{self.public_key_path.name}.{os.getpid()}.tmp"
        )
        subprocess.run(
            (
                "openssl",
                "pkey",
                "-in",
                str(self.signing_key),
                "-pubout",
                "-out",
                str(temporary),
            ),
            check=True,
        )
        os.chmod(temporary, 0o644)
        os.replace(temporary, self.public_key_path)

    def run(self, *, round_budget: int | None = None) -> Path | None:
        if self.manifest_path.exists():
            verify_manifest(self.manifest_path, self.public_key_path)
            return self.manifest_path
        state = self._load_state()
        rounds_run = 0
        while not self.plan.is_complete(state):
            if round_budget is not None and rounds_run >= round_budget:
                return None
            self._run_one(state)
            rounds_run += 1
        return self._finalize(state)


def resolve_artifact(root: Path, relative: object) -> Path:
    if not isinstance(relative, str) or not relative or "\x00" in relative:
        raise EvidenceError("artifact path must be a nonempty string")
    candidate = Path(relative)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise EvidenceError(f"unsafe artifact path: {relative}")
    resolved_root = root.resolve()
    unresolved = resolved_root / candidate
    if unresolved.is_symlink():
        raise EvidenceError(f"artifact symlinks are not accepted: {relative}")
    resolved = unresolved.resolve(strict=True)
    if resolved_root not in resolved.parents:
        raise EvidenceError(f"artifact escapes evidence directory: {relative}")
    if resolved.is_symlink() or not resolved.is_file():
        raise EvidenceError(f"artifact must be a regular file: {relative}")
    return resolved


def verify_receipt(receipt: object, root: Path) -> None:
    if not isinstance(receipt, dict):
        raise EvidenceError("artifact receipt must be an object")
    path = resolve_artifact(root, receipt.get("path"))
    observed = file_receipt(path, root)
    if receipt != observed:
        raise EvidenceError(f"artifact receipt mismatch: {receipt.get('path')}")


def verify_manifest(manifest_path: Path, public_key_path: Path) -> None:
    root = manifest_path.resolve().parent
    manifest_bytes = manifest_path.read_bytes()
    manifest = load_json(manifest_path)
    if manifest_bytes != canonical_bytes(manifest):
        raise EvidenceError("evidence manifest is not canonical JSON")
    if manifest.get("schema") != MANIFEST_SCHEMA or manifest.get("status") != "passed":
        raise EvidenceError("evidence manifest is not a passed v1 campaign")
    signature = manifest.get("signature")
    if not isinstance(signature, dict) or signature.get("algorithm") != "Ed25519":
        raise EvidenceError("evidence manifest has an unsupported signature")
    expected_public = resolve_artifact(root, signature.get("public_key"))
    if expected_public != public_key_path.resolve():
        raise EvidenceError("verification key does not match the manifest key path")
    signature_file = resolve_artifact(root, signature.get("signature"))
    public_der = subprocess.run(
        (
            "openssl",
            "pkey",
            "-pubin",
            "-in",
            str(public_key_path),
            "-outform",
            "DER",
        ),
        check=True,
        stdout=subprocess.PIPE,
    ).stdout
    if signature.get("public_key_sha256") != sha256_bytes(public_der):
        raise EvidenceError("evidence public-key fingerprint mismatch")
    verification = subprocess.run(
        (
            "openssl",
            "pkeyutl",
            "-verify",
            "-pubin",
            "-inkey",
            str(public_key_path),
            "-rawin",
            "-in",
            str(manifest_path),
            "-sigfile",
            str(signature_file),
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if verification.returncode != 0:
        raise EvidenceError("evidence manifest signature is invalid")
    verify_receipt(manifest.get("event_log"), root)
    attempts = manifest.get("attempts")
    if not isinstance(attempts, list) or not attempts:
        raise EvidenceError("evidence manifest has no attempts")
    passed = 0
    for attempt in attempts:
        if not isinstance(attempt, dict):
            raise EvidenceError("evidence attempt must be an object")
        artifacts = attempt.get("artifacts")
        if not isinstance(artifacts, list):
            raise EvidenceError("evidence attempt artifacts must be an array")
        for receipt in artifacts:
            verify_receipt(receipt, root)
        if attempt.get("status") == "passed":
            result = attempt.get("result")
            if not isinstance(result, dict):
                raise EvidenceError("passed attempt omitted its result")
            invariants = result.get("invariants")
            if not isinstance(invariants, dict) or any(
                invariants.get(name) is not True for name in REQUIRED_INVARIANTS
            ):
                raise EvidenceError("passed attempt does not prove all invariants")
            passed += 1
    summary = manifest.get("summary")
    if not isinstance(summary, dict) or summary.get("passed_rounds") != passed:
        raise EvidenceError("evidence summary does not match passed attempts")
    plan_document = manifest.get("plan")
    if manifest.get("plan_sha256") != sha256_bytes(canonical_bytes(plan_document)):
        raise EvidenceError("evidence campaign plan digest mismatch")
    target_rounds = (
        plan_document.get("target_rounds") if isinstance(plan_document, dict) else None
    )
    target_active_ms = (
        plan_document.get("target_active_ms")
        if isinstance(plan_document, dict)
        else None
    )
    if not isinstance(target_rounds, int) or passed < target_rounds:
        raise EvidenceError("evidence did not complete the target round count")
    if (
        not isinstance(target_active_ms, int)
        or not isinstance(summary.get("active_ms"), int)
        or summary["active_ms"] < target_active_ms
    ):
        raise EvidenceError("evidence did not complete the target active duration")


def generate_key(output: Path) -> Path:
    output = output.resolve()
    if output.exists():
        raise EvidenceError(f"refusing to overwrite signing key: {output}")
    output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    subprocess.run(
        ("openssl", "genpkey", "-algorithm", "ED25519", "-out", str(output)),
        check=True,
    )
    os.chmod(output, 0o600)
    return output


def parse_positive(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    keygen = subparsers.add_parser("keygen", help="create an Ed25519 evidence key")
    keygen.add_argument("--output", type=Path, required=True)

    run = subparsers.add_parser("run", help="run or resume a fault campaign")
    run.add_argument("--state-dir", type=Path, required=True)
    run.add_argument("--profile", choices=tuple(PLANS), required=True)
    run.add_argument("--signing-key", type=Path, required=True)
    run.add_argument("--round-budget", type=parse_positive)
    run.add_argument("--round-timeout-seconds", type=parse_positive, default=1800)

    verify = subparsers.add_parser("verify", help="verify signed campaign evidence")
    verify.add_argument("--manifest", type=Path, required=True)
    verify.add_argument("--public-key", type=Path, required=True)
    return parser


def main(arguments: Sequence[str] | None = None) -> int:
    parser = build_parser()
    options = parser.parse_args(arguments)
    try:
        if options.command == "keygen":
            print(generate_key(options.output))
            return 0
        if options.command == "verify":
            verify_manifest(options.manifest, options.public_key)
            print(f"verified signed soak evidence: {options.manifest}")
            return 0
        campaign = Campaign(
            options.state_dir,
            PLANS[options.profile],
            options.signing_key,
            round_timeout_seconds=options.round_timeout_seconds,
        )
        manifest = campaign.run(round_budget=options.round_budget)
        if manifest is None:
            print(f"campaign checkpointed but incomplete: {campaign.state_path}")
            return 75
        print(f"campaign passed with signed evidence: {manifest}")
        return 0
    except (EvidenceError, OSError, subprocess.SubprocessError) as error:
        print(f"soak evidence failure: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

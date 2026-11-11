"""Mounted-WAL proof must replace only its owned, synchronously removed container."""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
STEP = "Prove mounted local durability across container replacement"
CONTAINER = "epoch-node-ci-smoke"
VOLUME = "epoch-node-ci-data"
FOREIGN_ID = "f" * 64

MOCK = r"""#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

program, args = Path(sys.argv[0]).name, sys.argv[1:]
path = Path(os.environ["EPOCH_SMOKE_STATE"])
state = json.loads(path.read_text())
state["commands"].append([program, args])
mode = os.environ.get("EPOCH_SMOKE_MODE", "success")

def finish(code=0, output=""):
    path.write_text(json.dumps(state))
    if output:
        print(output)
    raise SystemExit(code)

if program == "sleep":
    finish()

if program == "curl":
    url = args[-1]
    if url.endswith("/healthz"):
        finish(22 if mode == "unhealthy" else 0)
    if "--data" in args:
        state["writes"].append([url, json.loads(args[args.index("--data") + 1])])
        if mode == "write-error":
            finish(48)
        state["segments"] += 1
        finish()
    if mode == "read-error":
        finish(49)
    if "/streams/audit/records?" in url:
        finish(output="[]" if mode == "missing-record" else '[{"id":"container-1"}]')
    if url.endswith("/queues/jobs/counts"):
        finish(output='{"ready":0}' if mode == "missing-queue" else '{"ready":1}')
    finish(91)

if args[:2] == ["volume", "create"]:
    finish(40 if mode == "volume-error" else 0, "epoch-node-ci-data")
if args[0] == "run":
    state["runs"] += 1
    if mode == "initial-run-error" and state["runs"] == 1:
        finish(43)
    if mode == "second-run-error" and state["runs"] == 2:
        finish(44)
    if state["current"] is not None:
        finish(125, "container name is still in use during asynchronous removal")
    identity = f'{state["runs"]:064x}'
    state["current"] = identity
    state["owned"].append(identity)
    state["containers"][identity] = {"running": True, "auto_remove": "--rm" in args}
    finish(output=identity)
if args[0] in ("stop", "rm", "exec", "logs"):
    target = args[-1] if args[0] in ("stop", "rm", "logs") else args[1]
    identity = state["current"] if target == "epoch-node-ci-smoke" else target
    if identity not in state["containers"]:
        finish(1)
    if identity == "f" * 64 and args[0] in ("stop", "rm"):
        finish(90, "attempted to delete a foreign container")
    if args[0] == "stop":
        if mode == "stop-error":
            finish(45)
        state["containers"][identity]["running"] = False
        # --rm is deliberately delayed beyond the next run. This deterministically
        # reproduces the real Docker stop/auto-remove scheduling window.
        finish(output=identity)
    if args[0] == "rm":
        if mode == "remove-error":
            finish(46)
        if state["containers"][identity]["running"]:
            finish(92)
        del state["containers"][identity]
        state["current"] = None
        finish(output=identity)
    if args[0] == "logs":
        finish(output="fixture health did not become ready")
    if mode == "exec-error":
        finish(47)
    state["segment_checks"] += 1
    count = state["segments"]
    if mode == "missing-segments":
        count = 1
    if mode == "regressed-segments" and state["segment_checks"] > 1:
        count = 2
    finish(output=str(count))
finish(93)
"""


def workflow_command() -> str:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text()
    match = re.search(
        rf"^      - name: {re.escape(STEP)}\n(.*?)(?=^      - |\Z)",
        workflow,
        re.MULTILINE | re.DOTALL,
    )
    if match is None:
        raise AssertionError("mounted durability proof must remain mandatory")
    block = match[1]
    if re.search(r"^        (?:if|continue-on-error):", block, re.MULTILINE):
        raise AssertionError("mounted durability proof cannot be skipped or ignored")
    if "        run: |\n" in block:
        return textwrap.dedent(block.split("        run: |\n", 1)[1])
    return next(
        line.removeprefix("        run: ")
        for line in block.splitlines()
        if line.startswith("        run: ")
    )


class LocalDurabilityContractTest(unittest.TestCase):
    def run_smoke(self, mode: str = "success", *, foreign: bool = False):
        with tempfile.TemporaryDirectory(prefix="epoch-smoke-contract-") as folder:
            directory = Path(folder)
            binaries = directory / "bin"
            binaries.mkdir()
            for name in ("docker", "curl", "sleep"):
                command = binaries / name
                command.write_text(MOCK)
                command.chmod(0o755)
            state_path = directory / "state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "commands": [],
                        "writes": [],
                        "runs": 0,
                        "segments": 0,
                        "segment_checks": 0,
                        "owned": [],
                        "current": FOREIGN_ID if foreign else None,
                        "containers": {FOREIGN_ID: {"running": True}}
                        if foreign
                        else {},
                    }
                )
            )
            result = subprocess.run(
                ["bash", "-Eeuo", "pipefail", "-c", workflow_command()],
                cwd=ROOT,
                env={
                    **os.environ,
                    "PATH": str(binaries) + os.pathsep + os.environ["PATH"],
                    "EPOCH_SMOKE_STATE": str(state_path),
                    "EPOCH_SMOKE_MODE": mode,
                },
                text=True,
                capture_output=True,
                timeout=20,
                check=False,
            )
            return result, json.loads(state_path.read_text())

    def test_replacement_waits_for_explicit_owned_container_removal(self) -> None:
        result, state = self.run_smoke()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        runs = [args for program, args in state["commands"] if args[0] == "run"]
        self.assertEqual(len(runs), 2)
        for args in runs:
            self.assertNotIn("--rm", args)
            self.assertEqual(args[args.index("--name") + 1], CONTAINER)
            self.assertEqual(
                args[args.index("--volume") + 1], VOLUME + ":/var/lib/epoch"
            )
            self.assertEqual(
                args[args.index("--env") + 1], "EPOCH_WAL_SEGMENT_BYTES=512"
            )
        docker = [args for program, args in state["commands"] if program == "docker"]
        first, second = state["owned"]
        second_run = docker.index(runs[1], docker.index(runs[0]) + 1)
        self.assertLess(docker.index(["rm", first]), second_run)
        self.assertIn(["stop", "--timeout", "10", first], docker)
        self.assertIn(["stop", "--timeout", "10", second], docker)
        self.assertIn(["rm", second], docker)
        self.assertEqual(state["segment_checks"], 2)
        self.assertEqual(
            len(
                [
                    args
                    for program, args in state["commands"]
                    if program == "curl" and args[-1].endswith("/healthz")
                ]
            ),
            2,
        )
        self.assertIsNone(state["current"])

    def test_exact_requests_and_recovery_checks_are_preserved(self) -> None:
        result, state = self.run_smoke()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            [
                args[args.index("--data") + 1]
                for program, args in state["commands"]
                if program == "curl" and "--data" in args
            ],
            [
                '{"partitions":1,"durability":"local_durable","max_records_per_partition":null}',
                '{"envelope":{"id":"container-1","source":"ci","type":"audit.created","time_ms":1,"payload":{"ok":true}}}',
                '{"durability":"local_durable","visibility_timeout_ms":30000,"max_messages":100,"retry":{"strategy":"fixed","initial_delay_ms":1000,"max_delay_ms":1000,"jitter_percent":0,"max_attempts":3,"max_age_ms":null},"dedupe_window_ms":null}',
                '{"id":"container-job-1","source":"ci","type":"job.created","time_ms":1,"payload":{"ok":true}}',
            ],
        )
        self.assertEqual(
            state["writes"],
            [
                [
                    "http://127.0.0.1:17661/v1/streams/audit",
                    {
                        "partitions": 1,
                        "durability": "local_durable",
                        "max_records_per_partition": None,
                    },
                ],
                [
                    "http://127.0.0.1:17661/v1/streams/audit/records",
                    {
                        "envelope": {
                            "id": "container-1",
                            "source": "ci",
                            "type": "audit.created",
                            "time_ms": 1,
                            "payload": {"ok": True},
                        }
                    },
                ],
                [
                    "http://127.0.0.1:17661/v1/queues/jobs",
                    {
                        "durability": "local_durable",
                        "visibility_timeout_ms": 30000,
                        "max_messages": 100,
                        "retry": {
                            "strategy": "fixed",
                            "initial_delay_ms": 1000,
                            "max_delay_ms": 1000,
                            "jitter_percent": 0,
                            "max_attempts": 3,
                            "max_age_ms": None,
                        },
                        "dedupe_window_ms": None,
                    },
                ],
                [
                    "http://127.0.0.1:17661/v1/queues/jobs/messages",
                    {
                        "id": "container-job-1",
                        "source": "ci",
                        "type": "job.created",
                        "time_ms": 1,
                        "payload": {"ok": True},
                    },
                ],
            ],
        )

    def test_failed_initial_run_cannot_clean_up_a_foreign_named_container(self) -> None:
        for mode, expected in (("success", 125), ("initial-run-error", 43)):
            with self.subTest(mode=mode):
                result, state = self.run_smoke(mode, foreign=True)
                self.assertEqual(result.returncode, expected, result.stderr)
                self.assertTrue(state["containers"][FOREIGN_ID]["running"])
                self.assertFalse(state["owned"])
                self.assertFalse(
                    [
                        args
                        for program, args in state["commands"]
                        if program == "docker" and args[0] in ("stop", "rm")
                    ]
                )

    def test_command_and_recovery_failures_are_not_masked_by_cleanup(self) -> None:
        for mode, expected in (
            ("volume-error", 40),
            ("initial-run-error", 43),
            ("second-run-error", 44),
            ("stop-error", 45),
            ("remove-error", 46),
            ("exec-error", 47),
            ("write-error", 48),
            ("read-error", 49),
            ("unhealthy", 1),
            ("missing-record", 1),
            ("missing-queue", 1),
            ("missing-segments", 1),
            ("regressed-segments", 1),
        ):
            with self.subTest(mode=mode):
                result, state = self.run_smoke(mode)
                self.assertEqual(
                    result.returncode, expected, result.stdout + result.stderr
                )
                for program, args in state["commands"]:
                    if program == "docker" and args[0] in ("stop", "rm"):
                        self.assertIn(args[-1], state["owned"])
                if mode == "unhealthy":
                    self.assertEqual(
                        len(
                            [
                                args
                                for program, args in state["commands"]
                                if program == "curl" and args[-1].endswith("/healthz")
                            ]
                        ),
                        100,
                    )


if __name__ == "__main__":
    unittest.main()

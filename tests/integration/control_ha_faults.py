"""Scoped caller-response loss for real Catalog-leader failure campaigns."""

from __future__ import annotations

import hashlib
import json
import re
import socket
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

LEADER_SCHEMA = "epoch.control-ha.catalog-leader-unknown/v1"


def validate_leader_evidence(proof: dict[str, Any]) -> None:
    count = proof.get("controller_count")
    old, new = proof.get("old_catalog_leader"), proof.get("new_catalog_leader")
    if (
        proof.get("schema") != LEADER_SCHEMA
        or type(count) is not int
        or count not in (3, 5)
        or type(old) is not int
        or type(new) is not int
        or old not in (1, 2, 3)
        or new not in (1, 2, 3)
        or old == new
        or re.fullmatch(r"[0-9a-f]{64}", str(proof.get("killed_container", ""))) is None
        or type(proof.get("pending_callers_at_kill")) is not int
        or proof["pending_callers_at_kill"] != count
        or proof.get("old_leader_stopped_before_response_drop") is not True
    ):
        raise ValueError("missing real leader loss with concurrent pending callers")
    requests = proof.get("requests")
    if not isinstance(requests, list) or len(requests) != count:
        raise ValueError("every pending caller requires original-token evidence")
    tokens = set()
    for request in requests:
        if not isinstance(request, dict):
            raise ValueError("malformed lost-response request evidence")
        token = request.get("request_token")
        if (
            not isinstance(token, str)
            or not 1 <= len(token) <= 128
            or token in tokens
            or re.fullmatch(
                r"sha256:[0-9a-f]{64}", str(request.get("command_sha256", ""))
            )
            is None
            or type(request.get("upstream_status")) is not int
            or request["upstream_status"] not in (200, 201)
            or type(request.get("generation")) is not int
            or request["generation"] != 2
            or request.get("caller_status") is not None
            or request.get("caller_unknown") is not True
            or type(request.get("replay_status")) is not int
            or request["replay_status"] != 200
            or request.get("replayed") is not True
            or type(request.get("recovered_generation")) is not int
            or request["recovered_generation"] != request["generation"]
        ):
            raise ValueError(
                "unknown caller result must resolve the exact committed token"
            )
        tokens.add(token)


def request_unknown(
    url: str, body: dict[str, Any], authorization: str
) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(body, separators=(",", ":")).encode(),
        headers={"content-type": "application/json", "authorization": authorization},
        method="PUT",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            response.read()
            return {"caller_status": response.status, "caller_unknown": False}
    except urllib.error.HTTPError as error:
        code = error.code
        error.close()
        return {"caller_status": code, "caller_unknown": False}
    except OSError as error:
        return {
            "caller_status": None,
            "caller_unknown": True,
            "caller_error_kind": type(error).__name__,
        }


class DropCommittedResponses:
    """Forward a bounded planned workload, then hold all successful responses.

    The caller sees no fabricated status: the driver kills its verified Catalog
    leader while these sockets are still pending, then explicitly closes them.
    This proves the committed/lost-ack branch of unknown-outcome recovery, not
    an uncommitted proposal or a production proxy implementation.
    """

    def __init__(
        self, plan: Mapping[str, Callable[[dict[str, Any]], Any]], authorization: str
    ) -> None:
        if not 1 <= len(plan) <= 8 or not authorization:
            raise ValueError("response-loss proxy requires a bounded explicit workload")
        self.plan = dict(plan)
        self.authorization = authorization
        self.lock = threading.Lock()
        self.release = threading.Event()
        self.committed = threading.Event()
        self.active: set[str] = set()
        self.observed: dict[str, dict[str, Any]] = {}
        self.workers: list[threading.Thread] = []
        self.started = False
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            def reject(self, code: int) -> None:
                self.send_response(code)
                self.send_header("content-length", "0")
                self.end_headers()
                self.close_connection = True

            def do_PUT(self) -> None:
                if self.path != "/v1/resources":
                    self.reject(404)
                    return
                if self.headers.get("authorization") != proxy.authorization:
                    self.reject(401)
                    return
                try:
                    size = int(self.headers.get("content-length", "0"))
                    if not 0 < size <= 512 * 1024:
                        raise ValueError("bounded body required")
                    body = json.loads(self.rfile.read(size))
                    if not isinstance(body, dict):
                        raise ValueError("object body required")
                    token = body.get("request_token")
                    if not isinstance(token, str) or token not in proxy.plan:
                        raise ValueError("unplanned request token")
                except (ValueError, OSError):
                    self.reject(400)
                    return
                with proxy.lock:
                    if token in proxy.active or token in proxy.observed:
                        duplicate = True
                    else:
                        duplicate = False
                        proxy.active.add(token)
                        proxy.workers.append(threading.current_thread())
                if duplicate:
                    self.reject(409)
                    return
                try:
                    record: dict[str, Any] = {
                        "request_token": token,
                        "command_sha256": "sha256:"
                        + hashlib.sha256(
                            json.dumps(
                                body, sort_keys=True, separators=(",", ":")
                            ).encode()
                        ).hexdigest(),
                    }
                    try:
                        response = proxy.plan[token](body)
                        record["upstream_status"] = response.status
                        resource = response.document.get("resource", {})
                        record["generation"] = (
                            resource.get("generation")
                            if isinstance(resource, dict)
                            else None
                        )
                    except Exception as error:
                        record.update(
                            upstream_status=None,
                            generation=None,
                            upstream_error_kind=type(error).__name__,
                        )
                    with proxy.lock:
                        proxy.observed[token] = record
                        if len(proxy.observed) == len(proxy.plan):
                            proxy.committed.set()
                    proxy.release.wait(timeout=60)
                    try:
                        self.connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    self.close_connection = True
                finally:
                    with proxy.lock:
                        proxy.active.discard(token)

            def log_message(self, _format: str, *_args: object) -> None:
                # No credentials, request bodies, or private response logs.
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.server.block_on_close = False
        self.server_thread = threading.Thread(
            target=lambda: self.server.serve_forever(poll_interval=0.1),
            name="epoch-owned-lost-response-proxy",
            daemon=True,
        )

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}/v1/resources"

    @property
    def pending_callers(self) -> int:
        with self.lock:
            return len(self.active)

    @property
    def records(self) -> list[dict[str, Any]]:
        with self.lock:
            return [dict(self.observed[token]) for token in sorted(self.observed)]

    def start(self) -> None:
        if self.started:
            raise ValueError("proxy already owns a server thread")
        self.started = True
        self.server_thread.start()

    def await_committed(self, *, timeout: float) -> bool:
        return self.committed.wait(timeout)

    def drop_responses(self) -> None:
        self.release.set()

    def close(self) -> None:
        self.release.set()
        if self.started:
            self.server.shutdown()
            self.server_thread.join(timeout=3)
        self.server.server_close()
        deadline = time.monotonic() + 15
        for worker in self.workers:
            worker.join(timeout=max(0, deadline - time.monotonic()))
        if self.server_thread.is_alive() or any(
            worker.is_alive() for worker in self.workers
        ):
            raise AssertionError("owned lost-response proxy threads did not stop")

    def __enter__(self) -> DropCommittedResponses:
        self.start()
        return self

    def __exit__(self, *_exception: object) -> None:
        self.close()

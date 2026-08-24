#!/usr/bin/env python3
"""Live Kubernetes alpha-exit campaign for Epoch.

The campaign deliberately uses only the Python standard library plus the
repository's declared command-line prerequisites.  It creates a disposable
five-node Kind cluster (one control plane and four workers), installs the real
operator, and proves the complete managed lifecycle:

* mTLS-protected operator install and four-profile traffic;
* encrypted semantic backup;
* one joint-consensus voter replacement from physical node 3 to node 4;
* backup-gated, one-node-at-a-time guarded image rollout; and
* fresh-cluster restore with exact Catalog and profile digest comparison.

The upgrade uses a second tag for the exact same locally built node image.  It
therefore proves the operator's rollout orchestration and persistence gates,
not mixed-version binary compatibility.  Evidence makes that boundary
explicit and makes no production SLO, throughput, latency, RPO, or RTO claim.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import dataclasses
import datetime as dt
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, TypeVar


REPO_ROOT = Path(__file__).resolve().parents[2]
NAMESPACE = "epoch-system"
SOURCE_CLUSTER = "epoch"
RESTORED_CLUSTER = "epoch-restored"
ADMIN_TOKEN = "epoch-dev-admin-v1"
CONTROL_TOKEN = "epoch-dev-control-v1"
POLICY_PATH = REPO_ROOT / "spec/auth/bootstrap-policy-v1.example.json"
KIND_NODE_IMAGE = (
    "kindest/node:v1.34.0@"
    "sha256:7416a61b42b1662ca6ca89f02028ac133a309a2a30ba309614e8ec94d976dc5a"
)
NODE_IMAGE = "epoch/node:kubernetes-alpha-exit"
UPGRADE_NODE_IMAGE = "epoch/node:kubernetes-alpha-exit-upgrade"
CONTROL_IMAGE = "epoch/control:kubernetes-alpha-exit"
OPERATOR_IMAGE = "epoch/operator:kubernetes-alpha-exit"
EVIDENCE_SCHEMA = "epoch.kubernetes-alpha-exit.evidence/v1"
BACKUP_KEY_ID = "kubernetes-alpha-exit-2026-08"
PROFILES = (
    ("stream", "orders"),
    ("cache", "sessions"),
    ("queue", "jobs"),
    ("event-bus", "events"),
)
SOURCE_IDENTITY_PATHS = (
    "VERSION",
    "deploy/docker",
    "deploy/kubernetes",
    "spec/auth",
    "tests/integration/kubernetes_alpha_exit.py",
    "tests/integration/test_kubernetes_alpha_exit.py",
)
T = TypeVar("T")


class CampaignError(RuntimeError):
    """A bounded campaign assertion or external command failed."""


@dataclasses.dataclass(frozen=True)
class HTTPResponse:
    status: int
    document: Any


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise CampaignError(message)


def exact_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str) and value.isdecimal():
        return int(value)
    return None


def wait_until(
    description: str,
    check: Callable[[], T | None | bool],
    *,
    timeout: float = 600.0,
    interval: float = 1.0,
) -> T:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result is not None and result is not False:
                return result  # type: ignore[return-value]
        except (
            CampaignError,
            OSError,
            ValueError,
            subprocess.SubprocessError,
        ) as error:
            last_error = error
        time.sleep(interval)
    suffix = f": {last_error}" if last_error is not None else ""
    raise CampaignError(f"timed out waiting for {description}{suffix}")


def source_tree_sha256() -> str:
    # Image IDs below are the exact identity of application binaries. Hash the
    # remaining inputs that affect deployment and evidence. Deliberately avoid
    # rereading image source and unrelated docs/console files: macOS may
    # represent those as iCloud placeholders, while the immutable image IDs
    # already fix the executable bytes used by this campaign.
    listed = subprocess.run(
        [
            "git",
            "ls-files",
            "-co",
            "--exclude-standard",
            "-z",
            "--",
            *SOURCE_IDENTITY_PATHS,
        ],
        cwd=REPO_ROOT,
        check=True,
        stdout=subprocess.PIPE,
    ).stdout
    paths = sorted(path for path in listed.split(b"\0") if path)
    digest = hashlib.sha256()
    for encoded in paths:
        relative = encoded.decode("utf-8", errors="strict")
        path = REPO_ROOT / relative
        require(
            path.is_file() and not path.is_symlink(),
            f"unsupported Kubernetes runtime source entry: {relative}",
        )
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        size = path.stat().st_size
        digest.update(size.to_bytes(8, "big"))
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
    return digest.hexdigest()


def source_status() -> bytes:
    return subprocess.run(
        [
            "git",
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--",
            *SOURCE_IDENTITY_PATHS,
        ],
        cwd=REPO_ROOT,
        check=True,
        stdout=subprocess.PIPE,
    ).stdout


def b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def resource_path(kind: str, name: str, shard: int = 0) -> str:
    return (
        "/experimental/v1/regional/resources/"
        f"acme/shop/dev/core/{kind}/{name}/shards/{shard}"
    )


def canonical_name(kind: str, name: str) -> str:
    return f"acme/shop/dev/core/{kind}/{name}"


def management_kind(kind: str) -> str:
    return "event_bus" if kind == "event-bus" else kind


def route_kind(kind: str) -> str:
    return "event-bus" if kind == "event_bus" else kind


def management_canonical_name(kind: str, name: str) -> str:
    return f"acme/shop/dev/core/{management_kind(kind)}/{name}"


def managed_resource_request(kind: str, name: str) -> dict[str, Any]:
    # The management model uses the protobuf/JSON enum spelling ``event_bus``;
    # the native HTTP adapter intentionally uses the URL segment ``event-bus``.
    kind_for_management = management_kind(kind)
    return {
        "request_token": f"kubernetes-create-{kind}-{name}-v1",
        "expected_generation": 0,
        "resource": {
            "organization": "acme",
            "project": "shop",
            "environment": "dev",
            "namespace": "core",
            "kind": kind_for_management,
            "name": name,
            "governance": {
                "owner": "team:platform",
                "cost_center": "cc-1042",
                "classification": "confidential",
                "tags": {"profile": kind_for_management, "service": name},
            },
            "spec": {
                "shard_count": 1,
                "replica_count": 3,
                "placement": {
                    "allowed_regions": ["ap-south"],
                    "minimum_zones": 3,
                    "required_node_class": "general-purpose",
                },
            },
        },
    }


def profile_write(kind: str, name: str, term: str, sequence: int) -> tuple[str, dict]:
    common_key = f"kubernetes-{kind}-{sequence}"
    if kind == "stream":
        return "records", {
            "idempotency_key": common_key,
            "expected_term": term,
            "partition": 0,
            "envelope": {
                "id": f"{name}-{sequence}",
                "source": "kubernetes-alpha-exit",
                "type": "order.created",
                "time_ms": str(sequence),
                "payload": {"id": sequence},
            },
        }
    if kind == "cache":
        return "mutations", {
            "idempotency_key": common_key,
            "expected_term": term,
            "operation": {
                "kind": "set",
                "key": f"session-{sequence}",
                "value": {"kind": "string", "value": "ready"},
            },
        }
    operation = "enqueue" if kind == "queue" else "publish"
    envelope_type = "job.created" if kind == "queue" else "order.created"
    return "mutations", {
        "idempotency_key": common_key,
        "expected_term": term,
        "operation": {
            "kind": operation,
            "envelope": {
                "id": f"{name}-{sequence}",
                "source": "kubernetes-alpha-exit",
                "type": envelope_type,
                "time_ms": str(sequence),
                "payload": {"id": sequence},
            },
        },
    }


def plan_single_voter_replacement(
    voters: list[str], physical_nodes: list[str]
) -> tuple[str, str, list[str]]:
    require(
        len(voters) in (3, 5)
        and voters == sorted(voters, key=int)
        and len(set(voters)) == len(voters),
        f"replacement requires a canonical three- or five-voter set: {voters}",
    )
    require(
        physical_nodes == sorted(physical_nodes, key=int)
        and len(set(physical_nodes)) == len(physical_nodes)
        and set(voters).issubset(physical_nodes),
        f"replacement voters are outside the physical-node directory: {physical_nodes}",
    )
    candidates = [node for node in physical_nodes if node not in voters]
    require(candidates, "replacement requires at least one non-voting physical node")
    removed = voters[-1]
    added = candidates[0]
    target = sorted([voter for voter in voters if voter != removed] + [added], key=int)
    return removed, added, target


class Campaign:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.kind_name = args.cluster_name
        self.context = f"kind-{self.kind_name}"
        self.secure_directory = tempfile.TemporaryDirectory(
            prefix="epoch-kubernetes-alpha-exit-secure-"
        )
        self.secure_path = Path(self.secure_directory.name)
        self.backup_path = self.secure_path / "backups"
        self.backup_path.mkdir(mode=0o777)
        self.backup_path.chmod(0o777)
        if args.evidence_dir is None:
            self.evidence_path = Path(
                tempfile.mkdtemp(prefix="epoch-kubernetes-alpha-exit-evidence-")
            )
        else:
            self.evidence_path = args.evidence_dir.resolve()
            self.evidence_path.mkdir(parents=True, exist_ok=True)
        self.created_cluster = False
        self.started_at = utc_now()
        self.started_monotonic = time.monotonic()
        self.steps: list[dict[str, Any]] = []
        self.image_ids: dict[str, str] = {}
        self.phase_history: list[str] = []
        self.result: dict[str, Any] = {}
        self.start_source_hash: str | None = None

    def command(
        self,
        arguments: list[str],
        *,
        input_text: str | None = None,
        timeout: float = 600.0,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        completed = subprocess.run(
            arguments,
            cwd=REPO_ROOT,
            input=input_text,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
        if check and completed.returncode != 0:
            raise CampaignError(
                f"command failed ({completed.returncode}): {' '.join(arguments)}\n"
                f"stdout:\n{completed.stdout[-8000:]}\n"
                f"stderr:\n{completed.stderr[-8000:]}"
            )
        return completed

    def kubectl(
        self,
        *arguments: str,
        input_text: str | None = None,
        timeout: float = 600.0,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        return self.command(
            ["kubectl", "--context", self.context, *arguments],
            input_text=input_text,
            timeout=timeout,
            check=check,
        )

    def record_step(self, name: str, **evidence: Any) -> None:
        self.steps.append({"name": name, "completed_at": utc_now(), **evidence})
        print(f"[epoch-kubernetes] completed: {name}", flush=True)

    def prerequisite_check(self) -> None:
        missing = [
            binary
            for binary in ("docker", "git", "kind", "kubectl", "openssl")
            if shutil.which(binary) is None
        ]
        require(not missing, f"missing required commands: {', '.join(missing)}")
        self.command(["docker", "info"], timeout=30)
        existing = self.command(["kind", "get", "clusters"]).stdout.splitlines()
        require(
            self.kind_name not in existing,
            f"refusing to overwrite existing Kind cluster {self.kind_name!r}",
        )
        require(POLICY_PATH.is_file(), f"missing auth policy {POLICY_PATH}")
        self.start_source_hash = source_tree_sha256()
        self.record_step("prerequisites", source_tree_sha256=self.start_source_hash)

    def build_images(self) -> None:
        revision = self.command(["git", "rev-parse", "HEAD"]).stdout.strip()
        version = "0.2.0-beta.dev"
        builds = (
            (NODE_IMAGE, "deploy/docker/Dockerfile.node"),
            (CONTROL_IMAGE, "deploy/docker/Dockerfile.control"),
            (OPERATOR_IMAGE, "deploy/docker/Dockerfile.operator"),
        )
        if not self.args.skip_build:
            for image, dockerfile in builds:
                print(f"[epoch-kubernetes] building {image}", flush=True)
                self.command(
                    [
                        "docker",
                        "build",
                        "--file",
                        dockerfile,
                        "--tag",
                        image,
                        "--build-arg",
                        f"EPOCH_VERSION={version}",
                        "--build-arg",
                        f"VCS_REF={revision}",
                        ".",
                    ],
                    timeout=3600,
                )
        for image, _dockerfile in builds:
            inspected = self.command(
                ["docker", "image", "inspect", "--format", "{{.Id}}", image]
            ).stdout.strip()
            require(
                inspected.startswith("sha256:"), f"image {image} has no immutable ID"
            )
            self.image_ids[image] = inspected
        self.command(["docker", "tag", NODE_IMAGE, UPGRADE_NODE_IMAGE])
        upgrade_id = self.command(
            ["docker", "image", "inspect", "--format", "{{.Id}}", UPGRADE_NODE_IMAGE]
        ).stdout.strip()
        require(
            upgrade_id == self.image_ids[NODE_IMAGE],
            "upgrade orchestration image must be an exact alias of the source image",
        )
        self.image_ids[UPGRADE_NODE_IMAGE] = upgrade_id
        self.record_step("exact-source-images-built", image_ids=self.image_ids)

    def create_kind_cluster(self) -> None:
        nodes = []
        for role in ("control-plane", "worker", "worker", "worker", "worker"):
            nodes.append(
                {
                    "role": role,
                    "extraMounts": [
                        {
                            "hostPath": str(self.backup_path),
                            "containerPath": "/epoch-backups",
                        }
                    ],
                }
            )
        config = {
            "kind": "Cluster",
            "apiVersion": "kind.x-k8s.io/v1alpha4",
            "nodes": nodes,
        }
        config_path = self.secure_path / "kind.json"
        config_path.write_bytes(canonical_json(config))
        print(f"[epoch-kubernetes] creating Kind cluster {self.kind_name}", flush=True)
        try:
            self.command(
                [
                    "kind",
                    "create",
                    "cluster",
                    "--name",
                    self.kind_name,
                    "--image",
                    KIND_NODE_IMAGE,
                    "--config",
                    str(config_path),
                    "--wait",
                    "300s",
                ],
                timeout=900,
            )
            self.created_cluster = True
        except Exception:
            existing = self.command(["kind", "get", "clusters"], check=False)
            self.created_cluster = self.kind_name in existing.stdout.splitlines()
            raise
        self.command(
            [
                "kind",
                "load",
                "docker-image",
                "--name",
                self.kind_name,
                NODE_IMAGE,
                UPGRADE_NODE_IMAGE,
                CONTROL_IMAGE,
                OPERATOR_IMAGE,
            ],
            timeout=900,
        )
        node_count = len(
            json.loads(self.kubectl("get", "nodes", "-o", "json").stdout)["items"]
        )
        require(node_count == 5, f"Kind topology has {node_count} nodes, expected 5")
        self.record_step(
            "kind-cluster-created", nodes=node_count, image=KIND_NODE_IMAGE
        )

    def generate_certificate(
        self,
        name: str,
        common_name: str,
        dns_names: list[str],
    ) -> tuple[bytes, bytes]:
        key = self.secure_path / f"{name}.key"
        csr = self.secure_path / f"{name}.csr"
        certificate = self.secure_path / f"{name}.crt"
        extension = self.secure_path / f"{name}.ext"
        sans = ",".join(f"DNS:{value}" for value in sorted(set(dns_names)))
        extension.write_text(
            "basicConstraints=critical,CA:FALSE\n"
            "keyUsage=critical,digitalSignature,keyEncipherment\n"
            "extendedKeyUsage=serverAuth,clientAuth\n"
            f"subjectAltName={sans}\n",
            encoding="utf-8",
        )
        self.command(["openssl", "genrsa", "-out", str(key), "2048"])
        key.chmod(0o600)
        self.command(
            [
                "openssl",
                "req",
                "-new",
                "-key",
                str(key),
                "-out",
                str(csr),
                "-subj",
                f"/CN={common_name}",
            ]
        )
        self.command(
            [
                "openssl",
                "x509",
                "-req",
                "-in",
                str(csr),
                "-CA",
                str(self.secure_path / "ca.crt"),
                "-CAkey",
                str(self.secure_path / "ca.key"),
                "-CAcreateserial",
                "-out",
                str(certificate),
                "-days",
                "3",
                "-sha256",
                "-extfile",
                str(extension),
            ]
        )
        return certificate.read_bytes(), key.read_bytes()

    def generate_pki(self) -> dict[str, bytes]:
        ca_key = self.secure_path / "ca.key"
        ca_certificate = self.secure_path / "ca.crt"
        self.command(["openssl", "genrsa", "-out", str(ca_key), "2048"])
        ca_key.chmod(0o600)
        self.command(
            [
                "openssl",
                "req",
                "-x509",
                "-new",
                "-key",
                str(ca_key),
                "-sha256",
                "-days",
                "3",
                "-out",
                str(ca_certificate),
                "-subj",
                "/CN=Epoch Kubernetes alpha-exit test CA",
            ]
        )
        data_names: list[str] = []
        control_names: list[str] = []
        for cluster in (SOURCE_CLUSTER, RESTORED_CLUSTER):
            peer = f"{cluster}-peer"
            public = cluster
            control = f"{cluster}-control"
            data_names.extend(
                [
                    peer,
                    f"{peer}.{NAMESPACE}",
                    f"{peer}.{NAMESPACE}.svc",
                    f"{peer}.{NAMESPACE}.svc.cluster.local",
                    public,
                    f"{public}.{NAMESPACE}",
                    f"{public}.{NAMESPACE}.svc",
                    f"{public}.{NAMESPACE}.svc.cluster.local",
                ]
            )
            control_names.extend(
                [
                    control,
                    f"{control}.{NAMESPACE}",
                    f"{control}.{NAMESPACE}.svc",
                    f"{control}.{NAMESPACE}.svc.cluster.local",
                ]
            )
            for ordinal in range(4):
                node = f"{cluster}-node-{ordinal}.{peer}"
                data_names.extend(
                    [
                        node,
                        f"{node}.{NAMESPACE}",
                        f"{node}.{NAMESPACE}.svc",
                        f"{node}.{NAMESPACE}.svc.cluster.local",
                    ]
                )
            control_pod = f"{control}-0.{control}"
            control_names.extend(
                [
                    control_pod,
                    f"{control_pod}.{NAMESPACE}",
                    f"{control_pod}.{NAMESPACE}.svc",
                    f"{control_pod}.{NAMESPACE}.svc.cluster.local",
                ]
            )
        data_certificate, data_key = self.generate_certificate(
            "data", "epoch-data-plane", data_names
        )
        control_certificate, control_key = self.generate_certificate(
            "control", "epoch-control-plane", control_names
        )
        return {
            "ca": ca_certificate.read_bytes(),
            "data_certificate": data_certificate,
            "data_key": data_key,
            "control_certificate": control_certificate,
            "control_key": control_key,
        }

    def apply(self, document: dict[str, Any]) -> None:
        self.kubectl("apply", "-f", "-", input_text=json.dumps(document))

    def install_operator_and_prerequisites(self) -> None:
        self.kubectl("apply", "-k", "deploy/kubernetes/operator")
        self.kubectl(
            "-n",
            NAMESPACE,
            "set",
            "image",
            "deployment/epoch-operator",
            f"operator={OPERATOR_IMAGE}",
        )
        self.kubectl(
            "-n",
            NAMESPACE,
            "rollout",
            "status",
            "deployment/epoch-operator",
            "--timeout=300s",
            timeout=360,
        )
        pki = self.generate_pki()
        policy = POLICY_PATH.read_text(encoding="utf-8")
        resources = [
            {
                "apiVersion": "v1",
                "kind": "PersistentVolume",
                "metadata": {"name": "epoch-backups-pv"},
                "spec": {
                    "capacity": {"storage": "1Gi"},
                    "accessModes": ["ReadWriteMany"],
                    "persistentVolumeReclaimPolicy": "Retain",
                    "storageClassName": "epoch-static",
                    "volumeMode": "Filesystem",
                    "hostPath": {"path": "/epoch-backups", "type": "Directory"},
                },
            },
            {
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {"name": "epoch-backups", "namespace": NAMESPACE},
                "spec": {
                    "accessModes": ["ReadWriteMany"],
                    "storageClassName": "epoch-static",
                    "volumeName": "epoch-backups-pv",
                    "resources": {"requests": {"storage": "1Gi"}},
                },
            },
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "epoch-auth-policy", "namespace": NAMESPACE},
                "data": {"bootstrap-policy.json": policy},
            },
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": "epoch-control-credentials",
                    "namespace": NAMESPACE,
                },
                "type": "Opaque",
                "data": {"regional-token": b64(CONTROL_TOKEN.encode("utf-8"))},
            },
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": "epoch-data-plane-tls", "namespace": NAMESPACE},
                "type": "kubernetes.io/tls",
                "data": {
                    "ca.crt": b64(pki["ca"]),
                    "tls.crt": b64(pki["data_certificate"]),
                    "tls.key": b64(pki["data_key"]),
                },
            },
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": "epoch-control-plane-tls",
                    "namespace": NAMESPACE,
                },
                "type": "kubernetes.io/tls",
                "data": {
                    "ca.crt": b64(pki["ca"]),
                    "tls.crt": b64(pki["control_certificate"]),
                    "tls.key": b64(pki["control_key"]),
                },
            },
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": "epoch-backup-key", "namespace": NAMESPACE},
                "type": "Opaque",
                "data": {"encryption.key": b64(os.urandom(32))},
            },
        ]
        for resource in resources:
            self.apply(resource)
        wait_until(
            "ReadWriteMany backup PVC to bind",
            lambda: (
                self.get_json("pvc", "epoch-backups").get("status", {}).get("phase")
                == "Bound"
            ),
            timeout=120,
        )
        self.record_step("operator-and-prerequisites-installed")

    def epoch_cluster_document(
        self, name: str, *, restore_object: str | None = None
    ) -> dict[str, Any]:
        spec: dict[str, Any] = {
            "nodeImage": UPGRADE_NODE_IMAGE if restore_object else NODE_IMAGE,
            "controlImage": CONTROL_IMAGE,
            "region": "ap-south",
            "nodeClass": "general-purpose",
            "replicas": 4,
            "catalogReplicas": 3,
            "storage": "256Mi",
            "storageClassName": "standard",
            "authPolicyConfigMap": "epoch-auth-policy",
            "credentialSecret": "epoch-control-credentials",
            "transportSecurity": {
                "dataPlaneSecret": "epoch-data-plane-tls",
                "controlPlaneSecret": "epoch-control-plane-tls",
                "regionalServerName": f"{name}-peer.{NAMESPACE}.svc",
            },
            "backup": {
                "schedule": "0 0 1 1 *",
                "destinationPVC": "epoch-backups",
                "encryptionSecret": "epoch-backup-key",
                "keyID": BACKUP_KEY_ID,
                "retentionCount": 7,
            },
            "upgrade": {
                "backupMaxAgeSeconds": 3600,
                "stepDeadlineSeconds": 300,
                "rollbackOnFailure": True,
            },
            "serviceType": "ClusterIP",
            "allowedOrigins": ["https://console.example.test"],
            "nodeResources": {
                "requests": {"cpu": "50m", "memory": "96Mi"},
                "limits": {"cpu": "1", "memory": "512Mi"},
            },
            "controlResources": {
                "requests": {"cpu": "25m", "memory": "48Mi"},
                "limits": {"cpu": "500m", "memory": "256Mi"},
            },
        }
        if restore_object is not None:
            spec["restore"] = {
                "objectName": restore_object,
                "encryptionSecret": "epoch-backup-key",
            }
        return {
            "apiVersion": "platform.epoch.dev/v1alpha1",
            "kind": "EpochCluster",
            "metadata": {"name": name, "namespace": NAMESPACE},
            "spec": spec,
        }

    def get_json(
        self, kind: str, name: str | None = None, *extra: str
    ) -> dict[str, Any]:
        arguments = ["-n", NAMESPACE, "get", kind]
        if name is not None:
            arguments.append(name)
        arguments.extend(extra)
        arguments.extend(["-o", "json"])
        return json.loads(self.kubectl(*arguments).stdout)

    def wait_cluster_ready(
        self, name: str, *, timeout: float = 900.0
    ) -> dict[str, Any]:
        def ready() -> dict[str, Any] | None:
            cluster = self.get_json("epochcluster", name)
            status = cluster.get("status", {})
            if (
                status.get("readyNodes") == 4
                and status.get("controlReady") is True
                and status.get("upgrade", {}).get("phase") == "Stable"
            ):
                return cluster
            return None

        cluster = wait_until(
            f"EpochCluster {name} to become ready", ready, timeout=timeout
        )

        def live_apis() -> bool:
            control = self.control_request(name, "GET", "/v1/regional/resources")
            catalog = self.data_request(
                name, 0, "GET", "/experimental/v1/regional/catalog"
            )
            return control.status == 200 and catalog.status == 200

        wait_until(
            f"EpochCluster {name} mTLS APIs to accept traffic",
            live_apis,
            timeout=180,
        )
        return cluster

    def install_source_cluster(self) -> None:
        self.apply(self.epoch_cluster_document(SOURCE_CLUSTER))
        cluster = self.wait_cluster_ready(SOURCE_CLUSTER)
        self.record_step(
            "source-cluster-ready",
            ready_nodes=cluster["status"]["readyNodes"],
            endpoint=cluster["status"]["endpoint"],
        )

    def pod_request(
        self,
        execution_cluster: str,
        target: str,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 20.0,
    ) -> HTTPResponse:
        marker = "__EPOCH_HTTP_STATUS__:"
        arguments = [
            "-n",
            NAMESPACE,
            "exec",
            f"{execution_cluster}-node-0",
            "--",
            "curl",
            "--silent",
            "--show-error",
            "--connect-timeout",
            "3",
            "--max-time",
            str(int(timeout)),
            "--noproxy",
            "*",
            "--cacert",
            "/etc/epoch/tls/ca.crt",
            "--cert",
            "/etc/epoch/tls/tls.crt",
            "--key",
            "/etc/epoch/tls/tls.key",
            "--request",
            method,
            "--header",
            "accept: application/json",
            "--header",
            f"authorization: Bearer {ADMIN_TOKEN}",
        ]
        for key, value in (headers or {}).items():
            arguments.extend(["--header", f"{key}: {value}"])
        if body is not None:
            arguments.extend(
                [
                    "--header",
                    "content-type: application/json",
                    "--data-binary",
                    canonical_json(body).decode("utf-8"),
                ]
            )
        arguments.extend(["--write-out", f"\n{marker}%{{http_code}}", target + path])
        completed = self.kubectl(*arguments, timeout=timeout + 15)
        payload, separator, status_text = completed.stdout.rpartition(marker)
        require(
            separator == marker,
            f"curl response did not contain status marker: {payload}",
        )
        status = int(status_text.strip())
        payload = payload.rstrip("\n")
        document: Any = {}
        if payload:
            document = json.loads(payload)
        return HTTPResponse(status=status, document=document)

    def data_request(
        self,
        cluster: str,
        ordinal: int,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> HTTPResponse:
        target = f"https://{cluster}-node-{ordinal}.{cluster}-peer:7601"
        return self.pod_request(
            cluster, target, method, path, body=body, headers=headers
        )

    def control_request(
        self,
        cluster: str,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
    ) -> HTTPResponse:
        return self.pod_request(
            cluster,
            f"https://{cluster}-control:8080",
            method,
            path,
            body=body,
            headers={"origin": "https://console.example.test"},
        )

    def create_profiles(self) -> None:
        for kind, name in PROFILES:
            response = self.control_request(
                SOURCE_CLUSTER,
                "PUT",
                "/v1/resources",
                body=managed_resource_request(kind, name),
            )
            require(
                response.status == 201,
                f"managed {kind}/{name} creation returned {response.status}: {response.document}",
            )

        def all_ready() -> dict[str, Any] | None:
            response = self.control_request(
                SOURCE_CLUSTER, "GET", "/v1/regional/resources"
            )
            if response.status != 200 or not isinstance(response.document, dict):
                return None
            resources = response.document.get("resources")
            if not isinstance(resources, list) or len(resources) != len(PROFILES):
                return None
            observed = {
                item.get("canonical_name"): item
                for item in resources
                if isinstance(item, dict)
            }
            for kind, name in PROFILES:
                resource = observed.get(management_canonical_name(kind, name))
                if not isinstance(resource, dict) or resource.get("phase") != "ready":
                    return None
                tablets = resource.get("tablets")
                if not isinstance(tablets, list) or len(tablets) != 1:
                    return None
                tablet = tablets[0]
                voters = tablet.get("voter_node_ids")
                reachable = tablet.get("reachable_voter_node_ids")
                if (
                    not isinstance(voters, list)
                    or len(voters) != 3
                    or voters != sorted(voters, key=int)
                    or len(set(voters)) != 3
                    or not set(voters).issubset({"1", "2", "3", "4"})
                    or reachable != voters
                ):
                    return None
                placement = resource.get("placement", {})
                nodes = placement.get("nodes") if isinstance(placement, dict) else None
                if (
                    not isinstance(nodes, list)
                    or placement.get("achieved_zones", 0) < 3
                    or len({node.get("zone") for node in nodes}) < 3
                ):
                    return None
            return response.document

        inventory = wait_until(
            "all managed profiles to reach ready placement", all_ready, timeout=600
        )
        self.record_step(
            "four-managed-profiles-ready",
            resource_count=len(inventory["resources"]),
            profiles=[kind for kind, _name in PROFILES],
        )

    def catalog_snapshot(self, cluster: str) -> dict[str, Any]:
        observations: list[dict[str, Any]] = []
        for ordinal in range(3):
            response = self.data_request(
                cluster, ordinal, "GET", "/experimental/v1/regional/catalog"
            )
            if response.status != 200 or not isinstance(response.document, dict):
                raise CampaignError(
                    f"catalog node {ordinal + 1} returned {response.status}: {response.document}"
                )
            observations.append(response.document)
        digests = {item.get("state_digest") for item in observations}
        resources = [canonical_json(item.get("resources")) for item in observations]
        require(
            len(digests) == 1 and None not in digests, "Catalog voters did not converge"
        )
        require(
            len(set(resources)) == 1, "Catalog resource descriptors did not converge"
        )
        return observations[0]

    def route_leader(
        self, cluster: str, kind: str, name: str
    ) -> tuple[int, dict[str, Any]]:
        def elected() -> tuple[int, dict[str, Any]] | None:
            leaders: list[tuple[int, dict[str, Any]]] = []
            for ordinal in range(4):
                response = self.data_request(
                    cluster, ordinal, "GET", resource_path(kind, name)
                )
                if response.status == 200 and isinstance(response.document, dict):
                    if response.document.get("accepts_writes") is True:
                        leaders.append((ordinal, response.document))
            return leaders[0] if len(leaders) == 1 else None

        return wait_until(
            f"one leader for {cluster} {kind}/{name}", elected, timeout=300
        )

    def write_profiles(self, cluster: str, sequence: int) -> None:
        for kind, name in PROFILES:
            ordinal, route = self.route_leader(cluster, kind, name)
            term = route.get("term")
            generation = route.get("resource_generation")
            tablet_epoch = route.get("tablet_epoch")
            require(
                isinstance(term, str)
                and term.isdecimal()
                and isinstance(generation, str)
                and generation.isdecimal()
                and isinstance(tablet_epoch, str)
                and tablet_epoch.isdecimal(),
                f"route for {kind}/{name} has invalid browser-safe fences: {route}",
            )
            operation, body = profile_write(kind, name, term, sequence)
            response = self.data_request(
                cluster,
                ordinal,
                "POST",
                f"{resource_path(kind, name)}/data/{operation}",
                body=body,
                headers={
                    "x-epoch-resource-generation": generation,
                    "x-epoch-tablet-epoch": tablet_epoch,
                },
            )
            require(
                200 <= response.status < 300
                and isinstance(response.document, dict)
                and response.document.get("state") == "committed",
                f"{kind}/{name} write failed: {response.status} {response.document}",
            )
        self.wait_profile_convergence(cluster, minimum_applied=sequence)
        self.record_step(
            f"{cluster}-all-profile-traffic-{sequence}",
            profiles=[kind for kind, _name in PROFILES],
        )

    def wait_profile_convergence(
        self, cluster: str, *, minimum_applied: int
    ) -> dict[str, dict[str, Any]]:
        def converged() -> dict[str, dict[str, Any]] | None:
            catalog = self.catalog_snapshot(cluster)
            result: dict[str, dict[str, Any]] = {}
            resources = catalog.get("resources")
            if not isinstance(resources, list) or len(resources) != len(PROFILES):
                return None
            for resource in resources:
                if not isinstance(resource, dict):
                    return None
                name_document = resource.get("name")
                if not isinstance(name_document, dict):
                    return None
                catalog_kind = name_document.get("kind")
                name = name_document.get("name")
                tablets = resource.get("tablets")
                if not isinstance(catalog_kind, str) or not isinstance(name, str):
                    return None
                kind = route_kind(catalog_kind)
                if not isinstance(tablets, list) or len(tablets) != 1:
                    return None
                tablet = tablets[0]
                voters = tablet.get("voter_node_ids")
                generation = tablet.get("resource_generation")
                epoch = tablet.get("tablet_epoch")
                if not isinstance(voters, list) or not all(
                    isinstance(value, str) and value.isdecimal() for value in voters
                ):
                    return None
                observations: list[tuple[int, str]] = []
                for voter in voters:
                    response = self.data_request(
                        cluster,
                        int(voter) - 1,
                        "GET",
                        f"{resource_path(kind, name)}/data/status",
                        headers={
                            "x-epoch-resource-generation": str(generation),
                            "x-epoch-tablet-epoch": str(epoch),
                            "x-epoch-read-consistency": "local_stale",
                        },
                    )
                    if response.status != 200 or not isinstance(
                        response.document, dict
                    ):
                        return None
                    applied = exact_int(response.document.get("applied_command_count"))
                    digest = response.document.get("state_digest")
                    if (
                        applied is None
                        or applied < minimum_applied
                        or not isinstance(digest, str)
                    ):
                        return None
                    observations.append((applied, digest))
                if len(set(observations)) != 1:
                    return None
                result[canonical_name(kind, name)] = {
                    "applied_command_count": str(observations[0][0]),
                    "state_digest": observations[0][1],
                    "voter_node_ids": voters,
                }
            return result

        return wait_until(
            f"all {cluster} profile voters to converge", converged, timeout=600
        )

    def capture_state(self, cluster: str) -> dict[str, Any]:
        profiles = self.wait_profile_convergence(cluster, minimum_applied=1)
        catalog = self.catalog_snapshot(cluster)
        resources = catalog.get("resources")
        return {
            "catalog_state_digest": catalog["state_digest"],
            "catalog_resources_sha256": sha256_bytes(canonical_json(resources)),
            "resource_count": catalog["resource_count"],
            "tablet_count": catalog["tablet_count"],
            "profiles": profiles,
        }

    def run_backup(self, cluster: str, label: str) -> dict[str, Any]:
        job_name = f"{cluster}-backup-{label}-{int(time.time())}"
        self.kubectl(
            "-n",
            NAMESPACE,
            "create",
            "job",
            f"--from=cronjob/{cluster}-backup",
            job_name,
        )
        self.kubectl(
            "-n",
            NAMESPACE,
            "wait",
            "--for=condition=complete",
            f"job/{job_name}",
            "--timeout=600s",
            timeout=660,
        )

        def receipt_ready() -> dict[str, Any] | None:
            pods = self.get_json("pods", None, "-l", f"job-name={job_name}")
            for pod in pods.get("items", []):
                statuses = pod.get("status", {}).get("containerStatuses", [])
                for status in statuses:
                    terminated = status.get("state", {}).get("terminated")
                    if status.get("name") != "epoch-backup" or not isinstance(
                        terminated, dict
                    ):
                        continue
                    message = terminated.get("message")
                    if terminated.get("exitCode") == 0 and isinstance(message, str):
                        return json.loads(message)
            return None

        receipt = wait_until(
            f"termination receipt for backup Job {job_name}", receipt_ready, timeout=120
        )
        require(
            receipt.get("state") == "succeeded", f"invalid backup receipt: {receipt}"
        )
        require(
            receipt.get("key_id") == BACKUP_KEY_ID,
            f"backup receipt has wrong key ID: {receipt}",
        )
        object_name = receipt.get("object_name")
        require(
            isinstance(object_name, str) and object_name.endswith(".epoch-backup.enc"),
            f"backup receipt has unsafe object name: {receipt}",
        )

        def reflected() -> dict[str, Any] | None:
            status = (
                self.get_json("epochcluster", cluster)
                .get("status", {})
                .get("backup", {})
            )
            return status if status.get("lastSuccessfulObject") == object_name else None

        status = wait_until(
            f"operator status to reflect backup {object_name}", reflected, timeout=180
        )
        self.record_step(
            f"{label}-encrypted-backup",
            job=job_name,
            receipt=receipt,
            operator_status=status,
        )
        return receipt

    def replace_stream_voter(self) -> dict[str, Any]:
        def stream_inventory() -> dict[str, Any] | None:
            response = self.control_request(
                SOURCE_CLUSTER, "GET", "/v1/regional/resources"
            )
            if response.status != 200 or not isinstance(response.document, dict):
                return None
            for resource in response.document.get("resources", []):
                if resource.get("canonical_name") == canonical_name("stream", "orders"):
                    return resource
            return None

        before = wait_until(
            "Stream inventory before voter replacement", stream_inventory
        )
        tablet = before["tablets"][0]
        initial_voters = tablet.get("voter_node_ids")
        require(
            isinstance(initial_voters, list)
            and len(initial_voters) == 3
            and initial_voters == sorted(initial_voters, key=int)
            and set(initial_voters).issubset({"1", "2", "3", "4"}),
            f"unexpected initial Stream voters: {tablet}",
        )
        removed_voter, added_voter, target_voters = plan_single_voter_replacement(
            initial_voters, ["1", "2", "3", "4"]
        )
        body = {
            "request_token": (
                f"kubernetes-replace-stream-{removed_voter}-with-{added_voter}-v1"
            ),
            "expected_tablet_epoch": tablet["tablet_epoch"],
            "expected_resource_generation": tablet["resource_generation"],
            "target_voter_node_ids": target_voters,
        }
        accepted: HTTPResponse | None = None
        for ordinal in range(3):
            response = self.data_request(
                SOURCE_CLUSTER,
                ordinal,
                "POST",
                "/experimental/v1/regional/catalog/tablets/"
                f"{tablet['tablet_id']}/membership",
                body=body,
            )
            if response.status == 202:
                accepted = response
                break
            require(
                response.status == 409,
                f"membership plan returned {response.status}: {response.document}",
            )
        require(accepted is not None, "no Catalog leader accepted the membership plan")

        def replaced() -> dict[str, Any] | None:
            resource = stream_inventory()
            if resource is None or resource.get("phase") != "ready":
                return None
            current = resource.get("tablets", [None])[0]
            if not isinstance(current, dict):
                return None
            if (
                current.get("voter_node_ids") == target_voters
                and current.get("reachable_voter_node_ids") == target_voters
                and current.get("target_voter_node_ids", []) == []
            ):
                return resource
            return None

        after = wait_until(
            "Stream joint-consensus replacement to finalize", replaced, timeout=600
        )
        profiles = self.wait_profile_convergence(SOURCE_CLUSTER, minimum_applied=1)
        stream = profiles[canonical_name("stream", "orders")]
        require(
            stream["voter_node_ids"] == target_voters,
            f"Stream state did not move to replacement node {added_voter}: {stream}",
        )
        evidence = {
            "tablet_id": tablet["tablet_id"],
            "removed_voter": removed_voter,
            "added_voter": added_voter,
            "before_voters": initial_voters,
            "after_voters": after["tablets"][0]["voter_node_ids"],
            "state_digest": stream["state_digest"],
        }
        self.record_step("stream-single-voter-replacement", **evidence)
        return evidence

    def patch_upgrade_and_wait_for_backup_gate(self) -> None:
        patch = json.dumps({"spec": {"nodeImage": UPGRADE_NODE_IMAGE}})
        self.kubectl(
            "-n",
            NAMESPACE,
            "patch",
            "epochcluster",
            SOURCE_CLUSTER,
            "--type=merge",
            "-p",
            patch,
        )

        def waiting() -> dict[str, Any] | None:
            upgrade = (
                self.get_json("epochcluster", SOURCE_CLUSTER)
                .get("status", {})
                .get("upgrade", {})
            )
            phase = upgrade.get("phase")
            if isinstance(phase, str) and (
                not self.phase_history or self.phase_history[-1] != phase
            ):
                self.phase_history.append(phase)
            return upgrade if phase == "WaitingForBackup" else None

        upgrade = wait_until(
            "upgrade to stop at fresh-backup gate", waiting, timeout=180
        )
        require(
            upgrade.get("currentNodeImage") == NODE_IMAGE
            and upgrade.get("targetNodeImage") == UPGRADE_NODE_IMAGE,
            f"upgrade persisted the wrong image identities: {upgrade}",
        )
        self.record_step("upgrade-waiting-for-post-request-backup", upgrade=upgrade)

    def wait_upgrade_stable(self) -> dict[str, Any]:
        def stable() -> dict[str, Any] | None:
            cluster = self.get_json("epochcluster", SOURCE_CLUSTER)
            upgrade = cluster.get("status", {}).get("upgrade", {})
            phase = upgrade.get("phase")
            if isinstance(phase, str) and (
                not self.phase_history or self.phase_history[-1] != phase
            ):
                self.phase_history.append(phase)
            if phase == "Failed":
                raise CampaignError(f"guarded upgrade failed: {upgrade}")
            if (
                phase == "Stable"
                and upgrade.get("currentNodeImage") == UPGRADE_NODE_IMAGE
                and cluster.get("status", {}).get("readyNodes") == 4
            ):
                return cluster
            return None

        cluster = wait_until("guarded upgrade to become stable", stable, timeout=1200)
        pods = self.get_json(
            "pods",
            None,
            "-l",
            "app.kubernetes.io/instance=epoch,app.kubernetes.io/component=data-plane",
        )
        images = {
            container.get("image")
            for pod in pods.get("items", [])
            for container in pod.get("spec", {}).get("containers", [])
        }
        require(
            images == {UPGRADE_NODE_IMAGE}, f"upgrade left mixed pod tags: {images}"
        )
        jobs = self.get_json(
            "jobs", None, "-l", "platform.epoch.dev/upgrade-owner=epoch"
        ).get("items", [])
        stages = [
            job.get("metadata", {})
            .get("labels", {})
            .get("platform.epoch.dev/upgrade-stage")
            for job in jobs
        ]
        expected_stages = {
            f"{stage}:{ordinal}"
            for ordinal in range(4)
            for stage in ("preflight", "drain", "postflight")
        }
        require(
            len(jobs) == 12
            and all(job.get("status", {}).get("succeeded") == 1 for job in jobs),
            f"upgrade did not retain 12 successful maintenance Jobs: {stages}",
        )
        # Names end in ``-<ordinal>-<stage>``; assert the full serial plan.
        normalized = {
            f"{job['metadata']['labels']['platform.epoch.dev/upgrade-stage']}:"
            f"{job['metadata']['name'].rsplit('-', 2)[1]}"
            for job in jobs
        }
        require(
            normalized == expected_stages,
            f"upgrade stage evidence is incomplete: {normalized}",
        )
        self.record_step(
            "guarded-four-node-rollout-stable",
            phase_history=self.phase_history,
            maintenance_jobs=len(jobs),
            node_image=UPGRADE_NODE_IMAGE,
            compatibility_claim="same-binary tag rollout orchestration only",
        )
        return cluster

    def restore_and_compare(
        self, receipt: dict[str, Any], source_state: dict[str, Any]
    ) -> dict[str, Any]:
        object_name = receipt["object_name"]
        self.apply(
            self.epoch_cluster_document(RESTORED_CLUSTER, restore_object=object_name)
        )
        restored = self.wait_cluster_ready(RESTORED_CLUSTER, timeout=1200)
        status = restored.get("status", {})
        require(
            status.get("restoreObject") == object_name
            and status.get("restoreEncryptionSecret") == "epoch-backup-key",
            f"operator did not persist the immutable restore identity: {status}",
        )
        restored_state = self.capture_state(RESTORED_CLUSTER)
        require(
            restored_state == source_state,
            "fresh restore state differs from backup source:\n"
            + json.dumps(
                {"source": source_state, "restored": restored_state},
                sort_keys=True,
                indent=2,
            ),
        )
        self.record_step(
            "fresh-cluster-restore-digests-match",
            object_name=object_name,
            state=restored_state,
        )
        self.write_profiles(RESTORED_CLUSTER, 2)
        return restored_state

    def collect_diagnostics(self) -> None:
        if not self.created_cluster:
            return
        commands = {
            "resources.json": [
                "-n",
                NAMESPACE,
                "get",
                "all,pvc,epochcluster,jobs",
                "-o",
                "json",
            ],
            "events.txt": [
                "-n",
                NAMESPACE,
                "get",
                "events",
                "--sort-by=.metadata.creationTimestamp",
            ],
            "nodes.json": ["get", "nodes", "-o", "json"],
        }
        for name, arguments in commands.items():
            completed = self.kubectl(*arguments, check=False, timeout=120)
            (self.evidence_path / name).write_text(
                completed.stdout
                + ("\nSTDERR:\n" + completed.stderr if completed.stderr else ""),
                encoding="utf-8",
            )
        pods = self.kubectl(
            "-n", NAMESPACE, "get", "pods", "-o", "name", check=False
        ).stdout.splitlines()
        log_directory = self.evidence_path / "logs"
        log_directory.mkdir(exist_ok=True)
        for pod in pods:
            safe_name = pod.replace("/", "-")
            completed = self.kubectl(
                "-n",
                NAMESPACE,
                "logs",
                pod,
                "--all-containers=true",
                "--prefix=true",
                check=False,
                timeout=120,
            )
            (log_directory / f"{safe_name}.log").write_text(
                completed.stdout
                + ("\nSTDERR:\n" + completed.stderr if completed.stderr else ""),
                encoding="utf-8",
            )

    def write_evidence(self, status: str, error: str | None = None) -> None:
        revision = self.command(["git", "rev-parse", "HEAD"]).stdout.strip()
        runtime_source_status = source_status()
        end_source_hash = source_tree_sha256()
        kubernetes_version: str | None = None
        if self.created_cluster:
            completed = self.kubectl("version", "-o", "json", check=False)
            if completed.returncode == 0:
                kubernetes_version = (
                    json.loads(completed.stdout)
                    .get("serverVersion", {})
                    .get("gitVersion")
                )
        evidence = {
            "schema": EVIDENCE_SCHEMA,
            "status": status,
            "started_at": self.started_at,
            "completed_at": utc_now(),
            "duration_ms": int((time.monotonic() - self.started_monotonic) * 1000),
            "identity": {
                "git_revision": revision,
                "worktree_dirty": bool(runtime_source_status),
                "source_tree_sha256": end_source_hash,
                "source_tree_sha256_at_start": self.start_source_hash,
                "source_tree_scope": (
                    "deployment-and-campaign-inputs; executable source fixed separately "
                    "by immutable image IDs"
                ),
                "source_identity_drifted": (
                    self.start_source_hash is not None
                    and self.start_source_hash != end_source_hash
                ),
                "image_ids": self.image_ids,
                "kind_node_image": KIND_NODE_IMAGE,
                "kubernetes_version": kubernetes_version,
            },
            "scope": {
                "physical_nodes": 4,
                "catalog_voters": 3,
                "profiles": [kind for kind, _name in PROFILES],
                "operations": [
                    "install",
                    "traffic",
                    "encrypted_backup",
                    "single_voter_replacement",
                    "guarded_same_binary_tag_rollout",
                    "fresh_restore",
                    "digest_comparison",
                    "post_restore_traffic",
                ],
            },
            "claims": {
                "live_kubernetes_lifecycle": status == "passed",
                "mixed_version_compatibility": False,
                "production_slo": False,
                "throughput_or_latency": False,
                "rpo_or_rto": False,
            },
            "steps": self.steps,
            "result": self.result,
            "error": error,
        }
        destination = self.evidence_path / "evidence.json"
        temporary = self.evidence_path / ".evidence.json.tmp"
        temporary.write_bytes(canonical_json(evidence) + b"\n")
        os.replace(temporary, destination)
        receipts: list[str] = []
        for path in sorted(self.evidence_path.rglob("*")):
            if path.is_file() and path.name != "manifest.sha256":
                relative = path.relative_to(self.evidence_path).as_posix()
                receipts.append(f"{sha256_bytes(path.read_bytes())}  {relative}")
        (self.evidence_path / "manifest.sha256").write_text(
            "\n".join(receipts) + "\n", encoding="utf-8"
        )

    def cleanup(self) -> None:
        if self.created_cluster and not self.args.keep_cluster:
            print(
                f"[epoch-kubernetes] deleting Kind cluster {self.kind_name}", flush=True
            )
            self.command(
                ["kind", "delete", "cluster", "--name", self.kind_name],
                timeout=300,
                check=False,
            )
            self.created_cluster = False
        self.secure_directory.cleanup()

    def run(self) -> None:
        error: str | None = None
        status = "failed"
        try:
            self.prerequisite_check()
            self.build_images()
            self.create_kind_cluster()
            self.install_operator_and_prerequisites()
            self.install_source_cluster()
            self.create_profiles()
            self.write_profiles(SOURCE_CLUSTER, 1)
            first_backup = self.run_backup(SOURCE_CLUSTER, "pre-replacement")
            replacement = self.replace_stream_voter()
            self.patch_upgrade_and_wait_for_backup_gate()
            restore_backup = self.run_backup(SOURCE_CLUSTER, "upgrade-gate")
            require(
                restore_backup["object_name"] != first_backup["object_name"],
                "post-upgrade-request backup reused the pre-request object",
            )
            self.wait_upgrade_stable()
            source_state = self.capture_state(SOURCE_CLUSTER)
            restored_state = self.restore_and_compare(restore_backup, source_state)
            require(
                self.start_source_hash == source_tree_sha256(),
                "repository source identity changed during the live campaign",
            )
            self.result = {
                "invariants": {
                    "all_profiles_committed": True,
                    "encrypted_backup_reflected_in_status": True,
                    "single_voter_replacement_finalized": True,
                    "fresh_backup_gated_upgrade": True,
                    "one_node_at_a_time_maintenance_jobs": True,
                    "catalog_digest_restored": True,
                    "profile_digests_restored": True,
                    "restored_cluster_accepts_traffic": True,
                },
                "first_backup_object": first_backup["object_name"],
                "restore_backup_object": restore_backup["object_name"],
                "replacement": replacement,
                "source_state": source_state,
                "restored_state": restored_state,
            }
            status = "passed"
        except BaseException as caught:  # evidence must record interrupts too
            error = f"{type(caught).__name__}: {caught}"
            print(f"[epoch-kubernetes] FAILED: {error}", file=sys.stderr, flush=True)
        finally:
            with contextlib.suppress(Exception):
                self.collect_diagnostics()
            with contextlib.suppress(Exception):
                self.write_evidence(status, error)
            self.cleanup()
        print(f"[epoch-kubernetes] evidence: {self.evidence_path}", flush=True)
        if status != "passed":
            raise CampaignError(error or "Kubernetes campaign failed")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cluster-name",
        default=f"epoch-alpha-exit-{os.getpid()}",
        help="unique disposable Kind cluster name",
    )
    parser.add_argument(
        "--evidence-dir",
        type=Path,
        help="directory for non-secret evidence (a temporary directory is used by default)",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="reuse already-built exact local image tags",
    )
    parser.add_argument(
        "--keep-cluster",
        action="store_true",
        help="keep the exact disposable cluster for debugging",
    )
    args = parser.parse_args()
    require(
        args.cluster_name.startswith("epoch-alpha-exit-")
        and all(
            character.isalnum() or character == "-" for character in args.cluster_name
        )
        and len(args.cluster_name) <= 48,
        "cluster name must be a <=48-character epoch-alpha-exit-* identifier",
    )
    return args


def main() -> int:
    Campaign(parse_args()).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

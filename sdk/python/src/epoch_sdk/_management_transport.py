"""Validate the entire controller allowlist before constructing lazy connections."""

from __future__ import annotations

import ipaddress
import math
import re
import ssl
from collections.abc import Sequence
from pathlib import Path

import grpc

from ._management_types import ManagementConfig, ManagementRPCError
from .transport import _USER_AGENT, _tls_context

Metadata = tuple[tuple[str, str | bytes], ...]
_AUTHORITY = re.compile(r"(\[[0-9a-fA-F:.]+\]|[a-zA-Z0-9][a-zA-Z0-9.-]*):([1-9][0-9]{0,4})")
_METADATA_KEY = re.compile(r"[a-z0-9_.-]+")
_OPTIONS: tuple[tuple[str, int | str], ...] = (
    ("grpc.enable_retries", 0),
    ("grpc.enable_http_proxy", 0),
    ("grpc.service_config_disable_resolution", 1),
    ("grpc.max_send_message_length", 5 << 20),
    ("grpc.max_receive_message_length", 5 << 20),
    ("grpc.primary_user_agent", _USER_AGENT),
)


def connections(config: ManagementConfig) -> tuple[grpc.Channel, ...]:
    if not config.endpoints or (
        isinstance(config.timeout, bool) or not math.isfinite(config.timeout) or config.timeout <= 0
    ):
        raise ValueError("management requires endpoints and a finite positive timeout")
    token = config.bearer_token
    if not 1 <= len(token) <= 4096 or any(not 33 <= ord(value) <= 126 for value in token):
        raise ValueError("management bearer must be a bounded visible-ASCII credential")
    if (config.tls is None) == (not config.allow_insecure_loopback):
        raise ValueError("choose explicit TLS trust or explicit insecure loopback development")
    endpoints = tuple(config.endpoints)
    seen: set[str] = set()
    for endpoint in endpoints:
        match = _AUTHORITY.fullmatch(endpoint)
        if match is None or int(match[2]) > 65535 or endpoint.casefold() in seen:
            raise ValueError(
                "management endpoints must be distinct canonical host:port authorities"
            )
        seen.add(endpoint.casefold())
        host = match[1].strip("[]")
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            if ":" in host:
                raise ValueError("management IPv6 authority is invalid") from None
            address = None
        if (
            config.allow_insecure_loopback
            and host != "localhost"
            and (address is None or not address.is_loopback)
        ):
            raise ValueError("plaintext management endpoints must be loopback")
    trust: grpc.ChannelCredentials | None = None
    if config.tls is not None:
        try:
            # Validate CA/key matching before any connection. This SSLContext is
            # not the gRPC transport; Python gRPC has no public min-TLS selector.
            # Epoch controllers must retain their enforced TLS 1.3 server policy.
            _tls_context(config.tls)
            roots = Path(config.tls.root_ca).read_bytes()
            certificate = (
                None
                if config.tls.certificate is None
                else Path(config.tls.certificate).read_bytes()
            )
            private_key = (
                None
                if config.tls.private_key is None
                else Path(config.tls.private_key).read_bytes()
            )
            trust = grpc.ssl_channel_credentials(roots, private_key, certificate)
        except (OSError, ssl.SSLError, ValueError):
            raise ValueError("invalid management TLS trust or client identity") from None
    channels: list[grpc.Channel] = []
    try:
        for endpoint in endpoints:
            target = "dns:///" + endpoint
            channels.append(
                grpc.insecure_channel(target, options=_OPTIONS)
                if trust is None
                else grpc.secure_channel(target, trust, options=_OPTIONS)
            )
    except BaseException:
        for channel in channels:
            channel.close()
        raise
    return tuple(channels)


def authenticated_metadata(token: str, incoming: Sequence[tuple[str, str | bytes]]) -> Metadata:
    result: list[tuple[str, str | bytes]] = []
    for key, value in incoming:
        if key.lower() == "authorization":
            continue
        if (
            not _METADATA_KEY.fullmatch(key)
            or (key.endswith("-bin") and not isinstance(value, bytes))
            or (
                not key.endswith("-bin")
                and (
                    not isinstance(value, str) or any(not 32 <= ord(char) <= 126 for char in value)
                )
            )
        ):
            raise ManagementRPCError(grpc.StatusCode.INVALID_ARGUMENT)
        result.append((key, value))
    result.append(("authorization", "Bearer " + token))
    return tuple(result)

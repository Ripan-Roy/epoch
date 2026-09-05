"""Transport abstraction and standard-library HTTP implementation."""

from __future__ import annotations

import json
import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .errors import EpochAPIError


@dataclass(frozen=True)
class TLSConfig:
    """Explicit CA trust and optional client identity for an HTTPS endpoint."""

    root_ca: str | Path
    certificate: str | Path | None = None
    private_key: str | Path | None = None


class Transport(Protocol):
    """Minimal transport consumed by EpochClient and replaceable in tests."""

    def request(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        query: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        """Send one request and return its decoded response."""


class UrllibTransport:
    """Synchronous HTTP transport using only the Python standard library."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 10.0,
        tls: TLSConfig | None = None,
    ) -> None:
        normalized = base_url.rstrip("/")
        if not normalized.startswith(("http://", "https://")):
            raise ValueError("base_url must use http or https")
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        if tls is not None and not normalized.startswith("https://"):
            raise ValueError("a TLS-configured transport requires an https base_url")
        self._base_url = normalized
        self._timeout = timeout
        self._ssl_context = _tls_context(tls) if tls is not None else None

    def request(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        query: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        url = f"{self._base_url}/{path.lstrip('/')}"
        if query:
            filtered = {key: value for key, value in query.items() if value is not None}
            if filtered:
                url = f"{url}?{urlencode(filtered)}"
        data = None if body is None else json.dumps(body, separators=(",", ":")).encode()
        request_headers = {"accept": "application/json", "user-agent": "epoch-python/0.2.0b6"}
        if data is not None:
            request_headers["content-type"] = "application/json"
        for name, value in (headers or {}).items():
            if name.lower() in {"host", "content-length"}:
                raise ValueError(f"request header {name} is managed by the HTTP transport")
            if "\r" in name or "\n" in name or "\r" in value or "\n" in value:
                raise ValueError("request headers must fit one HTTP header line")
            request_headers[name] = value
        request = Request(url, data=data, headers=request_headers, method=method.upper())
        try:
            with urlopen(
                request,
                timeout=self._timeout,
                context=self._ssl_context,
            ) as response:
                payload = response.read()
                return None if not payload else json.loads(payload)
        except HTTPError as error:
            raw = error.read()
            decoded = _decode_error_body(raw)
            code, detail = _error_fields(decoded, error.reason)
            raise EpochAPIError(error.code, code, detail, decoded) from error
        except URLError as error:
            raise EpochAPIError(0, "transport_error", str(error.reason)) from error


def _tls_context(config: TLSConfig) -> ssl.SSLContext:
    certificate_set = config.certificate is not None
    key_set = config.private_key is not None
    if certificate_set != key_set:
        raise ValueError("TLS certificate and private key must be configured together")
    context = ssl.create_default_context(cafile=str(config.root_ca))
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    if certificate_set:
        context.load_cert_chain(str(config.certificate), str(config.private_key))
    return context


def _decode_error_body(raw: bytes) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return raw.decode(errors="replace")


def _error_fields(body: Any, fallback: Any) -> tuple[str, str]:
    error = body.get("error", body) if isinstance(body, dict) else None
    if isinstance(error, dict):
        code = str(error.get("code", "http_error"))
        detail = str(error.get("detail", error.get("message", fallback)))
        return code, detail
    return "http_error", str(fallback)

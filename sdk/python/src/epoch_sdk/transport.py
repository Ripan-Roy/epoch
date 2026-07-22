"""Transport abstraction and standard-library HTTP implementation."""

from __future__ import annotations

import json
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .errors import EpochAPIError


class Transport(Protocol):
    """Minimal transport consumed by EpochClient and replaceable in tests."""

    def request(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        query: dict[str, Any] | None = None,
    ) -> Any:
        """Send one request and return its decoded response."""


class UrllibTransport:
    """Synchronous HTTP transport using only the Python standard library."""

    def __init__(self, base_url: str, *, timeout: float = 10.0) -> None:
        normalized = base_url.rstrip("/")
        if not normalized.startswith(("http://", "https://")):
            raise ValueError("base_url must use http or https")
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        self._base_url = normalized
        self._timeout = timeout

    def request(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        query: dict[str, Any] | None = None,
    ) -> Any:
        url = f"{self._base_url}/{path.lstrip('/')}"
        if query:
            filtered = {key: value for key, value in query.items() if value is not None}
            if filtered:
                url = f"{url}?{urlencode(filtered)}"
        data = None if body is None else json.dumps(body, separators=(",", ":")).encode()
        headers = {"accept": "application/json", "user-agent": "epoch-python/0.1.0a1"}
        if data is not None:
            headers["content-type"] = "application/json"
        request = Request(url, data=data, headers=headers, method=method.upper())
        try:
            with urlopen(request, timeout=self._timeout) as response:
                payload = response.read()
                return None if not payload else json.loads(payload)
        except HTTPError as error:
            raw = error.read()
            decoded = _decode_error_body(raw)
            code, detail = _error_fields(decoded, error.reason)
            raise EpochAPIError(error.code, code, detail, decoded) from error
        except URLError as error:
            raise EpochAPIError(0, "transport_error", str(error.reason)) from error


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

"""Runtime response boundaries shared by HTTP and injected transports."""

from __future__ import annotations

from typing import Any

from .errors import EpochProtocolError


def object_response(value: object) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise EpochProtocolError("Epoch response must be an object with string keys")
    return value


def object_list_response(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise EpochProtocolError("Epoch response must be an array of objects")
    return [object_response(item) for item in value]


def integer_fields_response(value: object) -> dict[str, int]:
    result: dict[str, int] = {}
    for key, item in object_response(value).items():
        if not isinstance(item, int) or isinstance(item, bool):
            raise EpochProtocolError("Epoch response fields must be integers, not booleans")
        result[key] = item
    return result


def boolean_fields_response(value: object) -> dict[str, bool]:
    result: dict[str, bool] = {}
    for key, item in object_response(value).items():
        if not isinstance(item, bool):
            raise EpochProtocolError("Epoch response fields must be booleans")
        result[key] = item
    return result


def empty_response(value: object) -> None:
    if value is not None:
        raise EpochProtocolError("Epoch response must be empty")

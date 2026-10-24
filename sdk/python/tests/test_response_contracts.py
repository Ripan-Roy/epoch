"""Public SDK response contracts must hold for HTTP and custom transports."""

from __future__ import annotations

import unittest
from typing import Any
from unittest.mock import Mock, patch

from epoch_sdk import (
    EpochClient,
    EpochProtocolError,
    RegionalBusClient,
    RegionalCacheClient,
    RegionalQueueClient,
    RegionalScope,
    RegionalStreamClient,
    UrllibTransport,
)


class ReplyTransport:
    def __init__(self, reply: object) -> None:
        self.reply = reply
        self.operation_calls = 0

    def request(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        query: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> object:
        if path.endswith("/shards/0"):
            return {
                "resource_generation": "5",
                "tablet_epoch": "3",
                "term": "8",
                "accepts_writes": True,
            }
        self.operation_calls += 1
        return self.reply


class ResponseContractTests(unittest.TestCase):
    def test_protocol_error_is_a_value_error_not_an_automatic_api_retry(self) -> None:
        error = EpochProtocolError("Epoch response is not valid JSON")
        self.assertIsInstance(error, ValueError)
        self.assertFalse(hasattr(error, "retryable"))

    def test_standalone_object_results_reject_non_objects_without_payload_leaks(self) -> None:
        for reply in (None, [], "private-response", 42, True, {1: "private-response"}):
            with self.subTest(reply=reply):
                client = EpochClient(transport=ReplyTransport(reply))
                with self.assertRaisesRegex(ValueError, "Epoch response") as caught:
                    client.health()
                self.assertNotIn("private-response", str(caught.exception))

    def test_standalone_lists_require_every_item_to_be_an_object(self) -> None:
        for reply in ({}, None, [None], [1], [{"name": "valid"}, "invalid"]):
            with self.subTest(reply=reply), self.assertRaisesRegex(ValueError, "Epoch response"):
                EpochClient(transport=ReplyTransport(reply)).resources()
        self.assertEqual([], EpochClient(transport=ReplyTransport([])).resources())
        self.assertEqual(
            [{"name": "valid"}],
            EpochClient(transport=ReplyTransport([{"name": "valid"}])).resources(),
        )

    def test_integer_response_fields_do_not_accept_booleans_or_coerce_strings(self) -> None:
        for value in (True, "42", 1.5, None):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "Epoch response"):
                EpochClient(transport=ReplyTransport({"value": value})).cache_increment(
                    "sessions", "visits"
                )
        self.assertEqual(
            {"value": -42},
            EpochClient(transport=ReplyTransport({"value": -42})).cache_increment(
                "sessions", "visits"
            ),
        )

    def test_empty_response_methods_do_not_return_arbitrary_decoded_bodies(self) -> None:
        for reply in ({}, [], "private-response", 1):
            with self.subTest(reply=reply), self.assertRaisesRegex(ValueError, "Epoch response"):
                EpochClient(transport=ReplyTransport(reply)).cache_delete("sessions", "key")
        self.assertIsNone(
            EpochClient(transport=ReplyTransport(None)).cache_delete("sessions", "key")
        )

    def test_boolean_response_fields_do_not_accept_integers_or_coerce_strings(self) -> None:
        for value in (1, "true", 1.0, None):
            with self.subTest(value=value), self.assertRaisesRegex(EpochProtocolError, "booleans"):
                EpochClient(transport=ReplyTransport({"dead_lettered": value})).reject(
                    "jobs", "original-lease", reason="invalid"
                )
        for value in (False, True):
            self.assertEqual(
                {"dead_lettered": value},
                EpochClient(transport=ReplyTransport({"dead_lettered": value})).reject(
                    "jobs", "original-lease", reason="invalid"
                ),
            )

    def test_every_regional_profile_rejects_invalid_operation_bodies_without_retry(self) -> None:
        scope = RegionalScope("acme", "shop", "dev", "core")
        for profile, collection in (
            (RegionalStreamClient, "streams"),
            (RegionalQueueClient, "queues"),
            (RegionalCacheClient, "caches"),
            (RegionalBusClient, "buses"),
        ):
            for reply in (None, [], "private-response", {1: "private-response"}):
                with self.subTest(profile=profile.__name__, reply=reply):
                    transport = ReplyTransport(reply)
                    client = profile.with_transports([transport], token="credential", scope=scope)
                    with self.assertRaisesRegex(ValueError, "Epoch response") as caught:
                        client.call(
                            collection,
                            "resource",
                            "resource",
                            0,
                            lambda route: ("GET", "/status", None, None, {}),
                        )
                    self.assertEqual(1, transport.operation_calls)
                    self.assertNotIn("private-response", str(caught.exception))
                    self.assertNotIn("credential", str(caught.exception))

    def test_http_decode_rejects_nonstandard_or_invalid_json_without_payload_leaks(self) -> None:
        for raw in (
            b'{"secret":"private-response"',
            b"\xff",
            b'{"value":NaN}',
            b'{"nested":{"value":Infinity}}',
            b'{"nested":{"value":1e999}}',
            b'{"value":1,"value":2}',
            b'[{"value":1,"value":2}]',
        ):
            response = Mock()
            response.__enter__ = Mock(return_value=response)
            response.__exit__ = Mock(return_value=False)
            response.read.return_value = raw
            with self.subTest(raw=raw), patch("epoch_sdk.transport.urlopen", return_value=response):
                with self.assertRaisesRegex(ValueError, "Epoch response") as caught:
                    UrllibTransport("https://epoch.example").request("GET", "/healthz")
                self.assertNotIn("private-response", str(caught.exception))

    def test_valid_http_json_preserves_large_decimal_ids_unicode_and_numbers(self) -> None:
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.read.return_value = (
            '{"cursor":"18446744073709551615","label":"café","counter":-42,"score":2.5}'
        ).encode()
        with patch("epoch_sdk.transport.urlopen", return_value=response):
            value = UrllibTransport("https://epoch.example").request("GET", "/data")
        self.assertEqual(
            {"cursor": "18446744073709551615", "label": "café", "counter": -42, "score": 2.5},
            value,
        )


if __name__ == "__main__":
    unittest.main()

import unittest

from epoch_sdk import verify_webhook_signature


class WebhookSignatureTest(unittest.TestCase):
    def test_cross_language_vector_and_replay_identity(self) -> None:
        verification = verify_webhook_signature(
            b"0123456789abcdef0123456789abcdef",
            b'{"order_id":"one"}',
            "epoch.bus.delivery.v1.1.orders",
            "2",
            "1700000000",
            "v1=866b035f5c00f59cc64a7caea8a4d16be04dd41966774cdfc336e7cf341d18d9",
            now_seconds=1_700_000_010,
            tolerance_seconds=30,
        )
        self.assertEqual(verification.delivery_id, "epoch.bus.delivery.v1.1.orders")
        self.assertEqual(verification.attempt, 2)

    def test_changed_or_stale_request_fails_closed(self) -> None:
        arguments = (
            b"0123456789abcdef0123456789abcdef",
            b'{"order_id":"one"}',
            "epoch.bus.delivery.v1.1.orders",
            "2",
            "1700000000",
            "v1=866b035f5c00f59cc64a7caea8a4d16be04dd41966774cdfc336e7cf341d18d9",
        )
        with self.assertRaisesRegex(ValueError, "outside the allowed tolerance"):
            verify_webhook_signature(
                *arguments,
                now_seconds=1_700_000_031,
                tolerance_seconds=30,
            )
        with self.assertRaisesRegex(ValueError, "signature is invalid"):
            verify_webhook_signature(
                arguments[0],
                b'{"order_id":"changed"}',
                *arguments[2:],
                now_seconds=1_700_000_010,
                tolerance_seconds=30,
            )

    def test_noncanonical_replay_headers_fail_closed(self) -> None:
        arguments = (
            b"0123456789abcdef0123456789abcdef",
            b'{"order_id":"one"}',
            "epoch.bus.delivery.v1.1.orders",
            "v1=866b035f5c00f59cc64a7caea8a4d16be04dd41966774cdfc336e7cf341d18d9",
        )
        with self.assertRaisesRegex(ValueError, "canonical decimal"):
            verify_webhook_signature(
                arguments[0],
                arguments[1],
                arguments[2],
                "02",
                "1700000000",
                arguments[3],
                now_seconds=1_700_000_010,
                tolerance_seconds=30,
            )
        with self.assertRaisesRegex(ValueError, "canonical decimal"):
            verify_webhook_signature(
                arguments[0],
                arguments[1],
                arguments[2],
                "2",
                "01700000000",
                arguments[3],
                now_seconds=1_700_000_010,
                tolerance_seconds=30,
            )


if __name__ == "__main__":
    unittest.main()

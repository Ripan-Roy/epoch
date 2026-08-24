from __future__ import annotations

import ssl
import unittest
from unittest.mock import Mock, patch

from epoch_sdk import TLSConfig, UrllibTransport


class TransportSecurityTests(unittest.TestCase):
    @patch("epoch_sdk.transport.ssl.create_default_context")
    def test_explicit_ca_and_client_identity_build_tls_13_context(
        self, create_default_context: Mock
    ) -> None:
        context = Mock(spec=ssl.SSLContext)
        create_default_context.return_value = context

        UrllibTransport(
            "https://epoch.example",
            tls=TLSConfig(
                root_ca="ca.pem",
                certificate="client.pem",
                private_key="client.key",
            ),
        )

        create_default_context.assert_called_once_with(cafile="ca.pem")
        self.assertEqual(context.minimum_version, ssl.TLSVersion.TLSv1_3)
        context.load_cert_chain.assert_called_once_with("client.pem", "client.key")

    def test_tls_configuration_fails_closed_for_plaintext_or_partial_identity(self) -> None:
        with self.assertRaisesRegex(ValueError, "https"):
            UrllibTransport("http://127.0.0.1:7601", tls=TLSConfig(root_ca="ca.pem"))
        with self.assertRaisesRegex(ValueError, "configured together"):
            UrllibTransport(
                "https://epoch.example",
                tls=TLSConfig(root_ca="ca.pem", certificate="client.pem"),
            )


if __name__ == "__main__":
    unittest.main()

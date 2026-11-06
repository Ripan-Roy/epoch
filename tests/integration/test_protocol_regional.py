"""Fail-closed contracts for real-client regional compatibility evidence."""

from __future__ import annotations

import copy
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

import protocol_regional as campaign


class ProtocolRegionalContractTest(unittest.TestCase):
    def test_fault_plan_keeps_one_representative_per_tablet_profile(self) -> None:
        self.assertEqual(
            [resource.kind for resource in campaign.LEADER_FAILURE_RESOURCES],
            ["cache", "stream", "queue"],
        )

    def java_client_versions(self) -> dict[str, str]:
        manifest = Path(__file__).parents[1] / "compatibility/java/pom.xml"
        namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
        dependencies = {
            (
                dependency.findtext("m:groupId", namespaces=namespace),
                dependency.findtext("m:artifactId", namespaces=namespace),
            ): dependency.findtext("m:version", namespaces=namespace)
            for dependency in ET.parse(manifest).findall(
                "m:dependencies/m:dependency", namespace
            )
        }
        return {
            "kafka": dependencies[("org.apache.kafka", "kafka-clients")],
            "rabbitmq": dependencies[("com.rabbitmq", "amqp-client")],
        }

    def test_reported_java_client_versions_match_manifest(self) -> None:
        for client, version in self.java_client_versions().items():
            with self.subTest(client=client):
                self.assertEqual(
                    campaign.CLIENTS[client],
                    version,
                    "Client updates must refresh the evidence metadata and rerun "
                    "conformance; do not report results for the previous version.",
                )

    def test_exact_java_client_assertions_match_manifest(self) -> None:
        source = (
            Path(__file__).parents[1] / "compatibility/java/ProtocolConformance.java"
        ).read_text()
        version_calls = {
            "kafka": "AppInfoParser.getVersion()",
            "rabbitmq": "ConnectionFactory.class.getPackage().getImplementationVersion()",
        }
        for client, version in self.java_client_versions().items():
            with self.subTest(client=client):
                self.assertTrue(
                    f'{version_calls[client]}.equals("{version}")' in source,
                    f"Conformance must require the exact {client} version {version}.",
                )

    def test_completion_banners_match_manifest(self) -> None:
        versions = self.java_client_versions()
        clients = f"Kafka {versions['kafka']} and RabbitMQ Java {versions['rabbitmq']}"
        fixture = Path(__file__).parents[1]
        java = (fixture / "compatibility/java/ProtocolConformance.java").read_text()
        shell = (fixture / "integration/protocol-compatibility.sh").read_text()
        self.assertTrue(
            f'System.out.println("{clients} conformance passed");' in java,
            "The Java completion banner must name the exact Maven client versions.",
        )
        self.assertTrue(
            (
                f"printf 'Redis CLI {campaign.CLIENTS['redis']}, "
                f"Kafka Java {versions['kafka']}, and RabbitMQ Java {versions['rabbitmq']} "
                "conformance passed.\\n'"
            )
            in shell,
            "The fixture completion banner must name the exact current clients.",
        )

    def evidence(self) -> dict:
        return {
            "schema": campaign.RESULT_SCHEMA,
            "clients": campaign.CLIENTS.copy(),
            "faults": [
                {"kind": "gateway_sigkill"},
                *[
                    {
                        "kind": "leader_sigkill",
                        "profile": profile,
                        "old_leader": 1,
                        "new_leader": 2,
                        "old_term": 3,
                        "new_term": 4,
                    }
                    for profile in ("cache", "stream", "queue")
                ],
                {"kind": "all_voter_sigkill_reopen"},
            ],
            "checks": list(campaign.REQUIRED_CHECKS),
        }

    def test_accepts_complete_fault_and_client_evidence(self) -> None:
        campaign.validate_evidence(self.evidence())

    def test_rejects_previous_rabbitmq_client_evidence(self) -> None:
        evidence = self.evidence()
        evidence["clients"]["rabbitmq"] = "5.35.0"
        with self.assertRaisesRegex(ValueError, "released client versions"):
            campaign.validate_evidence(evidence)

    def test_process_failure_diagnostics_accept_text_bytes_and_missing_output(
        self,
    ) -> None:
        self.assertEqual(
            campaign.process_output("compose failed\n"), "compose failed\n"
        )
        self.assertEqual(campaign.process_output(b"invalid \xff\n"), "invalid �\n")
        self.assertEqual(campaign.process_output(None), "")

    def test_rejects_missing_fault_or_client_or_check(self) -> None:
        for field in ("faults", "clients", "checks"):
            evidence = self.evidence()
            if isinstance(evidence[field], dict):
                evidence[field].pop("kafka")
            else:
                evidence[field].pop()
            with self.subTest(field=field), self.assertRaises(ValueError):
                campaign.validate_evidence(evidence)

    def test_requires_new_leader_and_strictly_newer_term(self) -> None:
        for field, value in (("new_leader", 1), ("new_term", 3), ("new_term", True)):
            evidence = copy.deepcopy(self.evidence())
            evidence["faults"][1][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                campaign.validate_evidence(evidence)

    def test_rejects_duplicate_profile_instead_of_missing_profile(self) -> None:
        evidence = self.evidence()
        evidence["faults"][2]["profile"] = "cache"
        with self.assertRaises(ValueError):
            campaign.validate_evidence(evidence)


if __name__ == "__main__":
    unittest.main()

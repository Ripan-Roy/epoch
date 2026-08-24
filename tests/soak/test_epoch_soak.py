from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any


sys.path.insert(0, str(Path(__file__).resolve().parent))
import epoch_soak  # noqa: E402


class CampaignTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="epoch-soak-test-")
        self.root = Path(self.temporary.name)
        self.key = epoch_soak.generate_key(self.root / "signing-key.pem")
        self.identity: dict[str, object] = {
            "source": {
                "git_revision": "test-revision",
                "source_tree_sha256": "sha256:test-tree",
                "version": "0.1.0-test",
                "worktree_clean": True,
            },
            "runtime": {"image_id": "sha256:test-image"},
        }

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def result_document() -> dict[str, Any]:
        return {
            "schema": epoch_soak.REGIONAL_SCHEMA,
            "status": "passed",
            "profiles": list(epoch_soak.REQUIRED_PROFILES),
            "faults": list(epoch_soak.REQUIRED_FAULTS),
            "invariants": {name: True for name in epoch_soak.REQUIRED_INVARIANTS},
            "observations": {
                "catalog_digest": "test-digest",
                "physical_nodes": 3,
                "resources": 8,
                "tablets": 10,
            },
        }

    def passing_driver(self, round_dir: Path, round_number: int) -> Path:
        result = round_dir / "regional-result.json"
        (round_dir / "regional-runtime.log").write_text(
            f"round {round_number} passed\n", encoding="utf-8"
        )
        epoch_soak.atomic_write(
            result, epoch_soak.canonical_bytes(self.result_document())
        )
        return result

    def campaign(
        self,
        state_name: str,
        driver: epoch_soak.RoundDriver,
        plan: epoch_soak.CampaignPlan | None = None,
    ) -> epoch_soak.Campaign:
        return epoch_soak.Campaign(
            self.root / state_name,
            plan or epoch_soak.PLANS["accelerated"],
            self.key,
            identity=self.identity,
            environment={},
            driver=driver,
        )

    def test_accelerated_campaign_signs_and_verifies_exact_artifacts(self) -> None:
        campaign = self.campaign("complete", self.passing_driver)
        manifest = campaign.run()
        self.assertEqual(campaign.manifest_path, manifest)
        epoch_soak.verify_manifest(campaign.manifest_path, campaign.public_key_path)

        document = epoch_soak.load_json(campaign.manifest_path)
        self.assertEqual("passed", document["status"])
        self.assertEqual(1, document["summary"]["passed_rounds"])
        self.assertTrue(document["claim"]["accelerated_harness_only"])
        self.assertFalse(document["claim"]["managed_service_slo_claimed"])
        self.assertFalse(document["claim"]["throughput_or_latency_slo_claimed"])
        self.assertFalse(document["plan"]["load_shape"]["saturation_claimed"])

        def must_not_run(_round_dir: Path, _round_number: int) -> Path:
            raise AssertionError("completed campaign executed another round")

        resumed = self.campaign("complete", must_not_run)
        self.assertEqual(campaign.manifest_path, resumed.run())

    def test_failed_attempt_is_checkpointed_and_only_unfinished_round_retries(
        self,
    ) -> None:
        def failing_driver(round_dir: Path, _round_number: int) -> Path:
            (round_dir / "regional-runtime.log").write_text(
                "intentional failure\n", encoding="utf-8"
            )
            raise epoch_soak.EvidenceError("intentional round failure")

        first = self.campaign("resume", failing_driver)
        with self.assertRaises(epoch_soak.EvidenceError):
            first.run()
        failed_state = epoch_soak.load_json(first.state_path)
        self.assertEqual("failed", failed_state["attempts"][0]["status"])

        resumed = self.campaign("resume", self.passing_driver)
        resumed.run()
        evidence = epoch_soak.load_json(resumed.manifest_path)
        self.assertEqual(2, evidence["summary"]["attempts"])
        self.assertEqual(1, evidence["summary"]["failed_or_interrupted_attempts"])
        self.assertEqual(1, evidence["summary"]["passed_rounds"])
        self.assertEqual(1, evidence["attempts"][1]["round"])
        self.assertEqual(2, evidence["attempts"][1]["attempt"])

    def test_resume_rejects_source_or_runtime_identity_change(self) -> None:
        campaign = self.campaign("identity", self.passing_driver)
        state = campaign._load_state()  # noqa: SLF001 - intentional recovery fixture
        self.assertEqual(self.identity, state["identity"])
        changed = dict(self.identity)
        changed["runtime"] = {"image_id": "sha256:other-image"}
        resumed = epoch_soak.Campaign(
            campaign.state_dir,
            epoch_soak.PLANS["accelerated"],
            self.key,
            identity=changed,
            environment={},
            driver=self.passing_driver,
        )
        with self.assertRaisesRegex(epoch_soak.EvidenceError, "identity changed"):
            resumed.run()

    def test_artifact_tamper_invalidates_completed_evidence(self) -> None:
        campaign = self.campaign("tamper", self.passing_driver)
        campaign.run()
        state = epoch_soak.load_json(campaign.state_path)
        relative = state["attempts"][0]["artifacts"][0]["path"]
        artifact = campaign.state_dir / relative
        artifact.write_bytes(artifact.read_bytes() + b"tampered")
        with self.assertRaisesRegex(epoch_soak.EvidenceError, "receipt mismatch"):
            epoch_soak.verify_manifest(campaign.manifest_path, campaign.public_key_path)

    def test_round_budget_checkpoints_without_shortening_duration_target(self) -> None:
        duration_plan = epoch_soak.CampaignPlan(
            "duration-test", target_rounds=1, target_active_ms=60_000
        )
        campaign = self.campaign("duration", self.passing_driver, duration_plan)
        self.assertIsNone(campaign.run(round_budget=1))
        self.assertFalse(campaign.manifest_path.exists())
        state = epoch_soak.load_json(campaign.state_path)
        self.assertEqual("passed", state["attempts"][0]["status"])

    def test_private_key_cannot_be_collected_with_public_evidence(self) -> None:
        state_dir = self.root / "key-boundary"
        state_dir.mkdir()
        embedded_key = epoch_soak.generate_key(state_dir / "private-key.pem")
        with self.assertRaisesRegex(epoch_soak.EvidenceError, "outside evidence"):
            epoch_soak.Campaign(
                state_dir,
                epoch_soak.PLANS["accelerated"],
                embedded_key,
                identity=self.identity,
                environment={},
                driver=self.passing_driver,
            )


if __name__ == "__main__":
    unittest.main()

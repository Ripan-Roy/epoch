"""Contract tests for the live Kubernetes alpha-exit campaign."""

from __future__ import annotations

import argparse
import copy
import inspect
import tempfile
import unittest
from pathlib import Path

import kubernetes_alpha_exit as campaign


class KubernetesAlphaExitContractTest(unittest.TestCase):
    def test_node_image_exposes_a_numeric_non_root_runtime_identity(self) -> None:
        dockerfile = (campaign.REPO_ROOT / "deploy/docker/Dockerfile.node").read_text(
            encoding="utf-8"
        )

        self.assertIn("USER 10001:10001", dockerfile)
        self.assertNotIn("USER epoch:epoch", dockerfile)

    def test_managed_resource_request_preserves_governance_and_three_zone_intent(
        self,
    ) -> None:
        request = campaign.managed_resource_request("stream", "orders")

        self.assertEqual(request["expected_generation"], 0)
        self.assertEqual(
            request["resource"]["governance"],
            {
                "owner": "team:platform",
                "cost_center": "cc-1042",
                "classification": "confidential",
                "tags": {"profile": "stream", "service": "orders"},
            },
        )

    def test_event_bus_management_kind_maps_to_the_canonical_enum_spelling(
        self,
    ) -> None:
        request = campaign.managed_resource_request("event-bus", "events")

        self.assertEqual(request["resource"]["kind"], "event_bus")
        self.assertEqual(
            request["resource"]["governance"]["tags"]["profile"], "event_bus"
        )
        self.assertEqual(
            campaign.resource_path("event-bus", "events"),
            "/experimental/v1/regional/resources/acme/shop/dev/core/event-bus/events/shards/0",
        )
        self.assertEqual(
            campaign.management_canonical_name("event-bus", "events"),
            "acme/shop/dev/core/event_bus/events",
        )
        self.assertEqual(campaign.management_kind("event-bus"), "event_bus")
        self.assertEqual(campaign.route_kind("event_bus"), "event-bus")
        self.assertEqual(
            request["resource"]["spec"],
            {
                "shard_count": 1,
                "replica_count": 3,
                "placement": {
                    "allowed_regions": ["ap-south"],
                    "minimum_zones": 3,
                    "required_node_class": "general-purpose",
                },
            },
        )

    def test_profile_writes_cover_every_native_profile_with_stable_identity(
        self,
    ) -> None:
        stream_path, stream = campaign.profile_write("stream", "orders", "7", 2)
        cache_path, cache = campaign.profile_write("cache", "sessions", "7", 2)
        queue_path, queue = campaign.profile_write("queue", "jobs", "7", 2)
        bus_path, bus = campaign.profile_write("event-bus", "events", "7", 2)

        self.assertEqual(stream_path, "records")
        self.assertEqual(stream["expected_term"], "7")
        self.assertEqual(stream["partition"], 0)
        self.assertEqual(cache_path, "mutations")
        self.assertEqual(cache["operation"]["kind"], "set")
        self.assertEqual(queue_path, "mutations")
        self.assertEqual(queue["operation"]["kind"], "enqueue")
        self.assertEqual(bus_path, "mutations")
        self.assertEqual(bus["operation"]["kind"], "publish")
        self.assertEqual(
            {
                stream["idempotency_key"],
                cache["idempotency_key"],
                queue["idempotency_key"],
                bus["idempotency_key"],
            },
            {
                "kubernetes-stream-2",
                "kubernetes-cache-2",
                "kubernetes-queue-2",
                "kubernetes-event-bus-2",
            },
        )

    def test_restore_spec_uses_fresh_cluster_identity_and_exact_backup_object(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as evidence:
            runner = campaign.Campaign(
                argparse.Namespace(
                    cluster_name="epoch-alpha-exit-unit",
                    evidence_dir=Path(evidence),
                    skip_build=True,
                    keep_cluster=False,
                )
            )
            self.addCleanup(runner.secure_directory.cleanup)
            source = runner.epoch_cluster_document(campaign.SOURCE_CLUSTER)
            restored = runner.epoch_cluster_document(
                campaign.RESTORED_CLUSTER,
                restore_object="100-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.epoch-backup.enc",
            )

            self.assertEqual(source["spec"]["nodeImage"], campaign.NODE_IMAGE)
            self.assertNotIn("restore", source["spec"])
            self.assertEqual(restored["spec"]["nodeImage"], campaign.UPGRADE_NODE_IMAGE)
            self.assertEqual(
                restored["spec"]["restore"],
                {
                    "objectName": "100-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.epoch-backup.enc",
                    "encryptionSecret": "epoch-backup-key",
                },
            )
            self.assertEqual(restored["spec"]["replicas"], 4)
            self.assertEqual(restored["spec"]["catalogReplicas"], 3)
            self.assertEqual(
                restored["spec"]["transportSecurity"]["regionalServerName"],
                "epoch-restored-peer.epoch-system.svc",
            )

    def test_single_voter_replacement_uses_available_capacity_for_n_nodes(self) -> None:
        removed, added, target = campaign.plan_single_voter_replacement(
            ["2", "4", "7"], [str(node) for node in range(1, 9)]
        )

        self.assertEqual(removed, "7")
        self.assertEqual(added, "1")
        self.assertEqual(target, ["1", "2", "4"])
        self.assertEqual(len(set(target) & {"2", "4", "7"}), 2)

    def test_single_voter_replacement_requires_spare_physical_capacity(self) -> None:
        with self.assertRaisesRegex(campaign.CampaignError, "non-voting physical node"):
            campaign.plan_single_voter_replacement(["1", "2", "3"], ["1", "2", "3"])

    def test_first_observed_tablet_waits_through_incomplete_inventory(self) -> None:
        self.assertIsNone(campaign.first_observed_tablet({}))
        self.assertIsNone(campaign.first_observed_tablet({"tablets": []}))
        self.assertIsNone(campaign.first_observed_tablet({"tablets": "invalid"}))
        self.assertIsNone(campaign.first_observed_tablet({"tablets": [None]}))

        tablet = {"tablet_id": "tablet-1", "voter_node_ids": ["1", "2", "3"]}
        self.assertIs(campaign.first_observed_tablet({"tablets": [tablet]}), tablet)

    def test_control_and_catalog_generation_cursors_may_diverge_coherently(
        self,
    ) -> None:
        resource = {
            "generation": "2",
            "observed_generation": "2",
            "catalog_generation": "1",
            "tablets": [{"resource_generation": "1"}],
        }
        self.assertTrue(
            campaign.generation_cursors_match(
                resource, expected_control="2", expected_catalog="1"
            )
        )
        self.assertFalse(
            campaign.generation_cursors_match(
                {**resource, "tablets": [{"resource_generation": "2"}]}
            )
        )
        self.assertFalse(
            campaign.generation_cursors_match({**resource, "catalog_generation": "3"})
        )

    def test_live_repair_is_requested_through_managed_policy(self) -> None:
        source = inspect.getsource(campaign.Campaign.replace_stream_voter)

        self.assertIn('placement["excluded_node_ids"]', source)
        self.assertIn("automatic policy repair", source)
        self.assertNotIn("/membership", source)

    def test_wait_until_rejects_an_unmet_contract(self) -> None:
        with self.assertRaisesRegex(campaign.CampaignError, "timed out waiting"):
            campaign.wait_until(
                "impossible fixture", lambda: None, timeout=0.01, interval=0.001
            )

    def test_exact_int_accepts_internal_numbers_and_browser_safe_decimals(self) -> None:
        self.assertEqual(campaign.exact_int(7), 7)
        self.assertEqual(campaign.exact_int("7"), 7)
        self.assertIsNone(campaign.exact_int(True))
        self.assertIsNone(campaign.exact_int(-1))
        self.assertIsNone(campaign.exact_int("7.0"))

    def test_restorable_management_view_excludes_runtime_observations(self) -> None:
        inventory = {
            "resources": [
                {
                    "canonical_name": "acme/shop/dev/core/stream/orders",
                    "kind": "stream",
                    "generation": "2",
                    "observed_generation": "2",
                    "catalog_generation": "1",
                    "workload_profile": "stream_log",
                    "shard_count": 1,
                    "phase": "ready",
                    "message": "converged",
                    "governance": {"owner": "team:platform"},
                    "placement": {
                        "allowed_regions": ["ap-south"],
                        "minimum_zones": 3,
                        "minimum_racks": 3,
                        "required_node_class": "general-purpose",
                        "excluded_node_ids": [1],
                        "achieved_zones": 3,
                        "achieved_racks": 3,
                        "nodes": [{"node_id": "2", "available_consensus_groups": 5}],
                    },
                    "tablets": [
                        {
                            "tablet_id": "1",
                            "consensus_group_id": "2",
                            "shard_index": 0,
                            "tablet_epoch": "1",
                            "resource_generation": "1",
                            "desired_replicas": 3,
                            "assigned_node_ids": ["2", "3", "4"],
                            "bootstrap_voter_node_ids": ["1", "2", "3"],
                            "target_voter_node_ids": [],
                            "voter_node_ids": ["2", "3", "4"],
                            "reachable_voter_node_ids": ["2", "3", "4"],
                            "leader_node_id": "2",
                        }
                    ],
                }
            ]
        }
        restarted = copy.deepcopy(inventory)
        restarted_resource = restarted["resources"][0]
        restarted_resource["phase"] = "degraded"
        restarted_resource["message"] = "leader election in progress"
        restarted_resource["placement"]["achieved_zones"] = 2
        restarted_resource["placement"]["nodes"][0]["available_consensus_groups"] = 4
        restarted_resource["tablets"][0]["reachable_voter_node_ids"] = ["3", "4"]
        restarted_resource["tablets"][0]["leader_node_id"] = "3"

        self.assertEqual(
            campaign.restorable_management_view(inventory),
            campaign.restorable_management_view(restarted),
        )

        changed = copy.deepcopy(restarted)
        changed["resources"][0]["generation"] = "3"
        self.assertNotEqual(
            campaign.restorable_management_view(inventory),
            campaign.restorable_management_view(changed),
        )


if __name__ == "__main__":
    unittest.main()

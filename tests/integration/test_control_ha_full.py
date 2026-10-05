"""The complete bounded concurrent HA matrix cannot omit leader uncertainty."""

from __future__ import annotations

import copy
import unittest

import control_ha_full
from test_control_ha_api import api_evidence_fixture
from test_control_ha_faults import leader_fixture


def full_fixture() -> dict:
    evidence = api_evidence_fixture()
    evidence["schema"] = control_ha_full.FULL_SCHEMA
    for fleet in evidence["fleets"]:
        fleet["catalog_leader_unknown"] = leader_fixture(fleet["controller_count"])
        fleet["catalog_leader_unknown"]["lookups_bound_into_generated_client_proof"] = (
            True
        )
    return evidence


class FullEvidenceTest(unittest.TestCase):
    def test_lookup_plan_binds_original_tokens_and_exact_qualified_names(self) -> None:
        body = {
            "request_token": "original",
            "resource": {
                "organization": "acme",
                "project": "shop",
                "environment": "dev",
                "namespace": "core",
                "kind": "cache",
                "name": "writer-0",
            },
        }
        plan = {
            "schema": "epoch.control-ha.operation-bindings/v1",
            "requests": [
                {
                    "request_token": "original",
                    "affected_resources": [
                        {
                            **{
                                key: value
                                for key, value in body["resource"].items()
                                if key != "kind"
                            },
                            "kind": "RESOURCE_KIND_CACHE",
                        }
                    ],
                }
            ],
        }
        control_ha_full.validate_lookup_plan(plan, [body])
        for mutate in (
            lambda value: value.update(schema="wrong-schema"),
            lambda value: value["requests"][0].update(request_token="wrong"),
            lambda value: value["requests"][0]["affected_resources"][0].update(
                organization="otherco"
            ),
            lambda value: value["requests"].append(value["requests"][0]),
        ):
            broken = copy.deepcopy(plan)
            mutate(broken)
            with self.assertRaises(ValueError):
                control_ha_full.validate_lookup_plan(broken, [body])

        for bodies in (None, [], [None], [body, body], [{"request_token": "original"}]):
            with self.subTest(bodies=bodies), self.assertRaises(ValueError):
                control_ha_full.validate_lookup_plan(plan, bodies)

    def test_generated_proof_must_bind_every_lookup_without_dropped_proto_bytes(
        self,
    ) -> None:
        plan = {"requests": [{"request_token": "original"}]}
        proof = {
            "lookup_bindings": copy.deepcopy(plan["requests"]),
            "operations": [
                {"kind": "batch"},
                {
                    "kind": "lookup",
                    "grpc_code": 0,
                    "request_proto": "CgFh",
                    "operation_proto": "CgFi",
                },
            ],
        }
        control_ha_full.validate_lookup_proof(proof, plan)
        for mutate in (
            lambda value: value.pop("lookup_bindings"),
            lambda value: value["lookup_bindings"][0].update(request_token="wrong"),
            lambda value: value["operations"].pop(),
            lambda value: value["operations"][1].update(request_proto=""),
            lambda value: value["operations"][1].update(operation_proto="not-base64"),
            lambda value: value["operations"][1].update(grpc_code=True),
            lambda value: value["operations"].append(value["operations"][1]),
        ):
            broken = copy.deepcopy(proof)
            mutate(broken)
            with self.assertRaises(ValueError):
                control_ha_full.validate_lookup_proof(broken, plan)

    def test_both_api_and_real_leader_unknown_matrix_are_required(self) -> None:
        control_ha_full.validate_full_evidence(full_fixture())
        evidence = api_evidence_fixture()
        evidence["schema"] = control_ha_full.FULL_SCHEMA
        with self.assertRaises(ValueError):
            control_ha_full.validate_full_evidence(evidence)
        for value in (None, False, 1):
            evidence = full_fixture()
            evidence["fleets"][0]["catalog_leader_unknown"][
                "lookups_bound_into_generated_client_proof"
            ] = value
            with self.assertRaises(ValueError):
                control_ha_full.validate_full_evidence(evidence)

    def test_leader_case_must_match_the_actual_controller_fleet(self) -> None:
        evidence = full_fixture()
        evidence["fleets"][0]["catalog_leader_unknown"] = leader_fixture(5)
        with self.assertRaises(ValueError):
            control_ha_full.validate_full_evidence(evidence)


if __name__ == "__main__":
    unittest.main()

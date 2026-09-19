use epoch_catalog::{
    AcquireControlLease, ApplyDesiredResources, Catalog, CatalogCommand, CatalogError,
    CatalogMutation, CatalogRejectionCode, ControlLeaseGuard, DeleteDesiredResource,
    DeleteManagedResource, DesiredResourceWrite, ImportManagedResources, ManagedResourcePlacement,
    ManagedResourceRecord, NodeCapacityObservation, PlanManagedTabletMembership,
    ReconcileManagedResources, ResourceGeneration, ResourceName, ResourceSpec, TabletDescriptor,
    TabletPlacement, UpdateManagedResourceStatus,
};
use epoch_core::{ResourceKind, WorkloadProfile};
use serde_json::json;

fn name(value: &str) -> ResourceName {
    ResourceName::new(
        "acme",
        "payments",
        "production",
        "core",
        ResourceKind::Stream,
        value,
    )
    .unwrap()
}

fn desired(value: &str, expected_generation: Option<u64>) -> DesiredResourceWrite {
    DesiredResourceWrite {
        name: name(value),
        expected_generation,
        desired: json!({
            "workload_profile": "WORKLOAD_PROFILE_STREAM_LOG",
            "replicas": 3,
            "configuration": {"shard_count": 1},
            "governance": {
                "owner": "team-payments",
                "cost_center": "payments",
                "classification": "DATA_CLASSIFICATION_INTERNAL"
            }
        }),
    }
}

fn apply_desired(token: &str, resources: Vec<DesiredResourceWrite>) -> CatalogCommand {
    CatalogCommand::ApplyDesired(ApplyDesiredResources {
        request_token: token.into(),
        resources,
    })
}

fn lease(token: &str, owner: &str, now_ms: u64) -> CatalogCommand {
    CatalogCommand::AcquireControlLease(AcquireControlLease {
        request_token: token.into(),
        owner_id: owner.into(),
        now_ms,
        ttl_ms: 10_000,
    })
}

fn guard(owner: &str, fence: u64, now_ms: u64) -> ControlLeaseGuard {
    ControlLeaseGuard {
        owner_id: owner.into(),
        fence,
        now_ms,
    }
}

fn placement(value: &str, desired_generation: u64) -> ManagedResourcePlacement {
    ManagedResourcePlacement {
        name: name(value),
        expected_desired_generation: desired_generation,
        expected_catalog_generation: 0,
        spec: ResourceSpec {
            workload_profile: WorkloadProfile::StreamLog,
            shard_count: 1,
            replica_count: 3,
            configuration: None,
            governance: None,
        },
        tablet_placements: vec![TabletPlacement {
            shard_index: 0,
            voter_node_ids: vec![1, 2, 3],
        }],
    }
}

fn capacity(catalog_groups: u32) -> Vec<NodeCapacityObservation> {
    (1..=3)
        .map(|node_id| NodeCapacityObservation {
            node_id,
            max_consensus_groups: 3,
            used_consensus_groups: 1 + catalog_groups,
            catalog_groups,
        })
        .collect()
}

fn managed_membership_command(
    tablet: &TabletDescriptor,
    token: &str,
    lease: ControlLeaseGuard,
    node_four_limit: u32,
) -> CatalogCommand {
    CatalogCommand::PlanManagedMembership(PlanManagedTabletMembership {
        request_token: token.into(),
        lease,
        capacity: vec![
            NodeCapacityObservation {
                node_id: 1,
                max_consensus_groups: 3,
                used_consensus_groups: 2,
                catalog_groups: 1,
            },
            NodeCapacityObservation {
                node_id: 2,
                max_consensus_groups: 3,
                used_consensus_groups: 2,
                catalog_groups: 1,
            },
            NodeCapacityObservation {
                node_id: 3,
                max_consensus_groups: 3,
                used_consensus_groups: 2,
                catalog_groups: 1,
            },
            NodeCapacityObservation {
                node_id: 4,
                max_consensus_groups: node_four_limit,
                used_consensus_groups: 1,
                catalog_groups: 0,
            },
        ],
        name: name("orders"),
        expected_desired_generation: 1,
        tablet_id: tablet.tablet_id,
        expected_tablet_epoch: tablet.tablet_epoch,
        expected_resource_generation: tablet.resource_generation,
        target_voter_node_ids: vec![1, 2, 4],
    })
}

fn managed_target_voters(catalog: &Catalog) -> Vec<u64> {
    catalog.resource(&name("orders")).unwrap().tablets[0]
        .target_voter_node_ids
        .clone()
}

#[test]
fn desired_batch_is_atomic_idempotent_and_watch_resumable() {
    let mut catalog = Catalog::new();
    let command = apply_desired(
        "desired-batch-1",
        vec![desired("audit", Some(0)), desired("orders", Some(0))],
    );
    let first = catalog.apply(command.clone()).unwrap();
    let CatalogMutation::DesiredApplied {
        resources,
        changed,
        replayed,
    } = first
    else {
        panic!("expected desired apply");
    };
    assert!(changed);
    assert!(!replayed);
    assert_eq!(resources.len(), 2);
    assert_eq!(catalog.managed_resource_count(), 2);
    assert_eq!(catalog.latest_change_cursor(), 2);

    let changes = catalog.changes_after(0, 10).unwrap();
    assert_eq!(changes.earliest_cursor, 1);
    assert_eq!(changes.latest_cursor, 2);
    assert_eq!(changes.changes.len(), 2);
    assert_eq!(changes.changes[0].name, name("audit"));
    assert_eq!(changes.changes[1].name, name("orders"));
    assert!(matches!(
        catalog.changes_after(3, 10),
        Err(CatalogError::InvalidSpec(message)) if message.contains("future")
    ));

    assert!(matches!(
        catalog.apply(command).unwrap(),
        CatalogMutation::DesiredApplied { replayed: true, .. }
    ));
    let operation = catalog.operation("desired-batch-1").unwrap();
    assert_eq!(operation.first_change_cursor, 1);
    assert_eq!(operation.last_change_cursor, 2);
    assert_eq!(
        operation.resource_names,
        vec![name("audit"), name("orders")]
    );

    let mixed = catalog
        .apply(apply_desired(
            "desired-batch-mixed",
            vec![desired("audit", Some(1)), desired("invoices", Some(0))],
        ))
        .unwrap();
    let CatalogMutation::DesiredApplied { resources, .. } = mixed else {
        panic!("expected desired apply");
    };
    assert!(!resources[0].changed);
    assert!(resources[1].changed);
    let mixed_changes = catalog.changes_after(2, 10).unwrap();
    assert_eq!(mixed_changes.changes.len(), 1);
    assert_eq!(mixed_changes.changes[0].name, name("invoices"));
    let mixed_operation = catalog.operation("desired-batch-mixed").unwrap();
    assert_eq!(mixed_operation.first_change_cursor, 3);
    assert_eq!(mixed_operation.last_change_cursor, 3);
    assert_eq!(
        mixed_operation.resource_names,
        vec![name("audit"), name("invoices")]
    );

    let before_audit = catalog.managed_resource(&name("audit")).unwrap().clone();
    let before_orders = catalog.managed_resource(&name("orders")).unwrap().clone();
    let before_cursor = catalog.latest_change_cursor();
    let conflicting = apply_desired(
        "desired-batch-conflict",
        vec![desired("audit", Some(1)), desired("orders", Some(9))],
    );
    assert!(matches!(
        catalog.apply(conflicting).unwrap(),
        CatalogMutation::Rejected {
            code: CatalogRejectionCode::Conflict,
            ..
        }
    ));
    assert_eq!(
        catalog
            .operation("desired-batch-conflict")
            .unwrap()
            .resource_names,
        vec![name("audit"), name("orders")]
    );
    assert_eq!(
        catalog.managed_resource(&name("audit")).unwrap(),
        &before_audit
    );
    assert_eq!(
        catalog.managed_resource(&name("orders")).unwrap(),
        &before_orders
    );
    assert_eq!(catalog.latest_change_cursor(), before_cursor);
}

#[test]
fn desired_update_preserves_last_observed_status_for_reconciliation_cursor() {
    let mut catalog = Catalog::new();
    catalog
        .apply(apply_desired(
            "desired-orders-v1",
            vec![desired("orders", Some(0))],
        ))
        .unwrap();
    catalog
        .apply(lease("lease-control-a", "control-a", 1_000))
        .unwrap();
    let observed = json!({
        "phase": "ready",
        "observed_generation": "1",
        "catalog_generation": "7"
    });
    catalog
        .apply(CatalogCommand::UpdateManagedStatus(
            UpdateManagedResourceStatus {
                request_token: "status-orders-v1".into(),
                lease: guard("control-a", 1, 1_001),
                name: name("orders"),
                expected_generation: 1,
                status: observed.clone(),
            },
        ))
        .unwrap();

    let mut updated = desired("orders", Some(1));
    updated.desired["placement"] = json!({"excluded_node_ids": [1]});
    let mutation = catalog
        .apply(apply_desired("desired-orders-v2", vec![updated]))
        .unwrap();
    let CatalogMutation::DesiredApplied { resources, .. } = mutation else {
        panic!("expected desired update");
    };
    assert_eq!(resources[0].resource.generation, 2);
    assert_eq!(resources[0].resource.status, observed);
    assert_eq!(
        catalog.managed_resource(&name("orders")).unwrap().status,
        observed
    );
}

#[test]
fn leases_fence_stale_controllers_and_survive_snapshot_recovery() {
    let mut catalog = Catalog::new();
    catalog
        .apply(apply_desired(
            "desired-orders",
            vec![desired("orders", Some(0))],
        ))
        .unwrap();

    let first = catalog
        .apply(lease("lease-a-1", "control-a", 1_000))
        .unwrap();
    let CatalogMutation::ControlLeaseAcquired {
        lease: active_lease,
        ..
    } = first
    else {
        panic!("expected lease");
    };
    assert_eq!(active_lease.fence, 1);
    assert_eq!(active_lease.valid_until_ms, 11_000);

    assert!(matches!(
        catalog
            .apply(lease("lease-b-too-early", "control-b", 10_999))
            .unwrap(),
        CatalogMutation::Rejected {
            code: CatalogRejectionCode::Conflict,
            ..
        }
    ));
    let second = catalog
        .apply(lease("lease-b-after-expiry", "control-b", 11_000))
        .unwrap();
    let CatalogMutation::ControlLeaseAcquired {
        lease: active_lease,
        ..
    } = second
    else {
        panic!("expected replacement lease");
    };
    assert_eq!(active_lease.owner_id, "control-b");
    assert_eq!(active_lease.fence, 2);

    let stale = CatalogCommand::UpdateManagedStatus(UpdateManagedResourceStatus {
        request_token: "stale-status".into(),
        lease: guard("control-a", 1, 11_001),
        name: name("orders"),
        expected_generation: 1,
        status: json!({"phase": "ready", "observed_generation": "1"}),
    });
    assert!(matches!(
        catalog.apply(stale).unwrap(),
        CatalogMutation::Rejected {
            code: CatalogRejectionCode::Fenced,
            ..
        }
    ));

    catalog
        .apply(CatalogCommand::UpdateManagedStatus(
            UpdateManagedResourceStatus {
                request_token: "current-status".into(),
                lease: guard("control-b", 2, 11_001),
                name: name("orders"),
                expected_generation: 1,
                status: json!({"phase": "ready", "observed_generation": "1"}),
            },
        ))
        .unwrap();
    let encoded = catalog.encode_snapshot().unwrap();
    let restored = Catalog::decode_snapshot(&encoded).unwrap();
    assert_eq!(restored.control_lease(), catalog.control_lease());
    assert_eq!(
        restored.managed_resource(&name("orders")).unwrap().status,
        json!({"phase": "ready", "observed_generation": "1"})
    );
    assert_eq!(
        restored.state_digest().unwrap(),
        catalog.state_digest().unwrap()
    );
}

#[test]
fn periodic_control_lease_outcomes_have_bounded_retention() {
    let mut catalog = Catalog::new();
    for index in 0..32_u64 {
        catalog
            .apply(lease(
                &format!("lease-control-a-{index:02}"),
                "control-a",
                1_000 + index,
            ))
            .unwrap();
    }
    assert!(catalog.operation("lease-control-a-00").is_none());
    assert!(catalog.operation("lease-control-a-31").is_some());
    let encoded = catalog.encode_snapshot().unwrap();
    let restored = Catalog::decode_snapshot(&encoded).unwrap();
    assert!(restored.operation("lease-control-a-00").is_none());
    assert!(restored.operation("lease-control-a-31").is_some());
}

#[test]
fn reconciliation_reserves_batch_capacity_atomically_across_controllers() {
    let mut catalog = Catalog::new();
    catalog
        .apply(apply_desired(
            "desired-batch",
            vec![desired("audit", Some(0)), desired("orders", Some(0))],
        ))
        .unwrap();
    catalog.apply(lease("lease-a", "control-a", 1_000)).unwrap();

    let reconcile = CatalogCommand::ReconcileManaged(ReconcileManagedResources {
        request_token: "reconcile-batch".into(),
        lease: guard("control-a", 1, 1_001),
        capacity: capacity(0),
        resources: vec![placement("audit", 1), placement("orders", 1)],
    });
    let mutation = catalog.apply(reconcile).unwrap();
    let CatalogMutation::ManagedReconciled { resources, .. } = mutation else {
        panic!("expected managed reconciliation");
    };
    assert_eq!(resources.len(), 2);
    assert_eq!(catalog.resource_count(), 2);

    catalog
        .apply(apply_desired(
            "desired-invoices",
            vec![desired("invoices", Some(0))],
        ))
        .unwrap();
    let before_resource_count = catalog.resource_count();
    let before_tablet_count = catalog.tablet_count();
    let before_cursor = catalog.latest_change_cursor();
    let stale_capacity = CatalogCommand::ReconcileManaged(ReconcileManagedResources {
        request_token: "reconcile-stale-capacity".into(),
        lease: guard("control-a", 1, 1_002),
        capacity: capacity(0),
        resources: vec![placement("invoices", 1)],
    });
    assert!(matches!(
        catalog.apply(stale_capacity).unwrap(),
        CatalogMutation::Rejected {
            code: CatalogRejectionCode::Conflict,
            ..
        }
    ));
    assert_eq!(catalog.resource_count(), before_resource_count);
    assert_eq!(catalog.tablet_count(), before_tablet_count);
    assert_eq!(catalog.latest_change_cursor(), before_cursor);
    assert!(catalog.resource(&name("invoices")).is_err());

    let saturated = CatalogCommand::ReconcileManaged(ReconcileManagedResources {
        request_token: "reconcile-saturated".into(),
        lease: guard("control-a", 1, 1_003),
        capacity: capacity(2),
        resources: vec![placement("invoices", 1)],
    });
    assert!(matches!(
        catalog.apply(saturated).unwrap(),
        CatalogMutation::Rejected {
            code: CatalogRejectionCode::CapacityExceeded,
            ..
        }
    ));
    assert_eq!(catalog.resource_count(), before_resource_count);
    assert_eq!(catalog.tablet_count(), before_tablet_count);
    assert_eq!(catalog.latest_change_cursor(), before_cursor);
    assert!(catalog.resource(&name("invoices")).is_err());
}

#[test]
fn managed_membership_plan_is_lease_fenced_and_reserves_transition_capacity() {
    let mut catalog = Catalog::new();
    catalog
        .apply(apply_desired(
            "desired-orders",
            vec![desired("orders", Some(0))],
        ))
        .unwrap();
    catalog.apply(lease("lease-a", "control-a", 1_000)).unwrap();
    catalog
        .apply(CatalogCommand::ReconcileManaged(
            ReconcileManagedResources {
                request_token: "reconcile-orders".into(),
                lease: guard("control-a", 1, 1_001),
                capacity: vec![
                    NodeCapacityObservation {
                        node_id: 1,
                        max_consensus_groups: 3,
                        used_consensus_groups: 1,
                        catalog_groups: 0,
                    },
                    NodeCapacityObservation {
                        node_id: 2,
                        max_consensus_groups: 3,
                        used_consensus_groups: 1,
                        catalog_groups: 0,
                    },
                    NodeCapacityObservation {
                        node_id: 3,
                        max_consensus_groups: 3,
                        used_consensus_groups: 1,
                        catalog_groups: 0,
                    },
                    NodeCapacityObservation {
                        node_id: 4,
                        max_consensus_groups: 2,
                        used_consensus_groups: 1,
                        catalog_groups: 0,
                    },
                ],
                resources: vec![placement("orders", 1)],
            },
        ))
        .unwrap();
    let tablet = catalog.resource(&name("orders")).unwrap().tablets[0].clone();
    assert!(matches!(
        catalog
            .apply(managed_membership_command(
                &tablet,
                "membership-stale-owner",
                guard("control-b", 1, 1_002),
                2,
            ))
            .unwrap(),
        CatalogMutation::Rejected {
            code: CatalogRejectionCode::Fenced,
            ..
        }
    ));
    assert!(managed_target_voters(&catalog).is_empty());

    assert!(matches!(
        catalog
            .apply(managed_membership_command(
                &tablet,
                "membership-capacity-rejected",
                guard("control-a", 1, 1_002),
                1,
            ))
            .unwrap(),
        CatalogMutation::Rejected {
            code: CatalogRejectionCode::CapacityExceeded,
            ..
        }
    ));
    assert!(managed_target_voters(&catalog).is_empty());

    let accepted = managed_membership_command(
        &tablet,
        "membership-accepted",
        guard("control-a", 1, 1_002),
        2,
    );
    assert!(matches!(
        catalog.apply(accepted.clone()).unwrap(),
        CatalogMutation::Applied { changed: true, .. }
    ));
    assert_eq!(managed_target_voters(&catalog), vec![1, 2, 4]);
    assert!(matches!(
        catalog.apply(accepted).unwrap(),
        CatalogMutation::Applied { replayed: true, .. }
    ));
    let encoded = catalog.encode_snapshot().unwrap();
    let restored = Catalog::decode_snapshot(&encoded).unwrap();
    assert_eq!(
        restored.state_digest().unwrap(),
        catalog.state_digest().unwrap()
    );
}

#[test]
fn legacy_import_preserves_live_and_tombstoned_generations_atomically() {
    let imported = CatalogCommand::ImportManaged(ImportManagedResources {
        request_token: "legacy-import-v1".into(),
        resources: vec![ManagedResourceRecord {
            name: name("orders"),
            generation: 7,
            desired: desired("orders", None).desired,
            status: json!({"phase": "ready", "observed_generation": 7}),
            deletion_requested: false,
        }],
        generations: vec![
            ResourceGeneration {
                name: name("audit"),
                generation: 4,
            },
            ResourceGeneration {
                name: name("orders"),
                generation: 7,
            },
        ],
    });
    let mut catalog = Catalog::new();
    assert!(matches!(
        catalog.apply(imported.clone()).unwrap(),
        CatalogMutation::DesiredApplied { changed: true, .. }
    ));
    let orders = catalog.managed_resource(&name("orders")).unwrap();
    assert_eq!(orders.generation, 7);
    assert_eq!(orders.status["phase"], "ready");
    assert!(matches!(
        catalog
            .apply(CatalogCommand::DeleteDesired(DeleteDesiredResource {
                request_token: "delete-missing-audit".into(),
                expected_generation: Some(0),
                name: name("audit"),
            }))
            .unwrap(),
        CatalogMutation::DesiredDeleted {
            generation: 4,
            deleted: false,
            ..
        }
    ));
    assert!(matches!(
        catalog.apply(imported).unwrap(),
        CatalogMutation::DesiredApplied { replayed: true, .. }
    ));

    let conflicting = CatalogCommand::ImportManaged(ImportManagedResources {
        request_token: "another-legacy-import".into(),
        resources: Vec::new(),
        generations: vec![ResourceGeneration {
            name: name("invoices"),
            generation: 1,
        }],
    });
    assert!(matches!(
        catalog.apply(conflicting).unwrap(),
        CatalogMutation::Rejected {
            code: CatalogRejectionCode::Conflict,
            ..
        }
    ));
    let encoded = catalog.encode_snapshot().unwrap();
    let restored = Catalog::decode_snapshot(&encoded).unwrap();
    assert_eq!(
        restored.state_digest().unwrap(),
        catalog.state_digest().unwrap()
    );
}

#[test]
fn legacy_import_accepts_histories_larger_than_a_managed_write_batch() {
    let generations = (0..129)
        .map(|index| ResourceGeneration {
            name: name(&format!("history-{index:03}")),
            generation: u64::try_from(index + 1).unwrap(),
        })
        .collect();
    let mut catalog = Catalog::new();
    let mutation = catalog
        .apply(CatalogCommand::ImportManaged(ImportManagedResources {
            request_token: "legacy-import-large-history".into(),
            resources: Vec::new(),
            generations,
        }))
        .unwrap();
    assert!(matches!(
        mutation,
        CatalogMutation::DesiredApplied { changed: false, .. }
    ));
    assert_eq!(
        catalog
            .apply(CatalogCommand::DeleteDesired(DeleteDesiredResource {
                request_token: "delete-last-imported-generation".into(),
                expected_generation: Some(0),
                name: name("history-128"),
            }))
            .unwrap(),
        CatalogMutation::DesiredDeleted {
            name: name("history-128"),
            generation: 129,
            deleted: false,
            replayed: false,
        }
    );
}

#[test]
fn managed_delete_removes_desired_and_native_state_under_one_lease_fence() {
    let mut catalog = Catalog::new();
    catalog
        .apply(apply_desired(
            "desired-orders",
            vec![desired("orders", Some(0))],
        ))
        .unwrap();
    catalog.apply(lease("lease-a", "control-a", 1_000)).unwrap();
    catalog
        .apply(CatalogCommand::ReconcileManaged(
            ReconcileManagedResources {
                request_token: "reconcile-orders".into(),
                lease: guard("control-a", 1, 1_001),
                capacity: capacity(0),
                resources: vec![placement("orders", 1)],
            },
        ))
        .unwrap();
    let managed_delete = |token: &str, lease: ControlLeaseGuard| {
        CatalogCommand::DeleteManaged(DeleteManagedResource {
            request_token: token.into(),
            lease,
            name: name("orders"),
            expected_desired_generation: 1,
            expected_catalog_generation: 1,
        })
    };
    assert!(matches!(
        catalog
            .apply(managed_delete(
                "delete-stale-owner",
                guard("control-b", 1, 1_002),
            ))
            .unwrap(),
        CatalogMutation::Rejected {
            code: CatalogRejectionCode::Fenced,
            ..
        }
    ));
    assert!(catalog.managed_resource(&name("orders")).is_ok());
    assert!(catalog.resource(&name("orders")).is_ok());

    let accepted = managed_delete("delete-orders", guard("control-a", 1, 1_002));
    assert!(matches!(
        catalog.apply(accepted.clone()).unwrap(),
        CatalogMutation::ManagedDeleted {
            desired_generation: 2,
            catalog_generation: 2,
            deleted: true,
            ..
        }
    ));
    assert!(catalog.managed_resource(&name("orders")).is_err());
    assert!(catalog.resource(&name("orders")).is_err());
    assert_eq!(catalog.latest_change_cursor(), 2);
    assert!(matches!(
        catalog.apply(accepted).unwrap(),
        CatalogMutation::ManagedDeleted { replayed: true, .. }
    ));
    let encoded = catalog.encode_snapshot().unwrap();
    let restored = Catalog::decode_snapshot(&encoded).unwrap();
    assert_eq!(
        restored.state_digest().unwrap(),
        catalog.state_digest().unwrap()
    );
}

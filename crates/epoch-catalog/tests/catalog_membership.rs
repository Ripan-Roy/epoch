use epoch_catalog::{
    ApplyResource, CATALOG_MEMBERSHIP_COMMAND_FORMAT_VERSION,
    CATALOG_MEMBERSHIP_SNAPSHOT_FORMAT_VERSION, Catalog, CatalogCommand, CatalogMutation,
    FinalizeTabletMembership, PlanTabletMembership, ResourceName, ResourceSpec, TabletPlacement,
};
use epoch_core::{ResourceKind, WorkloadProfile};

fn name() -> ResourceName {
    ResourceName::new(
        "acme",
        "payments",
        "production",
        "core",
        ResourceKind::Stream,
        "orders",
    )
    .unwrap()
}

fn create_command() -> CatalogCommand {
    CatalogCommand::Apply(ApplyResource {
        request_token: "orders-create-v1".into(),
        expected_generation: None,
        name: name(),
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
    })
}

#[test]
fn learner_first_plan_and_finalize_preserve_bootstrap_identity_across_snapshot() {
    let mut catalog = Catalog::new();
    let created = catalog.apply(create_command()).unwrap();
    let tablet = created.resource().unwrap().tablets[0].clone();
    assert!(tablet.bootstrap_voter_node_ids.is_empty());

    let plan = CatalogCommand::PlanMembership(PlanTabletMembership {
        request_token: "orders-tablet-1-replace-3-with-4".into(),
        tablet_id: tablet.tablet_id,
        expected_tablet_epoch: tablet.tablet_epoch,
        expected_resource_generation: 1,
        target_voter_node_ids: vec![1, 2, 4],
    });
    let encoded_plan = plan.encode().unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&encoded_plan).unwrap()["format_version"],
        CATALOG_MEMBERSHIP_COMMAND_FORMAT_VERSION
    );
    assert_eq!(CatalogCommand::decode(&encoded_plan).unwrap(), plan);
    let planned = catalog.apply(plan.clone()).unwrap();
    let planned_resource = planned.resource().unwrap();
    assert_eq!(planned_resource.generation, 1);
    assert_eq!(planned_resource.tablets[0].voter_node_ids, vec![1, 2, 3]);
    assert_eq!(
        planned_resource.tablets[0].bootstrap_voter_node_ids,
        vec![1, 2, 3]
    );
    assert_eq!(
        planned_resource.tablets[0].target_voter_node_ids,
        vec![1, 2, 4]
    );
    let planned_digest = catalog.state_digest().unwrap();
    for rejected in [
        CatalogCommand::PlanMembership(PlanTabletMembership {
            request_token: "orders-stale-generation".into(),
            tablet_id: tablet.tablet_id,
            expected_tablet_epoch: tablet.tablet_epoch,
            expected_resource_generation: 0,
            target_voter_node_ids: vec![1, 2, 4],
        }),
        CatalogCommand::PlanMembership(PlanTabletMembership {
            request_token: "orders-stale-tablet-epoch".into(),
            tablet_id: tablet.tablet_id,
            expected_tablet_epoch: tablet.tablet_epoch + 1,
            expected_resource_generation: 1,
            target_voter_node_ids: vec![1, 2, 4],
        }),
        CatalogCommand::PlanMembership(PlanTabletMembership {
            request_token: "orders-conflicting-replacement".into(),
            tablet_id: tablet.tablet_id,
            expected_tablet_epoch: tablet.tablet_epoch,
            expected_resource_generation: 1,
            target_voter_node_ids: vec![1, 3, 4],
        }),
    ] {
        assert!(catalog.apply(rejected).is_err());
        assert_eq!(catalog.state_digest().unwrap(), planned_digest);
    }
    let planned_snapshot = catalog.encode_snapshot().unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&planned_snapshot).unwrap()["format_version"],
        CATALOG_MEMBERSHIP_SNAPSHOT_FORMAT_VERSION
    );
    let reopened = Catalog::decode_snapshot(&planned_snapshot).unwrap();
    assert_eq!(reopened.resource(&name()).unwrap(), planned_resource);

    let finalize = CatalogCommand::FinalizeMembership(FinalizeTabletMembership {
        request_token: "orders-tablet-1-finalize-4".into(),
        tablet_id: tablet.tablet_id,
        expected_tablet_epoch: tablet.tablet_epoch,
        expected_resource_generation: 1,
        target_voter_node_ids: vec![1, 2, 4],
    });
    let encoded_finalize = finalize.encode().unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&encoded_finalize).unwrap()["format_version"],
        CATALOG_MEMBERSHIP_COMMAND_FORMAT_VERSION
    );
    assert_eq!(CatalogCommand::decode(&encoded_finalize).unwrap(), finalize);
    let finalized = catalog.apply(finalize.clone()).unwrap();
    let finalized_resource = finalized.resource().unwrap();
    assert_eq!(finalized_resource.generation, 1);
    assert_eq!(finalized_resource.tablets[0].voter_node_ids, vec![1, 2, 4]);
    assert_eq!(
        finalized_resource.tablets[0].bootstrap_voter_node_ids,
        vec![1, 2, 3]
    );
    assert!(
        finalized_resource.tablets[0]
            .target_voter_node_ids
            .is_empty()
    );

    let replay = catalog.apply(finalize).unwrap();
    assert!(matches!(
        replay,
        CatalogMutation::Applied { replayed: true, .. }
    ));
    let reopened = Catalog::decode_snapshot(&catalog.encode_snapshot().unwrap()).unwrap();
    assert_eq!(reopened.resource(&name()).unwrap(), finalized_resource);
}

#[test]
fn direct_or_multi_voter_placement_changes_fail_without_partial_catalog_mutation() {
    let mut catalog = Catalog::new();
    let created = catalog.apply(create_command()).unwrap();
    let tablet = created.resource().unwrap().tablets[0].clone();
    let before = catalog.state_digest().unwrap();

    let mut direct = create_command();
    let CatalogCommand::Apply(request) = &mut direct else {
        unreachable!();
    };
    request.request_token = "unsafe-direct-placement-v2".into();
    request.expected_generation = Some(1);
    request.tablet_placements[0].voter_node_ids = vec![1, 2, 4];
    assert!(catalog.apply(direct).is_err());

    let unsafe_plan = CatalogCommand::PlanMembership(PlanTabletMembership {
        request_token: "unsafe-two-voter-replacement".into(),
        tablet_id: tablet.tablet_id,
        expected_tablet_epoch: tablet.tablet_epoch,
        expected_resource_generation: 1,
        target_voter_node_ids: vec![2, 4, 5],
    });
    assert!(catalog.apply(unsafe_plan).is_err());

    let premature = CatalogCommand::FinalizeMembership(FinalizeTabletMembership {
        request_token: "premature-finalize".into(),
        tablet_id: tablet.tablet_id,
        expected_tablet_epoch: tablet.tablet_epoch,
        expected_resource_generation: 1,
        target_voter_node_ids: vec![1, 2, 4],
    });
    assert!(catalog.apply(premature).is_err());
    assert_eq!(catalog.state_digest().unwrap(), before);
    assert_eq!(catalog.resource(&name()).unwrap().generation, 1);
}

#[test]
fn resource_update_during_membership_transition_preserves_and_refences_the_plan() {
    let mut catalog = Catalog::new();
    let created = catalog.apply(create_command()).unwrap();
    let tablet = created.resource().unwrap().tablets[0].clone();
    catalog
        .apply(CatalogCommand::PlanMembership(PlanTabletMembership {
            request_token: "orders-tablet-1-plan-before-expand".into(),
            tablet_id: tablet.tablet_id,
            expected_tablet_epoch: tablet.tablet_epoch,
            expected_resource_generation: 1,
            target_voter_node_ids: vec![1, 2, 4],
        }))
        .unwrap();

    let mut expand = create_command();
    let CatalogCommand::Apply(request) = &mut expand else {
        unreachable!();
    };
    request.request_token = "orders-expand-during-membership-v2".into();
    request.expected_generation = Some(1);
    request.spec.shard_count = 2;
    request.tablet_placements.push(TabletPlacement {
        shard_index: 1,
        voter_node_ids: vec![2, 3, 4],
    });
    let expanded = catalog.apply(expand).unwrap();
    let expanded = expanded.resource().unwrap();
    assert_eq!(expanded.generation, 2);
    assert_eq!(expanded.tablets.len(), 2);
    assert_eq!(expanded.tablets[0].resource_generation, 2);
    assert_eq!(expanded.tablets[0].voter_node_ids, vec![1, 2, 3]);
    assert_eq!(expanded.tablets[0].bootstrap_voter_node_ids, vec![1, 2, 3]);
    assert_eq!(expanded.tablets[0].target_voter_node_ids, vec![1, 2, 4]);

    let stale_finalize = CatalogCommand::FinalizeMembership(FinalizeTabletMembership {
        request_token: "orders-tablet-1-stale-finalize".into(),
        tablet_id: tablet.tablet_id,
        expected_tablet_epoch: tablet.tablet_epoch,
        expected_resource_generation: 1,
        target_voter_node_ids: vec![1, 2, 4],
    });
    let before_stale = catalog.state_digest().unwrap();
    assert!(catalog.apply(stale_finalize).is_err());
    assert_eq!(catalog.state_digest().unwrap(), before_stale);

    let finalized = catalog
        .apply(CatalogCommand::FinalizeMembership(
            FinalizeTabletMembership {
                request_token: "orders-tablet-1-finalize-after-expand".into(),
                tablet_id: tablet.tablet_id,
                expected_tablet_epoch: tablet.tablet_epoch,
                expected_resource_generation: 2,
                target_voter_node_ids: vec![1, 2, 4],
            },
        ))
        .unwrap();
    let finalized = finalized.resource().unwrap();
    assert_eq!(finalized.generation, 2);
    assert_eq!(finalized.tablets[0].resource_generation, 2);
    assert_eq!(finalized.tablets[0].voter_node_ids, vec![1, 2, 4]);
    assert!(finalized.tablets[0].target_voter_node_ids.is_empty());
    assert_eq!(finalized.tablets[1].voter_node_ids, vec![2, 3, 4]);
}

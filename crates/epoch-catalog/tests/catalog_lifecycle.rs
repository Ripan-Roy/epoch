use epoch_catalog::{
    ApplyResource, Catalog, CatalogCommand, CatalogError, CatalogMutation, DeleteResource,
    ResourceName, ResourceSpec,
};
use epoch_core::{ResourceKind, WorkloadProfile};

fn stream(name: &str, shards: u32) -> CatalogCommand {
    CatalogCommand::Apply(ApplyResource {
        request_token: format!("apply-{name}-{shards}"),
        expected_generation: None,
        name: ResourceName::new(
            "acme",
            "payments",
            "production",
            "core",
            ResourceKind::Stream,
            name,
        )
        .unwrap(),
        spec: ResourceSpec {
            workload_profile: WorkloadProfile::StreamLog,
            shard_count: shards,
            replica_count: 3,
            configuration: None,
        },
    })
}

fn applied(mutation: CatalogMutation) -> epoch_catalog::ResourceRecord {
    let CatalogMutation::Applied { resource, .. } = mutation else {
        panic!("expected an applied resource mutation");
    };
    resource
}

#[test]
fn creates_multiple_resources_and_routes_every_shard_without_identity_collisions() {
    let mut catalog = Catalog::new();
    let orders = applied(catalog.apply(stream("orders", 3)).unwrap());
    let audit = applied(catalog.apply(stream("audit", 2)).unwrap());

    assert_eq!(catalog.resource_count(), 2);
    assert_eq!(catalog.tablet_count(), 5);
    assert_eq!(orders.generation, 1);
    assert_eq!(audit.generation, 1);

    let mut tablet_ids = orders
        .tablets
        .iter()
        .chain(&audit.tablets)
        .map(|tablet| tablet.tablet_id)
        .collect::<Vec<_>>();
    tablet_ids.sort_unstable();
    tablet_ids.dedup();
    assert_eq!(tablet_ids.len(), 5);

    for resource in [&orders, &audit] {
        for tablet in &resource.tablets {
            assert_eq!(
                catalog.route(&resource.name, tablet.shard_index).unwrap(),
                tablet
            );
            assert_eq!(
                catalog.tablet(tablet.tablet_id).unwrap().resource,
                resource.name
            );
        }
    }
}

#[test]
fn shard_expansion_preserves_existing_identity_and_allocates_fresh_tablets() {
    let mut catalog = Catalog::new();
    let created = applied(catalog.apply(stream("orders", 2)).unwrap());
    let original_ids = created
        .tablets
        .iter()
        .map(|tablet| tablet.tablet_id)
        .collect::<Vec<_>>();

    let mut expansion = stream("orders", 4);
    let CatalogCommand::Apply(request) = &mut expansion else {
        unreachable!();
    };
    request.request_token = "expand-orders".into();
    request.expected_generation = Some(1);
    let expanded = applied(catalog.apply(expansion).unwrap());

    assert_eq!(expanded.generation, 2);
    assert_eq!(expanded.tablets.len(), 4);
    assert_eq!(
        expanded.tablets[..2]
            .iter()
            .map(|tablet| tablet.tablet_id)
            .collect::<Vec<_>>(),
        original_ids
    );
    assert!(expanded.tablets[2].tablet_id > original_ids[1]);
    assert!(expanded.tablets[3].tablet_id > expanded.tablets[2].tablet_id);
    assert!(
        expanded
            .tablets
            .iter()
            .all(|tablet| tablet.resource_generation == 2)
    );
}

#[test]
fn deletion_and_recreation_fence_generation_and_never_reuse_tablet_identity() {
    let mut catalog = Catalog::new();
    let created = applied(catalog.apply(stream("orders", 1)).unwrap());
    let original_tablet_id = created.tablets[0].tablet_id;
    let name = created.name.clone();

    let deleted = catalog
        .apply(CatalogCommand::Delete(DeleteResource {
            request_token: "delete-orders".into(),
            expected_generation: Some(1),
            name: name.clone(),
        }))
        .unwrap();
    assert_eq!(
        deleted,
        CatalogMutation::Deleted {
            name: name.clone(),
            generation: 2,
            deleted: true,
            replayed: false,
        }
    );
    assert!(catalog.route(&name, 0).is_err());
    assert!(catalog.tablet(original_tablet_id).is_err());

    let mut recreate = stream("orders", 1);
    let CatalogCommand::Apply(request) = &mut recreate else {
        unreachable!();
    };
    request.request_token = "recreate-orders".into();
    request.expected_generation = Some(0);
    let recreated = applied(catalog.apply(recreate).unwrap());
    assert_eq!(recreated.generation, 3);
    assert!(recreated.tablets[0].tablet_id > original_tablet_id);
}

#[test]
fn idempotency_replays_exact_commands_and_rejects_token_rebinding() {
    let mut catalog = Catalog::new();
    let command = stream("orders", 1);
    let first = catalog.apply(command.clone()).unwrap();
    let replay = catalog.apply(command).unwrap();
    let CatalogMutation::Applied {
        replayed, resource, ..
    } = replay
    else {
        panic!("expected apply replay");
    };
    assert!(replayed);
    assert_eq!(resource.generation, 1);

    let mut rebound = stream("orders", 2);
    let CatalogCommand::Apply(request) = &mut rebound else {
        unreachable!();
    };
    request.request_token = "apply-orders-1".into();
    assert!(matches!(
        catalog.apply(rebound),
        Err(CatalogError::IdempotencyConflict)
    ));
    assert_eq!(first.resource().unwrap().generation, 1);
}

#[test]
fn rejects_profile_changes_shard_shrink_and_stale_generations() {
    let mut catalog = Catalog::new();
    catalog.apply(stream("orders", 3)).unwrap();

    let mut shrink = stream("orders", 2);
    let CatalogCommand::Apply(request) = &mut shrink else {
        unreachable!();
    };
    request.request_token = "shrink-orders".into();
    request.expected_generation = Some(1);
    assert!(matches!(
        catalog.apply(shrink),
        Err(CatalogError::ShardCountDecrease {
            current: 3,
            requested: 2
        })
    ));

    let mut profile_change = stream("orders", 3);
    let CatalogCommand::Apply(request) = &mut profile_change else {
        unreachable!();
    };
    request.request_token = "change-profile".into();
    request.spec.workload_profile = WorkloadProfile::WorkQueue;
    assert!(matches!(
        catalog.apply(profile_change),
        Err(CatalogError::ProfileMismatch { .. })
    ));

    let mut stale = stream("orders", 4);
    let CatalogCommand::Apply(request) = &mut stale else {
        unreachable!();
    };
    request.request_token = "stale-update".into();
    request.expected_generation = Some(9);
    assert!(matches!(
        catalog.apply(stale),
        Err(CatalogError::GenerationConflict {
            expected: 9,
            actual: 1
        })
    ));
}

#[test]
fn canonical_command_replay_reconstructs_the_same_catalog_snapshot() {
    let commands = [
        stream("orders", 2),
        stream("audit", 1),
        CatalogCommand::Delete(DeleteResource {
            request_token: "delete-audit".into(),
            expected_generation: Some(1),
            name: ResourceName::new(
                "acme",
                "payments",
                "production",
                "core",
                ResourceKind::Stream,
                "audit",
            )
            .unwrap(),
        }),
    ];
    let mut live = Catalog::new();
    let mut recovered = Catalog::new();
    for command in commands {
        live.apply(command.clone()).unwrap();
        let encoded = command.encode().unwrap();
        recovered
            .apply(CatalogCommand::decode(&encoded).unwrap())
            .unwrap();
    }
    assert_eq!(recovered.snapshot(), live.snapshot());
    assert_eq!(
        recovered.state_digest().unwrap(),
        live.state_digest().unwrap()
    );

    let canonical = stream("pretty", 1).encode().unwrap();
    let pretty = serde_json::to_vec_pretty(
        &serde_json::from_slice::<serde_json::Value>(&canonical).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        CatalogCommand::decode(&pretty),
        Err(CatalogError::NonCanonicalCommand)
    ));
}

#[test]
fn native_snapshot_round_trip_restores_state_idempotency_and_identity_high_water_marks() {
    let mut live = Catalog::with_reserved_consensus_group(1).unwrap();
    let command = stream("orders", 2);
    let first = live.apply(command.clone()).unwrap();
    live.apply(stream("audit", 1)).unwrap();
    let encoded = live.encode_snapshot().unwrap();

    let mut restored = Catalog::decode_snapshot(&encoded).unwrap();
    assert_eq!(restored.snapshot(), live.snapshot());
    assert_eq!(
        restored.state_digest().unwrap(),
        live.state_digest().unwrap()
    );
    assert!(matches!(
        restored.apply(command).unwrap(),
        CatalogMutation::Applied { replayed: true, .. }
    ));

    let original_max_tablet = first
        .resource()
        .unwrap()
        .tablets
        .iter()
        .map(|tablet| tablet.tablet_id)
        .max()
        .unwrap();
    let next = applied(restored.apply(stream("next", 1)).unwrap());
    assert!(next.tablets[0].tablet_id > original_max_tablet);
    assert_ne!(next.tablets[0].consensus_group_id, 1);
}

#[test]
fn native_snapshot_rejects_noncanonical_corrupt_and_unknown_version_bytes() {
    let mut catalog = Catalog::new();
    catalog.apply(stream("orders", 1)).unwrap();
    let canonical = catalog.encode_snapshot().unwrap();

    let pretty = serde_json::to_vec_pretty(
        &serde_json::from_slice::<serde_json::Value>(&canonical).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        Catalog::decode_snapshot(&pretty),
        Err(CatalogError::NonCanonicalSnapshot)
    ));

    let mut corrupt: serde_json::Value = serde_json::from_slice(&canonical).unwrap();
    corrupt["snapshot"]["next_tablet_id"] = serde_json::json!(999);
    let corrupt = serde_json::to_vec(&corrupt).unwrap();
    assert!(matches!(
        Catalog::decode_snapshot(&corrupt),
        Err(CatalogError::SnapshotDigestMismatch)
    ));

    let mut unknown: serde_json::Value = serde_json::from_slice(&canonical).unwrap();
    unknown["format_version"] = serde_json::json!(2);
    let unknown = serde_json::to_vec(&unknown).unwrap();
    assert!(matches!(
        Catalog::decode_snapshot(&unknown),
        Err(CatalogError::UnsupportedSnapshotVersion(2))
    ));
}

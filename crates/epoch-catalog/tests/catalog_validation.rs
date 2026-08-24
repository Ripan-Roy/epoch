use std::collections::BTreeMap;

use epoch_catalog::{
    ApplyResource, CATALOG_COMMAND_FORMAT_VERSION, CATALOG_CONFIG_COMMAND_FORMAT_VERSION,
    CATALOG_GOVERNANCE_COMMAND_FORMAT_VERSION, CATALOG_MEMBERSHIP_COMMAND_FORMAT_VERSION,
    CATALOG_PLACEMENT_COMMAND_FORMAT_VERSION, CATALOG_PLACEMENT_SNAPSHOT_FORMAT_VERSION, Catalog,
    CatalogCommand, CatalogError, DataClassification, ResourceGovernance, ResourceName,
    ResourceSpec, TabletPlacement, catalog_proposal_id_for,
};
use epoch_core::{ResourceKind, WorkloadProfile};

fn name(kind: ResourceKind) -> ResourceName {
    ResourceName::new("acme", "payments", "production", "core", kind, "orders").unwrap()
}

fn apply(kind: ResourceKind, profile: WorkloadProfile) -> CatalogCommand {
    CatalogCommand::Apply(ApplyResource {
        request_token: "create-orders".into(),
        expected_generation: Some(0),
        name: name(kind),
        spec: ResourceSpec {
            workload_profile: profile,
            shard_count: 1,
            replica_count: 3,
            configuration: None,
            governance: None,
        },
        tablet_placements: Vec::new(),
    })
}

fn governance() -> ResourceGovernance {
    ResourceGovernance {
        owner: "team:payments".into(),
        cost_center: "cc-1042".into(),
        classification: DataClassification::Confidential,
        tags: BTreeMap::from([
            ("service".into(), "checkout".into()),
            ("tier".into(), "critical".into()),
        ]),
    }
}

#[test]
fn explicit_per_shard_placement_is_canonical_durable_and_bounded() {
    let mut command = apply(ResourceKind::Stream, WorkloadProfile::StreamLog);
    let CatalogCommand::Apply(request) = &mut command else {
        unreachable!();
    };
    request.spec.shard_count = 2;
    request.tablet_placements = vec![
        TabletPlacement {
            shard_index: 0,
            voter_node_ids: vec![1, 3, 5],
        },
        TabletPlacement {
            shard_index: 1,
            voter_node_ids: vec![2, 4, 6],
        },
    ];

    let encoded = command.encode().unwrap();
    let document: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(
        document["format_version"],
        CATALOG_PLACEMENT_COMMAND_FORMAT_VERSION
    );
    assert_eq!(CatalogCommand::decode(&encoded).unwrap(), command);

    let mut catalog = Catalog::new();
    let mutation = catalog.apply(command).unwrap();
    let tablets = mutation.resource().unwrap().tablets.clone();
    assert_eq!(tablets[0].voter_node_ids, [1, 3, 5]);
    assert_eq!(tablets[1].voter_node_ids, [2, 4, 6]);

    let snapshot = catalog.encode_snapshot().unwrap();
    let document: serde_json::Value = serde_json::from_slice(&snapshot).unwrap();
    assert_eq!(
        document["format_version"],
        CATALOG_PLACEMENT_SNAPSHOT_FORMAT_VERSION
    );
    let reopened = Catalog::decode_snapshot(&snapshot).unwrap();
    assert_eq!(
        reopened.state_digest().unwrap(),
        catalog.state_digest().unwrap()
    );
    assert_eq!(
        reopened
            .resource(&name(ResourceKind::Stream))
            .unwrap()
            .tablets,
        tablets
    );
}

#[test]
fn explicit_placement_rejects_partial_duplicate_and_replica_mismatched_assignments() {
    let invalid = [
        vec![TabletPlacement {
            shard_index: 0,
            voter_node_ids: vec![1, 2, 3],
        }],
        vec![
            TabletPlacement {
                shard_index: 0,
                voter_node_ids: vec![1, 2, 3],
            },
            TabletPlacement {
                shard_index: 0,
                voter_node_ids: vec![4, 5, 6],
            },
        ],
        vec![
            TabletPlacement {
                shard_index: 0,
                voter_node_ids: vec![1, 1, 3],
            },
            TabletPlacement {
                shard_index: 1,
                voter_node_ids: vec![4, 5, 6],
            },
        ],
    ];
    for placements in invalid {
        let mut command = apply(ResourceKind::Stream, WorkloadProfile::StreamLog);
        let CatalogCommand::Apply(request) = &mut command else {
            unreachable!();
        };
        request.spec.shard_count = 2;
        request.tablet_placements = placements;
        assert!(matches!(
            Catalog::new().apply(command),
            Err(CatalogError::InvalidSpec(_))
        ));
    }
}

#[test]
fn data_resource_kind_requires_its_matching_immutable_profile() {
    let cases = [
        (ResourceKind::Cache, WorkloadProfile::CacheAndState),
        (ResourceKind::Table, WorkloadProfile::CacheAndState),
        (ResourceKind::Stream, WorkloadProfile::StreamLog),
        (ResourceKind::Queue, WorkloadProfile::WorkQueue),
        (ResourceKind::EventBus, WorkloadProfile::EventBus),
    ];
    for (kind, profile) in cases {
        Catalog::new()
            .apply(apply(kind, profile))
            .expect("matching data resource and workload profile should be accepted");
    }

    assert!(matches!(
        Catalog::new().apply(apply(ResourceKind::Stream, WorkloadProfile::WorkQueue)),
        Err(CatalogError::InvalidSpec(_))
    ));
    assert!(matches!(
        Catalog::new().apply(apply(ResourceKind::Subscription, WorkloadProfile::EventBus)),
        Err(CatalogError::InvalidSpec(_))
    ));
}

#[test]
fn resource_names_reject_ambiguous_or_unbounded_components() {
    for invalid in ["", " core", "core ", "co/re"] {
        assert!(matches!(
            ResourceName::new(
                "acme",
                "payments",
                "production",
                invalid,
                ResourceKind::Stream,
                "orders"
            ),
            Err(CatalogError::InvalidName(_))
        ));
    }
    assert!(matches!(
        ResourceName::new(
            "x".repeat(129),
            "payments",
            "production",
            "core",
            ResourceKind::Stream,
            "orders"
        ),
        Err(CatalogError::InvalidName(_))
    ));
}

#[test]
fn versioned_command_decoder_rejects_unknown_versions_and_unknown_fields() {
    let command = apply(ResourceKind::Stream, WorkloadProfile::StreamLog);
    let encoded = command.encode().unwrap();
    let mut document: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    document["format_version"] = serde_json::json!(CATALOG_MEMBERSHIP_COMMAND_FORMAT_VERSION + 1);
    let unsupported = serde_json::to_vec(&document).unwrap();
    assert!(matches!(
        CatalogCommand::decode(&unsupported),
        Err(CatalogError::UnsupportedCommandVersion(version))
            if version == CATALOG_MEMBERSHIP_COMMAND_FORMAT_VERSION + 1
    ));

    document["format_version"] = serde_json::json!(CATALOG_COMMAND_FORMAT_VERSION);
    document["unexpected"] = serde_json::json!(true);
    let unknown = serde_json::to_vec(&document).unwrap();
    assert!(matches!(
        CatalogCommand::decode(&unknown),
        Err(CatalogError::Decoding(_))
    ));
}

#[test]
fn profile_configuration_is_versioned_persisted_and_immutable() {
    let mut configured = apply(ResourceKind::Cache, WorkloadProfile::CacheAndState);
    let CatalogCommand::Apply(request) = &mut configured else {
        unreachable!();
    };
    request.spec.configuration = Some(serde_json::json!({
        "max_entries": 2,
        "default_ttl_ms": null,
        "eviction": "all_keys_lru",
        "durability": "quorum_durable"
    }));
    let expected_configuration = request.spec.configuration.clone();
    let encoded = configured.encode().unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&encoded).unwrap()["format_version"],
        CATALOG_CONFIG_COMMAND_FORMAT_VERSION
    );
    assert_eq!(CatalogCommand::decode(&encoded).unwrap(), configured);

    let mut catalog = Catalog::new();
    let created = catalog.apply(configured.clone()).unwrap();
    assert_eq!(
        created.resource().unwrap().spec.configuration,
        expected_configuration
    );
    let snapshot = catalog.encode_snapshot().unwrap();
    let restored = Catalog::decode_snapshot(&snapshot).unwrap();
    assert_eq!(
        restored.state_digest().unwrap(),
        catalog.state_digest().unwrap()
    );

    let mut changed = configured;
    let CatalogCommand::Apply(request) = &mut changed else {
        unreachable!();
    };
    request.request_token = "change-cache-policy".into();
    request.expected_generation = Some(1);
    request.spec.configuration.as_mut().unwrap()["eviction"] = serde_json::json!("all_keys_lfu");
    assert_eq!(
        catalog.apply(changed).unwrap_err(),
        CatalogError::ConfigurationMismatch
    );

    let mut invalid = apply(ResourceKind::Cache, WorkloadProfile::CacheAndState);
    let CatalogCommand::Apply(request) = &mut invalid else {
        unreachable!();
    };
    request.spec.configuration = Some(serde_json::json!("not-an-object"));
    assert!(matches!(
        Catalog::new().apply(invalid),
        Err(CatalogError::InvalidSpec(_))
    ));
}

#[test]
fn governance_is_versioned_mutable_and_snapshot_persistent() {
    let mut command = apply(ResourceKind::Stream, WorkloadProfile::StreamLog);
    let CatalogCommand::Apply(request) = &mut command else {
        unreachable!();
    };
    request.spec.governance = Some(governance());
    let encoded = command.encode().unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&encoded).unwrap()["format_version"],
        CATALOG_GOVERNANCE_COMMAND_FORMAT_VERSION
    );
    assert_eq!(CatalogCommand::decode(&encoded).unwrap(), command);

    let mut catalog = Catalog::new();
    let created = catalog.apply(command.clone()).unwrap();
    assert_eq!(
        created
            .resource()
            .unwrap()
            .spec
            .governance
            .as_ref()
            .unwrap(),
        &governance()
    );
    let snapshot = catalog.encode_snapshot().unwrap();
    let restored = Catalog::decode_snapshot(&snapshot).unwrap();
    assert_eq!(
        restored.state_digest().unwrap(),
        catalog.state_digest().unwrap()
    );

    let CatalogCommand::Apply(request) = &mut command else {
        unreachable!();
    };
    request.request_token = "transfer-orders-ownership".into();
    request.expected_generation = Some(1);
    request.spec.governance.as_mut().unwrap().owner = "team:platform".into();
    let updated = catalog.apply(command).unwrap();
    assert_eq!(updated.resource().unwrap().generation, 2);
    assert_eq!(
        updated
            .resource()
            .unwrap()
            .spec
            .governance
            .as_ref()
            .unwrap()
            .owner,
        "team:platform"
    );
}

#[test]
fn governance_rejects_noncanonical_or_unbounded_metadata() {
    let invalid = [
        ResourceGovernance {
            owner: " Team:Payments".into(),
            ..governance()
        },
        ResourceGovernance {
            cost_center: String::new(),
            ..governance()
        },
        ResourceGovernance {
            tags: BTreeMap::from([("epoch.io/forged".into(), "true".into())]),
            ..governance()
        },
        ResourceGovernance {
            tags: (0..33)
                .map(|index| (format!("tag-{index}"), "value".into()))
                .collect(),
            ..governance()
        },
    ];
    for (index, governance) in invalid.into_iter().enumerate() {
        let mut command = apply(ResourceKind::Stream, WorkloadProfile::StreamLog);
        let CatalogCommand::Apply(request) = &mut command else {
            unreachable!();
        };
        request.request_token = format!("invalid-governance-{index}");
        request.spec.governance = Some(governance);
        assert!(matches!(
            Catalog::new().apply(command),
            Err(CatalogError::InvalidSpec(_))
        ));
    }
}

#[test]
fn replica_updates_preserve_tablet_identity_and_fence_resource_generation() {
    let mut catalog = Catalog::new();
    let first = catalog
        .apply(apply(ResourceKind::Stream, WorkloadProfile::StreamLog))
        .unwrap()
        .resource()
        .unwrap()
        .clone();
    let mut update = apply(ResourceKind::Stream, WorkloadProfile::StreamLog);
    let CatalogCommand::Apply(request) = &mut update else {
        unreachable!();
    };
    request.request_token = "increase-replicas".into();
    request.expected_generation = Some(1);
    request.spec.replica_count = 5;
    let updated = catalog.apply(update).unwrap().resource().unwrap().clone();

    assert_eq!(updated.generation, 2);
    assert_eq!(updated.tablets[0].tablet_id, first.tablets[0].tablet_id);
    assert_eq!(
        updated.tablets[0].consensus_group_id,
        first.tablets[0].consensus_group_id
    );
    assert_eq!(updated.tablets[0].resource_generation, 2);
    assert_eq!(updated.tablets[0].replica_count, 5);
}

#[test]
fn state_digest_covers_resource_state_and_completed_idempotency_history() {
    let mut live = Catalog::new();
    let empty_digest = live.state_digest().unwrap();
    let command = apply(ResourceKind::Stream, WorkloadProfile::StreamLog);
    live.apply(command.clone()).unwrap();
    let applied_digest = live.state_digest().unwrap();
    assert_ne!(applied_digest, empty_digest);

    let mut recovered = Catalog::new();
    recovered.apply(command).unwrap();
    assert_eq!(recovered.state_digest().unwrap(), applied_digest);

    let mut same_resource_with_another_completed_request = recovered.clone();
    let mut no_op = apply(ResourceKind::Stream, WorkloadProfile::StreamLog);
    let CatalogCommand::Apply(request) = &mut no_op else {
        unreachable!();
    };
    request.request_token = "observe-same-resource".into();
    request.expected_generation = Some(1);
    same_resource_with_another_completed_request
        .apply(no_op)
        .unwrap();
    assert_eq!(
        same_resource_with_another_completed_request
            .resource(&name(ResourceKind::Stream))
            .unwrap(),
        recovered.resource(&name(ResourceKind::Stream)).unwrap()
    );
    assert_ne!(
        same_resource_with_another_completed_request
            .state_digest()
            .unwrap(),
        applied_digest
    );
}

#[test]
fn proposal_identity_is_stable_scope_separated_and_validated() {
    let proposal = catalog_proposal_id_for(7, 3, "create-orders").unwrap();
    assert_eq!(
        proposal,
        catalog_proposal_id_for(7, 3, "create-orders").unwrap()
    );
    assert_ne!(
        proposal,
        catalog_proposal_id_for(8, 3, "create-orders").unwrap()
    );
    assert_ne!(
        proposal,
        catalog_proposal_id_for(7, 4, "create-orders").unwrap()
    );
    assert_ne!(
        proposal,
        catalog_proposal_id_for(7, 3, "create-payments").unwrap()
    );
    assert!(catalog_proposal_id_for(0, 3, "create-orders").is_err());
    assert!(catalog_proposal_id_for(7, 0, "create-orders").is_err());
    assert!(catalog_proposal_id_for(7, 3, " ").is_err());
}

#[test]
fn reserved_catalog_group_is_never_allocated_to_a_data_tablet() {
    let mut catalog = Catalog::with_reserved_consensus_group(1).unwrap();
    let first = catalog
        .apply(apply(ResourceKind::Stream, WorkloadProfile::StreamLog))
        .unwrap();
    assert_eq!(first.resource().unwrap().tablets[0].consensus_group_id, 2);

    let mut later = Catalog::with_reserved_consensus_group(2).unwrap();
    let first = later
        .apply(apply(ResourceKind::Stream, WorkloadProfile::StreamLog))
        .unwrap();
    assert_eq!(first.resource().unwrap().tablets[0].consensus_group_id, 1);
    let mut expand = apply(ResourceKind::Stream, WorkloadProfile::StreamLog);
    let CatalogCommand::Apply(request) = &mut expand else {
        unreachable!();
    };
    request.request_token = "expand-orders".into();
    request.expected_generation = Some(1);
    request.spec.shard_count = 2;
    let expanded = later.apply(expand).unwrap();
    assert_eq!(
        expanded.resource().unwrap().tablets[1].consensus_group_id,
        3
    );
}

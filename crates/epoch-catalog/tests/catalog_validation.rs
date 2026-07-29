use epoch_catalog::{
    ApplyResource, CATALOG_COMMAND_FORMAT_VERSION, Catalog, CatalogCommand, CatalogError,
    ResourceName, ResourceSpec, catalog_proposal_id_for,
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
        },
    })
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
    document["format_version"] = serde_json::json!(CATALOG_COMMAND_FORMAT_VERSION + 1);
    let unsupported = serde_json::to_vec(&document).unwrap();
    assert!(matches!(
        CatalogCommand::decode(&unsupported),
        Err(CatalogError::UnsupportedCommandVersion(version))
            if version == CATALOG_COMMAND_FORMAT_VERSION + 1
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

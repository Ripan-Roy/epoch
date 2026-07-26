use epoch_catalog::{
    ApplyResource, CATALOG_COMMAND_FORMAT_VERSION, Catalog, CatalogCommand, CatalogError,
    ResourceName, ResourceSpec,
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

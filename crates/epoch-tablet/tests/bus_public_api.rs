use epoch_bus::{BusConfig, EventFilter, EventTransform, Subscription, SubscriptionTarget};
use epoch_core::EventEnvelope;
use epoch_tablet::{
    BUS_TABLET_COMMAND_FORMAT_VERSION, BusTablet, BusTabletCommand, BusTabletDisposition,
    BusTabletOperationResult, BusTabletOutcome, BusTabletScope, BusTabletWriteEvidence,
    CommittedCommand, MAX_BUS_TABLET_COMMAND_BYTES, bus_proposal_id_for,
};
use serde_json::json;

fn scope() -> BusTabletScope {
    BusTabletScope::new(29, 4, "orders-bus").unwrap()
}

#[test]
fn bus_route_plan_is_usable_through_the_public_crate_api() {
    let scope = scope();
    let command = BusTabletCommand::upsert_subscription(
        &scope,
        "route-audit",
        Subscription {
            name: "audit".into(),
            filter: EventFilter {
                event_type_patterns: vec!["order.*".into()],
                ..EventFilter::default()
            },
            target: SubscriptionTarget::Pull,
            transform: EventTransform::default(),
        },
        1_700_000_000_123,
    )
    .unwrap();
    let proposal_id = command.proposal_id(&scope).unwrap();
    assert_eq!(
        bus_proposal_id_for(&scope, "route-audit").unwrap(),
        proposal_id
    );
    let payload = command.encode(&scope).unwrap();
    assert_eq!(BusTabletCommand::decode(&payload, &scope).unwrap(), command);
    let committed = CommittedCommand {
        group_id: 29,
        group_epoch: 4,
        proposal_id,
        term: 9,
        log_index: 23,
        payload: &payload,
    };
    let mut tablet = BusTablet::new(scope.clone(), BusConfig::default()).unwrap();
    let route_receipt = tablet.apply(committed).unwrap();
    assert_eq!(
        route_receipt.outcome,
        BusTabletOutcome::Applied {
            result: BusTabletOperationResult::SubscriptionUpserted {
                name: "audit".into(),
                replaced: false,
                route_plan_version: 2,
            },
        }
    );

    let mut envelope =
        EventEnvelope::new("checkout", "order.created", json!({"order_id": "one"}), 10);
    envelope.id = "evt-1".into();
    let publish =
        BusTabletCommand::publish(&scope, "publish-1", envelope, 1_700_000_000_124).unwrap();
    let publish_id = publish.proposal_id(&scope).unwrap();
    let publish_payload = publish.encode(&scope).unwrap();
    let publish_receipt = tablet
        .apply(CommittedCommand {
            group_id: 29,
            group_epoch: 4,
            proposal_id: publish_id,
            term: 9,
            log_index: 24,
            payload: &publish_payload,
        })
        .unwrap();
    assert_eq!(
        publish_receipt.write_evidence,
        BusTabletWriteEvidence::FixedVoterMajorityPersisted
    );
    assert_eq!(publish_receipt.disposition, BusTabletDisposition::New);
    assert!(matches!(
        publish_receipt.outcome,
        BusTabletOutcome::Applied {
            result: BusTabletOperationResult::Published {
                position: 1,
                route_plan_version: 2,
                delivery_count: 1,
                ..
            }
        }
    ));

    let encoded = serde_json::to_value(&publish_receipt).unwrap();
    assert_eq!(encoded["proposal_id"], json!(publish_id.to_string()));
    assert_eq!(encoded["tablet_id"], json!("29"));
    assert_eq!(encoded["tablet_epoch"], json!("4"));
    assert_eq!(encoded["term"], json!("9"));
    assert_eq!(encoded["commit_index"], json!("24"));
    assert_eq!(encoded["applied_at_ms"], json!("1700000000124"));
    assert_eq!(
        encoded["write_evidence"],
        json!("fixed_voter_majority_persisted")
    );
    assert_eq!(BUS_TABLET_COMMAND_FORMAT_VERSION, 1);
    assert_eq!(MAX_BUS_TABLET_COMMAND_BYTES, 512 * 1024);
}

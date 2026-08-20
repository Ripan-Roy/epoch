use std::collections::{BTreeMap, BTreeSet};

use epoch_bus::{
    BusConfig, DeliveryBackoffStrategy, DeliveryPolicy, DeliveryRetryPolicy, DeliveryState,
    EpochTargetDestination, EpochTargetKind, EventFilter, EventTransform, Subscription,
    SubscriptionTarget,
};
use epoch_core::{EpochError, EventEnvelope};
use serde_json::{Value, json};

use super::*;
use crate::{CommittedCommand, TabletError};

fn scope() -> BusTabletScope {
    BusTabletScope::new(29, 4, "orders-bus").unwrap()
}

fn event(id: &str, event_type: &str) -> EventEnvelope {
    let mut envelope = EventEnvelope::new(
        "checkout",
        event_type,
        json!({"order": {"id": id, "total": 42}}),
        10,
    );
    envelope.id = id.into();
    envelope.headers.insert("tenant".into(), "acme".into());
    envelope
}

fn subscription(name: &str, target: SubscriptionTarget) -> Subscription {
    Subscription {
        name: name.into(),
        filter: EventFilter {
            event_type_patterns: vec!["order.*".into()],
            headers: BTreeMap::from([("tenant".into(), "acme".into())]),
            ..EventFilter::default()
        },
        target,
        transform: EventTransform::default(),
        delivery_policy: DeliveryPolicy::default(),
    }
}

fn encoded(command: &BusTabletCommand) -> (u64, Vec<u8>) {
    let scope = scope();
    let proposal_id = command.proposal_id(&scope).unwrap();
    (proposal_id, command.encode(&scope).unwrap())
}

fn committed(proposal_id: u64, term: u64, log_index: u64, payload: &[u8]) -> CommittedCommand<'_> {
    CommittedCommand {
        group_id: 29,
        group_epoch: 4,
        proposal_id,
        term,
        log_index,
        payload,
    }
}

#[test]
fn command_codec_is_versioned_bounded_strict_and_scope_separated() {
    let command =
        BusTabletCommand::publish(&scope(), "publish-1", event("evt-1", "order.created"), 11)
            .unwrap();
    let (_, valid) = encoded(&command);
    assert_eq!(
        BusTabletCommand::decode(&valid, &scope())
            .unwrap()
            .format_version,
        1
    );
    assert_eq!(
        String::from_utf8(valid.clone()).unwrap(),
        r#"{"format_version":1,"tablet_id":29,"tablet_epoch":4,"resource":"orders-bus","idempotency_key":"publish-1","applied_at_ms":11,"operation":{"kind":"publish","envelope":{"id":"evt-1","source":"checkout","type":"order.created","time_ms":10,"headers":{"tenant":"acme"},"content_type":"application/json","payload":{"order":{"id":"evt-1","total":42}},"priority":0,"extensions":{}}}}"#
    );
    assert!(matches!(
        BusTabletCommand::decode(&serde_json::to_vec_pretty(&command).unwrap(), &scope()),
        Err(TabletError::Decoding(_))
    ));

    let mut document: Value = serde_json::from_slice(&valid).unwrap();
    document["unknown"] = json!(true);
    assert!(matches!(
        BusTabletCommand::decode(&serde_json::to_vec(&document).unwrap(), &scope()),
        Err(TabletError::Decoding(_))
    ));
    assert!(matches!(
        BusTabletCommand::decode(&vec![b'x'; MAX_BUS_TABLET_COMMAND_BYTES + 1], &scope()),
        Err(TabletError::InvalidCommand(_))
    ));

    let first = bus_proposal_id_for(&scope(), "publish-1").unwrap();
    assert_eq!(first, command.proposal_id(&scope()).unwrap());
    assert_ne!(
        first,
        bus_proposal_id_for(
            &BusTabletScope::new(29, 5, "orders-bus").unwrap(),
            "publish-1"
        )
        .unwrap()
    );
}

#[test]
fn signed_targets_and_terminal_rejection_use_v2_without_rewriting_v1_commands() {
    let legacy = BusTabletCommand::publish(
        &scope(),
        "publish-legacy",
        event("evt-legacy", "order.created"),
        11,
    )
    .unwrap();
    assert_eq!(legacy.format_version, 1);
    let unsigned = BusTabletCommand::upsert_subscription(
        &scope(),
        "unsigned-webhook",
        subscription(
            "unsigned",
            SubscriptionTarget::Webhook {
                url: "https://example.com/unsigned".into(),
                signing_key_id: None,
            },
        ),
        12,
    )
    .unwrap();
    assert_eq!(unsigned.format_version, 1);
    let signed = BusTabletCommand::upsert_subscription(
        &scope(),
        "signed-webhook",
        subscription(
            "signed",
            SubscriptionTarget::Webhook {
                url: "https://example.com/signed".into(),
                signing_key_id: Some("primary".into()),
            },
        ),
        12,
    )
    .unwrap();
    assert_eq!(signed.format_version, 2);
    assert_eq!(
        BusTabletCommand::decode(&signed.encode(&scope()).unwrap(), &scope()).unwrap(),
        signed
    );
    let exact_acquire = BusTabletCommand::new(
        &scope(),
        "exact-acquire",
        12,
        BusTabletOperation::AcquireDeliveries {
            subscription: "signed".into(),
            dispatcher: "webhook-sender".into(),
            dispatcher_epoch: 1,
            max_deliveries: 1,
            expected_delivery_id: Some("epoch.bus.delivery.v1.1.signed".into()),
            destination: None,
        },
    )
    .unwrap();
    assert_eq!(exact_acquire.format_version, 2);
    assert_eq!(
        BusTabletCommand::decode(&exact_acquire.encode(&scope()).unwrap(), &scope()).unwrap(),
        exact_acquire
    );

    let rejection = BusTabletCommand::new(
        &scope(),
        "reject-1",
        12,
        BusTabletOperation::RejectDelivery {
            delivery_id: "epoch.bus.delivery.v1.1.orders".into(),
            dispatcher: "webhook-sender".into(),
            dispatcher_epoch: 7,
            lease_token: "epoch.bus.delivery.lease.v1.token".into(),
            reason: "http_status_400".into(),
        },
    )
    .unwrap();
    assert_eq!(rejection.format_version, 2);
    let encoded = rejection.encode(&scope()).unwrap();
    assert_eq!(
        BusTabletCommand::decode(&encoded, &scope()).unwrap(),
        rejection
    );

    let mut mislabeled: Value = serde_json::from_slice(&encoded).unwrap();
    mislabeled["format_version"] = json!(1);
    assert!(matches!(
        BusTabletCommand::decode(&serde_json::to_vec(&mislabeled).unwrap(), &scope()),
        Err(TabletError::InvalidCommand(_))
    ));
}

#[test]
fn exact_epoch_target_acquisition_uses_canonical_v3_destination_binding() {
    let destination =
        EpochTargetDestination::new(EpochTargetKind::Stream, "events", 8, 3, 83, 2).unwrap();
    let command = BusTabletCommand::new(
        &scope(),
        "epoch-target-acquire",
        12,
        BusTabletOperation::AcquireDeliveries {
            subscription: "audit".into(),
            dispatcher: "epoch-target-v1".into(),
            dispatcher_epoch: 1,
            max_deliveries: 1,
            expected_delivery_id: Some("epoch.bus.delivery.v1.1.audit".into()),
            destination: Some(destination),
        },
    )
    .unwrap();
    assert_eq!(command.format_version, 3);
    let encoded = command.encode(&scope()).unwrap();
    assert_eq!(
        String::from_utf8(encoded.clone()).unwrap(),
        r#"{"format_version":3,"tablet_id":29,"tablet_epoch":4,"resource":"orders-bus","idempotency_key":"epoch-target-acquire","applied_at_ms":12,"operation":{"kind":"acquire_deliveries","subscription":"audit","dispatcher":"epoch-target-v1","dispatcher_epoch":1,"max_deliveries":1,"expected_delivery_id":"epoch.bus.delivery.v1.1.audit","destination":{"kind":"stream","resource":"events","resource_generation":8,"shard_index":3,"tablet_id":83,"tablet_epoch":2}}}"#
    );
    assert_eq!(
        BusTabletCommand::decode(&encoded, &scope()).unwrap(),
        command
    );

    let mut mislabeled: Value = serde_json::from_slice(&encoded).unwrap();
    mislabeled["format_version"] = json!(2);
    assert!(matches!(
        BusTabletCommand::decode(&serde_json::to_vec(&mislabeled).unwrap(), &scope()),
        Err(TabletError::InvalidCommand(_))
    ));
}

#[test]
fn committed_route_plan_is_sorted_captured_and_transformation_stable() {
    let mut tablet = BusTablet::new(scope(), BusConfig::default()).unwrap();
    let worker = BusTabletCommand::upsert_subscription(
        &scope(),
        "route-worker",
        subscription(
            "worker",
            SubscriptionTarget::Queue {
                resource: "orders".into(),
            },
        ),
        11,
    )
    .unwrap();
    let mut audit_subscription = subscription("audit", SubscriptionTarget::Pull);
    audit_subscription.transform = EventTransform {
        add_headers: BTreeMap::from([("routed-by".into(), "epoch".into())]),
        payload_projection: BTreeMap::from([("order_id".into(), "order.id".into())]),
    };
    let audit =
        BusTabletCommand::upsert_subscription(&scope(), "route-audit", audit_subscription, 12)
            .unwrap();
    let publish =
        BusTabletCommand::publish(&scope(), "publish-1", event("evt-1", "order.created"), 13)
            .unwrap();

    for (index, command) in [worker, audit].into_iter().enumerate() {
        let (proposal_id, payload) = encoded(&command);
        tablet
            .apply(committed(
                proposal_id,
                2,
                u64::try_from(index).unwrap() + 4,
                &payload,
            ))
            .unwrap();
    }
    let (proposal_id, payload) = encoded(&publish);
    let receipt = tablet
        .apply(committed(proposal_id, 2, 6, &payload))
        .unwrap();
    let BusTabletOutcome::Applied {
        result:
            BusTabletOperationResult::Published {
                position,
                route_plan_version,
                delivery_count,
                delivery_plan_digest,
            },
    } = receipt.outcome
    else {
        panic!("expected a successful publish");
    };
    assert_eq!(position, 1);
    assert_eq!(route_plan_version, 3);
    assert_eq!(delivery_count, 2);
    assert_eq!(delivery_plan_digest.len(), 64);

    let remove =
        BusTabletCommand::remove_subscription(&scope(), "remove-audit", "audit", 14).unwrap();
    let (remove_id, remove_payload) = encoded(&remove);
    tablet
        .apply(committed(remove_id, 2, 7, &remove_payload))
        .unwrap();
    assert_eq!(tablet.route_plan_version(), 4);
    assert_eq!(tablet.subscription_count(), 1);
    assert_eq!(tablet.commit_position(), 1);
}

#[test]
fn exact_replay_returns_original_result_without_mutating_state() {
    let command =
        BusTabletCommand::publish(&scope(), "publish-1", event("evt-1", "order.created"), 11)
            .unwrap();
    let (proposal_id, payload) = encoded(&command);
    let commit = committed(proposal_id, 2, 4, &payload);
    let mut tablet = BusTablet::new(scope(), BusConfig::default()).unwrap();
    let original = tablet.apply(commit).unwrap();
    let digest = tablet.state_digest();
    let replayed = tablet.apply(commit).unwrap();

    assert_eq!(original.outcome, replayed.outcome);
    assert_eq!(replayed.disposition, BusTabletDisposition::Replayed);
    assert_eq!(tablet.applied_command_count(), 1);
    assert_eq!(tablet.commit_position(), 1);
    assert_eq!(tablet.state_digest(), digest);
    assert_eq!(
        digest,
        [
            150, 209, 60, 201, 34, 99, 218, 148, 227, 242, 74, 50, 250, 3, 158, 61, 242, 216, 76,
            14, 201, 32, 60, 41, 228, 224, 110, 149, 173, 53, 19, 122,
        ]
    );
}

#[test]
fn recordable_capacity_failure_is_committed_but_business_state_is_atomic() {
    let mut tablet = BusTablet::new(
        scope(),
        BusConfig {
            max_archive_events: 1,
            ..BusConfig::default()
        },
    )
    .unwrap();
    for (index, id) in ["evt-1", "evt-2"].into_iter().enumerate() {
        let command = BusTabletCommand::publish(
            &scope(),
            format!("publish-{id}"),
            event(id, "order.created"),
            11 + u64::try_from(index).unwrap(),
        )
        .unwrap();
        let (proposal_id, payload) = encoded(&command);
        let receipt = tablet
            .apply(committed(
                proposal_id,
                2,
                u64::try_from(index).unwrap() + 4,
                &payload,
            ))
            .unwrap();
        if index == 1 {
            assert!(matches!(
                receipt.outcome,
                BusTabletOutcome::Rejected {
                    code: BusTabletRejectionCode::Capacity,
                    ..
                }
            ));
        }
    }
    assert_eq!(tablet.commit_position(), 1);
    assert_eq!(tablet.archived_event_count(), 1);
    assert_eq!(tablet.applied_command_count(), 2);
    assert_eq!(tablet.last_applied_command_index(), 5);
}

#[test]
fn conflicting_or_out_of_order_commits_fail_closed() {
    let command =
        BusTabletCommand::publish(&scope(), "publish-1", event("evt-1", "order.created"), 11)
            .unwrap();
    let conflicting = BusTabletCommand::publish(
        &scope(),
        "publish-1",
        event("evt-other", "order.created"),
        11,
    )
    .unwrap();
    let (proposal_id, payload) = encoded(&command);
    let (_, conflicting_payload) = encoded(&conflicting);
    let mut tablet = BusTablet::new(scope(), BusConfig::default()).unwrap();
    tablet
        .apply(committed(proposal_id, 2, 4, &payload))
        .unwrap();
    assert!(matches!(
        tablet.apply(committed(proposal_id, 2, 4, &conflicting_payload)),
        Err(TabletError::ConflictingCommand { .. })
    ));

    let next =
        BusTabletCommand::publish(&scope(), "publish-2", event("evt-2", "order.created"), 12)
            .unwrap();
    let (next_id, next_payload) = encoded(&next);
    assert!(matches!(
        tablet.apply(committed(next_id, 2, 3, &next_payload)),
        Err(TabletError::CommitOrder { .. })
    ));
    assert_eq!(tablet.commit_position(), 1);
}

#[test]
fn identical_committed_history_converges_on_every_voter() {
    let history = vec![
        BusTabletCommand::upsert_subscription(
            &scope(),
            "route-audit",
            subscription("audit", SubscriptionTarget::Pull),
            11,
        )
        .unwrap(),
        BusTabletCommand::publish(&scope(), "publish-1", event("evt-1", "order.created"), 12)
            .unwrap(),
        BusTabletCommand::publish(&scope(), "publish-2", event("evt-2", "order.cancelled"), 13)
            .unwrap(),
    ]
    .into_iter()
    .map(|command| encoded(&command))
    .collect::<Vec<_>>();
    let mut tablets = [
        BusTablet::new(scope(), BusConfig::default()).unwrap(),
        BusTablet::new(scope(), BusConfig::default()).unwrap(),
        BusTablet::new(scope(), BusConfig::default()).unwrap(),
    ];
    for tablet in &mut tablets {
        for (index, (proposal_id, payload)) in history.iter().enumerate() {
            tablet
                .apply(committed(
                    *proposal_id,
                    3,
                    u64::try_from(index).unwrap() + 8,
                    payload,
                ))
                .unwrap();
        }
    }

    let expected = (
        tablets[0].state_digest(),
        tablets[0].business_state_digest(),
        tablets[0].route_plan_version(),
        tablets[0].commit_position(),
        tablets[0].archived_event_count(),
    );
    for tablet in &tablets[1..] {
        assert_eq!(
            (
                tablet.state_digest(),
                tablet.business_state_digest(),
                tablet.route_plan_version(),
                tablet.commit_position(),
                tablet.archived_event_count(),
            ),
            expected
        );
    }
}

#[test]
fn native_snapshot_restores_routes_archive_delivery_state_and_retry_suffix() {
    let mut live = BusTablet::new(scope(), BusConfig::default()).unwrap();
    let route = BusTabletCommand::upsert_subscription(
        &scope(),
        "route-audit",
        subscription("audit", SubscriptionTarget::Pull),
        11,
    )
    .unwrap();
    let (route_id, route_payload) = encoded(&route);
    live.apply(committed(route_id, 2, 4, &route_payload))
        .unwrap();
    let publish =
        BusTabletCommand::publish(&scope(), "publish-1", event("evt-1", "order.created"), 12)
            .unwrap();
    let (publish_id, publish_payload) = encoded(&publish);
    live.apply(committed(publish_id, 2, 5, &publish_payload))
        .unwrap();
    let expected_digest = live.state_digest();
    let expected_business_digest = live.business_state_digest();

    let snapshot = live.encode_snapshot(&BTreeSet::from([publish_id])).unwrap();
    let mut restored = BusTablet::decode_snapshot(&scope(), &snapshot).unwrap();

    assert_eq!(restored.subscription_count(), 1);
    assert_eq!(restored.commit_position(), 1);
    assert_eq!(restored.archived_event_count(), 1);
    assert_eq!(restored.delivery_counts().pending, 1);
    assert_eq!(restored.state_digest(), expected_digest);
    assert_eq!(restored.business_state_digest(), expected_business_digest);
    assert_eq!(restored.last_applied_command_index(), 5);
    assert_eq!(restored.applied_command_count(), 1);
    assert!(
        restored
            .receipt_for_committed(committed(route_id, 2, 4, &route_payload))
            .unwrap()
            .is_none()
    );
    assert!(
        restored
            .receipt_for_committed(committed(publish_id, 2, 5, &publish_payload))
            .unwrap()
            .is_some()
    );

    let next =
        BusTabletCommand::publish(&scope(), "publish-2", event("evt-2", "order.updated"), 13)
            .unwrap();
    let (next_id, next_payload) = encoded(&next);
    restored
        .apply(committed(next_id, 2, 6, &next_payload))
        .unwrap();
    assert_eq!(restored.commit_position(), 2);
    assert_eq!(restored.delivery_counts().pending, 2);
}

#[test]
fn native_snapshot_rejects_noncanonical_or_foreign_images() {
    let mut live = BusTablet::new(scope(), BusConfig::default()).unwrap();
    let publish =
        BusTabletCommand::publish(&scope(), "publish-1", event("evt-1", "order.created"), 12)
            .unwrap();
    let (proposal_id, payload) = encoded(&publish);
    live.apply(committed(proposal_id, 2, 4, &payload)).unwrap();
    let snapshot = live
        .encode_snapshot(&BTreeSet::from([proposal_id]))
        .unwrap();
    let document: VersionedBusTabletSnapshot = serde_json::from_slice(&snapshot).unwrap();

    assert!(
        BusTablet::decode_snapshot(&scope(), &serde_json::to_vec_pretty(&document).unwrap())
            .is_err()
    );
    assert!(
        BusTablet::decode_snapshot(
            &BusTabletScope::new(30, 4, "orders-bus").unwrap(),
            &snapshot,
        )
        .is_err()
    );
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one sequential lifecycle test keeps acquire, retry, acknowledgement, exact replay, and full recovery evidence together"
)]
fn delivery_commands_are_fenced_retriable_and_recoverable() {
    let mut route = subscription("audit", SubscriptionTarget::Pull);
    route.delivery_policy = DeliveryPolicy {
        timeout_ms: 10,
        max_in_flight: 1,
        retry: DeliveryRetryPolicy {
            strategy: DeliveryBackoffStrategy::Fixed,
            initial_delay_ms: 5,
            max_delay_ms: 5,
            jitter_percent: 0,
            max_attempts: 2,
            max_age_ms: None,
        },
    };
    let commands = [
        BusTabletCommand::upsert_subscription(&scope(), "route-audit", route, 100).unwrap(),
        BusTabletCommand::publish(&scope(), "publish-1", event("evt-1", "order.created"), 101)
            .unwrap(),
        BusTabletCommand::new(
            &scope(),
            "acquire-1",
            102,
            BusTabletOperation::AcquireDeliveries {
                subscription: "audit".into(),
                dispatcher: "sender".into(),
                dispatcher_epoch: 1,
                max_deliveries: 1,
                expected_delivery_id: None,
                destination: None,
            },
        )
        .unwrap(),
    ];
    let mut tablet = BusTablet::new(scope(), BusConfig::default()).unwrap();
    let mut last_receipt = None;
    for (offset, command) in commands.iter().enumerate() {
        let (proposal_id, payload) = encoded(command);
        last_receipt = Some(
            tablet
                .apply(committed(
                    proposal_id,
                    2,
                    u64::try_from(offset).unwrap() + 4,
                    &payload,
                ))
                .unwrap(),
        );
    }
    let BusTabletOutcome::Applied {
        result: BusTabletOperationResult::DeliveriesAcquired { deliveries },
    } = last_receipt.unwrap().outcome
    else {
        panic!("expected a delivery lease");
    };
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].attempt, 1);
    let delivery_id = deliveries[0].delivery_id.clone();
    let first_token = deliveries[0].lease_token.clone();

    let failed = BusTabletCommand::new(
        &scope(),
        "fail-1",
        103,
        BusTabletOperation::FailDelivery {
            delivery_id: delivery_id.clone(),
            dispatcher: "sender".into(),
            dispatcher_epoch: 1,
            lease_token: first_token,
            reason: "downstream unavailable".into(),
        },
    )
    .unwrap();
    let (proposal_id, payload) = encoded(&failed);
    tablet
        .apply(committed(proposal_id, 2, 7, &payload))
        .unwrap();
    assert!(matches!(
        tablet.delivery(&delivery_id).unwrap().state,
        DeliveryState::Pending {
            eligible_at_ms: 108
        }
    ));

    let reacquire = BusTabletCommand::new(
        &scope(),
        "acquire-2",
        108,
        BusTabletOperation::AcquireDeliveries {
            subscription: "audit".into(),
            dispatcher: "sender".into(),
            dispatcher_epoch: 1,
            max_deliveries: 1,
            expected_delivery_id: None,
            destination: None,
        },
    )
    .unwrap();
    let (proposal_id, payload) = encoded(&reacquire);
    let receipt = tablet
        .apply(committed(proposal_id, 2, 8, &payload))
        .unwrap();
    let BusTabletOutcome::Applied {
        result: BusTabletOperationResult::DeliveriesAcquired { deliveries },
    } = receipt.outcome
    else {
        panic!("expected a retry lease");
    };
    assert_eq!(deliveries[0].attempt, 2);

    let acknowledge = BusTabletCommand::new(
        &scope(),
        "ack-2",
        109,
        BusTabletOperation::AcknowledgeDelivery {
            delivery_id: delivery_id.clone(),
            dispatcher: "sender".into(),
            dispatcher_epoch: 1,
            lease_token: deliveries[0].lease_token.clone(),
        },
    )
    .unwrap();
    let (proposal_id, payload) = encoded(&acknowledge);
    let committed_ack = committed(proposal_id, 2, 9, &payload);
    let original = tablet.apply(committed_ack).unwrap();
    let digest = tablet.state_digest();
    let replayed = tablet.apply(committed_ack).unwrap();
    assert_eq!(original.outcome, replayed.outcome);
    assert_eq!(tablet.state_digest(), digest);
    assert_eq!(tablet.delivery_counts().acknowledged, 1);

    let mut recovered = BusTablet::new(scope(), BusConfig::default()).unwrap();
    for (index, command) in commands
        .iter()
        .chain([&failed, &reacquire, &acknowledge])
        .enumerate()
    {
        let (proposal_id, payload) = encoded(command);
        recovered
            .apply(committed(
                proposal_id,
                2,
                u64::try_from(index).unwrap() + 4,
                &payload,
            ))
            .unwrap();
    }
    assert_eq!(recovered.state_digest(), tablet.state_digest());
    assert_eq!(
        recovered.business_state_digest(),
        tablet.business_state_digest()
    );
}

#[test]
fn invalid_subscription_is_rejected_before_consensus_application() {
    let invalid = subscription(
        "bad/name",
        SubscriptionTarget::Webhook {
            url: "https://example.com".into(),
            signing_key_id: None,
        },
    );
    assert!(matches!(
        BusTabletCommand::upsert_subscription(&scope(), "route-invalid", invalid, 11),
        Err(TabletError::Profile(EpochError::InvalidArgument(_)))
    ));
}

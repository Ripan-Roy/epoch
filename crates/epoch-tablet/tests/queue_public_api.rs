use epoch_core::EventEnvelope;
use epoch_tablet::{
    CommittedCommand, MAX_QUEUE_ACQUIRE_BATCH_SIZE, MAX_QUEUE_CONSUMER_BYTES,
    MAX_QUEUE_MESSAGE_ID_BYTES, MAX_QUEUE_REASON_BYTES, MAX_QUEUE_TABLET_COMMAND_BYTES,
    QUEUE_TABLET_COMMAND_FORMAT_VERSION, QueueEnqueueCommand, QueueTablet, QueueTabletCommand,
    QueueTabletDisposition, QueueTabletOperation, QueueTabletOperationResult, QueueTabletOutcome,
    QueueTabletReceipt, QueueTabletScope, QueueTabletWriteEvidence, StreamTabletCommand,
    StreamTabletScope, proposal_id_for, queue_proposal_id_for,
};
use serde_json::{Value, json};

fn event(id: &str, event_type: &str, time_ms: u64) -> EventEnvelope {
    let mut envelope = EventEnvelope::new("tests", event_type, json!({"id": id}), time_ms);
    id.clone_into(&mut envelope.id);
    envelope
}

#[test]
fn queue_enqueue_is_usable_through_the_public_crate_api() {
    let scope = QueueTabletScope::new(11, 4, "jobs").unwrap();
    let envelope = event("job-1", "job.created", 1_700_000_000_000);
    let command = QueueTabletCommand::enqueue(
        &scope,
        "enqueue-request-1",
        envelope.clone(),
        1_700_000_000_123,
    )
    .unwrap();
    let explicitly_constructed = QueueTabletCommand::new(
        &scope,
        "enqueue-request-1",
        1_700_000_000_123,
        QueueTabletOperation::Enqueue(Box::new(QueueEnqueueCommand {
            partition: 0,
            envelope,
        })),
    )
    .unwrap();
    assert_eq!(command, explicitly_constructed);

    let proposal_id = command.proposal_id(&scope).unwrap();
    assert_eq!(proposal_id, 6_929_955_191_292_836_212);
    assert_eq!(
        queue_proposal_id_for(&scope, "enqueue-request-1").unwrap(),
        proposal_id
    );
    let payload = command.encode(&scope).unwrap();
    assert_eq!(
        QueueTabletCommand::decode(&payload, &scope).unwrap(),
        command
    );

    let committed = CommittedCommand {
        group_id: 11,
        group_epoch: 4,
        proposal_id,
        term: 7,
        log_index: 13,
        payload: &payload,
    };
    let receipt: QueueTabletReceipt = QueueTablet::with_default_config(scope)
        .unwrap()
        .apply(committed)
        .unwrap();

    assert_eq!(receipt.proposal_id, proposal_id);
    assert_eq!(receipt.tablet_id, 11);
    assert_eq!(receipt.tablet_epoch, 4);
    assert_eq!(receipt.term, 7);
    assert_eq!(receipt.commit_index, 13);
    assert_eq!(receipt.applied_at_ms, 1_700_000_000_123);
    assert_eq!(
        receipt.write_evidence,
        QueueTabletWriteEvidence::FixedVoterMajorityPersisted
    );
    assert_eq!(receipt.durable_voter_acks, 2);
    assert_eq!(receipt.disposition, QueueTabletDisposition::New);
    assert_eq!(
        receipt.outcome,
        QueueTabletOutcome::Applied {
            result: QueueTabletOperationResult::Enqueued {
                message_id: "job-1".into(),
                duplicate: false,
            },
        }
    );

    let encoded_receipt = serde_json::to_value(&receipt).unwrap();
    assert_eq!(
        encoded_receipt,
        json!({
            "proposal_id": "6929955191292836212",
            "tablet_id": "11",
            "tablet_epoch": "4",
            "term": "7",
            "commit_index": "13",
            "applied_at_ms": "1700000000123",
            "write_evidence": "fixed_voter_majority_persisted",
            "durable_voter_acks": 2,
            "disposition": "new",
            "outcome": {
                "status": "applied",
                "result": {
                    "kind": "enqueued",
                    "message_id": "job-1",
                    "duplicate": false
                }
            }
        })
    );

    assert_eq!(QUEUE_TABLET_COMMAND_FORMAT_VERSION, 1);
    assert_eq!(MAX_QUEUE_TABLET_COMMAND_BYTES, 512 * 1024);
    assert_eq!(MAX_QUEUE_ACQUIRE_BATCH_SIZE, 100);
    assert_eq!(MAX_QUEUE_CONSUMER_BYTES, 256);
    assert_eq!(MAX_QUEUE_REASON_BYTES, 4 * 1024);
    assert_eq!(MAX_QUEUE_MESSAGE_ID_BYTES, 1024);
}

#[test]
fn original_stream_public_goldens_remain_compatible() {
    let scope = StreamTabletScope::new(7, 3, "orders").unwrap();
    let command =
        StreamTabletCommand::append(&scope, "request-1", event("one", "order.created", 10), 11)
            .unwrap();

    assert_eq!(
        proposal_id_for(&scope, "request-1").unwrap(),
        298_544_817_787_184_225
    );
    assert_eq!(
        String::from_utf8(command.encode(&scope).unwrap()).unwrap(),
        r#"{"format_version":1,"tablet_id":7,"tablet_epoch":3,"resource":"orders","idempotency_key":"request-1","applied_at_ms":11,"operation":{"kind":"append","partition":0,"envelope":{"id":"one","source":"tests","type":"order.created","time_ms":10,"headers":{},"content_type":"application/json","payload":{"id":"one"},"priority":0,"extensions":{}}}}"#
    );

    let _: Value = serde_json::to_value(&command).unwrap();
}

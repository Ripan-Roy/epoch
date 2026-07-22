use epoch_core::EventEnvelope;
use epoch_tablet::{
    CommittedCommand, MAX_IDEMPOTENCY_KEY_BYTES, MAX_STREAM_TABLET_COMMAND_BYTES,
    STREAM_TABLET_COMMAND_FORMAT_VERSION, StreamAppendCommand, StreamTablet,
    StreamTabletAppendDisposition, StreamTabletAppendReceipt, StreamTabletCommand,
    StreamTabletOperation, StreamTabletScope, StreamTabletWriteEvidence, TabletError, TabletResult,
    proposal_id_for,
};
use serde_json::json;

#[test]
fn original_stream_tablet_root_api_remains_source_compatible() {
    let scope = StreamTabletScope::new(7, 3, "orders").unwrap();
    let mut envelope = EventEnvelope::new("tests", "order.created", json!({"id": 1}), 10);
    envelope.id = "order-1".into();
    let command = StreamTabletCommand::append(&scope, "request-1", envelope, 11).unwrap();
    let proposal_id = proposal_id_for(&scope, "request-1").unwrap();
    let payload = command.encode(&scope).unwrap();
    let committed = CommittedCommand {
        group_id: 7,
        group_epoch: 3,
        proposal_id,
        term: 2,
        log_index: 4,
        payload: &payload,
    };
    let receipt: TabletResult<StreamTabletAppendReceipt> =
        StreamTablet::new(scope).unwrap().apply(committed);

    assert_eq!(
        receipt.unwrap().write_evidence,
        StreamTabletWriteEvidence::FixedVoterMajorityPersisted
    );
    assert_eq!(STREAM_TABLET_COMMAND_FORMAT_VERSION, 1);
    assert_eq!(MAX_STREAM_TABLET_COMMAND_BYTES, 512 * 1024);
    assert_eq!(MAX_IDEMPOTENCY_KEY_BYTES, 128);

    let operation = StreamTabletOperation::Append(StreamAppendCommand {
        partition: 0,
        envelope: EventEnvelope::new("tests", "order.created", json!({}), 10),
    });
    assert!(matches!(operation, StreamTabletOperation::Append(_)));
    let disposition = StreamTabletAppendDisposition::Replayed;
    assert!(matches!(
        disposition,
        StreamTabletAppendDisposition::Replayed
    ));
    let error = TabletError::InvalidCommand("invalid".into());
    assert!(error.to_string().contains("invalid tablet command"));
}

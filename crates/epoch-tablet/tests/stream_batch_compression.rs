use epoch_core::EventEnvelope;
use epoch_tablet::{
    CommittedCommand, MAX_STREAM_BATCH_RECORDS, STREAM_TABLET_BATCH_COMMAND_FORMAT_VERSION,
    StreamBatchPayload, StreamBatchRecord, StreamCompression, StreamTablet,
    StreamTabletAppendDisposition, StreamTabletCommand, StreamTabletOperation, StreamTabletScope,
    TabletError, encode_stream_batch_payload,
};
use serde_json::{Value, json};

fn scope() -> StreamTabletScope {
    StreamTabletScope::new(7, 3, "orders").unwrap()
}

fn record(client_sequence: u32, id: &str) -> StreamBatchRecord {
    let mut envelope = EventEnvelope::new(
        "batch-tests",
        "order.created",
        json!({"id": id, "padding": "epoch-epoch-epoch-epoch"}),
        10,
    );
    id.clone_into(&mut envelope.id);
    StreamBatchRecord {
        client_sequence,
        envelope,
    }
}

fn committed(proposal_id: u64, payload: &[u8]) -> CommittedCommand<'_> {
    CommittedCommand {
        group_id: 7,
        group_epoch: 3,
        proposal_id,
        term: 2,
        log_index: 4,
        payload,
    }
}

#[test]
fn every_advertised_codec_round_trips_through_the_canonical_v2_command() {
    let records = vec![record(9, "one"), record(42, "two")];
    for compression in [
        StreamCompression::None,
        StreamCompression::Gzip,
        StreamCompression::Lz4,
        StreamCompression::Snappy,
        StreamCompression::Zstd,
    ] {
        let command = StreamTabletCommand::append_batch(
            &scope(),
            format!("batch-{compression:?}"),
            compression,
            &records,
            11,
        )
        .unwrap();
        assert_eq!(
            command.format_version,
            STREAM_TABLET_BATCH_COMMAND_FORMAT_VERSION
        );
        let encoded = command.encode(&scope()).unwrap();
        let decoded = StreamTabletCommand::decode(&encoded, &scope()).unwrap();
        assert_eq!(decoded.decode_batch_records().unwrap(), records);
        let StreamTabletOperation::AppendBatch(batch) = decoded.operation else {
            panic!("v2 command must remain a batch");
        };
        assert_eq!(batch.payload.compression, compression);
        assert_eq!(usize::from(batch.payload.record_count), records.len());
        assert!(batch.payload.uncompressed_bytes > 0);
        assert!(!batch.payload.payload_base64.is_empty());
    }
}

#[test]
fn v2_none_batch_has_a_golden_canonical_command_vector() {
    let command = StreamTabletCommand::append_batch(
        &scope(),
        "batch-golden",
        StreamCompression::None,
        &[record(1, "one")],
        11,
    )
    .unwrap();
    let encoded = command.encode(&scope()).unwrap();
    assert_eq!(
        String::from_utf8(encoded).unwrap(),
        r#"{"format_version":2,"tablet_id":7,"tablet_epoch":3,"resource":"orders","idempotency_key":"batch-golden","applied_at_ms":11,"operation":{"kind":"append_batch","partition":0,"payload":{"compression":"none","record_count":1,"uncompressed_bytes":241,"compressed_bytes":241,"payload_base64":"W3siY2xpZW50X3NlcXVlbmNlIjoxLCJlbnZlbG9wZSI6eyJpZCI6Im9uZSIsInNvdXJjZSI6ImJhdGNoLXRlc3RzIiwidHlwZSI6Im9yZGVyLmNyZWF0ZWQiLCJ0aW1lX21zIjoxMCwiaGVhZGVycyI6e30sImNvbnRlbnRfdHlwZSI6ImFwcGxpY2F0aW9uL2pzb24iLCJwYXlsb2FkIjp7ImlkIjoib25lIiwicGFkZGluZyI6ImVwb2NoLWVwb2NoLWVwb2NoLWVwb2NoIn0sInByaW9yaXR5IjowLCJleHRlbnNpb25zIjp7fX19XQ=="}}}"#
    );
}

#[test]
fn batch_apply_is_ordered_correlated_replay_safe_and_browser_safe() {
    let records = vec![record(9, "one"), record(u32::MAX, "two")];
    let command = StreamTabletCommand::append_batch(
        &scope(),
        "batch-1",
        StreamCompression::Zstd,
        &records,
        11,
    )
    .unwrap();
    let proposal_id = command.proposal_id(&scope()).unwrap();
    let payload = command.encode(&scope()).unwrap();
    let commit = committed(proposal_id, &payload);
    let mut tablet = StreamTablet::new(scope()).unwrap();

    let receipt = tablet.apply(commit).unwrap();
    let digest = tablet.state_digest();
    let batch = receipt.batch.as_ref().expect("batch evidence");
    assert_eq!(batch.compression, StreamCompression::Zstd);
    assert_eq!(batch.record_count, 2);
    assert_eq!(
        batch
            .records
            .iter()
            .map(|result| (result.client_sequence, result.offset))
            .collect::<Vec<_>>(),
        [(9, 0), (u32::MAX, 1)]
    );
    assert_eq!(tablet.fetch(0, 10).unwrap().len(), 2);

    let replay = tablet.apply(commit).unwrap();
    assert_eq!(replay.disposition, StreamTabletAppendDisposition::Replayed);
    assert_eq!(replay.batch, receipt.batch);
    assert_eq!(tablet.fetch(0, 10).unwrap().len(), 2);
    assert_eq!(tablet.state_digest(), digest);

    let document = serde_json::to_value(receipt).unwrap();
    assert_eq!(document["offset"], "0");
    assert_eq!(document["batch"]["records"][0]["offset"], "0");
    assert_eq!(document["batch"]["records"][1]["offset"], "1");
    assert_eq!(document["batch"]["records"][1]["client_sequence"], u32::MAX);
}

#[test]
fn mixed_profile_deduplication_reports_the_batch_as_new_when_any_record_is_new() {
    let mut existing = record(1, "existing").envelope;
    existing.dedupe_id = Some("dedupe-existing".into());
    let single =
        StreamTabletCommand::append(&scope(), "seed-existing", existing.clone(), 10).unwrap();
    let single_proposal_id = single.proposal_id(&scope()).unwrap();
    let single_payload = single.encode(&scope()).unwrap();
    let mut tablet = StreamTablet::new(scope()).unwrap();
    tablet
        .apply(committed(single_proposal_id, &single_payload))
        .unwrap();

    let mut duplicate = record(2, "duplicate");
    duplicate.envelope.dedupe_id = existing.dedupe_id;
    let batch = StreamTabletCommand::append_batch(
        &scope(),
        "mixed-batch",
        StreamCompression::Gzip,
        &[duplicate, record(3, "new")],
        11,
    )
    .unwrap();
    let batch_proposal_id = batch.proposal_id(&scope()).unwrap();
    let batch_payload = batch.encode(&scope()).unwrap();
    let receipt = tablet
        .apply(CommittedCommand {
            group_id: 7,
            group_epoch: 3,
            proposal_id: batch_proposal_id,
            term: 2,
            log_index: 5,
            payload: &batch_payload,
        })
        .unwrap();

    assert_eq!(receipt.disposition, StreamTabletAppendDisposition::New);
    assert_eq!(tablet.fetch(0, 10).unwrap().len(), 2);
    assert_eq!(
        receipt
            .batch
            .unwrap()
            .records
            .into_iter()
            .map(|record| record.disposition)
            .collect::<Vec<_>>(),
        [
            StreamTabletAppendDisposition::ProfileDeduplicated,
            StreamTabletAppendDisposition::New,
        ]
    );
}

#[test]
fn duplicate_client_sequences_and_out_of_bounds_batches_fail_before_encoding() {
    let duplicate = vec![record(7, "one"), record(7, "two")];
    assert!(matches!(
        StreamTabletCommand::append_batch(
            &scope(),
            "batch-duplicate",
            StreamCompression::Gzip,
            &duplicate,
            11,
        ),
        Err(TabletError::InvalidCommand(_))
    ));

    assert!(matches!(
        StreamTabletCommand::append_batch(
            &scope(),
            "batch-empty",
            StreamCompression::Gzip,
            &[],
            11,
        ),
        Err(TabletError::InvalidCommand(_))
    ));

    let oversized = (0..=MAX_STREAM_BATCH_RECORDS)
        .map(|sequence| record(u32::from(sequence), &format!("record-{sequence}")))
        .collect::<Vec<_>>();
    assert!(matches!(
        StreamTabletCommand::append_batch(
            &scope(),
            "batch-oversized",
            StreamCompression::Gzip,
            &oversized,
            11,
        ),
        Err(TabletError::InvalidCommand(_))
    ));
}

#[test]
fn malformed_or_mismatched_compressed_payloads_fail_without_state_mutation() {
    let valid = encode_stream_batch_payload(
        &[record(1, "one"), record(2, "two")],
        StreamCompression::Gzip,
    )
    .unwrap();
    let cases = [
        StreamBatchPayload {
            uncompressed_bytes: valid.uncompressed_bytes + 1,
            ..valid.clone()
        },
        StreamBatchPayload {
            record_count: valid.record_count + 1,
            ..valid.clone()
        },
        StreamBatchPayload {
            payload_base64: "not canonical base64!".to_owned(),
            ..valid
        },
    ];

    for (index, malformed) in cases.into_iter().enumerate() {
        assert!(matches!(
            StreamTabletCommand::append_compressed_batch(
                &scope(),
                format!("malformed-{index}"),
                malformed,
                11,
            ),
            Err(TabletError::InvalidCommand(_)
                | TabletError::Decoding(_)
                | TabletError::Encoding(_))
        ));
    }

    let tablet = StreamTablet::new(scope()).unwrap();
    assert!(tablet.fetch(0, 10).unwrap().is_empty());
    assert_eq!(tablet.applied_command_count(), 0);
}

#[test]
fn v1_single_record_golden_and_digest_remain_byte_for_byte_compatible() {
    let mut envelope = EventEnvelope::new("tests", "order.created", json!({"id": "one"}), 10);
    envelope.id = "one".into();
    let command = StreamTabletCommand::append(&scope(), "request-1", envelope, 11).unwrap();
    let payload = command.encode(&scope()).unwrap();
    assert_eq!(
        String::from_utf8(payload.clone()).unwrap(),
        r#"{"format_version":1,"tablet_id":7,"tablet_epoch":3,"resource":"orders","idempotency_key":"request-1","applied_at_ms":11,"operation":{"kind":"append","partition":0,"envelope":{"id":"one","source":"tests","type":"order.created","time_ms":10,"headers":{},"content_type":"application/json","payload":{"id":"one"},"priority":0,"extensions":{}}}}"#
    );
    let proposal_id = command.proposal_id(&scope()).unwrap();
    let mut tablet = StreamTablet::new(scope()).unwrap();
    let receipt = tablet.apply(committed(proposal_id, &payload)).unwrap();
    assert!(receipt.batch.is_none());
    assert_eq!(
        tablet.state_digest(),
        [
            0xc1, 0x30, 0xe8, 0x46, 0x59, 0x49, 0xd7, 0x2c, 0x4d, 0x37, 0x4d, 0x05, 0xa3, 0xb7,
            0xb2, 0x00, 0xa5, 0x85, 0x3d, 0x7c, 0xdf, 0x34, 0x55, 0xe4, 0xd6, 0xc3, 0x5a, 0x29,
            0x4f, 0x18, 0x39, 0x5f,
        ]
    );

    let value: Value = serde_json::to_value(receipt).unwrap();
    assert!(value.get("batch").is_none());
}

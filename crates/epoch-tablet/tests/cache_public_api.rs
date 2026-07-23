use epoch_cache::{CacheConfig, CacheValue, EvictionPolicy};
use epoch_core::DurabilityProfile;
use epoch_tablet::{
    CACHE_TABLET_COMMAND_FORMAT_VERSION, CacheSetCommand, CacheTablet, CacheTabletCommand,
    CacheTabletDisposition, CacheTabletOperation, CacheTabletOperationResult, CacheTabletOutcome,
    CacheTabletReceipt, CacheTabletScope, CacheTabletWriteEvidence, CommittedCommand,
    MAX_CACHE_TABLET_COMMAND_BYTES, cache_proposal_id_for,
};
use serde_json::json;

fn scope() -> CacheTabletScope {
    CacheTabletScope::new(17, 5, "sessions").unwrap()
}

fn config() -> CacheConfig {
    CacheConfig {
        max_entries: 100,
        default_ttl_ms: None,
        eviction: EvictionPolicy::NoEviction,
        durability: DurabilityProfile::QuorumDurable,
    }
}

#[test]
fn cache_set_is_usable_through_the_public_crate_api() {
    let scope = scope();
    let command = CacheTabletCommand::new(
        &scope,
        "set-request-1",
        1_700_000_000_123,
        CacheTabletOperation::Set(CacheSetCommand {
            shard: 0,
            key: "user:1".into(),
            value: CacheValue::String("ready".into()),
            ttl_ms: Some(60_000),
            lock_guard: None,
        }),
    )
    .unwrap();

    let proposal_id = command.proposal_id(&scope).unwrap();
    assert_eq!(
        cache_proposal_id_for(&scope, "set-request-1").unwrap(),
        proposal_id
    );
    let payload = command.encode(&scope).unwrap();
    assert_eq!(
        CacheTabletCommand::decode(&payload, &scope).unwrap(),
        command
    );

    let committed = CommittedCommand {
        group_id: 17,
        group_epoch: 5,
        proposal_id,
        term: 9,
        log_index: 23,
        payload: &payload,
    };
    let receipt: CacheTabletReceipt = CacheTablet::new(scope, config())
        .unwrap()
        .apply(committed)
        .unwrap();

    assert_eq!(receipt.proposal_id, proposal_id);
    assert_eq!(receipt.tablet_id, 17);
    assert_eq!(receipt.tablet_epoch, 5);
    assert_eq!(receipt.term, 9);
    assert_eq!(receipt.commit_index, 23);
    assert_eq!(receipt.applied_at_ms, 1_700_000_000_123);
    assert_eq!(
        receipt.write_evidence,
        CacheTabletWriteEvidence::FixedVoterMajorityPersisted
    );
    assert_eq!(receipt.durable_voter_acks, 2);
    assert_eq!(receipt.disposition, CacheTabletDisposition::New);
    assert_eq!(
        receipt.outcome,
        CacheTabletOutcome::Applied {
            result: CacheTabletOperationResult::Set {
                key: "user:1".into(),
                item: epoch_tablet::CacheTabletItem {
                    value: CacheValue::String("ready".into()),
                    version: 1,
                    expires_at_ms: Some(1_700_000_060_123),
                },
            },
        }
    );

    let encoded = serde_json::to_value(&receipt).unwrap();
    assert_eq!(encoded["proposal_id"], json!(proposal_id.to_string()));
    assert_eq!(encoded["tablet_id"], json!("17"));
    assert_eq!(encoded["tablet_epoch"], json!("5"));
    assert_eq!(encoded["term"], json!("9"));
    assert_eq!(encoded["commit_index"], json!("23"));
    assert_eq!(encoded["applied_at_ms"], json!("1700000000123"));
    assert_eq!(encoded["outcome"]["result"]["item"]["version"], json!("1"));
    assert_eq!(
        encoded["outcome"]["result"]["item"]["expires_at_ms"],
        json!("1700000060123")
    );

    assert_eq!(CACHE_TABLET_COMMAND_FORMAT_VERSION, 1);
    assert_eq!(MAX_CACHE_TABLET_COMMAND_BYTES, 512 * 1024);
}

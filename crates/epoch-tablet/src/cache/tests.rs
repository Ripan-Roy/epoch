use epoch_cache::{CacheConfig, CacheValue, EvictionPolicy};
use epoch_core::{DurabilityProfile, EpochError};
use serde_json::{Value, json};
use std::fmt::Write;

use super::*;

fn scope() -> CacheTabletScope {
    CacheTabletScope::new(7, 3, "sessions").unwrap()
}

fn config() -> CacheConfig {
    CacheConfig {
        max_entries: 100,
        default_ttl_ms: None,
        eviction: EvictionPolicy::NoEviction,
        durability: DurabilityProfile::QuorumDurable,
    }
}

fn command(key: &str, applied_at_ms: u64, operation: CacheTabletOperation) -> CacheTabletCommand {
    CacheTabletCommand::new(&scope(), key, applied_at_ms, operation).unwrap()
}

fn committed(proposal_id: u64, term: u64, log_index: u64, payload: &[u8]) -> CommittedCommand<'_> {
    CommittedCommand {
        group_id: 7,
        group_epoch: 3,
        proposal_id,
        term,
        log_index,
        payload,
    }
}

fn apply_command(
    tablet: &mut CacheTablet,
    command: &CacheTabletCommand,
    term: u64,
    log_index: u64,
) -> CacheTabletReceipt {
    let payload = command.encode(&scope()).unwrap();
    tablet
        .apply(committed(
            command.proposal_id(&scope()).unwrap(),
            term,
            log_index,
            &payload,
        ))
        .unwrap()
}

fn apply_all(
    tablets: &mut [CacheTablet; 3],
    command: &CacheTabletCommand,
    term: u64,
    log_index: u64,
) -> Vec<CacheTabletReceipt> {
    let payload = command.encode(&scope()).unwrap();
    let proposal_id = command.proposal_id(&scope()).unwrap();
    tablets
        .iter_mut()
        .map(|tablet| {
            tablet
                .apply(committed(proposal_id, term, log_index, &payload))
                .unwrap()
        })
        .collect()
}

fn assert_rejected(receipt: &CacheTabletReceipt, expected: CacheTabletRejectionCode) {
    assert!(matches!(
        &receipt.outcome,
        CacheTabletOutcome::Rejected { code, .. } if *code == expected
    ));
}

fn set(key: &str, value: CacheValue) -> CacheTabletOperation {
    CacheTabletOperation::Set(CacheSetCommand {
        shard: 0,
        key: key.into(),
        value,
        ttl_ms: None,
        lock_guard: None,
    })
}

fn acquired_token(receipt: &CacheTabletReceipt) -> String {
    let CacheTabletOutcome::Applied {
        result: CacheTabletOperationResult::LockAcquired { lease_token, .. },
    } = &receipt.outcome
    else {
        panic!("expected lock acquisition: {receipt:?}")
    };
    lease_token.clone()
}

fn hex_digest(digest: [u8; 32]) -> String {
    digest.iter().fold(String::new(), |mut encoded, byte| {
        write!(encoded, "{byte:02x}").unwrap();
        encoded
    })
}

#[test]
fn command_codec_is_strict_canonical_bounded_and_golden() {
    let command = command(
        "request-1",
        11,
        CacheTabletOperation::Set(CacheSetCommand {
            shard: 0,
            key: "user:1".into(),
            value: CacheValue::String("ready".into()),
            ttl_ms: Some(50),
            lock_guard: None,
        }),
    );
    let encoded = command.encode(&scope()).unwrap();
    assert_eq!(
        String::from_utf8(encoded.clone()).unwrap(),
        r#"{"format_version":1,"tablet_id":7,"tablet_epoch":3,"resource":"sessions","idempotency_key":"request-1","applied_at_ms":11,"operation":{"kind":"set","shard":0,"key":"user:1","value":{"kind":"string","value":"ready"},"ttl_ms":50,"lock_guard":null}}"#
    );

    let pretty = serde_json::to_vec_pretty(&command).unwrap();
    assert!(matches!(
        CacheTabletCommand::decode(&pretty, &scope()),
        Err(TabletError::Decoding(_))
    ));

    let mut document: Value = serde_json::from_slice(&encoded).unwrap();
    document["operation"]["unknown"] = json!(true);
    assert!(matches!(
        CacheTabletCommand::decode(&serde_json::to_vec(&document).unwrap(), &scope()),
        Err(TabletError::Decoding(_))
    ));
    assert!(matches!(
        CacheTabletCommand::decode(&vec![b'x'; MAX_CACHE_TABLET_COMMAND_BYTES + 1], &scope()),
        Err(TabletError::InvalidCommand(_))
    ));

    assert!(matches!(
        CacheTabletCommand::new(
            &scope(),
            "wrong-shard",
            12,
            CacheTabletOperation::Maintain(CacheMaintainCommand {
                shard: 1,
                max_expirations: 1,
            }),
        ),
        Err(TabletError::InvalidCommand(_))
    ));
}

#[test]
fn nested_and_non_set_operation_unknown_fields_are_rejected() {
    let transaction = command(
        "nested-schema",
        10,
        CacheTabletOperation::Transaction(CacheTransactionCommand {
            shard: 0,
            expected_revision: 0,
            mutations: vec![CacheTransactionMutation::CompareAndSet {
                key: "key".into(),
                expected: CacheCasExpectation::Missing { shard_revision: 0 },
                value: CacheValue::String("value".into()),
                ttl_ms: None,
            }],
            lock_guards: Vec::new(),
        }),
    );
    let encoded = transaction.encode(&scope()).unwrap();

    let mut unknown_expectation: Value = serde_json::from_slice(&encoded).unwrap();
    unknown_expectation["operation"]["mutations"][0]["expected"]["typo"] = json!(true);
    assert!(matches!(
        CacheTabletCommand::decode(&serde_json::to_vec(&unknown_expectation).unwrap(), &scope(),),
        Err(TabletError::Decoding(_))
    ));

    let mut unknown_mutation: Value = serde_json::from_slice(&encoded).unwrap();
    unknown_mutation["operation"]["mutations"][0]["typo"] = json!(true);
    assert!(matches!(
        CacheTabletCommand::decode(&serde_json::to_vec(&unknown_mutation).unwrap(), &scope()),
        Err(TabletError::Decoding(_))
    ));

    let acquire = command(
        "operation-schema",
        11,
        CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
            shard: 0,
            lock_key: "lock".into(),
            owner: "owner".into(),
            owner_epoch: 1,
            lease_ms: 10,
        }),
    );
    let mut unknown_operation: Value =
        serde_json::from_slice(&acquire.encode(&scope()).unwrap()).unwrap();
    unknown_operation["operation"]["typo"] = json!(true);
    assert!(matches!(
        CacheTabletCommand::decode(&serde_json::to_vec(&unknown_operation).unwrap(), &scope()),
        Err(TabletError::Decoding(_))
    ));
}

#[test]
fn command_validation_enforces_nested_key_value_collection_and_duration_bounds() {
    let too_long_key = "k".repeat(MAX_CACHE_KEY_BYTES + 1);
    assert!(matches!(
        CacheTabletCommand::new(
            &scope(),
            "long-key",
            1,
            set(&too_long_key, CacheValue::Counter(1)),
        ),
        Err(TabletError::InvalidCommand(_))
    ));

    let too_many = (0..=MAX_CACHE_COLLECTION_ENTRIES)
        .map(|index| format!("member-{index}"))
        .collect();
    assert!(matches!(
        CacheTabletCommand::new(
            &scope(),
            "large-collection",
            1,
            set("set", CacheValue::Set(too_many)),
        ),
        Err(TabletError::InvalidCommand(_))
    ));

    let oversized_member = "m".repeat(MAX_CACHE_MEMBER_BYTES + 1);
    assert!(matches!(
        CacheTabletCommand::new(
            &scope(),
            "large-member",
            1,
            set("list", CacheValue::List(vec![oversized_member]),),
        ),
        Err(TabletError::InvalidCommand(_))
    ));

    let oversized_value = "v".repeat(MAX_CACHE_VALUE_BYTES);
    assert!(matches!(
        CacheTabletCommand::new(
            &scope(),
            "large-value",
            1,
            set("value", CacheValue::String(oversized_value)),
        ),
        Err(TabletError::InvalidCommand(_))
    ));

    for ttl_ms in [0, MAX_CACHE_TTL_MS + 1] {
        assert!(matches!(
            CacheTabletCommand::new(
                &scope(),
                format!("ttl-{ttl_ms}"),
                1,
                CacheTabletOperation::Set(CacheSetCommand {
                    shard: 0,
                    key: "key".into(),
                    value: CacheValue::Counter(1),
                    ttl_ms: Some(ttl_ms),
                    lock_guard: None,
                }),
            ),
            Err(TabletError::InvalidCommand(_))
        ));
    }
    for lease_ms in [0, MAX_CACHE_LOCK_LEASE_MS + 1] {
        assert!(matches!(
            CacheTabletCommand::new(
                &scope(),
                format!("lease-{lease_ms}"),
                1,
                CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
                    shard: 0,
                    lock_key: "lock".into(),
                    owner: "owner".into(),
                    owner_epoch: 1,
                    lease_ms,
                }),
            ),
            Err(TabletError::InvalidCommand(_))
        ));
    }
    assert!(matches!(
        CacheTabletCommand::new(
            &scope(),
            "zero-maintain",
            1,
            CacheTabletOperation::Maintain(CacheMaintainCommand {
                shard: 0,
                max_expirations: 0,
            }),
        ),
        Err(TabletError::InvalidCommand(_))
    ));
    assert!(matches!(
        CacheTabletCommand::new(
            &scope(),
            "large-maintain",
            1,
            CacheTabletOperation::Maintain(CacheMaintainCommand {
                shard: 0,
                max_expirations: MAX_CACHE_MAINTENANCE_EXPIRATIONS + 1,
            }),
        ),
        Err(TabletError::InvalidCommand(_))
    ));
}

#[test]
fn config_validation_and_normalization_are_pinned() {
    let mut volatile = config();
    volatile.durability = DurabilityProfile::Volatile;
    let volatile = CacheTablet::new(scope(), volatile).unwrap();
    let quorum = CacheTablet::new(scope(), config()).unwrap();
    assert_eq!(volatile.state_digest(), quorum.state_digest());
    assert_eq!(
        volatile.cache_recovery_state_digest(),
        quorum.cache_recovery_state_digest()
    );

    let mut changed_ttl = config();
    changed_ttl.default_ttl_ms = Some(1);
    assert_ne!(
        CacheTablet::new(scope(), changed_ttl)
            .unwrap()
            .state_digest(),
        quorum.state_digest()
    );

    let mut changed_capacity = config();
    changed_capacity.max_entries += 1;
    assert_ne!(
        CacheTablet::new(scope(), changed_capacity)
            .unwrap()
            .state_digest(),
        quorum.state_digest()
    );
}

#[test]
fn invalid_cache_configs_fail_before_tablet_construction() {
    for max_entries in [0, MAX_CACHE_TABLET_ENTRIES + 1] {
        let mut invalid = config();
        invalid.max_entries = max_entries;
        assert!(matches!(
            CacheTablet::new(scope(), invalid),
            Err(TabletError::Profile(EpochError::InvalidArgument(_)))
        ));
    }
    for default_ttl_ms in [0, MAX_CACHE_TTL_MS + 1] {
        let mut invalid = config();
        invalid.default_ttl_ms = Some(default_ttl_ms);
        assert!(matches!(
            CacheTablet::new(scope(), invalid),
            Err(TabletError::Profile(EpochError::InvalidArgument(_)))
        ));
    }
    let mut invalid = config();
    invalid.eviction = EvictionPolicy::VolatileLru;
    assert!(matches!(
        CacheTablet::new(scope(), invalid),
        Err(TabletError::Profile(EpochError::InvalidArgument(_)))
    ));
}

#[test]
fn set_cas_increment_delete_and_atomic_transaction_converge() {
    let mut tablets = [
        CacheTablet::new(scope(), config()).unwrap(),
        CacheTablet::new(scope(), config()).unwrap(),
        CacheTablet::new(scope(), config()).unwrap(),
    ];
    let first = command("set", 10, set("counter", CacheValue::Counter(1)));
    apply_all(&mut tablets, &first, 2, 1);

    let cas = command(
        "cas",
        11,
        CacheTabletOperation::CompareAndSet(CacheCompareAndSetCommand {
            shard: 0,
            key: "counter".into(),
            expected: CacheCasExpectation::Version { version: 1 },
            value: CacheValue::Counter(2),
            ttl_ms: None,
            lock_guard: None,
        }),
    );
    apply_all(&mut tablets, &cas, 2, 2);

    let transaction = command(
        "transaction",
        12,
        CacheTabletOperation::Transaction(CacheTransactionCommand {
            shard: 0,
            expected_revision: 2,
            mutations: vec![
                CacheTransactionMutation::Increment {
                    key: "counter".into(),
                    delta: 3,
                    expected_version: Some(2),
                    ttl_ms: None,
                },
                CacheTransactionMutation::Set {
                    key: "state".into(),
                    value: CacheValue::String("committed".into()),
                    ttl_ms: None,
                },
            ],
            lock_guards: Vec::new(),
        }),
    );
    let receipts = apply_all(&mut tablets, &transaction, 2, 3);
    assert!(receipts.windows(2).all(|pair| pair[0] == pair[1]));

    let delete = command(
        "delete",
        13,
        CacheTabletOperation::Delete(CacheDeleteCommand {
            shard: 0,
            key: "state".into(),
            expected_version: Some(3),
            lock_guard: None,
        }),
    );
    apply_all(&mut tablets, &delete, 2, 4);

    for tablet in &tablets {
        assert_eq!(tablet.cache_revision(), 4);
        assert_eq!(
            tablet.observe("counter").item.unwrap().value,
            CacheValue::Counter(5)
        );
        assert!(tablet.observe("state").item.is_none());
    }
    assert!(
        tablets
            .windows(2)
            .all(|pair| pair[0].state_digest() == pair[1].state_digest())
    );
}

#[test]
fn transaction_revision_and_operation_failure_are_recorded_without_partial_state() {
    let mut tablet = CacheTablet::new(scope(), config()).unwrap();
    apply_command(
        &mut tablet,
        &command("seed", 1, set("counter", CacheValue::Counter(i64::MAX))),
        2,
        1,
    );
    let before = tablet.state_digest();

    let stale = command(
        "stale-transaction",
        2,
        CacheTabletOperation::Transaction(CacheTransactionCommand {
            shard: 0,
            expected_revision: 0,
            mutations: vec![CacheTransactionMutation::Set {
                key: "must-not-exist".into(),
                value: CacheValue::String("bad".into()),
                ttl_ms: None,
            }],
            lock_guards: Vec::new(),
        }),
    );
    assert_rejected(
        &apply_command(&mut tablet, &stale, 2, 2),
        CacheTabletRejectionCode::Conflict,
    );
    assert!(tablet.observe("must-not-exist").item.is_none());
    assert_eq!(tablet.cache_revision(), 1);
    assert_ne!(tablet.state_digest(), before);

    let overflow = command(
        "overflow-transaction",
        3,
        CacheTabletOperation::Transaction(CacheTransactionCommand {
            shard: 0,
            expected_revision: 1,
            mutations: vec![
                CacheTransactionMutation::Set {
                    key: "new".into(),
                    value: CacheValue::String("rollback".into()),
                    ttl_ms: None,
                },
                CacheTransactionMutation::Increment {
                    key: "counter".into(),
                    delta: 1,
                    expected_version: Some(1),
                    ttl_ms: None,
                },
            ],
            lock_guards: Vec::new(),
        }),
    );
    assert_rejected(
        &apply_command(&mut tablet, &overflow, 2, 3),
        CacheTabletRejectionCode::Capacity,
    );
    assert!(tablet.observe("new").item.is_none());
    assert_eq!(tablet.cache_revision(), 1);
}

#[test]
fn missing_cas_revision_prevents_absent_create_delete_aba() {
    let mut tablet = CacheTablet::new(scope(), config()).unwrap();
    assert_eq!(tablet.observe("key").shard_revision, 0);
    apply_command(
        &mut tablet,
        &command(
            "create",
            1,
            set("key", CacheValue::String("temporary".into())),
        ),
        2,
        1,
    );
    apply_command(
        &mut tablet,
        &command(
            "delete",
            2,
            CacheTabletOperation::Delete(CacheDeleteCommand {
                shard: 0,
                key: "key".into(),
                expected_version: Some(1),
                lock_guard: None,
            }),
        ),
        2,
        2,
    );
    assert!(tablet.observe("key").item.is_none());

    let stale_missing = command(
        "stale-missing",
        3,
        CacheTabletOperation::CompareAndSet(CacheCompareAndSetCommand {
            shard: 0,
            key: "key".into(),
            expected: CacheCasExpectation::Missing { shard_revision: 0 },
            value: CacheValue::String("stale".into()),
            ttl_ms: None,
            lock_guard: None,
        }),
    );
    assert_rejected(
        &apply_command(&mut tablet, &stale_missing, 2, 3),
        CacheTabletRejectionCode::Conflict,
    );
    assert!(tablet.observe("key").item.is_none());

    let current_missing = command(
        "current-missing",
        4,
        CacheTabletOperation::CompareAndSet(CacheCompareAndSetCommand {
            shard: 0,
            key: "key".into(),
            expected: CacheCasExpectation::Missing { shard_revision: 2 },
            value: CacheValue::String("current".into()),
            ttl_ms: None,
            lock_guard: None,
        }),
    );
    apply_command(&mut tablet, &current_missing, 2, 4);
    assert_eq!(
        tablet.observe("key").item.unwrap().value,
        CacheValue::String("current".into())
    );
}

#[test]
fn missing_increment_uses_explicit_ttl_while_existing_increment_preserves_it() {
    let mut tablet = CacheTablet::new(scope(), config()).unwrap();
    let create = command(
        "increment-create",
        100,
        CacheTabletOperation::Increment(CacheIncrementCommand {
            shard: 0,
            key: "counter".into(),
            delta: 2,
            expected_version: Some(0),
            ttl_ms: Some(10),
            lock_guard: None,
        }),
    );
    apply_command(&mut tablet, &create, 2, 1);
    let created = tablet.observe("counter").item.unwrap();
    assert_eq!(created.value, CacheValue::Counter(2));
    assert_eq!(created.expires_at_ms, Some(110));

    let existing = command(
        "increment-existing",
        101,
        CacheTabletOperation::Increment(CacheIncrementCommand {
            shard: 0,
            key: "counter".into(),
            delta: 3,
            expected_version: Some(1),
            ttl_ms: Some(1),
            lock_guard: None,
        }),
    );
    apply_command(&mut tablet, &existing, 2, 2);
    let incremented = tablet.observe("counter").item.unwrap();
    assert_eq!(incremented.value, CacheValue::Counter(5));
    assert_eq!(incremented.version, 2);
    assert_eq!(incremented.expires_at_ms, Some(110));

    let transaction = command(
        "increment-transaction",
        102,
        CacheTabletOperation::Transaction(CacheTransactionCommand {
            shard: 0,
            expected_revision: 2,
            mutations: vec![CacheTransactionMutation::Increment {
                key: "other".into(),
                delta: 7,
                expected_version: Some(0),
                ttl_ms: Some(5),
            }],
            lock_guards: Vec::new(),
        }),
    );
    apply_command(&mut tablet, &transaction, 2, 3);
    let other = tablet.observe("other").item.unwrap();
    assert_eq!(other.value, CacheValue::Counter(7));
    assert_eq!(other.version, 3);
    assert_eq!(other.expires_at_ms, Some(107));
}

#[test]
fn no_eviction_capacity_failure_rolls_back_the_complete_transaction() {
    let mut bounded_config = config();
    bounded_config.max_entries = 1;
    let mut tablet = CacheTablet::new(scope(), bounded_config).unwrap();
    let transaction = command(
        "over-capacity",
        1,
        CacheTabletOperation::Transaction(CacheTransactionCommand {
            shard: 0,
            expected_revision: 0,
            mutations: vec![
                CacheTransactionMutation::Set {
                    key: "a".into(),
                    value: CacheValue::Counter(1),
                    ttl_ms: None,
                },
                CacheTransactionMutation::Set {
                    key: "b".into(),
                    value: CacheValue::Counter(2),
                    ttl_ms: None,
                },
            ],
            lock_guards: Vec::new(),
        }),
    );
    assert_rejected(
        &apply_command(&mut tablet, &transaction, 2, 1),
        CacheTabletRejectionCode::Capacity,
    );
    assert_eq!(tablet.cache_revision(), 0);
    assert_eq!(tablet.cache_entry_count(), 0);

    apply_command(
        &mut tablet,
        &command("keep", 2, set("keep", CacheValue::String("safe".into()))),
        2,
        2,
    );
    let replacement = command(
        "must-not-evict",
        3,
        set("other", CacheValue::String("unsafe".into())),
    );
    assert_rejected(
        &apply_command(&mut tablet, &replacement, 2, 3),
        CacheTabletRejectionCode::Capacity,
    );
    assert_eq!(
        tablet.observe("keep").item.unwrap().value,
        CacheValue::String("safe".into())
    );
    assert!(tablet.observe("other").item.is_none());
}

#[test]
fn committed_order_clamps_time_for_ttl_and_maintenance() {
    let mut tablet = CacheTablet::new(scope(), config()).unwrap();
    let set_command = command(
        "set-with-ttl",
        100,
        CacheTabletOperation::Set(CacheSetCommand {
            shard: 0,
            key: "short".into(),
            value: CacheValue::String("value".into()),
            ttl_ms: Some(10),
            lock_guard: None,
        }),
    );
    apply_command(&mut tablet, &set_command, 2, 1);

    let rollback = command(
        "maintain-before-deadline",
        1,
        CacheTabletOperation::Maintain(CacheMaintainCommand {
            shard: 0,
            max_expirations: 10,
        }),
    );
    let receipt = apply_command(&mut tablet, &rollback, 2, 2);
    assert_eq!(receipt.applied_at_ms, 100);
    assert!(tablet.observe("short").item.is_some());

    let expire = command(
        "maintain-at-deadline",
        110,
        CacheTabletOperation::Maintain(CacheMaintainCommand {
            shard: 0,
            max_expirations: 10,
        }),
    );
    apply_command(&mut tablet, &expire, 2, 3);
    assert!(tablet.observe("short").item.is_none());

    let rollback_again = command("rollback-set", 2, set("later", CacheValue::Counter(1)));
    let receipt = apply_command(&mut tablet, &rollback_again, 2, 4);
    assert_eq!(receipt.applied_at_ms, 110);
    assert_eq!(tablet.last_applied_time_ms(), 110);
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one sequential lifecycle test keeps acquisition, renewal, replay, stale-token rejection, release, and reacquisition evidence together"
)]
fn lock_lifecycle_rotates_tokens_and_reacquisition_increases_fence() {
    let mut tablet = CacheTablet::new(scope(), config()).unwrap();
    let acquire = command(
        "acquire",
        10,
        CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
            shard: 0,
            lock_key: "account:1".into(),
            owner: "worker".into(),
            owner_epoch: 1,
            lease_ms: 10,
        }),
    );
    let acquired = apply_command(&mut tablet, &acquire, 2, 1);
    let CacheTabletOutcome::Applied {
        result:
            CacheTabletOperationResult::LockAcquired {
                fencing_token,
                lease_token,
                lease_deadline_ms,
                ..
            },
    } = acquired.outcome
    else {
        panic!("expected lock acquisition")
    };
    assert_eq!(
        fencing_token,
        CacheLockFencingToken {
            tablet_epoch: 3,
            acquisition_index: 1,
        }
    );
    assert_eq!(lease_deadline_ms, 20);
    let metadata = CacheLockTokenMetadata::parse(&lease_token).unwrap();
    assert_eq!(metadata.lock_key(), "account:1");
    assert_eq!(metadata.owner(), "worker");
    assert_eq!(metadata.acquisition_index(), 1);
    assert_eq!(metadata.lease_generation(), 1);

    let renew = command(
        "renew",
        11,
        CacheTabletOperation::RenewLock(CacheRenewLockCommand {
            shard: 0,
            lock_key: "account:1".into(),
            owner: "worker".into(),
            owner_epoch: 1,
            lease_token: lease_token.clone(),
            extension_ms: 20,
        }),
    );
    let renewed = apply_command(&mut tablet, &renew, 2, 2);
    let renewed_json = serde_json::to_value(&renewed).unwrap();
    assert_eq!(
        renewed_json["outcome"]["result"]["lease_generation"],
        json!("2")
    );
    assert_eq!(
        renewed_json["outcome"]["result"]["lease_deadline_ms"],
        json!("31")
    );
    assert_eq!(
        renewed_json["outcome"]["result"]["fencing_token"]["tablet_epoch"],
        json!("3")
    );
    assert_eq!(
        renewed_json["outcome"]["result"]["fencing_token"]["acquisition_index"],
        json!("1")
    );
    let CacheTabletOutcome::Applied {
        result:
            CacheTabletOperationResult::LockRenewed {
                fencing_token: renewed_fence,
                lease_token: renewed_token,
                lease_generation,
                lease_deadline_ms: renewed_deadline,
                ..
            },
    } = renewed.outcome
    else {
        panic!("expected lock renewal")
    };
    assert_eq!(renewed_fence, fencing_token);
    assert_ne!(renewed_token, lease_token);
    assert_eq!(lease_generation, 2);
    assert_eq!(
        CacheLockTokenMetadata::parse(&renewed_token)
            .unwrap()
            .lease_generation(),
        2
    );
    assert_eq!(renewed_deadline, 31);

    let replayed = apply_command(&mut tablet, &renew, 2, 2);
    assert_eq!(replayed.disposition, CacheTabletDisposition::Replayed);
    assert!(matches!(
        replayed.outcome,
        CacheTabletOutcome::Applied {
            result: CacheTabletOperationResult::LockRenewed {
                lease_token,
                lease_generation: 2,
                ..
            }
        } if lease_token == renewed_token
    ));

    let stale_release = command(
        "stale-release",
        12,
        CacheTabletOperation::ReleaseLock(CacheReleaseLockCommand {
            shard: 0,
            lock_key: "account:1".into(),
            owner: "worker".into(),
            owner_epoch: 1,
            lease_token,
        }),
    );
    assert_rejected(
        &apply_command(&mut tablet, &stale_release, 2, 3),
        CacheTabletRejectionCode::Fenced,
    );

    let release = command(
        "release",
        13,
        CacheTabletOperation::ReleaseLock(CacheReleaseLockCommand {
            shard: 0,
            lock_key: "account:1".into(),
            owner: "worker".into(),
            owner_epoch: 1,
            lease_token: renewed_token,
        }),
    );
    apply_command(&mut tablet, &release, 2, 4);

    let reacquire = command(
        "reacquire",
        14,
        CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
            shard: 0,
            lock_key: "account:1".into(),
            owner: "worker-2".into(),
            owner_epoch: 1,
            lease_ms: 10,
        }),
    );
    let receipt = apply_command(&mut tablet, &reacquire, 2, 5);
    assert!(matches!(
        receipt.outcome,
        CacheTabletOutcome::Applied {
            result: CacheTabletOperationResult::LockAcquired {
                fencing_token: CacheLockFencingToken {
                    acquisition_index: 5,
                    ..
                },
                ..
            }
        }
    ));
}

#[test]
fn malformed_corrupt_owner_mismatch_and_held_lock_requests_are_fenced_or_conflicted() {
    let mut tablet = CacheTablet::new(scope(), config()).unwrap();
    let acquire = command(
        "acquire",
        10,
        CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
            shard: 0,
            lock_key: "protected".into(),
            owner: "owner".into(),
            owner_epoch: 1,
            lease_ms: 50,
        }),
    );
    let receipt = apply_command(&mut tablet, &acquire, 2, 1);
    let token = acquired_token(&receipt);

    let contended = command(
        "contended",
        11,
        CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
            shard: 0,
            lock_key: "protected".into(),
            owner: "other".into(),
            owner_epoch: 1,
            lease_ms: 10,
        }),
    );
    assert_rejected(
        &apply_command(&mut tablet, &contended, 2, 2),
        CacheTabletRejectionCode::Conflict,
    );

    let malformed = command(
        "malformed",
        12,
        CacheTabletOperation::Set(CacheSetCommand {
            shard: 0,
            key: "value".into(),
            value: CacheValue::Counter(1),
            ttl_ms: None,
            lock_guard: Some(CacheLockGuard {
                lock_key: "protected".into(),
                owner: "owner".into(),
                owner_epoch: 1,
                lease_token: "not-a-token".into(),
            }),
        }),
    );
    assert_rejected(
        &apply_command(&mut tablet, &malformed, 2, 3),
        CacheTabletRejectionCode::Fenced,
    );

    let mut corrupted = token.clone();
    let replacement = if corrupted.ends_with('0') { '1' } else { '0' };
    corrupted.pop();
    corrupted.push(replacement);
    assert!(CacheLockTokenMetadata::parse(&corrupted).is_err());
    let corrupt_guard = command(
        "corrupt",
        13,
        CacheTabletOperation::Set(CacheSetCommand {
            shard: 0,
            key: "value".into(),
            value: CacheValue::Counter(1),
            ttl_ms: None,
            lock_guard: Some(CacheLockGuard {
                lock_key: "protected".into(),
                owner: "owner".into(),
                owner_epoch: 1,
                lease_token: corrupted,
            }),
        }),
    );
    assert_rejected(
        &apply_command(&mut tablet, &corrupt_guard, 2, 4),
        CacheTabletRejectionCode::Fenced,
    );

    let owner_mismatch = command(
        "owner-mismatch",
        14,
        CacheTabletOperation::Set(CacheSetCommand {
            shard: 0,
            key: "value".into(),
            value: CacheValue::Counter(1),
            ttl_ms: None,
            lock_guard: Some(CacheLockGuard {
                lock_key: "protected".into(),
                owner: "impostor".into(),
                owner_epoch: 1,
                lease_token: token,
            }),
        }),
    );
    assert_rejected(
        &apply_command(&mut tablet, &owner_mismatch, 2, 5),
        CacheTabletRejectionCode::Fenced,
    );
    assert!(tablet.observe("value").item.is_none());
}

#[test]
fn committed_lock_history_and_guarded_mutation_converge_on_three_tablets() {
    let mut tablets = [
        CacheTablet::new(scope(), config()).unwrap(),
        CacheTablet::new(scope(), config()).unwrap(),
        CacheTablet::new(scope(), config()).unwrap(),
    ];
    let acquire = command(
        "convergent-acquire",
        10,
        CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
            shard: 0,
            lock_key: "protected".into(),
            owner: "worker".into(),
            owner_epoch: 1,
            lease_ms: 20,
        }),
    );
    let acquired = apply_all(&mut tablets, &acquire, 2, 1);
    assert!(acquired.windows(2).all(|pair| pair[0] == pair[1]));
    let token = acquired_token(&acquired[0]);

    let renew = command(
        "convergent-renew",
        11,
        CacheTabletOperation::RenewLock(CacheRenewLockCommand {
            shard: 0,
            lock_key: "protected".into(),
            owner: "worker".into(),
            owner_epoch: 1,
            lease_token: token,
            extension_ms: 30,
        }),
    );
    let renewed = apply_all(&mut tablets, &renew, 2, 2);
    assert!(renewed.windows(2).all(|pair| pair[0] == pair[1]));
    let CacheTabletOutcome::Applied {
        result: CacheTabletOperationResult::LockRenewed { lease_token, .. },
    } = &renewed[0].outcome
    else {
        panic!("expected convergent renewal")
    };
    let guarded = command(
        "convergent-guarded-set",
        12,
        CacheTabletOperation::Set(CacheSetCommand {
            shard: 0,
            key: "value".into(),
            value: CacheValue::String("safe".into()),
            ttl_ms: None,
            lock_guard: Some(CacheLockGuard {
                lock_key: "protected".into(),
                owner: "worker".into(),
                owner_epoch: 1,
                lease_token: lease_token.clone(),
            }),
        }),
    );
    let guarded_receipts = apply_all(&mut tablets, &guarded, 2, 3);
    assert!(guarded_receipts.windows(2).all(|pair| pair[0] == pair[1]));
    assert!(tablets.windows(2).all(|pair| {
        pair[0].state_digest() == pair[1].state_digest()
            && pair[0].observe("value") == pair[1].observe("value")
            && pair[0].active_lock_count() == pair[1].active_lock_count()
    }));
    assert_eq!(tablets[0].active_lock_count(), 1);
    assert_eq!(
        tablets[0].observe("value").item.unwrap().value,
        CacheValue::String("safe".into())
    );
}

#[test]
fn lock_maintenance_is_bounded_and_orders_deadline_then_key() {
    let mut tablet = CacheTablet::new(scope(), config()).unwrap();
    for (index, lock_key, lease_ms) in [(1, "b", 5), (2, "a", 5), (3, "first", 4)] {
        let acquire = command(
            &format!("acquire-{lock_key}"),
            0,
            CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
                shard: 0,
                lock_key: lock_key.into(),
                owner: format!("owner-{lock_key}"),
                owner_epoch: 1,
                lease_ms,
            }),
        );
        apply_command(&mut tablet, &acquire, 2, index);
    }

    let maintain = command(
        "maintain-1",
        5,
        CacheTabletOperation::Maintain(CacheMaintainCommand {
            shard: 0,
            max_expirations: 2,
        }),
    );
    let receipt = apply_command(&mut tablet, &maintain, 2, 4);
    assert!(matches!(
        receipt.outcome,
        CacheTabletOutcome::Applied {
            result: CacheTabletOperationResult::Maintained {
                expired_locks,
                ..
            }
        } if expired_locks == ["first", "a"]
    ));
    assert_eq!(tablet.active_lock_count(), 0);

    let maintain = command(
        "maintain-2",
        5,
        CacheTabletOperation::Maintain(CacheMaintainCommand {
            shard: 0,
            max_expirations: 2,
        }),
    );
    let receipt = apply_command(&mut tablet, &maintain, 2, 5);
    assert!(matches!(
        receipt.outcome,
        CacheTabletOutcome::Applied {
            result: CacheTabletOperationResult::Maintained {
                expired_locks,
                ..
            }
        } if expired_locks == ["b"]
    ));
}

#[test]
fn owner_epoch_capacity_is_reclaimed_and_expired_other_lock_does_not_block_acquire() {
    let mut bounded_config = config();
    bounded_config.max_entries = 1;
    let mut tablet = CacheTablet::new(scope(), bounded_config).unwrap();
    let mut log_index = 0;

    for owner_number in 0..3 {
        log_index += 1;
        let owner = format!("owner-{owner_number}");
        let lock_key = format!("lock-{owner_number}");
        let acquire = command(
            &format!("acquire-{owner_number}"),
            u64::try_from(owner_number).unwrap(),
            CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
                shard: 0,
                lock_key: lock_key.clone(),
                owner: owner.clone(),
                owner_epoch: 1,
                lease_ms: 10,
            }),
        );
        let receipt = apply_command(&mut tablet, &acquire, 2, log_index);
        let token = acquired_token(&receipt);

        log_index += 1;
        let release = command(
            &format!("release-{owner_number}"),
            u64::try_from(owner_number).unwrap(),
            CacheTabletOperation::ReleaseLock(CacheReleaseLockCommand {
                shard: 0,
                lock_key,
                owner,
                owner_epoch: 1,
                lease_token: token,
            }),
        );
        apply_command(&mut tablet, &release, 2, log_index);
    }

    log_index += 1;
    let old = command(
        "old-lock",
        10,
        CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
            shard: 0,
            lock_key: "old".into(),
            owner: "old-owner".into(),
            owner_epoch: 1,
            lease_ms: 1,
        }),
    );
    apply_command(&mut tablet, &old, 2, log_index);

    log_index += 1;
    let replacement = command(
        "replacement-lock",
        11,
        CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
            shard: 0,
            lock_key: "new".into(),
            owner: "new-owner".into(),
            owner_epoch: 1,
            lease_ms: 10,
        }),
    );
    let receipt = apply_command(&mut tablet, &replacement, 2, log_index);
    assert!(matches!(
        receipt.outcome,
        CacheTabletOutcome::Applied {
            result: CacheTabletOperationResult::LockAcquired { lock_key, .. }
        } if lock_key == "new"
    ));
}

#[test]
fn owner_epoch_high_water_is_scoped_to_active_locks() {
    let mut tablet = CacheTablet::new(scope(), config()).unwrap();
    let acquire = command(
        "active-owner-10",
        1,
        CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
            shard: 0,
            lock_key: "first".into(),
            owner: "worker".into(),
            owner_epoch: 10,
            lease_ms: 100,
        }),
    );
    let acquired = apply_command(&mut tablet, &acquire, 2, 1);
    let lease_token = acquired_token(&acquired);

    let stale_while_active = command(
        "active-owner-1",
        2,
        CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
            shard: 0,
            lock_key: "second".into(),
            owner: "worker".into(),
            owner_epoch: 1,
            lease_ms: 100,
        }),
    );
    assert_rejected(
        &apply_command(&mut tablet, &stale_while_active, 2, 2),
        CacheTabletRejectionCode::Fenced,
    );

    let release = command(
        "release-owner-10",
        3,
        CacheTabletOperation::ReleaseLock(CacheReleaseLockCommand {
            shard: 0,
            lock_key: "first".into(),
            owner: "worker".into(),
            owner_epoch: 10,
            lease_token,
        }),
    );
    apply_command(&mut tablet, &release, 2, 3);

    let reacquire = command(
        "inactive-owner-1",
        4,
        CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
            shard: 0,
            lock_key: "second".into(),
            owner: "worker".into(),
            owner_epoch: 1,
            lease_ms: 100,
        }),
    );
    let receipt = apply_command(&mut tablet, &reacquire, 2, 4);
    assert!(matches!(
        receipt.outcome,
        CacheTabletOutcome::Applied {
            result: CacheTabletOperationResult::LockAcquired {
                fencing_token: CacheLockFencingToken {
                    acquisition_index: 4,
                    ..
                },
                ..
            }
        }
    ));
}

#[test]
fn expired_owner_epoch_does_not_fence_a_new_acquisition() {
    let mut tablet = CacheTablet::new(scope(), config()).unwrap();
    let expiring = command(
        "expiring-owner-10",
        1,
        CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
            shard: 0,
            lock_key: "expiring-first".into(),
            owner: "expiring-worker".into(),
            owner_epoch: 10,
            lease_ms: 1,
        }),
    );
    apply_command(&mut tablet, &expiring, 2, 1);
    let after_expiry = command(
        "expired-owner-1",
        2,
        CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
            shard: 0,
            lock_key: "expiring-second".into(),
            owner: "expiring-worker".into(),
            owner_epoch: 1,
            lease_ms: 100,
        }),
    );
    let receipt = apply_command(&mut tablet, &after_expiry, 2, 2);
    assert!(matches!(
        receipt.outcome,
        CacheTabletOutcome::Applied {
            result: CacheTabletOperationResult::LockAcquired {
                fencing_token: CacheLockFencingToken {
                    acquisition_index: 2,
                    ..
                },
                ..
            }
        }
    ));
}

#[test]
fn receipt_json_serializes_signed_counter_extremes_as_decimal_strings() {
    let mut tablet = CacheTablet::new(scope(), config()).unwrap();
    let maximum = apply_command(
        &mut tablet,
        &command("maximum", 1, set("maximum", CacheValue::Counter(i64::MAX))),
        2,
        1,
    );
    let maximum = serde_json::to_value(maximum).unwrap();
    assert_eq!(
        maximum["outcome"]["result"]["item"]["value"]["value"],
        json!(i64::MAX.to_string())
    );

    let minimum = command(
        "minimum",
        2,
        CacheTabletOperation::Increment(CacheIncrementCommand {
            shard: 0,
            key: "minimum".into(),
            delta: i64::MIN,
            expected_version: Some(0),
            ttl_ms: None,
            lock_guard: None,
        }),
    );
    let minimum = serde_json::to_value(apply_command(&mut tablet, &minimum, 2, 2)).unwrap();
    assert_eq!(
        minimum["outcome"]["result"]["value"],
        json!(i64::MIN.to_string())
    );
}

#[test]
fn runtime_deadline_overflow_is_a_recorded_capacity_rejection() {
    let mut tablet = CacheTablet::new(scope(), config()).unwrap();
    let lock = command(
        "overflow-lock",
        u64::MAX - 5,
        CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
            shard: 0,
            lock_key: "lock".into(),
            owner: "owner".into(),
            owner_epoch: 1,
            lease_ms: 10,
        }),
    );
    assert_rejected(
        &apply_command(&mut tablet, &lock, 2, 1),
        CacheTabletRejectionCode::Capacity,
    );
    assert_eq!(tablet.active_lock_count(), 0);

    let ttl = command(
        "overflow-ttl",
        u64::MAX - 4,
        CacheTabletOperation::Set(CacheSetCommand {
            shard: 0,
            key: "value".into(),
            value: CacheValue::Counter(1),
            ttl_ms: Some(10),
            lock_guard: None,
        }),
    );
    assert_rejected(
        &apply_command(&mut tablet, &ttl, 2, 2),
        CacheTabletRejectionCode::Capacity,
    );
    assert!(tablet.observe("value").item.is_none());
}

#[test]
fn old_term_and_expired_lock_tokens_cannot_guard_cache_mutations() {
    let mut tablet = CacheTablet::new(scope(), config()).unwrap();
    let acquire = command(
        "acquire",
        10,
        CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
            shard: 0,
            lock_key: "protected".into(),
            owner: "worker".into(),
            owner_epoch: 1,
            lease_ms: 10,
        }),
    );
    let receipt = apply_command(&mut tablet, &acquire, 2, 1);
    let CacheTabletOutcome::Applied {
        result: CacheTabletOperationResult::LockAcquired { lease_token, .. },
    } = receipt.outcome
    else {
        panic!("expected lock acquisition")
    };
    let guard = CacheLockGuard {
        lock_key: "protected".into(),
        owner: "worker".into(),
        owner_epoch: 1,
        lease_token,
    };

    let old_term = command(
        "old-term-guard",
        11,
        CacheTabletOperation::Set(CacheSetCommand {
            shard: 0,
            key: "value".into(),
            value: CacheValue::String("unsafe".into()),
            ttl_ms: None,
            lock_guard: Some(guard.clone()),
        }),
    );
    assert_rejected(
        &apply_command(&mut tablet, &old_term, 3, 2),
        CacheTabletRejectionCode::Fenced,
    );
    assert!(tablet.observe("value").item.is_none());

    let before_deadline = command(
        "contended",
        19,
        CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
            shard: 0,
            lock_key: "protected".into(),
            owner: "new-worker".into(),
            owner_epoch: 1,
            lease_ms: 10,
        }),
    );
    assert_rejected(
        &apply_command(&mut tablet, &before_deadline, 3, 3),
        CacheTabletRejectionCode::Conflict,
    );

    let at_deadline = command(
        "takeover",
        20,
        CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
            shard: 0,
            lock_key: "protected".into(),
            owner: "new-worker".into(),
            owner_epoch: 1,
            lease_ms: 10,
        }),
    );
    let takeover = apply_command(&mut tablet, &at_deadline, 3, 4);
    assert!(matches!(
        takeover.outcome,
        CacheTabletOutcome::Applied {
            result: CacheTabletOperationResult::LockAcquired { .. }
        }
    ));

    let expired_guard = command(
        "expired-guard",
        21,
        CacheTabletOperation::Set(CacheSetCommand {
            shard: 0,
            key: "value".into(),
            value: CacheValue::String("unsafe".into()),
            ttl_ms: None,
            lock_guard: Some(guard),
        }),
    );
    assert_rejected(
        &apply_command(&mut tablet, &expired_guard, 3, 5),
        CacheTabletRejectionCode::Fenced,
    );
}

#[test]
fn exact_replay_returns_original_result_and_conflicting_commit_fails_closed() {
    let mut tablet = CacheTablet::new(scope(), config()).unwrap();
    let command = command("set", 10, set("key", CacheValue::String("one".into())));
    let payload = command.encode(&scope()).unwrap();
    let proposal_id = command.proposal_id(&scope()).unwrap();
    let first = tablet
        .apply(committed(proposal_id, 2, 1, &payload))
        .unwrap();
    let replayed = tablet
        .apply(committed(proposal_id, 2, 1, &payload))
        .unwrap();
    assert_eq!(replayed.disposition, CacheTabletDisposition::Replayed);
    assert_eq!(tablet.applied_command_count(), 1);
    assert_eq!(tablet.cache_revision(), 1);
    assert_eq!(first.outcome, replayed.outcome);

    let conflicting = CacheTabletCommand::new(
        &scope(),
        "set",
        10,
        set("key", CacheValue::String("two".into())),
    )
    .unwrap()
    .encode(&scope())
    .unwrap();
    assert!(matches!(
        tablet.apply(committed(proposal_id, 2, 1, &conflicting)),
        Err(TabletError::ConflictingCommand { .. })
    ));
}

#[test]
fn malformed_out_of_order_and_mismatched_commits_fail_without_mutation() {
    let mut tablet = CacheTablet::new(scope(), config()).unwrap();
    let seed = command("seed", 10, set("key", CacheValue::String("value".into())));
    let seed_payload = seed.encode(&scope()).unwrap();
    let seed_proposal = seed.proposal_id(&scope()).unwrap();
    tablet
        .apply(committed(seed_proposal, 2, 2, &seed_payload))
        .unwrap();
    let before = (
        tablet.state_digest(),
        tablet.cache_recovery_state_digest(),
        tablet.cache_revision(),
        tablet.applied_command_count(),
        tablet.last_applied_command_index(),
        tablet.last_applied_time_ms(),
    );

    let older = command("older", 11, set("other", CacheValue::Counter(1)));
    let older_payload = older.encode(&scope()).unwrap();
    assert!(matches!(
        tablet.apply(committed(
            older.proposal_id(&scope()).unwrap(),
            2,
            1,
            &older_payload,
        )),
        Err(TabletError::CommitOrder { .. })
    ));
    assert!(matches!(
        tablet.apply(committed(123, 2, 3, b"{}")),
        Err(TabletError::Decoding(_))
    ));

    let next = command("next", 11, set("other", CacheValue::Counter(1)));
    let next_payload = next.encode(&scope()).unwrap();
    let expected_proposal = next.proposal_id(&scope()).unwrap();
    let wrong_proposal = if expected_proposal == u64::MAX {
        expected_proposal - 1
    } else {
        expected_proposal + 1
    };
    assert!(matches!(
        tablet.apply(committed(wrong_proposal, 2, 3, &next_payload)),
        Err(TabletError::InvalidCommand(_))
    ));

    assert_eq!(
        (
            tablet.state_digest(),
            tablet.cache_recovery_state_digest(),
            tablet.cache_revision(),
            tablet.applied_command_count(),
            tablet.last_applied_command_index(),
            tablet.last_applied_time_ms(),
        ),
        before
    );
}

#[test]
fn live_and_recovered_histories_have_identical_digest_and_receipts() {
    let history = [
        command("set", 100, set("a", CacheValue::Counter(1))),
        command(
            "increment",
            90,
            CacheTabletOperation::Increment(CacheIncrementCommand {
                shard: 0,
                key: "a".into(),
                delta: 2,
                expected_version: Some(1),
                ttl_ms: None,
                lock_guard: None,
            }),
        ),
        command(
            "maintain",
            101,
            CacheTabletOperation::Maintain(CacheMaintainCommand {
                shard: 0,
                max_expirations: 10,
            }),
        ),
    ];
    let mut live = CacheTablet::new(scope(), config()).unwrap();
    let mut recovered = CacheTablet::new(scope(), config()).unwrap();
    let mut live_receipts = Vec::new();
    let mut recovered_receipts = Vec::new();
    for (offset, command) in history.iter().enumerate() {
        let index = u64::try_from(offset + 1).unwrap();
        live_receipts.push(apply_command(&mut live, command, 2, index));
    }
    for (offset, command) in history.iter().enumerate() {
        let index = u64::try_from(offset + 1).unwrap();
        recovered_receipts.push(apply_command(&mut recovered, command, 2, index));
    }
    assert_eq!(live_receipts, recovered_receipts);
    assert_eq!(live.state_digest(), recovered.state_digest());
    assert_eq!(
        live.cache_recovery_state_checksum(),
        recovered.cache_recovery_state_checksum()
    );
    assert_eq!(live.observe("a"), recovered.observe("a"));
}

#[test]
fn receipt_and_complete_state_digest_have_golden_vectors() {
    let mut tablet = CacheTablet::new(scope(), config()).unwrap();
    let golden = command(
        "golden-set",
        11,
        CacheTabletOperation::Set(CacheSetCommand {
            shard: 0,
            key: "golden".into(),
            value: CacheValue::String("value".into()),
            ttl_ms: Some(9),
            lock_guard: None,
        }),
    );
    let receipt = apply_command(&mut tablet, &golden, 2, 1);
    assert_eq!(
        serde_json::to_string(&receipt).unwrap(),
        r#"{"proposal_id":"15848596182136469974","tablet_id":"7","tablet_epoch":"3","term":"2","commit_index":"1","applied_at_ms":"11","write_evidence":"fixed_voter_majority_persisted","durable_voter_acks":2,"disposition":"new","outcome":{"status":"applied","result":{"kind":"set","key":"golden","item":{"value":{"kind":"string","value":"value"},"version":"1","expires_at_ms":"20"}}}}"#
    );
    assert_eq!(
        hex_digest(tablet.state_digest()),
        "31d5169383b2c2eb2f0df96d7924f6e965d9eb7dee56314e38db8b88e1e5a134"
    );
}

#[test]
fn state_digest_is_sensitive_to_complete_cache_state() {
    let mut left = CacheTablet::new(scope(), config()).unwrap();
    let mut right = CacheTablet::new(scope(), config()).unwrap();
    apply_command(
        &mut left,
        &command("left", 1, set("key", CacheValue::String("left".into()))),
        2,
        1,
    );
    apply_command(
        &mut right,
        &command("right", 1, set("key", CacheValue::String("right".into()))),
        2,
        1,
    );
    assert_ne!(
        left.cache_recovery_state_checksum(),
        right.cache_recovery_state_checksum()
    );
    assert_ne!(
        left.cache_recovery_state_digest(),
        right.cache_recovery_state_digest()
    );
    assert_ne!(left.state_digest(), right.state_digest());
}

use std::collections::BTreeMap;

use epoch_core::{DurabilityProfile, EpochError, EventEnvelope};
use epoch_queue::{
    LeaseFence, Queue, QueueAdvancedConfig, QueueCircuitState, QueueDispatchPolicy, QueueIngress,
    QueueOverflowPolicy, QueueState,
};

fn envelope(id: &str, priority: u8, dedupe_id: Option<&str>) -> EventEnvelope {
    EventEnvelope {
        id: id.into(),
        source: "advanced-queue-tests".into(),
        event_type: "job.created".into(),
        subject: None,
        time_ms: 1,
        key: None,
        headers: BTreeMap::new(),
        content_type: "application/json".into(),
        schema_ref: None,
        traceparent: None,
        payload: serde_json::json!({"id": id}),
        deliver_at_ms: None,
        ttl_ms: None,
        priority,
        dedupe_id: dedupe_id.map(str::to_owned),
        transaction_id: None,
        extensions: BTreeMap::new(),
    }
}

fn fence(consumer_epoch: u64) -> LeaseFence {
    LeaseFence::new(7, 3, 0, 11, consumer_epoch).expect("valid fence")
}

fn configured(advanced: QueueAdvancedConfig) -> Queue {
    configured_with_max_messages(advanced, 2)
}

fn configured_with_max_messages(advanced: QueueAdvancedConfig, max_messages: usize) -> Queue {
    Queue::new(epoch_queue::QueueConfig {
        durability: DurabilityProfile::Volatile,
        visibility_timeout_ms: 30,
        max_messages,
        retry: epoch_queue::RetryPolicy::default(),
        dedupe_window_ms: Some(100),
        advanced: Some(advanced),
    })
    .expect("valid queue")
}

#[test]
fn duplicate_is_resolved_before_drop_oldest_overflow() {
    let mut queue = configured(QueueAdvancedConfig {
        overflow: QueueOverflowPolicy::DropOldest,
        ..QueueAdvancedConfig::default()
    });
    let first = queue
        .enqueue_advanced(QueueIngress::new(envelope("one", 0, Some("same"))), 1)
        .expect("first enqueue");
    queue
        .enqueue_advanced(QueueIngress::new(envelope("two", 0, None)), 2)
        .expect("second enqueue");

    let duplicate = queue
        .enqueue_advanced(
            QueueIngress::new(envelope("replacement", 9, Some("same"))),
            3,
        )
        .expect("dedupe retry");
    assert_eq!(duplicate.message_id, first.message_id);
    assert!(duplicate.acknowledgement.duplicate);
    assert_eq!(queue.get("one").expect("one").state, QueueState::Ready);
    assert_eq!(queue.get("two").expect("two").state, QueueState::Ready);

    queue
        .enqueue_advanced(QueueIngress::new(envelope("three", 0, None)), 4)
        .expect("overflow enqueue");
    assert_eq!(queue.get("one").expect("one").state, QueueState::Expired);
    assert_eq!(queue.active_len(), 2);
}

#[test]
fn byte_limit_and_idle_expiry_are_durable_state() {
    let probe = QueueIngress::new(envelope("probe", 0, None));
    let charge = probe.charged_bytes().expect("charge");
    let mut queue = configured(QueueAdvancedConfig {
        max_active_bytes: Some(charge),
        idle_expiry_ms: Some(10),
        ..QueueAdvancedConfig::default()
    });
    queue.enqueue_advanced(probe, 5).expect("fits exactly");
    let error = queue
        .enqueue_advanced(QueueIngress::new(envelope("larger", 0, None)), 6)
        .expect_err("byte ceiling");
    assert!(matches!(error, EpochError::Capacity(_)));

    let delivery = queue
        .acquire_advanced("worker", 1, None, 7, fence(1))
        .expect("acquire")
        .pop()
        .expect("delivery");
    queue
        .acknowledge_fenced(&delivery.lease_token, fence(1), 8)
        .expect("ack");
    queue.maintain_advanced(17, 0).expect("not expired yet");
    assert!(!queue.advanced_observation().expired);
    queue.maintain_advanced(18, 0).expect("expiry boundary");
    assert!(queue.advanced_observation().expired);
    assert!(matches!(
        queue.enqueue_advanced(QueueIngress::new(envelope("late", 0, None)), 19),
        Err(EpochError::Unavailable(_))
    ));
}

#[test]
fn session_lock_is_exclusive_renewable_and_fifo() {
    let mut queue = configured(QueueAdvancedConfig::default());
    for id in ["one", "two"] {
        queue
            .enqueue_advanced(
                QueueIngress::new(envelope(id, 0, None)).with_session("account-7"),
                1,
            )
            .expect("session enqueue");
    }
    assert!(
        queue
            .acquire_advanced("ordinary", 2, None, 2, fence(1))
            .expect("ordinary acquire")
            .is_empty()
    );

    let acquired = queue
        .acquire_session_fenced("account-7", "worker-a", 1, None, None, 2, fence(2))
        .expect("session acquire");
    assert_eq!(acquired.deliveries[0].message.id, "one");
    assert!(matches!(
        queue.acquire_session_fenced("account-7", "worker-b", 1, None, None, 3, fence(3)),
        Err(EpochError::Conflict(_))
    ));
    let renewed = queue
        .renew_session_lock_fenced(&acquired.lock_token, 20, 4, fence(2))
        .expect("renew lock");
    assert!(renewed.lock_deadline_ms > acquired.lock_deadline_ms);
    assert!(matches!(
        queue.release_session_lock_fenced(&acquired.lock_token, fence(2), 5),
        Err(EpochError::Fenced)
    ));
    queue
        .release_session_lock_fenced(&renewed.lock_token, fence(2), 5)
        .expect("release renewed lock");
}

#[test]
fn priority_aging_prevents_starvation_deterministically() {
    let mut queue = configured(QueueAdvancedConfig {
        priority_aging_interval_ms: Some(10),
        ..QueueAdvancedConfig::default()
    });
    queue
        .enqueue_advanced(QueueIngress::new(envelope("old-low", 0, None)), 0)
        .expect("old low");
    queue
        .enqueue_advanced(QueueIngress::new(envelope("new-high", 2, None)), 19)
        .expect("new high");

    let delivery = queue
        .acquire_advanced("worker", 1, None, 20, fence(1))
        .expect("acquire")
        .pop()
        .expect("delivery");
    assert_eq!(delivery.message.id, "old-low");
}

#[test]
fn dispatch_bucket_concurrency_and_breaker_gate_acquisition() {
    let mut queue = configured_with_max_messages(
        QueueAdvancedConfig {
            dispatch: Some(QueueDispatchPolicy {
                messages_per_second: 1,
                burst: 1,
                max_in_flight: 2,
                failure_threshold: 1,
                open_interval_ms: 10,
            }),
            ..QueueAdvancedConfig::default()
        },
        3,
    );
    for id in ["one", "two", "three"] {
        queue
            .enqueue_advanced(QueueIngress::new(envelope(id, 0, None)), 0)
            .expect("enqueue");
    }
    let first = queue
        .acquire_advanced("worker", 2, None, 0, fence(1))
        .expect("first acquire");
    assert_eq!(first.len(), 1);
    queue
        .nack_fenced(&first[0].lease_token, fence(1), "downstream", 1)
        .expect("nack");
    queue.record_dispatch_failure(1).expect("open breaker");
    assert_eq!(
        queue.advanced_observation().circuit_state,
        QueueCircuitState::Open
    );
    assert!(
        queue
            .acquire_advanced("worker", 1, None, 10, fence(1))
            .expect("open acquire")
            .is_empty()
    );
    let probe = queue
        .acquire_advanced("worker", 1, None, 11, fence(1))
        .expect("half-open probe");
    assert_eq!(probe.len(), 1);
    let mut restored = Queue::decode_snapshot(&queue.encode_snapshot().expect("probe snapshot"))
        .expect("restore half-open probe");
    assert!(
        restored
            .acquire_advanced("worker", 1, None, 12, fence(1))
            .expect("restored second half-open acquire")
            .is_empty(),
        "snapshot recovery must retain the live half-open probe"
    );
    assert!(
        queue
            .acquire_advanced("worker", 1, None, 12, fence(1))
            .expect("second half-open acquire")
            .is_empty(),
        "a live half-open probe must exclude another probe even after token refill"
    );

    queue
        .nack_fenced(&probe[0].lease_token, fence(1), "probe failed", 13)
        .expect("nack probe");
    queue.record_dispatch_failure(13).expect("reopen breaker");
    assert_eq!(
        queue.advanced_observation().circuit_state,
        QueueCircuitState::Open
    );
}

#[test]
fn deferred_exact_receive_and_correlation_survive_snapshot() {
    let mut queue = configured(QueueAdvancedConfig::default());
    queue
        .enqueue_advanced(
            QueueIngress::new(envelope("request", 0, None))
                .with_correlation("correlation-1")
                .with_reply_to("reply-temporary"),
            1,
        )
        .expect("enqueue");
    let delivery = queue
        .acquire_advanced("worker", 1, None, 2, fence(1))
        .expect("acquire")
        .pop()
        .expect("delivery");
    queue
        .defer_fenced(&delivery.lease_token, fence(1), "awaiting dependency", 3)
        .expect("defer");
    assert!(
        queue
            .acquire_advanced("other", 1, None, 4, fence(2))
            .expect("ordinary acquire")
            .is_empty()
    );
    assert_eq!(queue.lookup_correlation("correlation-1").len(), 1);

    let restored = Queue::decode_snapshot(&queue.encode_snapshot().expect("snapshot"))
        .expect("restore advanced snapshot");
    assert_eq!(restored.lookup_correlation("correlation-1").len(), 1);
    let exact = restored
        .clone()
        .receive_deferred_fenced("request", "other", None, 4, fence(2))
        .expect("exact receive");
    assert_eq!(exact.message.id, "request");
}

//! Compatibility gateways that translate supported Redis, Kafka, and `RabbitMQ`
//! wire operations into Epoch's native Cache, Stream, and Queue contracts.

pub mod amqp;
pub mod backend;
pub mod kafka;
pub mod redis;
pub mod scanner;

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod test_support;

pub use backend::{
    BackendError, CacheAtomicMutation, CacheCollectionMutation, CacheCollectionResult, CacheEntry,
    CacheMultiSetEntry, CachePubSubMessage, CacheSetCondition, CacheSetOptions, CacheSetOutcome,
    CacheSnapshot, CacheStorageClass, CacheValue, CompatibilityBackend, NativeHttpBackend,
    NativeHttpConfig, QueueDelivery, QueueMessage, StreamGroupIdentity, StreamGroupMember,
    StreamGroupRejection, StreamGroupSession, StreamGroupSessionResult, StreamRecord,
};

/// Maximum accepted protocol frame size across compatibility listeners.
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

/// Maximum accepted message body after protocol metadata is removed.
pub const MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

/// Maximum number of logical items in one protocol request.
pub const MAX_REQUEST_ITEMS: usize = 1_024;

pub(crate) fn observe_protocol(
    metrics: Option<&epoch_observability::MetricsRegistry>,
    protocol: epoch_observability::Protocol,
    operation: epoch_observability::ProtocolOperation,
    outcome: epoch_observability::Outcome,
    elapsed: std::time::Duration,
) {
    if let Some(metrics) = metrics {
        metrics.record_protocol(&epoch_observability::ProtocolObservation {
            protocol,
            operation,
            outcome,
            elapsed,
        });
    }
}

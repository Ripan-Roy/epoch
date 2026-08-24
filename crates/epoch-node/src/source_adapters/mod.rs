//! Transport-specific readers feeding the shared durable source-ingestion path.

mod kafka;
mod mysql;
mod object;
mod postgres;

use epoch_core::EventEnvelope;

pub(crate) use kafka::{KafkaSource, KafkaSourceAdapter};
pub(crate) use mysql::{MySqlSource, MySqlSourceAdapter};
pub(crate) use object::{ObjectSource, ObjectSourceAdapter};
pub(crate) use postgres::{PostgresSource, PostgresSourceAdapter};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SourceRecord {
    Event(Box<EventEnvelope>),
    Error { record_id: String, reason: String },
}

impl SourceRecord {
    pub(crate) fn record_id(&self) -> &str {
        match self {
            Self::Event(event) => &event.id,
            Self::Error { record_id, .. } => record_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SourceBatch {
    pub batch_id: String,
    pub source_from: String,
    pub source_to: String,
    pub records: Vec<SourceRecord>,
}

#[cfg(test)]
fn test_delivery_config(
    secrets: &serde_json::Value,
) -> crate::managed_target_delivery::ManagedTargetDeliveryConfig {
    use std::{io::Write as _, sync::Arc, time::Duration};

    let mut file = tempfile::NamedTempFile::new().expect("create connector secret fixture");
    write!(
        file,
        "{}",
        serde_json::json!({
            "format_version": 1,
            "secrets": secrets
        })
    )
    .expect("write connector secret fixture");
    crate::managed_target_delivery::ManagedTargetDeliveryConfig {
        interval: Duration::from_millis(10),
        allow_http_loopback: true,
        secrets: Arc::new(
            crate::managed_target_delivery::ManagedSecretStore::load(file.path())
                .expect("load connector secret fixture"),
        ),
    }
}

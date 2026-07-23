//! Shared HTTP contract helpers for typed tablet profiles.

use std::collections::BTreeMap;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use epoch_consensus::ConsensusError;
use epoch_core::EventEnvelope;
use epoch_tablet::TabletError;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::consensus::ConsensusProbeError;

pub(crate) type TabletApiResult<T> = Result<T, TabletApiError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StrictEventEnvelope {
    pub(crate) id: String,
    pub(crate) source: String,
    #[serde(rename = "type")]
    pub(crate) event_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) subject: Option<String>,
    #[serde(
        deserialize_with = "deserialize_u64_from_number_or_decimal",
        serialize_with = "serialize_u64_as_decimal"
    )]
    pub(crate) time_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) key: Option<String>,
    #[serde(default)]
    pub(crate) headers: BTreeMap<String, String>,
    #[serde(default = "default_content_type")]
    pub(crate) content_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) schema_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) traceparent: Option<String>,
    #[serde(default)]
    pub(crate) payload: Value,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_u64_from_number_or_decimal",
        serialize_with = "serialize_optional_u64_as_decimal"
    )]
    pub(crate) deliver_at_ms: Option<u64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_u64_from_number_or_decimal",
        serialize_with = "serialize_optional_u64_as_decimal"
    )]
    pub(crate) ttl_ms: Option<u64>,
    #[serde(default)]
    pub(crate) priority: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) dedupe_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) transaction_id: Option<String>,
    #[serde(default)]
    pub(crate) extensions: BTreeMap<String, Value>,
}

impl From<StrictEventEnvelope> for EventEnvelope {
    fn from(envelope: StrictEventEnvelope) -> Self {
        Self {
            id: envelope.id,
            source: envelope.source,
            event_type: envelope.event_type,
            subject: envelope.subject,
            time_ms: envelope.time_ms,
            key: envelope.key,
            headers: envelope.headers,
            content_type: envelope.content_type,
            schema_ref: envelope.schema_ref,
            traceparent: envelope.traceparent,
            payload: envelope.payload,
            deliver_at_ms: envelope.deliver_at_ms,
            ttl_ms: envelope.ttl_ms,
            priority: envelope.priority,
            dedupe_id: envelope.dedupe_id,
            transaction_id: envelope.transaction_id,
            extensions: envelope.extensions,
        }
    }
}

impl From<EventEnvelope> for StrictEventEnvelope {
    fn from(envelope: EventEnvelope) -> Self {
        Self {
            id: envelope.id,
            source: envelope.source,
            event_type: envelope.event_type,
            subject: envelope.subject,
            time_ms: envelope.time_ms,
            key: envelope.key,
            headers: envelope.headers,
            content_type: envelope.content_type,
            schema_ref: envelope.schema_ref,
            traceparent: envelope.traceparent,
            payload: envelope.payload,
            deliver_at_ms: envelope.deliver_at_ms,
            ttl_ms: envelope.ttl_ms,
            priority: envelope.priority,
            dedupe_id: envelope.dedupe_id,
            transaction_id: envelope.transaction_id,
            extensions: envelope.extensions,
        }
    }
}

pub(crate) fn deserialize_strict_event_envelope<'de, D>(
    deserializer: D,
) -> Result<EventEnvelope, D::Error>
where
    D: Deserializer<'de>,
{
    StrictEventEnvelope::deserialize(deserializer).map(Into::into)
}

#[derive(Deserialize)]
#[serde(untagged)]
enum U64Input {
    Number(u64),
    Decimal(String),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum I64Input {
    Number(i64),
    Decimal(String),
}

pub(crate) fn deserialize_u64_from_number_or_decimal<'de, D>(
    deserializer: D,
) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_u64_input(U64Input::deserialize(deserializer)?).map_err(serde::de::Error::custom)
}

pub(crate) fn deserialize_optional_u64_from_number_or_decimal<'de, D>(
    deserializer: D,
) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<U64Input>::deserialize(deserializer)?.map_or(Ok(None), |input| {
        deserialize_u64_input(input)
            .map(Some)
            .map_err(serde::de::Error::custom)
    })
}

pub(crate) fn deserialize_i64_from_number_or_decimal<'de, D>(
    deserializer: D,
) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_i64_input(I64Input::deserialize(deserializer)?).map_err(serde::de::Error::custom)
}

fn deserialize_u64_input(input: U64Input) -> Result<u64, &'static str> {
    match input {
        U64Input::Number(value) => Ok(value),
        U64Input::Decimal(value) => value
            .parse()
            .map_err(|_| "expected an unsigned decimal integer"),
    }
}

fn deserialize_i64_input(input: I64Input) -> Result<i64, &'static str> {
    match input {
        I64Input::Number(value) => Ok(value),
        I64Input::Decimal(value) => value
            .parse()
            .map_err(|_| "expected a signed decimal integer"),
    }
}

fn default_content_type() -> String {
    "application/json".to_owned()
}

#[derive(Debug)]
pub(crate) enum TabletApiError {
    RequestBody { status: StatusCode, message: String },
    InvalidRequest(String),
    IdempotencyConflict,
    Consensus(ConsensusProbeError),
    Tablet(TabletError),
    Profile(String),
}

impl From<ConsensusProbeError> for TabletApiError {
    fn from(error: ConsensusProbeError) -> Self {
        Self::Consensus(error)
    }
}

impl From<TabletError> for TabletApiError {
    fn from(error: TabletError) -> Self {
        Self::Tablet(error)
    }
}

impl From<String> for TabletApiError {
    fn from(error: String) -> Self {
        Self::Profile(error)
    }
}

#[derive(Debug, Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
    outcome_certainty: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_optional_u64_as_decimal")]
    leader_hint: Option<u64>,
}

impl IntoResponse for TabletApiError {
    fn into_response(self) -> Response {
        let (status, code, message, certainty, leader_hint) = match self {
            Self::RequestBody { status, message } => (
                status,
                "invalid_request",
                message,
                "definite_not_committed",
                None,
            ),
            Self::InvalidRequest(message) => (
                StatusCode::BAD_REQUEST,
                "invalid_request",
                message,
                "definite_not_committed",
                None,
            ),
            Self::IdempotencyConflict => (
                StatusCode::CONFLICT,
                "idempotency_conflict",
                "idempotency key is already bound to different semantic input".into(),
                "unknown",
                None,
            ),
            Self::Tablet(error) => (
                StatusCode::BAD_REQUEST,
                "invalid_tablet_command",
                error.to_string(),
                "definite_not_committed",
                None,
            ),
            Self::Profile(message) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "profile_unavailable",
                message,
                "unknown",
                None,
            ),
            Self::Consensus(ConsensusProbeError::Consensus(ConsensusError::NotLeader {
                leader_hint,
            })) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "not_leader",
                "request reached a follower".into(),
                "unknown",
                leader_hint.map(epoch_consensus::NodeId::get),
            ),
            Self::Consensus(ConsensusProbeError::Consensus(ConsensusError::StaleTerm {
                ..
            })) => (
                StatusCode::CONFLICT,
                "stale_term",
                "expected_term does not match the current term".into(),
                // Lookup and proposal are not atomic. Another leader may have
                // committed this deterministic proposal ID in between.
                "unknown",
                None,
            ),
            Self::Consensus(ConsensusProbeError::Consensus(
                ConsensusError::ConflictingProposal(_),
            )) => (
                StatusCode::CONFLICT,
                "idempotency_conflict",
                "proposal ID is already bound to different command bytes".into(),
                "unknown",
                None,
            ),
            Self::Consensus(error) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "consensus_unavailable",
                error.to_string(),
                "unknown",
                None,
            ),
        };
        (
            status,
            Json(ErrorEnvelope {
                error: ErrorBody {
                    code,
                    message,
                    outcome_certainty: certainty,
                    leader_hint,
                },
            }),
        )
            .into_response()
    }
}

pub(crate) fn hex_digest(digest: [u8; 32]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

#[allow(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde serialize_with requires a shared reference"
)]
pub(crate) fn serialize_u64_as_decimal<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&value.to_string())
}

#[allow(
    clippy::ref_option,
    reason = "serde serialize_with requires a shared reference to the field"
)]
pub(crate) fn serialize_optional_u64_as_decimal<S>(
    value: &Option<u64>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match value {
        Some(value) => serializer.serialize_some(&value.to_string()),
        None => serializer.serialize_none(),
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::{Value, json};

    use super::deserialize_i64_from_number_or_decimal;

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct SignedInput {
        #[serde(deserialize_with = "deserialize_i64_from_number_or_decimal")]
        value: i64,
    }

    #[test]
    fn signed_i64_accepts_numbers_and_full_range_decimal_strings() {
        assert_eq!(
            serde_json::from_value::<SignedInput>(json!({"value": -42})).unwrap(),
            SignedInput { value: -42 }
        );
        assert_eq!(
            serde_json::from_value::<SignedInput>(json!({"value": i64::MIN.to_string()})).unwrap(),
            SignedInput { value: i64::MIN }
        );
        assert_eq!(
            serde_json::from_value::<SignedInput>(json!({"value": i64::MAX.to_string()})).unwrap(),
            SignedInput { value: i64::MAX }
        );
    }

    #[test]
    fn signed_i64_rejects_non_integral_and_out_of_range_inputs() {
        for value in [
            json!(1.5),
            json!("1.5"),
            json!("9223372036854775808"),
            json!("-9223372036854775809"),
            Value::Bool(true),
        ] {
            assert!(serde_json::from_value::<SignedInput>(json!({"value": value})).is_err());
        }
    }
}

//! Replicated Stream retention policy and maintenance contracts.

use epoch_stream::StreamRetentionPolicy;
use serde::{Deserialize, Serialize};

use crate::{
    StreamTabletWriteEvidence, TabletError, TabletResult,
    common::{deserialize_u64_from_number_or_decimal, serialize_u64_as_decimal},
};

pub const STREAM_TABLET_RETENTION_COMMAND_FORMAT_VERSION: u16 = 4;
pub const MAX_STREAM_RETENTION_RECORDS_PER_PARTITION: usize = 100_000;
pub const MAX_STREAM_RETENTION_BYTES_PER_PARTITION: u64 = 3 * 1024 * 1024;
pub const MAX_STREAM_RETENTION_AGE_MS: u64 = 10 * 365 * 24 * 60 * 60 * 1_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamRetentionCommand {
    pub mode: StreamTabletRetentionMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<StreamRetentionPolicy>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamTabletRetentionMode {
    Configure,
    Maintain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamTabletRetentionDisposition {
    New,
    Replayed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamTabletRetentionReceipt {
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub proposal_id: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub tablet_id: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub tablet_epoch: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub term: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub commit_index: u64,
    pub mode: StreamTabletRetentionMode,
    pub policy: StreamRetentionPolicy,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_u64_as_decimal",
        deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
    )]
    pub cutoff_ms: Option<u64>,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub previous_base_offset: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub base_offset: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub end_offset: u64,
    pub removed_records: usize,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub removed_bytes: u64,
    pub retained_records: usize,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub retained_bytes: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub applied_at_ms: u64,
    pub write_evidence: StreamTabletWriteEvidence,
    pub durable_voter_acks: u16,
    pub disposition: StreamTabletRetentionDisposition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamTabletRetentionObservation {
    pub policy: StreamRetentionPolicy,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_u64_as_decimal"
    )]
    pub retention_watermark_ms: Option<u64>,
    pub partition: u32,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub base_offset: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub end_offset: u64,
    pub retained_records: usize,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub retained_bytes: u64,
}

pub(crate) fn validate_retention_command(command: &StreamRetentionCommand) -> TabletResult<()> {
    match (command.mode, command.policy) {
        (StreamTabletRetentionMode::Configure, Some(policy)) => validate_retention_policy(policy),
        (StreamTabletRetentionMode::Configure, None) => Err(TabletError::InvalidCommand(
            "retention configuration requires a policy".into(),
        )),
        (StreamTabletRetentionMode::Maintain, None) => Ok(()),
        (StreamTabletRetentionMode::Maintain, Some(_)) => Err(TabletError::InvalidCommand(
            "retention maintenance cannot replace the policy".into(),
        )),
    }
}

pub fn validate_retention_policy(policy: StreamRetentionPolicy) -> TabletResult<()> {
    if policy.max_records_per_partition == Some(0)
        || policy
            .max_records_per_partition
            .is_some_and(|limit| limit > MAX_STREAM_RETENTION_RECORDS_PER_PARTITION)
    {
        return Err(TabletError::InvalidCommand(format!(
            "retention max_records_per_partition must be between 1 and {MAX_STREAM_RETENTION_RECORDS_PER_PARTITION}"
        )));
    }
    if policy.max_bytes_per_partition == Some(0)
        || policy
            .max_bytes_per_partition
            .is_some_and(|limit| limit > MAX_STREAM_RETENTION_BYTES_PER_PARTITION)
    {
        return Err(TabletError::InvalidCommand(format!(
            "retention max_bytes_per_partition must be between 1 and {MAX_STREAM_RETENTION_BYTES_PER_PARTITION}"
        )));
    }
    if policy.max_age_ms == Some(0)
        || policy
            .max_age_ms
            .is_some_and(|limit| limit > MAX_STREAM_RETENTION_AGE_MS)
    {
        return Err(TabletError::InvalidCommand(format!(
            "retention max_age_ms must be between 1 and {MAX_STREAM_RETENTION_AGE_MS}"
        )));
    }
    Ok(())
}

#[allow(
    clippy::ref_option,
    reason = "serde serialize_with requires a shared reference"
)]
fn serialize_optional_u64_as_decimal<S>(
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

fn deserialize_optional_u64_from_number_or_decimal<'de, D>(
    deserializer: D,
) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OptionalU64 {
        Number(u64),
        Decimal(String),
        Null,
    }

    match OptionalU64::deserialize(deserializer)? {
        OptionalU64::Number(value) => Ok(Some(value)),
        OptionalU64::Decimal(value) => value.parse().map(Some).map_err(serde::de::Error::custom),
        OptionalU64::Null => Ok(None),
    }
}

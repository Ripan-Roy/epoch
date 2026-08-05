//! Replicated Stream consumer-group checkpoint contracts.

use serde::{Deserialize, Serialize};

use crate::{
    StreamTabletWriteEvidence, TabletError, TabletResult, common::serialize_u64_as_decimal,
};

pub const STREAM_TABLET_GROUP_COMMAND_FORMAT_VERSION: u16 = 3;
pub const MAX_STREAM_CONSUMER_GROUP_BYTES: usize = 256;
pub const MAX_STREAM_CONSUMER_MEMBER_BYTES: usize = 256;
pub const MAX_STREAM_CONSUMER_GROUPS: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamGroupOffsetCommand {
    pub group: String,
    pub member_id: String,
    pub group_generation: u64,
    pub partition: u32,
    pub next_offset: u64,
    pub mode: StreamGroupOffsetMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamGroupOffsetMode {
    Commit,
    Reset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamTabletGroupDisposition {
    New,
    Replayed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamTabletGroupOutcome {
    Applied,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamTabletGroupRejection {
    OwnerMismatch,
    StaleGeneration,
    GenerationGap,
    CommitRewind,
    OffsetBeforeRetained,
    OffsetBeyondEnd,
    GroupCapacityReached,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamTabletGroupReceipt {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub proposal_id: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub tablet_id: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub tablet_epoch: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub term: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub commit_index: u64,
    pub group: String,
    pub member_id: String,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub group_generation: u64,
    pub partition: u32,
    pub mode: StreamGroupOffsetMode,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub requested_next_offset: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub previous_offset: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub committed_offset: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub end_offset: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub lag: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub applied_at_ms: u64,
    pub outcome: StreamTabletGroupOutcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rejection: Option<StreamTabletGroupRejection>,
    pub write_evidence: StreamTabletWriteEvidence,
    pub durable_voter_acks: u16,
    pub disposition: StreamTabletGroupDisposition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamTabletGroupObservation {
    pub exists: bool,
    pub group: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member_id: Option<String>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_u64_as_decimal"
    )]
    pub group_generation: Option<u64>,
    pub partition: u32,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub base_offset: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub committed_offset: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub end_offset: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub lag: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamConsumerGroupOwner {
    pub member_id: String,
    pub generation: u64,
}

pub(crate) fn validate_group_offset_command(
    command: &StreamGroupOffsetCommand,
) -> TabletResult<()> {
    validate_bounded_identifier(
        "consumer group",
        &command.group,
        MAX_STREAM_CONSUMER_GROUP_BYTES,
    )?;
    validate_bounded_identifier(
        "consumer member_id",
        &command.member_id,
        MAX_STREAM_CONSUMER_MEMBER_BYTES,
    )?;
    if command.group_generation == 0 {
        return Err(TabletError::InvalidCommand(
            "consumer group_generation must be non-zero".into(),
        ));
    }
    if command.partition != 0 {
        return Err(TabletError::InvalidCommand(
            "the first Stream tablet slice supports only partition 0".into(),
        ));
    }
    Ok(())
}

pub fn validate_stream_consumer_group(group: &str) -> TabletResult<()> {
    validate_bounded_identifier("consumer group", group, MAX_STREAM_CONSUMER_GROUP_BYTES)
}

fn validate_bounded_identifier(name: &str, value: &str, maximum: usize) -> TabletResult<()> {
    if value.trim().is_empty() {
        return Err(TabletError::InvalidCommand(format!("{name} is required")));
    }
    if value.len() > maximum {
        return Err(TabletError::InvalidCommand(format!(
            "{name} is {} bytes; maximum is {maximum}",
            value.len()
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(TabletError::InvalidCommand(format!(
            "{name} cannot contain control characters"
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

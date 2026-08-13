//! Replicated Stream consumer-session membership and assignment contracts.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    StreamTabletWriteEvidence, TabletError, TabletResult,
    common::{deserialize_u64_from_number_or_decimal, serialize_u64_as_decimal},
    stream_group::{
        MAX_STREAM_CONSUMER_GROUP_BYTES, MAX_STREAM_CONSUMER_GROUPS,
        MAX_STREAM_CONSUMER_MEMBER_BYTES, validate_bounded_identifier,
    },
};

pub const STREAM_TABLET_SESSION_COMMAND_FORMAT_VERSION: u16 = 5;
pub const MAX_STREAM_CONSUMER_MEMBERS_PER_GROUP: usize = 1_024;
pub const MAX_STREAM_SESSION_SHARDS: u32 = 4_096;
pub const MIN_STREAM_SESSION_TIMEOUT_MS: u64 = 1_000;
pub const MAX_STREAM_SESSION_TIMEOUT_MS: u64 = 300_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamGroupSessionCommand {
    pub group: String,
    pub shard_count: u32,
    pub action: StreamGroupSessionAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StreamGroupSessionAction {
    Join {
        member_id: String,
        session_timeout_ms: u64,
    },
    Heartbeat {
        member_id: String,
        group_generation: u64,
    },
    Leave {
        member_id: String,
        group_generation: u64,
    },
    Maintain,
}

impl StreamGroupSessionAction {
    pub const fn operation(&self) -> StreamGroupSessionOperation {
        match self {
            Self::Join { .. } => StreamGroupSessionOperation::Join,
            Self::Heartbeat { .. } => StreamGroupSessionOperation::Heartbeat,
            Self::Leave { .. } => StreamGroupSessionOperation::Leave,
            Self::Maintain => StreamGroupSessionOperation::Maintain,
        }
    }

    pub fn member_id(&self) -> Option<&str> {
        match self {
            Self::Join { member_id, .. }
            | Self::Heartbeat { member_id, .. }
            | Self::Leave { member_id, .. } => Some(member_id),
            Self::Maintain => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamGroupSessionOperation {
    Join,
    Heartbeat,
    Leave,
    Maintain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamTabletSessionDisposition {
    New,
    Replayed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamTabletSessionOutcome {
    Applied,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamTabletSessionRejection {
    UnknownGroup,
    UnknownMember,
    StaleGeneration,
    ShardCountMismatch,
    GroupCapacityReached,
    MemberCapacityReached,
    DeadlineOverflow,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamTabletSessionMember {
    pub member_id: String,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub session_timeout_ms: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub deadline_ms: u64,
    pub assigned_shards: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamTabletSessionReceipt {
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
    pub group: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member_id: Option<String>,
    pub operation: StreamGroupSessionOperation,
    pub shard_count: u32,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub group_generation: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub watermark_ms: u64,
    pub members: Vec<StreamTabletSessionMember>,
    pub assigned_shards: Vec<u32>,
    pub expired_members: Vec<String>,
    pub outcome: StreamTabletSessionOutcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rejection: Option<StreamTabletSessionRejection>,
    pub write_evidence: StreamTabletWriteEvidence,
    pub durable_voter_acks: u16,
    pub disposition: StreamTabletSessionDisposition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamTabletSessionObservation {
    pub exists: bool,
    pub group: String,
    pub shard_count: Option<u32>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_u64_as_decimal"
    )]
    pub group_generation: Option<u64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_u64_as_decimal"
    )]
    pub watermark_ms: Option<u64>,
    pub members: Vec<StreamTabletSessionMember>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamConsumerSessionMember {
    pub session_timeout_ms: u64,
    pub deadline_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamConsumerSessionGroup {
    pub shard_count: u32,
    pub generation: u64,
    pub watermark_ms: u64,
    pub members: BTreeMap<String, StreamConsumerSessionMember>,
}

#[derive(Debug)]
pub(crate) struct StreamSessionTransition {
    pub outcome: StreamTabletSessionOutcome,
    pub rejection: Option<StreamTabletSessionRejection>,
    pub expired_members: Vec<String>,
}

impl StreamConsumerSessionGroup {
    pub(crate) fn new(shard_count: u32) -> Self {
        Self {
            shard_count,
            generation: 0,
            watermark_ms: 0,
            members: BTreeMap::new(),
        }
    }

    pub(crate) fn apply(
        &mut self,
        command: &StreamGroupSessionCommand,
        applied_at_ms: u64,
    ) -> StreamSessionTransition {
        if command.shard_count != self.shard_count {
            return rejected(StreamTabletSessionRejection::ShardCountMismatch);
        }

        self.watermark_ms = self.watermark_ms.max(applied_at_ms);
        let expired_members = self.expire_members();
        let mut membership_changed = !expired_members.is_empty();
        let rejection = match &command.action {
            StreamGroupSessionAction::Join {
                member_id,
                session_timeout_ms,
            } => {
                if let Some(deadline_ms) = self.watermark_ms.checked_add(*session_timeout_ms) {
                    if let Some(member) = self.members.get_mut(member_id) {
                        member.session_timeout_ms = *session_timeout_ms;
                        member.deadline_ms = deadline_ms;
                        None
                    } else if self.members.len() >= MAX_STREAM_CONSUMER_MEMBERS_PER_GROUP {
                        Some(StreamTabletSessionRejection::MemberCapacityReached)
                    } else {
                        self.members.insert(
                            member_id.clone(),
                            StreamConsumerSessionMember {
                                session_timeout_ms: *session_timeout_ms,
                                deadline_ms,
                            },
                        );
                        membership_changed = true;
                        None
                    }
                } else {
                    Some(StreamTabletSessionRejection::DeadlineOverflow)
                }
            }
            StreamGroupSessionAction::Heartbeat {
                member_id,
                group_generation,
            } => {
                if !self.members.contains_key(member_id) {
                    Some(StreamTabletSessionRejection::UnknownMember)
                } else if *group_generation != self.next_generation(membership_changed) {
                    Some(StreamTabletSessionRejection::StaleGeneration)
                } else {
                    let session_timeout_ms = self
                        .members
                        .get(member_id)
                        .expect("membership was checked")
                        .session_timeout_ms;
                    if let Some(deadline_ms) = self.watermark_ms.checked_add(session_timeout_ms) {
                        self.members
                            .get_mut(member_id)
                            .expect("membership was checked")
                            .deadline_ms = deadline_ms;
                        None
                    } else {
                        Some(StreamTabletSessionRejection::DeadlineOverflow)
                    }
                }
            }
            StreamGroupSessionAction::Leave {
                member_id,
                group_generation,
            } => {
                if !self.members.contains_key(member_id) {
                    Some(StreamTabletSessionRejection::UnknownMember)
                } else if *group_generation != self.next_generation(membership_changed) {
                    Some(StreamTabletSessionRejection::StaleGeneration)
                } else {
                    self.members.remove(member_id);
                    membership_changed = true;
                    None
                }
            }
            StreamGroupSessionAction::Maintain => None,
        };

        if membership_changed {
            self.generation = self.generation.saturating_add(1).max(1);
        }
        StreamSessionTransition {
            outcome: if rejection.is_some() {
                StreamTabletSessionOutcome::Rejected
            } else {
                StreamTabletSessionOutcome::Applied
            },
            rejection,
            expired_members,
        }
    }

    pub(crate) fn observation(&self, group: &str) -> StreamTabletSessionObservation {
        StreamTabletSessionObservation {
            exists: true,
            group: group.to_owned(),
            shard_count: Some(self.shard_count),
            group_generation: Some(self.generation),
            watermark_ms: Some(self.watermark_ms),
            members: self.members_with_assignments(),
        }
    }

    pub(crate) fn next_deadline_ms(&self) -> Option<u64> {
        self.members.values().map(|member| member.deadline_ms).min()
    }

    pub(crate) fn members_with_assignments(&self) -> Vec<StreamTabletSessionMember> {
        self.members
            .iter()
            .map(|(member_id, member)| StreamTabletSessionMember {
                member_id: member_id.clone(),
                session_timeout_ms: member.session_timeout_ms,
                deadline_ms: member.deadline_ms,
                assigned_shards: self.assigned_shards(member_id),
            })
            .collect()
    }

    pub(crate) fn assigned_shards(&self, member_id: &str) -> Vec<u32> {
        let Some(member_position) = self.members.keys().position(|member| member == member_id)
        else {
            return Vec::new();
        };
        let member_count = self.members.len();
        (0..self.shard_count)
            .filter(|shard| (*shard as usize) % member_count == member_position)
            .collect()
    }

    pub(crate) fn validate(&self) -> TabletResult<()> {
        validate_shard_count(self.shard_count)?;
        if self.generation == 0 {
            return Err(TabletError::InvalidCommand(
                "Stream session group has a zero generation".into(),
            ));
        }
        if self.members.len() > MAX_STREAM_CONSUMER_MEMBERS_PER_GROUP {
            return Err(TabletError::InvalidCommand(
                "Stream session group exceeds the member bound".into(),
            ));
        }
        for (member_id, member) in &self.members {
            validate_bounded_identifier(
                "consumer member_id",
                member_id,
                MAX_STREAM_CONSUMER_MEMBER_BYTES,
            )?;
            validate_session_timeout(member.session_timeout_ms)?;
            if member.deadline_ms <= self.watermark_ms {
                return Err(TabletError::InvalidCommand(
                    "Stream session snapshot contains an expired active member".into(),
                ));
            }
        }
        Ok(())
    }

    fn next_generation(&self, membership_changed: bool) -> u64 {
        if membership_changed {
            self.generation.saturating_add(1).max(1)
        } else {
            self.generation
        }
    }

    fn expire_members(&mut self) -> Vec<String> {
        let expired = self
            .members
            .iter()
            .filter(|(_, member)| member.deadline_ms <= self.watermark_ms)
            .map(|(member_id, _)| member_id.clone())
            .collect::<Vec<_>>();
        for member_id in &expired {
            self.members.remove(member_id);
        }
        expired
    }
}

pub(crate) fn absent_session_observation(group: &str) -> StreamTabletSessionObservation {
    StreamTabletSessionObservation {
        exists: false,
        group: group.to_owned(),
        shard_count: None,
        group_generation: None,
        watermark_ms: None,
        members: Vec::new(),
    }
}

pub(crate) fn validate_session_command(command: &StreamGroupSessionCommand) -> TabletResult<()> {
    validate_bounded_identifier(
        "consumer group",
        &command.group,
        MAX_STREAM_CONSUMER_GROUP_BYTES,
    )?;
    validate_shard_count(command.shard_count)?;
    match &command.action {
        StreamGroupSessionAction::Join {
            member_id,
            session_timeout_ms,
        } => {
            validate_member(member_id)?;
            validate_session_timeout(*session_timeout_ms)
        }
        StreamGroupSessionAction::Heartbeat {
            member_id,
            group_generation,
        }
        | StreamGroupSessionAction::Leave {
            member_id,
            group_generation,
        } => {
            validate_member(member_id)?;
            if *group_generation == 0 {
                return Err(TabletError::InvalidCommand(
                    "consumer group_generation must be non-zero".into(),
                ));
            }
            Ok(())
        }
        StreamGroupSessionAction::Maintain => Ok(()),
    }
}

pub(crate) fn validate_session_group_count(group_count: usize) -> TabletResult<()> {
    if group_count > MAX_STREAM_CONSUMER_GROUPS {
        return Err(TabletError::InvalidCommand(
            "Stream session snapshot exceeds the consumer-group bound".into(),
        ));
    }
    Ok(())
}

fn validate_member(member_id: &str) -> TabletResult<()> {
    validate_bounded_identifier(
        "consumer member_id",
        member_id,
        MAX_STREAM_CONSUMER_MEMBER_BYTES,
    )
}

fn validate_shard_count(shard_count: u32) -> TabletResult<()> {
    if shard_count == 0 || shard_count > MAX_STREAM_SESSION_SHARDS {
        return Err(TabletError::InvalidCommand(format!(
            "consumer session shard_count must be between 1 and {MAX_STREAM_SESSION_SHARDS}"
        )));
    }
    Ok(())
}

fn validate_session_timeout(session_timeout_ms: u64) -> TabletResult<()> {
    if !(MIN_STREAM_SESSION_TIMEOUT_MS..=MAX_STREAM_SESSION_TIMEOUT_MS)
        .contains(&session_timeout_ms)
    {
        return Err(TabletError::InvalidCommand(format!(
            "session_timeout_ms must be between {MIN_STREAM_SESSION_TIMEOUT_MS} and {MAX_STREAM_SESSION_TIMEOUT_MS}"
        )));
    }
    Ok(())
}

fn rejected(rejection: StreamTabletSessionRejection) -> StreamSessionTransition {
    StreamSessionTransition {
        outcome: StreamTabletSessionOutcome::Rejected,
        rejection: Some(rejection),
        expired_members: Vec::new(),
    }
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

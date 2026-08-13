//! Ordered, replayable, partitioned stream state machine.

use std::collections::{HashMap, VecDeque};

use epoch_core::{AckMetadata, DurabilityProfile, EpochError, EpochResult, EventEnvelope};
use serde::{Deserialize, Serialize};

pub const STREAM_SNAPSHOT_FORMAT_VERSION: u16 = 1;
pub const MAX_STREAM_SNAPSHOT_BYTES: usize = 4 * 1024 * 1024;
/// Stable cross-language partitioner advertised by the regional Stream API.
pub const STREAM_PARTITIONER: &str = "fnv1a64_utf8_mod_n_v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamConfig {
    pub partitions: u32,
    pub durability: DurabilityProfile,
    pub max_records_per_partition: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes_per_partition: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age_ms: Option<u64>,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            partitions: 1,
            durability: DurabilityProfile::Volatile,
            max_records_per_partition: None,
            max_bytes_per_partition: None,
            max_age_ms: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamRetentionPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_records_per_partition: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes_per_partition: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamRecord {
    pub partition: u32,
    pub offset: u64,
    pub appended_at_ms: u64,
    pub envelope: EventEnvelope,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppendReceipt {
    pub partition: u32,
    pub offset: u64,
    pub acknowledgement: AckMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerLag {
    pub group: String,
    pub partition: u32,
    pub base_offset: u64,
    pub committed_offset: u64,
    pub end_offset: u64,
    pub lag: u64,
    pub checkpoint_out_of_range: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamPartitionRetentionReport {
    pub partition: u32,
    pub previous_base_offset: u64,
    pub base_offset: u64,
    pub end_offset: u64,
    pub removed_records: usize,
    pub removed_bytes: u64,
    pub retained_records: usize,
    pub retained_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRetentionReport {
    pub as_of_ms: u64,
    pub cutoff_ms: Option<u64>,
    pub removed_records: usize,
    pub removed_bytes: u64,
    pub partitions: Vec<StreamPartitionRetentionReport>,
}

#[derive(Debug, Clone, Default)]
struct Partition {
    base_offset: u64,
    next_offset: u64,
    records: VecDeque<StreamRecord>,
}

#[derive(Debug, Clone)]
pub struct Stream {
    config: StreamConfig,
    partitions: Vec<Partition>,
    group_offsets: HashMap<(String, u32), u64>,
    dedupe: HashMap<String, AppendReceipt>,
    commit_position: u64,
    retention_watermark_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VersionedStreamSnapshot {
    format_version: u16,
    config: StreamConfig,
    partitions: Vec<PartitionSnapshot>,
    group_offsets: Vec<GroupOffsetSnapshot>,
    dedupe: Vec<DedupeSnapshot>,
    commit_position: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retention_watermark_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PartitionSnapshot {
    base_offset: u64,
    next_offset: u64,
    records: Vec<StreamRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupOffsetSnapshot {
    group: String,
    partition: u32,
    next_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DedupeSnapshot {
    dedupe_id: String,
    receipt: AppendReceipt,
}

impl Stream {
    pub fn new(config: StreamConfig) -> EpochResult<Self> {
        if config.partitions == 0 {
            return Err(EpochError::InvalidArgument(
                "stream must have at least one partition".into(),
            ));
        }
        validate_retention_policy(config.retention_policy())?;
        let partitions = (0..config.partitions)
            .map(|_| Partition::default())
            .collect();
        Ok(Self {
            config,
            partitions,
            group_offsets: HashMap::new(),
            dedupe: HashMap::new(),
            commit_position: 0,
            retention_watermark_ms: None,
        })
    }

    pub fn config(&self) -> &StreamConfig {
        &self.config
    }

    pub fn retention_policy(&self) -> StreamRetentionPolicy {
        self.config.retention_policy()
    }

    pub const fn retention_watermark_ms(&self) -> Option<u64> {
        self.retention_watermark_ms
    }

    pub fn append(
        &mut self,
        envelope: EventEnvelope,
        requested_partition: Option<u32>,
        now_ms: u64,
    ) -> EpochResult<AppendReceipt> {
        envelope.validate()?;
        if let Some(dedupe_id) = &envelope.dedupe_id
            && let Some(original) = self.dedupe.get(dedupe_id)
        {
            let mut duplicate = original.clone();
            duplicate.acknowledgement.duplicate = true;
            return Ok(duplicate);
        }
        let partition_id = match requested_partition {
            Some(partition) if partition < self.config.partitions => partition,
            Some(partition) => {
                return Err(EpochError::InvalidArgument(format!(
                    "partition {partition} does not exist"
                )));
            }
            None => stream_partition_for(
                envelope
                    .key
                    .as_deref()
                    .filter(|key| !key.is_empty())
                    .unwrap_or(&envelope.id),
                self.config.partitions,
            )?,
        };
        let effective_now_ms = self.retention_watermark_ms.unwrap_or(now_ms).max(now_ms);
        let partition = &mut self.partitions[partition_id as usize];
        let offset = partition.next_offset;
        let record = StreamRecord {
            partition: partition_id,
            offset,
            appended_at_ms: effective_now_ms,
            envelope: envelope.clone(),
        };
        let record_bytes = retained_record_bytes(&record)?;
        if self
            .config
            .max_bytes_per_partition
            .is_some_and(|limit| record_bytes > limit)
        {
            return Err(EpochError::Capacity(format!(
                "record exceeds the {0}-byte per-partition retention limit",
                self.config.max_bytes_per_partition.unwrap_or_default()
            )));
        }
        partition.next_offset = partition.next_offset.saturating_add(1);
        partition.records.push_back(record);
        self.maintain_retention(effective_now_ms)?;
        self.commit_position = self.commit_position.saturating_add(1);
        let receipt = AppendReceipt {
            partition: partition_id,
            offset,
            acknowledgement: AckMetadata::standalone(self.commit_position, self.config.durability),
        };
        if let Some(dedupe_id) = envelope.dedupe_id {
            self.dedupe.insert(dedupe_id, receipt.clone());
        }
        Ok(receipt)
    }

    pub fn configure_retention(
        &mut self,
        policy: StreamRetentionPolicy,
        now_ms: u64,
    ) -> EpochResult<StreamRetentionReport> {
        validate_retention_policy(policy)?;
        self.config.max_records_per_partition = policy.max_records_per_partition;
        self.config.max_bytes_per_partition = policy.max_bytes_per_partition;
        self.config.max_age_ms = policy.max_age_ms;
        self.maintain_retention(now_ms)
    }

    pub fn maintain_retention(&mut self, now_ms: u64) -> EpochResult<StreamRetentionReport> {
        let effective_now_ms = self.retention_watermark_ms.unwrap_or(now_ms).max(now_ms);
        let cutoff_ms = self
            .config
            .max_age_ms
            .and_then(|max_age_ms| effective_now_ms.checked_sub(max_age_ms));
        if self.config.max_age_ms.is_some() {
            self.retention_watermark_ms = Some(effective_now_ms);
        }

        let mut partitions = Vec::with_capacity(self.partitions.len());
        let mut removed_records = 0_usize;
        let mut removed_bytes = 0_u64;
        for partition_id in 0..self.partitions.len() {
            let report = self.maintain_partition(partition_id, cutoff_ms)?;
            removed_records = removed_records.saturating_add(report.removed_records);
            removed_bytes = removed_bytes.saturating_add(report.removed_bytes);
            partitions.push(report);
        }
        Ok(StreamRetentionReport {
            as_of_ms: effective_now_ms,
            cutoff_ms,
            removed_records,
            removed_bytes,
            partitions,
        })
    }

    pub fn retained_bytes(&self, partition_id: u32) -> EpochResult<u64> {
        let partition = self.partition(partition_id)?;
        partition.records.iter().try_fold(0_u64, |total, record| {
            total
                .checked_add(retained_record_bytes(record)?)
                .ok_or_else(|| EpochError::Capacity("Stream retained byte count overflow".into()))
        })
    }

    pub fn fetch(
        &self,
        partition_id: u32,
        offset: u64,
        limit: usize,
    ) -> EpochResult<Vec<StreamRecord>> {
        let partition = self.partition(partition_id)?;
        if offset < partition.base_offset {
            return Err(EpochError::Conflict(format!(
                "offset {offset} was removed by retention; earliest is {}",
                partition.base_offset
            )));
        }
        Ok(partition
            .records
            .iter()
            .filter(|record| record.offset >= offset)
            .take(limit)
            .cloned()
            .collect())
    }

    pub fn commit_offset(
        &mut self,
        group: impl Into<String>,
        partition: u32,
        next_offset: u64,
    ) -> EpochResult<()> {
        let group = group.into();
        if group.is_empty() {
            return Err(EpochError::InvalidArgument(
                "consumer group is required".into(),
            ));
        }
        let end = self.partition(partition)?.next_offset;
        if next_offset > end {
            return Err(EpochError::InvalidArgument(format!(
                "offset {next_offset} is beyond end offset {end}"
            )));
        }
        let current = self
            .group_offsets
            .entry((group, partition))
            .or_insert(next_offset);
        if next_offset < *current {
            return Err(EpochError::Conflict(
                "commit cannot move backwards; use reset_offset".into(),
            ));
        }
        *current = next_offset;
        Ok(())
    }

    pub fn reset_offset(
        &mut self,
        group: impl Into<String>,
        partition: u32,
        next_offset: u64,
    ) -> EpochResult<()> {
        let partition_state = self.partition(partition)?;
        if next_offset < partition_state.base_offset || next_offset > partition_state.next_offset {
            return Err(EpochError::InvalidArgument(format!(
                "offset must be within retained range {}..={}",
                partition_state.base_offset, partition_state.next_offset
            )));
        }
        self.group_offsets
            .insert((group.into(), partition), next_offset);
        Ok(())
    }

    pub fn lag(&self, group: &str, partition_id: u32) -> EpochResult<ConsumerLag> {
        let partition = self.partition(partition_id)?;
        let committed_offset = self
            .group_offsets
            .get(&(group.to_owned(), partition_id))
            .copied()
            .unwrap_or(partition.base_offset);
        Ok(ConsumerLag {
            group: group.to_owned(),
            partition: partition_id,
            base_offset: partition.base_offset,
            committed_offset,
            end_offset: partition.next_offset,
            lag: partition
                .next_offset
                .saturating_sub(committed_offset.max(partition.base_offset)),
            checkpoint_out_of_range: committed_offset < partition.base_offset,
        })
    }

    pub fn offsets(&self, partition_id: u32) -> EpochResult<(u64, u64)> {
        let partition = self.partition(partition_id)?;
        Ok((partition.base_offset, partition.next_offset))
    }

    pub fn encode_snapshot(&self) -> EpochResult<Vec<u8>> {
        let mut group_offsets = self
            .group_offsets
            .iter()
            .map(|((group, partition), next_offset)| GroupOffsetSnapshot {
                group: group.clone(),
                partition: *partition,
                next_offset: *next_offset,
            })
            .collect::<Vec<_>>();
        group_offsets.sort_by(|left, right| {
            (&left.group, left.partition).cmp(&(&right.group, right.partition))
        });
        let mut dedupe = self
            .dedupe
            .iter()
            .map(|(dedupe_id, receipt)| DedupeSnapshot {
                dedupe_id: dedupe_id.clone(),
                receipt: receipt.clone(),
            })
            .collect::<Vec<_>>();
        dedupe.sort_by(|left, right| left.dedupe_id.cmp(&right.dedupe_id));
        let snapshot = VersionedStreamSnapshot {
            format_version: STREAM_SNAPSHOT_FORMAT_VERSION,
            config: self.config.clone(),
            partitions: self
                .partitions
                .iter()
                .map(|partition| PartitionSnapshot {
                    base_offset: partition.base_offset,
                    next_offset: partition.next_offset,
                    records: partition.records.iter().cloned().collect(),
                })
                .collect(),
            group_offsets,
            dedupe,
            commit_position: self.commit_position,
            retention_watermark_ms: self.retention_watermark_ms,
        };
        let encoded = serde_json::to_vec(&snapshot)
            .map_err(|error| EpochError::Internal(error.to_string()))?;
        if encoded.len() > MAX_STREAM_SNAPSHOT_BYTES {
            return Err(EpochError::Capacity(format!(
                "Stream snapshot is {} bytes; maximum is {MAX_STREAM_SNAPSHOT_BYTES}",
                encoded.len()
            )));
        }
        Ok(encoded)
    }

    pub fn decode_snapshot(encoded: &[u8]) -> EpochResult<Self> {
        if encoded.len() > MAX_STREAM_SNAPSHOT_BYTES {
            return Err(EpochError::Capacity(format!(
                "Stream snapshot is {} bytes; maximum is {MAX_STREAM_SNAPSHOT_BYTES}",
                encoded.len()
            )));
        }
        let snapshot: VersionedStreamSnapshot = serde_json::from_slice(encoded)
            .map_err(|error| EpochError::Storage(format!("invalid Stream snapshot: {error}")))?;
        if snapshot.format_version != STREAM_SNAPSHOT_FORMAT_VERSION {
            return Err(EpochError::InvalidArgument(format!(
                "unsupported Stream snapshot format version {}",
                snapshot.format_version
            )));
        }
        let stream = Self::from_snapshot(snapshot)?;
        if stream.encode_snapshot()?.as_slice() != encoded {
            return Err(EpochError::Storage(
                "Stream snapshot is not canonically encoded".into(),
            ));
        }
        Ok(stream)
    }

    fn from_snapshot(snapshot: VersionedStreamSnapshot) -> EpochResult<Self> {
        let VersionedStreamSnapshot {
            format_version: _,
            config,
            partitions: saved_partitions,
            group_offsets: saved_group_offsets,
            dedupe: saved_dedupe,
            commit_position,
            retention_watermark_ms,
        } = snapshot;
        let expected_partition_count = Self::new(config.clone())?.partitions.len();
        if saved_partitions.len() != expected_partition_count {
            return Err(EpochError::Storage(
                "Stream snapshot partition count does not match its configuration".into(),
            ));
        }
        let (partitions, total_next_offsets) =
            restore_partitions(&config, saved_partitions, retention_watermark_ms)?;
        if commit_position != total_next_offsets {
            return Err(EpochError::Storage(
                "Stream snapshot commit position does not match appended offsets".into(),
            ));
        }
        let group_offsets = restore_group_offsets(saved_group_offsets, &partitions)?;
        let dedupe = restore_dedupe(saved_dedupe, &partitions, &config, commit_position)?;
        Ok(Self {
            config,
            partitions,
            group_offsets,
            dedupe,
            commit_position,
            retention_watermark_ms,
        })
    }

    fn maintain_partition(
        &mut self,
        partition_id: usize,
        cutoff_ms: Option<u64>,
    ) -> EpochResult<StreamPartitionRetentionReport> {
        let partition_number =
            u32::try_from(partition_id).map_err(|error| EpochError::Capacity(error.to_string()))?;
        let previous_base_offset = self.partitions[partition_id].base_offset;
        let mut retained_bytes =
            self.partitions[partition_id]
                .records
                .iter()
                .try_fold(0_u64, |total, record| {
                    total
                        .checked_add(retained_record_bytes(record)?)
                        .ok_or_else(|| {
                            EpochError::Capacity("Stream retained byte count overflow".into())
                        })
                })?;
        let mut removed = Vec::new();
        loop {
            let partition = &self.partitions[partition_id];
            let remove_for_age = cutoff_ms.is_some_and(|cutoff| {
                partition
                    .records
                    .front()
                    .is_some_and(|record| record.appended_at_ms <= cutoff)
            });
            let remove_for_records = self
                .config
                .max_records_per_partition
                .is_some_and(|limit| partition.records.len() > limit);
            let remove_for_bytes = self
                .config
                .max_bytes_per_partition
                .is_some_and(|limit| retained_bytes > limit);
            if !(remove_for_age || remove_for_records || remove_for_bytes) {
                break;
            }
            let Some(record) = self.partitions[partition_id].records.pop_front() else {
                break;
            };
            let bytes = retained_record_bytes(&record)?;
            retained_bytes = retained_bytes.saturating_sub(bytes);
            self.partitions[partition_id].base_offset = record.offset.saturating_add(1);
            removed.push((record, bytes));
        }

        let removed_bytes = removed.iter().map(|(_, bytes)| *bytes).sum();
        for (record, _) in &removed {
            if let Some(dedupe_id) = record.envelope.dedupe_id.as_ref()
                && self.dedupe.get(dedupe_id).is_some_and(|receipt| {
                    receipt.partition == record.partition && receipt.offset == record.offset
                })
            {
                self.dedupe.remove(dedupe_id);
            }
        }
        let partition = &self.partitions[partition_id];
        Ok(StreamPartitionRetentionReport {
            partition: partition_number,
            previous_base_offset,
            base_offset: partition.base_offset,
            end_offset: partition.next_offset,
            removed_records: removed.len(),
            removed_bytes,
            retained_records: partition.records.len(),
            retained_bytes,
        })
    }

    fn partition(&self, id: u32) -> EpochResult<&Partition> {
        self.partitions
            .get(id as usize)
            .ok_or_else(|| EpochError::NotFound(format!("stream partition {id}")))
    }
}

impl StreamConfig {
    pub const fn retention_policy(&self) -> StreamRetentionPolicy {
        StreamRetentionPolicy {
            max_records_per_partition: self.max_records_per_partition,
            max_bytes_per_partition: self.max_bytes_per_partition,
            max_age_ms: self.max_age_ms,
        }
    }
}

fn restore_partitions(
    config: &StreamConfig,
    saved_partitions: Vec<PartitionSnapshot>,
    retention_watermark_ms: Option<u64>,
) -> EpochResult<(Vec<Partition>, u64)> {
    let mut partitions = Vec::with_capacity(saved_partitions.len());
    let mut total_next_offsets = 0_u64;
    for (partition_id, saved) in saved_partitions.into_iter().enumerate() {
        let partition_id =
            u32::try_from(partition_id).map_err(|error| EpochError::Capacity(error.to_string()))?;
        let partition = restore_partition(config, saved, retention_watermark_ms, partition_id)?;
        total_next_offsets = total_next_offsets
            .checked_add(partition.next_offset)
            .ok_or_else(|| EpochError::Capacity("Stream offset sum overflow".into()))?;
        partitions.push(partition);
    }
    Ok((partitions, total_next_offsets))
}

fn restore_partition(
    config: &StreamConfig,
    saved: PartitionSnapshot,
    retention_watermark_ms: Option<u64>,
    partition_id: u32,
) -> EpochResult<Partition> {
    if saved.base_offset > saved.next_offset {
        return Err(EpochError::Storage(
            "Stream snapshot partition offsets regress".into(),
        ));
    }
    if config
        .max_records_per_partition
        .is_some_and(|limit| saved.records.len() > limit)
    {
        return Err(EpochError::Storage(
            "Stream snapshot exceeds its configured retention bound".into(),
        ));
    }
    let retained_bytes = saved.records.iter().try_fold(0_u64, |total, record| {
        total
            .checked_add(retained_record_bytes(record)?)
            .ok_or_else(|| EpochError::Capacity("Stream retained byte count overflow".into()))
    })?;
    if config
        .max_bytes_per_partition
        .is_some_and(|limit| retained_bytes > limit)
    {
        return Err(EpochError::Storage(
            "Stream snapshot exceeds its configured byte-retention bound".into(),
        ));
    }
    let cutoff_ms = retention_watermark_ms.and_then(|watermark| {
        config
            .max_age_ms
            .and_then(|max_age_ms| watermark.checked_sub(max_age_ms))
    });
    if cutoff_ms.is_some_and(|cutoff| {
        saved
            .records
            .iter()
            .any(|record| record.appended_at_ms <= cutoff)
    }) {
        return Err(EpochError::Storage(
            "Stream snapshot contains records beyond its age-retention boundary".into(),
        ));
    }
    let expected_len = saved.next_offset.saturating_sub(saved.base_offset);
    if u64::try_from(saved.records.len()).ok() != Some(expected_len) {
        return Err(EpochError::Storage(
            "Stream snapshot retained offsets are not contiguous".into(),
        ));
    }
    for (offset, record) in (saved.base_offset..saved.next_offset).zip(&saved.records) {
        if record.partition != partition_id || record.offset != offset {
            return Err(EpochError::Storage(
                "Stream snapshot record position is invalid".into(),
            ));
        }
        record.envelope.validate()?;
    }
    Ok(Partition {
        base_offset: saved.base_offset,
        next_offset: saved.next_offset,
        records: saved.records.into(),
    })
}

fn restore_group_offsets(
    saved_offsets: Vec<GroupOffsetSnapshot>,
    partitions: &[Partition],
) -> EpochResult<HashMap<(String, u32), u64>> {
    let mut group_offsets = HashMap::new();
    let mut previous_group: Option<(String, u32)> = None;
    for offset in saved_offsets {
        let key = (offset.group, offset.partition);
        let partition = partitions.get(key.1 as usize);
        if key.0.is_empty()
            || previous_group
                .as_ref()
                .is_some_and(|previous| previous >= &key)
            || partition.is_none_or(|partition| offset.next_offset > partition.next_offset)
            || group_offsets
                .insert(key.clone(), offset.next_offset)
                .is_some()
        {
            return Err(EpochError::Storage(
                "Stream snapshot consumer-group offsets are invalid".into(),
            ));
        }
        previous_group = Some(key);
    }
    Ok(group_offsets)
}

fn restore_dedupe(
    saved_dedupe: Vec<DedupeSnapshot>,
    partitions: &[Partition],
    config: &StreamConfig,
    commit_position: u64,
) -> EpochResult<HashMap<String, AppendReceipt>> {
    let mut dedupe = HashMap::new();
    let mut previous_dedupe: Option<String> = None;
    for entry in saved_dedupe {
        if entry.dedupe_id.is_empty()
            || previous_dedupe
                .as_ref()
                .is_some_and(|previous| previous >= &entry.dedupe_id)
            || retained_record_for_receipt(partitions, &entry.receipt)
                .and_then(|record| record.envelope.dedupe_id.as_deref())
                != Some(entry.dedupe_id.as_str())
            || !valid_snapshot_acknowledgement(&entry.receipt, config, commit_position)
            || dedupe
                .insert(entry.dedupe_id.clone(), entry.receipt)
                .is_some()
        {
            return Err(EpochError::Storage(
                "Stream snapshot deduplication registry is invalid".into(),
            ));
        }
        previous_dedupe = Some(entry.dedupe_id);
    }
    Ok(dedupe)
}

fn valid_snapshot_acknowledgement(
    receipt: &AppendReceipt,
    config: &StreamConfig,
    commit_position: u64,
) -> bool {
    let acknowledgement = &receipt.acknowledgement;
    acknowledgement.durability == config.durability
        && acknowledgement.resource_epoch == 1
        && acknowledgement.commit_position > 0
        && acknowledgement.commit_position <= commit_position
        && acknowledgement.replica_acks == 1
        && !acknowledgement.duplicate
}

fn validate_retention_policy(policy: StreamRetentionPolicy) -> EpochResult<()> {
    if policy.max_records_per_partition == Some(0) {
        return Err(EpochError::InvalidArgument(
            "retention record limit must be greater than zero".into(),
        ));
    }
    if policy.max_bytes_per_partition == Some(0) {
        return Err(EpochError::InvalidArgument(
            "retention byte limit must be greater than zero".into(),
        ));
    }
    if policy.max_age_ms == Some(0) {
        return Err(EpochError::InvalidArgument(
            "retention age must be greater than zero".into(),
        ));
    }
    Ok(())
}

fn retained_record_bytes(record: &StreamRecord) -> EpochResult<u64> {
    let encoded = serde_json::to_vec(record)
        .map_err(|error| EpochError::Internal(format!("record encoding failed: {error}")))?;
    u64::try_from(encoded.len()).map_err(|error| EpochError::Capacity(error.to_string()))
}

fn retained_record_for_receipt<'a>(
    partitions: &'a [Partition],
    receipt: &AppendReceipt,
) -> Option<&'a StreamRecord> {
    let partition = partitions.get(receipt.partition as usize)?;
    let index = receipt.offset.checked_sub(partition.base_offset)?;
    let index = usize::try_from(index).ok()?;
    partition
        .records
        .get(index)
        .filter(|record| record.partition == receipt.partition && record.offset == receipt.offset)
}

/// Maps UTF-8 bytes to a logical Stream partition with unsigned FNV-1a 64-bit
/// arithmetic. The identifier in [`STREAM_PARTITIONER`] versions this exact
/// algorithm for first-party and external clients.
pub fn stream_partition_for(value: &str, partitions: u32) -> EpochResult<u32> {
    if partitions == 0 {
        return Err(EpochError::InvalidArgument(
            "stream partition count must be greater than zero".into(),
        ));
    }
    let hash = value.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    Ok(u32::try_from(hash % u64::from(partitions)).expect("modulo fits u32"))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn event(id: &str, key: Option<&str>) -> EventEnvelope {
        let mut event = EventEnvelope::new("tests", "test.event", json!({"id": id}), 1);
        event.id = id.into();
        event.key = key.map(str::to_owned);
        event
    }

    #[test]
    fn records_are_ordered_and_replayable_per_partition() {
        let mut stream = Stream::new(StreamConfig {
            partitions: 2,
            ..StreamConfig::default()
        })
        .unwrap();
        let first = stream.append(event("one", None), Some(1), 10).unwrap();
        let second = stream.append(event("two", None), Some(1), 11).unwrap();
        assert_eq!((first.offset, second.offset), (0, 1));
        let replay = stream.fetch(1, 0, 10).unwrap();
        assert_eq!(
            replay
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            [0, 1]
        );
    }

    #[test]
    fn key_partitioning_is_stable() {
        let mut stream = Stream::new(StreamConfig {
            partitions: 8,
            ..StreamConfig::default()
        })
        .unwrap();
        let a = stream
            .append(event("one", Some("customer-42")), None, 1)
            .unwrap();
        let b = stream
            .append(event("two", Some("customer-42")), None, 2)
            .unwrap();
        assert_eq!(a.partition, b.partition);
    }

    #[test]
    fn published_partitioning_vectors_are_stable_across_utf8_clients() {
        assert_eq!(STREAM_PARTITIONER, "fnv1a64_utf8_mod_n_v1");
        assert_eq!(stream_partition_for("customer-42", 16).unwrap(), 14);
        assert_eq!(stream_partition_for("order-1", 16).unwrap(), 13);
        assert_eq!(stream_partition_for("café", 16).unwrap(), 9);
        assert_eq!(stream_partition_for("東京", 16).unwrap(), 15);
        assert!(stream_partition_for("customer-42", 0).is_err());
    }

    #[test]
    fn empty_partition_key_falls_back_to_the_event_id() {
        let mut stream = Stream::new(StreamConfig {
            partitions: 16,
            ..StreamConfig::default()
        })
        .unwrap();
        let receipt = stream.append(event("order-1", Some("")), None, 1).unwrap();
        assert_eq!(receipt.partition, 13);
    }

    #[test]
    fn group_offsets_report_lag_and_require_explicit_rewind() {
        let mut stream = Stream::new(StreamConfig::default()).unwrap();
        stream.append(event("one", None), None, 1).unwrap();
        stream.append(event("two", None), None, 2).unwrap();
        stream.commit_offset("workers", 0, 1).unwrap();
        assert_eq!(stream.lag("workers", 0).unwrap().lag, 1);
        assert!(stream.commit_offset("workers", 0, 0).is_err());
        stream.reset_offset("workers", 0, 0).unwrap();
        assert_eq!(stream.lag("workers", 0).unwrap().lag, 2);
    }

    #[test]
    fn retention_reports_truncated_offsets() {
        let mut stream = Stream::new(StreamConfig {
            max_records_per_partition: Some(1),
            ..StreamConfig::default()
        })
        .unwrap();
        stream.append(event("one", None), None, 1).unwrap();
        stream.append(event("two", None), None, 2).unwrap();
        assert!(stream.fetch(0, 0, 10).is_err());
        assert_eq!(stream.fetch(0, 1, 10).unwrap()[0].envelope.id, "two");
    }

    #[test]
    fn time_retention_expires_at_the_inclusive_age_boundary() {
        let mut stream = Stream::new(StreamConfig {
            max_age_ms: Some(10),
            ..StreamConfig::default()
        })
        .unwrap();
        stream.append(event("one", None), None, 100).unwrap();
        stream.append(event("two", None), None, 109).unwrap();

        let report = stream.maintain_retention(110).unwrap();

        assert_eq!(report.removed_records, 1);
        assert_eq!(report.partitions[0].previous_base_offset, 0);
        assert_eq!(report.partitions[0].base_offset, 1);
        assert_eq!(stream.fetch(0, 1, 10).unwrap()[0].envelope.id, "two");
    }

    #[test]
    fn byte_retention_uses_canonical_record_bytes_and_prunes_deduplication() {
        let mut sizing = Stream::new(StreamConfig::default()).unwrap();
        let mut first = event("one", None);
        first.dedupe_id = Some("request-one".into());
        sizing.append(first.clone(), None, 100).unwrap();
        let one_record_bytes = sizing.retained_bytes(0).unwrap();

        let mut stream = Stream::new(StreamConfig {
            max_bytes_per_partition: Some(one_record_bytes),
            ..StreamConfig::default()
        })
        .unwrap();
        stream.append(first.clone(), None, 100).unwrap();
        stream.append(event("two", None), None, 101).unwrap();

        assert_eq!(stream.offsets(0).unwrap(), (1, 2));
        assert!(stream.retained_bytes(0).unwrap() <= one_record_bytes);
        let retried = stream.append(first, None, 102).unwrap();
        assert_eq!(retried.offset, 2);
        assert!(!retried.acknowledgement.duplicate);
    }

    #[test]
    fn combined_retention_applies_age_bytes_and_record_bounds_together() {
        let mut sizing = Stream::new(StreamConfig::default()).unwrap();
        sizing.append(event("one", None), None, 1).unwrap();
        let one_record_bytes = sizing.retained_bytes(0).unwrap();

        let mut stream = Stream::new(StreamConfig {
            max_records_per_partition: Some(2),
            max_bytes_per_partition: Some(one_record_bytes * 2),
            max_age_ms: Some(10),
            ..StreamConfig::default()
        })
        .unwrap();
        stream.append(event("one", None), None, 1).unwrap();
        stream.append(event("two", None), None, 2).unwrap();
        stream.append(event("three", None), None, 20).unwrap();

        assert_eq!(stream.offsets(0).unwrap(), (2, 3));
        assert_eq!(stream.fetch(0, 2, 10).unwrap()[0].envelope.id, "three");
    }

    #[test]
    fn retention_marks_stale_consumer_checkpoints_until_explicit_reset() {
        let mut stream = Stream::new(StreamConfig {
            max_records_per_partition: Some(1),
            ..StreamConfig::default()
        })
        .unwrap();
        stream.append(event("one", None), None, 1).unwrap();
        stream.commit_offset("workers", 0, 0).unwrap();
        stream.append(event("two", None), None, 2).unwrap();

        let lag = stream.lag("workers", 0).unwrap();
        assert_eq!(lag.base_offset, 1);
        assert_eq!(lag.committed_offset, 0);
        assert!(lag.checkpoint_out_of_range);
        assert_eq!(lag.lag, 1);
        assert!(stream.fetch(0, lag.committed_offset, 10).is_err());

        stream.reset_offset("workers", 0, lag.base_offset).unwrap();
        assert!(!stream.lag("workers", 0).unwrap().checkpoint_out_of_range);
    }

    #[test]
    fn snapshot_round_trip_preserves_retention_policy_boundary_and_watermark() {
        let mut stream = Stream::new(StreamConfig {
            max_records_per_partition: Some(3),
            max_bytes_per_partition: Some(32 * 1024),
            max_age_ms: Some(10),
            ..StreamConfig::default()
        })
        .unwrap();
        stream.append(event("one", None), None, 1).unwrap();
        stream.append(event("two", None), None, 2).unwrap();
        stream.maintain_retention(11).unwrap();
        let encoded = stream.encode_snapshot().unwrap();

        let restored = Stream::decode_snapshot(&encoded).unwrap();

        assert_eq!(restored.config(), stream.config());
        assert_eq!(restored.offsets(0).unwrap(), (1, 2));
        assert_eq!(restored.retention_watermark_ms(), Some(11));
        assert_eq!(restored.encode_snapshot().unwrap(), encoded);
    }

    #[test]
    fn disabling_and_reenabling_age_retention_never_regresses_its_watermark() {
        let mut stream = Stream::new(StreamConfig {
            max_age_ms: Some(10),
            ..StreamConfig::default()
        })
        .unwrap();
        stream.append(event("one", None), None, 100).unwrap();
        stream.maintain_retention(110).unwrap();

        stream
            .configure_retention(StreamRetentionPolicy::default(), 90)
            .unwrap();
        assert_eq!(stream.retention_watermark_ms(), Some(110));
        let encoded = stream.encode_snapshot().unwrap();
        let mut restored = Stream::decode_snapshot(&encoded).unwrap();

        restored.append(event("two", None), None, 95).unwrap();
        assert_eq!(restored.fetch(0, 1, 10).unwrap()[0].appended_at_ms, 110);
        restored
            .configure_retention(
                StreamRetentionPolicy {
                    max_age_ms: Some(10),
                    ..StreamRetentionPolicy::default()
                },
                100,
            )
            .unwrap();
        assert_eq!(restored.retention_watermark_ms(), Some(110));
        assert_eq!(restored.offsets(0).unwrap(), (1, 2));
    }

    #[test]
    fn dedupe_id_returns_original_position() {
        let mut stream = Stream::new(StreamConfig::default()).unwrap();
        let mut value = event("one", None);
        value.dedupe_id = Some("request-1".into());
        let original = stream.append(value.clone(), None, 1).unwrap();
        let duplicate = stream.append(value, None, 2).unwrap();
        assert_eq!(original.offset, duplicate.offset);
        assert!(duplicate.acknowledgement.duplicate);
    }

    #[test]
    fn native_snapshot_restores_records_offsets_groups_and_deduplication() {
        let mut stream = Stream::new(StreamConfig {
            partitions: 2,
            max_records_per_partition: Some(2),
            ..StreamConfig::default()
        })
        .unwrap();
        let mut first = event("one", None);
        first.dedupe_id = Some("dedupe-one".into());
        stream.append(first.clone(), Some(1), 10).unwrap();
        stream.append(event("two", None), Some(1), 11).unwrap();
        stream.commit_offset("workers", 1, 1).unwrap();
        let encoded = stream.encode_snapshot().unwrap();

        let mut restored = Stream::decode_snapshot(&encoded).unwrap();
        assert_eq!(
            restored.fetch(1, 0, 10).unwrap(),
            stream.fetch(1, 0, 10).unwrap()
        );
        assert_eq!(
            restored.lag("workers", 1).unwrap(),
            stream.lag("workers", 1).unwrap()
        );
        assert!(
            restored
                .append(first, Some(1), 12)
                .unwrap()
                .acknowledgement
                .duplicate
        );
        assert_eq!(restored.encode_snapshot().unwrap(), encoded);
    }

    #[test]
    fn native_snapshot_rejects_noncanonical_and_inconsistent_state() {
        let mut stream = Stream::new(StreamConfig::default()).unwrap();
        stream.append(event("one", None), None, 1).unwrap();
        let encoded = stream.encode_snapshot().unwrap();
        let pretty = serde_json::to_vec_pretty(
            &serde_json::from_slice::<serde_json::Value>(&encoded).unwrap(),
        )
        .unwrap();
        assert!(Stream::decode_snapshot(&pretty).is_err());

        let mut invalid: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        invalid["commit_position"] = serde_json::json!(99);
        assert!(Stream::decode_snapshot(&serde_json::to_vec(&invalid).unwrap()).is_err());

        let mut retained = Stream::new(StreamConfig {
            max_records_per_partition: Some(1),
            ..StreamConfig::default()
        })
        .unwrap();
        let mut removed = event("removed", None);
        removed.dedupe_id = Some("removed-request".into());
        let removed_receipt = retained.append(removed, None, 1).unwrap();
        retained.append(event("retained", None), None, 2).unwrap();
        let mut invalid: VersionedStreamSnapshot =
            serde_json::from_slice(&retained.encode_snapshot().unwrap()).unwrap();
        invalid.dedupe = vec![DedupeSnapshot {
            dedupe_id: "removed-request".into(),
            receipt: removed_receipt,
        }];
        assert!(Stream::from_snapshot(invalid).is_err());
    }
}

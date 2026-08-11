//! Ordered, replayable, partitioned stream state machine.

use std::collections::{HashMap, VecDeque};

use epoch_core::{AckMetadata, DurabilityProfile, EpochError, EpochResult, EventEnvelope};
use serde::{Deserialize, Serialize};

pub const STREAM_SNAPSHOT_FORMAT_VERSION: u16 = 1;
pub const MAX_STREAM_SNAPSHOT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamConfig {
    pub partitions: u32,
    pub durability: DurabilityProfile,
    pub max_records_per_partition: Option<usize>,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            partitions: 1,
            durability: DurabilityProfile::Volatile,
            max_records_per_partition: None,
        }
    }
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
    pub committed_offset: u64,
    pub end_offset: u64,
    pub lag: u64,
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
        if config.max_records_per_partition == Some(0) {
            return Err(EpochError::InvalidArgument(
                "retention record limit must be greater than zero".into(),
            ));
        }
        let partitions = (0..config.partitions)
            .map(|_| Partition::default())
            .collect();
        Ok(Self {
            config,
            partitions,
            group_offsets: HashMap::new(),
            dedupe: HashMap::new(),
            commit_position: 0,
        })
    }

    pub fn config(&self) -> &StreamConfig {
        &self.config
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
            None => select_partition(
                envelope.key.as_deref(),
                &envelope.id,
                self.config.partitions,
            ),
        };
        let partition = &mut self.partitions[partition_id as usize];
        let offset = partition.next_offset;
        partition.next_offset = partition.next_offset.saturating_add(1);
        partition.records.push_back(StreamRecord {
            partition: partition_id,
            offset,
            appended_at_ms: now_ms,
            envelope: envelope.clone(),
        });
        if let Some(limit) = self.config.max_records_per_partition {
            while partition.records.len() > limit {
                if let Some(removed) = partition.records.pop_front() {
                    partition.base_offset = removed.offset.saturating_add(1);
                }
            }
        }
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
            committed_offset,
            end_offset: partition.next_offset,
            lag: partition.next_offset.saturating_sub(committed_offset),
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
        let empty = Self::new(snapshot.config.clone())?;
        if snapshot.partitions.len() != empty.partitions.len() {
            return Err(EpochError::Storage(
                "Stream snapshot partition count does not match its configuration".into(),
            ));
        }
        let mut partitions = Vec::with_capacity(snapshot.partitions.len());
        let mut total_next_offsets = 0_u64;
        for (partition_id, saved) in snapshot.partitions.into_iter().enumerate() {
            if saved.base_offset > saved.next_offset {
                return Err(EpochError::Storage(
                    "Stream snapshot partition offsets regress".into(),
                ));
            }
            if snapshot
                .config
                .max_records_per_partition
                .is_some_and(|limit| saved.records.len() > limit)
            {
                return Err(EpochError::Storage(
                    "Stream snapshot exceeds its configured retention bound".into(),
                ));
            }
            let expected_len = saved.next_offset.saturating_sub(saved.base_offset);
            if u64::try_from(saved.records.len()).ok() != Some(expected_len) {
                return Err(EpochError::Storage(
                    "Stream snapshot retained offsets are not contiguous".into(),
                ));
            }
            let partition_id = u32::try_from(partition_id)
                .map_err(|error| EpochError::Capacity(error.to_string()))?;
            for (offset, record) in (saved.base_offset..saved.next_offset).zip(&saved.records) {
                if record.partition != partition_id || record.offset != offset {
                    return Err(EpochError::Storage(
                        "Stream snapshot record position is invalid".into(),
                    ));
                }
                record.envelope.validate()?;
            }
            total_next_offsets = total_next_offsets
                .checked_add(saved.next_offset)
                .ok_or_else(|| EpochError::Capacity("Stream offset sum overflow".into()))?;
            partitions.push(Partition {
                base_offset: saved.base_offset,
                next_offset: saved.next_offset,
                records: saved.records.into(),
            });
        }
        if snapshot.commit_position != total_next_offsets {
            return Err(EpochError::Storage(
                "Stream snapshot commit position does not match appended offsets".into(),
            ));
        }

        let mut group_offsets = HashMap::new();
        let mut previous_group: Option<(String, u32)> = None;
        for offset in snapshot.group_offsets {
            let key = (offset.group, offset.partition);
            if key.0.is_empty()
                || key.1 >= snapshot.config.partitions
                || previous_group
                    .as_ref()
                    .is_some_and(|previous| previous >= &key)
                || offset.next_offset > partitions[key.1 as usize].next_offset
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
        let mut dedupe = HashMap::new();
        let mut previous_dedupe: Option<String> = None;
        for entry in snapshot.dedupe {
            if entry.dedupe_id.is_empty()
                || previous_dedupe
                    .as_ref()
                    .is_some_and(|previous| previous >= &entry.dedupe_id)
                || entry.receipt.partition >= snapshot.config.partitions
                || entry.receipt.offset >= partitions[entry.receipt.partition as usize].next_offset
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
        Ok(Self {
            config: snapshot.config,
            partitions,
            group_offsets,
            dedupe,
            commit_position: snapshot.commit_position,
        })
    }

    fn partition(&self, id: u32) -> EpochResult<&Partition> {
        self.partitions
            .get(id as usize)
            .ok_or_else(|| EpochError::NotFound(format!("stream partition {id}")))
    }
}

fn select_partition(key: Option<&str>, fallback: &str, partitions: u32) -> u32 {
    let value = key.unwrap_or(fallback);
    let hash = value.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    u32::try_from(hash % u64::from(partitions)).expect("modulo fits u32")
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
    }
}

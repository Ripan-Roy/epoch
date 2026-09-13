use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;

use crate::backend::{
    BackendError, CacheAtomicMutation, CacheCollectionMutation, CacheCollectionResult, CacheEntry,
    CacheMultiSetEntry, CachePubSubMessage, CacheSetCondition, CacheSetOptions, CacheSetOutcome,
    CacheSnapshot, CacheStorageClass, CacheValue, CompatibilityBackend, QueueDelivery,
    QueueMessage, StreamGroupIdentity, StreamGroupMember, StreamGroupRejection, StreamGroupSession,
    StreamGroupSessionResult, StreamRecord, plan_collection_mutation,
    validate_cache_atomic_mutations, validate_cache_multi_set,
};

#[derive(Debug, Default)]
pub struct MemoryBackend {
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    version: u64,
    caches: BTreeMap<(String, String), CacheEntry>,
    streams: BTreeMap<(String, u32), Vec<StreamRecord>>,
    stream_partitions: BTreeMap<String, u32>,
    offsets: BTreeMap<(String, String, u32), u64>,
    stream_groups: BTreeMap<(String, String), MemoryStreamGroup>,
    stream_producers: BTreeMap<(String, u32, i64), MemoryStreamProducer>,
    group_claims: BTreeMap<(String, String, u32), (String, u64)>,
    queues: BTreeMap<String, VecDeque<QueueMessage>>,
    queue_dead_letter_targets: BTreeMap<String, String>,
    leases: BTreeMap<String, (String, QueueMessage)>,
    next_lease: u64,
    pubsub_subscriptions: BTreeMap<String, MemoryPubSubSubscription>,
    next_pubsub_subscription: u64,
}

#[derive(Debug, Default)]
struct MemoryStreamGroup {
    generation: u64,
    members: BTreeMap<String, MemoryGroupMember>,
}

#[derive(Debug)]
struct MemoryGroupMember {
    deadline_ms: u64,
    session_timeout_ms: u64,
}

#[derive(Debug, Default)]
struct MemoryStreamProducer {
    epoch: i16,
    next_sequence: i32,
    history: BTreeMap<i32, (Vec<StreamRecord>, u64)>,
}

#[derive(Debug, Default)]
struct MemoryPubSubSubscription {
    channels: BTreeSet<String>,
    patterns: BTreeSet<String>,
    messages: VecDeque<CachePubSubMessage>,
}

impl MemoryBackend {
    pub fn with_resources(cache: &str, stream: &str, partitions: u32, queue: &str) -> Self {
        let mut state = State::default();
        state.stream_partitions.insert(stream.into(), partitions);
        state.queues.insert(queue.into(), VecDeque::new());
        let backend = Self {
            state: Mutex::new(state),
        };
        let _ = cache;
        backend
    }

    pub fn add_queue(&self, queue: &str) {
        self.state
            .lock()
            .unwrap()
            .queues
            .entry(queue.into())
            .or_default();
    }

    pub fn add_stream(&self, stream: &str, partitions: u32) {
        assert!(partitions > 0, "Stream must have at least one partition");
        self.state
            .lock()
            .unwrap()
            .stream_partitions
            .insert(stream.into(), partitions);
    }

    pub fn configure_queue_dead_letter(&self, queue: &str, target: &str) {
        let mut state = self.state.lock().unwrap();
        assert!(state.queues.contains_key(queue), "source Queue must exist");
        assert!(state.queues.contains_key(target), "target Queue must exist");
        state
            .queue_dead_letter_targets
            .insert(queue.to_owned(), target.to_owned());
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn glob_matches(pattern: &[u8], value: &[u8]) -> bool {
    let (mut pattern_index, mut value_index) = (0, 0);
    let (mut star_index, mut star_value_index) = (None, 0);
    while value_index < value.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == b'?' || pattern[pattern_index] == value[value_index])
        {
            pattern_index += 1;
            value_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            star_index = Some(pattern_index);
            pattern_index += 1;
            star_value_index = value_index;
        } else if let Some(star) = star_index {
            pattern_index = star + 1;
            star_value_index += 1;
            value_index = star_value_index;
        } else {
            return false;
        }
    }
    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

fn live_entry(state: &mut State, cache: &str, key: &str) -> Option<CacheEntry> {
    let identity = (cache.to_owned(), key.to_owned());
    if state
        .caches
        .get(&identity)
        .and_then(|entry| entry.expires_at_ms)
        .is_some_and(|deadline| deadline <= now_ms())
    {
        state.caches.remove(&identity);
    }
    state.caches.get(&identity).cloned()
}

fn expire_group_members(group: &mut MemoryStreamGroup) {
    let before = group.members.len();
    let now = now_ms();
    group.members.retain(|_, member| member.deadline_ms > now);
    if group.members.len() != before {
        group.generation = group.generation.saturating_add(1);
    }
}

fn assigned_partitions(
    group: &MemoryStreamGroup,
    partition_count: u32,
    member_id: &str,
) -> Vec<u32> {
    let Some(member_index) = group
        .members
        .keys()
        .position(|candidate| candidate == member_id)
    else {
        return Vec::new();
    };
    let member_count = group.members.len();
    (0..partition_count)
        .filter(|partition| {
            usize::try_from(*partition)
                .is_ok_and(|partition| partition % member_count == member_index)
        })
        .collect()
}

fn memory_session_result(
    group: &MemoryStreamGroup,
    partition_count: u32,
    member_id: &str,
    rejection: Option<StreamGroupRejection>,
) -> StreamGroupSessionResult {
    StreamGroupSessionResult {
        session: StreamGroupSession {
            generation: group.generation,
            members: group
                .members
                .keys()
                .map(|member_id| StreamGroupMember {
                    member_id: member_id.clone(),
                    assigned_partitions: assigned_partitions(group, partition_count, member_id),
                })
                .collect(),
            assigned_partitions: assigned_partitions(group, partition_count, member_id),
        },
        rejection,
    }
}

fn empty_session_result(rejection: StreamGroupRejection) -> StreamGroupSessionResult {
    StreamGroupSessionResult {
        session: StreamGroupSession {
            generation: 0,
            members: Vec::new(),
            assigned_partitions: Vec::new(),
        },
        rejection: Some(rejection),
    }
}

#[async_trait]
impl CompatibilityBackend for MemoryBackend {
    async fn cache_get(&self, cache: &str, key: &str) -> Result<Option<CacheEntry>, BackendError> {
        Ok(live_entry(&mut self.state.lock().unwrap(), cache, key))
    }

    async fn cache_snapshot(
        &self,
        cache: &str,
        keys: &[String],
    ) -> Result<CacheSnapshot, BackendError> {
        let keys = keys.iter().collect::<std::collections::BTreeSet<_>>();
        if keys.is_empty() || keys.len() > crate::backend::MAX_CACHE_MULTI_SET_ENTRIES {
            return Err(BackendError::Invalid(
                "invalid Cache snapshot key count".into(),
            ));
        }
        let mut state = self.state.lock().unwrap();
        let mut entries = BTreeMap::new();
        for key in keys {
            entries.insert(key.clone(), live_entry(&mut state, cache, key));
        }
        Ok(CacheSnapshot {
            revision: state.version,
            entries,
        })
    }

    async fn cache_compare_and_apply(
        &self,
        cache: &str,
        expected_revision: u64,
        mutations: &[CacheAtomicMutation],
    ) -> Result<(), BackendError> {
        validate_cache_atomic_mutations(mutations)?;
        let mut state = self.state.lock().unwrap();
        if state.version != expected_revision {
            return Err(BackendError::Conflict);
        }
        let next_version = state.version.saturating_add(1);
        for mutation in mutations {
            match mutation {
                CacheAtomicMutation::Put {
                    key,
                    value,
                    ttl_ms,
                    storage_class,
                } => {
                    state.caches.insert(
                        (cache.to_owned(), key.clone()),
                        CacheEntry {
                            value: value.clone(),
                            version: next_version,
                            expires_at_ms: ttl_ms.map(|ttl| now_ms().saturating_add(ttl)),
                            storage_class: *storage_class,
                        },
                    );
                }
                CacheAtomicMutation::Delete { key } => {
                    state.caches.remove(&(cache.to_owned(), key.clone()));
                }
            }
        }
        state.version = next_version;
        Ok(())
    }

    async fn cache_pubsub_subscribe(
        &self,
        _cache: &str,
        channels: &[String],
        patterns: &[String],
    ) -> Result<String, BackendError> {
        if channels.is_empty() && patterns.is_empty() {
            return Err(BackendError::Invalid("Pub/Sub filter is required".into()));
        }
        let mut state = self.state.lock().unwrap();
        state.next_pubsub_subscription = state.next_pubsub_subscription.saturating_add(1);
        let id = format!("memory-pubsub-{}", state.next_pubsub_subscription);
        state.pubsub_subscriptions.insert(
            id.clone(),
            MemoryPubSubSubscription {
                channels: channels.iter().cloned().collect(),
                patterns: patterns.iter().cloned().collect(),
                messages: VecDeque::new(),
            },
        );
        Ok(id)
    }

    async fn cache_pubsub_unsubscribe(
        &self,
        _cache: &str,
        subscription_id: &str,
    ) -> Result<(), BackendError> {
        self.state
            .lock()
            .unwrap()
            .pubsub_subscriptions
            .remove(subscription_id)
            .map(|_| ())
            .ok_or(BackendError::NotFound)
    }

    async fn cache_pubsub_publish(
        &self,
        _cache: &str,
        channel: &str,
        payload: &[u8],
    ) -> Result<u64, BackendError> {
        let mut state = self.state.lock().unwrap();
        let mut delivered = 0_u64;
        for subscription in state.pubsub_subscriptions.values_mut() {
            if subscription.channels.contains(channel)
                || subscription
                    .patterns
                    .iter()
                    .any(|pattern| glob_matches(pattern.as_bytes(), channel.as_bytes()))
            {
                subscription.messages.push_back(CachePubSubMessage {
                    channel: channel.to_owned(),
                    payload: payload.to_vec(),
                });
                delivered = delivered.saturating_add(1);
            }
        }
        Ok(delivered)
    }

    async fn cache_pubsub_poll(
        &self,
        _cache: &str,
        subscription_id: &str,
        limit: u16,
    ) -> Result<Vec<CachePubSubMessage>, BackendError> {
        if limit == 0 {
            return Err(BackendError::Invalid(
                "Pub/Sub poll limit is required".into(),
            ));
        }
        let mut state = self.state.lock().unwrap();
        let subscription = state
            .pubsub_subscriptions
            .get_mut(subscription_id)
            .ok_or(BackendError::NotFound)?;
        Ok(subscription
            .messages
            .drain(..usize::from(limit).min(subscription.messages.len()))
            .collect())
    }

    async fn cache_set(
        &self,
        cache: &str,
        key: &str,
        value: CacheValue,
        options: CacheSetOptions,
    ) -> Result<CacheSetOutcome, BackendError> {
        let mut state = self.state.lock().unwrap();
        let current = live_entry(&mut state, cache, key);
        if options.return_previous
            && current.as_ref().is_some_and(|entry| {
                !matches!(
                    entry.value,
                    CacheValue::String(_) | CacheValue::Blob(_) | CacheValue::Counter(_)
                )
            })
        {
            return Err(BackendError::WrongType);
        }
        let condition_matches = match options.condition {
            CacheSetCondition::Always => true,
            CacheSetCondition::Missing => current.is_none(),
            CacheSetCondition::Present => current.is_some(),
        };
        if !condition_matches {
            return Ok(CacheSetOutcome {
                applied: false,
                previous: current,
            });
        }
        state.version = state.version.saturating_add(1);
        let entry = CacheEntry {
            value,
            version: state.version,
            expires_at_ms: options.ttl_ms.map(|ttl| now_ms().saturating_add(ttl)),
            storage_class: CacheStorageClass::Memory,
        };
        state
            .caches
            .insert((cache.to_owned(), key.to_owned()), entry);
        Ok(CacheSetOutcome {
            applied: true,
            previous: current,
        })
    }

    async fn cache_multi_set(
        &self,
        cache: &str,
        entries: &[CacheMultiSetEntry],
        only_if_all_missing: bool,
    ) -> Result<bool, BackendError> {
        validate_cache_multi_set(entries)?;
        let mut state = self.state.lock().unwrap();
        if only_if_all_missing
            && entries
                .iter()
                .any(|entry| live_entry(&mut state, cache, &entry.key).is_some())
        {
            return Ok(false);
        }
        state.version = state.version.saturating_add(1);
        let version = state.version;
        for entry in entries {
            state.caches.insert(
                (cache.to_owned(), entry.key.clone()),
                CacheEntry {
                    value: CacheValue::Blob(entry.value.clone()),
                    version,
                    expires_at_ms: None,
                    storage_class: CacheStorageClass::Memory,
                },
            );
        }
        Ok(true)
    }

    async fn cache_delete(&self, cache: &str, keys: &[String]) -> Result<u64, BackendError> {
        let mut state = self.state.lock().unwrap();
        let mut deleted = 0;
        for key in keys {
            let _ = live_entry(&mut state, cache, key);
            if state
                .caches
                .remove(&(cache.to_owned(), key.clone()))
                .is_some()
            {
                deleted += 1;
            }
        }
        if deleted > 0 {
            state.version = state.version.saturating_add(1);
        }
        Ok(deleted)
    }

    async fn cache_increment(
        &self,
        cache: &str,
        key: &str,
        delta: i64,
    ) -> Result<i64, BackendError> {
        let mut state = self.state.lock().unwrap();
        let current = live_entry(&mut state, cache, key);
        let value = match current.as_ref().map(|entry| &entry.value) {
            None => 0,
            Some(CacheValue::Counter(value)) => *value,
            Some(CacheValue::String(value)) => value
                .parse()
                .map_err(|_| BackendError::Invalid("value is not an integer".into()))?,
            Some(CacheValue::Blob(value)) => std::str::from_utf8(value)
                .ok()
                .and_then(|value| value.parse().ok())
                .ok_or_else(|| BackendError::Invalid("value is not an integer".into()))?,
            Some(_) => return Err(BackendError::Invalid("value is not an integer".into())),
        };
        let value = value
            .checked_add(delta)
            .ok_or_else(|| BackendError::Invalid("integer overflow".into()))?;
        state.version = state.version.saturating_add(1);
        let version = state.version;
        state.caches.insert(
            (cache.to_owned(), key.to_owned()),
            CacheEntry {
                value: CacheValue::Counter(value),
                version,
                expires_at_ms: current.and_then(|entry| entry.expires_at_ms),
                storage_class: CacheStorageClass::Memory,
            },
        );
        Ok(value)
    }

    async fn cache_expire(
        &self,
        cache: &str,
        key: &str,
        ttl_ms: Option<u64>,
    ) -> Result<bool, BackendError> {
        let mut state = self.state.lock().unwrap();
        let _ = live_entry(&mut state, cache, key);
        let Some(entry) = state.caches.get_mut(&(cache.to_owned(), key.to_owned())) else {
            return Ok(false);
        };
        if entry.expires_at_ms == ttl_ms.map(|ttl| now_ms().saturating_add(ttl))
            || (ttl_ms.is_none() && entry.expires_at_ms.is_none())
        {
            return Ok(false);
        }
        entry.expires_at_ms = ttl_ms.map(|ttl| now_ms().saturating_add(ttl));
        state.version = state.version.saturating_add(1);
        Ok(true)
    }

    async fn cache_collection_mutate(
        &self,
        cache: &str,
        key: &str,
        mutation: CacheCollectionMutation,
    ) -> Result<CacheCollectionResult, BackendError> {
        let mut state = self.state.lock().unwrap();
        let current = live_entry(&mut state, cache, key);
        let plan = plan_collection_mutation(current.as_ref().map(|entry| &entry.value), &mutation)?;
        if !plan.changed {
            return Ok(plan.result);
        }
        state.version = state.version.saturating_add(1);
        let version = state.version;
        let identity = (cache.to_owned(), key.to_owned());
        match plan.value {
            Some(value) => {
                state.caches.insert(
                    identity,
                    CacheEntry {
                        value,
                        version,
                        expires_at_ms: current.as_ref().and_then(|entry| entry.expires_at_ms),
                        storage_class: current
                            .as_ref()
                            .map_or(CacheStorageClass::Memory, |entry| entry.storage_class),
                    },
                );
            }
            None => {
                state.caches.remove(&identity);
            }
        }
        Ok(plan.result)
    }

    async fn stream_partition_count(&self, stream: &str) -> Result<u32, BackendError> {
        self.state
            .lock()
            .unwrap()
            .stream_partitions
            .get(stream)
            .copied()
            .ok_or(BackendError::NotFound)
    }

    async fn stream_append(
        &self,
        stream: &str,
        partition: u32,
        records: Vec<StreamRecord>,
    ) -> Result<u64, BackendError> {
        let mut state = self.state.lock().unwrap();
        let count = state
            .stream_partitions
            .get(stream)
            .copied()
            .ok_or(BackendError::NotFound)?;
        if partition >= count {
            return Err(BackendError::NotFound);
        }
        let log = state
            .streams
            .entry((stream.to_owned(), partition))
            .or_default();
        let first = u64::try_from(log.len()).unwrap_or(u64::MAX);
        for mut record in records {
            record.offset = u64::try_from(log.len()).unwrap_or(u64::MAX);
            log.push(record);
        }
        Ok(first)
    }

    async fn stream_append_idempotent(
        &self,
        stream: &str,
        partition: u32,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        records: Vec<StreamRecord>,
    ) -> Result<u64, BackendError> {
        if producer_id < 0 || producer_epoch < 0 || base_sequence < 0 || records.is_empty() {
            return Err(BackendError::Invalid(
                "invalid idempotent producer batch".into(),
            ));
        }
        let sequence_span = i32::try_from(records.len())
            .map_err(|_| BackendError::Invalid("idempotent producer batch is too large".into()))?;
        let next_sequence = base_sequence
            .checked_add(sequence_span)
            .ok_or_else(|| BackendError::Invalid("producer sequence overflow".into()))?;
        let mut state = self.state.lock().unwrap();
        let count = state
            .stream_partitions
            .get(stream)
            .copied()
            .ok_or(BackendError::NotFound)?;
        if partition >= count {
            return Err(BackendError::NotFound);
        }
        let producer_key = (stream.to_owned(), partition, producer_id);
        if let Some(producer) = state.stream_producers.get_mut(&producer_key) {
            if producer_epoch < producer.epoch {
                return Err(BackendError::Conflict);
            }
            if producer_epoch > producer.epoch {
                producer.epoch = producer_epoch;
                producer.next_sequence = 0;
                producer.history.clear();
            }
            if base_sequence < producer.next_sequence {
                return producer
                    .history
                    .get(&base_sequence)
                    .filter(|(previous, _)| previous == &records)
                    .map(|(_, offset)| *offset)
                    .ok_or(BackendError::Conflict);
            }
            if base_sequence != producer.next_sequence {
                return Err(BackendError::Conflict);
            }
        } else if base_sequence != 0 {
            return Err(BackendError::Conflict);
        }
        let input = records.clone();
        let log = state
            .streams
            .entry((stream.to_owned(), partition))
            .or_default();
        let first = u64::try_from(log.len()).unwrap_or(u64::MAX);
        for mut record in records {
            record.offset = u64::try_from(log.len()).unwrap_or(u64::MAX);
            log.push(record);
        }
        let producer = state.stream_producers.entry(producer_key).or_default();
        producer.epoch = producer_epoch;
        producer.next_sequence = next_sequence;
        producer.history.insert(base_sequence, (input, first));
        while producer.history.len() > 128 {
            let Some(sequence) = producer.history.keys().next().copied() else {
                break;
            };
            producer.history.remove(&sequence);
        }
        Ok(first)
    }

    async fn stream_fetch(
        &self,
        stream: &str,
        partition: u32,
        offset: u64,
        limit: u32,
    ) -> Result<Vec<StreamRecord>, BackendError> {
        let state = self.state.lock().unwrap();
        if !state.stream_partitions.contains_key(stream) {
            return Err(BackendError::NotFound);
        }
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        Ok(state
            .streams
            .get(&(stream.to_owned(), partition))
            .map(|records| {
                records
                    .iter()
                    .skip(start)
                    .take(usize::try_from(limit).unwrap_or(usize::MAX))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn stream_end_offset(&self, stream: &str, partition: u32) -> Result<u64, BackendError> {
        let state = self.state.lock().unwrap();
        if !state.stream_partitions.contains_key(stream) {
            return Err(BackendError::NotFound);
        }
        Ok(state
            .streams
            .get(&(stream.to_owned(), partition))
            .map_or(0, |records| {
                u64::try_from(records.len()).unwrap_or(u64::MAX)
            }))
    }

    async fn stream_start_offset(&self, stream: &str, partition: u32) -> Result<u64, BackendError> {
        let state = self.state.lock().unwrap();
        if !state.stream_partitions.contains_key(stream)
            || partition >= state.stream_partitions[stream]
        {
            return Err(BackendError::NotFound);
        }
        Ok(0)
    }

    async fn stream_commit_offset(
        &self,
        group: &str,
        stream: &str,
        partition: u32,
        next_offset: u64,
        identity: Option<&StreamGroupIdentity>,
    ) -> Result<(), BackendError> {
        let mut state = self.state.lock().unwrap();
        if let Some(identity) = identity {
            let claim = state
                .group_claims
                .get(&(stream.to_owned(), group.to_owned(), partition));
            if !claim.is_some_and(|(member_id, generation)| {
                member_id == &identity.member_id && *generation == identity.generation
            }) {
                return Err(BackendError::Conflict);
            }
        }
        state.offsets.insert(
            (group.to_owned(), stream.to_owned(), partition),
            next_offset,
        );
        Ok(())
    }

    async fn stream_committed_offset(
        &self,
        group: &str,
        stream: &str,
        partition: u32,
    ) -> Result<Option<u64>, BackendError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .offsets
            .get(&(group.to_owned(), stream.to_owned(), partition))
            .copied())
    }

    async fn stream_group_join(
        &self,
        stream: &str,
        group: &str,
        member_id: &str,
        session_timeout_ms: u64,
    ) -> Result<StreamGroupSessionResult, BackendError> {
        if !(1_000..=300_000).contains(&session_timeout_ms) {
            return Err(BackendError::Invalid("invalid session timeout".into()));
        }
        let mut state = self.state.lock().unwrap();
        let partition_count = state
            .stream_partitions
            .get(stream)
            .copied()
            .ok_or(BackendError::NotFound)?;
        let group_state = state
            .stream_groups
            .entry((stream.to_owned(), group.to_owned()))
            .or_default();
        expire_group_members(group_state);
        if !group_state.members.contains_key(member_id) {
            group_state.generation = group_state.generation.saturating_add(1).max(1);
        }
        group_state.members.insert(
            member_id.to_owned(),
            MemoryGroupMember {
                deadline_ms: now_ms().saturating_add(session_timeout_ms),
                session_timeout_ms,
            },
        );
        Ok(memory_session_result(
            group_state,
            partition_count,
            member_id,
            None,
        ))
    }

    async fn stream_group_observe(
        &self,
        stream: &str,
        group: &str,
        member_id: &str,
    ) -> Result<Option<StreamGroupSession>, BackendError> {
        let mut state = self.state.lock().unwrap();
        let partition_count = state
            .stream_partitions
            .get(stream)
            .copied()
            .ok_or(BackendError::NotFound)?;
        let Some(group_state) = state
            .stream_groups
            .get_mut(&(stream.to_owned(), group.to_owned()))
        else {
            return Ok(None);
        };
        expire_group_members(group_state);
        Ok(Some(
            memory_session_result(group_state, partition_count, member_id, None).session,
        ))
    }

    async fn stream_group_heartbeat(
        &self,
        stream: &str,
        group: &str,
        member_id: &str,
        generation: u64,
    ) -> Result<StreamGroupSessionResult, BackendError> {
        let mut state = self.state.lock().unwrap();
        let partition_count = state
            .stream_partitions
            .get(stream)
            .copied()
            .ok_or(BackendError::NotFound)?;
        let Some(group_state) = state
            .stream_groups
            .get_mut(&(stream.to_owned(), group.to_owned()))
        else {
            return Ok(empty_session_result(StreamGroupRejection::UnknownGroup));
        };
        expire_group_members(group_state);
        let rejection = if !group_state.members.contains_key(member_id) {
            Some(StreamGroupRejection::UnknownMember)
        } else if generation != group_state.generation {
            Some(StreamGroupRejection::StaleGeneration)
        } else {
            None
        };
        if rejection.is_none() {
            let timeout = group_state
                .members
                .get(member_id)
                .map_or(1_000, |member| member.session_timeout_ms);
            group_state.members.insert(
                member_id.to_owned(),
                MemoryGroupMember {
                    deadline_ms: now_ms().saturating_add(timeout),
                    session_timeout_ms: timeout,
                },
            );
        }
        Ok(memory_session_result(
            group_state,
            partition_count,
            member_id,
            rejection,
        ))
    }

    async fn stream_group_leave(
        &self,
        stream: &str,
        group: &str,
        member_id: &str,
        generation: u64,
    ) -> Result<StreamGroupSessionResult, BackendError> {
        let mut state = self.state.lock().unwrap();
        let partition_count = state
            .stream_partitions
            .get(stream)
            .copied()
            .ok_or(BackendError::NotFound)?;
        let Some(group_state) = state
            .stream_groups
            .get_mut(&(stream.to_owned(), group.to_owned()))
        else {
            return Ok(empty_session_result(StreamGroupRejection::UnknownGroup));
        };
        expire_group_members(group_state);
        let rejection = if !group_state.members.contains_key(member_id) {
            Some(StreamGroupRejection::UnknownMember)
        } else if generation != group_state.generation {
            Some(StreamGroupRejection::StaleGeneration)
        } else {
            None
        };
        if rejection.is_none() {
            group_state.members.remove(member_id);
            group_state.generation = group_state.generation.saturating_add(1);
        }
        Ok(memory_session_result(
            group_state,
            partition_count,
            member_id,
            rejection,
        ))
    }

    async fn stream_group_claim(
        &self,
        stream: &str,
        group: &str,
        member_id: &str,
        generation: u64,
        partitions: &[u32],
    ) -> Result<(), BackendError> {
        let mut state = self.state.lock().unwrap();
        let partition_count = state
            .stream_partitions
            .get(stream)
            .copied()
            .ok_or(BackendError::NotFound)?;
        let group_key = (stream.to_owned(), group.to_owned());
        let group_state = state
            .stream_groups
            .get_mut(&group_key)
            .ok_or(BackendError::Conflict)?;
        expire_group_members(group_state);
        if generation != group_state.generation || !group_state.members.contains_key(member_id) {
            return Err(BackendError::Conflict);
        }
        let assigned = assigned_partitions(group_state, partition_count, member_id);
        if partitions
            .iter()
            .any(|partition| !assigned.contains(partition))
        {
            return Err(BackendError::Conflict);
        }
        for partition in partitions {
            state.group_claims.insert(
                (stream.to_owned(), group.to_owned(), *partition),
                (member_id.to_owned(), generation),
            );
        }
        Ok(())
    }

    async fn queue_exists(&self, queue: &str) -> Result<bool, BackendError> {
        Ok(self.state.lock().unwrap().queues.contains_key(queue))
    }

    async fn queue_dead_letter_target(&self, queue: &str) -> Result<Option<String>, BackendError> {
        let state = self.state.lock().unwrap();
        if !state.queues.contains_key(queue) {
            return Err(BackendError::NotFound);
        }
        Ok(state.queue_dead_letter_targets.get(queue).cloned())
    }

    async fn queue_publish(&self, queue: &str, message: QueueMessage) -> Result<(), BackendError> {
        self.state
            .lock()
            .unwrap()
            .queues
            .get_mut(queue)
            .ok_or(BackendError::NotFound)?
            .push_back(message);
        Ok(())
    }

    async fn queue_acquire(
        &self,
        queue: &str,
        _consumer: &str,
        max_messages: u16,
        _visibility_timeout_ms: u64,
    ) -> Result<Vec<QueueDelivery>, BackendError> {
        let mut state = self.state.lock().unwrap();
        let mut messages = Vec::new();
        for _ in 0..max_messages {
            let Some(message) = state
                .queues
                .get_mut(queue)
                .ok_or(BackendError::NotFound)?
                .pop_front()
            else {
                break;
            };
            state.next_lease = state.next_lease.saturating_add(1);
            let token = format!("lease-{}", state.next_lease);
            state
                .leases
                .insert(token.clone(), (queue.to_owned(), message.clone()));
            messages.push(QueueDelivery {
                message_id: format!("message-{}", state.next_lease),
                lease_token: token,
                redelivered: false,
                message,
            });
        }
        Ok(messages)
    }

    async fn queue_ack(
        &self,
        _queue: &str,
        _consumer: &str,
        lease_token: &str,
    ) -> Result<(), BackendError> {
        self.state
            .lock()
            .unwrap()
            .leases
            .remove(lease_token)
            .map(|_| ())
            .ok_or(BackendError::Conflict)
    }

    async fn queue_reject(
        &self,
        _queue: &str,
        _consumer: &str,
        lease_token: &str,
        requeue: bool,
    ) -> Result<(), BackendError> {
        let mut state = self.state.lock().unwrap();
        let (queue, mut message) = state
            .leases
            .remove(lease_token)
            .ok_or(BackendError::Conflict)?;
        if requeue {
            state.queues.get_mut(&queue).unwrap().push_front(message);
        } else if let Some(target) = state.queue_dead_letter_targets.get(&queue).cloned() {
            let first_exchange = message.exchange.clone().unwrap_or_default();
            let first_routing_key = message.routing_key.clone().unwrap_or_default();
            message.expiration = None;
            message.ttl_ms = None;
            message.exchange = Some(
                message
                    .headers
                    .remove("x-epoch-compat-dlx-exchange")
                    .unwrap_or_default(),
            );
            message.routing_key = Some(
                message
                    .headers
                    .remove("x-epoch-compat-dlx-routing-key")
                    .unwrap_or_else(|| target.clone()),
            );
            message
                .headers
                .entry("x-first-death-exchange".into())
                .or_insert(first_exchange.clone());
            message
                .headers
                .entry("x-first-death-queue".into())
                .or_insert(queue.clone());
            message
                .headers
                .entry("x-first-death-reason".into())
                .or_insert_with(|| "rejected".into());
            message
                .headers
                .insert("x-last-death-exchange".into(), first_exchange.clone());
            message
                .headers
                .insert("x-last-death-queue".into(), queue.clone());
            message
                .headers
                .insert("x-last-death-reason".into(), "rejected".into());
            let mut history = message
                .headers
                .get("x-epoch-compat-death-history")
                .and_then(|history| serde_json::from_str::<Vec<serde_json::Value>>(history).ok())
                .unwrap_or_default();
            history.push(serde_json::json!({
                "count":1,
                "exchange":first_exchange,
                "queue":queue,
                "reason":"rejected",
                "routing_keys":[first_routing_key],
            }));
            if let Ok(history) = serde_json::to_string(&history) {
                message
                    .headers
                    .insert("x-epoch-compat-death-history".into(), history);
            }
            state
                .queues
                .get_mut(&target)
                .ok_or(BackendError::NotFound)?
                .push_back(message);
        }
        Ok(())
    }
}

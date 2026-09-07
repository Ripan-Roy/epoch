use std::{
    collections::{BTreeMap, VecDeque},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;

use crate::backend::{
    BackendError, CacheCollectionMutation, CacheCollectionResult, CacheEntry, CacheSetCondition,
    CacheSetOptions, CacheSetOutcome, CacheStorageClass, CacheValue, CompatibilityBackend,
    QueueDelivery, QueueMessage, StreamGroupIdentity, StreamGroupMember, StreamGroupRejection,
    StreamGroupSession, StreamGroupSessionResult, StreamRecord, plan_collection_mutation,
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
    group_claims: BTreeMap<(String, String, u32), (String, u64)>,
    queues: BTreeMap<String, VecDeque<QueueMessage>>,
    leases: BTreeMap<String, (String, QueueMessage)>,
    next_lease: u64,
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
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
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
        entry.expires_at_ms = ttl_ms.map(|ttl| now_ms().saturating_add(ttl));
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
        let (queue, message) = state
            .leases
            .remove(lease_token)
            .ok_or(BackendError::Conflict)?;
        if requeue {
            state.queues.get_mut(&queue).unwrap().push_front(message);
        }
        Ok(())
    }
}

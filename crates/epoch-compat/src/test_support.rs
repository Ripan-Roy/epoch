use std::{
    collections::{BTreeMap, VecDeque},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;

use crate::backend::{
    BackendError, CacheEntry, CacheValue, CompatibilityBackend, QueueDelivery, QueueMessage,
    StreamRecord,
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
    queues: BTreeMap<String, VecDeque<QueueMessage>>,
    leases: BTreeMap<String, (String, QueueMessage)>,
    next_lease: u64,
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
        ttl_ms: Option<u64>,
        only_if_absent: bool,
        only_if_present: bool,
    ) -> Result<Option<CacheEntry>, BackendError> {
        let mut state = self.state.lock().unwrap();
        let current = live_entry(&mut state, cache, key);
        if (only_if_absent && current.is_some()) || (only_if_present && current.is_none()) {
            return Ok(None);
        }
        state.version = state.version.saturating_add(1);
        let entry = CacheEntry {
            value,
            version: state.version,
            expires_at_ms: ttl_ms.map(|ttl| now_ms().saturating_add(ttl)),
        };
        state
            .caches
            .insert((cache.to_owned(), key.to_owned()), entry.clone());
        Ok(Some(entry))
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
    ) -> Result<(), BackendError> {
        self.state.lock().unwrap().offsets.insert(
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

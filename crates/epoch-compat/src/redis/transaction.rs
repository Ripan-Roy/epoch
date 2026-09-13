//! Redis transaction planning over one replicated Cache revision.
//!
//! Commands execute sequentially against an isolated image. Only their final
//! per-key state is submitted to the backend, so one `EXEC` becomes one native
//! Cache transaction and is never exposed partially.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::backend::{
    BackendError, CacheAtomicMutation, CacheCollectionMutation, CacheCollectionResult, CacheEntry,
    CacheSnapshot, CacheStorageClass, CacheValue, CompatibilityBackend,
    MAX_CACHE_MULTI_SET_ENTRIES, plan_collection_mutation,
};

use super::{
    RespValue, arity, backend_error, bulk, cache_value, count_u64, error, finite_f64, integer,
    now_ms, positive_u64, redis_index, redis_range, signed_i64, text, upper, utf8_values,
};

const MAX_TRANSACTION_ATTEMPTS: usize = 4;
pub(super) const MAX_QUEUED_COMMANDS: usize = 128;

#[derive(Debug, Default)]
pub(super) struct TransactionState {
    pub(super) queue: Option<TransactionQueue>,
    watched: BTreeMap<String, Option<u64>>,
}

#[derive(Debug, Default)]
pub(super) struct TransactionQueue {
    commands: Vec<Vec<Vec<u8>>>,
    dirty: bool,
}

impl TransactionState {
    pub(super) fn begin(&mut self) -> RespValue {
        if self.queue.is_some() {
            return error("MULTI calls can not be nested");
        }
        self.queue = Some(TransactionQueue::default());
        RespValue::Simple("OK".into())
    }

    pub(super) fn discard(&mut self) -> RespValue {
        if self.queue.take().is_none() {
            return error("DISCARD without MULTI");
        }
        self.watched.clear();
        RespValue::Simple("OK".into())
    }

    pub(super) fn unwatch(&mut self) -> RespValue {
        if self.queue.is_some() {
            return error("UNWATCH inside MULTI is not allowed");
        }
        self.watched.clear();
        RespValue::Simple("OK".into())
    }

    pub(super) fn queue(&mut self, arguments: Vec<Vec<u8>>) -> RespValue {
        let Some(queue) = self.queue.as_mut() else {
            return error("internal transaction state is missing");
        };
        if let Err(response) = validate_command(&arguments) {
            queue.dirty = true;
            return response;
        }
        if queue.commands.len() >= MAX_QUEUED_COMMANDS {
            queue.dirty = true;
            return error("transaction command limit exceeded");
        }
        queue.commands.push(arguments);
        RespValue::Simple("QUEUED".into())
    }

    pub(super) async fn watch<B: CompatibilityBackend>(
        &mut self,
        backend: &Arc<B>,
        cache: &str,
        arguments: &[Vec<u8>],
    ) -> RespValue {
        if self.queue.is_some() {
            return error("WATCH inside MULTI is not allowed");
        }
        let Ok(keys) = parse_keys(arguments, "watch") else {
            return arity("watch");
        };
        match backend.cache_snapshot(cache, &keys).await {
            Ok(snapshot) => {
                for (key, entry) in snapshot.entries {
                    self.watched.insert(key, entry.map(|entry| entry.version));
                }
                RespValue::Simple("OK".into())
            }
            Err(error) => backend_error(error),
        }
    }

    pub(super) async fn execute<B: CompatibilityBackend>(
        &mut self,
        backend: &Arc<B>,
        cache: &str,
    ) -> RespValue {
        let Some(queue) = self.queue.take() else {
            return error("EXEC without MULTI");
        };
        if queue.dirty {
            self.watched.clear();
            return RespValue::Error(
                "EXECABORT Transaction discarded because of previous errors.".into(),
            );
        }
        if queue.commands.is_empty() {
            self.watched.clear();
            return RespValue::Array(Vec::new());
        }
        let mut keys = self.watched.keys().cloned().collect::<BTreeSet<_>>();
        for command in &queue.commands {
            match command_keys(command) {
                Ok(command_keys) => keys.extend(command_keys),
                Err(response) => {
                    self.watched.clear();
                    return response;
                }
            }
        }
        if keys.is_empty() {
            self.watched.clear();
            return RespValue::Array(
                queue
                    .commands
                    .iter()
                    .map(|command| execute_stateless(command))
                    .collect(),
            );
        }
        if keys.len() > MAX_CACHE_MULTI_SET_ENTRIES {
            self.watched.clear();
            return error("transaction touches too many distinct keys");
        }
        let keys = keys.into_iter().collect::<Vec<_>>();
        for attempt in 0..MAX_TRANSACTION_ATTEMPTS {
            let snapshot = match backend.cache_snapshot(cache, &keys).await {
                Ok(snapshot) => snapshot,
                Err(BackendError::Conflict) if attempt + 1 < MAX_TRANSACTION_ATTEMPTS => continue,
                Err(error) => {
                    self.watched.clear();
                    return backend_error(error);
                }
            };
            if watched_key_changed(&self.watched, &snapshot) {
                self.watched.clear();
                return RespValue::Null;
            }
            let mut image = TransactionImage::new(snapshot);
            let replies = queue
                .commands
                .iter()
                .map(|command| image.execute(command))
                .collect::<Vec<_>>();
            let revision = image.revision;
            let mutations = image.into_mutations();
            if mutations.is_empty() {
                self.watched.clear();
                return RespValue::Array(replies);
            }
            match backend
                .cache_compare_and_apply(cache, revision, &mutations)
                .await
            {
                Ok(()) => {
                    self.watched.clear();
                    return RespValue::Array(replies);
                }
                Err(BackendError::Conflict) if attempt + 1 < MAX_TRANSACTION_ATTEMPTS => {}
                Err(error) => {
                    self.watched.clear();
                    return backend_error(error);
                }
            }
        }
        self.watched.clear();
        backend_error(BackendError::Conflict)
    }
}

fn watched_key_changed(watched: &BTreeMap<String, Option<u64>>, snapshot: &CacheSnapshot) -> bool {
    watched.iter().any(|(key, expected)| {
        snapshot
            .entries
            .get(key)
            .and_then(|entry| entry.as_ref())
            .map(|entry| entry.version)
            != *expected
    })
}

#[derive(Debug)]
struct TransactionImage {
    revision: u64,
    entries: BTreeMap<String, Option<CacheEntry>>,
    dirty: BTreeSet<String>,
}

impl TransactionImage {
    fn new(snapshot: CacheSnapshot) -> Self {
        Self {
            revision: snapshot.revision,
            entries: snapshot.entries,
            dirty: BTreeSet::new(),
        }
    }

    fn entry(&self, key: &str) -> Option<&CacheEntry> {
        self.entries.get(key).and_then(Option::as_ref)
    }

    fn put(&mut self, key: &str, entry: CacheEntry) {
        self.entries.insert(key.to_owned(), Some(entry));
        self.dirty.insert(key.to_owned());
    }

    fn remove(&mut self, key: &str) -> bool {
        let existed = self
            .entries
            .insert(key.to_owned(), None)
            .flatten()
            .is_some();
        if existed {
            self.dirty.insert(key.to_owned());
        }
        existed
    }

    fn into_mutations(self) -> Vec<CacheAtomicMutation> {
        let now = now_ms();
        self.dirty
            .into_iter()
            .map(
                |key| match self.entries.get(&key).and_then(Option::as_ref) {
                    Some(entry) if entry.expires_at_ms.is_none_or(|expiry| expiry > now) => {
                        CacheAtomicMutation::Put {
                            key,
                            value: entry.value.clone(),
                            ttl_ms: entry
                                .expires_at_ms
                                .map(|expiry| expiry.saturating_sub(now).max(1)),
                            storage_class: entry.storage_class,
                        }
                    }
                    _ => CacheAtomicMutation::Delete { key },
                },
            )
            .collect()
    }

    fn execute(&mut self, arguments: &[Vec<u8>]) -> RespValue {
        execute_image_command(self, arguments)
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "the exhaustive match is the fail-closed Redis transaction command boundary"
)]
fn execute_image_command(image: &mut TransactionImage, arguments: &[Vec<u8>]) -> RespValue {
    let Some(command) = arguments.first().and_then(|value| upper(value)) else {
        return error("command must be UTF-8");
    };
    let args = &arguments[1..];
    match command.as_str() {
        "PING" | "ECHO" | "SELECT" => execute_stateless(arguments),
        "GET" => image
            .entry(text(&args[0]).unwrap_or_default())
            .map_or(RespValue::Null, |entry| cache_value(entry.value.clone())),
        "SET" => execute_set(image, args),
        "DEL" => integer(
            args.iter()
                .filter_map(|key| text(key))
                .filter(|key| image.remove(key))
                .count()
                .try_into()
                .unwrap_or(u64::MAX),
        ),
        "EXISTS" => integer(
            args.iter()
                .filter_map(|key| text(key))
                .filter(|key| image.entry(key).is_some())
                .count()
                .try_into()
                .unwrap_or(u64::MAX),
        ),
        "MGET" => RespValue::Array(
            args.iter()
                .map(|key| {
                    image
                        .entry(text(key).unwrap_or_default())
                        .map_or(RespValue::Null, |entry| cache_value(entry.value.clone()))
                })
                .collect(),
        ),
        "MSET" => {
            for pair in args.chunks_exact(2) {
                let key = text(&pair[0]).unwrap_or_default();
                image.put(
                    key,
                    entry(
                        CacheValue::Blob(pair[1].clone()),
                        None,
                        CacheStorageClass::Memory,
                    ),
                );
            }
            RespValue::Simple("OK".into())
        }
        "MSETNX" => {
            if args
                .chunks_exact(2)
                .any(|pair| image.entry(text(&pair[0]).unwrap_or_default()).is_some())
            {
                RespValue::Integer(0)
            } else {
                for pair in args.chunks_exact(2) {
                    let key = text(&pair[0]).unwrap_or_default();
                    image.put(
                        key,
                        entry(
                            CacheValue::Blob(pair[1].clone()),
                            None,
                            CacheStorageClass::Memory,
                        ),
                    );
                }
                RespValue::Integer(1)
            }
        }
        "INCR" => execute_increment(image, args, 1),
        "DECR" => execute_increment(image, args, -1),
        "INCRBY" => execute_increment_by(image, args, 1),
        "DECRBY" => execute_increment_by(image, args, -1),
        "TTL" => execute_ttl(image, args, false),
        "PTTL" => execute_ttl(image, args, true),
        "EXPIRE" => execute_expire(image, args, 1_000),
        "PEXPIRE" => execute_expire(image, args, 1),
        "PERSIST" => execute_persist(image, args),
        "TYPE" => execute_type(image, args),
        "HSET" | "HDEL" | "LPUSH" | "RPUSH" | "LPOP" | "RPOP" | "SADD" | "SREM" | "ZADD"
        | "ZREM" => execute_collection_mutation(image, &command, args),
        "HGET" | "HMGET" | "HEXISTS" | "HLEN" | "HGETALL" | "LLEN" | "LRANGE" | "LINDEX"
        | "SMEMBERS" | "SCARD" | "SISMEMBER" | "ZCARD" | "ZSCORE" | "ZRANGE" => {
            execute_collection_read(image, &command, args)
        }
        _ => error("unsupported command in transaction"),
    }
}

fn entry(value: CacheValue, ttl_ms: Option<u64>, storage_class: CacheStorageClass) -> CacheEntry {
    CacheEntry {
        value,
        version: 0,
        expires_at_ms: ttl_ms.map(|ttl| now_ms().saturating_add(ttl)),
        storage_class,
    }
}

fn execute_set(image: &mut TransactionImage, args: &[Vec<u8>]) -> RespValue {
    let Ok(plan) = parse_set(args) else {
        return error("invalid SET queued state");
    };
    let previous = image.entry(&plan.key).cloned();
    if plan.return_previous
        && previous.as_ref().is_some_and(|entry| {
            !matches!(
                entry.value,
                CacheValue::String(_) | CacheValue::Blob(_) | CacheValue::Counter(_)
            )
        })
    {
        return wrong_type();
    }
    let condition_matches = (!plan.only_if_absent || previous.is_none())
        && (!plan.only_if_present || previous.is_some());
    if condition_matches {
        image.put(
            &plan.key,
            entry(
                CacheValue::Blob(plan.value),
                plan.ttl_ms,
                CacheStorageClass::Memory,
            ),
        );
    }
    if plan.return_previous {
        previous.map_or(RespValue::Null, |entry| cache_value(entry.value))
    } else if condition_matches {
        RespValue::Simple("OK".into())
    } else {
        RespValue::Null
    }
}

fn execute_increment(image: &mut TransactionImage, args: &[Vec<u8>], delta: i64) -> RespValue {
    execute_numeric_increment(image, text(&args[0]).unwrap_or_default(), delta)
}

fn execute_increment_by(
    image: &mut TransactionImage,
    args: &[Vec<u8>],
    direction: i64,
) -> RespValue {
    let Some(delta) = signed_i64(&args[1]).and_then(|value| value.checked_mul(direction)) else {
        return error("value is not an integer or out of range");
    };
    execute_numeric_increment(image, text(&args[0]).unwrap_or_default(), delta)
}

fn execute_numeric_increment(image: &mut TransactionImage, key: &str, delta: i64) -> RespValue {
    let previous = image.entry(key).cloned();
    let value = match previous.as_ref().map(|entry| &entry.value) {
        None => 0,
        Some(CacheValue::Counter(value)) => *value,
        Some(CacheValue::String(value)) => match value.parse::<i64>() {
            Ok(value) => value,
            Err(_) => return error("value is not an integer or out of range"),
        },
        Some(CacheValue::Blob(value)) => match std::str::from_utf8(value)
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
        {
            Some(value) => value,
            None => return error("value is not an integer or out of range"),
        },
        Some(_) => return error("value is not an integer or out of range"),
    };
    let Some(value) = value.checked_add(delta) else {
        return error("increment or decrement would overflow");
    };
    image.put(
        key,
        CacheEntry {
            value: CacheValue::Counter(value),
            version: 0,
            expires_at_ms: previous.as_ref().and_then(|entry| entry.expires_at_ms),
            storage_class: previous
                .as_ref()
                .map_or(CacheStorageClass::Memory, |entry| entry.storage_class),
        },
    );
    RespValue::Integer(value)
}

fn execute_ttl(image: &TransactionImage, args: &[Vec<u8>], milliseconds: bool) -> RespValue {
    let Some(entry) = image.entry(text(&args[0]).unwrap_or_default()) else {
        return RespValue::Integer(-2);
    };
    let Some(expiry) = entry.expires_at_ms else {
        return RespValue::Integer(-1);
    };
    let remaining = expiry.saturating_sub(now_ms());
    integer(if milliseconds {
        remaining
    } else {
        remaining / 1_000
    })
}

fn execute_expire(image: &mut TransactionImage, args: &[Vec<u8>], multiplier: u64) -> RespValue {
    let key = text(&args[0]).unwrap_or_default();
    let Some(mut entry) = image.entry(key).cloned() else {
        return RespValue::Integer(0);
    };
    let ttl = positive_u64(&args[1])
        .and_then(|value| value.checked_mul(multiplier))
        .unwrap_or_default();
    entry.expires_at_ms = Some(now_ms().saturating_add(ttl));
    image.put(key, entry);
    RespValue::Integer(1)
}

fn execute_persist(image: &mut TransactionImage, args: &[Vec<u8>]) -> RespValue {
    let key = text(&args[0]).unwrap_or_default();
    let Some(mut entry) = image.entry(key).cloned() else {
        return RespValue::Integer(0);
    };
    if entry.expires_at_ms.take().is_none() {
        return RespValue::Integer(0);
    }
    image.put(key, entry);
    RespValue::Integer(1)
}

fn execute_type(image: &TransactionImage, args: &[Vec<u8>]) -> RespValue {
    let kind = match image
        .entry(text(&args[0]).unwrap_or_default())
        .map(|entry| &entry.value)
    {
        None => "none",
        Some(CacheValue::String(_) | CacheValue::Blob(_) | CacheValue::Counter(_)) => "string",
        Some(CacheValue::Hash(_)) => "hash",
        Some(CacheValue::List(_)) => "list",
        Some(CacheValue::Set(_)) => "set",
        Some(CacheValue::SortedSet(_)) => "zset",
    };
    RespValue::Simple(kind.into())
}

fn execute_collection_mutation(
    image: &mut TransactionImage,
    command: &str,
    args: &[Vec<u8>],
) -> RespValue {
    let key = text(&args[0]).unwrap_or_default();
    let mutation = match command {
        "HSET" => {
            let entries = args[1..]
                .chunks_exact(2)
                .map(|pair| {
                    (
                        text(&pair[0]).unwrap_or_default().to_owned(),
                        text(&pair[1]).unwrap_or_default().to_owned(),
                    )
                })
                .collect();
            CacheCollectionMutation::HashSet { entries }
        }
        "HDEL" => CacheCollectionMutation::HashDelete {
            fields: utf8_values(&args[1..]).unwrap_or_default(),
        },
        "LPUSH" | "RPUSH" => CacheCollectionMutation::ListPush {
            values: utf8_values(&args[1..]).unwrap_or_default(),
            front: command == "LPUSH",
        },
        "LPOP" | "RPOP" => CacheCollectionMutation::ListPop {
            count: args
                .get(1)
                .and_then(|value| super::bounded_count(value))
                .unwrap_or(1),
            front: command == "LPOP",
        },
        "SADD" => CacheCollectionMutation::SetAdd {
            members: utf8_values(&args[1..]).unwrap_or_default(),
        },
        "SREM" => CacheCollectionMutation::SetRemove {
            members: utf8_values(&args[1..]).unwrap_or_default(),
        },
        "ZADD" => CacheCollectionMutation::SortedSetAdd {
            entries: args[1..]
                .chunks_exact(2)
                .map(|pair| {
                    (
                        text(&pair[1]).unwrap_or_default().to_owned(),
                        finite_f64(&pair[0]).unwrap_or_default(),
                    )
                })
                .collect(),
        },
        "ZREM" => CacheCollectionMutation::SortedSetRemove {
            members: utf8_values(&args[1..]).unwrap_or_default(),
        },
        _ => return error("unsupported collection mutation in transaction"),
    };
    let previous = image.entry(key).cloned();
    let plan =
        match plan_collection_mutation(previous.as_ref().map(|entry| &entry.value), &mutation) {
            Ok(plan) => plan,
            Err(error) => return backend_error(error),
        };
    if plan.changed {
        match plan.value {
            Some(value) => image.put(
                key,
                CacheEntry {
                    value,
                    version: 0,
                    expires_at_ms: previous.as_ref().and_then(|entry| entry.expires_at_ms),
                    storage_class: previous
                        .as_ref()
                        .map_or(CacheStorageClass::Memory, |entry| entry.storage_class),
                },
            ),
            None => {
                image.remove(key);
            }
        }
    }
    match plan.result {
        CacheCollectionResult::HashSet { added }
        | CacheCollectionResult::SetAdd { added }
        | CacheCollectionResult::SortedSetAdd { added } => integer(added),
        CacheCollectionResult::HashDelete { removed }
        | CacheCollectionResult::SetRemove { removed }
        | CacheCollectionResult::SortedSetRemove { removed } => integer(removed),
        CacheCollectionResult::ListPush { length } => integer(length),
        CacheCollectionResult::ListPop { values } if values.is_empty() => RespValue::Null,
        CacheCollectionResult::ListPop { values } if args.len() == 2 => {
            RespValue::Array(values.into_iter().map(|value| bulk(&value)).collect())
        }
        CacheCollectionResult::ListPop { mut values } => bulk(&values.remove(0)),
    }
}

fn execute_collection_read(image: &TransactionImage, command: &str, args: &[Vec<u8>]) -> RespValue {
    let key = text(&args[0]).unwrap_or_default();
    match command {
        "HGET" | "HMGET" | "HEXISTS" | "HLEN" | "HGETALL" => {
            execute_hash_read(image, command, key, args)
        }
        "LLEN" | "LRANGE" | "LINDEX" => execute_list_read(image, command, key, args),
        "SMEMBERS" | "SCARD" | "SISMEMBER" => execute_set_read(image, command, key, args),
        "ZCARD" | "ZSCORE" | "ZRANGE" => execute_sorted_set_read(image, command, key, args),
        _ => error("unsupported collection read in transaction"),
    }
}

fn execute_hash_read(
    image: &TransactionImage,
    command: &str,
    key: &str,
    args: &[Vec<u8>],
) -> RespValue {
    let hash = match image.entry(key).map(|entry| &entry.value) {
        None => None,
        Some(CacheValue::Hash(hash)) => Some(hash),
        Some(_) => return wrong_type(),
    };
    match command {
        "HGET" => hash
            .and_then(|hash| hash.get(text(&args[1]).unwrap_or_default()))
            .map_or(RespValue::Null, |value| bulk(value)),
        "HMGET" => RespValue::Array(
            args[1..]
                .iter()
                .map(|field| {
                    hash.and_then(|hash| hash.get(text(field).unwrap_or_default()))
                        .map_or(RespValue::Null, |value| bulk(value))
                })
                .collect(),
        ),
        "HEXISTS" => RespValue::Integer(i64::from(
            hash.is_some_and(|hash| hash.contains_key(text(&args[1]).unwrap_or_default())),
        )),
        "HLEN" => integer(hash.map_or(0, |hash| count_u64(hash.len()))),
        "HGETALL" => RespValue::Array(
            hash.into_iter()
                .flat_map(|hash| hash.iter())
                .flat_map(|(field, value)| [bulk(field), bulk(value)])
                .collect(),
        ),
        _ => unreachable!(),
    }
}

fn execute_list_read(
    image: &TransactionImage,
    command: &str,
    key: &str,
    args: &[Vec<u8>],
) -> RespValue {
    let list = match image.entry(key).map(|entry| &entry.value) {
        None => None,
        Some(CacheValue::List(list)) => Some(list),
        Some(_) => return wrong_type(),
    };
    match command {
        "LLEN" => integer(list.map_or(0, |list| count_u64(list.len()))),
        "LINDEX" => list
            .and_then(|list| {
                redis_index(list.len(), signed_i64(&args[1]).unwrap_or_default())
                    .and_then(|index| list.get(index))
            })
            .map_or(RespValue::Null, |value| bulk(value)),
        "LRANGE" => {
            let Some(list) = list else {
                return RespValue::Array(Vec::new());
            };
            let Some((start, end)) = redis_range(
                list.len(),
                signed_i64(&args[1]).unwrap_or_default(),
                signed_i64(&args[2]).unwrap_or_default(),
            ) else {
                return RespValue::Array(Vec::new());
            };
            RespValue::Array(list[start..end].iter().map(|value| bulk(value)).collect())
        }
        _ => unreachable!(),
    }
}

fn execute_set_read(
    image: &TransactionImage,
    command: &str,
    key: &str,
    args: &[Vec<u8>],
) -> RespValue {
    let set = match image.entry(key).map(|entry| &entry.value) {
        None => None,
        Some(CacheValue::Set(set)) => Some(set),
        Some(_) => return wrong_type(),
    };
    match command {
        "SMEMBERS" => RespValue::Array(
            set.into_iter()
                .flat_map(|set| set.iter())
                .map(|value| bulk(value))
                .collect(),
        ),
        "SCARD" => integer(set.map_or(0, |set| count_u64(set.len()))),
        "SISMEMBER" => RespValue::Integer(i64::from(set.is_some_and(|set| {
            set.iter()
                .any(|value| value == text(&args[1]).unwrap_or_default())
        }))),
        _ => unreachable!(),
    }
}

fn execute_sorted_set_read(
    image: &TransactionImage,
    command: &str,
    key: &str,
    args: &[Vec<u8>],
) -> RespValue {
    let set = match image.entry(key).map(|entry| &entry.value) {
        None => None,
        Some(CacheValue::SortedSet(set)) => Some(set),
        Some(_) => return wrong_type(),
    };
    match command {
        "ZCARD" => integer(set.map_or(0, |set| count_u64(set.len()))),
        "ZSCORE" => set
            .and_then(|set| set.get(text(&args[1]).unwrap_or_default()))
            .map_or(RespValue::Null, |score| bulk(&score.to_string())),
        "ZRANGE" => {
            let mut values = set
                .into_iter()
                .flat_map(|set| set.iter())
                .collect::<Vec<_>>();
            values
                .sort_by(|left, right| left.1.total_cmp(right.1).then_with(|| left.0.cmp(right.0)));
            let Some((start, end)) = redis_range(
                values.len(),
                signed_i64(&args[1]).unwrap_or_default(),
                signed_i64(&args[2]).unwrap_or_default(),
            ) else {
                return RespValue::Array(Vec::new());
            };
            let values = &values[start..end];
            if args.len() == 4 {
                RespValue::Array(
                    values
                        .iter()
                        .flat_map(|(member, score)| [bulk(member), bulk(&score.to_string())])
                        .collect(),
                )
            } else {
                RespValue::Array(values.iter().map(|(member, _)| bulk(member)).collect())
            }
        }
        _ => unreachable!(),
    }
}

fn wrong_type() -> RespValue {
    RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
}

fn execute_stateless(arguments: &[Vec<u8>]) -> RespValue {
    match arguments.first().and_then(|value| upper(value)).as_deref() {
        Some("PING") => match &arguments[1..] {
            [] => RespValue::Simple("PONG".into()),
            [value] => RespValue::Bulk(value.clone()),
            _ => arity("ping"),
        },
        Some("ECHO") => match &arguments[1..] {
            [value] => RespValue::Bulk(value.clone()),
            _ => arity("echo"),
        },
        Some("SELECT") if arguments.get(1).is_some_and(|value| value == b"0") => {
            RespValue::Simple("OK".into())
        }
        _ => error("unsupported command in transaction"),
    }
}

fn validate_command(arguments: &[Vec<u8>]) -> Result<(), RespValue> {
    command_keys(arguments).map(|_| ())
}

pub(super) fn command_keys(arguments: &[Vec<u8>]) -> Result<BTreeSet<String>, RespValue> {
    let Some(command) = arguments.first().and_then(|value| upper(value)) else {
        return Err(error("command must be UTF-8"));
    };
    let args = &arguments[1..];
    let keys = match command.as_str() {
        "PING" if args.len() <= 1 => Vec::new(),
        "ECHO" if args.len() == 1 => Vec::new(),
        "SELECT" if args.len() == 1 && args[0] == b"0" => Vec::new(),
        "GET" | "TTL" | "PTTL" | "PERSIST" | "TYPE" | "HLEN" | "HGETALL" | "LLEN" | "SMEMBERS"
        | "SCARD" | "ZCARD"
            if args.len() == 1 =>
        {
            vec![parse_key(&args[0])?]
        }
        "HGET" | "HEXISTS" | "LINDEX" | "SISMEMBER" | "ZSCORE" if args.len() == 2 => {
            vec![parse_key(&args[0])?]
        }
        "LRANGE"
            if args.len() == 3
                && signed_i64(&args[1]).is_some()
                && signed_i64(&args[2]).is_some() =>
        {
            vec![parse_key(&args[0])?]
        }
        "ZRANGE"
            if (3..=4).contains(&args.len())
                && signed_i64(&args[1]).is_some()
                && signed_i64(&args[2]).is_some()
                && args
                    .get(3)
                    .is_none_or(|value| upper(value).as_deref() == Some("WITHSCORES")) =>
        {
            vec![parse_key(&args[0])?]
        }
        "SET" => vec![parse_set(args)?.key],
        "DEL" | "EXISTS" | "MGET" if !args.is_empty() => parse_keys(args, &command)?,
        "MSET" | "MSETNX" if !args.is_empty() && args.len().is_multiple_of(2) => args
            .chunks_exact(2)
            .map(|pair| parse_key(&pair[0]))
            .collect::<Result<Vec<_>, _>>()?,
        "INCR" | "DECR" if args.len() == 1 => vec![parse_key(&args[0])?],
        "INCRBY" | "DECRBY" if args.len() == 2 && signed_i64(&args[1]).is_some() => {
            vec![parse_key(&args[0])?]
        }
        "EXPIRE" | "PEXPIRE" if args.len() == 2 && positive_u64(&args[1]).is_some() => {
            vec![parse_key(&args[0])?]
        }
        "HSET"
            if args.len() >= 3 && !args.len().is_multiple_of(2) && utf8_values(args).is_some() =>
        {
            vec![parse_key(&args[0])?]
        }
        "HDEL" | "HMGET" | "LPUSH" | "RPUSH" | "SADD" | "SREM" | "ZREM"
            if args.len() >= 2 && utf8_values(args).is_some() =>
        {
            vec![parse_key(&args[0])?]
        }
        "LPOP" | "RPOP"
            if (1..=2).contains(&args.len())
                && args
                    .get(1)
                    .is_none_or(|value| super::bounded_count(value).is_some()) =>
        {
            vec![parse_key(&args[0])?]
        }
        "ZADD"
            if args.len() >= 3
                && !args.len().is_multiple_of(2)
                && args[1..]
                    .chunks_exact(2)
                    .all(|pair| finite_f64(&pair[0]).is_some() && text(&pair[1]).is_some()) =>
        {
            vec![parse_key(&args[0])?]
        }
        _ => {
            return Err(error(
                "unsupported command or invalid arguments in transaction",
            ));
        }
    };
    Ok(keys.into_iter().collect())
}

fn parse_key(value: &[u8]) -> Result<String, RespValue> {
    text(value)
        .filter(|key| !key.is_empty() && !super::is_reserved_cache_key(key.as_bytes()))
        .map(str::to_owned)
        .ok_or_else(|| error("key must be non-empty UTF-8 and outside Epoch's reserved namespace"))
}

fn parse_keys(values: &[Vec<u8>], command: &str) -> Result<Vec<String>, RespValue> {
    if values.is_empty() {
        return Err(arity(&command.to_ascii_lowercase()));
    }
    values.iter().map(|value| parse_key(value)).collect()
}

#[derive(Debug)]
struct SetPlan {
    key: String,
    value: Vec<u8>,
    ttl_ms: Option<u64>,
    only_if_absent: bool,
    only_if_present: bool,
    return_previous: bool,
}

fn parse_set(args: &[Vec<u8>]) -> Result<SetPlan, RespValue> {
    if args.len() < 2 {
        return Err(arity("set"));
    }
    let mut plan = SetPlan {
        key: parse_key(&args[0])?,
        value: args[1].clone(),
        ttl_ms: None,
        only_if_absent: false,
        only_if_present: false,
        return_previous: false,
    };
    let mut index = 2;
    while index < args.len() {
        match upper(&args[index]).as_deref() {
            Some("NX") => {
                plan.only_if_absent = true;
                index += 1;
            }
            Some("XX") => {
                plan.only_if_present = true;
                index += 1;
            }
            Some("GET") => {
                plan.return_previous = true;
                index += 1;
            }
            Some("EX") if index + 1 < args.len() => {
                plan.ttl_ms =
                    positive_u64(&args[index + 1]).and_then(|value| value.checked_mul(1_000));
                if plan.ttl_ms.is_none() {
                    return Err(error("invalid expire time in 'set' command"));
                }
                index += 2;
            }
            Some("PX") if index + 1 < args.len() => {
                plan.ttl_ms = positive_u64(&args[index + 1]);
                if plan.ttl_ms.is_none() {
                    return Err(error("invalid expire time in 'set' command"));
                }
                index += 2;
            }
            _ => return Err(error("syntax error")),
        }
    }
    if plan.only_if_absent && plan.only_if_present {
        return Err(error("syntax error"));
    }
    Ok(plan)
}

//! Memory-first cache state machine.
//!
//! Volatile operations deliberately have no dependency on the durable storage
//! crate. A replicated/durable tablet adapter can persist deterministic
//! mutations around this state machine without changing the volatile path.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use epoch_core::{DurabilityProfile, EpochError, EpochResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum CacheValue {
    String(String),
    Blob(Vec<u8>),
    Counter(i64),
    Hash(BTreeMap<String, String>),
    List(Vec<String>),
    Set(BTreeSet<String>),
    SortedSet(BTreeMap<String, f64>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EvictionPolicy {
    #[default]
    NoEviction,
    AllKeysLru,
    AllKeysLfu,
    AllKeysRandom,
    VolatileLru,
    VolatileLfu,
    VolatileRandom,
    VolatileTtl,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheConfig {
    pub max_entries: usize,
    pub default_ttl_ms: Option<u64>,
    pub eviction: EvictionPolicy,
    pub durability: DurabilityProfile,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 10_000,
            default_ttl_ms: None,
            eviction: EvictionPolicy::NoEviction,
            durability: DurabilityProfile::Volatile,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SetOptions {
    pub ttl_ms: Option<u64>,
    pub expected_version: Option<u64>,
    pub only_if_absent: bool,
    pub only_if_present: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheItem {
    pub value: CacheValue,
    pub version: u64,
    pub expires_at_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheEventKind {
    Set,
    Delete,
    Expire,
    Evict,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheEvent {
    pub sequence: u64,
    pub key: String,
    pub kind: CacheEventKind,
    pub at_ms: u64,
}

#[derive(Debug, Clone)]
struct Entry {
    value: CacheValue,
    version: u64,
    expires_at_ms: Option<u64>,
    last_access_ms: u64,
    accesses: u64,
    insertion_sequence: u64,
}

#[derive(Debug)]
pub struct Cache {
    config: CacheConfig,
    entries: HashMap<String, Entry>,
    sequence: u64,
    events: Vec<CacheEvent>,
}

impl Cache {
    pub fn new(config: CacheConfig) -> EpochResult<Self> {
        if config.max_entries == 0 {
            return Err(EpochError::InvalidArgument(
                "cache max_entries must be greater than zero".into(),
            ));
        }
        Ok(Self {
            config,
            entries: HashMap::new(),
            sequence: 0,
            events: Vec::new(),
        })
    }

    pub fn config(&self) -> &CacheConfig {
        &self.config
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn set(
        &mut self,
        key: impl Into<String>,
        value: CacheValue,
        options: SetOptions,
        now_ms: u64,
    ) -> EpochResult<CacheItem> {
        let key = key.into();
        if key.is_empty() {
            return Err(EpochError::InvalidArgument("cache key is required".into()));
        }
        self.expire_key_if_needed(&key, now_ms);
        let existing_version = self.entries.get(&key).map(|entry| entry.version);
        if options.only_if_absent && existing_version.is_some() {
            return Err(EpochError::Conflict(format!("key already exists: {key}")));
        }
        if options.only_if_present && existing_version.is_none() {
            return Err(EpochError::NotFound(key));
        }
        if let Some(expected) = options.expected_version {
            let actual = existing_version.unwrap_or(0);
            if expected != actual {
                return Err(EpochError::Conflict(format!(
                    "cache version mismatch for {key}: expected {expected}, current {actual}"
                )));
            }
        }
        if existing_version.is_none() && self.entries.len() >= self.config.max_entries {
            self.evict_one(now_ms)?;
        }
        let version = existing_version.map_or(1, |version| version.saturating_add(1));
        let ttl_ms = options.ttl_ms.or(self.config.default_ttl_ms);
        let expires_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        self.sequence = self.sequence.saturating_add(1);
        self.entries.insert(
            key.clone(),
            Entry {
                value: value.clone(),
                version,
                expires_at_ms,
                last_access_ms: now_ms,
                accesses: 0,
                insertion_sequence: self.sequence,
            },
        );
        self.record_event(key, CacheEventKind::Set, now_ms);
        Ok(CacheItem {
            value,
            version,
            expires_at_ms,
        })
    }

    pub fn get(&mut self, key: &str, now_ms: u64) -> Option<CacheItem> {
        self.expire_key_if_needed(key, now_ms);
        self.entries.get_mut(key).map(|entry| {
            entry.last_access_ms = now_ms;
            entry.accesses = entry.accesses.saturating_add(1);
            CacheItem {
                value: entry.value.clone(),
                version: entry.version,
                expires_at_ms: entry.expires_at_ms,
            }
        })
    }

    pub fn delete(&mut self, key: &str, now_ms: u64) -> bool {
        let deleted = self.entries.remove(key).is_some();
        if deleted {
            self.record_event(key.to_owned(), CacheEventKind::Delete, now_ms);
        }
        deleted
    }

    pub fn expire(&mut self, key: &str, ttl_ms: u64, now_ms: u64) -> EpochResult<u64> {
        self.expire_key_if_needed(key, now_ms);
        let entry = self
            .entries
            .get_mut(key)
            .ok_or_else(|| EpochError::NotFound(key.to_owned()))?;
        entry.expires_at_ms = Some(now_ms.saturating_add(ttl_ms));
        entry.version = entry.version.saturating_add(1);
        Ok(entry.version)
    }

    pub fn increment(&mut self, key: &str, delta: i64, now_ms: u64) -> EpochResult<i64> {
        self.expire_key_if_needed(key, now_ms);
        if let Some(entry) = self.entries.get_mut(key) {
            let CacheValue::Counter(current) = &mut entry.value else {
                return Err(EpochError::Conflict(format!(
                    "cache value at {key} is not a counter"
                )));
            };
            *current = current
                .checked_add(delta)
                .ok_or_else(|| EpochError::Capacity("counter overflow".into()))?;
            entry.version = entry.version.saturating_add(1);
            entry.last_access_ms = now_ms;
            return Ok(*current);
        }
        self.set(
            key,
            CacheValue::Counter(delta),
            SetOptions::default(),
            now_ms,
        )?;
        Ok(delta)
    }

    pub fn hash_put(
        &mut self,
        key: &str,
        field: impl Into<String>,
        value: impl Into<String>,
        now_ms: u64,
    ) -> EpochResult<u64> {
        self.mutate_collection(
            key,
            CacheValue::Hash(BTreeMap::new()),
            now_ms,
            |cache_value| {
                let CacheValue::Hash(hash) = cache_value else {
                    return Err("hash");
                };
                hash.insert(field.into(), value.into());
                Ok(())
            },
        )
    }

    pub fn list_push(
        &mut self,
        key: &str,
        value: impl Into<String>,
        front: bool,
        now_ms: u64,
    ) -> EpochResult<u64> {
        self.mutate_collection(key, CacheValue::List(Vec::new()), now_ms, |cache_value| {
            let CacheValue::List(list) = cache_value else {
                return Err("list");
            };
            if front {
                list.insert(0, value.into());
            } else {
                list.push(value.into());
            }
            Ok(())
        })
    }

    pub fn set_add(
        &mut self,
        key: &str,
        member: impl Into<String>,
        now_ms: u64,
    ) -> EpochResult<u64> {
        self.mutate_collection(
            key,
            CacheValue::Set(BTreeSet::new()),
            now_ms,
            |cache_value| {
                let CacheValue::Set(set) = cache_value else {
                    return Err("set");
                };
                set.insert(member.into());
                Ok(())
            },
        )
    }

    pub fn sorted_set_add(
        &mut self,
        key: &str,
        member: impl Into<String>,
        score: f64,
        now_ms: u64,
    ) -> EpochResult<u64> {
        if !score.is_finite() {
            return Err(EpochError::InvalidArgument(
                "sorted-set score must be finite".into(),
            ));
        }
        self.mutate_collection(
            key,
            CacheValue::SortedSet(BTreeMap::new()),
            now_ms,
            |cache_value| {
                let CacheValue::SortedSet(set) = cache_value else {
                    return Err("sorted_set");
                };
                set.insert(member.into(), score);
                Ok(())
            },
        )
    }

    pub fn purge_expired(&mut self, now_ms: u64, limit: usize) -> usize {
        let expired: Vec<String> = self
            .entries
            .iter()
            .filter_map(|(key, entry)| {
                entry
                    .expires_at_ms
                    .filter(|deadline| *deadline <= now_ms)
                    .map(|_| key.clone())
            })
            .take(limit)
            .collect();
        for key in &expired {
            self.entries.remove(key);
            self.record_event(key.clone(), CacheEventKind::Expire, now_ms);
        }
        expired.len()
    }

    pub fn events_from(&self, sequence: u64, limit: usize) -> Vec<CacheEvent> {
        self.events
            .iter()
            .filter(|event| event.sequence >= sequence)
            .take(limit)
            .cloned()
            .collect()
    }

    fn mutate_collection<F>(
        &mut self,
        key: &str,
        initial: CacheValue,
        now_ms: u64,
        operation: F,
    ) -> EpochResult<u64>
    where
        F: FnOnce(&mut CacheValue) -> Result<(), &'static str>,
    {
        self.expire_key_if_needed(key, now_ms);
        if !self.entries.contains_key(key) {
            self.set(key, initial, SetOptions::default(), now_ms)?;
        }
        let entry = self.entries.get_mut(key).expect("inserted above");
        operation(&mut entry.value).map_err(|expected| {
            EpochError::Conflict(format!("cache value at {key} is not a {expected}"))
        })?;
        entry.version = entry.version.saturating_add(1);
        entry.last_access_ms = now_ms;
        Ok(entry.version)
    }

    fn expire_key_if_needed(&mut self, key: &str, now_ms: u64) {
        let expired = self
            .entries
            .get(key)
            .and_then(|entry| entry.expires_at_ms)
            .is_some_and(|deadline| deadline <= now_ms);
        if expired {
            self.entries.remove(key);
            self.record_event(key.to_owned(), CacheEventKind::Expire, now_ms);
        }
    }

    fn evict_one(&mut self, now_ms: u64) -> EpochResult<()> {
        let policy = self.config.eviction;
        if policy == EvictionPolicy::NoEviction {
            return Err(EpochError::Capacity(
                "cache is full and no-eviction is configured".into(),
            ));
        }
        let volatile_only = matches!(
            policy,
            EvictionPolicy::VolatileLru
                | EvictionPolicy::VolatileLfu
                | EvictionPolicy::VolatileRandom
                | EvictionPolicy::VolatileTtl
        );
        let victim = self
            .entries
            .iter()
            .filter(|(_, entry)| !volatile_only || entry.expires_at_ms.is_some())
            .min_by_key(|(_, entry)| match policy {
                EvictionPolicy::AllKeysLru | EvictionPolicy::VolatileLru => entry.last_access_ms,
                EvictionPolicy::AllKeysLfu | EvictionPolicy::VolatileLfu => entry.accesses,
                EvictionPolicy::VolatileTtl => entry.expires_at_ms.unwrap_or(u64::MAX),
                EvictionPolicy::AllKeysRandom | EvictionPolicy::VolatileRandom => {
                    entry.insertion_sequence.wrapping_mul(0x9e37_79b9)
                }
                EvictionPolicy::NoEviction => u64::MAX,
            })
            .map(|(key, _)| key.clone())
            .ok_or_else(|| EpochError::Capacity("no key is eligible for eviction".into()))?;
        self.entries.remove(&victim);
        self.record_event(victim, CacheEventKind::Evict, now_ms);
        Ok(())
    }

    fn record_event(&mut self, key: String, kind: CacheEventKind, at_ms: u64) {
        self.sequence = self.sequence.saturating_add(1);
        self.events.push(CacheEvent {
            sequence: self.sequence,
            key,
            kind,
            at_ms,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supports_p0_data_structures_and_cas() {
        let mut cache = Cache::new(CacheConfig::default()).unwrap();
        let item = cache
            .set(
                "name",
                CacheValue::String("epoch".into()),
                SetOptions::default(),
                10,
            )
            .unwrap();
        assert_eq!(item.version, 1);
        cache.increment("count", 2, 10).unwrap();
        assert_eq!(cache.increment("count", 3, 10).unwrap(), 5);
        cache.hash_put("hash", "field", "value", 10).unwrap();
        cache.list_push("list", "a", false, 10).unwrap();
        cache.set_add("set", "a", 10).unwrap();
        cache.sorted_set_add("zset", "a", 1.5, 10).unwrap();
        assert!(
            cache
                .set(
                    "name",
                    CacheValue::String("bad".into()),
                    SetOptions {
                        expected_version: Some(99),
                        ..SetOptions::default()
                    },
                    11,
                )
                .is_err()
        );
    }

    #[test]
    fn ttl_is_passive_and_actively_purgeable() {
        let mut cache = Cache::new(CacheConfig::default()).unwrap();
        cache
            .set(
                "short",
                CacheValue::String("value".into()),
                SetOptions {
                    ttl_ms: Some(10),
                    ..SetOptions::default()
                },
                100,
            )
            .unwrap();
        assert!(cache.get("short", 109).is_some());
        assert!(cache.get("short", 110).is_none());
        assert!(
            cache
                .events_from(0, 100)
                .iter()
                .any(|event| { event.key == "short" && event.kind == CacheEventKind::Expire })
        );
    }

    #[test]
    fn lru_evicts_least_recently_read_key() {
        let mut cache = Cache::new(CacheConfig {
            max_entries: 2,
            eviction: EvictionPolicy::AllKeysLru,
            ..CacheConfig::default()
        })
        .unwrap();
        cache
            .set("a", CacheValue::Counter(1), SetOptions::default(), 1)
            .unwrap();
        cache
            .set("b", CacheValue::Counter(2), SetOptions::default(), 2)
            .unwrap();
        cache.get("a", 3);
        cache
            .set("c", CacheValue::Counter(3), SetOptions::default(), 4)
            .unwrap();
        assert!(cache.get("a", 5).is_some());
        assert!(cache.get("b", 5).is_none());
    }
}

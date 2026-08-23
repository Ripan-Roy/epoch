//! Event discovery metadata and deterministic global endpoint health routing.

use std::collections::{BTreeMap, BTreeSet};

use epoch_core::{EpochError, EpochResult, validate_resource_name};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

const MAX_CATALOG_ENTRIES: usize = 100_000;
const MAX_LINEAGE_ENTRIES: usize = 1_000;
const MAX_SAMPLE_BYTES: usize = 256 * 1024;
const MAX_CATALOG_TEXT_BYTES: usize = 4 * 1024;
const MAX_ENDPOINT_POOLS: usize = 10_000;
const MAX_ENDPOINTS_PER_POOL: usize = 100;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventCatalogEntry {
    pub event_type: String,
    pub owner: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_ref: Option<String>,
    #[serde(default)]
    pub sources: BTreeSet<String>,
    #[serde(default)]
    pub consumers: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_payload: Option<Value>,
    pub classification: String,
    pub revision: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventCatalog {
    entries: BTreeMap<String, EventCatalogEntry>,
}

impl EventCatalog {
    pub fn upsert(&mut self, mut entry: EventCatalogEntry) -> EpochResult<u64> {
        validate_catalog_entry(&entry)?;
        if !self.entries.contains_key(&entry.event_type)
            && self.entries.len() >= MAX_CATALOG_ENTRIES
        {
            return Err(EpochError::Capacity(format!(
                "event catalog reached its {MAX_CATALOG_ENTRIES} entry limit"
            )));
        }
        let revision =
            self.entries
                .get(&entry.event_type)
                .map_or(Ok(1), |current| {
                    current.revision.checked_add(1).ok_or_else(|| {
                        EpochError::Capacity("event catalog revision overflow".into())
                    })
                })?;
        entry.revision = revision;
        self.entries.insert(entry.event_type.clone(), entry);
        Ok(revision)
    }

    pub fn remove(&mut self, event_type: &str) -> EpochResult<bool> {
        validate_text("event type", event_type)?;
        Ok(self.entries.remove(event_type).is_some())
    }

    pub fn entry(&self, event_type: &str) -> Option<&EventCatalogEntry> {
        self.entries.get(event_type)
    }

    pub fn search(&self, query: &str, limit: usize) -> EpochResult<Vec<EventCatalogEntry>> {
        if limit == 0 || limit > MAX_CATALOG_ENTRIES {
            return Err(EpochError::InvalidArgument(format!(
                "event catalog search limit must be between 1 and {MAX_CATALOG_ENTRIES}"
            )));
        }
        let query = query.trim().to_ascii_lowercase();
        Ok(self
            .entries
            .values()
            .filter(|entry| {
                query.is_empty()
                    || entry.event_type.to_ascii_lowercase().contains(&query)
                    || entry.owner.to_ascii_lowercase().contains(&query)
                    || entry
                        .sources
                        .iter()
                        .chain(&entry.consumers)
                        .any(|value| value.to_ascii_lowercase().contains(&query))
            })
            .take(limit)
            .cloned()
            .collect())
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn validate_snapshot(&self) -> EpochResult<()> {
        if self.entries.len() > MAX_CATALOG_ENTRIES {
            return Err(EpochError::InvalidArgument(
                "event catalog snapshot exceeds its entry limit".into(),
            ));
        }
        for (event_type, entry) in &self.entries {
            validate_catalog_entry(entry)?;
            if entry.event_type != *event_type || entry.revision == 0 {
                return Err(EpochError::InvalidArgument(format!(
                    "event catalog entry {event_type} snapshot identity is invalid"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointObservation {
    pub pool: String,
    pub endpoint: String,
    pub region: String,
    pub priority: u16,
    pub healthy: bool,
    pub observed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointRoute {
    pub pool: String,
    pub endpoint: String,
    pub region: String,
    pub priority: u16,
    pub observed_at_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointRegistry {
    pools: BTreeMap<String, BTreeMap<String, EndpointObservation>>,
}

impl EndpointRegistry {
    pub fn observe(&mut self, observation: EndpointObservation) -> EpochResult<()> {
        validate_observation(&observation)?;
        if !self.pools.contains_key(&observation.pool) && self.pools.len() >= MAX_ENDPOINT_POOLS {
            return Err(EpochError::Capacity(format!(
                "endpoint registry reached its {MAX_ENDPOINT_POOLS} pool limit"
            )));
        }
        let pool = self.pools.entry(observation.pool.clone()).or_default();
        if !pool.contains_key(&observation.endpoint) && pool.len() >= MAX_ENDPOINTS_PER_POOL {
            return Err(EpochError::Capacity(format!(
                "endpoint pool reached its {MAX_ENDPOINTS_PER_POOL} endpoint limit"
            )));
        }
        if pool
            .get(&observation.endpoint)
            .is_some_and(|current| observation.observed_at_ms < current.observed_at_ms)
        {
            return Err(EpochError::Conflict(
                "endpoint health observation is older than current state".into(),
            ));
        }
        pool.insert(observation.endpoint.clone(), observation);
        Ok(())
    }

    pub fn route(&self, pool: &str) -> EpochResult<EndpointRoute> {
        validate_resource_name(pool)?;
        let observation = self
            .pools
            .get(pool)
            .and_then(|endpoints| {
                endpoints
                    .values()
                    .filter(|endpoint| endpoint.healthy)
                    .min_by(|left, right| {
                        (left.priority, &left.region, &left.endpoint).cmp(&(
                            right.priority,
                            &right.region,
                            &right.endpoint,
                        ))
                    })
            })
            .ok_or_else(|| {
                EpochError::Unavailable(format!("endpoint pool {pool} has no healthy route"))
            })?;
        Ok(EndpointRoute {
            pool: observation.pool.clone(),
            endpoint: observation.endpoint.clone(),
            region: observation.region.clone(),
            priority: observation.priority,
            observed_at_ms: observation.observed_at_ms,
        })
    }

    pub fn observations(&self, pool: &str) -> EpochResult<Vec<EndpointObservation>> {
        validate_resource_name(pool)?;
        self.pools
            .get(pool)
            .map(|endpoints| endpoints.values().cloned().collect())
            .ok_or_else(|| EpochError::NotFound(pool.to_owned()))
    }

    pub fn is_empty(&self) -> bool {
        self.pools.is_empty()
    }

    pub(crate) fn validate_snapshot(&self) -> EpochResult<()> {
        if self.pools.len() > MAX_ENDPOINT_POOLS {
            return Err(EpochError::InvalidArgument(
                "endpoint snapshot exceeds its pool limit".into(),
            ));
        }
        for (pool_name, endpoints) in &self.pools {
            validate_resource_name(pool_name)?;
            if endpoints.is_empty() || endpoints.len() > MAX_ENDPOINTS_PER_POOL {
                return Err(EpochError::InvalidArgument(format!(
                    "endpoint pool {pool_name} snapshot size is invalid"
                )));
            }
            for (endpoint_url, observation) in endpoints {
                validate_observation(observation)?;
                if observation.pool != *pool_name || observation.endpoint != *endpoint_url {
                    return Err(EpochError::InvalidArgument(format!(
                        "endpoint pool {pool_name} snapshot identity is invalid"
                    )));
                }
            }
        }
        Ok(())
    }
}

fn validate_catalog_entry(entry: &EventCatalogEntry) -> EpochResult<()> {
    validate_text("event type", &entry.event_type)?;
    validate_text("event owner", &entry.owner)?;
    validate_text("event classification", &entry.classification)?;
    if let Some(reference) = &entry.schema_ref {
        validate_text("event schema reference", reference)?;
    }
    if entry.sources.len() > MAX_LINEAGE_ENTRIES || entry.consumers.len() > MAX_LINEAGE_ENTRIES {
        return Err(EpochError::InvalidArgument(format!(
            "event lineage sets cannot exceed {MAX_LINEAGE_ENTRIES} entries"
        )));
    }
    for value in entry.sources.iter().chain(&entry.consumers) {
        validate_text("event lineage value", value)?;
    }
    if let Some(sample) = &entry.sample_payload {
        let size = serde_json::to_vec(sample)
            .map_err(|error| EpochError::InvalidArgument(error.to_string()))?
            .len();
        if size > MAX_SAMPLE_BYTES {
            return Err(EpochError::InvalidArgument(format!(
                "event sample is {size} bytes; maximum is {MAX_SAMPLE_BYTES}"
            )));
        }
    }
    Ok(())
}

fn validate_observation(observation: &EndpointObservation) -> EpochResult<()> {
    validate_resource_name(&observation.pool)?;
    validate_resource_name(&observation.region)?;
    let url = Url::parse(&observation.endpoint)
        .map_err(|error| EpochError::InvalidArgument(format!("invalid endpoint URL: {error}")))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(EpochError::InvalidArgument(
            "endpoint must be an absolute credential-free HTTP(S) URL without a fragment".into(),
        ));
    }
    Ok(())
}

fn validate_text(field: &str, value: &str) -> EpochResult<()> {
    if value.is_empty()
        || value.len() > MAX_CATALOG_TEXT_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(EpochError::InvalidArgument(format!(
            "{field} must be between 1 and {MAX_CATALOG_TEXT_BYTES} printable bytes"
        )));
    }
    Ok(())
}

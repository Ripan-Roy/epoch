//! Bounded advanced Cache value types with deterministic, portable behavior.

use std::collections::{BTreeMap, BTreeSet};

use epoch_core::{EpochError, EpochResult};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

pub const MAX_CACHE_BITMAP_BITS: u32 = 1_048_576;
pub const MAX_CACHE_FILTER_BITS: u32 = 8_388_608;
pub const MAX_CACHE_GEO_POINTS: usize = 10_000;
pub const MAX_CACHE_JSON_BYTES: usize = 256 * 1024;
pub const MAX_CACHE_JSON_INDEX_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_CACHE_JSON_INDEX_DOCUMENTS: usize = 10_000;
pub const MAX_CACHE_JSON_INDEX_POINTERS: usize = 32;
pub const MAX_CACHE_VECTOR_DIMENSIONS: usize = 2_048;
pub const MAX_CACHE_VECTOR_DOCUMENTS: usize = 10_000;
const MIN_CARDINALITY_PRECISION: u8 = 4;
const MAX_CARDINALITY_PRECISION: u8 = 16;
const MAX_CUCKOO_BUCKETS: u32 = 65_536;
const MAX_CUCKOO_BUCKET_SIZE: u8 = 8;
const MAX_CUCKOO_KICKS: usize = 256;
const EARTH_RADIUS_METERS: f64 = 6_371_000.0;
const MAX_U64_AS_F64: f64 = 18_446_744_073_709_551_615.0;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct CacheBitmap {
    #[serde(with = "decimal_u64_vec")]
    words: Vec<u64>,
}

impl CacheBitmap {
    pub fn set(&mut self, bit: u32, value: bool) -> EpochResult<bool> {
        if bit >= MAX_CACHE_BITMAP_BITS {
            return Err(EpochError::InvalidArgument(format!(
                "bitmap bit {bit} exceeds maximum {}",
                MAX_CACHE_BITMAP_BITS - 1
            )));
        }
        let word = usize::try_from(bit / 64)
            .map_err(|_| EpochError::Capacity("bitmap index cannot be represented".into()))?;
        let mask = 1_u64 << (bit % 64);
        let previous = self
            .words
            .get(word)
            .is_some_and(|current| current & mask != 0);
        if value {
            if self.words.len() <= word {
                self.words.resize(word + 1, 0);
            }
            self.words[word] |= mask;
        } else if let Some(current) = self.words.get_mut(word) {
            *current &= !mask;
            while self.words.last() == Some(&0) {
                self.words.pop();
            }
        }
        Ok(previous)
    }

    pub fn get(&self, bit: u32) -> EpochResult<bool> {
        if bit >= MAX_CACHE_BITMAP_BITS {
            return Err(EpochError::InvalidArgument(format!(
                "bitmap bit {bit} exceeds maximum {}",
                MAX_CACHE_BITMAP_BITS - 1
            )));
        }
        let word = usize::try_from(bit / 64)
            .map_err(|_| EpochError::Capacity("bitmap index cannot be represented".into()))?;
        Ok(self
            .words
            .get(word)
            .is_some_and(|current| current & (1_u64 << (bit % 64)) != 0))
    }

    pub fn count(&self) -> u64 {
        self.words
            .iter()
            .map(|word| u64::from(word.count_ones()))
            .sum()
    }

    pub fn validate(&self) -> EpochResult<()> {
        let maximum_words = usize::try_from(MAX_CACHE_BITMAP_BITS.div_ceil(64))
            .map_err(|_| EpochError::Capacity("bitmap limit cannot be represented".into()))?;
        if self.words.len() > maximum_words || self.words.last() == Some(&0) {
            return Err(EpochError::InvalidArgument(
                "bitmap words are oversized or non-canonical".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheCardinality {
    precision: u8,
    registers: Vec<u8>,
}

impl CacheCardinality {
    pub fn new(precision: u8) -> EpochResult<Self> {
        validate_cardinality_precision(precision)?;
        Ok(Self {
            precision,
            registers: vec![0; 1_usize << precision],
        })
    }

    pub fn add(&mut self, value: &[u8]) -> bool {
        let hash = stable_hash(value);
        let index_mask = (1_u64 << self.precision) - 1;
        let index = usize::try_from(hash & index_mask).unwrap_or(0);
        let remainder = hash >> self.precision;
        let rank = u8::try_from(
            remainder
                .leading_zeros()
                .saturating_sub(u32::from(self.precision))
                + 1,
        )
        .unwrap_or(u8::MAX);
        let previous = self.registers[index];
        self.registers[index] = previous.max(rank);
        self.registers[index] != previous
    }

    pub fn estimate(&self) -> u64 {
        let register_count = f64::from(u32::try_from(self.registers.len()).unwrap_or(u32::MAX));
        let alpha = match self.registers.len() {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / register_count),
        };
        let sum = self
            .registers
            .iter()
            .map(|register| 2_f64.powi(-i32::from(*register)))
            .sum::<f64>();
        let raw = alpha * register_count * register_count / sum;
        let zero_registers = f64::from(
            self.registers
                .iter()
                .fold(0_u32, |count, register| count + u32::from(*register == 0)),
        );
        let corrected = if raw <= 2.5 * register_count && zero_registers > 0.0 {
            register_count * (register_count / zero_registers).ln()
        } else {
            raw
        };
        if !corrected.is_finite() || corrected >= MAX_U64_AS_F64 {
            return u64::MAX;
        }
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "the finite non-negative estimate is explicitly rounded and range-checked above"
        )]
        {
            corrected.round().max(0.0) as u64
        }
    }

    pub const fn precision(&self) -> u8 {
        self.precision
    }

    pub fn validate(&self) -> EpochResult<()> {
        validate_cardinality_precision(self.precision)?;
        if self.registers.len() != 1_usize << self.precision
            || self.registers.iter().any(|register| *register > 65)
        {
            return Err(EpochError::InvalidArgument(
                "cardinality register layout is invalid".into(),
            ));
        }
        Ok(())
    }
}

fn validate_cardinality_precision(precision: u8) -> EpochResult<()> {
    if !(MIN_CARDINALITY_PRECISION..=MAX_CARDINALITY_PRECISION).contains(&precision) {
        return Err(EpochError::InvalidArgument(format!(
            "cardinality precision must be between {MIN_CARDINALITY_PRECISION} and {MAX_CARDINALITY_PRECISION}"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheBloomFilter {
    bit_count: u32,
    hashes: u8,
    #[serde(with = "decimal_u64_vec")]
    words: Vec<u64>,
}

impl CacheBloomFilter {
    pub fn new(bit_count: u32, hashes: u8) -> EpochResult<Self> {
        validate_filter_shape(bit_count, hashes)?;
        let words = usize::try_from(bit_count.div_ceil(64))
            .map_err(|_| EpochError::Capacity("Bloom filter size cannot be represented".into()))?;
        Ok(Self {
            bit_count,
            hashes,
            words: vec![0; words],
        })
    }

    pub fn add(&mut self, value: &[u8]) -> bool {
        let positions = filter_positions(value, self.bit_count, self.hashes);
        let changed = positions
            .iter()
            .any(|position| !bit_is_set(&self.words, *position));
        for position in positions {
            set_bit(&mut self.words, position);
        }
        changed
    }

    pub fn contains(&self, value: &[u8]) -> bool {
        filter_positions(value, self.bit_count, self.hashes)
            .into_iter()
            .all(|position| bit_is_set(&self.words, position))
    }

    pub fn validate(&self) -> EpochResult<()> {
        validate_filter_shape(self.bit_count, self.hashes)?;
        let expected = usize::try_from(self.bit_count.div_ceil(64))
            .map_err(|_| EpochError::Capacity("Bloom filter size cannot be represented".into()))?;
        if self.words.len() != expected {
            return Err(EpochError::InvalidArgument(
                "Bloom filter word layout is invalid".into(),
            ));
        }
        let unused = expected * 64 - usize::try_from(self.bit_count).unwrap_or(0);
        if unused > 0
            && self
                .words
                .last()
                .is_some_and(|word| *word >> (64 - unused) != 0)
        {
            return Err(EpochError::InvalidArgument(
                "Bloom filter contains non-canonical high bits".into(),
            ));
        }
        Ok(())
    }
}

fn validate_filter_shape(bit_count: u32, hashes: u8) -> EpochResult<()> {
    if !(64..=MAX_CACHE_FILTER_BITS).contains(&bit_count) || !(1..=16).contains(&hashes) {
        return Err(EpochError::InvalidArgument(format!(
            "filter requires 64..={MAX_CACHE_FILTER_BITS} bits and 1..=16 hashes"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheCuckooFilter {
    bucket_count: u32,
    bucket_size: u8,
    buckets: Vec<Vec<u16>>,
}

impl CacheCuckooFilter {
    pub fn new(bucket_count: u32, bucket_size: u8) -> EpochResult<Self> {
        if !bucket_count.is_power_of_two()
            || !(2..=MAX_CUCKOO_BUCKETS).contains(&bucket_count)
            || !(1..=MAX_CUCKOO_BUCKET_SIZE).contains(&bucket_size)
        {
            return Err(EpochError::InvalidArgument(format!(
                "Cuckoo filter requires a power-of-two bucket count between 2 and {MAX_CUCKOO_BUCKETS} and bucket size 1..={MAX_CUCKOO_BUCKET_SIZE}"
            )));
        }
        Ok(Self {
            bucket_count,
            bucket_size,
            buckets: vec![Vec::new(); usize::try_from(bucket_count).unwrap_or(0)],
        })
    }

    pub fn add(&mut self, value: &[u8]) -> EpochResult<bool> {
        let (fingerprint, first, second) = self.locations(value);
        if self.buckets[first].contains(&fingerprint) || self.buckets[second].contains(&fingerprint)
        {
            return Ok(false);
        }
        let mut candidate = self.clone();
        if candidate.insert_if_available(first, fingerprint)
            || candidate.insert_if_available(second, fingerprint)
        {
            *self = candidate;
            return Ok(true);
        }
        let mut current = fingerprint;
        let mut bucket = first;
        for step in 0..MAX_CUCKOO_KICKS {
            candidate.buckets[bucket].sort_unstable();
            let slot = step % candidate.buckets[bucket].len();
            std::mem::swap(&mut candidate.buckets[bucket][slot], &mut current);
            bucket = candidate.alternate(bucket, current);
            if candidate.insert_if_available(bucket, current) {
                *self = candidate;
                return Ok(true);
            }
        }
        Err(EpochError::Capacity(
            "Cuckoo filter insertion exhausted its deterministic relocation budget".into(),
        ))
    }

    pub fn contains(&self, value: &[u8]) -> bool {
        let (fingerprint, first, second) = self.locations(value);
        self.buckets[first].contains(&fingerprint) || self.buckets[second].contains(&fingerprint)
    }

    pub fn delete(&mut self, value: &[u8]) -> bool {
        let (fingerprint, first, second) = self.locations(value);
        for bucket in [first, second] {
            if let Some(position) = self.buckets[bucket]
                .iter()
                .position(|candidate| *candidate == fingerprint)
            {
                self.buckets[bucket].remove(position);
                return true;
            }
        }
        false
    }

    fn locations(&self, value: &[u8]) -> (u16, usize, usize) {
        let hash = stable_hash(value);
        let fingerprint = u16::try_from((hash >> 48) | 1).unwrap_or(1);
        let mask = u64::from(self.bucket_count - 1);
        let first = usize::try_from(hash & mask).unwrap_or(0);
        let second = self.alternate(first, fingerprint);
        (fingerprint, first, second)
    }

    fn alternate(&self, bucket: usize, fingerprint: u16) -> usize {
        let hash = stable_hash(&fingerprint.to_be_bytes());
        let mask = u64::from(self.bucket_count - 1);
        bucket ^ usize::try_from(hash & mask).unwrap_or(0)
    }

    fn insert_if_available(&mut self, bucket: usize, fingerprint: u16) -> bool {
        if self.buckets[bucket].len() >= usize::from(self.bucket_size) {
            return false;
        }
        self.buckets[bucket].push(fingerprint);
        self.buckets[bucket].sort_unstable();
        true
    }

    pub fn validate(&self) -> EpochResult<()> {
        Self::new(self.bucket_count, self.bucket_size)?;
        if self.buckets.len() != usize::try_from(self.bucket_count).unwrap_or(0)
            || self.buckets.iter().any(|bucket| {
                !bucket.is_empty()
                    && (bucket.len() > usize::from(self.bucket_size)
                        || !bucket.windows(2).all(|pair| pair[0] < pair[1])
                        || bucket.contains(&0))
            })
        {
            return Err(EpochError::InvalidArgument(
                "Cuckoo filter bucket layout is invalid".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheGeoPoint {
    longitude_microdegrees: i32,
    latitude_microdegrees: i32,
}

impl CacheGeoPoint {
    pub fn from_degrees(longitude: f64, latitude: f64) -> EpochResult<Self> {
        if !longitude.is_finite()
            || !latitude.is_finite()
            || !(-180.0..=180.0).contains(&longitude)
            || !(-90.0..=90.0).contains(&latitude)
        {
            return Err(EpochError::InvalidArgument(
                "geospatial coordinates must be finite and within longitude [-180,180], latitude [-90,90]"
                    .into(),
            ));
        }
        #[allow(clippy::cast_possible_truncation)]
        Ok(Self {
            longitude_microdegrees: (longitude * 1_000_000.0).round() as i32,
            latitude_microdegrees: (latitude * 1_000_000.0).round() as i32,
        })
    }

    pub fn longitude(&self) -> f64 {
        f64::from(self.longitude_microdegrees) / 1_000_000.0
    }

    pub fn latitude(&self) -> f64 {
        f64::from(self.latitude_microdegrees) / 1_000_000.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct CacheGeoIndex {
    points: BTreeMap<String, CacheGeoPoint>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheGeoHit {
    pub member: String,
    pub point: CacheGeoPoint,
    pub distance_meters: f64,
}

impl CacheGeoIndex {
    pub fn upsert(&mut self, member: impl Into<String>, point: CacheGeoPoint) -> EpochResult<bool> {
        let member = member.into();
        if member.is_empty() || member.len() > 4_096 {
            return Err(EpochError::InvalidArgument(
                "geospatial member must be 1..=4096 bytes".into(),
            ));
        }
        if !self.points.contains_key(&member) && self.points.len() >= MAX_CACHE_GEO_POINTS {
            return Err(EpochError::Capacity(format!(
                "geospatial index supports at most {MAX_CACHE_GEO_POINTS} points"
            )));
        }
        Ok(self.points.insert(member, point).is_none())
    }

    pub fn remove(&mut self, member: &str) -> bool {
        self.points.remove(member).is_some()
    }

    pub fn radius(
        &self,
        center: CacheGeoPoint,
        radius_meters: f64,
        limit: usize,
    ) -> EpochResult<Vec<CacheGeoHit>> {
        if !radius_meters.is_finite() || radius_meters < 0.0 || !(1..=1_000).contains(&limit) {
            return Err(EpochError::InvalidArgument(
                "geospatial radius must be finite and non-negative and limit must be 1..=1000"
                    .into(),
            ));
        }
        let mut hits = self
            .points
            .iter()
            .filter_map(|(member, point)| {
                let distance_meters = geo_distance(center, *point);
                (distance_meters <= radius_meters).then(|| CacheGeoHit {
                    member: member.clone(),
                    point: *point,
                    distance_meters,
                })
            })
            .collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            left.distance_meters
                .total_cmp(&right.distance_meters)
                .then_with(|| left.member.cmp(&right.member))
        });
        hits.truncate(limit);
        Ok(hits)
    }

    pub fn validate(&self) -> EpochResult<()> {
        if self.points.len() > MAX_CACHE_GEO_POINTS
            || self
                .points
                .keys()
                .any(|member| member.is_empty() || member.len() > 4_096)
        {
            return Err(EpochError::InvalidArgument(
                "geospatial index is oversized or contains an invalid member".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheJsonDocument {
    value: Value,
}

impl CacheJsonDocument {
    pub fn new(value: Value) -> EpochResult<Self> {
        let document = Self { value };
        document.validate()?;
        Ok(document)
    }

    pub const fn value(&self) -> &Value {
        &self.value
    }

    pub fn pointer(&self, pointer: &str) -> Option<&Value> {
        self.value.pointer(pointer)
    }

    pub fn set_pointer(&mut self, pointer: &str, value: Value) -> EpochResult<Option<Value>> {
        let mut candidate = self.clone();
        let previous = candidate.set_pointer_unchecked(pointer, value)?;
        candidate.validate()?;
        *self = candidate;
        Ok(previous)
    }

    fn set_pointer_unchecked(&mut self, pointer: &str, value: Value) -> EpochResult<Option<Value>> {
        if pointer.is_empty() {
            let previous = std::mem::replace(&mut self.value, value);
            return Ok(Some(previous));
        }
        let tokens = json_pointer_tokens(pointer)?;
        let mut current = &mut self.value;
        for token in &tokens[..tokens.len() - 1] {
            if !current.is_object() {
                *current = Value::Object(Map::new());
            }
            current = current
                .as_object_mut()
                .expect("object initialized above")
                .entry(token.clone())
                .or_insert_with(|| Value::Object(Map::new()));
        }
        if !current.is_object() {
            *current = Value::Object(Map::new());
        }
        let previous = current
            .as_object_mut()
            .expect("object initialized above")
            .insert(tokens.last().cloned().unwrap_or_default(), value);
        Ok(previous)
    }

    pub fn remove_pointer(&mut self, pointer: &str) -> EpochResult<Option<Value>> {
        let tokens = json_pointer_tokens(pointer)?;
        let Some((last, parents)) = tokens.split_last() else {
            return Err(EpochError::InvalidArgument(
                "removing the JSON document root is not supported".into(),
            ));
        };
        let mut current = &mut self.value;
        for token in parents {
            let Some(next) = current
                .as_object_mut()
                .and_then(|object| object.get_mut(token))
            else {
                return Ok(None);
            };
            current = next;
        }
        Ok(current
            .as_object_mut()
            .and_then(|object| object.remove(last)))
    }

    pub fn validate(&self) -> EpochResult<()> {
        let encoded = serde_json::to_vec(&self.value)
            .map_err(|error| EpochError::InvalidArgument(error.to_string()))?;
        if encoded.len() > MAX_CACHE_JSON_BYTES || json_depth(&self.value) > 64 {
            return Err(EpochError::InvalidArgument(format!(
                "JSON document exceeds {MAX_CACHE_JSON_BYTES} bytes or depth 64"
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheJsonIndex {
    indexed_pointers: BTreeSet<String>,
    documents: BTreeMap<String, CacheJsonDocument>,
    secondary: BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheJsonHit {
    pub id: String,
    pub document: CacheJsonDocument,
}

impl CacheJsonIndex {
    pub fn new(indexed_pointers: BTreeSet<String>) -> EpochResult<Self> {
        validate_indexed_pointers(&indexed_pointers)?;
        Ok(Self {
            indexed_pointers,
            documents: BTreeMap::new(),
            secondary: BTreeMap::new(),
        })
    }

    pub fn upsert(
        &mut self,
        id: impl Into<String>,
        document: CacheJsonDocument,
    ) -> EpochResult<bool> {
        let id = id.into();
        document.validate()?;
        if id.is_empty() || id.len() > 1_024 {
            return Err(EpochError::InvalidArgument(
                "JSON index document ID must be 1..=1024 bytes".into(),
            ));
        }
        if !self.documents.contains_key(&id)
            && self.documents.len() >= MAX_CACHE_JSON_INDEX_DOCUMENTS
        {
            return Err(EpochError::Capacity(format!(
                "JSON index supports at most {MAX_CACHE_JSON_INDEX_DOCUMENTS} documents"
            )));
        }
        let mut candidate = self.clone();
        let added = !candidate.documents.contains_key(&id);
        candidate.documents.insert(id, document);
        candidate.rebuild_secondary()?;
        candidate.validate()?;
        *self = candidate;
        Ok(added)
    }

    pub fn remove(&mut self, id: &str) -> EpochResult<bool> {
        let mut candidate = self.clone();
        let removed = candidate.documents.remove(id).is_some();
        if removed {
            candidate.rebuild_secondary()?;
            candidate.validate()?;
            *self = candidate;
        }
        Ok(removed)
    }

    pub fn search_exact(
        &self,
        pointer: &str,
        value: &Value,
        limit: usize,
    ) -> EpochResult<Vec<CacheJsonHit>> {
        if !self.indexed_pointers.contains(pointer) || !(1..=100).contains(&limit) {
            return Err(EpochError::InvalidArgument(
                "JSON search pointer is not indexed or limit is outside 1..=100".into(),
            ));
        }
        let canonical = canonical_json(value)?;
        Ok(self
            .secondary
            .get(pointer)
            .and_then(|values| values.get(&canonical))
            .into_iter()
            .flatten()
            .take(limit)
            .filter_map(|id| {
                self.documents
                    .get(id)
                    .cloned()
                    .map(|document| CacheJsonHit {
                        id: id.clone(),
                        document,
                    })
            })
            .collect())
    }

    pub fn validate(&self) -> EpochResult<()> {
        validate_indexed_pointers(&self.indexed_pointers)?;
        if self.documents.len() > MAX_CACHE_JSON_INDEX_DOCUMENTS
            || self.documents.iter().any(|(id, document)| {
                id.is_empty() || id.len() > 1_024 || document.validate().is_err()
            })
        {
            return Err(EpochError::InvalidArgument(
                "JSON secondary index document registry is invalid".into(),
            ));
        }
        let mut expected = Self {
            indexed_pointers: self.indexed_pointers.clone(),
            documents: self.documents.clone(),
            secondary: BTreeMap::new(),
        };
        expected.rebuild_secondary()?;
        if expected.secondary != self.secondary {
            return Err(EpochError::InvalidArgument(
                "JSON secondary index postings are not canonical".into(),
            ));
        }
        let encoded = serde_json::to_vec(self)
            .map_err(|error| EpochError::InvalidArgument(error.to_string()))?;
        if encoded.len() > MAX_CACHE_JSON_INDEX_BYTES {
            return Err(EpochError::Capacity(format!(
                "JSON secondary index is {} bytes; maximum is {MAX_CACHE_JSON_INDEX_BYTES}",
                encoded.len()
            )));
        }
        Ok(())
    }

    fn rebuild_secondary(&mut self) -> EpochResult<()> {
        self.secondary.clear();
        for (id, document) in &self.documents {
            for pointer in &self.indexed_pointers {
                let Some(value) = document.pointer(pointer) else {
                    continue;
                };
                self.secondary
                    .entry(pointer.clone())
                    .or_default()
                    .entry(canonical_json(value)?)
                    .or_default()
                    .insert(id.clone());
            }
        }
        Ok(())
    }
}

fn validate_indexed_pointers(pointers: &BTreeSet<String>) -> EpochResult<()> {
    if pointers.is_empty() || pointers.len() > MAX_CACHE_JSON_INDEX_POINTERS {
        return Err(EpochError::InvalidArgument(format!(
            "JSON secondary index requires 1..={MAX_CACHE_JSON_INDEX_POINTERS} pointers"
        )));
    }
    for pointer in pointers {
        if pointer.is_empty() || pointer.len() > 1_024 {
            return Err(EpochError::InvalidArgument(
                "JSON secondary index pointer is empty or oversized".into(),
            ));
        }
        json_pointer_tokens(pointer)?;
    }
    Ok(())
}

fn canonical_json(value: &Value) -> EpochResult<String> {
    serde_json::to_string(value).map_err(|error| EpochError::InvalidArgument(error.to_string()))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheVectorDocument {
    vector: Vec<f32>,
    #[serde(default)]
    text: String,
    #[serde(default)]
    metadata: BTreeMap<String, String>,
}

impl CacheVectorDocument {
    pub fn new(
        vector: Vec<f32>,
        text: impl Into<String>,
        metadata: BTreeMap<String, String>,
    ) -> EpochResult<Self> {
        let document = Self {
            vector,
            text: text.into(),
            metadata,
        };
        document.validate(None)?;
        Ok(document)
    }

    pub fn dimensions(&self) -> usize {
        self.vector.len()
    }

    fn validate(&self, dimensions: Option<usize>) -> EpochResult<()> {
        if self.vector.is_empty()
            || self.vector.len() > MAX_CACHE_VECTOR_DIMENSIONS
            || dimensions.is_some_and(|expected| self.vector.len() != expected)
            || self.vector.iter().any(|component| !component.is_finite())
            || vector_norm(&self.vector) == 0.0
            || self.text.len() > 64 * 1024
            || self.metadata.len() > 64
            || self
                .metadata
                .iter()
                .any(|(key, value)| key.is_empty() || key.len() > 128 || value.len() > 1_024)
        {
            return Err(EpochError::InvalidArgument(
                "vector document dimensions, values, text, or metadata are invalid".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheVectorIndex {
    dimensions: usize,
    documents: BTreeMap<String, CacheVectorDocument>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheVectorHit {
    pub id: String,
    pub score: f64,
    pub vector_score: f64,
    pub text_score: f64,
    pub metadata: BTreeMap<String, String>,
}

impl CacheVectorIndex {
    pub fn new(dimensions: usize) -> EpochResult<Self> {
        if !(1..=MAX_CACHE_VECTOR_DIMENSIONS).contains(&dimensions) {
            return Err(EpochError::InvalidArgument(format!(
                "vector dimensions must be between 1 and {MAX_CACHE_VECTOR_DIMENSIONS}"
            )));
        }
        Ok(Self {
            dimensions,
            documents: BTreeMap::new(),
        })
    }

    pub fn upsert(
        &mut self,
        id: impl Into<String>,
        document: CacheVectorDocument,
    ) -> EpochResult<bool> {
        let id = id.into();
        document.validate(Some(self.dimensions))?;
        if id.is_empty() || id.len() > 1_024 {
            return Err(EpochError::InvalidArgument(
                "vector document ID must be 1..=1024 bytes".into(),
            ));
        }
        if !self.documents.contains_key(&id) && self.documents.len() >= MAX_CACHE_VECTOR_DOCUMENTS {
            return Err(EpochError::Capacity(format!(
                "vector index supports at most {MAX_CACHE_VECTOR_DOCUMENTS} documents"
            )));
        }
        Ok(self.documents.insert(id, document).is_none())
    }

    pub fn remove(&mut self, id: &str) -> bool {
        self.documents.remove(id).is_some()
    }

    pub fn search(
        &self,
        query_vector: &[f32],
        query_text: &str,
        vector_weight: f64,
        filters: &BTreeMap<String, String>,
        limit: usize,
    ) -> EpochResult<Vec<CacheVectorHit>> {
        if query_vector.len() != self.dimensions
            || query_vector.iter().any(|component| !component.is_finite())
            || vector_norm(query_vector) == 0.0
            || !vector_weight.is_finite()
            || !(0.0..=1.0).contains(&vector_weight)
            || !(1..=1_000).contains(&limit)
        {
            return Err(EpochError::InvalidArgument(
                "vector query dimensions, weights, or limit are invalid".into(),
            ));
        }
        let query_tokens = text_tokens(query_text);
        let mut hits = self
            .documents
            .iter()
            .filter(|(_, document)| {
                filters
                    .iter()
                    .all(|(key, value)| document.metadata.get(key) == Some(value))
            })
            .map(|(id, document)| {
                let vector_score = cosine_similarity(query_vector, &document.vector);
                let text_score = text_similarity(&query_tokens, &text_tokens(&document.text));
                CacheVectorHit {
                    id: id.clone(),
                    score: vector_score * vector_weight + text_score * (1.0 - vector_weight),
                    vector_score,
                    text_score,
                    metadata: document.metadata.clone(),
                }
            })
            .collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.id.cmp(&right.id))
        });
        hits.truncate(limit);
        Ok(hits)
    }

    pub fn validate(&self) -> EpochResult<()> {
        Self::new(self.dimensions)?;
        if self.documents.len() > MAX_CACHE_VECTOR_DOCUMENTS {
            return Err(EpochError::InvalidArgument(
                "vector index document limit is invalid".into(),
            ));
        }
        for (id, document) in &self.documents {
            if id.is_empty() || id.len() > 1_024 {
                return Err(EpochError::InvalidArgument(
                    "vector index contains an invalid document ID".into(),
                ));
            }
            document.validate(Some(self.dimensions))?;
        }
        Ok(())
    }
}

fn stable_hash(value: &[u8]) -> u64 {
    let digest = Sha256::digest(value);
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(bytes)
}

mod decimal_u64_vec {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(values: &[u64], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        values
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<String>::deserialize(deserializer)?
            .into_iter()
            .map(|value| value.parse().map_err(serde::de::Error::custom))
            .collect()
    }
}

fn filter_positions(value: &[u8], bit_count: u32, hashes: u8) -> Vec<u32> {
    let digest = Sha256::digest(value);
    let mut first = [0_u8; 8];
    let mut second = [0_u8; 8];
    first.copy_from_slice(&digest[..8]);
    second.copy_from_slice(&digest[8..16]);
    let first = u64::from_be_bytes(first);
    let second = u64::from_be_bytes(second) | 1;
    (0..hashes)
        .map(|index| {
            u32::try_from(
                first.wrapping_add(u64::from(index).wrapping_mul(second)) % u64::from(bit_count),
            )
            .unwrap_or(0)
        })
        .collect()
}

fn bit_is_set(words: &[u64], bit: u32) -> bool {
    let word = usize::try_from(bit / 64).unwrap_or(0);
    words[word] & (1_u64 << (bit % 64)) != 0
}

fn set_bit(words: &mut [u64], bit: u32) {
    let word = usize::try_from(bit / 64).unwrap_or(0);
    words[word] |= 1_u64 << (bit % 64);
}

fn geo_distance(left: CacheGeoPoint, right: CacheGeoPoint) -> f64 {
    let latitude_delta = (right.latitude() - left.latitude()).to_radians();
    let longitude_delta = (right.longitude() - left.longitude()).to_radians();
    let left_latitude = left.latitude().to_radians();
    let right_latitude = right.latitude().to_radians();
    let haversine = (latitude_delta / 2.0).sin().powi(2)
        + left_latitude.cos() * right_latitude.cos() * (longitude_delta / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_METERS * haversine.sqrt().asin()
}

fn json_pointer_tokens(pointer: &str) -> EpochResult<Vec<String>> {
    if !pointer.starts_with('/') {
        return Err(EpochError::InvalidArgument(
            "JSON pointer must be empty or begin with '/'".into(),
        ));
    }
    pointer[1..]
        .split('/')
        .map(|token| {
            let mut decoded = String::new();
            let mut characters = token.chars();
            while let Some(character) = characters.next() {
                if character != '~' {
                    decoded.push(character);
                    continue;
                }
                match characters.next() {
                    Some('0') => decoded.push('~'),
                    Some('1') => decoded.push('/'),
                    _ => {
                        return Err(EpochError::InvalidArgument(
                            "JSON pointer contains an invalid escape".into(),
                        ));
                    }
                }
            }
            Ok(decoded)
        })
        .collect()
}

fn json_depth(value: &Value) -> usize {
    match value {
        Value::Array(values) => 1 + values.iter().map(json_depth).max().unwrap_or(0),
        Value::Object(values) => 1 + values.values().map(json_depth).max().unwrap_or(0),
        _ => 1,
    }
}

fn vector_norm(vector: &[f32]) -> f64 {
    vector
        .iter()
        .map(|component| f64::from(*component).powi(2))
        .sum::<f64>()
        .sqrt()
}

fn cosine_similarity(left: &[f32], right: &[f32]) -> f64 {
    let dot = left
        .iter()
        .zip(right)
        .map(|(left, right)| f64::from(*left) * f64::from(*right))
        .sum::<f64>();
    dot / (vector_norm(left) * vector_norm(right))
}

fn text_tokens(text: &str) -> BTreeSet<String> {
    text.split(|character: char| !character.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_lowercase)
        .collect()
}

fn text_similarity(left: &BTreeSet<String>, right: &BTreeSet<String>) -> f64 {
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }
    let intersection =
        f64::from(u32::try_from(left.intersection(right).count()).unwrap_or(u32::MAX));
    let union = f64::from(u32::try_from(left.union(right).count()).unwrap_or(u32::MAX));
    intersection / union
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitmap_is_bounded_canonical_and_counts_bits() {
        let mut bitmap = CacheBitmap::default();
        assert!(!bitmap.set(65, true).unwrap());
        assert!(bitmap.get(65).unwrap());
        assert_eq!(bitmap.count(), 1);
        assert!(bitmap.set(65, false).unwrap());
        assert_eq!(bitmap, CacheBitmap::default());
        assert!(bitmap.set(MAX_CACHE_BITMAP_BITS, true).is_err());
    }

    #[test]
    fn cardinality_is_deterministic_and_bounded() {
        let mut cardinality = CacheCardinality::new(10).unwrap();
        let mut changed = 0;
        for value in 0_u32..1_000 {
            changed += usize::from(cardinality.add(&value.to_be_bytes()));
            assert!(!cardinality.add(&value.to_be_bytes()));
        }
        assert!(changed > 500);
        assert!((900..=1_100).contains(&cardinality.estimate()));
        cardinality.validate().unwrap();
    }

    #[test]
    fn bloom_and_cuckoo_filters_support_membership_and_delete() {
        let mut bloom = CacheBloomFilter::new(1_024, 5).unwrap();
        assert!(bloom.add(b"epoch"));
        assert!(!bloom.add(b"epoch"));
        assert!(bloom.contains(b"epoch"));
        bloom.validate().unwrap();

        let mut cuckoo = CacheCuckooFilter::new(32, 4).unwrap();
        assert!(cuckoo.add(b"epoch").unwrap());
        assert!(!cuckoo.add(b"epoch").unwrap());
        assert!(cuckoo.contains(b"epoch"));
        assert!(cuckoo.delete(b"epoch"));
        assert!(!cuckoo.contains(b"epoch"));
        cuckoo.validate().unwrap();
    }

    #[test]
    fn geo_radius_is_exact_and_deterministically_ordered() {
        let mut geo = CacheGeoIndex::default();
        let kolkata = CacheGeoPoint::from_degrees(88.3639, 22.5726).unwrap();
        geo.upsert("kolkata", kolkata).unwrap();
        geo.upsert(
            "nearby",
            CacheGeoPoint::from_degrees(88.3640, 22.5727).unwrap(),
        )
        .unwrap();
        let hits = geo.radius(kolkata, 100.0, 10).unwrap();
        assert_eq!(
            hits.iter()
                .map(|hit| hit.member.as_str())
                .collect::<Vec<_>>(),
            vec!["kolkata", "nearby"]
        );
    }

    #[test]
    fn json_pointer_operations_are_canonical_and_bounded() {
        let mut document =
            CacheJsonDocument::new(serde_json::json!({"user":{"active":true}})).unwrap();
        assert_eq!(document.pointer("/user/active"), Some(&Value::Bool(true)));
        document
            .set_pointer("/user/name", Value::String("Ada".into()))
            .unwrap();
        assert_eq!(
            document.pointer("/user/name"),
            Some(&Value::String("Ada".into()))
        );
        assert_eq!(
            document.remove_pointer("/user/active").unwrap(),
            Some(Value::Bool(true))
        );
    }

    #[test]
    fn json_secondary_index_updates_exact_postings_atomically() {
        let mut index =
            CacheJsonIndex::new(BTreeSet::from(["/tenant".into(), "/user/active".into()])).unwrap();
        index
            .upsert(
                "b",
                CacheJsonDocument::new(
                    serde_json::json!({"tenant":"north","user":{"active":true}}),
                )
                .unwrap(),
            )
            .unwrap();
        index
            .upsert(
                "a",
                CacheJsonDocument::new(
                    serde_json::json!({"tenant":"north","user":{"active":false}}),
                )
                .unwrap(),
            )
            .unwrap();
        let hits = index
            .search_exact("/tenant", &Value::String("north".into()), 10)
            .unwrap();
        assert_eq!(
            hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );

        index
            .upsert(
                "a",
                CacheJsonDocument::new(serde_json::json!({"tenant":"south"})).unwrap(),
            )
            .unwrap();
        assert_eq!(
            index
                .search_exact("/tenant", &Value::String("north".into()), 10)
                .unwrap()
                .len(),
            1
        );
        assert!(index.remove("b").unwrap());
        assert!(
            index
                .search_exact("/tenant", &Value::String("north".into()), 10)
                .unwrap()
                .is_empty()
        );
        index.validate().unwrap();
    }

    #[test]
    fn vector_hybrid_search_filters_and_breaks_ties_by_id() {
        let mut index = CacheVectorIndex::new(3).unwrap();
        index
            .upsert(
                "b",
                CacheVectorDocument::new(
                    vec![1.0, 0.0, 0.0],
                    "epoch cache",
                    BTreeMap::from([("tenant".into(), "a".into())]),
                )
                .unwrap(),
            )
            .unwrap();
        index
            .upsert(
                "a",
                CacheVectorDocument::new(
                    vec![1.0, 0.0, 0.0],
                    "epoch cache",
                    BTreeMap::from([("tenant".into(), "a".into())]),
                )
                .unwrap(),
            )
            .unwrap();
        let hits = index
            .search(
                &[1.0, 0.0, 0.0],
                "epoch",
                0.5,
                &BTreeMap::from([("tenant".into(), "a".into())]),
                10,
            )
            .unwrap();
        assert_eq!(
            hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }
}

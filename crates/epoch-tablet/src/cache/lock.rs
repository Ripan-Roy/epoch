//! Deterministic opaque lease tokens for fenced Cache locks.

use epoch_core::{EpochError, EpochResult};
use sha2::{Digest, Sha256};

use super::command::MAX_CACHE_LOCK_TOKEN_BYTES;

pub const CACHE_LOCK_TOKEN_FORMAT_VERSION: u16 = 1;
const CACHE_LOCK_TOKEN_PREFIX: &str = "epoch.cache.lock.";
const CACHE_LOCK_TOKEN_CHECKSUM_DOMAIN: &[u8] = b"epoch/cache-lock-token/v1\0";
const CACHE_LOCK_TOKEN_FIXED_PAYLOAD_BYTES: usize = 70;

/// Validated server-owned metadata carried by an opaque Cache lock token.
///
/// The checksum catches accidental corruption. It is not authentication; a
/// mutating request must also match this metadata against the replicated live
/// lock and the current committed leader term.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheLockTokenMetadata {
    tablet_id: u64,
    tablet_epoch: u64,
    shard: u32,
    leader_term: u64,
    owner_epoch: u64,
    acquisition_index: u64,
    lease_generation: u64,
    lease_deadline_ms: u64,
    lock_key: String,
    owner: String,
}

impl CacheLockTokenMetadata {
    pub fn parse(token: &str) -> EpochResult<Self> {
        if token.len() > MAX_CACHE_LOCK_TOKEN_BYTES {
            return Err(invalid_lock_token("token exceeds the maximum size"));
        }
        let encoded = token
            .strip_prefix(CACHE_LOCK_TOKEN_PREFIX)
            .ok_or_else(|| invalid_lock_token("unsupported token format"))?;
        let (payload_hex, checksum_hex) = encoded
            .rsplit_once('.')
            .ok_or_else(|| invalid_lock_token("checksum is missing"))?;
        if checksum_hex.len() != 8 {
            return Err(invalid_lock_token("checksum has an invalid length"));
        }
        let payload = decode_hex(payload_hex)?;
        let checksum: [u8; 4] = decode_hex(checksum_hex)?
            .try_into()
            .map_err(|_| invalid_lock_token("checksum has an invalid length"))?;
        if checksum != lock_token_checksum(&payload) {
            return Err(invalid_lock_token("checksum does not match"));
        }
        Self::decode_payload(&payload)
    }

    pub const fn tablet_id(&self) -> u64 {
        self.tablet_id
    }

    pub const fn tablet_epoch(&self) -> u64 {
        self.tablet_epoch
    }

    pub const fn shard(&self) -> u32 {
        self.shard
    }

    pub const fn leader_term(&self) -> u64 {
        self.leader_term
    }

    pub const fn owner_epoch(&self) -> u64 {
        self.owner_epoch
    }

    pub const fn acquisition_index(&self) -> u64 {
        self.acquisition_index
    }

    pub const fn lease_generation(&self) -> u64 {
        self.lease_generation
    }

    pub const fn lease_deadline_ms(&self) -> u64 {
        self.lease_deadline_ms
    }

    pub fn lock_key(&self) -> &str {
        &self.lock_key
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the complete ownership fence must be constructed atomically"
    )]
    pub(super) fn new(
        tablet_id: u64,
        tablet_epoch: u64,
        shard: u32,
        leader_term: u64,
        owner_epoch: u64,
        acquisition_index: u64,
        lease_generation: u64,
        lease_deadline_ms: u64,
        lock_key: String,
        owner: String,
    ) -> EpochResult<Self> {
        if tablet_id == 0
            || tablet_epoch == 0
            || leader_term == 0
            || owner_epoch == 0
            || acquisition_index == 0
            || lease_generation == 0
            || lease_deadline_ms == 0
        {
            return Err(invalid_lock_token(
                "tablet, epoch, term, owner epoch, acquisition index, lease generation, and deadline must be non-zero",
            ));
        }
        if lock_key.trim().is_empty() || owner.trim().is_empty() {
            return Err(invalid_lock_token("lock key and owner are required"));
        }
        Ok(Self {
            tablet_id,
            tablet_epoch,
            shard,
            leader_term,
            owner_epoch,
            acquisition_index,
            lease_generation,
            lease_deadline_ms,
            lock_key,
            owner,
        })
    }

    pub(super) fn encode(&self) -> EpochResult<String> {
        let lock_key = self.lock_key.as_bytes();
        let owner = self.owner.as_bytes();
        let lock_key_len = u32::try_from(lock_key.len())
            .map_err(|_| invalid_lock_token("lock key is too large"))?;
        let owner_len =
            u32::try_from(owner.len()).map_err(|_| invalid_lock_token("owner is too large"))?;
        let payload_len = CACHE_LOCK_TOKEN_FIXED_PAYLOAD_BYTES
            .checked_add(lock_key.len())
            .and_then(|length| length.checked_add(owner.len()))
            .ok_or_else(|| invalid_lock_token("payload length overflowed"))?;
        let token_len = payload_len
            .checked_mul(2)
            .and_then(|length| length.checked_add(CACHE_LOCK_TOKEN_PREFIX.len() + 9))
            .ok_or_else(|| invalid_lock_token("token length overflowed"))?;
        if token_len > MAX_CACHE_LOCK_TOKEN_BYTES {
            return Err(invalid_lock_token("token exceeds the maximum size"));
        }

        let mut payload = Vec::with_capacity(payload_len);
        payload.extend_from_slice(&CACHE_LOCK_TOKEN_FORMAT_VERSION.to_be_bytes());
        payload.extend_from_slice(&self.tablet_id.to_be_bytes());
        payload.extend_from_slice(&self.tablet_epoch.to_be_bytes());
        payload.extend_from_slice(&self.shard.to_be_bytes());
        payload.extend_from_slice(&self.leader_term.to_be_bytes());
        payload.extend_from_slice(&self.owner_epoch.to_be_bytes());
        payload.extend_from_slice(&self.acquisition_index.to_be_bytes());
        payload.extend_from_slice(&self.lease_generation.to_be_bytes());
        payload.extend_from_slice(&self.lease_deadline_ms.to_be_bytes());
        payload.extend_from_slice(&lock_key_len.to_be_bytes());
        payload.extend_from_slice(&owner_len.to_be_bytes());
        payload.extend_from_slice(lock_key);
        payload.extend_from_slice(owner);
        let checksum = lock_token_checksum(&payload);
        Ok(format!(
            "{CACHE_LOCK_TOKEN_PREFIX}{}.{checksum}",
            encode_hex(&payload),
            checksum = encode_hex(&checksum)
        ))
    }

    fn decode_payload(payload: &[u8]) -> EpochResult<Self> {
        let mut cursor = 0;
        let format_version = u16::from_be_bytes(take_token_bytes(payload, &mut cursor)?);
        if format_version != CACHE_LOCK_TOKEN_FORMAT_VERSION {
            return Err(invalid_lock_token("unsupported token format version"));
        }
        let tablet_id = u64::from_be_bytes(take_token_bytes(payload, &mut cursor)?);
        let tablet_epoch = u64::from_be_bytes(take_token_bytes(payload, &mut cursor)?);
        let shard = u32::from_be_bytes(take_token_bytes(payload, &mut cursor)?);
        let leader_term = u64::from_be_bytes(take_token_bytes(payload, &mut cursor)?);
        let owner_epoch = u64::from_be_bytes(take_token_bytes(payload, &mut cursor)?);
        let acquisition_index = u64::from_be_bytes(take_token_bytes(payload, &mut cursor)?);
        let lease_generation = u64::from_be_bytes(take_token_bytes(payload, &mut cursor)?);
        let lease_deadline_ms = u64::from_be_bytes(take_token_bytes(payload, &mut cursor)?);
        let lock_key_len =
            usize::try_from(u32::from_be_bytes(take_token_bytes(payload, &mut cursor)?))
                .map_err(|_| invalid_lock_token("lock key length is unsupported"))?;
        let owner_len =
            usize::try_from(u32::from_be_bytes(take_token_bytes(payload, &mut cursor)?))
                .map_err(|_| invalid_lock_token("owner length is unsupported"))?;
        let lock_key_end = cursor
            .checked_add(lock_key_len)
            .ok_or_else(|| invalid_lock_token("lock key length overflowed"))?;
        let owner_end = lock_key_end
            .checked_add(owner_len)
            .ok_or_else(|| invalid_lock_token("owner length overflowed"))?;
        let lock_key = payload
            .get(cursor..lock_key_end)
            .ok_or_else(|| invalid_lock_token("lock key is truncated"))?;
        let owner = payload
            .get(lock_key_end..owner_end)
            .ok_or_else(|| invalid_lock_token("owner is truncated"))?;
        if owner_end != payload.len() {
            return Err(invalid_lock_token("payload has trailing bytes"));
        }
        let lock_key = String::from_utf8(lock_key.to_vec())
            .map_err(|_| invalid_lock_token("lock key is not UTF-8"))?;
        let owner = String::from_utf8(owner.to_vec())
            .map_err(|_| invalid_lock_token("owner is not UTF-8"))?;
        Self::new(
            tablet_id,
            tablet_epoch,
            shard,
            leader_term,
            owner_epoch,
            acquisition_index,
            lease_generation,
            lease_deadline_ms,
            lock_key,
            owner,
        )
    }
}

fn take_token_bytes<const N: usize>(payload: &[u8], cursor: &mut usize) -> EpochResult<[u8; N]> {
    let end = cursor
        .checked_add(N)
        .ok_or_else(|| invalid_lock_token("payload length overflowed"))?;
    let bytes = payload
        .get(*cursor..end)
        .ok_or_else(|| invalid_lock_token("payload is truncated"))?;
    *cursor = end;
    bytes
        .try_into()
        .map_err(|_| invalid_lock_token("payload field has an invalid length"))
}

fn lock_token_checksum(payload: &[u8]) -> [u8; 4] {
    let mut hasher = Sha256::new();
    hasher.update(CACHE_LOCK_TOKEN_CHECKSUM_DOMAIN);
    hasher.update(payload);
    hasher.finalize()[..4]
        .try_into()
        .expect("SHA-256 always contains four checksum bytes")
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn decode_hex(encoded: &str) -> EpochResult<Vec<u8>> {
    if !encoded.len().is_multiple_of(2) {
        return Err(invalid_lock_token("hex payload has an odd length"));
    }
    encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = decode_hex_nibble(pair[0])?;
            let low = decode_hex_nibble(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn decode_hex_nibble(value: u8) -> EpochResult<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(invalid_lock_token("hex payload is not canonical lowercase")),
    }
}

fn invalid_lock_token(detail: &str) -> EpochError {
    EpochError::InvalidArgument(format!("invalid Cache lock token: {detail}"))
}

//! Replicated Queue lease fences and their deterministic opaque token codec.

use crc32fast::Hasher;
use epoch_core::{EpochError, EpochResult};
use serde::{Deserialize, Deserializer, Serialize};

pub const LEASE_FENCE_FORMAT_VERSION: u16 = 1;
pub const MAX_FENCED_LEASE_TOKEN_BYTES: usize = 4 * 1024;
pub(crate) const FENCED_LEASE_TOKEN_PREFIX: &str = "epoch.queue.lease.";
const FENCED_LEASE_TOKEN_CHECKSUM_DOMAIN: &[u8] = b"epoch.queue.lease-token.v1\0";
const FENCED_LEASE_TOKEN_FIXED_PAYLOAD_BYTES: usize = 62;

/// Replicated ownership coordinates captured when a Queue lease is granted.
///
/// A fence is deliberately independent from any consensus implementation. The
/// Queue tablet supplies committed epoch values, while this domain type only
/// validates and carries them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct LeaseFence {
    format_version: u16,
    tablet_id: u64,
    tablet_epoch: u64,
    partition: u32,
    leader_term: u64,
    consumer_epoch: u64,
}

impl LeaseFence {
    pub fn new(
        tablet_id: u64,
        tablet_epoch: u64,
        partition: u32,
        leader_term: u64,
        consumer_epoch: u64,
    ) -> EpochResult<Self> {
        Self::with_format_version(
            LEASE_FENCE_FORMAT_VERSION,
            tablet_id,
            tablet_epoch,
            partition,
            leader_term,
            consumer_epoch,
        )
    }

    pub fn with_format_version(
        format_version: u16,
        tablet_id: u64,
        tablet_epoch: u64,
        partition: u32,
        leader_term: u64,
        consumer_epoch: u64,
    ) -> EpochResult<Self> {
        let fence = Self {
            format_version,
            tablet_id,
            tablet_epoch,
            partition,
            leader_term,
            consumer_epoch,
        };
        fence.validate()?;
        Ok(fence)
    }

    pub fn validate(&self) -> EpochResult<()> {
        if self.format_version != LEASE_FENCE_FORMAT_VERSION {
            return Err(EpochError::InvalidArgument(format!(
                "unsupported lease fence format_version {}",
                self.format_version
            )));
        }
        if self.tablet_id == 0 {
            return Err(EpochError::InvalidArgument(
                "lease fence tablet_id must be non-zero".into(),
            ));
        }
        if self.tablet_epoch == 0 {
            return Err(EpochError::InvalidArgument(
                "lease fence tablet_epoch must be non-zero".into(),
            ));
        }
        if self.leader_term == 0 {
            return Err(EpochError::InvalidArgument(
                "lease fence leader_term must be non-zero".into(),
            ));
        }
        if self.consumer_epoch == 0 {
            return Err(EpochError::InvalidArgument(
                "lease fence consumer_epoch must be non-zero".into(),
            ));
        }
        Ok(())
    }

    pub const fn format_version(self) -> u16 {
        self.format_version
    }

    pub const fn tablet_id(self) -> u64 {
        self.tablet_id
    }

    pub const fn tablet_epoch(self) -> u64 {
        self.tablet_epoch
    }

    /// Compatibility vocabulary for acknowledgement metadata.
    pub const fn resource_epoch(self) -> u64 {
        self.tablet_epoch
    }

    pub const fn partition(self) -> u32 {
        self.partition
    }

    pub const fn leader_term(self) -> u64 {
        self.leader_term
    }

    pub const fn consumer_epoch(self) -> u64 {
        self.consumer_epoch
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaseFenceWire {
    format_version: u16,
    tablet_id: u64,
    tablet_epoch: u64,
    partition: u32,
    leader_term: u64,
    consumer_epoch: u64,
}

impl<'de> Deserialize<'de> for LeaseFence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = LeaseFenceWire::deserialize(deserializer)?;
        Self::with_format_version(
            wire.format_version,
            wire.tablet_id,
            wire.tablet_epoch,
            wire.partition,
            wire.leader_term,
            wire.consumer_epoch,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Validated server-side metadata carried by a fenced lease token.
///
/// Clients continue to treat the token as an opaque string. Decoding validates
/// the canonical representation and its corruption-detection checksum. A
/// state-changing operation must additionally match the complete token against
/// the Queue's current lease; the checksum is not an authentication primitive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FencedLeaseTokenMetadata {
    pub(super) fence: LeaseFence,
    pub(super) consumer: String,
    pub(super) message_id: String,
    pub(super) lease_generation: u64,
    pub(super) lease_deadline_ms: u64,
}

impl FencedLeaseTokenMetadata {
    pub fn parse(token: &str) -> EpochResult<Self> {
        if token.len() > MAX_FENCED_LEASE_TOKEN_BYTES {
            return Err(invalid_lease_token("token exceeds the maximum size"));
        }
        let encoded = token
            .strip_prefix(FENCED_LEASE_TOKEN_PREFIX)
            .ok_or_else(|| invalid_lease_token("unsupported token format"))?;
        let (payload_hex, checksum_hex) = encoded
            .rsplit_once('.')
            .ok_or_else(|| invalid_lease_token("checksum is missing"))?;
        if checksum_hex.len() != 8 {
            return Err(invalid_lease_token("checksum has an invalid length"));
        }
        let payload = decode_hex(payload_hex)?;
        let checksum_bytes = decode_hex(checksum_hex)?;
        let checksum = u32::from_be_bytes(
            checksum_bytes
                .try_into()
                .map_err(|_| invalid_lease_token("checksum has an invalid length"))?,
        );
        if checksum != fenced_lease_token_checksum(&payload) {
            return Err(invalid_lease_token("checksum does not match"));
        }
        Self::decode_payload(&payload)
    }

    pub const fn fence(&self) -> LeaseFence {
        self.fence
    }

    pub fn consumer(&self) -> &str {
        &self.consumer
    }

    pub fn message_id(&self) -> &str {
        &self.message_id
    }

    pub const fn lease_generation(&self) -> u64 {
        self.lease_generation
    }

    pub const fn lease_deadline_ms(&self) -> u64 {
        self.lease_deadline_ms
    }

    pub(super) fn new(
        fence: LeaseFence,
        consumer: String,
        message_id: String,
        lease_generation: u64,
        lease_deadline_ms: u64,
    ) -> EpochResult<Self> {
        fence.validate()?;
        if consumer.trim().is_empty() {
            return Err(invalid_lease_token("consumer identity is required"));
        }
        if message_id.trim().is_empty() {
            return Err(invalid_lease_token("message id is required"));
        }
        if lease_generation == 0 {
            return Err(invalid_lease_token("lease generation must be non-zero"));
        }
        if lease_deadline_ms == 0 {
            return Err(invalid_lease_token("lease deadline must be non-zero"));
        }
        Ok(Self {
            fence,
            consumer,
            message_id,
            lease_generation,
            lease_deadline_ms,
        })
    }

    pub(super) fn encode(&self) -> EpochResult<String> {
        let message_id = self.message_id.as_bytes();
        let consumer = self.consumer.as_bytes();
        let message_id_len = u32::try_from(message_id.len())
            .map_err(|_| invalid_lease_token("message id is too large"))?;
        let consumer_len = u32::try_from(consumer.len())
            .map_err(|_| invalid_lease_token("consumer identity is too large"))?;
        let payload_len = FENCED_LEASE_TOKEN_FIXED_PAYLOAD_BYTES
            .checked_add(message_id.len())
            .and_then(|length| length.checked_add(consumer.len()))
            .ok_or_else(|| invalid_lease_token("payload length overflowed"))?;
        let token_len = payload_len
            .checked_mul(2)
            .and_then(|length| length.checked_add(FENCED_LEASE_TOKEN_PREFIX.len() + 9))
            .ok_or_else(|| invalid_lease_token("token length overflowed"))?;
        if token_len > MAX_FENCED_LEASE_TOKEN_BYTES {
            return Err(invalid_lease_token("token exceeds the maximum size"));
        }
        let mut payload = Vec::with_capacity(payload_len);
        payload.extend_from_slice(&self.fence.format_version.to_be_bytes());
        payload.extend_from_slice(&self.fence.tablet_id.to_be_bytes());
        payload.extend_from_slice(&self.fence.tablet_epoch.to_be_bytes());
        payload.extend_from_slice(&self.fence.partition.to_be_bytes());
        payload.extend_from_slice(&self.fence.leader_term.to_be_bytes());
        payload.extend_from_slice(&self.fence.consumer_epoch.to_be_bytes());
        payload.extend_from_slice(&self.lease_generation.to_be_bytes());
        payload.extend_from_slice(&self.lease_deadline_ms.to_be_bytes());
        payload.extend_from_slice(&message_id_len.to_be_bytes());
        payload.extend_from_slice(&consumer_len.to_be_bytes());
        payload.extend_from_slice(message_id);
        payload.extend_from_slice(consumer);
        let checksum = fenced_lease_token_checksum(&payload);
        Ok(format!(
            "{FENCED_LEASE_TOKEN_PREFIX}{}.{checksum:08x}",
            encode_hex(&payload)
        ))
    }

    fn decode_payload(payload: &[u8]) -> EpochResult<Self> {
        let mut cursor = 0;
        let format_version = u16::from_be_bytes(take_token_bytes(payload, &mut cursor)?);
        let tablet_id = u64::from_be_bytes(take_token_bytes(payload, &mut cursor)?);
        let tablet_epoch = u64::from_be_bytes(take_token_bytes(payload, &mut cursor)?);
        let partition = u32::from_be_bytes(take_token_bytes(payload, &mut cursor)?);
        let leader_term = u64::from_be_bytes(take_token_bytes(payload, &mut cursor)?);
        let consumer_epoch = u64::from_be_bytes(take_token_bytes(payload, &mut cursor)?);
        let lease_generation = u64::from_be_bytes(take_token_bytes(payload, &mut cursor)?);
        let lease_deadline_ms = u64::from_be_bytes(take_token_bytes(payload, &mut cursor)?);
        let message_id_len = u32::from_be_bytes(take_token_bytes(payload, &mut cursor)?);
        let consumer_len = u32::from_be_bytes(take_token_bytes(payload, &mut cursor)?);
        let message_id_len = usize::try_from(message_id_len)
            .map_err(|_| invalid_lease_token("message id length is unsupported"))?;
        let consumer_len = usize::try_from(consumer_len)
            .map_err(|_| invalid_lease_token("consumer identity length is unsupported"))?;
        let message_end = cursor
            .checked_add(message_id_len)
            .ok_or_else(|| invalid_lease_token("message id length overflowed"))?;
        let message_id = payload
            .get(cursor..message_end)
            .ok_or_else(|| invalid_lease_token("message id is truncated"))?;
        let consumer_end = message_end
            .checked_add(consumer_len)
            .ok_or_else(|| invalid_lease_token("consumer identity length overflowed"))?;
        let consumer = payload
            .get(message_end..consumer_end)
            .ok_or_else(|| invalid_lease_token("consumer identity is truncated"))?;
        if consumer_end != payload.len() {
            return Err(invalid_lease_token("payload has trailing bytes"));
        }
        let message_id = String::from_utf8(message_id.to_vec())
            .map_err(|_| invalid_lease_token("message id is not UTF-8"))?;
        let consumer = String::from_utf8(consumer.to_vec())
            .map_err(|_| invalid_lease_token("consumer identity is not UTF-8"))?;
        let fence = LeaseFence::with_format_version(
            format_version,
            tablet_id,
            tablet_epoch,
            partition,
            leader_term,
            consumer_epoch,
        )?;
        Self::new(
            fence,
            consumer,
            message_id,
            lease_generation,
            lease_deadline_ms,
        )
    }
}

fn take_token_bytes<const N: usize>(payload: &[u8], cursor: &mut usize) -> EpochResult<[u8; N]> {
    let end = cursor
        .checked_add(N)
        .ok_or_else(|| invalid_lease_token("payload length overflowed"))?;
    let bytes = payload
        .get(*cursor..end)
        .ok_or_else(|| invalid_lease_token("payload is truncated"))?;
    *cursor = end;
    bytes
        .try_into()
        .map_err(|_| invalid_lease_token("payload field has an invalid length"))
}

fn fenced_lease_token_checksum(payload: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(FENCED_LEASE_TOKEN_CHECKSUM_DOMAIN);
    hasher.update(payload);
    hasher.finalize()
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
        return Err(invalid_lease_token("hex payload has an odd length"));
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
        _ => Err(invalid_lease_token(
            "hex payload is not canonical lowercase",
        )),
    }
}

fn invalid_lease_token(detail: &str) -> EpochError {
    EpochError::InvalidArgument(format!("invalid fenced lease token: {detail}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lease_fence() -> LeaseFence {
        LeaseFence::new(7, 11, 3, 13, 17).unwrap()
    }

    fn token_from_payload(payload: &[u8]) -> String {
        let checksum = fenced_lease_token_checksum(payload);
        format!(
            "{FENCED_LEASE_TOKEN_PREFIX}{}.{checksum:08x}",
            encode_hex(payload)
        )
    }

    fn token_payload(token: &str) -> Vec<u8> {
        let encoded = token.strip_prefix(FENCED_LEASE_TOKEN_PREFIX).unwrap();
        let (payload, _) = encoded.rsplit_once('.').unwrap();
        decode_hex(payload).unwrap()
    }

    fn valid_token() -> String {
        FencedLeaseTokenMetadata::new(
            lease_fence(),
            "worker-a".into(),
            "token-message".into(),
            1,
            51,
        )
        .unwrap()
        .encode()
        .unwrap()
    }

    #[test]
    fn lease_fence_is_versioned_validated_and_round_trips() {
        let fence = lease_fence();
        assert_eq!(fence.format_version(), LEASE_FENCE_FORMAT_VERSION);
        assert_eq!(fence.tablet_id(), 7);
        assert_eq!(fence.tablet_epoch(), 11);
        assert_eq!(fence.resource_epoch(), 11);
        assert_eq!(fence.partition(), 3);
        assert_eq!(fence.leader_term(), 13);
        assert_eq!(fence.consumer_epoch(), 17);

        let encoded = serde_json::to_string(&fence).unwrap();
        assert_eq!(
            encoded,
            r#"{"format_version":1,"tablet_id":7,"tablet_epoch":11,"partition":3,"leader_term":13,"consumer_epoch":17}"#
        );
        assert_eq!(serde_json::from_str::<LeaseFence>(&encoded).unwrap(), fence);

        for invalid in [
            LeaseFence::new(0, 1, 0, 1, 1),
            LeaseFence::new(1, 0, 0, 1, 1),
            LeaseFence::new(1, 1, 0, 0, 1),
            LeaseFence::new(1, 1, 0, 1, 0),
            LeaseFence::with_format_version(2, 1, 1, 0, 1, 1),
        ] {
            assert!(matches!(invalid, Err(EpochError::InvalidArgument(_))));
        }
        assert!(
            serde_json::from_str::<LeaseFence>(
                r#"{"format_version":2,"tablet_id":7,"tablet_epoch":11,"partition":3,"leader_term":13,"consumer_epoch":17}"#
            )
            .is_err()
        );
    }

    #[test]
    fn parser_rejects_oversize_noncanonical_and_malformed_payloads() {
        let token = valid_token();
        let payload = token_payload(&token);

        let oversized = "x".repeat(MAX_FENCED_LEASE_TOKEN_BYTES + 1);
        assert!(matches!(
            FencedLeaseTokenMetadata::parse(&oversized),
            Err(EpochError::InvalidArgument(_))
        ));

        for truncated_length in [
            0,
            1,
            FENCED_LEASE_TOKEN_FIXED_PAYLOAD_BYTES - 1,
            payload.len() - 1,
        ] {
            let truncated = token_from_payload(&payload[..truncated_length]);
            assert!(matches!(
                FencedLeaseTokenMetadata::parse(&truncated),
                Err(EpochError::InvalidArgument(_))
            ));
        }

        let mut trailing = payload.clone();
        trailing.push(0);
        assert!(matches!(
            FencedLeaseTokenMetadata::parse(&token_from_payload(&trailing)),
            Err(EpochError::InvalidArgument(_))
        ));

        let encoded = token.strip_prefix(FENCED_LEASE_TOKEN_PREFIX).unwrap();
        let (payload_hex, checksum_hex) = encoded.rsplit_once('.').unwrap();
        assert!(payload_hex.bytes().any(|byte| matches!(byte, b'a'..=b'f')));
        let uppercase = format!(
            "{FENCED_LEASE_TOKEN_PREFIX}{}.{checksum_hex}",
            payload_hex.to_ascii_uppercase()
        );
        assert!(matches!(
            FencedLeaseTokenMetadata::parse(&uppercase),
            Err(EpochError::InvalidArgument(_))
        ));
        let trailing_after_checksum = format!("{token}00");
        assert!(matches!(
            FencedLeaseTokenMetadata::parse(&trailing_after_checksum),
            Err(EpochError::InvalidArgument(_))
        ));

        let odd_hex = format!("{FENCED_LEASE_TOKEN_PREFIX}0.{checksum_hex}");
        let invalid_hex = format!("{FENCED_LEASE_TOKEN_PREFIX}gg.{checksum_hex}");
        for malformed in [odd_hex, invalid_hex] {
            assert!(matches!(
                FencedLeaseTokenMetadata::parse(&malformed),
                Err(EpochError::InvalidArgument(_))
            ));
        }

        let mut bad_version = payload.clone();
        bad_version[..2].copy_from_slice(&2_u16.to_be_bytes());
        let mut bad_message_length = payload.clone();
        bad_message_length[54..58].copy_from_slice(&u32::MAX.to_be_bytes());
        let mut bad_consumer_length = payload.clone();
        bad_consumer_length[58..62].copy_from_slice(&u32::MAX.to_be_bytes());
        let mut bad_utf8 = payload;
        bad_utf8[FENCED_LEASE_TOKEN_FIXED_PAYLOAD_BYTES] = 0xff;
        for malformed_payload in [
            bad_version,
            bad_message_length,
            bad_consumer_length,
            bad_utf8,
        ] {
            assert!(matches!(
                FencedLeaseTokenMetadata::parse(&token_from_payload(&malformed_payload)),
                Err(EpochError::InvalidArgument(_))
            ));
        }
    }
}

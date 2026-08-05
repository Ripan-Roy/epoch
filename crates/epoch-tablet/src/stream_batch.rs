//! Canonical, bounded Stream batch compression shared by command and HTTP boundaries.

use std::{
    collections::BTreeSet,
    io::{Cursor, Read, Write},
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use epoch_core::EventEnvelope;
use flate2::{Compression, read::MultiGzDecoder, write::GzEncoder};
use lz4_flex::frame::{FrameDecoder as Lz4FrameDecoder, FrameEncoder as Lz4FrameEncoder};
use serde::{Deserialize, Serialize};
use snap::{read::FrameDecoder as SnappyFrameDecoder, write::FrameEncoder as SnappyFrameEncoder};

use crate::{TabletError, TabletResult};

/// Maximum records in one replicated Stream batch command.
pub const MAX_STREAM_BATCH_RECORDS: u16 = 1_000;
/// Maximum compressed frame stored in a consensus command. Its standard-base64
/// representation remains below the 512 KiB proposal ceiling with metadata.
pub const MAX_STREAM_BATCH_COMPRESSED_BYTES: usize = 360 * 1024;
/// Hard decompression ceiling applied before JSON parsing or profile mutation.
pub const MAX_STREAM_BATCH_UNCOMPRESSED_BYTES: usize = 4 * 1024 * 1024;
const ZSTD_MAX_WINDOW_LOG: u32 = 23;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamCompression {
    None,
    Gzip,
    Lz4,
    Snappy,
    Zstd,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamBatchRecord {
    pub client_sequence: u32,
    pub envelope: EventEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamBatchPayload {
    pub compression: StreamCompression,
    pub record_count: u16,
    pub uncompressed_bytes: u32,
    pub compressed_bytes: u32,
    pub payload_base64: String,
}

/// Validates records, serializes their canonical JSON array, and emits one
/// interoperable frame for the requested codec.
pub fn encode_stream_batch_payload(
    records: &[StreamBatchRecord],
    compression: StreamCompression,
) -> TabletResult<StreamBatchPayload> {
    validate_records(records)?;
    let uncompressed =
        serde_json::to_vec(records).map_err(|error| TabletError::Encoding(error.to_string()))?;
    validate_uncompressed_size(uncompressed.len())?;
    let compressed = compress(&uncompressed, compression)?;
    validate_compressed_size(compressed.len())?;
    Ok(StreamBatchPayload {
        compression,
        record_count: u16::try_from(records.len()).map_err(|_| {
            TabletError::InvalidCommand(format!(
                "Stream batch record count exceeds {MAX_STREAM_BATCH_RECORDS}"
            ))
        })?,
        uncompressed_bytes: u32::try_from(uncompressed.len()).map_err(|_| {
            TabletError::InvalidCommand("Stream batch uncompressed size exceeds u32".into())
        })?,
        compressed_bytes: u32::try_from(compressed.len()).map_err(|_| {
            TabletError::InvalidCommand("Stream batch compressed size exceeds u32".into())
        })?,
        payload_base64: BASE64_STANDARD.encode(compressed),
    })
}

/// Decodes and validates a client-supplied frame under all declared and hard
/// bounds. Success proves canonical JSON and exact count/size metadata.
pub fn decode_stream_batch_payload(
    payload: &StreamBatchPayload,
) -> TabletResult<Vec<StreamBatchRecord>> {
    validate_declared_bounds(payload)?;
    let compressed = BASE64_STANDARD
        .decode(payload.payload_base64.as_bytes())
        .map_err(|error| TabletError::InvalidCommand(format!("invalid batch base64: {error}")))?;
    if BASE64_STANDARD.encode(&compressed) != payload.payload_base64 {
        return Err(TabletError::InvalidCommand(
            "batch payload is not canonical standard base64".into(),
        ));
    }
    if compressed.len() != payload.compressed_bytes as usize {
        return Err(TabletError::InvalidCommand(format!(
            "batch declares {} compressed bytes but contains {}",
            payload.compressed_bytes,
            compressed.len()
        )));
    }
    validate_compressed_size(compressed.len())?;

    let uncompressed = decompress(&compressed, payload.compression)?;
    if uncompressed.len() != payload.uncompressed_bytes as usize {
        return Err(TabletError::InvalidCommand(format!(
            "batch declares {} uncompressed bytes but produced {}",
            payload.uncompressed_bytes,
            uncompressed.len()
        )));
    }
    let records: Vec<StreamBatchRecord> = serde_json::from_slice(&uncompressed)
        .map_err(|error| TabletError::Decoding(format!("invalid Stream batch JSON: {error}")))?;
    let canonical =
        serde_json::to_vec(&records).map_err(|error| TabletError::Encoding(error.to_string()))?;
    if canonical != uncompressed {
        return Err(TabletError::Decoding(
            "Stream batch records are not in canonical JSON encoding".into(),
        ));
    }
    validate_records(&records)?;
    if records.len() != usize::from(payload.record_count) {
        return Err(TabletError::InvalidCommand(format!(
            "batch declares {} records but contains {}",
            payload.record_count,
            records.len()
        )));
    }
    Ok(records)
}

fn validate_declared_bounds(payload: &StreamBatchPayload) -> TabletResult<()> {
    if payload.record_count == 0 || payload.record_count > MAX_STREAM_BATCH_RECORDS {
        return Err(TabletError::InvalidCommand(format!(
            "Stream batch record_count must be between 1 and {MAX_STREAM_BATCH_RECORDS}"
        )));
    }
    validate_compressed_size(payload.compressed_bytes as usize)?;
    validate_uncompressed_size(payload.uncompressed_bytes as usize)
}

fn validate_records(records: &[StreamBatchRecord]) -> TabletResult<()> {
    if records.is_empty() || records.len() > usize::from(MAX_STREAM_BATCH_RECORDS) {
        return Err(TabletError::InvalidCommand(format!(
            "Stream batch must contain between 1 and {MAX_STREAM_BATCH_RECORDS} records"
        )));
    }
    let mut client_sequences = BTreeSet::new();
    for record in records {
        if !client_sequences.insert(record.client_sequence) {
            return Err(TabletError::InvalidCommand(format!(
                "duplicate Stream batch client_sequence {}",
                record.client_sequence
            )));
        }
        record.envelope.validate()?;
    }
    Ok(())
}

fn validate_compressed_size(size: usize) -> TabletResult<()> {
    if size == 0 || size > MAX_STREAM_BATCH_COMPRESSED_BYTES {
        return Err(TabletError::InvalidCommand(format!(
            "Stream batch compressed size must be between 1 and {MAX_STREAM_BATCH_COMPRESSED_BYTES} bytes; observed {size}"
        )));
    }
    Ok(())
}

fn validate_uncompressed_size(size: usize) -> TabletResult<()> {
    if size == 0 || size > MAX_STREAM_BATCH_UNCOMPRESSED_BYTES {
        return Err(TabletError::InvalidCommand(format!(
            "Stream batch uncompressed size must be between 1 and {MAX_STREAM_BATCH_UNCOMPRESSED_BYTES} bytes; observed {size}"
        )));
    }
    Ok(())
}

fn compress(input: &[u8], compression: StreamCompression) -> TabletResult<Vec<u8>> {
    match compression {
        StreamCompression::None => Ok(input.to_vec()),
        StreamCompression::Gzip => {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder
                .write_all(input)
                .map_err(|error| compression_error(&error))?;
            encoder.finish().map_err(|error| compression_error(&error))
        }
        StreamCompression::Lz4 => {
            let mut encoder = Lz4FrameEncoder::new(Vec::new());
            encoder
                .write_all(input)
                .map_err(|error| compression_error(&error))?;
            encoder
                .finish()
                .map_err(|error| TabletError::Encoding(error.to_string()))
        }
        StreamCompression::Snappy => {
            let mut encoder = SnappyFrameEncoder::new(Vec::new());
            encoder
                .write_all(input)
                .map_err(|error| compression_error(&error))?;
            encoder
                .into_inner()
                .map_err(|error| TabletError::Encoding(error.error().to_string()))
        }
        StreamCompression::Zstd => zstd::stream::encode_all(Cursor::new(input), 0)
            .map_err(|error| compression_error(&error)),
    }
}

fn decompress(input: &[u8], compression: StreamCompression) -> TabletResult<Vec<u8>> {
    match compression {
        StreamCompression::None => {
            validate_uncompressed_size(input.len())?;
            Ok(input.to_vec())
        }
        StreamCompression::Gzip => read_bounded(MultiGzDecoder::new(Cursor::new(input))),
        StreamCompression::Lz4 => read_bounded(Lz4FrameDecoder::new(Cursor::new(input))),
        StreamCompression::Snappy => read_bounded(SnappyFrameDecoder::new(Cursor::new(input))),
        StreamCompression::Zstd => {
            let mut decoder = zstd::stream::read::Decoder::new(Cursor::new(input))
                .map_err(|error| decompression_error(&error))?;
            decoder
                .window_log_max(ZSTD_MAX_WINDOW_LOG)
                .map_err(|error| decompression_error(&error))?;
            read_bounded(decoder)
        }
    }
}

fn read_bounded(reader: impl Read) -> TabletResult<Vec<u8>> {
    let limit = u64::try_from(MAX_STREAM_BATCH_UNCOMPRESSED_BYTES)
        .expect("Stream batch ceiling fits u64")
        + 1;
    let mut output = Vec::new();
    reader
        .take(limit)
        .read_to_end(&mut output)
        .map_err(|error| decompression_error(&error))?;
    validate_uncompressed_size(output.len())?;
    Ok(output)
}

fn compression_error(error: &std::io::Error) -> TabletError {
    TabletError::Encoding(format!("Stream batch compression failed: {error}"))
}

fn decompression_error(error: &std::io::Error) -> TabletError {
    TabletError::Decoding(format!("Stream batch decompression failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn record(sequence: u32, padding: &str) -> StreamBatchRecord {
        let mut envelope = EventEnvelope::new(
            "codec-tests",
            "codec.record",
            json!({"padding": padding}),
            1,
        );
        envelope.id = format!("record-{sequence}");
        StreamBatchRecord {
            client_sequence: sequence,
            envelope,
        }
    }

    #[test]
    fn exact_uncompressed_ceiling_is_enforced_before_decompression_can_expand_further() {
        let oversized = vec![b'x'; MAX_STREAM_BATCH_UNCOMPRESSED_BYTES + 1];
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&oversized).unwrap();
        let compressed = encoder.finish().unwrap();
        let payload = StreamBatchPayload {
            compression: StreamCompression::Gzip,
            record_count: 1,
            uncompressed_bytes: u32::try_from(MAX_STREAM_BATCH_UNCOMPRESSED_BYTES).unwrap(),
            compressed_bytes: u32::try_from(compressed.len()).unwrap(),
            payload_base64: BASE64_STANDARD.encode(compressed),
        };
        assert!(matches!(
            decode_stream_batch_payload(&payload),
            Err(TabletError::InvalidCommand(_))
        ));
    }

    #[test]
    fn unknown_fields_and_noncanonical_json_are_rejected_after_decompression() {
        let canonical = serde_json::to_vec(&vec![record(1, "small")]).unwrap();
        let mut document: serde_json::Value = serde_json::from_slice(&canonical).unwrap();
        document[0]["unknown"] = json!(true);
        let noncanonical = serde_json::to_vec(&document).unwrap();
        let payload = StreamBatchPayload {
            compression: StreamCompression::None,
            record_count: 1,
            uncompressed_bytes: u32::try_from(noncanonical.len()).unwrap(),
            compressed_bytes: u32::try_from(noncanonical.len()).unwrap(),
            payload_base64: BASE64_STANDARD.encode(noncanonical),
        };
        assert!(matches!(
            decode_stream_batch_payload(&payload),
            Err(TabletError::Decoding(_))
        ));
    }
}

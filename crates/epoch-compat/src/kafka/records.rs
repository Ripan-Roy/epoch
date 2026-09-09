//! Lossless v2 record bodies. The upstream record type uses a map for headers,
//! whereas Kafka headers are an ordered multimap (KIP-82). Batch metadata and
//! CRC validation remain owned by kafka-protocol; this module owns bounded
//! record framing and our uncompressed Fetch batch encoder.

use anyhow::{Context, Result, bail, ensure};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::records::BatchDecodeInfo;

use crate::{MAX_MESSAGE_BYTES, backend::StreamRecord};

const MAX_HEADERS_PER_RECORD: usize = 1_024;

fn varlong(bytes: &mut Bytes, bits: u32) -> Result<i64> {
    let mut raw = 0_u64;
    for shift in (0..bits).step_by(7) {
        ensure!(bytes.has_remaining(), "truncated Kafka varint");
        let byte = bytes.get_u8();
        let payload = u64::from(byte & 0x7f);
        ensure!(
            payload < (1_u64 << (bits - shift).min(7)),
            "Kafka varint overflow"
        );
        raw |= payload << shift;
        if byte & 0x80 == 0 {
            return Ok(i64::try_from(raw >> 1)? ^ -i64::try_from(raw & 1)?);
        }
    }
    bail!("Kafka varint overflow")
}

fn varint(bytes: &mut Bytes) -> Result<i32> {
    i32::try_from(varlong(bytes, 32)?).context("Kafka varint overflow")
}

fn put_varlong(output: &mut BytesMut, value: i64) {
    let mut raw = u64::from_ne_bytes(((value << 1) ^ (value >> 63)).to_ne_bytes());
    while raw >= 0x80 {
        output.put_u8(raw.to_le_bytes()[0] | 0x80);
        raw >>= 7;
    }
    output.put_u8(raw.to_le_bytes()[0]);
}

fn put_varint(output: &mut BytesMut, value: i32) {
    put_varlong(output, i64::from(value));
}

fn take_bytes(bytes: &mut Bytes, length: i32) -> Result<Bytes> {
    let length = usize::try_from(length).context("negative Kafka byte length")?;
    ensure!(length <= bytes.remaining(), "truncated Kafka bytes");
    Ok(bytes.split_to(length))
}

fn nullable_bytes(bytes: &mut Bytes) -> Result<Option<Vec<u8>>> {
    let length = varint(bytes)?;
    if length == -1 {
        return Ok(None);
    }
    take_bytes(bytes, length).map(|value| Some(value.to_vec()))
}

pub(super) fn decode_records(
    mut bytes: Bytes,
    batch: &BatchDecodeInfo,
) -> Result<Vec<StreamRecord>> {
    ensure!(
        (0..=1_000).contains(&batch.record_count),
        "Kafka record count exceeds limit"
    );
    let mut records = Vec::new();
    for _ in 0..batch.record_count {
        let length = varint(&mut bytes)?;
        let mut record = take_bytes(&mut bytes, length)?;
        ensure!(record.has_remaining(), "missing Kafka record attributes");
        let attributes = record.get_i8();
        ensure!(attributes == 0, "unsupported Kafka record attributes");
        let delta = varlong(&mut record, 64)?;
        let timestamp = batch
            .min_timestamp
            .checked_add(delta)
            .context("Kafka timestamp overflow")?;
        let timestamp_ms = u64::try_from(timestamp).context("negative Kafka timestamp")?;
        let offset_delta = varint(&mut record)?;
        ensure!(offset_delta >= 0, "negative Kafka offset delta");
        let offset = batch
            .min_offset
            .checked_add(i64::from(offset_delta))
            .context("Kafka offset overflow")?;
        let offset = u64::try_from(offset).context("negative Kafka offset")?;
        let key = nullable_bytes(&mut record)?;
        let value = nullable_bytes(&mut record)?;
        let count = varint(&mut record)?;
        let count = usize::try_from(count).context("negative Kafka header count")?;
        ensure!(
            count <= MAX_HEADERS_PER_RECORD && count <= record.remaining() / 2,
            "Kafka header count exceeds limit or remaining bytes"
        );
        let mut headers = Vec::with_capacity(count);
        for _ in 0..count {
            let length = varint(&mut record)?;
            let name = take_bytes(&mut record, length)?;
            let name = std::str::from_utf8(&name)
                .context("Kafka header name is not UTF-8")?
                .to_owned();
            headers.push((name, nullable_bytes(&mut record)?));
        }
        ensure!(!record.has_remaining(), "trailing Kafka record bytes");
        records.push(StreamRecord {
            offset,
            timestamp_ms,
            key,
            value,
            headers,
        });
    }
    ensure!(!bytes.has_remaining(), "trailing Kafka batch records");
    Ok(records)
}

fn encode_nullable(output: &mut BytesMut, value: Option<&[u8]>) -> Result<()> {
    match value {
        None => put_varint(output, -1),
        Some(value) => {
            put_varint(output, i32::try_from(value.len())?);
            output.extend_from_slice(value);
        }
    }
    Ok(())
}

/// Emit a single uncompressed magic-2 batch. Offsets/timestamps are real native
/// observations, never synthesized from record position or fetch time.
pub(super) fn encode_records(records: &[StreamRecord]) -> Result<Bytes> {
    let Some(first) = records.first() else {
        return Ok(Bytes::new());
    };
    ensure!(records.len() <= 1_000, "Kafka record count exceeds limit");
    let first_offset = i64::try_from(first.offset)?;
    let first_timestamp = i64::try_from(first.timestamp_ms)?;
    let mut body = BytesMut::new();
    let mut previous = None;
    for record in records {
        let offset = i64::try_from(record.offset)?;
        ensure!(
            previous.is_none_or(|previous| offset > previous),
            "Kafka offsets must increase"
        );
        previous = Some(offset);
        let mut encoded = BytesMut::new();
        encoded.put_i8(0);
        put_varlong(
            &mut encoded,
            i64::try_from(record.timestamp_ms)? - first_timestamp,
        );
        put_varint(&mut encoded, i32::try_from(offset - first_offset)?);
        encode_nullable(&mut encoded, record.key.as_deref())?;
        encode_nullable(&mut encoded, record.value.as_deref())?;
        ensure!(
            record.headers.len() <= MAX_HEADERS_PER_RECORD,
            "Kafka header count exceeds limit"
        );
        put_varint(&mut encoded, i32::try_from(record.headers.len())?);
        for (name, value) in &record.headers {
            put_varint(&mut encoded, i32::try_from(name.len())?);
            encoded.extend_from_slice(name.as_bytes());
            encode_nullable(&mut encoded, value.as_deref())?;
        }
        put_varint(&mut body, i32::try_from(encoded.len())?);
        body.extend_from_slice(&encoded);
        if body.len() > MAX_MESSAGE_BYTES {
            bail!("Kafka fetch body exceeds limit");
        }
    }
    let mut batch = BytesMut::new();
    batch.put_i64(first_offset);
    batch.put_i32(i32::try_from(49 + body.len())?);
    batch.put_i32(0); // partition leader epoch
    batch.put_i8(2); // magic
    batch.put_u32(0); // CRC32C, filled after encoding the covered bytes
    batch.put_i16(0); // no compression; CreateTime
    batch.put_i32(i32::try_from(
        previous.context("empty Kafka batch")? - first_offset,
    )?);
    batch.put_i64(first_timestamp);
    batch.put_i64(i64::try_from(
        records
            .iter()
            .map(|record| record.timestamp_ms)
            .max()
            .unwrap_or(0),
    )?);
    batch.put_i64(-1); // no idempotent producer identity
    batch.put_i16(-1);
    batch.put_i32(-1);
    batch.put_i32(i32::try_from(records.len())?);
    batch.extend_from_slice(&body);
    let checksum = crc32c::crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&checksum.to_be_bytes());
    Ok(batch.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::records::RecordBatchDecoder;

    fn record() -> StreamRecord {
        StreamRecord {
            offset: 7,
            timestamp_ms: 42,
            key: None,
            value: Some(vec![0, 255]),
            headers: vec![
                ("z".into(), Some(vec![1])),
                ("a".into(), None),
                ("z".into(), Some(vec![])),
            ],
        }
    }

    #[test]
    fn preserves_duplicate_header_order_and_checksums_the_wire_batch() {
        let original = record();
        let bytes = encode_records(std::slice::from_ref(&original)).unwrap();
        let metadata = RecordBatchDecoder::decode_batch_info(&mut bytes.clone()).unwrap();
        let recovered = decode_records(bytes.slice(61..), &metadata[0]).unwrap();
        assert_eq!(recovered, vec![original]);
        // Independent decoder validates all other Kafka framing and CRC fields.
        let decoded = RecordBatchDecoder::decode_all(&mut bytes.clone()).unwrap();
        assert_eq!(decoded[0].records[0].offset, 7);
        assert_eq!(decoded[0].records[0].timestamp, 42);
        assert_eq!(
            decoded[0].records[0].value.as_deref(),
            Some([0, 255].as_slice())
        );
    }

    #[test]
    fn rejects_truncation_trailing_bytes_and_forged_counts() {
        let bytes = encode_records(&[record()]).unwrap();
        let mut metadata = RecordBatchDecoder::decode_batch_info(&mut bytes.clone())
            .unwrap()
            .remove(0);
        for end in 61..bytes.len() {
            assert!(decode_records(bytes.slice(61..end), &metadata).is_err());
        }
        let mut trailing = bytes.slice(61..).to_vec();
        trailing.push(0);
        assert!(decode_records(trailing.into(), &metadata).is_err());
        metadata.record_count = i32::MAX;
        assert!(decode_records(bytes.slice(61..), &metadata).is_err());
    }

    #[test]
    fn deterministic_record_mutation_corpus_never_panics_or_accepts_trailing_data() {
        let encoded = encode_records(&[record()]).unwrap();
        let metadata = RecordBatchDecoder::decode_batch_info(&mut encoded.clone())
            .unwrap()
            .remove(0);
        let mut state = 0xd1b5_4a32_d192_ed03_u64;
        for length in 0..=512 {
            let mut input = Vec::with_capacity(length);
            for _ in 0..length {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                input.push(state.to_le_bytes()[0]);
            }
            let _ = decode_records(Bytes::from(input), &metadata);
        }

        let mut valid_with_trailing = encoded.slice(61..).to_vec();
        valid_with_trailing.extend_from_slice(b"trailing");
        assert!(decode_records(valid_with_trailing.into(), &metadata).is_err());
    }

    #[test]
    fn signed_varints_cover_boundaries_and_reject_overflow() {
        for value in [
            i64::MIN,
            i64::MIN + 1,
            -129,
            -128,
            -1,
            0,
            1,
            127,
            128,
            i64::MAX,
        ] {
            let mut encoded = BytesMut::new();
            put_varlong(&mut encoded, value);
            let mut encoded = encoded.freeze();
            assert_eq!(varlong(&mut encoded, 64).unwrap(), value);
            assert!(encoded.is_empty());
        }
        for value in [i32::MIN, -1, 0, 1, i32::MAX] {
            let mut encoded = BytesMut::new();
            put_varint(&mut encoded, value);
            assert_eq!(varint(&mut encoded.freeze()).unwrap(), value);
        }
        assert!(varint(&mut Bytes::from_static(&[0x80; 5])).is_err());
        assert!(varlong(&mut Bytes::from_static(&[0xff; 10]), 64).is_err());
        assert!(varint(&mut Bytes::from_static(&[0x80, 0x80, 0x80, 0x80, 0x10])).is_err());
    }

    #[test]
    fn rejects_header_allocation_bombs_before_reserving_capacity() {
        let bytes = encode_records(&[record()]).unwrap();
        let metadata = RecordBatchDecoder::decode_batch_info(&mut bytes.clone()).unwrap();
        for count in [-1, 1025, i32::MAX] {
            let mut record = BytesMut::from(&[0, 0, 0, 1, 1][..]);
            put_varint(&mut record, count);
            let mut payload = BytesMut::new();
            put_varint(&mut payload, i32::try_from(record.len()).unwrap());
            payload.extend_from_slice(&record);
            assert!(decode_records(payload.freeze(), &metadata[0]).is_err());
        }
    }

    #[test]
    fn timestamps_need_not_increase_but_offsets_must_increase() {
        let first = record();
        let mut second = record();
        second.offset += 1;
        second.timestamp_ms = 1;
        let originals = vec![first, second];
        let bytes = encode_records(&originals).unwrap();
        let metadata = RecordBatchDecoder::decode_batch_info(&mut bytes.clone()).unwrap();
        assert_eq!(
            decode_records(bytes.slice(61..), &metadata[0]).unwrap(),
            originals
        );
        assert!(encode_records(&[record(), record()]).is_err());
    }
}

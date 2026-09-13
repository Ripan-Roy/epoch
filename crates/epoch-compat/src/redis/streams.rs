//! Durable Redis Streams subset backed by Epoch Stream tablets.
//!
//! A Redis stream key resolves to the same-named provisioned Epoch Stream and
//! uses shard zero. Consumer-group last-delivered and pending-entry state is a
//! bounded document in the configured replicated Cache.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{
    BackendError, CacheAtomicMutation, CacheEntry, CacheStorageClass, CacheValue,
    CompatibilityBackend, StreamRecord,
};

use super::{
    RespValue, arity, backend_error, bulk, error, integer, now_ms, positive_u64, text, upper,
};

const RECORD_MAGIC: &[u8; 8] = b"EPRSTRM1";
const MAX_FIELDS: usize = 1_024;
const MAX_PENDING: usize = 4_096;
const MAX_READ_COUNT: u32 = 1_000;
const MAX_BLOCK_MS: u64 = 30_000;
const LEDGER_VERSION: u16 = 1;
const UPDATE_ATTEMPTS: usize = 4;
type StreamFields = Vec<(Vec<u8>, Vec<u8>)>;

pub(super) async fn execute<B: CompatibilityBackend>(
    backend: &Arc<B>,
    cache: &str,
    command: &str,
    arguments: &[Vec<u8>],
) -> RespValue {
    match command {
        "XADD" => xadd(backend, arguments).await,
        "XLEN" => xlen(backend, arguments).await,
        "XRANGE" => xrange(backend, arguments).await,
        "XREAD" => xread(backend, arguments).await,
        "XGROUP" => xgroup(backend, cache, arguments).await,
        "XREADGROUP" => xreadgroup(backend, cache, arguments).await,
        "XACK" => xack(backend, cache, arguments).await,
        "XPENDING" => xpending(backend, cache, arguments).await,
        _ => error("unsupported Redis Streams command"),
    }
}

async fn xadd<B: CompatibilityBackend>(backend: &Arc<B>, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 4 || !args.len().is_multiple_of(2) {
        return arity("xadd");
    }
    let Some(stream) = text(&args[0]).filter(|stream| !stream.is_empty()) else {
        return error("stream key must be non-empty UTF-8");
    };
    if args[1] != b"*" {
        return error("only auto-generated XADD IDs are supported");
    }
    let fields = args[2..]
        .chunks_exact(2)
        .map(|pair| (pair[0].clone(), pair[1].clone()))
        .collect::<Vec<_>>();
    let payload = match encode_fields(&fields) {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let timestamp_ms = now_ms();
    match backend
        .stream_append(
            stream,
            0,
            vec![StreamRecord {
                offset: 0,
                timestamp_ms,
                key: None,
                value: Some(payload),
                headers: vec![("epoch.redis.stream".into(), Some(b"v1".to_vec()))],
            }],
        )
        .await
    {
        Ok(offset) => bulk(&entry_id(timestamp_ms, offset)),
        Err(error) => backend_error(error),
    }
}

async fn xlen<B: CompatibilityBackend>(backend: &Arc<B>, args: &[Vec<u8>]) -> RespValue {
    let [stream] = args else {
        return arity("xlen");
    };
    let Some(stream) = text(stream).filter(|stream| !stream.is_empty()) else {
        return error("stream key must be non-empty UTF-8");
    };
    let (start, end) = tokio::join!(
        backend.stream_start_offset(stream, 0),
        backend.stream_end_offset(stream, 0)
    );
    match (start, end) {
        (Ok(start), Ok(end)) => integer(end.saturating_sub(start)),
        (Err(error), _) | (_, Err(error)) => backend_error(error),
    }
}

async fn xrange<B: CompatibilityBackend>(backend: &Arc<B>, args: &[Vec<u8>]) -> RespValue {
    if !(3..=5).contains(&args.len()) {
        return arity("xrange");
    }
    let Some(stream) = text(&args[0]).filter(|stream| !stream.is_empty()) else {
        return error("stream key must be non-empty UTF-8");
    };
    let Some(start) = range_start(&args[1]) else {
        return error("invalid stream ID");
    };
    let stop = if args[2] == b"+" {
        None
    } else {
        let Some(stop) = parse_id(&args[2]) else {
            return error("invalid stream ID");
        };
        Some(stop)
    };
    let limit = if args.len() == 5 && upper(&args[3]).as_deref() == Some("COUNT") {
        match bounded_count(&args[4]) {
            Some(count) => count,
            None => return error("COUNT must be between 1 and 1000"),
        }
    } else if args.len() == 3 {
        MAX_READ_COUNT
    } else {
        return error("syntax error");
    };
    let records = match backend.stream_fetch(stream, 0, start, limit).await {
        Ok(records) => records,
        Err(error) => return backend_error(error),
    };
    let records = records
        .into_iter()
        .filter(|record| stop.is_none_or(|stop| record.offset <= stop))
        .collect::<Vec<_>>();
    entries_response(&records)
}

async fn xread<B: CompatibilityBackend>(backend: &Arc<B>, args: &[Vec<u8>]) -> RespValue {
    let request = match parse_read(args, false) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let mut starts = Vec::with_capacity(request.streams.len());
    for (stream, cursor) in request.streams.iter().zip(&request.cursors) {
        let start = if cursor == b"$" {
            match backend.stream_end_offset(stream, 0).await {
                Ok(offset) => offset,
                Err(error) => return backend_error(error),
            }
        } else {
            match read_start(cursor) {
                Some(offset) => offset,
                None => return error("invalid stream ID"),
            }
        };
        starts.push(start);
    }
    let deadline = request
        .block_ms
        .map(|duration| tokio::time::Instant::now() + duration);
    loop {
        let mut streams = Vec::new();
        for (stream, start) in request.streams.iter().zip(&starts) {
            match backend.stream_fetch(stream, 0, *start, request.count).await {
                Ok(records) if !records.is_empty() => {
                    streams.push(stream_response(stream, &records));
                }
                Ok(_) => {}
                Err(error) => return backend_error(error),
            }
        }
        if !streams.is_empty() {
            return RespValue::Array(streams);
        }
        let Some(deadline) = deadline else {
            return RespValue::Null;
        };
        if tokio::time::Instant::now() >= deadline {
            return RespValue::Null;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[derive(Debug)]
struct ReadRequest {
    count: u32,
    block_ms: Option<Duration>,
    streams: Vec<String>,
    cursors: Vec<Vec<u8>>,
}

fn parse_read(args: &[Vec<u8>], group: bool) -> Result<ReadRequest, RespValue> {
    let mut index = 0;
    let mut count = MAX_READ_COUNT;
    let mut block_ms = None;
    while index < args.len() && upper(&args[index]).as_deref() != Some("STREAMS") {
        match upper(&args[index]).as_deref() {
            Some("COUNT") if index + 1 < args.len() => {
                count = bounded_count(&args[index + 1])
                    .ok_or_else(|| error("COUNT must be between 1 and 1000"))?;
                index += 2;
            }
            Some("BLOCK") if index + 1 < args.len() => {
                let milliseconds = text(&args[index + 1])
                    .and_then(|value| value.parse::<u64>().ok())
                    .filter(|value| *value <= MAX_BLOCK_MS)
                    .ok_or_else(|| error("BLOCK must be between 0 and 30000 milliseconds"))?;
                block_ms = Some(Duration::from_millis(if milliseconds == 0 {
                    MAX_BLOCK_MS
                } else {
                    milliseconds
                }));
                index += 2;
            }
            Some("NOACK") if group => index += 1,
            _ => return Err(error("syntax error")),
        }
    }
    if args.get(index).and_then(|value| upper(value)).as_deref() != Some("STREAMS") {
        return Err(error("STREAMS is required"));
    }
    let tail = &args[index + 1..];
    if tail.len() < 2 || !tail.len().is_multiple_of(2) {
        return Err(error("STREAMS requires an equal number of keys and IDs"));
    }
    let split = tail.len() / 2;
    let streams = tail[..split]
        .iter()
        .map(|value| {
            text(value)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| error("stream keys must be non-empty UTF-8"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ReadRequest {
        count,
        block_ms,
        streams,
        cursors: tail[split..].to_vec(),
    })
}

fn bounded_count(value: &[u8]) -> Option<u32> {
    positive_u64(value)
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value <= MAX_READ_COUNT)
}

fn entry_id(timestamp_ms: u64, offset: u64) -> String {
    format!("{timestamp_ms}-{offset}")
}

fn read_start(value: &[u8]) -> Option<u64> {
    if value == b"0" || value == b"0-0" {
        return Some(0);
    }
    parse_id(value)?.checked_add(1)
}

fn range_start(value: &[u8]) -> Option<u64> {
    if value == b"-" || value == b"0" || value == b"0-0" {
        Some(0)
    } else {
        parse_id(value)
    }
}

fn parse_id(value: &[u8]) -> Option<u64> {
    let value = text(value)?;
    let (_, sequence) = value.split_once('-')?;
    sequence.parse().ok()
}

fn encode_fields(fields: &[(Vec<u8>, Vec<u8>)]) -> Result<Vec<u8>, RespValue> {
    if fields.is_empty() || fields.len() > MAX_FIELDS {
        return Err(error("stream entry field count is out of range"));
    }
    let mut output = Vec::new();
    output.extend_from_slice(RECORD_MAGIC);
    output.extend_from_slice(
        &u32::try_from(fields.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    for (field, value) in fields {
        for bytes in [field, value] {
            let length =
                u32::try_from(bytes.len()).map_err(|_| error("stream entry field is too large"))?;
            output.extend_from_slice(&length.to_be_bytes());
            output.extend_from_slice(bytes);
        }
    }
    if output.len() > crate::MAX_MESSAGE_BYTES {
        return Err(error("stream entry exceeds message limit"));
    }
    Ok(output)
}

fn decode_fields(value: &[u8]) -> Result<StreamFields, RespValue> {
    if value.len() < 12 || &value[..8] != RECORD_MAGIC {
        return Err(error(
            "entry was not written by the Redis Streams compatibility surface",
        ));
    }
    let count = u32::from_be_bytes(value[8..12].try_into().unwrap_or_default()) as usize;
    if count == 0 || count > MAX_FIELDS {
        return Err(error("stored stream entry field count is invalid"));
    }
    let mut cursor = 12;
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        let field = decode_blob(value, &mut cursor)?;
        let value = decode_blob(value, &mut cursor)?;
        fields.push((field, value));
    }
    if cursor != value.len() {
        return Err(error("stored stream entry has trailing bytes"));
    }
    Ok(fields)
}

fn decode_blob(input: &[u8], cursor: &mut usize) -> Result<Vec<u8>, RespValue> {
    let length_end = cursor
        .checked_add(4)
        .ok_or_else(|| error("stored stream entry overflow"))?;
    let length = input
        .get(*cursor..length_end)
        .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
        .map(u32::from_be_bytes)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| error("stored stream entry is truncated"))?;
    let end = length_end
        .checked_add(length)
        .ok_or_else(|| error("stored stream entry overflow"))?;
    let bytes = input
        .get(length_end..end)
        .ok_or_else(|| error("stored stream entry is truncated"))?
        .to_vec();
    *cursor = end;
    Ok(bytes)
}

fn entries_response(records: &[StreamRecord]) -> RespValue {
    RespValue::Array(records.iter().map(record_response).collect())
}

fn stream_response(stream: &str, records: &[StreamRecord]) -> RespValue {
    RespValue::Array(vec![bulk(stream), entries_response(records)])
}

fn record_response(record: &StreamRecord) -> RespValue {
    let id = entry_id(record.timestamp_ms, record.offset);
    let fields = record.value.as_deref().map_or_else(
        || Err(error("stored stream entry has no value")),
        decode_fields,
    );
    match fields {
        Ok(fields) => RespValue::Array(vec![
            bulk(&id),
            RespValue::Array(
                fields
                    .into_iter()
                    .flat_map(|(field, value)| [RespValue::Bulk(field), RespValue::Bulk(value)])
                    .collect(),
            ),
        ]),
        Err(response) => response,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupLedger {
    version: u16,
    next_offset: u64,
    pending: BTreeMap<String, PendingEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingEntry {
    consumer: String,
    delivery_count: u64,
    delivered_at_ms: u64,
    offset: u64,
    timestamp_ms: u64,
}

impl GroupLedger {
    fn new(next_offset: u64) -> Self {
        Self {
            version: LEDGER_VERSION,
            next_offset,
            pending: BTreeMap::new(),
        }
    }
}

async fn xgroup<B: CompatibilityBackend>(
    backend: &Arc<B>,
    cache: &str,
    args: &[Vec<u8>],
) -> RespValue {
    let Some(action) = args.first().and_then(|value| upper(value)) else {
        return arity("xgroup");
    };
    match action.as_str() {
        "CREATE" => xgroup_create(backend, cache, &args[1..]).await,
        "DESTROY" => xgroup_destroy(backend, cache, &args[1..]).await,
        "SETID" => xgroup_set_id(backend, cache, &args[1..]).await,
        _ => error("unsupported XGROUP subcommand"),
    }
}

async fn xgroup_create<B: CompatibilityBackend>(
    backend: &Arc<B>,
    cache: &str,
    args: &[Vec<u8>],
) -> RespValue {
    if !(3..=4).contains(&args.len())
        || args
            .get(3)
            .is_some_and(|value| upper(value).as_deref() != Some("MKSTREAM"))
    {
        return arity("xgroup create");
    }
    let (Some(stream), Some(group)) = (resource(&args[0]), resource(&args[1])) else {
        return error("stream key and group must be non-empty UTF-8");
    };
    // Epoch resources are provisioned out of band. Even a zero cursor must
    // prove that the same-named Stream exists; MKSTREAM cannot bypass Catalog.
    let end_offset = match backend.stream_end_offset(stream, 0).await {
        Ok(offset) => offset,
        Err(BackendError::NotFound) => return error("stream key does not exist"),
        Err(error) => return backend_error(error),
    };
    let next_offset = if args[2] == b"$" {
        end_offset
    } else {
        match read_start(&args[2]) {
            Some(offset) => offset,
            None => return error("invalid stream ID"),
        }
    };
    let key = ledger_key(stream, group);
    for attempt in 0..UPDATE_ATTEMPTS {
        let (revision, existing) = match read_ledger(backend, cache, &key).await {
            Ok(value) => value,
            Err(error) => return backend_error(error),
        };
        if existing.is_some() {
            return RespValue::Error("BUSYGROUP Consumer Group name already exists".into());
        }
        match write_ledger(
            backend,
            cache,
            &key,
            revision,
            Some(&GroupLedger::new(next_offset)),
        )
        .await
        {
            Ok(()) => return RespValue::Simple("OK".into()),
            Err(BackendError::Conflict) if attempt + 1 < UPDATE_ATTEMPTS => {}
            Err(error) => return backend_error(error),
        }
    }
    backend_error(BackendError::Conflict)
}

async fn xgroup_destroy<B: CompatibilityBackend>(
    backend: &Arc<B>,
    cache: &str,
    args: &[Vec<u8>],
) -> RespValue {
    let [stream, group] = args else {
        return arity("xgroup destroy");
    };
    let (Some(stream), Some(group)) = (resource(stream), resource(group)) else {
        return error("stream key and group must be non-empty UTF-8");
    };
    let key = ledger_key(stream, group);
    for attempt in 0..UPDATE_ATTEMPTS {
        let (revision, existing) = match read_ledger(backend, cache, &key).await {
            Ok(value) => value,
            Err(error) => return backend_error(error),
        };
        if existing.is_none() {
            return RespValue::Integer(0);
        }
        match write_ledger(backend, cache, &key, revision, None).await {
            Ok(()) => return RespValue::Integer(1),
            Err(BackendError::Conflict) if attempt + 1 < UPDATE_ATTEMPTS => {}
            Err(error) => return backend_error(error),
        }
    }
    backend_error(BackendError::Conflict)
}

async fn xgroup_set_id<B: CompatibilityBackend>(
    backend: &Arc<B>,
    cache: &str,
    args: &[Vec<u8>],
) -> RespValue {
    let [stream, group, id] = args else {
        return arity("xgroup setid");
    };
    let (Some(stream), Some(group)) = (resource(stream), resource(group)) else {
        return error("stream key and group must be non-empty UTF-8");
    };
    let next_offset = if id == b"$" {
        match backend.stream_end_offset(stream, 0).await {
            Ok(offset) => offset,
            Err(error) => return backend_error(error),
        }
    } else {
        match read_start(id) {
            Some(offset) => offset,
            None => return error("invalid stream ID"),
        }
    };
    let key = ledger_key(stream, group);
    for attempt in 0..UPDATE_ATTEMPTS {
        let (revision, Some(mut ledger)) = (match read_ledger(backend, cache, &key).await {
            Ok(value) => value,
            Err(error) => return backend_error(error),
        }) else {
            return no_group(stream, group);
        };
        ledger.next_offset = next_offset;
        match write_ledger(backend, cache, &key, revision, Some(&ledger)).await {
            Ok(()) => return RespValue::Simple("OK".into()),
            Err(BackendError::Conflict) if attempt + 1 < UPDATE_ATTEMPTS => {}
            Err(error) => return backend_error(error),
        }
    }
    backend_error(BackendError::Conflict)
}

async fn xreadgroup<B: CompatibilityBackend>(
    backend: &Arc<B>,
    cache: &str,
    args: &[Vec<u8>],
) -> RespValue {
    if args.len() < 5 || upper(&args[0]).as_deref() != Some("GROUP") {
        return arity("xreadgroup");
    }
    let (Some(group), Some(consumer)) = (resource(&args[1]), resource(&args[2])) else {
        return error("group and consumer must be non-empty UTF-8");
    };
    let no_ack = args[3..]
        .iter()
        .any(|value| upper(value).as_deref() == Some("NOACK"));
    let request = match parse_read(&args[3..], true) {
        Ok(request) => request,
        Err(response) => return response,
    };
    if request.streams.len() != 1 || request.cursors[0] != b">" {
        return error("XREADGROUP supports one stream and the '>' new-entry cursor");
    }
    let stream = &request.streams[0];
    let key = ledger_key(stream, group);
    let deadline = request
        .block_ms
        .map(|duration| tokio::time::Instant::now() + duration);
    let mut conflicts = 0;
    loop {
        let (revision, Some(mut ledger)) = (match read_ledger(backend, cache, &key).await {
            Ok(value) => value,
            Err(error) => return backend_error(error),
        }) else {
            return no_group(stream, group);
        };
        let records = match backend
            .stream_fetch(stream, 0, ledger.next_offset, request.count)
            .await
        {
            Ok(records) => records,
            Err(error) => return backend_error(error),
        };
        if records.is_empty() {
            let Some(deadline) = deadline else {
                return RespValue::Null;
            };
            if tokio::time::Instant::now() >= deadline {
                return RespValue::Null;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        }
        if !no_ack && ledger.pending.len().saturating_add(records.len()) > MAX_PENDING {
            return error("consumer-group pending-entry limit exceeded");
        }
        ledger.next_offset = records
            .last()
            .map_or(ledger.next_offset, |record| record.offset.saturating_add(1));
        if !no_ack {
            let delivered_at_ms = now_ms();
            for record in &records {
                ledger.pending.insert(
                    entry_id(record.timestamp_ms, record.offset),
                    PendingEntry {
                        consumer: consumer.to_owned(),
                        delivery_count: 1,
                        delivered_at_ms,
                        offset: record.offset,
                        timestamp_ms: record.timestamp_ms,
                    },
                );
            }
        }
        match write_ledger(backend, cache, &key, revision, Some(&ledger)).await {
            Ok(()) => return RespValue::Array(vec![stream_response(stream, &records)]),
            Err(BackendError::Conflict) if conflicts + 1 < UPDATE_ATTEMPTS => {
                conflicts += 1;
            }
            Err(error) => return backend_error(error),
        }
    }
}

async fn xack<B: CompatibilityBackend>(
    backend: &Arc<B>,
    cache: &str,
    args: &[Vec<u8>],
) -> RespValue {
    if args.len() < 3 {
        return arity("xack");
    }
    let (Some(stream), Some(group)) = (resource(&args[0]), resource(&args[1])) else {
        return error("stream key and group must be non-empty UTF-8");
    };
    let ids = args[2..]
        .iter()
        .map(|value| text(value).map(str::to_owned))
        .collect::<Option<Vec<_>>>();
    let Some(ids) = ids else {
        return error("stream IDs must be UTF-8");
    };
    let key = ledger_key(stream, group);
    for attempt in 0..UPDATE_ATTEMPTS {
        let (revision, Some(mut ledger)) = (match read_ledger(backend, cache, &key).await {
            Ok(value) => value,
            Err(error) => return backend_error(error),
        }) else {
            return no_group(stream, group);
        };
        let acknowledged = ids
            .iter()
            .filter(|id| ledger.pending.remove(*id).is_some())
            .count();
        if acknowledged == 0 {
            return RespValue::Integer(0);
        }
        match write_ledger(backend, cache, &key, revision, Some(&ledger)).await {
            Ok(()) => return integer(acknowledged.try_into().unwrap_or(u64::MAX)),
            Err(BackendError::Conflict) if attempt + 1 < UPDATE_ATTEMPTS => {}
            Err(error) => return backend_error(error),
        }
    }
    backend_error(BackendError::Conflict)
}

async fn xpending<B: CompatibilityBackend>(
    backend: &Arc<B>,
    cache: &str,
    args: &[Vec<u8>],
) -> RespValue {
    if args.len() != 2 && !(5..=6).contains(&args.len()) {
        return arity("xpending");
    }
    let (Some(stream), Some(group)) = (resource(&args[0]), resource(&args[1])) else {
        return error("stream key and group must be non-empty UTF-8");
    };
    let key = ledger_key(stream, group);
    let (_, Some(ledger)) = (match read_ledger(backend, cache, &key).await {
        Ok(value) => value,
        Err(error) => return backend_error(error),
    }) else {
        return no_group(stream, group);
    };
    if args.len() == 2 {
        let mut consumers = BTreeMap::<&str, u64>::new();
        for pending in ledger.pending.values() {
            *consumers.entry(&pending.consumer).or_default() += 1;
        }
        let minimum = ledger
            .pending
            .iter()
            .min_by_key(|(_, pending)| pending.offset)
            .map_or(RespValue::Null, |(id, _)| bulk(id));
        let maximum = ledger
            .pending
            .iter()
            .max_by_key(|(_, pending)| pending.offset)
            .map_or(RespValue::Null, |(id, _)| bulk(id));
        return RespValue::Array(vec![
            integer(ledger.pending.len().try_into().unwrap_or(u64::MAX)),
            minimum,
            maximum,
            RespValue::Array(
                consumers
                    .into_iter()
                    .map(|(consumer, count)| RespValue::Array(vec![bulk(consumer), integer(count)]))
                    .collect(),
            ),
        ]);
    }
    let count = match bounded_count(&args[4]) {
        Some(count) => usize::try_from(count).unwrap_or(usize::MAX),
        None => return error("COUNT must be between 1 and 1000"),
    };
    let start = if args[2] == b"-" {
        0
    } else {
        match parse_pending_boundary(&args[2]) {
            Some((offset, false)) => offset,
            Some((offset, true)) => offset.saturating_add(1),
            None => return error("invalid start stream ID"),
        }
    };
    let end = if args[3] == b"+" {
        u64::MAX
    } else {
        match parse_pending_boundary(&args[3]) {
            Some((offset, false)) => offset,
            Some((offset, true)) => offset.saturating_sub(1),
            None => return error("invalid end stream ID"),
        }
    };
    if start > end {
        return RespValue::Array(Vec::new());
    }
    let consumer = match args.get(5) {
        Some(value) => match resource(value) {
            Some(value) => Some(value),
            None => return error("consumer must be non-empty UTF-8"),
        },
        None => None,
    };
    let now = now_ms();
    let mut pending = ledger.pending.iter().collect::<Vec<_>>();
    pending.sort_by_key(|(_, pending)| pending.offset);
    RespValue::Array(
        pending
            .into_iter()
            .filter(|(_, pending)| {
                (start..=end).contains(&pending.offset)
                    && consumer.is_none_or(|consumer| pending.consumer == consumer)
            })
            .take(count)
            .map(|(id, pending)| {
                RespValue::Array(vec![
                    bulk(id),
                    bulk(&pending.consumer),
                    integer(now.saturating_sub(pending.delivered_at_ms)),
                    integer(pending.delivery_count),
                ])
            })
            .collect(),
    )
}

fn parse_pending_boundary(value: &[u8]) -> Option<(u64, bool)> {
    let (value, exclusive) = value
        .strip_prefix(b"(")
        .map_or((value, false), |value| (value, true));
    Some((parse_id(value)?, exclusive))
}

fn resource(value: &[u8]) -> Option<&str> {
    text(value).filter(|value| !value.is_empty() && value.len() <= 256)
}

fn no_group(stream: &str, group: &str) -> RespValue {
    RespValue::Error(format!(
        "NOGROUP No such consumer group '{group}' for stream '{stream}'"
    ))
}

fn ledger_key(stream: &str, group: &str) -> String {
    use sha2::{Digest as _, Sha256};
    let digest = Sha256::digest(format!("{stream}\0{group}").as_bytes());
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    format!("__epoch:redis-stream-group:{encoded}")
}

async fn read_ledger<B: CompatibilityBackend>(
    backend: &Arc<B>,
    cache: &str,
    key: &str,
) -> Result<(u64, Option<GroupLedger>), BackendError> {
    let snapshot = backend.cache_snapshot(cache, &[key.to_owned()]).await?;
    let ledger = snapshot
        .entries
        .get(key)
        .and_then(Option::as_ref)
        .map(decode_ledger)
        .transpose()?;
    Ok((snapshot.revision, ledger))
}

fn decode_ledger(entry: &CacheEntry) -> Result<GroupLedger, BackendError> {
    let bytes = match &entry.value {
        CacheValue::Blob(value) => value.as_slice(),
        CacheValue::String(value) => value.as_bytes(),
        _ => return Err(BackendError::WrongType),
    };
    let ledger: GroupLedger = serde_json::from_slice(bytes)
        .map_err(|_| BackendError::Invalid("Redis Streams group ledger is corrupt".into()))?;
    if ledger.version != LEDGER_VERSION || ledger.pending.len() > MAX_PENDING {
        return Err(BackendError::Invalid(
            "Redis Streams group ledger version or bounds are invalid".into(),
        ));
    }
    Ok(ledger)
}

async fn write_ledger<B: CompatibilityBackend>(
    backend: &Arc<B>,
    cache: &str,
    key: &str,
    revision: u64,
    ledger: Option<&GroupLedger>,
) -> Result<(), BackendError> {
    let mutation = match ledger {
        Some(ledger) => CacheAtomicMutation::Put {
            key: key.to_owned(),
            value: CacheValue::Blob(
                serde_json::to_vec(ledger)
                    .map_err(|_| BackendError::Invalid("Redis Streams ledger is invalid".into()))?,
            ),
            ttl_ms: None,
            storage_class: CacheStorageClass::Memory,
        },
        None => CacheAtomicMutation::Delete {
            key: key.to_owned(),
        },
    };
    backend
        .cache_compare_and_apply(cache, revision, &[mutation])
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_field_codec_is_binary_safe_and_rejects_malformed_storage() {
        let fields = vec![
            (b"a\0b".to_vec(), vec![0, 255]),
            (Vec::new(), b"value".to_vec()),
        ];
        let encoded = encode_fields(&fields).unwrap();
        assert_eq!(decode_fields(&encoded).unwrap(), fields);

        for malformed in [
            Vec::new(),
            RECORD_MAGIC.to_vec(),
            [RECORD_MAGIC.as_slice(), &0_u32.to_be_bytes()].concat(),
            encoded[..encoded.len() - 1].to_vec(),
            [encoded.as_slice(), b"trailing"].concat(),
        ] {
            assert!(decode_fields(&malformed).is_err());
        }
    }

    #[test]
    fn pending_boundaries_preserve_inclusive_and_exclusive_offsets() {
        assert_eq!(parse_pending_boundary(b"123-9"), Some((9, false)));
        assert_eq!(parse_pending_boundary(b"(123-9"), Some((9, true)));
        assert_eq!(parse_pending_boundary(b"invalid"), None);
    }
}

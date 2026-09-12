use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    Action, AuthenticationMethod, Decision, DecisionEvent, DecisionEventFields, DecisionReason,
    Principal, ResourceScope, encode_lower_hex,
};

const FORMAT_VERSION: u32 = 1;
const MAX_RECORD_BYTES: usize = 16 << 10;
const MAX_PAGE_SIZE: usize = 1_000;
const DIGEST_BYTES: usize = 32;
const ZERO_DIGEST: [u8; DIGEST_BYTES] = [0; DIGEST_BYTES];

/// Stable credential-free representation stored in the durable journal.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuditEventDocument {
    pub event_time_unix_ms: String,
    pub request_id: String,
    pub principal_id: String,
    pub policy_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authentication_method: Option<AuthenticationMethod>,
    pub action: Action,
    pub decision: Decision,
    pub reason: DecisionReason,
    pub scope: ResourceScope,
}

/// One independently verifiable immutable journal link.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuditJournalRecord {
    pub format_version: u32,
    pub sequence: String,
    pub previous_sha256: String,
    pub event: AuditEventDocument,
    pub record_sha256: String,
}

/// Bounded export page. Decimal sequence values are browser safe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditPage {
    pub records: Vec<AuditJournalRecord>,
    pub next_sequence: String,
    pub end_of_journal: bool,
}

/// Durable audit journal failure. Errors never include record contents.
#[derive(Debug, Error)]
pub enum AuditJournalError {
    #[error("audit journal path is invalid: {0}")]
    InvalidPath(String),
    #[error("audit journal I/O failed: {0}")]
    Io(String),
    #[error("audit journal integrity verification failed: {0}")]
    Integrity(String),
    #[error("audit event is invalid: {0}")]
    InvalidEvent(String),
    #[error("audit page size must be between 1 and {MAX_PAGE_SIZE}")]
    InvalidPageSize,
    #[error("audit journal is unavailable after a previous durable write failure")]
    Failed,
}

struct AuditJournalState {
    file: File,
    last_sequence: u64,
    last_digest: [u8; DIGEST_BYTES],
    failed: bool,
}

/// Owner-only, fsynced, hash-chained audit journal.
pub struct AuditJournal {
    path: PathBuf,
    state: Mutex<AuditJournalState>,
}

impl std::fmt::Debug for AuditJournal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuditJournal")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl AuditJournal {
    /// Opens a regular, owner-only file and verifies every existing link before
    /// accepting another event.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, AuditJournalError> {
        let path = path.as_ref();
        if path.as_os_str().is_empty() {
            return Err(AuditJournalError::InvalidPath("path is required".into()));
        }
        match fs::symlink_metadata(path) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_file() {
                    return Err(AuditJournalError::InvalidPath(
                        "path must be a regular file, not a symbolic link".into(),
                    ));
                }
                #[cfg(unix)]
                if metadata.permissions().mode() & 0o077 != 0 {
                    return Err(AuditJournalError::InvalidPath(
                        "permissions must not grant group or other access".into(),
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(AuditJournalError::Io(error.to_string())),
        }
        let parent = path
            .parent()
            .filter(|candidate| !candidate.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|error| AuditJournalError::Io(error.to_string()))?;
        let mut options = OpenOptions::new();
        options.create(true).read(true).append(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options
            .open(path)
            .map_err(|error| AuditJournalError::Io(error.to_string()))?;
        let verified = read_and_verify(&mut file, 0, 1, None)?;
        Ok(Self {
            path: path.to_owned(),
            state: Mutex::new(AuditJournalState {
                file,
                last_sequence: verified.last_sequence,
                last_digest: verified.last_digest,
                failed: false,
            }),
        })
    }

    /// Appends and synchronizes a decision before returning success.
    pub fn record(&self, event: &DecisionEvent) -> Result<(), AuditJournalError> {
        let mut state = self.state.lock().map_err(|_| AuditJournalError::Failed)?;
        if state.failed {
            return Err(AuditJournalError::Failed);
        }
        let sequence = state
            .last_sequence
            .checked_add(1)
            .ok_or_else(|| AuditJournalError::Integrity("sequence exhausted".into()))?;
        let event_time_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AuditJournalError::InvalidEvent("system time precedes Unix epoch".into()))?
            .as_millis();
        let event_time_unix_ms = u64::try_from(event_time_unix_ms)
            .map_err(|_| AuditJournalError::InvalidEvent("system time exceeds uint64".into()))?;
        let document = event_document(event, event_time_unix_ms);
        validate_event_document(&document)?;
        let digest = record_digest(sequence, &state.last_digest, &document)?;
        let record = AuditJournalRecord {
            format_version: FORMAT_VERSION,
            sequence: sequence.to_string(),
            previous_sha256: encode_lower_hex(&state.last_digest),
            event: document,
            record_sha256: encode_lower_hex(&digest),
        };
        let mut encoded = serde_json::to_vec(&record)
            .map_err(|error| AuditJournalError::InvalidEvent(error.to_string()))?;
        if encoded.len() > MAX_RECORD_BYTES {
            return Err(AuditJournalError::InvalidEvent(format!(
                "record exceeds {MAX_RECORD_BYTES} bytes"
            )));
        }
        encoded.push(b'\n');
        if state.file.write_all(&encoded).is_err() || state.file.sync_data().is_err() {
            state.failed = true;
            return Err(AuditJournalError::Failed);
        }
        state.last_sequence = sequence;
        state.last_digest = digest;
        Ok(())
    }

    /// Revalidates the whole chain and returns only records visible through the
    /// caller's audit grant and tenant scope.
    pub fn read_page(
        &self,
        after_sequence: u64,
        limit: usize,
        principal: &Principal,
    ) -> Result<AuditPage, AuditJournalError> {
        if !(1..=MAX_PAGE_SIZE).contains(&limit) {
            return Err(AuditJournalError::InvalidPageSize);
        }
        let mut state = self.state.lock().map_err(|_| AuditJournalError::Failed)?;
        if state.failed {
            return Err(AuditJournalError::Failed);
        }
        let verified =
            match read_and_verify(&mut state.file, after_sequence, limit, Some(principal)) {
                Ok(verified) => verified,
                Err(error) => {
                    state.failed = true;
                    return Err(error);
                }
            };
        if verified.last_sequence != state.last_sequence
            || verified.last_digest != state.last_digest
        {
            state.failed = true;
            return Err(AuditJournalError::Integrity(
                "file changed outside its owning process".into(),
            ));
        }
        Ok(AuditPage {
            records: verified.records,
            next_sequence: verified.next_sequence.to_string(),
            end_of_journal: verified.next_sequence == state.last_sequence,
        })
    }

    /// Flushes all accepted records to stable storage.
    pub fn sync(&self) -> Result<(), AuditJournalError> {
        self.state
            .lock()
            .map_err(|_| AuditJournalError::Failed)?
            .file
            .sync_all()
            .map_err(|error| AuditJournalError::Io(error.to_string()))
    }
}

struct VerifiedJournal {
    records: Vec<AuditJournalRecord>,
    next_sequence: u64,
    last_sequence: u64,
    last_digest: [u8; DIGEST_BYTES],
}

fn read_and_verify(
    file: &mut File,
    after_sequence: u64,
    limit: usize,
    principal: Option<&Principal>,
) -> Result<VerifiedJournal, AuditJournalError> {
    let length = file
        .metadata()
        .map_err(|error| AuditJournalError::Io(error.to_string()))?
        .len();
    if length > 0 {
        file.seek(SeekFrom::End(-1))
            .map_err(|error| AuditJournalError::Io(error.to_string()))?;
        let mut last = [0_u8; 1];
        file.read_exact(&mut last)
            .map_err(|error| AuditJournalError::Io(error.to_string()))?;
        if last[0] != b'\n' {
            return Err(AuditJournalError::Integrity(
                "file ends with a partial record".into(),
            ));
        }
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|error| AuditJournalError::Io(error.to_string()))?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut previous = ZERO_DIGEST;
    let mut sequence = 0_u64;
    let mut next_sequence = after_sequence;
    let mut records = Vec::with_capacity(limit);
    loop {
        line.clear();
        let read = reader
            .read_until(b'\n', &mut line)
            .map_err(|error| AuditJournalError::Io(error.to_string()))?;
        if read == 0 {
            break;
        }
        if line.len() > MAX_RECORD_BYTES + 1 || line.pop() != Some(b'\n') {
            return Err(AuditJournalError::Integrity(
                "record is oversized or partial".into(),
            ));
        }
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| AuditJournalError::Integrity("sequence exhausted".into()))?;
        let (record, digest) = decode_and_verify_record(&line, sequence, &previous)?;
        previous = digest;
        if sequence > after_sequence
            && records.len() < limit
            && principal
                .is_none_or(|principal| principal.allows(Action::AuditRead, &record.event.scope))
        {
            records.push(record);
            next_sequence = sequence;
        }
    }
    if records.len() < limit {
        next_sequence = sequence;
    }
    Ok(VerifiedJournal {
        records,
        next_sequence,
        last_sequence: sequence,
        last_digest: previous,
    })
}

fn decode_and_verify_record(
    encoded: &[u8],
    expected_sequence: u64,
    expected_previous: &[u8; DIGEST_BYTES],
) -> Result<(AuditJournalRecord, [u8; DIGEST_BYTES]), AuditJournalError> {
    let record: AuditJournalRecord = serde_json::from_slice(encoded)
        .map_err(|error| AuditJournalError::Integrity(error.to_string()))?;
    let canonical = serde_json::to_vec(&record)
        .map_err(|error| AuditJournalError::Integrity(error.to_string()))?;
    if canonical != encoded {
        return Err(AuditJournalError::Integrity(
            "record is not canonical JSON".into(),
        ));
    }
    let sequence = record
        .sequence
        .parse::<u64>()
        .map_err(|_| AuditJournalError::Integrity("record sequence is not a uint64".into()))?;
    if sequence.to_string() != record.sequence || sequence != expected_sequence {
        return Err(AuditJournalError::Integrity(
            "record sequence is not canonical or contiguous".into(),
        ));
    }
    if record.format_version != FORMAT_VERSION
        || record.previous_sha256 != encode_lower_hex(expected_previous)
    {
        return Err(AuditJournalError::Integrity(
            "record predecessor is invalid".into(),
        ));
    }
    validate_event_document(&record.event)?;
    let digest = record_digest(sequence, expected_previous, &record.event)?;
    if record.record_sha256 != encode_lower_hex(&digest) {
        return Err(AuditJournalError::Integrity(
            "record digest is invalid".into(),
        ));
    }
    Ok((record, digest))
}

fn event_document(event: &DecisionEvent, event_time_unix_ms: u64) -> AuditEventDocument {
    AuditEventDocument {
        event_time_unix_ms: event_time_unix_ms.to_string(),
        request_id: event.request_id().to_owned(),
        principal_id: event.principal_id().to_owned(),
        policy_id: event.policy_id().to_owned(),
        authentication_method: event.authentication_method(),
        action: event.action(),
        decision: event.decision(),
        reason: event.reason(),
        scope: event.scope().clone(),
    }
}

fn validate_event_document(event: &AuditEventDocument) -> Result<(), AuditJournalError> {
    let timestamp = event
        .event_time_unix_ms
        .parse::<u64>()
        .map_err(|_| AuditJournalError::InvalidEvent("event time is not a uint64".into()))?;
    if timestamp == 0
        || timestamp > i64::MAX as u64
        || timestamp.to_string() != event.event_time_unix_ms
    {
        return Err(AuditJournalError::InvalidEvent(
            "event time is not a canonical positive Unix millisecond".into(),
        ));
    }
    DecisionEvent::new(DecisionEventFields {
        request_id: event.request_id.clone(),
        principal_id: event.principal_id.clone(),
        policy_id: event.policy_id.clone(),
        authentication_method: event.authentication_method,
        action: event.action,
        decision: event.decision,
        reason: event.reason,
        scope: event.scope.clone(),
    })
    .map_err(|error| AuditJournalError::InvalidEvent(error.to_string()))?;
    Ok(())
}

fn record_digest(
    sequence: u64,
    previous: &[u8; DIGEST_BYTES],
    event: &AuditEventDocument,
) -> Result<[u8; DIGEST_BYTES], AuditJournalError> {
    let encoded = serde_json::to_vec(event)
        .map_err(|error| AuditJournalError::InvalidEvent(error.to_string()))?;
    let length = u64::try_from(encoded.len())
        .map_err(|_| AuditJournalError::InvalidEvent("event is oversized".into()))?;
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/audit-journal/v1\0");
    hasher.update(sequence.to_be_bytes());
    hasher.update(previous);
    hasher.update(length.to_be_bytes());
    hasher.update(encoded);
    Ok(hasher.finalize().into())
}

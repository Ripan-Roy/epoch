//! Explicitly versioned, checksummed append-only storage.
//!
//! The format here is the Phase-0 local WAL, not a frozen production format.
//! It deliberately avoids Rust-specific object serialization so recovery and
//! migrations can be implemented in other versions and languages.

use std::{
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crc32fast::Hasher;
use epoch_core::{DurabilityProfile, EpochError, EpochResult};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;

const MAGIC: [u8; 4] = *b"EPCH";
const FORMAT_VERSION: u16 = 1;
const HEADER_LEN: usize = 32;
const MAX_PAYLOAD_LEN: usize = 16 * 1024 * 1024;
const SEGMENT_PREFIX: &str = "segment-";
const SEGMENT_SUFFIX: &str = ".wal";
const WRITER_LOCK_FILE: &str = ".writer.lock";
const IDENTITY_FILE: &str = "identity.v1";
const IDENTITY_TEMP_FILE: &str = ".identity.v1.tmp";
const MANIFEST_FILE: &str = "manifest.v1";
const MANIFEST_TEMP_FILE: &str = ".manifest.v1.tmp";
const ACTIVATION_TEMP_FILE: &str = ".engine.wal.activation.tmp";
const METADATA_HEADER_LEN: usize = 16;
const MAX_METADATA_PAYLOAD_LEN: usize = 16 * 1024 * 1024;
const IDENTITY_MAGIC: [u8; 4] = *b"EPID";
const MANIFEST_MAGIC: [u8; 4] = *b"EPMF";
const METADATA_FORMAT_VERSION: u16 = 1;
const STAGING_ACTIVATION_MARKER: &[u8; 32] = b"EPOCH-SEGMENTED-WAL-STAGING-V1!!";
const ACTIVE_ACTIVATION_MARKER: &[u8; 32] = b"EPOCH-SEGMENTED-WAL-ACTIVE-V1!!!";

/// Smallest useful segment target: one encoded frame header.
pub const MIN_WAL_SEGMENT_BYTES: u64 = 32;

/// Default maximum encoded bytes targeted for each standalone WAL segment.
///
/// A single record larger than this target is accepted in an otherwise empty
/// segment, up to [`MAX_PAYLOAD_LEN`], so records are never split across files.
pub const DEFAULT_WAL_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRecord {
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub payload: Vec<u8>,
}

pub trait CommitLog: std::fmt::Debug + Send {
    /// Strongest acknowledgement this implementation can truthfully provide.
    fn durability(&self) -> DurabilityProfile;

    fn append(
        &mut self,
        timestamp_ms: u64,
        payload: &[u8],
        durable: bool,
    ) -> EpochResult<LogRecord>;
    fn records_from(&self, sequence: u64, limit: usize) -> Vec<LogRecord>;
    fn last_sequence(&self) -> Option<u64>;
}

#[derive(Debug, Default)]
pub struct MemoryLog {
    records: Vec<LogRecord>,
}

impl CommitLog for MemoryLog {
    fn durability(&self) -> DurabilityProfile {
        DurabilityProfile::Volatile
    }

    fn append(
        &mut self,
        timestamp_ms: u64,
        payload: &[u8],
        _durable: bool,
    ) -> EpochResult<LogRecord> {
        let sequence = u64::try_from(self.records.len())
            .map_err(|error| EpochError::Capacity(error.to_string()))?;
        let record = LogRecord {
            sequence,
            timestamp_ms,
            payload: payload.to_vec(),
        };
        self.records.push(record.clone());
        Ok(record)
    }

    fn records_from(&self, sequence: u64, limit: usize) -> Vec<LogRecord> {
        self.records
            .iter()
            .filter(|record| record.sequence >= sequence)
            .take(limit)
            .cloned()
            .collect()
    }

    fn last_sequence(&self) -> Option<u64> {
        self.records.last().map(|record| record.sequence)
    }
}

#[derive(Debug)]
pub struct FileWal {
    path: PathBuf,
    file: File,
    first_sequence: u64,
    records: Vec<LogRecord>,
    valid_len: u64,
    next_sequence: Option<u64>,
    content_hasher: Hasher,
    recovered_partial_tail: bool,
    poisoned: Option<String>,
}

#[derive(Debug, Clone)]
struct FileWalCheckpoint {
    valid_len: u64,
    next_sequence: Option<u64>,
    records_len: usize,
    content_hasher: Hasher,
}

#[derive(Debug)]
struct RecoveredFile {
    records: Vec<LogRecord>,
    valid_len: u64,
    recovered_partial_tail: bool,
    next_sequence: Option<u64>,
    content_hasher: Hasher,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalOpenMode {
    CreateIfMissing,
    ExistingOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum IdentityState {
    Staging,
    Active,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WalIdentity {
    format_version: u16,
    wal_id: Uuid,
    state: IdentityState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestSegment {
    first_sequence: u64,
    encoded_len: u64,
    last_sequence: Option<u64>,
    content_checksum: u32,
}

impl ManifestSegment {
    fn from_wal(wal: &FileWal) -> Self {
        Self {
            first_sequence: wal.first_sequence,
            encoded_len: wal.encoded_len(),
            last_sequence: wal.last_sequence(),
            content_checksum: wal.content_checksum(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WalManifest {
    format_version: u16,
    wal_id: Uuid,
    segments: Vec<ManifestSegment>,
    pending_segment: Option<u64>,
}

impl FileWal {
    pub fn open(path: impl AsRef<Path>) -> EpochResult<Self> {
        Self::open_segment(path.as_ref(), 0, true, WalOpenMode::CreateIfMissing)
    }

    fn open_segment(
        path: &Path,
        first_sequence: u64,
        allow_partial_tail_repair: bool,
        open_mode: WalOpenMode,
    ) -> EpochResult<Self> {
        let path = path.to_path_buf();
        let existed = path.exists();
        let file = OpenOptions::new()
            .create(open_mode == WalOpenMode::CreateIfMissing)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(storage_error)?;
        file.try_lock().map_err(|error| {
            EpochError::Storage(format!(
                "WAL {} is already owned by another process: {error}",
                path.display()
            ))
        })?;
        if !existed {
            file.sync_all().map_err(storage_error)?;
            sync_parent_directory(&path)?;
        }
        Self::from_locked_file(path, file, first_sequence, allow_partial_tail_repair)
    }

    fn from_locked_file(
        path: PathBuf,
        mut file: File,
        first_sequence: u64,
        allow_partial_tail_repair: bool,
    ) -> EpochResult<Self> {
        let recovered = recover(&mut file, first_sequence)?;
        if recovered.recovered_partial_tail {
            if !allow_partial_tail_repair {
                return Err(EpochError::Storage(format!(
                    "sealed WAL segment {} has an incomplete tail",
                    path.display()
                )));
            }
            file.set_len(recovered.valid_len).map_err(storage_error)?;
            file.sync_data().map_err(storage_error)?;
        }
        file.seek(SeekFrom::End(0)).map_err(storage_error)?;
        Ok(Self {
            path,
            file,
            first_sequence,
            records: recovered.records,
            valid_len: recovered.valid_len,
            next_sequence: recovered.next_sequence,
            content_hasher: recovered.content_hasher,
            recovered_partial_tail: recovered.recovered_partial_tail,
            poisoned: None,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub const fn recovered_partial_tail(&self) -> bool {
        self.recovered_partial_tail
    }

    pub fn sync(&self) -> EpochResult<()> {
        self.ensure_available()?;
        self.file.sync_data().map_err(storage_error)
    }

    fn ensure_available(&self) -> EpochResult<()> {
        if let Some(reason) = &self.poisoned {
            return Err(EpochError::Storage(format!(
                "WAL is unavailable after an append rollback failure: {reason}"
            )));
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    const fn encoded_len(&self) -> u64 {
        self.valid_len
    }

    const fn next_sequence(&self) -> Option<u64> {
        self.next_sequence
    }

    fn content_checksum(&self) -> u32 {
        self.content_hasher.clone().finalize()
    }

    fn checkpoint(&self) -> FileWalCheckpoint {
        FileWalCheckpoint {
            valid_len: self.valid_len,
            next_sequence: self.next_sequence,
            records_len: self.records.len(),
            content_hasher: self.content_hasher.clone(),
        }
    }

    fn rollback_to(&mut self, checkpoint: FileWalCheckpoint) -> EpochResult<()> {
        let rollback_result = self
            .file
            .set_len(checkpoint.valid_len)
            .and_then(|()| {
                self.file
                    .seek(SeekFrom::Start(checkpoint.valid_len))
                    .map(|_| ())
            })
            .and_then(|()| self.file.sync_data());
        if let Err(error) = rollback_result {
            let reason = format!(
                "WAL rollback to byte {} failed: {error}",
                checkpoint.valid_len
            );
            self.poisoned = Some(reason.clone());
            return Err(EpochError::Storage(reason));
        }
        self.valid_len = checkpoint.valid_len;
        self.next_sequence = checkpoint.next_sequence;
        self.records.truncate(checkpoint.records_len);
        self.content_hasher = checkpoint.content_hasher;
        Ok(())
    }
}

/// A single-writer, checksummed WAL split into bounded standalone files.
///
/// A durable identity and manifest bind the ordered segment set, committed
/// lengths, sequence high-water marks, and content checksums. Missing or
/// swapped files therefore fail closed instead of silently reusing positions.
#[derive(Debug)]
struct SegmentedWal {
    directory: PathBuf,
    _writer_lock: File,
    segments: Vec<FileWal>,
    manifest: WalManifest,
    max_segment_bytes: u64,
    recovered_partial_tail: bool,
    poisoned: Option<String>,
}

impl SegmentedWal {
    pub fn open(directory: impl AsRef<Path>, max_segment_bytes: u64) -> EpochResult<Self> {
        if max_segment_bytes < MIN_WAL_SEGMENT_BYTES {
            return Err(EpochError::InvalidArgument(format!(
                "WAL segment target must be at least {HEADER_LEN} bytes"
            )));
        }

        let directory = directory.as_ref();
        create_wal_directory(directory)?;
        let writer_lock = lock_wal_directory(directory)?;
        let discovered_segments = discover_segments(directory)?;
        let identity_path = directory.join(IDENTITY_FILE);
        let manifest_path = directory.join(MANIFEST_FILE);
        let identity_exists = identity_path.try_exists().map_err(storage_error)?;
        let manifest_exists = manifest_path.try_exists().map_err(storage_error)?;
        let mut identity = if identity_exists {
            read_identity(directory)?
        } else {
            if manifest_exists || !discovered_segments.is_empty() {
                return Err(EpochError::Storage(
                    "segmented WAL has data but no durable identity".into(),
                ));
            }
            let identity = WalIdentity {
                format_version: METADATA_FORMAT_VERSION,
                wal_id: Uuid::now_v7(),
                state: IdentityState::Staging,
            };
            write_identity(directory, &identity)?;
            identity
        };
        validate_identity(&identity)?;

        let (manifest, segments, recovered_partial_tail) = if manifest_exists {
            let mut manifest = read_manifest(directory)?;
            validate_manifest_identity(&manifest, &identity)?;
            recover_pending_rotation(directory, &mut manifest)?;
            let discovered_segments = discover_segments(directory)?;
            let (segments, recovered_partial_tail) =
                open_manifest_segments(&manifest, &discovered_segments)?;
            (manifest, segments, recovered_partial_tail)
        } else {
            if identity.state == IdentityState::Active {
                return Err(EpochError::Storage(
                    "active segmented WAL is missing its manifest".into(),
                ));
            }
            initialize_manifest(directory, &identity, &discovered_segments)?
        };

        if identity.state == IdentityState::Staging {
            identity.state = IdentityState::Active;
            write_identity(directory, &identity)?;
        }

        Ok(Self {
            directory: directory.to_path_buf(),
            _writer_lock: writer_lock,
            segments,
            manifest,
            max_segment_bytes,
            recovered_partial_tail,
            poisoned: None,
        })
    }

    pub const fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub const fn recovered_partial_tail(&self) -> bool {
        self.recovered_partial_tail
    }

    fn rotate(&mut self, first_sequence: u64) -> EpochResult<()> {
        self.segments
            .last()
            .ok_or_else(|| EpochError::Storage("segmented WAL has no active file".into()))?
            .sync()?;

        let mut pending_manifest = self.manifest.clone();
        pending_manifest.pending_segment = Some(first_sequence);
        if let Err(error) = write_manifest(&self.directory, &pending_manifest) {
            return Err(self.poison(format!("could not persist pending WAL rotation: {error}")));
        }
        self.manifest = pending_manifest;

        let path = self.directory.join(segment_file_name(first_sequence));
        let segment =
            match FileWal::open_segment(&path, first_sequence, false, WalOpenMode::CreateIfMissing)
            {
                Ok(segment) if segment.is_empty() => segment,
                Ok(_) => {
                    return Err(self.poison(format!(
                        "pending WAL segment {} is not empty",
                        path.display()
                    )));
                }
                Err(error) => {
                    return Err(self.poison(format!(
                        "could not create pending WAL segment {}: {error}",
                        path.display()
                    )));
                }
            };

        let mut committed_manifest = self.manifest.clone();
        committed_manifest
            .segments
            .push(ManifestSegment::from_wal(&segment));
        committed_manifest.pending_segment = None;
        if let Err(error) = write_manifest(&self.directory, &committed_manifest) {
            self.segments.push(segment);
            return Err(self.poison(format!("could not activate pending WAL segment: {error}")));
        }
        self.segments.push(segment);
        self.manifest = committed_manifest;
        Ok(())
    }

    fn poison(&mut self, reason: String) -> EpochError {
        self.poisoned = Some(reason.clone());
        EpochError::Storage(reason)
    }
}

impl CommitLog for SegmentedWal {
    fn durability(&self) -> DurabilityProfile {
        DurabilityProfile::LocalDurable
    }

    fn append(
        &mut self,
        timestamp_ms: u64,
        payload: &[u8],
        _durable: bool,
    ) -> EpochResult<LogRecord> {
        if let Some(reason) = &self.poisoned {
            return Err(EpochError::Storage(format!(
                "segmented WAL is unavailable after a metadata failure: {reason}"
            )));
        }
        if let Some(reason) = self
            .segments
            .last()
            .and_then(|active| active.poisoned.as_deref())
        {
            return Err(EpochError::Storage(format!(
                "segmented WAL is unavailable after its active segment was poisoned: {reason}"
            )));
        }
        let frame_len = encoded_frame_len(payload)?;
        let active = self
            .segments
            .last()
            .ok_or_else(|| EpochError::Storage("segmented WAL has no active file".into()))?;
        let should_rotate = !active.is_empty()
            && active
                .encoded_len()
                .checked_add(frame_len)
                .is_none_or(|projected| projected > self.max_segment_bytes);
        if should_rotate {
            let first_sequence = active.next_sequence().ok_or_else(|| {
                EpochError::Capacity("WAL sequence space has been exhausted".into())
            })?;
            self.rotate(first_sequence)?;
        }
        let active = self
            .segments
            .last_mut()
            .ok_or_else(|| EpochError::Storage("segmented WAL has no active file".into()))?;
        let checkpoint = active.checkpoint();
        let record = active.append(timestamp_ms, payload, true)?;
        let mut next_manifest = self.manifest.clone();
        let manifest_segment = next_manifest.segments.last_mut().ok_or_else(|| {
            EpochError::Storage("segmented WAL manifest has no active segment".into())
        })?;
        *manifest_segment = ManifestSegment::from_wal(active);
        if let Err(error) = write_manifest(&self.directory, &next_manifest) {
            return self.reconcile_failed_manifest_append(checkpoint, next_manifest, error);
        }
        self.manifest = next_manifest;
        Ok(record)
    }

    fn records_from(&self, sequence: u64, limit: usize) -> Vec<LogRecord> {
        let mut records = Vec::new();
        for segment in &self.segments {
            let remaining = limit.saturating_sub(records.len());
            if remaining == 0 {
                break;
            }
            records.extend(segment.records_from(sequence, remaining));
        }
        records
    }

    fn last_sequence(&self) -> Option<u64> {
        self.segments
            .iter()
            .rev()
            .find_map(CommitLog::last_sequence)
    }
}

impl SegmentedWal {
    fn reconcile_failed_manifest_append(
        &mut self,
        checkpoint: FileWalCheckpoint,
        next_manifest: WalManifest,
        write_error: EpochError,
    ) -> EpochResult<LogRecord> {
        match read_manifest(&self.directory) {
            Ok(on_disk) if on_disk == next_manifest => {
                if let Err(sync_error) = sync_parent_directory(&self.directory.join(MANIFEST_FILE))
                {
                    return Err(self.poison(format!(
                        "WAL append outcome is unknown after manifest sync failed: {write_error}; retry failed: {sync_error}"
                    )));
                }
                self.manifest = next_manifest;
                self.segments
                    .last()
                    .and_then(|segment| segment.records.last())
                    .cloned()
                    .ok_or_else(|| {
                        self.poison(
                            "manifest committed but active WAL record is unavailable".into(),
                        )
                    })
            }
            Ok(on_disk) if on_disk == self.manifest => {
                let active = self.segments.last_mut().ok_or_else(|| {
                    EpochError::Storage("segmented WAL has no active file".into())
                })?;
                active.rollback_to(checkpoint)?;
                Err(write_error)
            }
            Ok(_) => Err(self.poison(format!(
                "WAL append outcome is unknown after manifest diverged: {write_error}"
            ))),
            Err(read_error) => Err(self.poison(format!(
                "WAL append outcome is unknown after manifest update failed: {write_error}; manifest reread failed: {read_error}"
            ))),
        }
    }
}

/// Crash-safe standalone layout selector used by the runnable node.
///
/// New or empty data directories receive an invalid-to-old-readers activation
/// marker and use [`SegmentedWal`]. A pre-existing valid `engine.wal` remains
/// on the legacy single-file writer until an explicit migration exists, which
/// keeps both concurrent old/new startup and an offline downgrade fail-safe.
#[derive(Debug)]
pub struct StandaloneWal {
    layout: StandaloneWalLayout,
}

#[derive(Debug)]
enum StandaloneWalLayout {
    Legacy(FileWal),
    Segmented {
        _activation_lock: File,
        wal: SegmentedWal,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActivationState {
    Staging,
    Active,
    Legacy,
}

impl StandaloneWal {
    pub fn open(data_directory: impl AsRef<Path>, max_segment_bytes: u64) -> EpochResult<Self> {
        if max_segment_bytes < MIN_WAL_SEGMENT_BYTES {
            return Err(EpochError::InvalidArgument(format!(
                "WAL segment target must be at least {HEADER_LEN} bytes"
            )));
        }
        let data_directory = data_directory.as_ref();
        create_wal_directory(data_directory)?;
        let (legacy_path, mut activation_file) = lock_activation_file(data_directory)?;
        match classify_standalone_layout(data_directory, &legacy_path, &mut activation_file)? {
            ActivationState::Staging => Self::open_segmented(
                data_directory,
                max_segment_bytes,
                activation_file,
                ActivationState::Staging,
            ),
            ActivationState::Active => Self::open_segmented(
                data_directory,
                max_segment_bytes,
                activation_file,
                ActivationState::Active,
            ),
            ActivationState::Legacy => {
                if segmented_layout_has_state(&data_directory.join("engine-wal"))? {
                    return Err(EpochError::Storage(
                        "legacy and segmented WAL histories coexist without a valid activation marker"
                            .into(),
                    ));
                }
                let legacy = FileWal::from_locked_file(legacy_path, activation_file, 0, true)?;
                Ok(Self {
                    layout: StandaloneWalLayout::Legacy(legacy),
                })
            }
        }
    }

    fn open_segmented(
        data_directory: &Path,
        max_segment_bytes: u64,
        activation_lock: File,
        activation_state: ActivationState,
    ) -> EpochResult<Self> {
        let wal = SegmentedWal::open(data_directory.join("engine-wal"), max_segment_bytes)?;
        let activation_lock = if activation_state == ActivationState::Staging {
            activate_segmented_layout(data_directory, activation_lock)?
        } else {
            activation_lock
        };
        Ok(Self {
            layout: StandaloneWalLayout::Segmented {
                _activation_lock: activation_lock,
                wal,
            },
        })
    }

    pub const fn uses_legacy_layout(&self) -> bool {
        matches!(self.layout, StandaloneWalLayout::Legacy(_))
    }

    pub fn segment_count(&self) -> usize {
        match &self.layout {
            StandaloneWalLayout::Legacy(_) => 0,
            StandaloneWalLayout::Segmented { wal, .. } => wal.segment_count(),
        }
    }

    pub fn recovered_partial_tail(&self) -> bool {
        match &self.layout {
            StandaloneWalLayout::Legacy(wal) => wal.recovered_partial_tail(),
            StandaloneWalLayout::Segmented { wal, .. } => wal.recovered_partial_tail(),
        }
    }
}

fn lock_activation_file(data_directory: &Path) -> EpochResult<(PathBuf, File)> {
    let path = data_directory.join("engine.wal");
    let existed = path.try_exists().map_err(storage_error)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(storage_error)?;
    file.try_lock().map_err(|error| {
        EpochError::Storage(format!(
            "standalone WAL {} is already owned by another process: {error}",
            path.display()
        ))
    })?;
    if !existed {
        file.sync_all().map_err(storage_error)?;
        sync_parent_directory(&path)?;
    }
    Ok((path, file))
}

fn classify_standalone_layout(
    data_directory: &Path,
    legacy_path: &Path,
    activation_file: &mut File,
) -> EpochResult<ActivationState> {
    let segmented_directory = data_directory.join("engine-wal");
    let file_len = activation_file.metadata().map_err(storage_error)?.len();
    if file_len == 0 {
        if segmented_layout_has_state(&segmented_directory)? {
            return Err(EpochError::Storage(
                "empty legacy WAL exists beside an activated segmented layout".into(),
            ));
        }
        write_staging_activation_marker(legacy_path, activation_file)?;
        return Ok(ActivationState::Staging);
    }
    if file_len != u64::try_from(STAGING_ACTIVATION_MARKER.len()).unwrap_or(u64::MAX) {
        return Ok(ActivationState::Legacy);
    }

    let mut marker = [0_u8; STAGING_ACTIVATION_MARKER.len()];
    activation_file
        .seek(SeekFrom::Start(0))
        .map_err(storage_error)?;
    activation_file
        .read_exact(&mut marker)
        .map_err(storage_error)?;
    if marker == *ACTIVE_ACTIVATION_MARKER {
        if !segmented_layout_has_state(&segmented_directory)? {
            return Err(EpochError::Storage(
                "active segmented WAL marker exists but its data directory is missing".into(),
            ));
        }
        return Ok(ActivationState::Active);
    }
    if marker == *STAGING_ACTIVATION_MARKER {
        return Ok(ActivationState::Staging);
    }
    if is_staging_activation_marker(&marker) && !segmented_layout_has_state(&segmented_directory)? {
        write_staging_activation_marker(legacy_path, activation_file)?;
        return Ok(ActivationState::Staging);
    }
    Ok(ActivationState::Legacy)
}

fn write_staging_activation_marker(path: &Path, file: &mut File) -> EpochResult<()> {
    let marker_len = u64::try_from(STAGING_ACTIVATION_MARKER.len())
        .map_err(|error| EpochError::Capacity(format!("activation marker overflow: {error}")))?;
    file.set_len(marker_len).map_err(storage_error)?;
    file.sync_data().map_err(storage_error)?;
    file.seek(SeekFrom::Start(0)).map_err(storage_error)?;
    file.write_all(STAGING_ACTIVATION_MARKER)
        .map_err(storage_error)?;
    file.sync_data().map_err(storage_error)?;
    sync_parent_directory(path)
}

impl CommitLog for StandaloneWal {
    fn durability(&self) -> DurabilityProfile {
        DurabilityProfile::LocalDurable
    }

    fn append(
        &mut self,
        timestamp_ms: u64,
        payload: &[u8],
        durable: bool,
    ) -> EpochResult<LogRecord> {
        match &mut self.layout {
            StandaloneWalLayout::Legacy(wal) => wal.append(timestamp_ms, payload, durable),
            StandaloneWalLayout::Segmented { wal, .. } => {
                wal.append(timestamp_ms, payload, durable)
            }
        }
    }

    fn records_from(&self, sequence: u64, limit: usize) -> Vec<LogRecord> {
        match &self.layout {
            StandaloneWalLayout::Legacy(wal) => wal.records_from(sequence, limit),
            StandaloneWalLayout::Segmented { wal, .. } => wal.records_from(sequence, limit),
        }
    }

    fn last_sequence(&self) -> Option<u64> {
        match &self.layout {
            StandaloneWalLayout::Legacy(wal) => wal.last_sequence(),
            StandaloneWalLayout::Segmented { wal, .. } => wal.last_sequence(),
        }
    }
}

fn is_staging_activation_marker(bytes: &[u8; STAGING_ACTIVATION_MARKER.len()]) -> bool {
    let mut saw_zero = false;
    for (actual, expected) in bytes.iter().zip(STAGING_ACTIVATION_MARKER) {
        if *actual == 0 {
            saw_zero = true;
        } else if saw_zero || actual != expected {
            return false;
        }
    }
    saw_zero
}

fn activate_segmented_layout(data_directory: &Path, staging_lock: File) -> EpochResult<File> {
    let temporary_path = data_directory.join(ACTIVATION_TEMP_FILE);
    let active_path = data_directory.join("engine.wal");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary_path)
        .map_err(storage_error)?;
    file.write_all(ACTIVE_ACTIVATION_MARKER)
        .map_err(storage_error)?;
    file.sync_all().map_err(storage_error)?;
    fs::rename(&temporary_path, &active_path).map_err(storage_error)?;
    sync_parent_directory(&active_path)?;

    let active_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&active_path)
        .map_err(storage_error)?;
    active_lock.try_lock().map_err(|error| {
        EpochError::Storage(format!(
            "activated standalone WAL {} could not be locked: {error}",
            active_path.display()
        ))
    })?;
    drop(staging_lock);
    Ok(active_lock)
}

fn segmented_layout_has_state(directory: &Path) -> EpochResult<bool> {
    if !directory.try_exists().map_err(storage_error)? {
        return Ok(false);
    }
    for entry in fs::read_dir(directory).map_err(storage_error)? {
        let entry = entry.map_err(storage_error)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with(SEGMENT_PREFIX)
            || matches!(
                name,
                IDENTITY_FILE | IDENTITY_TEMP_FILE | MANIFEST_FILE | MANIFEST_TEMP_FILE
            )
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn validate_identity(identity: &WalIdentity) -> EpochResult<()> {
    if identity.format_version != METADATA_FORMAT_VERSION {
        return Err(EpochError::Storage(format!(
            "WAL identity uses unsupported format {}",
            identity.format_version
        )));
    }
    Ok(())
}

fn validate_manifest_identity(manifest: &WalManifest, identity: &WalIdentity) -> EpochResult<()> {
    if manifest.format_version != METADATA_FORMAT_VERSION {
        return Err(EpochError::Storage(format!(
            "WAL manifest uses unsupported format {}",
            manifest.format_version
        )));
    }
    if manifest.wal_id != identity.wal_id {
        return Err(EpochError::Storage(
            "WAL identity and manifest IDs do not match".into(),
        ));
    }
    if manifest.segments.is_empty() {
        return Err(EpochError::Storage(
            "WAL manifest contains no active segment".into(),
        ));
    }
    Ok(())
}

fn initialize_manifest(
    directory: &Path,
    identity: &WalIdentity,
    discovered_segments: &[(u64, PathBuf)],
) -> EpochResult<(WalManifest, Vec<FileWal>, bool)> {
    let segment = match discovered_segments {
        [] => FileWal::open_segment(
            &directory.join(segment_file_name(0)),
            0,
            false,
            WalOpenMode::CreateIfMissing,
        )?,
        [(0, path)] if fs::metadata(path).map_err(storage_error)?.len() == 0 => {
            FileWal::open_segment(path, 0, false, WalOpenMode::ExistingOnly)?
        }
        _ => {
            return Err(EpochError::Storage(
                "staging WAL has ambiguous data without a manifest".into(),
            ));
        }
    };
    let manifest = WalManifest {
        format_version: METADATA_FORMAT_VERSION,
        wal_id: identity.wal_id,
        segments: vec![ManifestSegment::from_wal(&segment)],
        pending_segment: None,
    };
    write_manifest(directory, &manifest)?;
    Ok((manifest, vec![segment], false))
}

fn recover_pending_rotation(directory: &Path, manifest: &mut WalManifest) -> EpochResult<()> {
    let Some(pending_sequence) = manifest.pending_segment else {
        return Ok(());
    };
    let expected_sequence = manifest_next_sequence(manifest).ok_or_else(|| {
        EpochError::Storage("pending WAL segment follows exhausted sequence u64::MAX".into())
    })?;
    if pending_sequence != expected_sequence {
        return Err(EpochError::Storage(format!(
            "pending WAL segment starts at {pending_sequence}; expected {expected_sequence}"
        )));
    }

    let discovered = discover_segments(directory)?;
    for segment in &manifest.segments {
        if !discovered
            .iter()
            .any(|(first_sequence, _)| *first_sequence == segment.first_sequence)
        {
            return Err(EpochError::Storage(format!(
                "WAL manifest segment {} is missing during rotation recovery",
                segment_file_name(segment.first_sequence)
            )));
        }
    }
    for (first_sequence, path) in &discovered {
        let is_committed = manifest
            .segments
            .iter()
            .any(|segment| segment.first_sequence == *first_sequence);
        if !is_committed && *first_sequence != pending_sequence {
            return Err(EpochError::Storage(format!(
                "untracked WAL segment {} exists during rotation recovery",
                path.display()
            )));
        }
    }

    let pending_path = directory.join(segment_file_name(pending_sequence));
    let segment = if pending_path.try_exists().map_err(storage_error)? {
        if fs::metadata(&pending_path).map_err(storage_error)?.len() != 0 {
            return Err(EpochError::Storage(format!(
                "pending WAL segment {} contains uncommitted data",
                pending_path.display()
            )));
        }
        FileWal::open_segment(
            &pending_path,
            pending_sequence,
            false,
            WalOpenMode::ExistingOnly,
        )?
    } else {
        FileWal::open_segment(
            &pending_path,
            pending_sequence,
            false,
            WalOpenMode::CreateIfMissing,
        )?
    };
    manifest.segments.push(ManifestSegment::from_wal(&segment));
    manifest.pending_segment = None;
    write_manifest(directory, manifest)
}

fn manifest_next_sequence(manifest: &WalManifest) -> Option<u64> {
    let last = manifest.segments.last()?;
    last.last_sequence
        .map_or(Some(last.first_sequence), |sequence| {
            sequence.checked_add(1)
        })
}

fn open_manifest_segments(
    manifest: &WalManifest,
    discovered_segments: &[(u64, PathBuf)],
) -> EpochResult<(Vec<FileWal>, bool)> {
    if manifest.pending_segment.is_some() {
        return Err(EpochError::Storage(
            "WAL manifest still has a pending rotation".into(),
        ));
    }
    if discovered_segments.len() != manifest.segments.len() {
        return Err(EpochError::Storage(format!(
            "WAL topology has {} segment files; manifest requires {}",
            discovered_segments.len(),
            manifest.segments.len()
        )));
    }

    let mut segments = Vec::with_capacity(manifest.segments.len());
    let mut expected_sequence = Some(0_u64);
    let mut recovered_tail = false;
    for (index, (manifest_segment, (discovered_sequence, path))) in manifest
        .segments
        .iter()
        .zip(discovered_segments)
        .enumerate()
    {
        let expected = expected_sequence.ok_or_else(|| {
            EpochError::Storage(format!(
                "WAL segment {} follows exhausted sequence u64::MAX",
                path.display()
            ))
        })?;
        if manifest_segment.first_sequence != expected || *discovered_sequence != expected {
            return Err(EpochError::Storage(format!(
                "WAL segment {} starts at {discovered_sequence}; expected {expected}",
                path.display()
            )));
        }
        let is_active = index + 1 == manifest.segments.len();
        let (segment, repaired_tail) = open_manifest_segment(path, manifest_segment, is_active)?;
        if !is_active && segment.is_empty() {
            return Err(EpochError::Storage(format!(
                "sealed WAL segment {} is empty",
                path.display()
            )));
        }
        expected_sequence = segment.next_sequence();
        recovered_tail |= repaired_tail;
        segments.push(segment);
    }
    Ok((segments, recovered_tail))
}

fn open_manifest_segment(
    path: &Path,
    expected: &ManifestSegment,
    is_active: bool,
) -> EpochResult<(FileWal, bool)> {
    let file = OpenOptions::new()
        .create(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(storage_error)?;
    file.try_lock().map_err(|error| {
        EpochError::Storage(format!(
            "WAL segment {} is already owned by another process: {error}",
            path.display()
        ))
    })?;
    let actual_len = file.metadata().map_err(storage_error)?.len();
    if actual_len < expected.encoded_len {
        return Err(EpochError::Storage(format!(
            "WAL segment {} is truncated to {actual_len} bytes; manifest requires {}",
            path.display(),
            expected.encoded_len
        )));
    }
    let repaired_tail = actual_len > expected.encoded_len;
    if repaired_tail {
        if !is_active {
            return Err(EpochError::Storage(format!(
                "sealed WAL segment {} has bytes beyond its committed manifest length",
                path.display()
            )));
        }
        file.set_len(expected.encoded_len).map_err(storage_error)?;
        file.sync_data().map_err(storage_error)?;
    }
    let wal = FileWal::from_locked_file(path.to_path_buf(), file, expected.first_sequence, false)?;
    if wal.encoded_len() != expected.encoded_len
        || wal.last_sequence() != expected.last_sequence
        || wal.content_checksum() != expected.content_checksum
    {
        return Err(EpochError::Storage(format!(
            "WAL segment {} does not match its committed manifest entry",
            path.display()
        )));
    }
    Ok((wal, repaired_tail))
}

fn create_wal_directory(directory: &Path) -> EpochResult<()> {
    let directory = if directory.as_os_str().is_empty() {
        Path::new(".")
    } else {
        directory
    };
    let mut missing = Vec::new();
    let mut current = directory.to_path_buf();
    loop {
        match fs::metadata(&current) {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => {
                return Err(EpochError::Storage(format!(
                    "WAL directory path {} is not a directory",
                    current.display()
                )));
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                missing.push(current.clone());
                current = parent_directory(&current)
                    .ok_or_else(|| {
                        EpochError::Storage(format!(
                            "WAL directory {} has no creatable parent",
                            directory.display()
                        ))
                    })?
                    .to_path_buf();
            }
            Err(error) => return Err(storage_error(error)),
        }
    }

    for path in missing.iter().rev() {
        match fs::create_dir(path) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                if !fs::metadata(path).map_err(storage_error)?.is_dir() {
                    return Err(EpochError::Storage(format!(
                        "WAL directory path {} is not a directory",
                        path.display()
                    )));
                }
            }
            Err(error) => return Err(storage_error(error)),
        }
        sync_parent_directory(path)?;
    }
    Ok(())
}

fn parent_directory(path: &Path) -> Option<&Path> {
    path.parent().map(|parent| {
        if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        }
    })
}

fn lock_wal_directory(directory: &Path) -> EpochResult<File> {
    let path = directory.join(WRITER_LOCK_FILE);
    let existed = path.exists();
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(storage_error)?;
    file.try_lock().map_err(|error| {
        EpochError::Storage(format!(
            "WAL directory {} is already owned by another process: {error}",
            directory.display()
        ))
    })?;
    if !existed {
        file.sync_all().map_err(storage_error)?;
        sync_parent_directory(&path)?;
    }
    Ok(file)
}

fn discover_segments(directory: &Path) -> EpochResult<Vec<(u64, PathBuf)>> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(directory).map_err(storage_error)? {
        let entry = entry.map_err(storage_error)?;
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        if !file_name.starts_with(SEGMENT_PREFIX) {
            continue;
        }
        let first_sequence = parse_segment_file_name(file_name).ok_or_else(|| {
            EpochError::Storage(format!(
                "malformed WAL segment name {}",
                entry.path().display()
            ))
        })?;
        if !entry.file_type().map_err(storage_error)?.is_file() {
            return Err(EpochError::Storage(format!(
                "WAL segment {} is not a regular file",
                entry.path().display()
            )));
        }
        segments.push((first_sequence, entry.path()));
    }
    segments.sort_unstable_by_key(|(first_sequence, _)| *first_sequence);
    Ok(segments)
}

fn parse_segment_file_name(file_name: &str) -> Option<u64> {
    let digits = file_name
        .strip_prefix(SEGMENT_PREFIX)?
        .strip_suffix(SEGMENT_SUFFIX)?;
    if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let sequence = digits.parse::<u64>().ok()?;
    (segment_file_name(sequence) == file_name).then_some(sequence)
}

fn segment_file_name(first_sequence: u64) -> String {
    format!("{SEGMENT_PREFIX}{first_sequence:020}{SEGMENT_SUFFIX}")
}

fn read_metadata<T: DeserializeOwned>(path: &Path, expected_magic: [u8; 4]) -> EpochResult<T> {
    let file_len = fs::metadata(path).map_err(storage_error)?.len();
    let maximum_len = u64::try_from(METADATA_HEADER_LEN + MAX_METADATA_PAYLOAD_LEN)
        .map_err(|error| EpochError::Capacity(error.to_string()))?;
    if file_len > maximum_len {
        return Err(EpochError::Storage(format!(
            "metadata file {} exceeds {maximum_len} bytes",
            path.display()
        )));
    }
    let bytes = fs::read(path).map_err(storage_error)?;
    if bytes.len() < METADATA_HEADER_LEN {
        return Err(EpochError::Storage(format!(
            "metadata file {} has an incomplete header",
            path.display()
        )));
    }
    if bytes[0..4] != expected_magic {
        return Err(EpochError::Storage(format!(
            "metadata file {} has invalid magic",
            path.display()
        )));
    }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != METADATA_FORMAT_VERSION {
        return Err(EpochError::Storage(format!(
            "metadata file {} uses unsupported version {version}",
            path.display()
        )));
    }
    let flags = u16::from_le_bytes([bytes[6], bytes[7]]);
    if flags != 0 {
        return Err(EpochError::Storage(format!(
            "metadata file {} uses unsupported flags {flags:#06x}",
            path.display()
        )));
    }
    let payload_len =
        u32::from_le_bytes(bytes[8..12].try_into().expect("fixed metadata header")) as usize;
    if payload_len > MAX_METADATA_PAYLOAD_LEN {
        return Err(EpochError::Storage(format!(
            "metadata file {} declares an oversized payload",
            path.display()
        )));
    }
    let expected_len = METADATA_HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| EpochError::Capacity("metadata length overflow".into()))?;
    if bytes.len() != expected_len {
        return Err(EpochError::Storage(format!(
            "metadata file {} length is {}; expected {expected_len}",
            path.display(),
            bytes.len()
        )));
    }
    let expected_checksum =
        u32::from_le_bytes(bytes[12..16].try_into().expect("fixed metadata header"));
    let payload = &bytes[METADATA_HEADER_LEN..];
    let actual_checksum = checksum(&bytes[4..12], payload);
    if actual_checksum != expected_checksum {
        return Err(EpochError::Storage(format!(
            "metadata file {} checksum mismatch",
            path.display()
        )));
    }
    serde_json::from_slice(payload).map_err(|error| {
        EpochError::Storage(format!(
            "metadata file {} could not be decoded: {error}",
            path.display()
        ))
    })
}

fn write_metadata<T: Serialize>(
    directory: &Path,
    file_name: &str,
    temporary_file_name: &str,
    magic: [u8; 4],
    value: &T,
) -> EpochResult<()> {
    let payload = serde_json::to_vec(value)
        .map_err(|error| EpochError::Internal(format!("metadata encoding failed: {error}")))?;
    if payload.len() > MAX_METADATA_PAYLOAD_LEN {
        return Err(EpochError::Capacity(format!(
            "metadata payload exceeds {MAX_METADATA_PAYLOAD_LEN} bytes"
        )));
    }
    let payload_len =
        u32::try_from(payload.len()).map_err(|error| EpochError::Capacity(error.to_string()))?;
    let mut header = [0_u8; METADATA_HEADER_LEN];
    header[0..4].copy_from_slice(&magic);
    header[4..6].copy_from_slice(&METADATA_FORMAT_VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&0_u16.to_le_bytes());
    header[8..12].copy_from_slice(&payload_len.to_le_bytes());
    let checksum = checksum(&header[4..12], &payload);
    header[12..16].copy_from_slice(&checksum.to_le_bytes());

    let temporary_path = directory.join(temporary_file_name);
    let final_path = directory.join(file_name);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary_path)
        .map_err(storage_error)?;
    file.write_all(&header).map_err(storage_error)?;
    file.write_all(&payload).map_err(storage_error)?;
    file.sync_all().map_err(storage_error)?;
    fs::rename(&temporary_path, &final_path).map_err(storage_error)?;
    sync_parent_directory(&final_path)
}

fn read_identity(directory: &Path) -> EpochResult<WalIdentity> {
    read_metadata(&directory.join(IDENTITY_FILE), IDENTITY_MAGIC)
}

fn write_identity(directory: &Path, identity: &WalIdentity) -> EpochResult<()> {
    write_metadata(
        directory,
        IDENTITY_FILE,
        IDENTITY_TEMP_FILE,
        IDENTITY_MAGIC,
        identity,
    )
}

fn read_manifest(directory: &Path) -> EpochResult<WalManifest> {
    read_metadata(&directory.join(MANIFEST_FILE), MANIFEST_MAGIC)
}

fn write_manifest(directory: &Path, manifest: &WalManifest) -> EpochResult<()> {
    write_metadata(
        directory,
        MANIFEST_FILE,
        MANIFEST_TEMP_FILE,
        MANIFEST_MAGIC,
        manifest,
    )
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> EpochResult<()> {
    if let Some(parent) = parent_directory(path) {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(storage_error)?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> EpochResult<()> {
    Ok(())
}

impl CommitLog for FileWal {
    fn durability(&self) -> DurabilityProfile {
        DurabilityProfile::LocalDurable
    }

    fn append(
        &mut self,
        timestamp_ms: u64,
        payload: &[u8],
        durable: bool,
    ) -> EpochResult<LogRecord> {
        self.ensure_available()?;
        let sequence = self
            .next_sequence
            .ok_or_else(|| EpochError::Capacity("WAL sequence space has been exhausted".into()))?;
        let frame = encode_frame(sequence, timestamp_ms, payload)?;
        let frame_len = u64::try_from(frame.len()).map_err(|error| {
            EpochError::Capacity(format!("WAL frame length cannot be represented: {error}"))
        })?;
        let next_valid_len = self
            .valid_len
            .checked_add(frame_len)
            .ok_or_else(|| EpochError::Capacity("WAL file length overflow".into()))?;
        let previous_len = self.valid_len;
        let write_result = self.file.write_all(&frame).and_then(|()| {
            if durable {
                self.file.sync_data()
            } else {
                self.file.flush()
            }
        });
        if let Err(error) = write_result {
            return Err(self.rollback_failed_append(previous_len, error));
        }

        let record = LogRecord {
            sequence,
            timestamp_ms,
            payload: payload.to_vec(),
        };
        self.valid_len = next_valid_len;
        self.next_sequence = sequence.checked_add(1);
        self.content_hasher.update(&frame);
        self.records.push(record.clone());
        Ok(record)
    }

    fn records_from(&self, sequence: u64, limit: usize) -> Vec<LogRecord> {
        self.records
            .iter()
            .filter(|record| record.sequence >= sequence)
            .take(limit)
            .cloned()
            .collect()
    }

    fn last_sequence(&self) -> Option<u64> {
        self.records.last().map(|record| record.sequence)
    }
}

impl FileWal {
    fn rollback_failed_append(
        &mut self,
        valid_len: u64,
        append_error: std::io::Error,
    ) -> EpochError {
        let rollback_result = self
            .file
            .set_len(valid_len)
            .and_then(|()| self.file.seek(SeekFrom::Start(valid_len)).map(|_| ()))
            .and_then(|()| self.file.sync_data());
        match rollback_result {
            Ok(()) => storage_error(append_error),
            Err(rollback_error) => {
                let reason = format!(
                    "WAL append failed: {append_error}; rollback to byte {valid_len} also failed: {rollback_error}"
                );
                self.poisoned = Some(reason.clone());
                EpochError::Storage(reason)
            }
        }
    }
}

fn encode_frame(sequence: u64, timestamp_ms: u64, payload: &[u8]) -> EpochResult<Vec<u8>> {
    if payload.len() > MAX_PAYLOAD_LEN {
        return Err(EpochError::Capacity(format!(
            "WAL record payload exceeds {MAX_PAYLOAD_LEN} bytes"
        )));
    }
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| EpochError::Capacity("WAL record exceeds 4 GiB".into()))?;
    let frame_capacity = HEADER_LEN
        .checked_add(payload.len())
        .ok_or_else(|| EpochError::Capacity("WAL frame length overflow".into()))?;
    let mut header = [0_u8; HEADER_LEN];
    header[0..4].copy_from_slice(&MAGIC);
    header[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&0_u16.to_le_bytes());
    header[8..16].copy_from_slice(&sequence.to_le_bytes());
    header[16..24].copy_from_slice(&timestamp_ms.to_le_bytes());
    header[24..28].copy_from_slice(&payload_len.to_le_bytes());
    let checksum = checksum(&header[4..28], payload);
    header[28..32].copy_from_slice(&checksum.to_le_bytes());

    let mut frame = Vec::with_capacity(frame_capacity);
    frame.extend_from_slice(&header);
    frame.extend_from_slice(payload);
    Ok(frame)
}

fn encoded_frame_len(payload: &[u8]) -> EpochResult<u64> {
    if payload.len() > MAX_PAYLOAD_LEN {
        return Err(EpochError::Capacity(format!(
            "WAL record payload exceeds {MAX_PAYLOAD_LEN} bytes"
        )));
    }
    u64::try_from(HEADER_LEN)
        .and_then(|header_len| {
            u64::try_from(payload.len()).map(|payload_len| header_len + payload_len)
        })
        .map_err(|error| {
            EpochError::Capacity(format!("WAL frame length cannot be represented: {error}"))
        })
}

fn recover(file: &mut File, first_sequence: u64) -> EpochResult<RecoveredFile> {
    file.seek(SeekFrom::Start(0)).map_err(storage_error)?;
    let mut records = Vec::new();
    let mut valid_len = 0_u64;
    let mut expected_sequence = Some(first_sequence);
    let mut content_hasher = Hasher::new();
    loop {
        let mut header = [0_u8; HEADER_LEN];
        match read_frame_part(file, &mut header) {
            Ok(ReadPart::End) => {
                return Ok(recovered_file(
                    records,
                    valid_len,
                    false,
                    expected_sequence,
                    content_hasher,
                ));
            }
            Ok(ReadPart::Partial) => {
                return Ok(recovered_file(
                    records,
                    valid_len,
                    true,
                    expected_sequence,
                    content_hasher,
                ));
            }
            Ok(ReadPart::Complete) => {}
            Err(error) => return Err(error),
        }

        validate_frame_header(&header, valid_len)?;
        let sequence = u64::from_le_bytes(header[8..16].try_into().expect("fixed header"));
        let expected = expected_sequence.ok_or_else(|| {
            EpochError::Storage("WAL contains records after sequence u64::MAX".into())
        })?;
        if sequence != expected {
            return Err(EpochError::Storage(format!(
                "non-contiguous WAL sequence {sequence}; expected {expected}"
            )));
        }
        let timestamp_ms = u64::from_le_bytes(header[16..24].try_into().expect("fixed header"));
        let payload_len =
            u32::from_le_bytes(header[24..28].try_into().expect("fixed header")) as usize;
        if payload_len > MAX_PAYLOAD_LEN {
            return Err(EpochError::Storage(format!(
                "WAL record at sequence {sequence} declares a payload larger than {MAX_PAYLOAD_LEN} bytes"
            )));
        }
        let expected_checksum =
            u32::from_le_bytes(header[28..32].try_into().expect("fixed header"));
        let mut payload = vec![0_u8; payload_len];
        match read_frame_part(file, &mut payload)? {
            ReadPart::Complete => {}
            ReadPart::End | ReadPart::Partial => {
                return Ok(recovered_file(
                    records,
                    valid_len,
                    true,
                    expected_sequence,
                    content_hasher,
                ));
            }
        }
        let actual_checksum = checksum(&header[4..28], &payload);
        if actual_checksum != expected_checksum {
            return Err(EpochError::Storage(format!(
                "WAL checksum mismatch for sequence {sequence}"
            )));
        }
        let frame_len = u64::try_from(HEADER_LEN)
            .and_then(|header_len| {
                u64::try_from(payload_len).map(|payload_len| header_len + payload_len)
            })
            .map_err(|error| {
                EpochError::Capacity(format!("WAL frame length cannot be represented: {error}"))
            })?;
        valid_len = valid_len
            .checked_add(frame_len)
            .ok_or_else(|| EpochError::Capacity("WAL file length overflow".into()))?;
        expected_sequence = sequence.checked_add(1);
        content_hasher.update(&header);
        content_hasher.update(&payload);
        records.push(LogRecord {
            sequence,
            timestamp_ms,
            payload,
        });
    }
}

fn validate_frame_header(header: &[u8; HEADER_LEN], offset: u64) -> EpochResult<()> {
    if header[0..4] != MAGIC {
        return Err(EpochError::Storage(format!(
            "invalid WAL magic at byte {offset}"
        )));
    }
    let version = u16::from_le_bytes([header[4], header[5]]);
    if version != FORMAT_VERSION {
        return Err(EpochError::Storage(format!(
            "unsupported WAL version {version} at byte {offset}"
        )));
    }
    let flags = u16::from_le_bytes([header[6], header[7]]);
    if flags != 0 {
        return Err(EpochError::Storage(format!(
            "unsupported WAL flags {flags:#06x} at byte {offset}"
        )));
    }
    Ok(())
}

fn recovered_file(
    records: Vec<LogRecord>,
    valid_len: u64,
    recovered_partial_tail: bool,
    next_sequence: Option<u64>,
    content_hasher: Hasher,
) -> RecoveredFile {
    RecoveredFile {
        records,
        valid_len,
        recovered_partial_tail,
        next_sequence,
        content_hasher,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadPart {
    End,
    Partial,
    Complete,
}

fn read_frame_part(file: &mut File, buffer: &mut [u8]) -> EpochResult<ReadPart> {
    let mut read = 0;
    while read < buffer.len() {
        match file.read(&mut buffer[read..]) {
            Ok(0) if read == 0 => return Ok(ReadPart::End),
            Ok(0) => return Ok(ReadPart::Partial),
            Ok(count) => read += count,
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(storage_error(error)),
        }
    }
    Ok(ReadPart::Complete)
}

fn checksum(header_without_magic_or_crc: &[u8], payload: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(header_without_magic_or_crc);
    hasher.update(payload);
    hasher.finalize()
}

#[allow(clippy::needless_pass_by_value)]
fn storage_error(error: std::io::Error) -> EpochError {
    EpochError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, io::Write as _};

    use tempfile::{NamedTempFile, TempDir};

    use super::*;

    #[test]
    fn memory_log_sequences_records() {
        let mut log = MemoryLog::default();
        assert_eq!(log.append(10, b"one", false).unwrap().sequence, 0);
        assert_eq!(log.append(20, b"two", false).unwrap().sequence, 1);
        assert_eq!(log.records_from(1, 10)[0].payload, b"two");
    }

    #[test]
    fn file_wal_round_trips() {
        let temp = NamedTempFile::new().unwrap();
        {
            let mut wal = FileWal::open(temp.path()).unwrap();
            wal.append(10, b"first", true).unwrap();
            wal.append(11, b"second", true).unwrap();
        }
        let wal = FileWal::open(temp.path()).unwrap();
        assert_eq!(wal.last_sequence(), Some(1));
        assert_eq!(wal.records_from(0, 10)[1].payload, b"second");
        assert!(!wal.recovered_partial_tail());
    }

    #[test]
    fn recovery_discards_partial_tail_only() {
        let temp = NamedTempFile::new().unwrap();
        {
            let mut wal = FileWal::open(temp.path()).unwrap();
            wal.append(10, b"complete", true).unwrap();
        }
        let valid_len = temp.as_file().metadata().unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(temp.path())
            .unwrap()
            .write_all(b"EPCHpartial")
            .unwrap();
        let wal = FileWal::open(temp.path()).unwrap();
        assert!(wal.recovered_partial_tail());
        assert_eq!(wal.last_sequence(), Some(0));
        assert_eq!(std::fs::metadata(temp.path()).unwrap().len(), valid_len);
    }

    #[test]
    fn checksum_corruption_is_not_silently_truncated() {
        let temp = NamedTempFile::new().unwrap();
        {
            let mut wal = FileWal::open(temp.path()).unwrap();
            wal.append(10, b"complete", true).unwrap();
        }
        let mut bytes = std::fs::read(temp.path()).unwrap();
        *bytes.last_mut().unwrap() ^= 0xff;
        std::fs::write(temp.path(), bytes).unwrap();
        assert!(matches!(
            FileWal::open(temp.path()),
            Err(EpochError::Storage(_))
        ));
    }

    #[test]
    fn file_wal_rejects_a_second_writer() {
        let temp = NamedTempFile::new().unwrap();
        let first = FileWal::open(temp.path()).unwrap();

        assert!(matches!(
            FileWal::open(temp.path()),
            Err(EpochError::Storage(_))
        ));
        drop(first);
        FileWal::open(temp.path()).expect("lock is released when the owner closes");
    }

    #[test]
    fn recovery_rejects_an_oversized_declared_payload() {
        let temp = NamedTempFile::new().unwrap();
        let mut header = [0_u8; HEADER_LEN];
        header[0..4].copy_from_slice(&MAGIC);
        header[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        let declared_len = u32::try_from(MAX_PAYLOAD_LEN + 1).unwrap();
        header[24..28].copy_from_slice(&declared_len.to_le_bytes());
        temp.as_file()
            .write_all(&header)
            .expect("synthetic header is written");

        assert!(matches!(
            FileWal::open(temp.path()),
            Err(EpochError::Storage(_))
        ));
    }

    #[test]
    fn recovery_rejects_unknown_flags_even_with_a_valid_checksum() {
        let temp = NamedTempFile::new().unwrap();
        let payload = b"complete";
        let mut frame = encode_frame(0, 10, payload).unwrap();
        frame[6..8].copy_from_slice(&1_u16.to_le_bytes());
        let checksum = checksum(&frame[4..28], payload);
        frame[28..32].copy_from_slice(&checksum.to_le_bytes());
        std::fs::write(temp.path(), frame).unwrap();

        assert!(matches!(
            FileWal::open(temp.path()),
            Err(EpochError::Storage(_))
        ));
    }

    #[test]
    fn segmented_wal_rotates_and_recovers_global_sequences() {
        let temporary = TempDir::new().unwrap();
        let directory = temporary.path().join("engine-wal");
        let max_segment_bytes = u64::try_from(HEADER_LEN + 5).unwrap();
        {
            let mut wal = SegmentedWal::open(&directory, max_segment_bytes).unwrap();
            assert_eq!(wal.append(10, b"first", true).unwrap().sequence, 0);
            assert_eq!(wal.append(20, b"second", true).unwrap().sequence, 1);
            assert_eq!(wal.append(30, b"third", true).unwrap().sequence, 2);
            assert_eq!(wal.segment_count(), 3);
        }

        let wal = SegmentedWal::open(&directory, max_segment_bytes).unwrap();
        assert_eq!(wal.segment_count(), 3);
        assert_eq!(wal.last_sequence(), Some(2));
        assert_eq!(wal.records_from(0, usize::MAX).len(), 3);
        assert_eq!(wal.records_from(1, 1)[0].payload, b"second");
        assert_eq!(
            wal.records_from(1, 10)
                .into_iter()
                .map(|record| record.payload)
                .collect::<Vec<_>>(),
            vec![b"second".to_vec(), b"third".to_vec()]
        );
        assert_eq!(
            segment_file_names(&directory),
            vec![
                "segment-00000000000000000000.wal",
                "segment-00000000000000000001.wal",
                "segment-00000000000000000002.wal",
            ]
        );
    }

    #[test]
    fn segmented_wal_repairs_only_the_active_tail() {
        let temporary = TempDir::new().unwrap();
        let directory = temporary.path().join("engine-wal");
        let max_segment_bytes = u64::try_from(HEADER_LEN + 3).unwrap();
        {
            let mut wal = SegmentedWal::open(&directory, max_segment_bytes).unwrap();
            wal.append(10, b"one", true).unwrap();
            wal.append(20, b"two", true).unwrap();
        }
        let active = directory.join("segment-00000000000000000001.wal");
        let active_valid_len = std::fs::metadata(&active).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&active)
            .unwrap()
            .write_all(b"EPCHpartial")
            .unwrap();

        let wal = SegmentedWal::open(&directory, max_segment_bytes).unwrap();
        assert!(wal.recovered_partial_tail());
        assert_eq!(wal.last_sequence(), Some(1));
        assert_eq!(std::fs::metadata(active).unwrap().len(), active_valid_len);
    }

    #[test]
    fn segmented_wal_rejects_a_partial_sealed_segment() {
        let temporary = TempDir::new().unwrap();
        let directory = temporary.path().join("engine-wal");
        let max_segment_bytes = u64::try_from(HEADER_LEN + 3).unwrap();
        {
            let mut wal = SegmentedWal::open(&directory, max_segment_bytes).unwrap();
            wal.append(10, b"one", true).unwrap();
            wal.append(20, b"two", true).unwrap();
        }
        OpenOptions::new()
            .append(true)
            .open(directory.join("segment-00000000000000000000.wal"))
            .unwrap()
            .write_all(b"EPCHpartial")
            .unwrap();

        assert!(matches!(
            SegmentedWal::open(&directory, max_segment_bytes),
            Err(EpochError::Storage(_))
        ));
    }

    #[test]
    fn segmented_wal_rejects_sequence_gaps_between_files() {
        let temporary = TempDir::new().unwrap();
        let directory = temporary.path().join("engine-wal");
        let max_segment_bytes = u64::try_from(HEADER_LEN + 3).unwrap();
        {
            let mut wal = SegmentedWal::open(&directory, max_segment_bytes).unwrap();
            wal.append(10, b"one", true).unwrap();
            wal.append(20, b"two", true).unwrap();
        }
        std::fs::rename(
            directory.join("segment-00000000000000000001.wal"),
            directory.join("segment-00000000000000000002.wal"),
        )
        .unwrap();

        assert!(matches!(
            SegmentedWal::open(&directory, max_segment_bytes),
            Err(EpochError::Storage(_))
        ));
    }

    #[test]
    fn segmented_wal_rejects_checksum_corruption() {
        let temporary = TempDir::new().unwrap();
        let directory = temporary.path().join("engine-wal");
        {
            let mut wal = SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES).unwrap();
            wal.append(10, b"complete", true).unwrap();
        }
        let segment = directory.join("segment-00000000000000000000.wal");
        let mut bytes = std::fs::read(&segment).unwrap();
        *bytes.last_mut().unwrap() ^= 0xff;
        std::fs::write(segment, bytes).unwrap();

        assert!(matches!(
            SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES),
            Err(EpochError::Storage(_))
        ));
    }

    #[test]
    fn segmented_wal_rejects_malformed_segment_names() {
        let temporary = TempDir::new().unwrap();
        let directory = temporary.path().join("engine-wal");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join("segment-not-a-sequence.wal"), []).unwrap();

        assert!(matches!(
            SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES),
            Err(EpochError::Storage(_))
        ));
    }

    #[test]
    fn segmented_wal_keeps_an_oversized_frame_whole() {
        let temporary = TempDir::new().unwrap();
        let directory = temporary.path().join("engine-wal");
        let mut wal = SegmentedWal::open(&directory, MIN_WAL_SEGMENT_BYTES).unwrap();

        wal.append(10, b"larger-than-the-target", true).unwrap();
        assert_eq!(wal.segment_count(), 1);
        wal.append(20, b"next", true).unwrap();
        assert_eq!(wal.segment_count(), 2);
        assert_eq!(wal.records_from(0, usize::MAX).len(), 2);
    }

    #[test]
    fn segmented_wal_rejects_a_target_smaller_than_a_frame_header() {
        let temporary = TempDir::new().unwrap();
        assert!(matches!(
            SegmentedWal::open(
                temporary.path().join("engine-wal"),
                MIN_WAL_SEGMENT_BYTES - 1
            ),
            Err(EpochError::InvalidArgument(_))
        ));
    }

    #[test]
    fn relative_wal_root_syncs_the_current_directory() {
        assert_eq!(parent_directory(Path::new(".epoch")), Some(Path::new(".")));
        assert_eq!(
            parent_directory(Path::new("nested/.epoch")),
            Some(Path::new("nested"))
        );
        sync_parent_directory(Path::new(".epoch")).unwrap();
    }

    #[test]
    fn segmented_wal_creates_nested_directory_components() {
        let temporary = TempDir::new().unwrap();
        let directory = temporary.path().join("one/two/engine-wal");
        let mut wal = SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES).unwrap();
        wal.append(10, b"committed", true).unwrap();
        drop(wal);

        let wal = SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES).unwrap();
        assert_eq!(wal.last_sequence(), Some(0));
    }

    #[test]
    fn segmented_wal_rejects_a_second_writer() {
        let temporary = TempDir::new().unwrap();
        let directory = temporary.path().join("engine-wal");
        let first = SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES).unwrap();

        assert!(matches!(
            SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES),
            Err(EpochError::Storage(_))
        ));
        drop(first);
        SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES)
            .expect("directory lock is released when the owner closes");
    }

    #[test]
    fn segmented_wal_cannot_rotate_past_a_poisoned_active_segment() {
        let temporary = TempDir::new().unwrap();
        let directory = temporary.path().join("engine-wal");
        let max_segment_bytes = 128;
        let mut wal = SegmentedWal::open(&directory, max_segment_bytes).unwrap();
        wal.append(10, b"committed", true).unwrap();

        let active = wal.segments.last_mut().unwrap();
        let writable_file = active.file.try_clone().unwrap();
        active.file = File::open(active.path()).unwrap();
        let failed = wal.append(20, b"x", true).unwrap_err();
        assert!(matches!(failed, EpochError::Storage(_)));
        assert!(wal.segments.last().unwrap().poisoned.is_some());

        wal.segments.last_mut().unwrap().file = writable_file;
        let manifest_before = read_manifest(&directory).unwrap();
        let segment_names_before = segment_file_names(&directory);
        let failed = wal.append(30, &[b'x'; 100], true).unwrap_err();
        assert!(matches!(
            failed,
            EpochError::Storage(message)
                if message.contains("active segment was poisoned")
        ));
        assert_eq!(wal.segment_count(), 1);
        assert_eq!(read_manifest(&directory).unwrap(), manifest_before);
        assert_eq!(segment_file_names(&directory), segment_names_before);
    }

    #[test]
    fn standalone_wal_activation_blocks_single_file_writers() {
        let temporary = TempDir::new().unwrap();
        let mut wal = StandaloneWal::open(temporary.path(), DEFAULT_WAL_SEGMENT_BYTES).unwrap();
        assert!(!wal.uses_legacy_layout());
        wal.append(10, b"segmented", true).unwrap();

        let legacy_path = temporary.path().join("engine.wal");
        assert!(matches!(
            FileWal::open(&legacy_path),
            Err(EpochError::Storage(_))
        ));
        drop(wal);
        assert!(matches!(
            FileWal::open(&legacy_path),
            Err(EpochError::Storage(_))
        ));

        let wal = StandaloneWal::open(temporary.path(), DEFAULT_WAL_SEGMENT_BYTES).unwrap();
        assert_eq!(wal.last_sequence(), Some(0));
    }

    #[test]
    fn standalone_wal_resumes_a_torn_staging_marker() {
        let temporary = TempDir::new().unwrap();
        let marker_path = temporary.path().join("engine.wal");
        let mut torn = [0_u8; STAGING_ACTIVATION_MARKER.len()];
        torn[..12].copy_from_slice(&STAGING_ACTIVATION_MARKER[..12]);
        std::fs::write(&marker_path, torn).unwrap();

        let wal = StandaloneWal::open(temporary.path(), DEFAULT_WAL_SEGMENT_BYTES).unwrap();
        assert!(!wal.uses_legacy_layout());
        assert_eq!(
            std::fs::read(marker_path).unwrap(),
            ACTIVE_ACTIVATION_MARKER
        );
    }

    #[test]
    fn standalone_wal_rejects_a_missing_activated_segment_directory() {
        let temporary = TempDir::new().unwrap();
        {
            let mut wal = StandaloneWal::open(temporary.path(), DEFAULT_WAL_SEGMENT_BYTES).unwrap();
            wal.append(10, b"committed", true).unwrap();
        }
        std::fs::remove_dir_all(temporary.path().join("engine-wal")).unwrap();

        assert!(matches!(
            StandaloneWal::open(temporary.path(), DEFAULT_WAL_SEGMENT_BYTES),
            Err(EpochError::Storage(_))
        ));
    }

    #[test]
    fn standalone_wal_keeps_existing_legacy_history_downgrade_safe() {
        let temporary = TempDir::new().unwrap();
        let legacy_path = temporary.path().join("engine.wal");
        {
            let mut legacy = FileWal::open(&legacy_path).unwrap();
            legacy.append(10, b"legacy", true).unwrap();
        }

        {
            let mut wal = StandaloneWal::open(temporary.path(), DEFAULT_WAL_SEGMENT_BYTES).unwrap();
            assert!(wal.uses_legacy_layout());
            assert_eq!(wal.append(20, b"still-legacy", true).unwrap().sequence, 1);
            assert_eq!(wal.segment_count(), 0);
        }

        let legacy = FileWal::open(&legacy_path).unwrap();
        assert_eq!(legacy.last_sequence(), Some(1));
        assert_eq!(legacy.records_from(0, usize::MAX).len(), 2);
        assert!(!temporary.path().join("engine-wal").exists());
    }

    #[test]
    fn standalone_wal_rejects_ambiguous_legacy_and_segmented_histories() {
        let temporary = TempDir::new().unwrap();
        {
            let mut legacy = FileWal::open(temporary.path().join("engine.wal")).unwrap();
            legacy.append(10, b"legacy", true).unwrap();
        }
        {
            let mut segmented = SegmentedWal::open(
                temporary.path().join("engine-wal"),
                DEFAULT_WAL_SEGMENT_BYTES,
            )
            .unwrap();
            segmented.append(10, b"different", true).unwrap();
        }

        assert!(matches!(
            StandaloneWal::open(temporary.path(), DEFAULT_WAL_SEGMENT_BYTES),
            Err(EpochError::Storage(_))
        ));
    }

    #[test]
    fn segmented_wal_rejects_a_missing_committed_final_segment() {
        let temporary = TempDir::new().unwrap();
        let directory = temporary.path().join("engine-wal");
        let max_segment_bytes = u64::try_from(HEADER_LEN + 3).unwrap();
        {
            let mut wal = SegmentedWal::open(&directory, max_segment_bytes).unwrap();
            wal.append(10, b"one", true).unwrap();
            wal.append(20, b"two", true).unwrap();
        }
        std::fs::remove_file(directory.join("segment-00000000000000000001.wal")).unwrap();

        assert!(matches!(
            SegmentedWal::open(&directory, max_segment_bytes),
            Err(EpochError::Storage(_))
        ));
    }

    #[test]
    fn segmented_wal_rejects_all_committed_segments_missing() {
        let temporary = TempDir::new().unwrap();
        let directory = temporary.path().join("engine-wal");
        {
            let mut wal = SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES).unwrap();
            wal.append(10, b"one", true).unwrap();
        }
        for name in segment_file_names(&directory) {
            std::fs::remove_file(directory.join(name)).unwrap();
        }

        assert!(matches!(
            SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES),
            Err(EpochError::Storage(_))
        ));
    }

    #[test]
    fn segmented_wal_rejects_a_missing_manifest_after_activation() {
        let temporary = TempDir::new().unwrap();
        let directory = temporary.path().join("engine-wal");
        {
            let mut wal = SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES).unwrap();
            wal.append(10, b"one", true).unwrap();
        }
        std::fs::remove_file(directory.join(MANIFEST_FILE)).unwrap();

        assert!(matches!(
            SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES),
            Err(EpochError::Storage(_))
        ));
    }

    #[test]
    fn segmented_wal_rejects_a_missing_identity() {
        let temporary = TempDir::new().unwrap();
        let directory = temporary.path().join("engine-wal");
        drop(SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES).unwrap());
        std::fs::remove_file(directory.join(IDENTITY_FILE)).unwrap();

        assert!(matches!(
            SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES),
            Err(EpochError::Storage(_))
        ));
    }

    #[test]
    fn segmented_wal_rejects_a_truncated_committed_active_segment() {
        let temporary = TempDir::new().unwrap();
        let directory = temporary.path().join("engine-wal");
        let segment = directory.join("segment-00000000000000000000.wal");
        {
            let mut wal = SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES).unwrap();
            wal.append(10, b"committed", true).unwrap();
        }
        let committed_len = std::fs::metadata(&segment).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&segment)
            .unwrap()
            .set_len(committed_len - 1)
            .unwrap();

        assert!(matches!(
            SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES),
            Err(EpochError::Storage(_))
        ));
    }

    #[test]
    fn segmented_wal_completes_a_manifested_pending_rotation() {
        let temporary = TempDir::new().unwrap();
        let directory = temporary.path().join("engine-wal");
        {
            let mut wal = SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES).unwrap();
            wal.append(10, b"one", true).unwrap();
        }
        let mut manifest = read_manifest(&directory).unwrap();
        manifest.pending_segment = Some(1);
        write_manifest(&directory, &manifest).unwrap();

        let mut wal = SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES).unwrap();
        assert_eq!(wal.segment_count(), 2);
        assert_eq!(wal.append(20, b"two", true).unwrap().sequence, 1);
        drop(wal);

        let manifest = read_manifest(&directory).unwrap();
        assert_eq!(manifest.pending_segment, None);
        assert_eq!(manifest.segments.len(), 2);
    }

    #[test]
    fn segmented_wal_rejects_a_foreign_manifest_identity() {
        let temporary = TempDir::new().unwrap();
        let directory = temporary.path().join("engine-wal");
        drop(SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES).unwrap());
        let mut manifest = read_manifest(&directory).unwrap();
        manifest.wal_id = Uuid::now_v7();
        write_manifest(&directory, &manifest).unwrap();

        assert!(matches!(
            SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES),
            Err(EpochError::Storage(_))
        ));
    }

    #[test]
    fn segmented_wal_rejects_valid_frames_that_do_not_match_the_manifest() {
        let temporary = TempDir::new().unwrap();
        let directory = temporary.path().join("engine-wal");
        let segment = directory.join("segment-00000000000000000000.wal");
        {
            let mut wal = SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES).unwrap();
            wal.append(10, b"aaaa", true).unwrap();
        }
        std::fs::write(&segment, encode_frame(0, 10, b"bbbb").unwrap()).unwrap();

        assert!(matches!(
            SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES),
            Err(EpochError::Storage(_))
        ));
    }

    #[test]
    fn segmented_wal_discards_bytes_not_committed_by_the_manifest() {
        let temporary = TempDir::new().unwrap();
        let directory = temporary.path().join("engine-wal");
        let segment = directory.join("segment-00000000000000000000.wal");
        let committed_len;
        {
            let mut wal = SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES).unwrap();
            wal.append(10, b"committed", true).unwrap();
            committed_len = std::fs::metadata(&segment).unwrap().len();
        }
        OpenOptions::new()
            .append(true)
            .open(&segment)
            .unwrap()
            .write_all(&encode_frame(1, 20, b"not-manifested").unwrap())
            .unwrap();

        let wal = SegmentedWal::open(&directory, DEFAULT_WAL_SEGMENT_BYTES).unwrap();
        assert!(wal.recovered_partial_tail());
        assert_eq!(wal.last_sequence(), Some(0));
        assert_eq!(std::fs::metadata(segment).unwrap().len(), committed_len);
    }

    fn segment_file_names(directory: &Path) -> Vec<String> {
        let mut names = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .filter(|name| parse_segment_file_name(name).is_some())
            .collect::<Vec<_>>();
        names.sort();
        names
    }
}

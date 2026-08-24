//! Portable, canonical node-volume backups and atomic fresh-volume restore.

use std::{
    fmt::Write as _,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const BACKUP_FORMAT_VERSION: u16 = 1;
const MAX_BACKUP_BYTES: u64 = 512 * 1024 * 1024;
const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_FILE_COUNT: usize = 65_536;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BackupFile {
    path: String,
    size_bytes: u64,
    sha256: String,
    data_base64: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RegionalBackupDocument {
    format_version: u16,
    epoch_version: String,
    captured_at_ms: u64,
    node_id: Option<u64>,
    files: Vec<BackupFile>,
    manifest_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RegionalBackupSetDocument {
    format_version: u16,
    epoch_version: String,
    captured_at_min_ms: u64,
    captured_at_max_ms: u64,
    nodes: Vec<RegionalBackupSetNode>,
    manifest_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RegionalBackupSetNode {
    node_id: u64,
    artifact_file: String,
    artifact_bytes: u64,
    artifact_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RegionalBackupMetadata {
    pub format_version: u16,
    pub epoch_version: String,
    pub captured_at_ms: u64,
    pub node_id: Option<u64>,
    pub file_count: usize,
    pub uncompressed_bytes: u64,
    pub manifest_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RegionalBackupSetMetadata {
    pub format_version: u16,
    pub epoch_version: String,
    pub captured_at_min_ms: u64,
    pub captured_at_max_ms: u64,
    pub node_ids: Vec<u64>,
    pub manifest_sha256: String,
}

#[derive(Debug, Error)]
pub enum RegionalBackupError {
    #[error("backup input is invalid: {0}")]
    InvalidInput(String),
    #[error("backup artifact is invalid: {0}")]
    InvalidArtifact(String),
    #[error("backup storage operation failed: {0}")]
    Storage(String),
}

pub fn create_backup(
    data_directory: &Path,
    output: &Path,
    captured_at_ms: u64,
    node_id: Option<u64>,
) -> Result<RegionalBackupMetadata, RegionalBackupError> {
    if captured_at_ms == 0 {
        return Err(RegionalBackupError::InvalidInput(
            "capture time must be non-zero".into(),
        ));
    }
    let source = data_directory
        .canonicalize()
        .map_err(|error| storage_error("canonicalize data directory", error))?;
    if !source.is_dir() {
        return Err(RegionalBackupError::InvalidInput(format!(
            "{} is not a directory",
            source.display()
        )));
    }
    let output_parent = output
        .parent()
        .ok_or_else(|| RegionalBackupError::InvalidInput("backup output has no parent".into()))?;
    fs::create_dir_all(output_parent)
        .map_err(|error| storage_error("create backup output parent", error))?;
    reject_output_inside_source(&source, output)?;
    let mut paths = Vec::new();
    collect_files(&source, &source, &mut paths)?;
    if paths.is_empty() {
        return Err(RegionalBackupError::InvalidInput(
            "data directory contains no files".into(),
        ));
    }
    if paths.len() > MAX_FILE_COUNT {
        return Err(RegionalBackupError::InvalidInput(format!(
            "data directory contains more than {MAX_FILE_COUNT} files"
        )));
    }
    paths.sort();
    let _stable_locks = acquire_stable_locks(&source, &paths)?;
    let mut files = Vec::with_capacity(paths.len());
    let mut uncompressed_bytes = 0_u64;
    for relative in paths {
        let absolute = source.join(&relative);
        let metadata = fs::symlink_metadata(&absolute)
            .map_err(|error| storage_error("inspect backup file", error))?;
        if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
            return Err(RegionalBackupError::InvalidInput(format!(
                "backup source {} is not a regular file",
                absolute.display()
            )));
        }
        if metadata.len() > MAX_FILE_BYTES {
            return Err(RegionalBackupError::InvalidInput(format!(
                "backup source {} exceeds {MAX_FILE_BYTES} bytes",
                absolute.display()
            )));
        }
        uncompressed_bytes = uncompressed_bytes
            .checked_add(metadata.len())
            .filter(|total| *total <= MAX_BACKUP_BYTES)
            .ok_or_else(|| {
                RegionalBackupError::InvalidInput(format!(
                    "backup source exceeds {MAX_BACKUP_BYTES} bytes"
                ))
            })?;
        let data = read_bounded_file(&absolute, metadata.len())?;
        files.push(BackupFile {
            path: portable_path(&relative)?,
            size_bytes: metadata.len(),
            sha256: hex_digest(&data),
            data_base64: BASE64.encode(data),
        });
    }
    let mut document = RegionalBackupDocument {
        format_version: BACKUP_FORMAT_VERSION,
        epoch_version: env!("CARGO_PKG_VERSION").into(),
        captured_at_ms,
        node_id,
        files,
        manifest_sha256: String::new(),
    };
    document.manifest_sha256 = document_digest(&document)?;
    let encoded = serde_json::to_vec(&document)
        .map_err(|error| RegionalBackupError::InvalidArtifact(error.to_string()))?;
    if encoded.len() as u64 > MAX_BACKUP_BYTES.saturating_mul(2) {
        return Err(RegionalBackupError::InvalidInput(
            "encoded backup exceeds the artifact size limit".into(),
        ));
    }
    atomic_write(output, &encoded)?;
    Ok(metadata(&document, uncompressed_bytes))
}

fn acquire_stable_locks(root: &Path, paths: &[PathBuf]) -> Result<Vec<File>, RegionalBackupError> {
    let journal_paths = paths
        .iter()
        .filter(|path| path.extension().is_some_and(|extension| extension == "wal"))
        .collect::<Vec<_>>();
    if journal_paths.is_empty() {
        return Err(RegionalBackupError::InvalidInput(
            "data directory contains no lockable Epoch journal".into(),
        ));
    }
    let mut locks = Vec::with_capacity(journal_paths.len());
    for path in journal_paths {
        let absolute = root.join(path);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&absolute)
            .map_err(|error| storage_error("open stable journal for backup", error))?;
        file.try_lock().map_err(|error| {
            RegionalBackupError::InvalidInput(format!(
                "data directory is active; journal {} cannot be locked: {error}",
                absolute.display()
            ))
        })?;
        locks.push(file);
    }
    Ok(locks)
}

pub fn inspect_backup(input: &Path) -> Result<RegionalBackupMetadata, RegionalBackupError> {
    let encoded = read_artifact(input)?;
    let (document, uncompressed_bytes) = decode_and_validate(&encoded)?;
    Ok(metadata(&document, uncompressed_bytes))
}

pub fn restore_backup(
    input: &Path,
    destination: &Path,
) -> Result<RegionalBackupMetadata, RegionalBackupError> {
    let encoded = read_artifact(input)?;
    let (document, uncompressed_bytes) = decode_and_validate(&encoded)?;
    validate_fresh_destination(destination)?;
    let parent = destination.parent().ok_or_else(|| {
        RegionalBackupError::InvalidInput("restore destination has no parent".into())
    })?;
    fs::create_dir_all(parent).map_err(|error| storage_error("create restore parent", error))?;
    let file_name = destination.file_name().ok_or_else(|| {
        RegionalBackupError::InvalidInput("restore destination has no file name".into())
    })?;
    let staging = parent.join(format!(
        ".{}.epoch-restore-{}",
        file_name.to_string_lossy(),
        std::process::id()
    ));
    if staging.exists() {
        return Err(RegionalBackupError::Storage(format!(
            "restore staging path {} already exists",
            staging.display()
        )));
    }
    fs::create_dir(&staging).map_err(|error| storage_error("create restore staging", error))?;
    let restore_result = restore_files(&staging, &document.files);
    if let Err(error) = restore_result {
        let _ = fs::remove_dir_all(&staging);
        return Err(error);
    }
    sync_directory_tree(&staging)?;
    if destination.exists() {
        fs::remove_dir(destination)
            .map_err(|error| storage_error("remove empty restore destination", error))?;
    }
    fs::rename(&staging, destination)
        .map_err(|error| storage_error("publish restored data directory", error))?;
    sync_directory(parent)?;
    Ok(metadata(&document, uncompressed_bytes))
}

pub fn create_backup_set(
    node_artifacts: &[PathBuf],
    output: &Path,
) -> Result<RegionalBackupSetMetadata, RegionalBackupError> {
    if !matches!(node_artifacts.len(), 3 | 5) {
        return Err(RegionalBackupError::InvalidInput(
            "a regional backup set requires exactly three or five node artifacts".into(),
        ));
    }
    let output_parent = output.parent().ok_or_else(|| {
        RegionalBackupError::InvalidInput("backup-set output has no parent".into())
    })?;
    fs::create_dir_all(output_parent)
        .map_err(|error| storage_error("create backup-set parent", error))?;
    let canonical_parent = output_parent
        .canonicalize()
        .map_err(|error| storage_error("canonicalize backup-set parent", error))?;
    let mut nodes = Vec::with_capacity(node_artifacts.len());
    let mut captured_at_min_ms = u64::MAX;
    let mut captured_at_max_ms = 0_u64;
    let mut epoch_version = None;
    for artifact_path in node_artifacts {
        let canonical = artifact_path
            .canonicalize()
            .map_err(|error| storage_error("canonicalize node artifact", error))?;
        if canonical.parent() != Some(canonical_parent.as_path()) {
            return Err(RegionalBackupError::InvalidInput(
                "node artifacts and backup-set manifest must share one directory".into(),
            ));
        }
        let encoded = read_artifact(&canonical)?;
        let (document, _) = decode_and_validate(&encoded)?;
        let node_id = document.node_id.ok_or_else(|| {
            RegionalBackupError::InvalidInput(
                "every regional node artifact must declare a node ID".into(),
            )
        })?;
        if node_id == 0
            || nodes
                .iter()
                .any(|node: &RegionalBackupSetNode| node.node_id == node_id)
        {
            return Err(RegionalBackupError::InvalidInput(
                "regional node IDs must be non-zero and unique".into(),
            ));
        }
        if epoch_version
            .as_deref()
            .is_some_and(|version| version != document.epoch_version)
        {
            return Err(RegionalBackupError::InvalidInput(
                "all node artifacts must use the same Epoch version".into(),
            ));
        }
        epoch_version.get_or_insert_with(|| document.epoch_version.clone());
        captured_at_min_ms = captured_at_min_ms.min(document.captured_at_ms);
        captured_at_max_ms = captured_at_max_ms.max(document.captured_at_ms);
        let file_name = canonical
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                RegionalBackupError::InvalidInput("node artifact names must be valid UTF-8".into())
            })?;
        validate_artifact_file_name(file_name)?;
        nodes.push(RegionalBackupSetNode {
            node_id,
            artifact_file: file_name.into(),
            artifact_bytes: encoded.len() as u64,
            artifact_sha256: hex_digest(&encoded),
        });
    }
    nodes.sort_by_key(|node| node.node_id);
    let expected = (1..=nodes.len() as u64).collect::<Vec<_>>();
    if nodes.iter().map(|node| node.node_id).collect::<Vec<_>>() != expected {
        return Err(RegionalBackupError::InvalidInput(
            "regional backup node IDs must be contiguous from one".into(),
        ));
    }
    let mut document = RegionalBackupSetDocument {
        format_version: BACKUP_FORMAT_VERSION,
        epoch_version: epoch_version.unwrap_or_default(),
        captured_at_min_ms,
        captured_at_max_ms,
        nodes,
        manifest_sha256: String::new(),
    };
    document.manifest_sha256 = backup_set_digest(&document)?;
    let encoded = serde_json::to_vec(&document)
        .map_err(|error| RegionalBackupError::InvalidArtifact(error.to_string()))?;
    atomic_write(output, &encoded)?;
    Ok(backup_set_metadata(&document))
}

pub fn inspect_backup_set(input: &Path) -> Result<RegionalBackupSetMetadata, RegionalBackupError> {
    let document = decode_backup_set(input)?;
    Ok(backup_set_metadata(&document))
}

pub fn restore_backup_set_node(
    manifest: &Path,
    node_id: u64,
    destination: &Path,
) -> Result<RegionalBackupMetadata, RegionalBackupError> {
    let document = decode_backup_set(manifest)?;
    let node = document
        .nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .ok_or_else(|| {
            RegionalBackupError::InvalidInput(format!(
                "backup set contains no node artifact for node {node_id}"
            ))
        })?;
    let artifact_path = manifest
        .parent()
        .ok_or_else(|| RegionalBackupError::InvalidInput("manifest has no parent".into()))?
        .join(&node.artifact_file);
    let encoded = read_artifact(&artifact_path)?;
    if encoded.len() as u64 != node.artifact_bytes || hex_digest(&encoded) != node.artifact_sha256 {
        return Err(RegionalBackupError::InvalidArtifact(format!(
            "node {node_id} artifact does not match the backup-set manifest"
        )));
    }
    let metadata = restore_backup(&artifact_path, destination)?;
    if metadata.node_id != Some(node_id) {
        return Err(RegionalBackupError::InvalidArtifact(format!(
            "restored artifact identity does not match node {node_id}"
        )));
    }
    Ok(metadata)
}

fn decode_backup_set(input: &Path) -> Result<RegionalBackupSetDocument, RegionalBackupError> {
    let encoded = read_artifact(input)?;
    let document: RegionalBackupSetDocument = serde_json::from_slice(&encoded)
        .map_err(|error| RegionalBackupError::InvalidArtifact(error.to_string()))?;
    if serde_json::to_vec(&document)
        .map_err(|error| RegionalBackupError::InvalidArtifact(error.to_string()))?
        != encoded
        || document.format_version != BACKUP_FORMAT_VERSION
        || document.epoch_version.is_empty()
        || !matches!(document.nodes.len(), 3 | 5)
        || document.captured_at_min_ms == 0
        || document.captured_at_min_ms > document.captured_at_max_ms
        || document.manifest_sha256.len() != 64
        || backup_set_digest(&document)? != document.manifest_sha256
    {
        return Err(RegionalBackupError::InvalidArtifact(
            "backup-set header, encoding, or digest is invalid".into(),
        ));
    }
    for (index, node) in document.nodes.iter().enumerate() {
        if node.node_id != index as u64 + 1
            || node.artifact_bytes == 0
            || node.artifact_bytes > MAX_BACKUP_BYTES * 2
            || node.artifact_sha256.len() != 64
        {
            return Err(RegionalBackupError::InvalidArtifact(
                "backup-set node registry is invalid".into(),
            ));
        }
        validate_artifact_file_name(&node.artifact_file)?;
    }
    Ok(document)
}

fn validate_artifact_file_name(name: &str) -> Result<(), RegionalBackupError> {
    let path = Path::new(name);
    if name.is_empty()
        || path.components().count() != 1
        || !matches!(path.components().next(), Some(Component::Normal(_)))
    {
        return Err(RegionalBackupError::InvalidArtifact(format!(
            "unsafe backup-set artifact name {name:?}"
        )));
    }
    Ok(())
}

fn backup_set_digest(document: &RegionalBackupSetDocument) -> Result<String, RegionalBackupError> {
    let mut unsigned = document.clone();
    unsigned.manifest_sha256.clear();
    serde_json::to_vec(&unsigned)
        .map(|encoded| hex_digest(&encoded))
        .map_err(|error| RegionalBackupError::InvalidArtifact(error.to_string()))
}

fn backup_set_metadata(document: &RegionalBackupSetDocument) -> RegionalBackupSetMetadata {
    RegionalBackupSetMetadata {
        format_version: document.format_version,
        epoch_version: document.epoch_version.clone(),
        captured_at_min_ms: document.captured_at_min_ms,
        captured_at_max_ms: document.captured_at_max_ms,
        node_ids: document.nodes.iter().map(|node| node.node_id).collect(),
        manifest_sha256: document.manifest_sha256.clone(),
    }
}

fn decode_and_validate(
    encoded: &[u8],
) -> Result<(RegionalBackupDocument, u64), RegionalBackupError> {
    let document: RegionalBackupDocument = serde_json::from_slice(encoded)
        .map_err(|error| RegionalBackupError::InvalidArtifact(error.to_string()))?;
    if document.format_version != BACKUP_FORMAT_VERSION {
        return Err(RegionalBackupError::InvalidArtifact(format!(
            "unsupported format version {}",
            document.format_version
        )));
    }
    if document.captured_at_ms == 0
        || document.epoch_version.is_empty()
        || document.files.is_empty()
        || document.files.len() > MAX_FILE_COUNT
    {
        return Err(RegionalBackupError::InvalidArtifact(
            "required manifest fields are missing or out of bounds".into(),
        ));
    }
    let canonical = serde_json::to_vec(&document)
        .map_err(|error| RegionalBackupError::InvalidArtifact(error.to_string()))?;
    if canonical != encoded {
        return Err(RegionalBackupError::InvalidArtifact(
            "artifact encoding is not canonical".into(),
        ));
    }
    if document.manifest_sha256.len() != 64
        || document_digest(&document)? != document.manifest_sha256
    {
        return Err(RegionalBackupError::InvalidArtifact(
            "manifest digest does not match".into(),
        ));
    }
    let mut previous = None;
    let mut total = 0_u64;
    for file in &document.files {
        validate_relative_path(&file.path)?;
        if previous
            .as_deref()
            .is_some_and(|path| path >= file.path.as_str())
        {
            return Err(RegionalBackupError::InvalidArtifact(
                "file paths are not unique and strictly sorted".into(),
            ));
        }
        previous = Some(file.path.clone());
        if file.size_bytes > MAX_FILE_BYTES {
            return Err(RegionalBackupError::InvalidArtifact(
                "file size exceeds the artifact limit".into(),
            ));
        }
        let data = BASE64.decode(&file.data_base64).map_err(|error| {
            RegionalBackupError::InvalidArtifact(format!("file payload is not base64: {error}"))
        })?;
        if data.len() as u64 != file.size_bytes || hex_digest(&data) != file.sha256 {
            return Err(RegionalBackupError::InvalidArtifact(format!(
                "file {} size or checksum does not match",
                file.path
            )));
        }
        total = total
            .checked_add(file.size_bytes)
            .filter(|value| *value <= MAX_BACKUP_BYTES)
            .ok_or_else(|| {
                RegionalBackupError::InvalidArtifact(
                    "uncompressed backup exceeds the artifact limit".into(),
                )
            })?;
    }
    Ok((document, total))
}

fn collect_files(
    root: &Path,
    directory: &Path,
    output: &mut Vec<PathBuf>,
) -> Result<(), RegionalBackupError> {
    let mut entries = fs::read_dir(directory)
        .map_err(|error| storage_error("read backup directory", error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| storage_error("read backup entry", error))?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        let kind = entry
            .file_type()
            .map_err(|error| storage_error("inspect backup entry", error))?;
        if kind.is_symlink() {
            return Err(RegionalBackupError::InvalidInput(format!(
                "symbolic links are not allowed in backups: {}",
                path.display()
            )));
        }
        if kind.is_dir() {
            collect_files(root, &path, output)?;
        } else if kind.is_file() {
            output.push(
                path.strip_prefix(root)
                    .map_err(|error| RegionalBackupError::InvalidInput(error.to_string()))?
                    .to_path_buf(),
            );
        } else {
            return Err(RegionalBackupError::InvalidInput(format!(
                "special files are not allowed in backups: {}",
                path.display()
            )));
        }
    }
    Ok(())
}

fn restore_files(root: &Path, files: &[BackupFile]) -> Result<(), RegionalBackupError> {
    for file in files {
        validate_relative_path(&file.path)?;
        let destination = root.join(&file.path);
        let parent = destination.parent().ok_or_else(|| {
            RegionalBackupError::InvalidArtifact("file path has no parent".into())
        })?;
        fs::create_dir_all(parent)
            .map_err(|error| storage_error("create restored directory", error))?;
        let data = BASE64.decode(&file.data_base64).map_err(|error| {
            RegionalBackupError::InvalidArtifact(format!("file payload is not base64: {error}"))
        })?;
        let mut output = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&destination)
            .map_err(|error| storage_error("create restored file", error))?;
        output
            .write_all(&data)
            .and_then(|()| output.sync_all())
            .map_err(|error| storage_error("write restored file", error))?;
    }
    Ok(())
}

fn validate_fresh_destination(destination: &Path) -> Result<(), RegionalBackupError> {
    if !destination.exists() {
        return Ok(());
    }
    if !destination.is_dir() {
        return Err(RegionalBackupError::InvalidInput(
            "restore destination exists and is not a directory".into(),
        ));
    }
    if fs::read_dir(destination)
        .map_err(|error| storage_error("inspect restore destination", error))?
        .next()
        .is_some()
    {
        return Err(RegionalBackupError::InvalidInput(
            "restore destination must be empty".into(),
        ));
    }
    Ok(())
}

fn validate_relative_path(path: &str) -> Result<(), RegionalBackupError> {
    if path.is_empty()
        || Path::new(path)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(RegionalBackupError::InvalidArtifact(format!(
            "unsafe backup path {path:?}"
        )));
    }
    Ok(())
}

fn portable_path(path: &Path) -> Result<String, RegionalBackupError> {
    let mut segments = Vec::new();
    for component in path.components() {
        let Component::Normal(segment) = component else {
            return Err(RegionalBackupError::InvalidInput(format!(
                "unsafe source path {}",
                path.display()
            )));
        };
        segments.push(segment.to_str().ok_or_else(|| {
            RegionalBackupError::InvalidInput("backup paths must be valid UTF-8".into())
        })?);
    }
    Ok(segments.join("/"))
}

fn document_digest(document: &RegionalBackupDocument) -> Result<String, RegionalBackupError> {
    let mut unsigned = document.clone();
    unsigned.manifest_sha256.clear();
    serde_json::to_vec(&unsigned)
        .map(|encoded| hex_digest(&encoded))
        .map_err(|error| RegionalBackupError::InvalidArtifact(error.to_string()))
}

fn hex_digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut encoded, byte| {
            write!(&mut encoded, "{byte:02x}")
                .expect("writing hexadecimal into String cannot fail");
            encoded
        })
}

fn metadata(document: &RegionalBackupDocument, uncompressed_bytes: u64) -> RegionalBackupMetadata {
    RegionalBackupMetadata {
        format_version: document.format_version,
        epoch_version: document.epoch_version.clone(),
        captured_at_ms: document.captured_at_ms,
        node_id: document.node_id,
        file_count: document.files.len(),
        uncompressed_bytes,
        manifest_sha256: document.manifest_sha256.clone(),
    }
}

fn read_artifact(path: &Path) -> Result<Vec<u8>, RegionalBackupError> {
    let metadata = fs::metadata(path).map_err(|error| storage_error("inspect artifact", error))?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_BACKUP_BYTES * 2 {
        return Err(RegionalBackupError::InvalidArtifact(
            "artifact must be a bounded non-empty regular file".into(),
        ));
    }
    read_bounded_file(path, metadata.len())
}

fn read_bounded_file(path: &Path, expected: u64) -> Result<Vec<u8>, RegionalBackupError> {
    let capacity = usize::try_from(expected)
        .map_err(|_| RegionalBackupError::InvalidInput("file size cannot be represented".into()))?;
    let mut data = Vec::with_capacity(capacity);
    File::open(path)
        .and_then(|input| input.take(expected + 1).read_to_end(&mut data))
        .map_err(|error| storage_error("read backup file", error))?;
    if data.len() as u64 != expected {
        return Err(RegionalBackupError::Storage(format!(
            "file {} changed while it was being read",
            path.display()
        )));
    }
    Ok(data)
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), RegionalBackupError> {
    let parent = path
        .parent()
        .ok_or_else(|| RegionalBackupError::InvalidInput("backup output has no parent".into()))?;
    fs::create_dir_all(parent).map_err(|error| storage_error("create backup parent", error))?;
    let name = path.file_name().ok_or_else(|| {
        RegionalBackupError::InvalidInput("backup output has no file name".into())
    })?;
    let temporary = parent.join(format!(
        ".{}.epoch-partial-{}",
        name.to_string_lossy(),
        std::process::id()
    ));
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| storage_error("create backup staging file", error))?;
    let write_result = output.write_all(bytes).and_then(|()| output.sync_all());
    if let Err(error) = write_result {
        let _ = fs::remove_file(&temporary);
        return Err(storage_error("write backup artifact", error));
    }
    if path.exists() {
        let _ = fs::remove_file(&temporary);
        return Err(RegionalBackupError::InvalidInput(format!(
            "backup output {} already exists",
            path.display()
        )));
    }
    fs::rename(&temporary, path)
        .map_err(|error| storage_error("publish backup artifact", error))?;
    sync_directory(parent)
}

fn reject_output_inside_source(source: &Path, output: &Path) -> Result<(), RegionalBackupError> {
    let parent = output
        .parent()
        .ok_or_else(|| RegionalBackupError::InvalidInput("backup output has no parent".into()))?;
    let canonical_parent = parent
        .canonicalize()
        .map_err(|error| storage_error("canonicalize backup output parent", error))?;
    if canonical_parent.starts_with(source) {
        return Err(RegionalBackupError::InvalidInput(
            "backup output must be outside the data directory".into(),
        ));
    }
    Ok(())
}

fn sync_directory_tree(root: &Path) -> Result<(), RegionalBackupError> {
    let mut directories = vec![root.to_path_buf()];
    let mut index = 0;
    while index < directories.len() {
        for entry in fs::read_dir(&directories[index])
            .map_err(|error| storage_error("inspect restore staging", error))?
        {
            let entry = entry.map_err(|error| storage_error("inspect restore entry", error))?;
            if entry
                .file_type()
                .map_err(|error| storage_error("inspect restored file type", error))?
                .is_dir()
            {
                directories.push(entry.path());
            }
        }
        index += 1;
    }
    for directory in directories.into_iter().rev() {
        sync_directory(&directory)?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), RegionalBackupError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| storage_error("sync directory", error))
}

fn storage_error(operation: &str, error: impl std::fmt::Display) -> RegionalBackupError {
    RegionalBackupError::Storage(format!("{operation}: {error}"))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn backup_is_canonical_tamper_evident_and_restores_atomically() {
        let workspace = TempDir::new().unwrap();
        let source = workspace.path().join("source");
        fs::create_dir_all(source.join("consensus/group-1")).unwrap();
        fs::write(source.join("engine.wal"), b"activation").unwrap();
        fs::write(source.join("consensus/group-1/node-1.wal"), b"catalog").unwrap();
        let artifact = workspace.path().join("node-1.epoch-backup.json");

        let created = create_backup(&source, &artifact, 42, Some(1)).unwrap();
        assert_eq!(created.file_count, 2);
        assert_eq!(inspect_backup(&artifact).unwrap(), created);

        let restored = workspace.path().join("restored");
        assert_eq!(restore_backup(&artifact, &restored).unwrap(), created);
        assert_eq!(
            fs::read(restored.join("engine.wal")).unwrap(),
            b"activation"
        );
        assert_eq!(
            fs::read(restored.join("consensus/group-1/node-1.wal")).unwrap(),
            b"catalog"
        );

        let mut corrupt = fs::read(&artifact).unwrap();
        let index = corrupt.iter().position(|byte| *byte == b'Y').unwrap();
        corrupt[index] = b'Z';
        let corrupt_path = workspace.path().join("corrupt.json");
        fs::write(&corrupt_path, corrupt).unwrap();
        assert!(inspect_backup(&corrupt_path).is_err());
    }

    #[test]
    fn restore_rejects_non_empty_destinations_and_unsafe_manifest_paths() {
        let workspace = TempDir::new().unwrap();
        let source = workspace.path().join("source");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("state.wal"), b"state").unwrap();
        let artifact = workspace.path().join("backup.json");
        create_backup(&source, &artifact, 42, None).unwrap();

        let destination = workspace.path().join("destination");
        fs::create_dir(&destination).unwrap();
        fs::write(destination.join("existing"), b"do not overwrite").unwrap();
        assert!(restore_backup(&artifact, &destination).is_err());
        assert_eq!(
            fs::read(destination.join("existing")).unwrap(),
            b"do not overwrite"
        );

        let encoded = fs::read(&artifact).unwrap();
        let mut document: RegionalBackupDocument = serde_json::from_slice(&encoded).unwrap();
        document.files[0].path = "../escape".into();
        document.manifest_sha256 = document_digest(&document).unwrap();
        let unsafe_path = workspace.path().join("unsafe.json");
        fs::write(&unsafe_path, serde_json::to_vec(&document).unwrap()).unwrap();
        assert!(restore_backup(&unsafe_path, &workspace.path().join("fresh")).is_err());
        assert!(!workspace.path().join("escape").exists());
    }

    #[test]
    fn backup_rejects_empty_sources_and_outputs_inside_the_source() {
        let workspace = TempDir::new().unwrap();
        let source = workspace.path().join("source");
        fs::create_dir(&source).unwrap();
        assert!(create_backup(&source, &workspace.path().join("empty.json"), 1, None).is_err());
        fs::write(source.join("state.wal"), b"state").unwrap();
        assert!(create_backup(&source, &source.join("backup.json"), 1, None).is_err());
    }

    #[test]
    fn backup_rejects_a_live_epoch_volume() {
        let workspace = TempDir::new().unwrap();
        let source = workspace.path().join("source");
        fs::create_dir(&source).unwrap();
        let journal_path = source.join("node-1.wal");
        fs::write(&journal_path, b"live state").unwrap();
        let live_journal = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&journal_path)
            .unwrap();
        live_journal.try_lock().unwrap();

        let error =
            create_backup(&source, &workspace.path().join("backup.json"), 1, Some(1)).unwrap_err();
        assert!(error.to_string().contains("data directory is active"));
    }

    #[test]
    fn three_voter_backup_set_verifies_every_artifact_and_restores_by_node() {
        let workspace = TempDir::new().unwrap();
        let mut artifacts = Vec::new();
        for node_id in 1..=3 {
            let source = workspace.path().join(format!("source-{node_id}"));
            fs::create_dir(&source).unwrap();
            fs::write(
                source.join(format!("node-{node_id}.wal")),
                format!("state-{node_id}"),
            )
            .unwrap();
            let artifact = workspace.path().join(format!("node-{node_id}.json"));
            create_backup(&source, &artifact, 100 + node_id, Some(node_id)).unwrap();
            artifacts.push(artifact);
        }
        let manifest = workspace.path().join("regional-set.json");
        let created = create_backup_set(&artifacts, &manifest).unwrap();
        assert_eq!(created.node_ids, vec![1, 2, 3]);
        assert_eq!(inspect_backup_set(&manifest).unwrap(), created);

        let restored = workspace.path().join("restored-node-2");
        let metadata = restore_backup_set_node(&manifest, 2, &restored).unwrap();
        assert_eq!(metadata.node_id, Some(2));
        assert_eq!(fs::read(restored.join("node-2.wal")).unwrap(), b"state-2");

        fs::write(&artifacts[2], b"tampered").unwrap();
        assert!(restore_backup_set_node(&manifest, 3, &workspace.path().join("node-3")).is_err());
    }
}

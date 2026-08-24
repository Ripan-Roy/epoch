//! Authenticated, bounded encryption for portable regional backup artifacts.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    fs::{self, File, OpenOptions},
    io::Write as _,
    path::{Path, PathBuf},
};

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD as BASE64};
use ring::{
    aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey},
    rand::{SecureRandom as _, SystemRandom},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::regional_backup_api::RegionalBackupArtifact;

const ENCRYPTED_BACKUP_MAGIC: [u8; 8] = *b"EPBKAE01";
const ENCRYPTED_BACKUP_FORMAT_VERSION: u16 = 1;
const ENCRYPTED_BACKUP_ALGORITHM: &str = "AES-256-GCM";
const NONCE_BYTES: usize = 12;
const TAG_BYTES: usize = 16;
const PREFIX_BYTES: usize = 12;
const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_ENCRYPTED_BACKUP_BYTES: u64 = 160 * 1024 * 1024;
const MAX_KEY_FILE_BYTES: u64 = 4 * 1024;
const MAX_KEY_ID_BYTES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EncryptedBackupHeader {
    format_version: u16,
    algorithm: String,
    key_id: String,
    created_at_ms: u64,
    nonce_base64: String,
    plaintext_bytes: u64,
    plaintext_sha256: String,
    manifest_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EncryptedBackupMetadata {
    pub format_version: u16,
    pub algorithm: String,
    pub key_id: String,
    pub created_at_ms: u64,
    pub plaintext_bytes: u64,
    pub plaintext_sha256: String,
    pub manifest_sha256: String,
    pub object_name: String,
}

#[derive(Debug, Error)]
pub enum EncryptedBackupError {
    #[error("encrypted backup input is invalid: {0}")]
    InvalidInput(String),
    #[error("encrypted backup artifact is invalid: {0}")]
    InvalidArtifact(String),
    #[error("encrypted backup authentication failed")]
    Authentication,
    #[error("encrypted backup storage operation failed: {0}")]
    Storage(String),
    #[error("regional backup payload is invalid: {0}")]
    RegionalArtifact(String),
}

pub fn load_encryption_key(path: &Path) -> Result<[u8; 32], EncryptedBackupError> {
    let metadata = fs::metadata(path).map_err(|error| storage("inspect encryption key", error))?;
    if !metadata.is_file() || metadata.len() != 32 || metadata.len() > MAX_KEY_FILE_BYTES {
        return Err(EncryptedBackupError::InvalidInput(
            "encryption key must be a regular file containing exactly 32 raw bytes".into(),
        ));
    }
    let encoded = fs::read(path).map_err(|error| storage("read encryption key", error))?;
    encoded.try_into().map_err(|_| {
        EncryptedBackupError::InvalidInput(
            "encryption key must contain exactly 32 raw bytes".into(),
        )
    })
}

pub fn load_retention_keyring(
    active_key_path: &Path,
    active_key_id: &str,
) -> Result<BTreeMap<String, [u8; 32]>, EncryptedBackupError> {
    validate_key_id(active_key_id)?;
    let active_key = load_encryption_key(active_key_path)?;
    let directory = active_key_path.parent().ok_or_else(|| {
        EncryptedBackupError::InvalidInput("encryption key path has no parent directory".into())
    })?;
    let mut keys = BTreeMap::from([(active_key_id.to_owned(), active_key)]);
    for entry in fs::read_dir(directory).map_err(|error| storage("read key directory", error))? {
        let entry = entry.map_err(|error| storage("read key directory entry", error))?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            return Err(EncryptedBackupError::InvalidInput(
                "key directory contains a non-UTF-8 entry".into(),
            ));
        };
        let Some(key_id) = name.strip_prefix("previous.") else {
            continue;
        };
        validate_key_id(key_id)?;
        let key = load_encryption_key(&entry.path())?;
        if keys.contains_key(key_id) || keys.values().any(|candidate| candidate == &key) {
            return Err(EncryptedBackupError::InvalidInput(format!(
                "backup keyring contains duplicate key ID or key material for {key_id}"
            )));
        }
        keys.insert(key_id.to_owned(), key);
    }
    Ok(keys)
}

pub fn encrypt_backup(
    artifact: &RegionalBackupArtifact,
    key: &[u8; 32],
    key_id: &str,
    created_at_ms: u64,
    output: &Path,
) -> Result<EncryptedBackupMetadata, EncryptedBackupError> {
    validate_key_id(key_id)?;
    if created_at_ms == 0 {
        return Err(EncryptedBackupError::InvalidInput(
            "encrypted backup creation time must be non-zero".into(),
        ));
    }
    let plaintext = artifact
        .encode()
        .map_err(|error| EncryptedBackupError::RegionalArtifact(error.to_string()))?;
    let encoded = encrypt_plaintext(
        &plaintext,
        key,
        key_id,
        created_at_ms,
        &artifact.manifest_sha256,
    )?;
    atomic_write(output, &encoded)?;
    metadata_from_header(&decode_envelope(&encoded)?.0, output)
}

pub fn decrypt_backup(
    input: &Path,
    key: &[u8; 32],
    output: &Path,
) -> Result<RegionalBackupArtifact, EncryptedBackupError> {
    let encoded = read_encrypted(input)?;
    let plaintext = decrypt_plaintext(&encoded, key)?;
    let artifact = RegionalBackupArtifact::decode(&plaintext)
        .map_err(|error| EncryptedBackupError::RegionalArtifact(error.to_string()))?;
    let (header, _, _) = decode_envelope(&encoded)?;
    if artifact.manifest_sha256 != header.manifest_sha256 {
        return Err(EncryptedBackupError::InvalidArtifact(
            "encrypted header does not match the regional manifest".into(),
        ));
    }
    atomic_write(output, &plaintext)?;
    Ok(artifact)
}

pub fn inspect_encrypted_backup(
    input: &Path,
) -> Result<EncryptedBackupMetadata, EncryptedBackupError> {
    let encoded = read_encrypted(input)?;
    let (header, _, ciphertext) = decode_envelope(&encoded)?;
    let expected = usize::try_from(header.plaintext_bytes)
        .map_err(|_| EncryptedBackupError::InvalidArtifact("plaintext size exceeds usize".into()))?
        .checked_add(TAG_BYTES)
        .ok_or_else(|| {
            EncryptedBackupError::InvalidArtifact("encrypted size overflows usize".into())
        })?;
    if ciphertext.len() != expected {
        return Err(EncryptedBackupError::InvalidArtifact(
            "encrypted payload length does not match its header".into(),
        ));
    }
    metadata_from_header(&header, input)
}

pub fn enforce_encrypted_retention(
    directory: &Path,
    retain: usize,
    keyring: &BTreeMap<String, [u8; 32]>,
) -> Result<usize, EncryptedBackupError> {
    if retain == 0 || retain > 10_000 || !directory.is_dir() || keyring.is_empty() {
        return Err(EncryptedBackupError::InvalidInput(
            "retention requires an existing directory, at least one key, and a count between 1 and 10000"
                .into(),
        ));
    }
    let mut objects = Vec::<(EncryptedBackupMetadata, PathBuf)>::new();
    for entry in fs::read_dir(directory).map_err(|error| storage("read backup directory", error))? {
        let entry = entry.map_err(|error| storage("read backup directory entry", error))?;
        let file_type = entry
            .file_type()
            .map_err(|error| storage("inspect backup directory entry", error))?;
        let path = entry.path();
        let is_backup = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".epoch-backup.enc"));
        if !is_backup {
            continue;
        }
        if !file_type.is_file() || file_type.is_symlink() {
            return Err(EncryptedBackupError::InvalidArtifact(format!(
                "backup retention refuses non-regular object {}",
                path.display()
            )));
        }
        let encoded = read_encrypted(&path)?;
        let (header, _, _) = decode_envelope(&encoded)?;
        let key = keyring.get(&header.key_id).ok_or_else(|| {
            EncryptedBackupError::InvalidInput(format!(
                "retention keyring does not contain key ID {} required by {}",
                header.key_id,
                path.display()
            ))
        })?;
        let _ = decrypt_plaintext(&encoded, key)?;
        objects.push((metadata_from_header(&header, &path)?, path));
    }
    objects.sort_by(|left, right| {
        right
            .0
            .created_at_ms
            .cmp(&left.0.created_at_ms)
            .then_with(|| right.0.object_name.cmp(&left.0.object_name))
    });
    for (_, path) in objects.iter().skip(retain) {
        fs::remove_file(path).map_err(|error| storage("remove expired backup", error))?;
    }
    sync_directory(directory)?;
    Ok(objects.len().min(retain))
}

fn encrypt_plaintext(
    plaintext: &[u8],
    key: &[u8; 32],
    key_id: &str,
    created_at_ms: u64,
    manifest_sha256: &str,
) -> Result<Vec<u8>, EncryptedBackupError> {
    let mut nonce = [0_u8; NONCE_BYTES];
    SystemRandom::new()
        .fill(&mut nonce)
        .map_err(|_| EncryptedBackupError::Authentication)?;
    let header = EncryptedBackupHeader {
        format_version: ENCRYPTED_BACKUP_FORMAT_VERSION,
        algorithm: ENCRYPTED_BACKUP_ALGORITHM.into(),
        key_id: key_id.into(),
        created_at_ms,
        nonce_base64: BASE64.encode(nonce),
        plaintext_bytes: u64::try_from(plaintext.len()).map_err(|_| {
            EncryptedBackupError::InvalidInput("regional backup size exceeds u64".into())
        })?,
        plaintext_sha256: hex_digest(plaintext),
        manifest_sha256: manifest_sha256.into(),
    };
    validate_header(&header)?;
    let header_bytes = serde_json::to_vec(&header)
        .map_err(|error| EncryptedBackupError::InvalidArtifact(error.to_string()))?;
    let header_length = u32::try_from(header_bytes.len()).map_err(|_| {
        EncryptedBackupError::InvalidArtifact("encrypted backup header is too large".into())
    })?;
    let mut output = Vec::with_capacity(
        PREFIX_BYTES
            .saturating_add(header_bytes.len())
            .saturating_add(plaintext.len())
            .saturating_add(TAG_BYTES),
    );
    output.extend_from_slice(&ENCRYPTED_BACKUP_MAGIC);
    output.extend_from_slice(&header_length.to_be_bytes());
    output.extend_from_slice(&header_bytes);
    let aad_length = output.len();
    let mut ciphertext = plaintext.to_vec();
    cipher(key)?
        .seal_in_place_append_tag(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(&output[..aad_length]),
            &mut ciphertext,
        )
        .map_err(|_| EncryptedBackupError::Authentication)?;
    output.extend_from_slice(&ciphertext);
    Ok(output)
}

fn decrypt_plaintext(encoded: &[u8], key: &[u8; 32]) -> Result<Vec<u8>, EncryptedBackupError> {
    let (header, aad, ciphertext) = decode_envelope(encoded)?;
    let nonce = BASE64
        .decode(&header.nonce_base64)
        .map_err(|error| EncryptedBackupError::InvalidArtifact(error.to_string()))?
        .try_into()
        .map_err(|_| EncryptedBackupError::InvalidArtifact("nonce has invalid length".into()))?;
    let mut plaintext = ciphertext.to_vec();
    let opened = cipher(key)?
        .open_in_place(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(aad),
            &mut plaintext,
        )
        .map_err(|_| EncryptedBackupError::Authentication)?;
    let plaintext = opened.to_vec();
    if plaintext.len()
        != usize::try_from(header.plaintext_bytes).map_err(|_| {
            EncryptedBackupError::InvalidArtifact("plaintext size exceeds usize".into())
        })?
        || hex_digest(&plaintext) != header.plaintext_sha256
    {
        return Err(EncryptedBackupError::InvalidArtifact(
            "decrypted payload length or digest does not match".into(),
        ));
    }
    Ok(plaintext)
}

fn decode_envelope(
    encoded: &[u8],
) -> Result<(EncryptedBackupHeader, &[u8], &[u8]), EncryptedBackupError> {
    if encoded.len() < PREFIX_BYTES.saturating_add(TAG_BYTES)
        || encoded[..8] != ENCRYPTED_BACKUP_MAGIC
    {
        return Err(EncryptedBackupError::InvalidArtifact(
            "encrypted backup prefix is invalid".into(),
        ));
    }
    let header_length = u32::from_be_bytes(encoded[8..12].try_into().map_err(|_| {
        EncryptedBackupError::InvalidArtifact("encrypted backup prefix is truncated".into())
    })?) as usize;
    if header_length == 0 || header_length > MAX_HEADER_BYTES {
        return Err(EncryptedBackupError::InvalidArtifact(
            "encrypted backup header size is invalid".into(),
        ));
    }
    let payload_offset = PREFIX_BYTES.checked_add(header_length).ok_or_else(|| {
        EncryptedBackupError::InvalidArtifact("encrypted backup header offset overflows".into())
    })?;
    if payload_offset.saturating_add(TAG_BYTES) > encoded.len() {
        return Err(EncryptedBackupError::InvalidArtifact(
            "encrypted backup is truncated".into(),
        ));
    }
    let header: EncryptedBackupHeader =
        serde_json::from_slice(&encoded[PREFIX_BYTES..payload_offset])
            .map_err(|error| EncryptedBackupError::InvalidArtifact(error.to_string()))?;
    if serde_json::to_vec(&header)
        .map_err(|error| EncryptedBackupError::InvalidArtifact(error.to_string()))?
        != encoded[PREFIX_BYTES..payload_offset]
    {
        return Err(EncryptedBackupError::InvalidArtifact(
            "encrypted backup header is not canonical".into(),
        ));
    }
    validate_header(&header)?;
    Ok((
        header,
        &encoded[..payload_offset],
        &encoded[payload_offset..],
    ))
}

fn validate_header(header: &EncryptedBackupHeader) -> Result<(), EncryptedBackupError> {
    validate_key_id(&header.key_id)?;
    if header.format_version != ENCRYPTED_BACKUP_FORMAT_VERSION
        || header.algorithm != ENCRYPTED_BACKUP_ALGORITHM
        || header.created_at_ms == 0
        || header.plaintext_bytes == 0
        || header.plaintext_sha256.len() != 64
        || header.manifest_sha256.len() != 64
        || !BASE64
            .decode(&header.nonce_base64)
            .is_ok_and(|nonce| nonce.len() == NONCE_BYTES)
    {
        return Err(EncryptedBackupError::InvalidArtifact(
            "encrypted backup header values are invalid".into(),
        ));
    }
    Ok(())
}

fn validate_key_id(key_id: &str) -> Result<(), EncryptedBackupError> {
    if key_id.is_empty()
        || key_id.len() > MAX_KEY_ID_BYTES
        || !key_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(EncryptedBackupError::InvalidInput(format!(
            "key ID must be a 1-{MAX_KEY_ID_BYTES} byte safe identifier"
        )));
    }
    Ok(())
}

fn cipher(key: &[u8; 32]) -> Result<LessSafeKey, EncryptedBackupError> {
    UnboundKey::new(&AES_256_GCM, key)
        .map(LessSafeKey::new)
        .map_err(|_| EncryptedBackupError::InvalidInput("AES-256-GCM key is invalid".into()))
}

fn read_encrypted(path: &Path) -> Result<Vec<u8>, EncryptedBackupError> {
    let metadata =
        fs::metadata(path).map_err(|error| storage("inspect encrypted backup", error))?;
    if !metadata.is_file()
        || metadata.len() < u64::try_from(PREFIX_BYTES + TAG_BYTES).unwrap()
        || metadata.len() > MAX_ENCRYPTED_BACKUP_BYTES
    {
        return Err(EncryptedBackupError::InvalidArtifact(
            "encrypted backup must be a bounded non-empty regular file".into(),
        ));
    }
    fs::read(path).map_err(|error| storage("read encrypted backup", error))
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), EncryptedBackupError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| EncryptedBackupError::InvalidInput("output has no parent".into()))?;
    if !parent.is_dir() {
        return Err(EncryptedBackupError::InvalidInput(format!(
            "output parent {} is not a directory",
            parent.display()
        )));
    }
    let name = path
        .file_name()
        .ok_or_else(|| EncryptedBackupError::InvalidInput("output has no file name".into()))?;
    let staging = parent.join(format!(
        ".{}.epoch-encrypted-staging-{}",
        name.to_string_lossy(),
        std::process::id()
    ));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&staging)
        .map_err(|error| storage("create encrypted backup staging file", error))?;
    let publish = (|| {
        file.write_all(bytes)
            .map_err(|error| storage("write encrypted backup", error))?;
        file.sync_all()
            .map_err(|error| storage("sync encrypted backup", error))?;
        drop(file);
        fs::hard_link(&staging, path)
            .map_err(|error| storage("publish encrypted backup", error))?;
        sync_directory(parent)?;
        fs::remove_file(&staging)
            .map_err(|error| storage("remove encrypted backup staging file", error))?;
        sync_directory(parent)
    })();
    if publish.is_err() {
        let _ = fs::remove_file(staging);
    }
    publish
}

fn sync_directory(path: &Path) -> Result<(), EncryptedBackupError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| storage("sync output directory", error))
}

fn metadata_from_header(
    header: &EncryptedBackupHeader,
    path: &Path,
) -> Result<EncryptedBackupMetadata, EncryptedBackupError> {
    let object_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| EncryptedBackupError::InvalidInput("object name is not UTF-8".into()))?;
    Ok(EncryptedBackupMetadata {
        format_version: header.format_version,
        algorithm: header.algorithm.clone(),
        key_id: header.key_id.clone(),
        created_at_ms: header.created_at_ms,
        plaintext_bytes: header.plaintext_bytes,
        plaintext_sha256: header.plaintext_sha256.clone(),
        manifest_sha256: header.manifest_sha256.clone(),
        object_name: object_name.into(),
    })
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

fn storage(operation: &str, error: impl std::fmt::Display) -> EncryptedBackupError {
    EncryptedBackupError::Storage(format!("{operation}: {error}"))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn authenticated_envelope_round_trips_and_rejects_tampering() {
        let key = [7_u8; 32];
        let plaintext = br#"{"format_version":1,"manifest_sha256":"fixture"}"#;
        let encoded =
            encrypt_plaintext(plaintext, &key, "key-2026-08", 42, &"a".repeat(64)).unwrap();
        assert_eq!(decrypt_plaintext(&encoded, &key).unwrap(), plaintext);
        assert!(decrypt_plaintext(&encoded, &[8_u8; 32]).is_err());

        let mut tampered = encoded;
        let last = tampered.len() - 1;
        tampered[last] ^= 0x01;
        assert!(decrypt_plaintext(&tampered, &key).is_err());
    }

    #[test]
    fn key_loading_and_atomic_publication_fail_closed() {
        let directory = TempDir::new().unwrap();
        let key_path = directory.path().join("key");
        fs::write(&key_path, [9_u8; 32]).unwrap();
        assert_eq!(load_encryption_key(&key_path).unwrap(), [9_u8; 32]);
        fs::write(directory.path().join("previous.key-2026-07"), [8_u8; 32]).unwrap();
        assert_eq!(
            load_retention_keyring(&key_path, "key-2026-08").unwrap(),
            BTreeMap::from([
                ("key-2026-07".into(), [8_u8; 32]),
                ("key-2026-08".into(), [9_u8; 32]),
            ])
        );
        fs::write(&key_path, [9_u8; 31]).unwrap();
        assert!(load_encryption_key(&key_path).is_err());

        let output = directory.path().join("artifact.enc");
        atomic_write(&output, b"first").unwrap();
        assert!(atomic_write(&output, b"overwrite").is_err());
        assert_eq!(fs::read(output).unwrap(), b"first");
    }

    #[test]
    fn retention_keeps_the_newest_authenticated_envelopes_across_key_rotation() {
        let directory = TempDir::new().unwrap();
        let active_key = [3_u8; 32];
        let previous_key = [4_u8; 32];
        for created_at_ms in 1..=3 {
            let (key_id, key) = if created_at_ms == 1 {
                ("rotation-1", &previous_key)
            } else {
                ("rotation-2", &active_key)
            };
            let encoded =
                encrypt_plaintext(b"payload", key, key_id, created_at_ms, &"c".repeat(64)).unwrap();
            fs::write(
                directory
                    .path()
                    .join(format!("{created_at_ms}.epoch-backup.enc")),
                encoded,
            )
            .unwrap();
        }
        let incomplete = BTreeMap::from([("rotation-2".to_owned(), active_key)]);
        assert!(enforce_encrypted_retention(directory.path(), 2, &incomplete).is_err());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 3);

        let keyring = BTreeMap::from([
            ("rotation-1".to_owned(), previous_key),
            ("rotation-2".to_owned(), active_key),
        ]);
        assert_eq!(
            enforce_encrypted_retention(directory.path(), 2, &keyring).unwrap(),
            2
        );
        assert!(!directory.path().join("1.epoch-backup.enc").exists());
        assert!(directory.path().join("2.epoch-backup.enc").exists());
        assert!(directory.path().join("3.epoch-backup.enc").exists());
    }
}

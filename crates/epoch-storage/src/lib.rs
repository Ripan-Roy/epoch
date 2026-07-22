//! Explicitly versioned, checksummed append-only storage.
//!
//! The format here is the Phase-0 local WAL, not a frozen production format.
//! It deliberately avoids Rust-specific object serialization so recovery and
//! migrations can be implemented in other versions and languages.

use std::{
    fs::{File, OpenOptions},
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crc32fast::Hasher;
use epoch_core::{EpochError, EpochResult};

const MAGIC: [u8; 4] = *b"EPCH";
const FORMAT_VERSION: u16 = 1;
const HEADER_LEN: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRecord {
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub payload: Vec<u8>,
}

pub trait CommitLog: std::fmt::Debug + Send {
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
    records: Vec<LogRecord>,
    recovered_partial_tail: bool,
}

impl FileWal {
    pub fn open(path: impl AsRef<Path>) -> EpochResult<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(storage_error)?;
        let (records, valid_len, recovered_partial_tail) = recover(&mut file)?;
        if recovered_partial_tail {
            file.set_len(valid_len).map_err(storage_error)?;
        }
        file.seek(SeekFrom::End(0)).map_err(storage_error)?;
        Ok(Self {
            path,
            file,
            records,
            recovered_partial_tail,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub const fn recovered_partial_tail(&self) -> bool {
        self.recovered_partial_tail
    }

    pub fn sync(&self) -> EpochResult<()> {
        self.file.sync_data().map_err(storage_error)
    }
}

impl CommitLog for FileWal {
    fn append(
        &mut self,
        timestamp_ms: u64,
        payload: &[u8],
        durable: bool,
    ) -> EpochResult<LogRecord> {
        let sequence = self
            .records
            .last()
            .map_or(0, |record| record.sequence.saturating_add(1));
        let payload_len = u32::try_from(payload.len())
            .map_err(|_| EpochError::Capacity("WAL record exceeds 4 GiB".into()))?;
        let mut header = [0_u8; HEADER_LEN];
        header[0..4].copy_from_slice(&MAGIC);
        header[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        header[6..8].copy_from_slice(&0_u16.to_le_bytes());
        header[8..16].copy_from_slice(&sequence.to_le_bytes());
        header[16..24].copy_from_slice(&timestamp_ms.to_le_bytes());
        header[24..28].copy_from_slice(&payload_len.to_le_bytes());
        let checksum = checksum(&header[4..28], payload);
        header[28..32].copy_from_slice(&checksum.to_le_bytes());

        self.file.write_all(&header).map_err(storage_error)?;
        self.file.write_all(payload).map_err(storage_error)?;
        if durable {
            self.file.sync_data().map_err(storage_error)?;
        } else {
            self.file.flush().map_err(storage_error)?;
        }

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

fn recover(file: &mut File) -> EpochResult<(Vec<LogRecord>, u64, bool)> {
    file.seek(SeekFrom::Start(0)).map_err(storage_error)?;
    let mut records = Vec::new();
    let mut valid_len = 0_u64;
    loop {
        let mut header = [0_u8; HEADER_LEN];
        match read_frame_part(file, &mut header) {
            Ok(ReadPart::End) => return Ok((records, valid_len, false)),
            Ok(ReadPart::Partial) => return Ok((records, valid_len, true)),
            Ok(ReadPart::Complete) => {}
            Err(error) => return Err(error),
        }

        if header[0..4] != MAGIC {
            return Err(EpochError::Storage(format!(
                "invalid WAL magic at byte {valid_len}"
            )));
        }
        let version = u16::from_le_bytes([header[4], header[5]]);
        if version != FORMAT_VERSION {
            return Err(EpochError::Storage(format!(
                "unsupported WAL version {version} at byte {valid_len}"
            )));
        }
        let sequence = u64::from_le_bytes(header[8..16].try_into().expect("fixed header"));
        let expected_sequence = u64::try_from(records.len()).unwrap_or(u64::MAX);
        if sequence != expected_sequence {
            return Err(EpochError::Storage(format!(
                "non-contiguous WAL sequence {sequence}; expected {expected_sequence}"
            )));
        }
        let timestamp_ms = u64::from_le_bytes(header[16..24].try_into().expect("fixed header"));
        let payload_len =
            u32::from_le_bytes(header[24..28].try_into().expect("fixed header")) as usize;
        let expected_checksum =
            u32::from_le_bytes(header[28..32].try_into().expect("fixed header"));
        let mut payload = vec![0_u8; payload_len];
        match read_frame_part(file, &mut payload)? {
            ReadPart::Complete => {}
            ReadPart::End | ReadPart::Partial => return Ok((records, valid_len, true)),
        }
        let actual_checksum = checksum(&header[4..28], &payload);
        if actual_checksum != expected_checksum {
            return Err(EpochError::Storage(format!(
                "WAL checksum mismatch for sequence {sequence}"
            )));
        }
        valid_len = valid_len
            .saturating_add(u64::try_from(HEADER_LEN).unwrap_or(u64::MAX))
            .saturating_add(u64::try_from(payload_len).unwrap_or(u64::MAX));
        records.push(LogRecord {
            sequence,
            timestamp_ms,
            payload,
        });
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

    use tempfile::NamedTempFile;

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
}

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::str;

const MAGIC: &[u8; 4] = b"EPTR";
const FORMAT_VERSION: u16 = 1;
const FORMAT_FLAGS: u16 = 0;
const MIN_ENCODED_EVENT_LEN: usize = 28;

/// One deterministic observation in a [`Trace`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TraceEvent {
    /// Zero-based event order.
    pub sequence: u64,
    /// Test-local monotonic time at which the event occurred.
    pub monotonic_ms: u64,
    /// Stable event name.
    pub kind: String,
    /// Event-specific canonical bytes.
    pub payload: Vec<u8>,
}

/// An ordered, byte-serializable record of a deterministic scenario.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Trace {
    events: Vec<TraceEvent>,
}

impl Trace {
    /// Creates an empty trace.
    pub const fn new() -> Self {
        Self { events: Vec::new() }
    }

    /// Returns the events in observation order.
    pub fn events(&self) -> &[TraceEvent] {
        &self.events
    }

    /// Appends an event and returns its sequence number.
    pub fn record(
        &mut self,
        monotonic_ms: u64,
        kind: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Result<u64, TraceError> {
        if let Some(previous) = self.events.last().map(|event| event.monotonic_ms)
            && monotonic_ms < previous
        {
            return Err(TraceError::NonMonotonicTime {
                previous,
                actual: monotonic_ms,
            });
        }
        let sequence = u64::try_from(self.events.len()).map_err(|_| TraceError::LengthOverflow)?;
        self.events.push(TraceEvent {
            sequence,
            monotonic_ms,
            kind: kind.into(),
            payload: payload.into(),
        });
        Ok(sequence)
    }

    /// Encodes this trace in the canonical little-endian EPTR version-1 format.
    pub fn to_bytes(&self) -> Result<Vec<u8>, TraceError> {
        let event_count =
            u64::try_from(self.events.len()).map_err(|_| TraceError::LengthOverflow)?;
        let mut output = Vec::new();
        output.extend_from_slice(MAGIC);
        output.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        output.extend_from_slice(&FORMAT_FLAGS.to_le_bytes());
        output.extend_from_slice(&event_count.to_le_bytes());

        for event in &self.events {
            let kind = event.kind.as_bytes();
            let kind_length = u32::try_from(kind.len()).map_err(|_| TraceError::LengthOverflow)?;
            let payload_length =
                u64::try_from(event.payload.len()).map_err(|_| TraceError::LengthOverflow)?;
            output.extend_from_slice(&event.sequence.to_le_bytes());
            output.extend_from_slice(&event.monotonic_ms.to_le_bytes());
            output.extend_from_slice(&kind_length.to_le_bytes());
            output.extend_from_slice(kind);
            output.extend_from_slice(&payload_length.to_le_bytes());
            output.extend_from_slice(&event.payload);
        }
        Ok(output)
    }

    /// Decodes and strictly validates canonical EPTR version-1 bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TraceError> {
        let mut reader = Reader::new(bytes);
        if reader.read_array::<4>()? != *MAGIC {
            return Err(TraceError::InvalidMagic);
        }
        let version = u16::from_le_bytes(reader.read_array()?);
        if version != FORMAT_VERSION {
            return Err(TraceError::UnsupportedVersion(version));
        }
        let flags = u16::from_le_bytes(reader.read_array()?);
        if flags != FORMAT_FLAGS {
            return Err(TraceError::UnsupportedFlags(flags));
        }

        let event_count = u64::from_le_bytes(reader.read_array()?);
        let capacity = usize::try_from(event_count).map_err(|_| TraceError::LengthOverflow)?;
        let maximum_encoded_events = reader.remaining_len() / MIN_ENCODED_EVENT_LEN;
        let mut events = Vec::with_capacity(capacity.min(maximum_encoded_events));
        let mut previous_monotonic_ms = None;
        for expected_sequence in 0..event_count {
            let sequence = u64::from_le_bytes(reader.read_array()?);
            if sequence != expected_sequence {
                return Err(TraceError::NonCanonicalSequence {
                    expected: expected_sequence,
                    actual: sequence,
                });
            }
            let monotonic_ms = u64::from_le_bytes(reader.read_array()?);
            if let Some(previous) = previous_monotonic_ms
                && monotonic_ms < previous
            {
                return Err(TraceError::NonMonotonicTime {
                    previous,
                    actual: monotonic_ms,
                });
            }
            let kind_length = u32::from_le_bytes(reader.read_array()?);
            let kind_bytes = reader.read_slice(
                usize::try_from(kind_length).map_err(|_| TraceError::LengthOverflow)?,
            )?;
            let kind = str::from_utf8(kind_bytes)
                .map_err(|_| TraceError::InvalidUtf8)?
                .to_owned();
            let payload_length = u64::from_le_bytes(reader.read_array()?);
            let payload = reader
                .read_slice(
                    usize::try_from(payload_length).map_err(|_| TraceError::LengthOverflow)?,
                )?
                .to_vec();
            events.push(TraceEvent {
                sequence,
                monotonic_ms,
                kind,
                payload,
            });
            previous_monotonic_ms = Some(monotonic_ms);
        }
        if !reader.is_finished() {
            return Err(TraceError::TrailingBytes);
        }
        Ok(Self { events })
    }

    /// Computes a stable FNV-1a checksum of the canonical serialized bytes.
    ///
    /// This is a history/equality digest, not a cryptographic authenticator.
    pub fn digest(&self) -> Result<TraceDigest, TraceError> {
        let mut value = 0xcbf2_9ce4_8422_2325_u64;
        for byte in self.to_bytes()? {
            value ^= u64::from(byte);
            value = value.wrapping_mul(0x0000_0100_0000_01b3);
        }
        Ok(TraceDigest(value))
    }
}

/// A stable 64-bit history checksum.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TraceDigest(u64);

impl TraceDigest {
    /// Returns the numeric checksum.
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl Display for TraceDigest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:016x}", self.0)
    }
}

/// Invalid or unrepresentable trace data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceError {
    /// Input did not start with the EPTR marker.
    InvalidMagic,
    /// Input uses an unknown format version.
    UnsupportedVersion(u16),
    /// Input sets flags this implementation does not understand.
    UnsupportedFlags(u16),
    /// Input ended before a declared field was complete.
    Truncated,
    /// Input contained bytes after the declared events.
    TrailingBytes,
    /// An event name was not valid UTF-8.
    InvalidUtf8,
    /// Event sequence numbers were not canonical and contiguous.
    NonCanonicalSequence {
        /// Required sequence number.
        expected: u64,
        /// Encoded sequence number.
        actual: u64,
    },
    /// Event time moved backwards within an ordered trace.
    NonMonotonicTime {
        /// Previous event time.
        previous: u64,
        /// Invalid current event time.
        actual: u64,
    },
    /// A collection or field length was not representable by the format.
    LengthOverflow,
}

impl Display for TraceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => formatter.write_str("invalid trace magic"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported trace version {version}")
            }
            Self::UnsupportedFlags(flags) => write!(formatter, "unsupported trace flags {flags}"),
            Self::Truncated => formatter.write_str("truncated trace"),
            Self::TrailingBytes => formatter.write_str("trailing bytes after trace"),
            Self::InvalidUtf8 => formatter.write_str("trace event kind is not valid UTF-8"),
            Self::NonCanonicalSequence { expected, actual } => write!(
                formatter,
                "non-canonical trace sequence: expected {expected}, got {actual}"
            ),
            Self::NonMonotonicTime { previous, actual } => write!(
                formatter,
                "trace monotonic time moved backwards from {previous} to {actual}"
            ),
            Self::LengthOverflow => formatter.write_str("trace length overflow"),
        }
    }
}

impl Error for TraceError {}

#[derive(Clone, Copy, Debug)]
struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn read_array<const SIZE: usize>(&mut self) -> Result<[u8; SIZE], TraceError> {
        let slice = self.read_slice(SIZE)?;
        slice.try_into().map_err(|_| TraceError::Truncated)
    }

    fn read_slice(&mut self, length: usize) -> Result<&'a [u8], TraceError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(TraceError::LengthOverflow)?;
        let slice = self
            .bytes
            .get(self.position..end)
            .ok_or(TraceError::Truncated)?;
        self.position = end;
        Ok(slice)
    }

    fn is_finished(self) -> bool {
        self.position == self.bytes.len()
    }

    fn remaining_len(self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }
}

#[cfg(test)]
mod tests {
    use super::{Trace, TraceError};

    fn compatibility_trace() -> Trace {
        let mut trace = Trace::new();
        trace.record(7, "x", [0, 0xff]).unwrap();
        trace.record(12, "deliver", b"ok").unwrap();
        trace
    }

    #[test]
    fn version_one_bytes_and_digest_are_compatibility_goldens() {
        let expected = [
            0x45, 0x50, 0x54, 0x52, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x78, 0x02, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0xff, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x64, 0x65, 0x6c,
            0x69, 0x76, 0x65, 0x72, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6f, 0x6b,
        ];
        let trace = compatibility_trace();

        assert_eq!(trace.to_bytes().unwrap(), expected);
        assert_eq!(trace.digest().unwrap().value(), 0xbd94_a233_541b_2179);
    }

    #[test]
    fn rejects_invalid_header_fields_and_truncation() {
        let bytes = compatibility_trace().to_bytes().unwrap();

        let mut invalid_magic = bytes.clone();
        invalid_magic[0] = b'X';
        assert_eq!(
            Trace::from_bytes(&invalid_magic),
            Err(TraceError::InvalidMagic)
        );

        let mut invalid_version = bytes.clone();
        invalid_version[4..6].copy_from_slice(&2_u16.to_le_bytes());
        assert_eq!(
            Trace::from_bytes(&invalid_version),
            Err(TraceError::UnsupportedVersion(2))
        );

        let mut invalid_flags = bytes.clone();
        invalid_flags[6..8].copy_from_slice(&1_u16.to_le_bytes());
        assert_eq!(
            Trace::from_bytes(&invalid_flags),
            Err(TraceError::UnsupportedFlags(1))
        );

        assert_eq!(
            Trace::from_bytes(&bytes[..bytes.len() - 1]),
            Err(TraceError::Truncated)
        );
    }

    #[test]
    fn rejects_noncanonical_event_sequences() {
        let mut bytes = compatibility_trace().to_bytes().unwrap();
        bytes[16..24].copy_from_slice(&1_u64.to_le_bytes());

        assert_eq!(
            Trace::from_bytes(&bytes),
            Err(TraceError::NonCanonicalSequence {
                expected: 0,
                actual: 1,
            })
        );
    }

    #[test]
    fn rejects_decreasing_monotonic_time_when_recording_or_decoding() {
        let mut trace = Trace::new();
        trace.record(10, "first", []).unwrap();
        assert_eq!(
            trace.record(9, "second", []),
            Err(TraceError::NonMonotonicTime {
                previous: 10,
                actual: 9,
            })
        );

        let mut bytes = compatibility_trace().to_bytes().unwrap();
        bytes[55..63].copy_from_slice(&6_u64.to_le_bytes());
        assert_eq!(
            Trace::from_bytes(&bytes),
            Err(TraceError::NonMonotonicTime {
                previous: 7,
                actual: 6,
            })
        );
    }

    #[test]
    fn rejects_trailing_bytes() {
        let mut bytes = Trace::new().to_bytes().unwrap();
        bytes.push(0);
        assert_eq!(Trace::from_bytes(&bytes), Err(TraceError::TrailingBytes));
    }
}

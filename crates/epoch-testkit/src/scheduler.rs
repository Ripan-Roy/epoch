use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// Stable identifier assigned in scheduling order.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ScheduledId(u64);

impl ScheduledId {
    /// Returns the numeric identifier.
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// An item removed from a [`SeededScheduler`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Scheduled<T> {
    /// Identifier assigned when the item was inserted.
    pub id: ScheduledId,
    /// Virtual deadline at which the item became ready.
    pub deadline_ms: u64,
    /// Scheduled value.
    pub value: T,
}

/// A deterministic single-threaded event queue with an explicit PRNG seed.
///
/// Deadlines are ordered first; insertion identifiers break ties. Randomness is
/// exposed only through explicit sampling and uses a fixed `SplitMix64` algorithm.
#[derive(Clone, Debug)]
pub struct SeededScheduler<T> {
    seed: u64,
    random_state: u64,
    now_ms: u64,
    next_id: u64,
    id_space_exhausted: bool,
    queued: BTreeMap<(u64, u64), T>,
}

impl<T> SeededScheduler<T> {
    /// Creates an empty scheduler at virtual time zero.
    pub const fn new(seed: u64) -> Self {
        Self {
            seed,
            random_state: seed,
            now_ms: 0,
            next_id: 0,
            id_space_exhausted: false,
            queued: BTreeMap::new(),
        }
    }

    /// Returns the seed used to initialize deterministic sampling.
    pub const fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns current scheduler time.
    pub const fn now_ms(&self) -> u64 {
        self.now_ms
    }

    /// Number of pending events.
    pub fn len(&self) -> usize {
        self.queued.len()
    }

    /// Whether there are no pending events.
    pub fn is_empty(&self) -> bool {
        self.queued.is_empty()
    }

    /// Inserts an event at an absolute virtual deadline.
    pub fn schedule_at(
        &mut self,
        deadline_ms: u64,
        value: T,
    ) -> Result<ScheduledId, ScheduleError> {
        if deadline_ms < self.now_ms {
            return Err(ScheduleError::DeadlineInPast {
                deadline_ms,
                now_ms: self.now_ms,
            });
        }
        if self.id_space_exhausted {
            return Err(ScheduleError::IdentifierSpaceExhausted);
        }

        let id = ScheduledId(self.next_id);
        if self.next_id == u64::MAX {
            self.id_space_exhausted = true;
        } else {
            self.next_id += 1;
        }
        let replaced = self.queued.insert((deadline_ms, id.0), value);
        debug_assert!(replaced.is_none(), "scheduled identifiers must be unique");
        Ok(id)
    }

    /// Inserts an event relative to current virtual time.
    pub fn schedule_after(
        &mut self,
        delay_ms: u64,
        value: T,
    ) -> Result<ScheduledId, ScheduleError> {
        let deadline_ms = self
            .now_ms
            .checked_add(delay_ms)
            .ok_or(ScheduleError::TimeOverflow)?;
        self.schedule_at(deadline_ms, value)
    }

    /// Removes the next event and advances virtual time to its deadline.
    pub fn pop_next(&mut self) -> Result<Option<Scheduled<T>>, ScheduleError> {
        let Some(((deadline_ms, raw_id), value)) = self.queued.pop_first() else {
            return Ok(None);
        };
        if deadline_ms < self.now_ms {
            return Err(ScheduleError::QueueInvariantViolation);
        }
        self.now_ms = deadline_ms;
        Ok(Some(Scheduled {
            id: ScheduledId(raw_id),
            deadline_ms,
            value,
        }))
    }

    /// Samples uniformly enough for fault simulation from `0..=maximum`.
    ///
    /// The exact modulo-based mapping is part of the deterministic contract and
    /// is intentionally stable. It favors reproducibility over cryptography.
    pub fn sample_inclusive(&mut self, maximum: u64) -> u64 {
        let random = self.next_random();
        if maximum == u64::MAX {
            random
        } else {
            random % (maximum + 1)
        }
    }

    fn next_random(&mut self) -> u64 {
        self.random_state = self.random_state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.random_state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }
}

/// Invalid scheduler operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScheduleError {
    /// An event was scheduled before current virtual time.
    DeadlineInPast {
        /// Requested deadline.
        deadline_ms: u64,
        /// Current scheduler time.
        now_ms: u64,
    },
    /// A relative deadline overflowed the virtual-time representation.
    TimeOverflow,
    /// Every stable event identifier has been consumed.
    IdentifierSpaceExhausted,
    /// Internal queue ordering contradicted scheduler time.
    QueueInvariantViolation,
}

impl Display for ScheduleError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeadlineInPast {
                deadline_ms,
                now_ms,
            } => write!(
                formatter,
                "deadline {deadline_ms}ms is before scheduler time {now_ms}ms"
            ),
            Self::TimeOverflow => formatter.write_str("scheduled deadline overflow"),
            Self::IdentifierSpaceExhausted => {
                formatter.write_str("scheduled identifier space exhausted")
            }
            Self::QueueInvariantViolation => {
                formatter.write_str("scheduler queue ordering invariant violated")
            }
        }
    }
}

impl Error for ScheduleError {}

#[cfg(test)]
mod tests {
    use super::SeededScheduler;

    #[test]
    fn splitmix_sample_sequence_is_a_compatibility_golden() {
        let mut scheduler = SeededScheduler::<()>::new(42);
        let actual: Vec<_> = (0..8).map(|_| scheduler.sample_inclusive(100)).collect();

        assert_eq!(actual, [23, 63, 43, 5, 42, 59, 93, 100]);
    }
}

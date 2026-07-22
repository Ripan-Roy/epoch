use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// A snapshot containing the two intentionally independent notions of time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VirtualTime {
    /// Calendar-like time, which tests may jump forwards or backwards.
    pub wall_time_ms: i64,
    /// Elapsed time, which can only advance.
    pub monotonic_ms: u64,
}

/// A manually driven clock that never sleeps or reads the host clock.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VirtualClock {
    wall_time_ms: i64,
    monotonic_ms: u64,
}

impl VirtualClock {
    /// Creates a clock at `wall_time_ms` with monotonic time starting at zero.
    pub const fn new(wall_time_ms: i64) -> Self {
        Self {
            wall_time_ms,
            monotonic_ms: 0,
        }
    }

    /// Returns both clock readings atomically.
    pub const fn snapshot(&self) -> VirtualTime {
        VirtualTime {
            wall_time_ms: self.wall_time_ms,
            monotonic_ms: self.monotonic_ms,
        }
    }

    /// Returns the current wall-clock reading in milliseconds.
    pub const fn wall_time_ms(&self) -> i64 {
        self.wall_time_ms
    }

    /// Returns the current monotonic reading in milliseconds.
    pub const fn monotonic_ms(&self) -> u64 {
        self.monotonic_ms
    }

    /// Advances wall and monotonic time by the same non-negative duration.
    ///
    /// The update is atomic: an overflow leaves both readings unchanged.
    pub fn advance(&mut self, delta_ms: u64) -> Result<VirtualTime, TimeError> {
        let next_monotonic = self
            .monotonic_ms
            .checked_add(delta_ms)
            .ok_or(TimeError::MonotonicOverflow)?;
        let wall_delta = i64::try_from(delta_ms).map_err(|_| TimeError::WallOverflow)?;
        let next_wall = self
            .wall_time_ms
            .checked_add(wall_delta)
            .ok_or(TimeError::WallOverflow)?;

        self.monotonic_ms = next_monotonic;
        self.wall_time_ms = next_wall;
        Ok(self.snapshot())
    }

    /// Adjusts only wall time, allowing clock-correction scenarios.
    ///
    /// Monotonic time is deliberately untouched even for a negative jump.
    pub fn jump_wall(&mut self, delta_ms: i64) -> Result<VirtualTime, TimeError> {
        let next_wall = self
            .wall_time_ms
            .checked_add(delta_ms)
            .ok_or(TimeError::WallOverflow)?;
        self.wall_time_ms = next_wall;
        Ok(self.snapshot())
    }
}

/// Errors returned by [`VirtualClock`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeError {
    /// The monotonic reading could not represent the requested advance.
    MonotonicOverflow,
    /// The wall-clock reading could not represent the requested change.
    WallOverflow,
}

impl Display for TimeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MonotonicOverflow => formatter.write_str("monotonic time overflow"),
            Self::WallOverflow => formatter.write_str("wall-clock time overflow"),
        }
    }
}

impl Error for TimeError {}

#[cfg(test)]
mod tests {
    use super::{TimeError, VirtualClock};

    #[test]
    fn overflowing_advance_is_atomic() {
        let mut clock = VirtualClock::new(i64::MAX);
        let before = clock.snapshot();

        assert_eq!(clock.advance(1), Err(TimeError::WallOverflow));
        assert_eq!(clock.snapshot(), before);
    }
}

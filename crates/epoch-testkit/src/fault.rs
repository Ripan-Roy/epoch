use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// A stable, human-readable injection location such as `disk.segment.write`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FaultPoint(String);

impl FaultPoint {
    /// Creates a non-empty fault point.
    pub fn new(name: impl Into<String>) -> Result<Self, FaultPlanError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(FaultPlanError::EmptyPoint);
        }
        Ok(Self(name))
    }

    /// The standard point evaluated for every peer-transport send attempt.
    pub fn transport_send() -> Self {
        Self("transport.send".to_owned())
    }

    /// Returns the stable name of this point.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for FaultPoint {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A fault that can be attached to an exact occurrence of a [`FaultPoint`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultAction {
    /// Simulate the component stopping at the point.
    Crash,
    /// Simulate an I/O operation failing before making progress.
    IoError,
    /// Simulate an operation writing only the given number of bytes.
    PartialWrite {
        /// Maximum number of bytes written before the operation stops.
        bytes: usize,
    },
    /// Silently discard the operation.
    Drop,
    /// Defer the operation by this many virtual milliseconds.
    Delay {
        /// Added virtual delay.
        by_ms: u64,
    },
    /// Perform the operation more than once.
    Duplicate {
        /// Number of copies in addition to the original operation.
        additional_copies: u16,
        /// Virtual milliseconds between each copy.
        spacing_ms: u64,
    },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct OccurrenceCounter {
    value: u64,
    exhausted: bool,
}

/// An occurrence-indexed, deterministic collection of scripted failures.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FaultPlan {
    rules: BTreeMap<(FaultPoint, u64), FaultAction>,
    occurrences: BTreeMap<FaultPoint, OccurrenceCounter>,
}

impl FaultPlan {
    /// Creates an empty plan.
    pub const fn new() -> Self {
        Self {
            rules: BTreeMap::new(),
            occurrences: BTreeMap::new(),
        }
    }

    /// Adds one action at a one-based occurrence number.
    pub fn add(
        &mut self,
        point: FaultPoint,
        occurrence: u64,
        action: FaultAction,
    ) -> Result<(), FaultPlanError> {
        if occurrence == 0 {
            return Err(FaultPlanError::OccurrenceMustBePositive);
        }
        let key = (point, occurrence);
        if self.rules.contains_key(&key) {
            return Err(FaultPlanError::DuplicateRule {
                point: key.0,
                occurrence,
            });
        }
        self.rules.insert(key, action);
        Ok(())
    }

    /// Records one visit and returns the action scripted for that exact visit.
    pub fn trigger(&mut self, point: &FaultPoint) -> TriggeredFault {
        let counter = self.occurrences.entry(point.clone()).or_default();
        if counter.exhausted {
            return TriggeredFault {
                occurrence: u64::MAX,
                action: None,
            };
        }
        if counter.value == u64::MAX {
            counter.exhausted = true;
            return TriggeredFault {
                occurrence: u64::MAX,
                action: None,
            };
        }

        counter.value += 1;
        TriggeredFault {
            occurrence: counter.value,
            action: self.rules.get(&(point.clone(), counter.value)).copied(),
        }
    }

    /// Returns how often a point has been visited.
    pub fn occurrence_count(&self, point: &FaultPoint) -> u64 {
        self.occurrences
            .get(point)
            .map_or(0, |counter| counter.value)
    }

    /// Resets occurrence counters while preserving the scripted rules.
    pub fn reset(&mut self) {
        self.occurrences.clear();
    }
}

/// Result of visiting a fault point.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TriggeredFault {
    /// One-based visit number.
    pub occurrence: u64,
    /// Action for exactly this occurrence, if one was scripted.
    pub action: Option<FaultAction>,
}

/// Invalid fault-plan construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FaultPlanError {
    /// A fault-point name was empty or whitespace-only.
    EmptyPoint,
    /// Occurrence zero was requested; occurrences are one-based.
    OccurrenceMustBePositive,
    /// More than one action was added at the same point and occurrence.
    DuplicateRule {
        /// Conflicting point.
        point: FaultPoint,
        /// Conflicting occurrence.
        occurrence: u64,
    },
}

impl Display for FaultPlanError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyPoint => formatter.write_str("fault-point name cannot be empty"),
            Self::OccurrenceMustBePositive => {
                formatter.write_str("fault occurrence must be one or greater")
            }
            Self::DuplicateRule { point, occurrence } => {
                write!(
                    formatter,
                    "duplicate fault rule at {point} occurrence {occurrence}"
                )
            }
        }
    }
}

impl Error for FaultPlanError {}

#[cfg(test)]
mod tests {
    use super::{FaultAction, FaultPlan, FaultPlanError, FaultPoint};

    #[test]
    fn rejects_duplicate_rules_without_overwriting_the_first() {
        let point = FaultPoint::new("store.commit").unwrap();
        let mut plan = FaultPlan::new();
        plan.add(point.clone(), 1, FaultAction::Crash).unwrap();

        assert!(matches!(
            plan.add(point.clone(), 1, FaultAction::Drop),
            Err(FaultPlanError::DuplicateRule { .. })
        ));
        assert_eq!(plan.trigger(&point).action, Some(FaultAction::Crash));
    }
}

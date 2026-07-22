//! Deterministic building blocks for testing distributed Epoch components.
//!
//! The crate deliberately avoids real time, threads, sockets, and global random
//! state. A test owns every clock, scheduler, fault plan, and transport instance,
//! so re-running the same operations with the same seed produces the same result.

mod clock;
mod fault;
mod scheduler;
mod trace;
mod transport;

pub use clock::{TimeError, VirtualClock, VirtualTime};
pub use fault::{FaultAction, FaultPlan, FaultPlanError, FaultPoint, TriggeredFault};
pub use scheduler::{ScheduleError, Scheduled, ScheduledId, SeededScheduler};
pub use trace::{Trace, TraceDigest, TraceError, TraceEvent};
pub use transport::{
    Delivery, DropReason, MessageId, PeerId, PeerTransport, SendOutcome, TransportError,
    TransportFault,
};

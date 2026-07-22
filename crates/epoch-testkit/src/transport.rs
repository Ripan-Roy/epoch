use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use crate::{
    FaultAction, FaultPlan, FaultPoint, ScheduleError, SeededScheduler, Trace, TraceError,
};

/// Stable identity of an in-memory peer.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PeerId(u64);

impl PeerId {
    /// Creates a peer identifier.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric identifier.
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl Display for PeerId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

/// Stable identity assigned to one logical send attempt.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MessageId(u64);

impl MessageId {
    /// Returns the numeric identifier.
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Why a message was not queued for delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DropReason {
    /// The directed link crossed an active network partition.
    Partition,
    /// An occurrence-based drop action fired.
    FaultPlan,
}

/// A non-delivery failure produced by an injected action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportFault {
    /// The sending component crashed.
    Crash,
    /// The send reported an I/O error.
    IoError,
    /// Only part of the payload was accepted.
    PartialWrite {
        /// Bytes accepted before the failure.
        written_bytes: usize,
        /// Total bytes in the attempted payload.
        total_bytes: usize,
    },
}

/// Observable result of a peer send attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SendOutcome {
    /// One or more copies were queued.
    Scheduled {
        /// Logical message identity shared by all copies.
        message_id: MessageId,
        /// Total copy count, including the original.
        copies: u16,
        /// Deadline of the original copy.
        first_delivery_at_ms: u64,
    },
    /// The message was intentionally discarded.
    Dropped {
        /// Logical message identity.
        message_id: MessageId,
        /// Source of the drop.
        reason: DropReason,
    },
    /// The send stopped with an injected failure.
    Faulted {
        /// Logical message identity.
        message_id: MessageId,
        /// Injected failure.
        fault: TransportFault,
    },
}

/// One delivered copy of a logical message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Delivery {
    /// Logical message identity.
    pub message_id: MessageId,
    /// Zero for the original; one or greater for injected duplicates.
    pub copy_index: u16,
    /// Sending peer.
    pub from: PeerId,
    /// Receiving peer.
    pub to: PeerId,
    /// Owned payload bytes.
    pub payload: Vec<u8>,
    /// Scheduler time at delivery.
    pub delivered_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct QueuedMessage {
    message_id: MessageId,
    copy_index: u16,
    from: PeerId,
    to: PeerId,
    payload: Vec<u8>,
}

/// A deterministic, manually drained peer network.
#[derive(Clone, Debug)]
pub struct PeerTransport {
    scheduler: SeededScheduler<QueuedMessage>,
    faults: FaultPlan,
    blocked_links: BTreeSet<(PeerId, PeerId)>,
    next_message_id: u64,
    message_id_space_exhausted: bool,
    trace: Trace,
}

impl PeerTransport {
    /// Creates a transport with no faults or partitions.
    pub fn new(seed: u64) -> Self {
        Self::with_fault_plan(seed, FaultPlan::new())
    }

    /// Creates a transport with an occurrence-based fault script.
    pub fn with_fault_plan(seed: u64, faults: FaultPlan) -> Self {
        Self {
            scheduler: SeededScheduler::new(seed),
            faults,
            blocked_links: BTreeSet::new(),
            next_message_id: 0,
            message_id_space_exhausted: false,
            trace: Trace::new(),
        }
    }

    /// Returns current virtual transport time.
    pub const fn now_ms(&self) -> u64 {
        self.scheduler.now_ms()
    }

    /// Returns the pending-delivery count.
    pub fn pending_len(&self) -> usize {
        self.scheduler.len()
    }

    /// Provides mutable access for adding faults before or between operations.
    pub fn fault_plan_mut(&mut self) -> &mut FaultPlan {
        &mut self.faults
    }

    /// Returns the trace accumulated so far.
    pub const fn trace(&self) -> &Trace {
        &self.trace
    }

    /// Consumes the transport and returns its trace.
    pub fn into_trace(self) -> Trace {
        self.trace
    }

    /// Blocks one directed link while leaving the reverse link unchanged.
    ///
    /// Returns whether this call changed the link state. The operation is
    /// trace-visible even when the link was already blocked.
    pub fn block_link(&mut self, from: PeerId, to: PeerId) -> Result<bool, TransportError> {
        let changed = self.blocked_links.insert((from, to));
        self.trace.record(
            self.now_ms(),
            "transport.link.block",
            encode_link_change(from, to, changed),
        )?;
        Ok(changed)
    }

    /// Heals one directed link while leaving the reverse link unchanged.
    ///
    /// Returns whether this call changed the link state. The operation is
    /// trace-visible even when the link was already healthy.
    pub fn heal_link(&mut self, from: PeerId, to: PeerId) -> Result<bool, TransportError> {
        let changed = self.blocked_links.remove(&(from, to));
        self.trace.record(
            self.now_ms(),
            "transport.link.heal",
            encode_link_change(from, to, changed),
        )?;
        Ok(changed)
    }

    /// Reports whether one directed link is currently blocked.
    pub fn is_link_blocked(&self, from: PeerId, to: PeerId) -> bool {
        self.blocked_links.contains(&(from, to))
    }

    /// Blocks every cross-link between two disjoint peer groups.
    pub fn partition(&mut self, left: &[PeerId], right: &[PeerId]) -> Result<(), TransportError> {
        let left: BTreeSet<_> = left.iter().copied().collect();
        let right: BTreeSet<_> = right.iter().copied().collect();
        if let Some(overlap) = left.intersection(&right).next() {
            return Err(TransportError::OverlappingPartition(*overlap));
        }

        let payload = encode_partition(&left, &right)?;
        self.trace
            .record(self.now_ms(), "transport.partition", payload)?;
        for &from in &left {
            for &to in &right {
                self.block_link(from, to)?;
                self.block_link(to, from)?;
            }
        }
        Ok(())
    }

    /// Removes every blocked link while preserving queued deliveries.
    pub fn heal_all(&mut self) -> Result<(), TransportError> {
        self.blocked_links.clear();
        self.trace
            .record(self.now_ms(), "transport.partition.heal_all", Vec::new())?;
        Ok(())
    }

    /// Sends one message without an added baseline delay.
    pub fn send(
        &mut self,
        from: PeerId,
        to: PeerId,
        payload: impl AsRef<[u8]>,
    ) -> Result<SendOutcome, TransportError> {
        self.send_after(from, to, payload.as_ref(), 0)
    }

    /// Sends one message after a deterministic seed-derived delay in `0..=max`.
    pub fn send_with_jitter(
        &mut self,
        from: PeerId,
        to: PeerId,
        payload: impl AsRef<[u8]>,
        maximum_delay_ms: u64,
    ) -> Result<SendOutcome, TransportError> {
        let delay_ms = self.scheduler.sample_inclusive(maximum_delay_ms);
        self.send_after(from, to, payload.as_ref(), delay_ms)
    }

    /// Sends one message after an explicit virtual delay.
    pub fn send_with_delay(
        &mut self,
        from: PeerId,
        to: PeerId,
        payload: impl AsRef<[u8]>,
        delay_ms: u64,
    ) -> Result<SendOutcome, TransportError> {
        self.send_after(from, to, payload.as_ref(), delay_ms)
    }

    /// Delivers the next queued copy, advancing only virtual scheduler time.
    pub fn deliver_next(&mut self) -> Result<Option<Delivery>, TransportError> {
        let Some(scheduled) = self.scheduler.pop_next()? else {
            return Ok(None);
        };
        let queued = scheduled.value;
        let delivery = Delivery {
            message_id: queued.message_id,
            copy_index: queued.copy_index,
            from: queued.from,
            to: queued.to,
            payload: queued.payload,
            delivered_at_ms: scheduled.deadline_ms,
        };
        let trace_payload = encode_delivery(&delivery)?;
        self.trace
            .record(delivery.delivered_at_ms, "transport.deliver", trace_payload)?;
        Ok(Some(delivery))
    }

    fn send_after(
        &mut self,
        from: PeerId,
        to: PeerId,
        payload: &[u8],
        base_delay_ms: u64,
    ) -> Result<SendOutcome, TransportError> {
        let message_id = self.allocate_message_id()?;
        self.trace.record(
            self.now_ms(),
            "transport.send.attempt",
            encode_message(message_id, from, to, payload)?,
        )?;

        let triggered = self.faults.trigger(&FaultPoint::transport_send());
        match triggered.action {
            Some(FaultAction::Crash) => {
                return self.faulted(message_id, TransportFault::Crash);
            }
            Some(FaultAction::IoError) => {
                return self.faulted(message_id, TransportFault::IoError);
            }
            Some(FaultAction::PartialWrite { bytes }) => {
                return self.faulted(
                    message_id,
                    TransportFault::PartialWrite {
                        written_bytes: bytes.min(payload.len()),
                        total_bytes: payload.len(),
                    },
                );
            }
            Some(FaultAction::Drop) => {
                return self.dropped(message_id, DropReason::FaultPlan);
            }
            _ => {}
        }

        if self.blocked_links.contains(&(from, to)) {
            return self.dropped(message_id, DropReason::Partition);
        }

        let (delay_ms, copies, spacing_ms) = match triggered.action {
            Some(FaultAction::Delay { by_ms }) => (
                base_delay_ms
                    .checked_add(by_ms)
                    .ok_or(TransportError::DeliveryTimeOverflow)?,
                1,
                0,
            ),
            Some(FaultAction::Duplicate {
                additional_copies,
                spacing_ms,
            }) => (
                base_delay_ms,
                additional_copies
                    .checked_add(1)
                    .ok_or(TransportError::CopyCountOverflow)?,
                spacing_ms,
            ),
            _ => (base_delay_ms, 1, 0),
        };

        let first_delivery_at_ms = self
            .now_ms()
            .checked_add(delay_ms)
            .ok_or(TransportError::DeliveryTimeOverflow)?;
        let mut deadlines = Vec::with_capacity(usize::from(copies));
        for copy_index in 0..copies {
            let spacing = spacing_ms
                .checked_mul(u64::from(copy_index))
                .ok_or(TransportError::DeliveryTimeOverflow)?;
            let deadline = first_delivery_at_ms
                .checked_add(spacing)
                .ok_or(TransportError::DeliveryTimeOverflow)?;
            deadlines.push((copy_index, deadline));
        }

        for (copy_index, deadline_ms) in deadlines {
            self.scheduler.schedule_at(
                deadline_ms,
                QueuedMessage {
                    message_id,
                    copy_index,
                    from,
                    to,
                    payload: payload.to_vec(),
                },
            )?;
            self.trace.record(
                self.now_ms(),
                "transport.send.scheduled",
                encode_scheduled(message_id, copy_index, from, to, deadline_ms, payload)?,
            )?;
        }
        Ok(SendOutcome::Scheduled {
            message_id,
            copies,
            first_delivery_at_ms,
        })
    }

    fn allocate_message_id(&mut self) -> Result<MessageId, TransportError> {
        if self.message_id_space_exhausted {
            return Err(TransportError::MessageIdSpaceExhausted);
        }
        let message_id = MessageId(self.next_message_id);
        if self.next_message_id == u64::MAX {
            self.message_id_space_exhausted = true;
        } else {
            self.next_message_id += 1;
        }
        Ok(message_id)
    }

    fn dropped(
        &mut self,
        message_id: MessageId,
        reason: DropReason,
    ) -> Result<SendOutcome, TransportError> {
        let reason_byte = match reason {
            DropReason::Partition => 0,
            DropReason::FaultPlan => 1,
        };
        let mut payload = Vec::with_capacity(9);
        payload.extend_from_slice(&message_id.0.to_le_bytes());
        payload.push(reason_byte);
        self.trace
            .record(self.now_ms(), "transport.send.dropped", payload)?;
        Ok(SendOutcome::Dropped { message_id, reason })
    }

    fn faulted(
        &mut self,
        message_id: MessageId,
        fault: TransportFault,
    ) -> Result<SendOutcome, TransportError> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&message_id.0.to_le_bytes());
        match fault {
            TransportFault::Crash => payload.push(0),
            TransportFault::IoError => payload.push(1),
            TransportFault::PartialWrite {
                written_bytes,
                total_bytes,
            } => {
                payload.push(2);
                push_usize(&mut payload, written_bytes)?;
                push_usize(&mut payload, total_bytes)?;
            }
        }
        self.trace
            .record(self.now_ms(), "transport.send.faulted", payload)?;
        Ok(SendOutcome::Faulted { message_id, fault })
    }
}

/// Invalid peer-transport operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransportError {
    /// A scheduler operation failed.
    Schedule(ScheduleError),
    /// Trace recording or encoding failed.
    Trace(TraceError),
    /// Every message identifier has been consumed.
    MessageIdSpaceExhausted,
    /// Delay or duplicate spacing exceeded virtual-time capacity.
    DeliveryTimeOverflow,
    /// Duplicate configuration could not represent total copies.
    CopyCountOverflow,
    /// A peer appeared on both sides of a partition.
    OverlappingPartition(PeerId),
    /// An in-memory length was not representable in canonical trace bytes.
    LengthOverflow,
}

impl Display for TransportError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Schedule(error) => write!(formatter, "transport scheduler: {error}"),
            Self::Trace(error) => write!(formatter, "transport trace: {error}"),
            Self::MessageIdSpaceExhausted => {
                formatter.write_str("transport message identifier space exhausted")
            }
            Self::DeliveryTimeOverflow => formatter.write_str("transport delivery time overflow"),
            Self::CopyCountOverflow => formatter.write_str("transport copy count overflow"),
            Self::OverlappingPartition(peer) => {
                write!(formatter, "peer {peer} appears on both partition sides")
            }
            Self::LengthOverflow => formatter.write_str("transport payload length overflow"),
        }
    }
}

impl Error for TransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Schedule(error) => Some(error),
            Self::Trace(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ScheduleError> for TransportError {
    fn from(error: ScheduleError) -> Self {
        Self::Schedule(error)
    }
}

impl From<TraceError> for TransportError {
    fn from(error: TraceError) -> Self {
        Self::Trace(error)
    }
}

fn encode_link_change(from: PeerId, to: PeerId, changed: bool) -> Vec<u8> {
    let mut output = Vec::with_capacity(17);
    output.extend_from_slice(&from.0.to_le_bytes());
    output.extend_from_slice(&to.0.to_le_bytes());
    output.push(u8::from(changed));
    output
}

fn encode_partition(
    left: &BTreeSet<PeerId>,
    right: &BTreeSet<PeerId>,
) -> Result<Vec<u8>, TransportError> {
    let mut output = Vec::new();
    push_usize(&mut output, left.len())?;
    for peer in left {
        output.extend_from_slice(&peer.0.to_le_bytes());
    }
    push_usize(&mut output, right.len())?;
    for peer in right {
        output.extend_from_slice(&peer.0.to_le_bytes());
    }
    Ok(output)
}

fn encode_message(
    message_id: MessageId,
    from: PeerId,
    to: PeerId,
    payload: &[u8],
) -> Result<Vec<u8>, TransportError> {
    let mut output = Vec::new();
    output.extend_from_slice(&message_id.0.to_le_bytes());
    output.extend_from_slice(&from.0.to_le_bytes());
    output.extend_from_slice(&to.0.to_le_bytes());
    push_usize(&mut output, payload.len())?;
    output.extend_from_slice(payload);
    Ok(output)
}

fn encode_scheduled(
    message_id: MessageId,
    copy_index: u16,
    from: PeerId,
    to: PeerId,
    deadline_ms: u64,
    payload: &[u8],
) -> Result<Vec<u8>, TransportError> {
    let mut output = encode_message(message_id, from, to, payload)?;
    output.extend_from_slice(&copy_index.to_le_bytes());
    output.extend_from_slice(&deadline_ms.to_le_bytes());
    Ok(output)
}

fn encode_delivery(delivery: &Delivery) -> Result<Vec<u8>, TransportError> {
    let mut output = encode_message(
        delivery.message_id,
        delivery.from,
        delivery.to,
        &delivery.payload,
    )?;
    output.extend_from_slice(&delivery.copy_index.to_le_bytes());
    output.extend_from_slice(&delivery.delivered_at_ms.to_le_bytes());
    Ok(output)
}

fn push_usize(output: &mut Vec<u8>, value: usize) -> Result<(), TransportError> {
    let value = u64::try_from(value).map_err(|_| TransportError::LengthOverflow)?;
    output.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{PeerId, PeerTransport, TransportError};

    #[test]
    fn rejects_overlapping_partition_membership() {
        let peer = PeerId::new(7);
        let mut transport = PeerTransport::new(1);
        assert!(matches!(
            transport.partition(&[peer], &[peer]),
            Err(TransportError::OverlappingPartition(value)) if value == peer
        ));
    }
}

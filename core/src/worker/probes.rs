//! The health probes of a worker: the state that `GET /livez` and
//! `GET /readyz` of its metrics endpoint read, and their answers. Built with
//! the `metrics-endpoint` feature.
//!
//! The run of a worker has three phases: the start, the claim loop, the
//! stop. The worker sets the phase; a probe reads it and answers at once.
//! A probe never waits on the database or on the process runtime.
//!
//! `/livez`: the process loop of the worker parent is its claim loop.
//! - Start and stop: live. Each step of the start (the children, the
//!   database start, the listener, the liveness lock) and the stop (the
//!   drain timeout) has its own bound, and a step that passes it ends the
//!   run.
//! - Claim loop: before each pass and before each wait, the loop sets the
//!   deadline of its next turn: the bound of the wait (the poll interval or
//!   the backoff delay) plus [`LIVEZ_TURN_ALLOWANCE`]. Live while the clock
//!   is at or before the deadline; past it, the loop is stuck (a pass that
//!   does not end, or a process runtime that does not run the loop).
//!
//! `/readyz`: the worker can claim.
//! - Start and stop: not ready.
//! - Claim loop: ready when the pools serve (no pool failure), one or more
//!   slots have a serving child, and the liveness lock is held. A worker
//!   that claims without its lock fails closed at its next watch, so a
//!   lock that is not held is not ready.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::worker::executor::slots::{PoolBoard, PoolHealth};
use crate::worker::liveness::{LockHold, LockHoldView};

/// The time that one pass of the claim loop, or the wake after one of its
/// waits, may take before `/livez` answers that the loop is stuck.
// Provisional value: the telemetry overhead measurement (the no-op drain
// with the worker endpoint on, against telemetry off) sets it.
pub const LIVEZ_TURN_ALLOWANCE: Duration = Duration::from_secs(30);

/// What the readiness probe reads while the claim loop runs.
#[derive(Clone)]
pub(crate) struct ServingFacts {
    /// The board of every slot of the worker.
    pub(crate) board: Arc<PoolBoard>,
    /// What the holder of the liveness lock last found.
    pub(crate) lock: LockHoldView,
}

/// The phase of the run of a worker.
#[derive(Clone)]
enum ProbePhase {
    /// The start runs: the children, the database, the listener, the lock.
    Starting,
    /// The claim loop runs.
    Claiming(ServingFacts),
    /// The claim loop ended; the stop drains.
    Stopping,
}

/// The state that the probes of one worker read. The worker writes it; the
/// endpoint thread reads it.
pub(crate) struct WorkerProbes {
    phase: Mutex<ProbePhase>,
    turns: ClaimTurns,
}

/// What `/livez` answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveAnswer {
    /// The start runs; each of its steps has its own bound.
    Starting,
    /// The claim loop turned by its deadline.
    ClaimLoopTurning,
    /// The claim loop is `late_by` past the deadline of its next turn: the
    /// worker parent is stuck.
    ClaimLoopLate { late_by: Duration },
    /// The stop runs; the drain timeout bounds it.
    Stopping,
}

/// What `/readyz` answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadyAnswer {
    /// The pools serve, a child serves, and the liveness lock is held.
    Ready,
    /// The start runs; the worker claims nothing yet.
    Starting,
    /// The claim loop ended; the worker claims nothing more.
    Stopping,
    /// The pools failed; the worker stops.
    PoolsFailed,
    /// No slot has a serving child: every slot boots, retires, closes or
    /// is down.
    NoChildServing,
    /// The liveness lock is not held, for `cause`.
    LockNotHeld { cause: String },
}

/// Whether a probe passes: HTTP 200, else 503.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeStatus {
    Pass,
    Fail,
}

impl LiveAnswer {
    pub fn status(&self) -> ProbeStatus {
        match self {
            Self::Starting | Self::ClaimLoopTurning | Self::Stopping => ProbeStatus::Pass,
            Self::ClaimLoopLate { .. } => ProbeStatus::Fail,
        }
    }

    /// The body of the answer: one line.
    pub fn text(&self) -> String {
        match self {
            Self::Starting => "live: the worker starts\n".to_owned(),
            Self::ClaimLoopTurning => "live: the claim loop turns\n".to_owned(),
            Self::ClaimLoopLate { late_by } => format!(
                "not live: the claim loop is {:.3}s past the deadline of its next turn\n",
                late_by.as_secs_f64()
            ),
            Self::Stopping => "live: the worker stops\n".to_owned(),
        }
    }
}

impl ReadyAnswer {
    pub fn status(&self) -> ProbeStatus {
        match self {
            Self::Ready => ProbeStatus::Pass,
            Self::Starting
            | Self::Stopping
            | Self::PoolsFailed
            | Self::NoChildServing
            | Self::LockNotHeld { .. } => ProbeStatus::Fail,
        }
    }

    /// The body of the answer: one line.
    pub fn text(&self) -> String {
        match self {
            Self::Ready => "ready\n".to_owned(),
            Self::Starting => "not ready: the worker starts\n".to_owned(),
            Self::Stopping => "not ready: the worker stops\n".to_owned(),
            Self::PoolsFailed => "not ready: the pools of the worker failed\n".to_owned(),
            Self::NoChildServing => "not ready: no child serves\n".to_owned(),
            Self::LockNotHeld { cause } => {
                format!("not ready: the worker liveness lock is not held: {cause}\n")
            }
        }
    }
}

impl WorkerProbes {
    /// The probes of a worker that starts.
    pub(crate) fn starting() -> Self {
        Self {
            phase: Mutex::new(ProbePhase::Starting),
            turns: ClaimTurns::new(Instant::now()),
        }
    }

    fn set_phase(&self, phase: ProbePhase) {
        *self.phase.lock().unwrap_or_else(PoisonError::into_inner) = phase;
    }

    fn phase(&self) -> ProbePhase {
        // A poisoned lock means a panic while the phase was written; the
        // phase is still a whole value.
        self.phase
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The claim loop starts on `facts`; its first turn is due within
    /// [`LIVEZ_TURN_ALLOWANCE`].
    pub(crate) fn claiming(&self, facts: ServingFacts) {
        self.turns.turn_within(Instant::now(), LIVEZ_TURN_ALLOWANCE);
        self.set_phase(ProbePhase::Claiming(facts));
    }

    /// The claim loop ended.
    pub(crate) fn stopping(&self) {
        self.set_phase(ProbePhase::Stopping);
    }

    /// The claim loop turns again within `wait` plus
    /// [`LIVEZ_TURN_ALLOWANCE`] from now. One relaxed atomic store.
    pub(crate) fn claim_turn_within(&self, wait: Duration) {
        self.turns
            .turn_within(Instant::now(), wait.saturating_add(LIVEZ_TURN_ALLOWANCE));
    }

    /// The answer of `/livez` now.
    pub(crate) fn live(&self) -> LiveAnswer {
        match self.phase() {
            ProbePhase::Starting => LiveAnswer::Starting,
            ProbePhase::Claiming(_) => live_of_turn(self.turns.check(Instant::now())),
            ProbePhase::Stopping => LiveAnswer::Stopping,
        }
    }

    /// The answer of `/readyz` now.
    pub(crate) fn ready(&self) -> ReadyAnswer {
        match self.phase() {
            ProbePhase::Starting => ReadyAnswer::Starting,
            ProbePhase::Claiming(facts) => ready_of(
                facts.board.health(),
                facts.board.serving_slots(),
                facts.lock.hold(),
            ),
            ProbePhase::Stopping => ReadyAnswer::Stopping,
        }
    }
}

/// Whether the claim loop turned by its deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TurnCheck {
    OnTime,
    Late(Duration),
}

/// The deadline of the next turn of the claim loop, in nanoseconds since
/// `origin`.
struct ClaimTurns {
    origin: Instant,
    turn_by_ns: AtomicU64,
}

impl ClaimTurns {
    fn new(origin: Instant) -> Self {
        Self {
            origin,
            turn_by_ns: AtomicU64::new(0),
        }
    }

    /// The next turn is due within `within` from `now`.
    fn turn_within(&self, now: Instant, within: Duration) {
        let deadline = now
            .saturating_duration_since(self.origin)
            .saturating_add(within);
        self.turn_by_ns
            .store(saturating_nanos(deadline), Ordering::Relaxed);
    }

    /// Whether `now` is at or before the deadline.
    fn check(&self, now: Instant) -> TurnCheck {
        let now_ns = saturating_nanos(now.saturating_duration_since(self.origin));
        let turn_by_ns = self.turn_by_ns.load(Ordering::Relaxed);
        match now_ns.checked_sub(turn_by_ns) {
            None | Some(0) => TurnCheck::OnTime,
            Some(late_ns) => TurnCheck::Late(Duration::from_nanos(late_ns)),
        }
    }
}

/// The nanoseconds of `duration`; 2^64 ns (about 584 years) and more read
/// as the largest value.
fn saturating_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// The liveness of a claim loop whose last deadline gave `check`. Pure.
fn live_of_turn(check: TurnCheck) -> LiveAnswer {
    match check {
        TurnCheck::OnTime => LiveAnswer::ClaimLoopTurning,
        TurnCheck::Late(late_by) => LiveAnswer::ClaimLoopLate { late_by },
    }
}

/// The readiness of a worker whose claim loop runs: the health of its
/// pools, its slots with a serving child, and its liveness lock. Pure.
fn ready_of(health: PoolHealth, serving_slots: usize, lock: LockHold) -> ReadyAnswer {
    match (health, serving_slots, lock) {
        (PoolHealth::Failed(_), _, _) => ReadyAnswer::PoolsFailed,
        (PoolHealth::Stopping, _, _) => ReadyAnswer::Stopping,
        (PoolHealth::Serving, 0, _) => ReadyAnswer::NoChildServing,
        (PoolHealth::Serving, _, LockHold::NotHeld { cause }) => ReadyAnswer::LockNotHeld { cause },
        (PoolHealth::Serving, _, LockHold::Held) => ReadyAnswer::Ready,
    }
}

#[cfg(test)]
mod tests;

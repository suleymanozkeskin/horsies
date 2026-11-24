//! The cancel router of a worker: it routes each operator cancel of a
//! RUNNING task to the slot that holds the run.
//!
//! - The cancel statement (`horsies_cancel_task` with `include_running`)
//!   moves a RUNNING row to history, records the attempt as `CANCELLED`,
//!   and sends one notice on [`CANCEL_CHANNEL`]: `<task id> <claim
//!   generation>`, the claim generation in whole microseconds since the Unix
//!   epoch. PostgreSQL delivers it at commit, to every worker.
//! - The router parses each notice ([`parse_cancel_notice`]) and asks the
//!   registry of held dispatches to route it
//!   ([`LiveDispatches::request_cancel`]). A notice for a run that this
//!   worker does not hold is dropped.
//! - The listener can lose notices: at a reconnect (`Gap`) and at a full
//!   payload queue (`Overflowed`). After either, and after a payload that
//!   does not parse, the router reads which held runs were cancelled
//!   ([`CancelledRunsReader`]), and routes each.
//! - A held run whose runner beat did not land is a beat miss
//!   ([`LiveDispatches::next_beat_misses`]): its row may be gone because an
//!   operator cancelled it. The router reads the misses the same way. So a
//!   lost notice is found within one runner beat interval while the beat
//!   connection works, also when the listener stays down.
//! - A read that fails is logged; the next gap or beat miss reads again.
//!   Each read takes at most [`MAX_CANCELLED_RUNS_READ`] runs per statement.

use std::future::Future;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::broker::cancelled_runs::{CancelledRunsReadError, RunClaim, MAX_CANCELLED_RUNS_READ};
use crate::broker::PostgresBroker;
use crate::db::{PayloadEvent, PayloadSubscription};
use crate::worker::executor::protocol::BoundedText;
use crate::worker::runner_beats::{CancelRouting, LiveDispatches};

/// The channel of the cancel notices of RUNNING tasks.
pub const CANCEL_CHANNEL: &str = "horsies_task_cancel";

/// The most characters of a refused payload that a refusal quotes.
pub const MAX_QUOTED_NOTICE_CHARS: usize = 120;

/// A payload of [`CANCEL_CHANNEL`] as a refusal quotes it.
pub type QuotedNotice = BoundedText<MAX_QUOTED_NOTICE_CHARS>;

/// Why a payload of [`CANCEL_CHANNEL`] is not a cancel notice.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NoticeRefused {
    /// The payload is not two fields with one space between them.
    #[error("the cancel notice '{payload}' is not '<task id> <claim generation>'")]
    NotTwoFields { payload: QuotedNotice },
    /// The first field is not a UUID.
    #[error("the task id of the cancel notice '{payload}' is not a UUID")]
    TaskIdInvalid { payload: QuotedNotice },
    /// The second field is not a whole number of microseconds.
    #[error("the claim generation of the cancel notice '{payload}' is not a whole number")]
    ClaimGenerationInvalid { payload: QuotedNotice },
    /// The second field is outside the range of a time.
    #[error(
        "the claim generation of the cancel notice '{payload}' is outside the range of a \
         time"
    )]
    ClaimGenerationOutOfRange { payload: QuotedNotice },
}

/// The run of one cancel notice: `<task id> <claim generation>`, the claim
/// generation in whole microseconds since the Unix epoch (the
/// `claimed_at` of the row, which the database keeps to the microsecond).
/// Pure.
///
/// Errors: [`NoticeRefused`].
pub fn parse_cancel_notice(payload: &str) -> Result<RunClaim, NoticeRefused> {
    let quoted = || QuotedNotice::cut(payload);
    let Some((task_text, micros_text)) = payload.split_once(' ') else {
        return Err(NoticeRefused::NotTwoFields { payload: quoted() });
    };
    match micros_text.contains(' ') {
        true => return Err(NoticeRefused::NotTwoFields { payload: quoted() }),
        false => {}
    }
    let task_id = Uuid::parse_str(task_text)
        .map_err(|_| NoticeRefused::TaskIdInvalid { payload: quoted() })?;
    let micros: i64 = micros_text
        .parse()
        .map_err(|_| NoticeRefused::ClaimGenerationInvalid { payload: quoted() })?;
    let claimed_at: DateTime<Utc> = DateTime::from_timestamp_micros(micros)
        .ok_or_else(|| NoticeRefused::ClaimGenerationOutOfRange { payload: quoted() })?;
    Ok(RunClaim {
        task_id,
        claimed_at,
    })
}

/// Where the router reads the runs whose task an operator cancelled: the
/// broker of the worker, or a scripted reader in a test.
pub trait CancelledRunsReader: Send + Sync {
    /// The runs of `runs` (at most [`MAX_CANCELLED_RUNS_READ`]) that an
    /// operator cancelled at their claim generation.
    ///
    /// Errors: [`CancelledRunsReadError`]; nothing was read.
    fn read_cancelled(
        &self,
        runs: &[RunClaim],
    ) -> impl Future<Output = Result<Vec<RunClaim>, CancelledRunsReadError>> + Send;
}

impl CancelledRunsReader for PostgresBroker {
    fn read_cancelled(
        &self,
        runs: &[RunClaim],
    ) -> impl Future<Output = Result<Vec<RunClaim>, CancelledRunsReadError>> + Send {
        self.read_cancelled_runs(runs)
    }
}

/// What woke the router.
#[derive(Debug)]
pub enum RouterStep {
    /// An event of the notice subscription.
    Notice(PayloadEvent),
    /// The held runs with no cancel whose beat did not land.
    BeatMisses(Vec<RunClaim>),
}

/// Whether the router goes on after a step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepEnd {
    Continue,
    /// The listener stopped: no notice comes again.
    ListenerStopped,
}

/// What one route did, for the log and the tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Routed {
    pub run: RunClaim,
    pub routing: CancelRouting,
}

/// Route the cancel of `run` to the slot that holds it.
///
/// A run that this worker holds is logged with its attempt. A notice of a
/// run that it does not hold names no attempt of this worker: every worker
/// gets every notice, so its event names the run as the notice does
/// (`notice_task_id`, `notice_claimed_at`), not as an attempt (`task_id`).
pub fn route_cancel(live: &LiveDispatches, run: RunClaim) -> Routed {
    let routing = live.request_cancel(run.task_id, run.claimed_at);
    match routing {
        CancelRouting::Routed { log } => tracing::info!(
            task_id = %run.task_id,
            attempt = log.attempt.get(),
            trace_id = log.trace_id.log_field(),
            claimed_at = %run.claimed_at,
            "an operator cancelled a task that this worker runs; telling its child",
        ),
        CancelRouting::NotHeld => tracing::debug!(
            notice_task_id = %run.task_id,
            notice_claimed_at = %run.claimed_at,
            "a cancel notice names a run that this worker does not hold",
        ),
        CancelRouting::AlreadyRequested { log } => tracing::debug!(
            task_id = %run.task_id,
            attempt = log.attempt.get(),
            trace_id = log.trace_id.log_field(),
            claimed_at = %run.claimed_at,
            "the cancel of this run was routed already",
        ),
    }
    Routed { run, routing }
}

/// Read which of `runs` an operator cancelled, in parts of at most
/// [`MAX_CANCELLED_RUNS_READ`], and route each. A part whose read fails is
/// logged and skipped. Returns what each route did.
pub async fn read_and_route<R: CancelledRunsReader>(
    live: &LiveDispatches,
    reader: &R,
    runs: &[RunClaim],
) -> Vec<Routed> {
    let mut routed = Vec::new();
    for part in runs.chunks(MAX_CANCELLED_RUNS_READ) {
        match reader.read_cancelled(part).await {
            Ok(cancelled) => {
                routed.extend(cancelled.into_iter().map(|run| route_cancel(live, run)));
            }
            Err(error) => tracing::warn!(
                %error,
                runs = part.len(),
                "the read of cancelled runs failed; the next listener gap or beat miss reads \
                 again: {error}",
            ),
        }
    }
    routed
}

/// Act on one step of the router. Returns what each route of the step did,
/// and whether the router goes on.
pub async fn on_step<R: CancelledRunsReader>(
    step: RouterStep,
    live: &LiveDispatches,
    reader: &R,
) -> (StepEnd, Vec<Routed>) {
    match step {
        RouterStep::Notice(PayloadEvent::Payload(payload)) => match parse_cancel_notice(&payload) {
            Ok(run) => (StepEnd::Continue, vec![route_cancel(live, run)]),
            Err(refused) => {
                tracing::warn!(
                    %refused,
                    "a cancel notice did not parse; reading the cancels of the held runs: \
                     {refused}",
                );
                let held = live.cancel_candidates();
                (StepEnd::Continue, read_and_route(live, reader, &held).await)
            }
        },
        RouterStep::Notice(PayloadEvent::Gap | PayloadEvent::Overflowed) => {
            tracing::debug!(
                "cancel notices may have been lost; reading the cancels of the held runs"
            );
            let held = live.cancel_candidates();
            (StepEnd::Continue, read_and_route(live, reader, &held).await)
        }
        RouterStep::Notice(PayloadEvent::Stopped) => (StepEnd::ListenerStopped, Vec::new()),
        RouterStep::BeatMisses(misses) => (
            StepEnd::Continue,
            read_and_route(live, reader, &misses).await,
        ),
    }
}

/// Start the router: it routes the notices of `notices` and the beat misses
/// of `live` until `cancel` fires or the listener stops. The worker
/// supervises the task: an end with no stop request stops the worker, as
/// the ping responder that shares the listener.
pub fn spawn_cancel_router(
    notices: PayloadSubscription,
    live: LiveDispatches,
    broker: Arc<PostgresBroker>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let step = tokio::select! {
                () = cancel.cancelled() => return,
                event = notices.recv() => RouterStep::Notice(event),
                misses = live.next_beat_misses() => RouterStep::BeatMisses(misses),
            };
            match on_step(step, &live, broker.as_ref()).await {
                (StepEnd::Continue, _routed) => {}
                (StepEnd::ListenerStopped, _) => {
                    tracing::warn!("the listener stopped; the cancel router ends");
                    return;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests;

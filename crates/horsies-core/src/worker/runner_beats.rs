//! Runner heartbeats of the worker parent (Python `core/worker/runner_heartbeat.py`).
//!
//! - The writer beats every task the worker holds, once per
//!   `runner_heartbeat_interval_ms`, in one fenced statement, on its own
//!   connection outside the shared pool, so a busy pool cannot starve it.
//! - A beat lands only on a row that is RUNNING, owned by this worker, at the
//!   claim generation of the dispatch, and without the finalizing marker.
//! - With `auto_fail_stale_running` on, a watchdog on its own OS thread ends
//!   the process with [`RUNNER_HEARTBEAT_LOST_EXIT_CODE`] when no beat lands
//!   for half of `running_stale_threshold_ms`, so no task runs without a beat
//!   when the reaper fails its row. The watchdog does not depend on the async
//!   runtime: a runtime blocked by task code cannot keep it from acting.
//! - With it off, no reaper fails a RUNNING row: late beats are logged and
//!   the worker runs on.

use std::collections::HashMap;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::core::config::recovery::{RecoveryConfig, FAIL_CLOSED_WINDOW_DIVISOR};

/// Exit code of the worker process when it failed closed because its runner
/// heartbeats stopped. No other horsies process uses it. The core parent
/// owns exit codes 76 and 77.
pub const RUNNER_HEARTBEAT_LOST_EXIT_CODE: i32 = 77;

/// How often the watchdog reads the time since the last beat.
const WATCH_INTERVAL: Duration = Duration::from_millis(250);

/// The fail-closed window ends at least this long before the reaper may count
/// the last beat as stale: two watch intervals. It is below the smallest beat
/// interval (1 s), so a beat that lands on time never shortens the window.
const FAIL_CLOSED_KILL_MARGIN: Duration = Duration::from_millis(500);

/// Wait before the first retry after a failed write; it doubles per failure,
/// at most [`BEAT_RETRY_DOUBLINGS_MAX`] times, up to [`BEAT_RETRY_MAX_DELAY`]
/// and never above one beat interval.
const BEAT_RETRY_FIRST_DELAY: Duration = Duration::from_millis(250);
const BEAT_RETRY_DOUBLINGS_MAX: u32 = 5;
const BEAT_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);

/// Failed writes and late beats are logged at most once per this time.
const BEAT_FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// With stale running recovery off, a beat is late after this many intervals.
const LATE_BEAT_INTERVALS: u32 = 2;

const WATCH_THREAD_NAME: &str = "horsies-runner-heartbeat-watch";

/// One statement per interval for every held dispatch. Each row is fenced
/// like every other write of a dispatch: this worker, and the claim
/// generation of the dispatch. A row with the finalizing marker gets no beat:
/// its attempt has ended, and the reaper reads the marker for that row.
const INSERT_RUNNER_HEARTBEATS_SQL: &str = "\
INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
SELECT t.id, $1::varchar, 'runner', NOW(), $2::varchar, $3::integer
FROM unnest($4::uuid[], $5::timestamptz[]) AS d(task_id, claimed_at)
JOIN horsies_tasks t ON t.id = d.task_id
WHERE t.status = 'RUNNING'
  AND t.claimed_by_worker_id = $1::varchar
  AND t.claimed_at = d.claimed_at
  AND t.finalizing_at IS NULL";

/// The dispatches this worker holds: task id and claim generation.
///
/// A dispatch is held from its start until its outcome is written; the
/// [`HeldDispatch`] guard removes it when dropped.
#[derive(Clone, Default)]
pub struct LiveDispatches {
    held: Arc<Mutex<HashMap<Uuid, DateTime<Utc>>>>,
}

/// One held dispatch. Dropping it ends the runner beats of the task.
pub struct HeldDispatch {
    live: LiveDispatches,
    task_id: Uuid,
}

impl LiveDispatches {
    /// Start beating `task_id` at the claim generation `claimed_at`.
    pub fn hold(&self, task_id: Uuid, claimed_at: DateTime<Utc>) -> HeldDispatch {
        // A poisoned lock means a panic while the map was held; the map
        // itself is still a valid set of ids, so it is used as it is.
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(task_id, claimed_at);
        HeldDispatch {
            live: self.clone(),
            task_id,
        }
    }

    /// The held task ids and their claim generations, in matching order.
    fn snapshot(&self) -> (Vec<Uuid>, Vec<DateTime<Utc>>) {
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(task_id, claimed_at)| (*task_id, *claimed_at))
            .unzip()
    }
}

impl Drop for HeldDispatch {
    fn drop(&mut self) {
        self.live
            .held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.task_id);
    }
}

/// Whether a reaper of this config fails RUNNING rows whose beats stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleRunningRecovery {
    /// The reaper fails a RUNNING row with no runner beat for `threshold`;
    /// the worker fails closed when no beat lands for half of it.
    On { threshold: Duration },
    /// No reaper fails a RUNNING row: a beat that cannot land puts no running
    /// task at risk, so the worker logs it and runs on.
    Off,
}

impl StaleRunningRecovery {
    /// Read the rule from the worker's recovery config.
    pub fn from_config(recovery: &RecoveryConfig) -> Self {
        match recovery.auto_fail_stale_running {
            true => Self::On {
                threshold: Duration::from_millis(recovery.running_stale_threshold_ms),
            },
            false => Self::Off,
        }
    }
}

/// Half of the stale threshold: the time with no landed beat after which the
/// worker fails closed.
fn fail_closed_window(threshold: Duration) -> Duration {
    threshold / FAIL_CLOSED_WINDOW_DIVISOR as u32
}

/// Where the fail-closed window of a landed beat starts: at the landing,
/// except for a beat whose write took longer than the window less the kill
/// margin. Its window starts earlier, so it ends [`FAIL_CLOSED_KILL_MARGIN`]
/// before the threshold counted from `started_at`. The reaper counts the beat
/// from its `sent_at`, which is not earlier than the start of the write.
fn window_start(threshold: Duration, started_at: Instant, landed_at: Instant) -> Instant {
    let latest = started_at
        + threshold.saturating_sub(FAIL_CLOSED_KILL_MARGIN + fail_closed_window(threshold));
    landed_at.min(latest)
}

/// The retry wait after `consecutive_failures` failed writes.
fn retry_delay(consecutive_failures: u32, interval: Duration) -> Duration {
    let doublings = consecutive_failures
        .saturating_sub(1)
        .min(BEAT_RETRY_DOUBLINGS_MAX);
    (BEAT_RETRY_FIRST_DELAY * 2u32.pow(doublings))
        .min(BEAT_RETRY_MAX_DELAY)
        .min(interval)
}

/// The last write that failed, or none since the last landed beat.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LastFailure {
    NoFailure,
    WriteFailed(String),
}

/// What the writer last achieved, shared with the watchdog thread.
#[derive(Debug, Clone)]
struct Coverage {
    /// From when the watchdog counts: the window start of the last landed
    /// beat, or the time of a pass that held no dispatch.
    covered_at: Instant,
    consecutive_failures: u32,
    last_failure: LastFailure,
}

#[derive(Clone)]
struct SharedCoverage(Arc<Mutex<Coverage>>);

impl SharedCoverage {
    fn new(now: Instant) -> Self {
        Self(Arc::new(Mutex::new(Coverage {
            covered_at: now,
            consecutive_failures: 0,
            last_failure: LastFailure::NoFailure,
        })))
    }

    fn read(&self) -> Coverage {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Record a covered pass; returns the failures it ended.
    fn record_covered(&self, covered_at: Instant) -> u32 {
        let mut coverage = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let ended = coverage.consecutive_failures;
        *coverage = Coverage {
            covered_at,
            consecutive_failures: 0,
            last_failure: LastFailure::NoFailure,
        };
        ended
    }

    /// Record a failed write; returns the failures in a row.
    fn record_failure(&self, detail: String) -> u32 {
        let mut coverage = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        coverage.consecutive_failures = coverage.consecutive_failures.saturating_add(1);
        coverage.last_failure = LastFailure::WriteFailed(detail);
        coverage.consecutive_failures
    }
}

/// Who writes the beats: the worker's identity on each heartbeat row.
#[derive(Debug, Clone)]
pub struct BeatSender {
    pub worker_id: String,
    pub hostname: String,
    pub pid: i32,
}

/// Write one runner-beat statement for the given dispatches; returns the
/// number of rows that passed the fence. It does not retry.
async fn write_beats(
    pool: &PgPool,
    sender: &BeatSender,
    task_ids: &[Uuid],
    claim_generations: &[DateTime<Utc>],
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(INSERT_RUNNER_HEARTBEATS_SQL)
        .bind(&sender.worker_id)
        .bind(&sender.hostname)
        .bind(sender.pid)
        .bind(task_ids)
        .bind(claim_generations)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

/// A one-connection pool for the beats, built from the runtime pool's
/// settings. The connection opens at the first write and again after a
/// failure; it never waits behind the shared pool.
pub fn runner_beat_pool(runtime_pool: &PgPool) -> PgPool {
    runtime_pool
        .options()
        .clone()
        .min_connections(0)
        .max_connections(1)
        .connect_lazy_with((*runtime_pool.connect_options()).clone())
}

/// The watchdog thread of the runner beats. Dropping it does not stop the
/// thread; call [`RunnerBeatWatch::stop`].
pub struct RunnerBeatWatch {
    stop_watch: Sender<()>,
    watch: std::thread::JoinHandle<()>,
}

/// Start the beat writer on the current runtime and its watchdog on its own
/// thread. Returns the watchdog and the writer task, which the worker
/// supervises with its other service loops. The writer stops when `cancel`
/// fires; the watchdog when [`RunnerBeatWatch::stop`] runs.
///
/// Errors: the OS refused to start the watchdog thread; the writer is not
/// started then.
pub fn launch_runner_beats(
    pool: PgPool,
    sender: BeatSender,
    interval: Duration,
    stale_running: StaleRunningRecovery,
    live: LiveDispatches,
    cancel: CancellationToken,
) -> std::io::Result<(RunnerBeatWatch, JoinHandle<()>)> {
    let coverage = SharedCoverage::new(Instant::now());
    let (stop_watch, stop_received) = std::sync::mpsc::channel::<()>();
    let watch_coverage = coverage.clone();
    let watch = std::thread::Builder::new()
        .name(WATCH_THREAD_NAME.to_owned())
        .spawn(move || run_watch(stale_running, interval, &watch_coverage, &stop_received))?;
    let writer = tokio::spawn(run_beats(
        pool,
        sender,
        interval,
        stale_running,
        live,
        coverage,
        cancel,
    ));
    Ok((RunnerBeatWatch { stop_watch, watch }, writer))
}

impl RunnerBeatWatch {
    /// Stop the watchdog and wait for its thread. Call it after every
    /// dispatch has ended: from then on no held task needs a beat.
    pub fn stop(self) {
        // A send error means the watchdog already ended.
        let _ignored = self.stop_watch.send(());
        if self.watch.join().is_err() {
            tracing::error!("the runner heartbeat watchdog thread panicked");
        }
    }
}

/// The writer loop: wait one interval (the first beat is written with the
/// RUNNING transition), then beat; a failed write is retried sooner.
async fn run_beats(
    pool: PgPool,
    sender: BeatSender,
    interval: Duration,
    stale_running: StaleRunningRecovery,
    live: LiveDispatches,
    coverage: SharedCoverage,
    cancel: CancellationToken,
) {
    let mut failure_log = LogGate::new(BEAT_FAILURE_LOG_INTERVAL);
    let mut delay = interval;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(delay) => {}
        }
        delay = beat_once(
            &pool,
            &sender,
            interval,
            stale_running,
            &live,
            &coverage,
            &mut failure_log,
        )
        .await;
    }
    pool.close().await;
}

/// One beat pass; returns the wait before the next pass.
async fn beat_once(
    pool: &PgPool,
    sender: &BeatSender,
    interval: Duration,
    stale_running: StaleRunningRecovery,
    live: &LiveDispatches,
    coverage: &SharedCoverage,
    failure_log: &mut LogGate,
) -> Duration {
    let started_at = Instant::now();
    let (task_ids, claim_generations) = live.snapshot();
    if task_ids.is_empty() {
        // No held task, so none is at risk.
        let _ended = coverage.record_covered(started_at);
        return interval;
    }
    match write_beats(pool, sender, &task_ids, &claim_generations).await {
        Ok(_rows) => {
            let landed_at = Instant::now();
            let counts_from = match stale_running {
                StaleRunningRecovery::On { threshold } => {
                    window_start(threshold, started_at, landed_at)
                }
                StaleRunningRecovery::Off => landed_at,
            };
            let ended = coverage.record_covered(counts_from);
            if ended > 0 {
                tracing::info!(failed_attempts = ended, "runner heartbeat write recovered");
                failure_log.reset();
            }
            (started_at + interval).saturating_duration_since(landed_at)
        }
        Err(error) => {
            let failures = coverage.record_failure(error.to_string());
            if failures == 1 {
                failure_log.reset();
            }
            if failure_log.due(Instant::now()) {
                let consequence = match stale_running {
                    StaleRunningRecovery::On { threshold } => format!(
                        "the worker stops at {:.1}s without a landed beat",
                        fail_closed_window(threshold).as_secs_f64()
                    ),
                    StaleRunningRecovery::Off => {
                        "no reaper fails a RUNNING row, so the worker runs on".to_owned()
                    }
                };
                tracing::error!(
                    attempt = failures,
                    held_tasks = task_ids.len(),
                    %error,
                    "runner heartbeat write failed; {consequence}",
                );
            }
            retry_delay(failures, interval)
        }
    }
}

/// Logs at most once per interval; a reset allows the next log at once.
struct LogGate {
    interval: Duration,
    last: Option<Instant>,
}

impl LogGate {
    fn new(interval: Duration) -> Self {
        Self {
            interval,
            last: None,
        }
    }

    fn due(&mut self, now: Instant) -> bool {
        match self.last {
            Some(at) if now.saturating_duration_since(at) < self.interval => false,
            Some(_) | None => {
                self.last = Some(now);
                true
            }
        }
    }

    fn reset(&mut self) {
        self.last = None;
    }
}

/// What one watchdog check found.
#[derive(Debug, Clone, PartialEq)]
enum WatchVerdict {
    /// A beat landed within the window.
    Current,
    /// No beat landed for `uncovered`, past `window`.
    Overdue {
        uncovered: Duration,
        window: Duration,
        last_failure: LastFailure,
    },
}

/// Compare the time since the last covered pass with `window`.
fn check_coverage(coverage: &Coverage, window: Duration, now: Instant) -> WatchVerdict {
    let uncovered = now.saturating_duration_since(coverage.covered_at);
    match uncovered > window {
        true => WatchVerdict::Overdue {
            uncovered,
            window,
            last_failure: coverage.last_failure.clone(),
        },
        false => WatchVerdict::Current,
    }
}

/// How a watch with stale running recovery on ended.
#[derive(Debug, Clone, PartialEq)]
enum WatchEnd {
    /// The worker asked the watchdog to stop.
    Stopped,
    /// No beat landed within the fail-closed window: the process must end.
    FailClosed(WatchVerdict),
}

/// Check every [`WATCH_INTERVAL`] until a stop or an overdue beat.
fn watch_until_end(window: Duration, coverage: &SharedCoverage, stop: &Receiver<()>) -> WatchEnd {
    loop {
        match stop.recv_timeout(WATCH_INTERVAL) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => return WatchEnd::Stopped,
            Err(RecvTimeoutError::Timeout) => {}
        }
        match check_coverage(&coverage.read(), window, Instant::now()) {
            WatchVerdict::Current => {}
            overdue @ WatchVerdict::Overdue { .. } => return WatchEnd::FailClosed(overdue),
        }
    }
}

/// The watchdog thread body.
fn run_watch(
    stale_running: StaleRunningRecovery,
    interval: Duration,
    coverage: &SharedCoverage,
    stop: &Receiver<()>,
) {
    match stale_running {
        StaleRunningRecovery::On { threshold } => {
            let window = fail_closed_window(threshold);
            match watch_until_end(window, coverage, stop) {
                WatchEnd::Stopped => {}
                WatchEnd::FailClosed(verdict) => fail_closed(&verdict),
            }
        }
        StaleRunningRecovery::Off => watch_and_log(interval * LATE_BEAT_INTERVALS, coverage, stop),
    }
}

/// End the process: no runner beat landed within the fail-closed window, so
/// the reaper may fail rows whose attempts still run here. Task code runs in
/// this process, so ending the process ends every attempt it holds.
fn fail_closed(verdict: &WatchVerdict) {
    tracing::error!(
        exit_code = RUNNER_HEARTBEAT_LOST_EXIT_CODE,
        cause = %describe(verdict),
        "worker failed closed: runner heartbeats stopped; ending the process",
    );
    std::process::exit(RUNNER_HEARTBEAT_LOST_EXIT_CODE);
}

fn describe(verdict: &WatchVerdict) -> String {
    match verdict {
        WatchVerdict::Current => "runner heartbeats are current".to_owned(),
        WatchVerdict::Overdue {
            uncovered,
            window,
            last_failure,
        } => {
            let last = match last_failure {
                LastFailure::NoFailure => "the last write has not returned".to_owned(),
                LastFailure::WriteFailed(detail) => format!("last error: {detail}"),
            };
            format!(
                "no runner heartbeat landed for {:.1}s, past {:.1}s (half of \
                 running_stale_threshold_ms); {last}",
                uncovered.as_secs_f64(),
                window.as_secs_f64(),
            )
        }
    }
}

/// With stale running recovery off: log late beats at most once per log
/// interval, never stop the worker.
fn watch_and_log(late_after: Duration, coverage: &SharedCoverage, stop: &Receiver<()>) {
    let mut late_log = LogGate::new(BEAT_FAILURE_LOG_INTERVAL);
    loop {
        match stop.recv_timeout(WATCH_INTERVAL) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
            Err(RecvTimeoutError::Timeout) => {}
        }
        let now = Instant::now();
        match check_coverage(&coverage.read(), late_after, now) {
            WatchVerdict::Current => late_log.reset(),
            verdict @ WatchVerdict::Overdue { .. } => {
                if late_log.due(now) {
                    tracing::warn!(
                        cause = %describe(&verdict),
                        "runner heartbeats are late; the worker runs on, because no \
                         reaper fails a RUNNING row (auto_fail_stale_running is off)",
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn held_dispatches_are_beaten_until_the_guard_drops() {
        let live = LiveDispatches::default();
        let task_id = Uuid::new_v4();
        let claimed_at = Utc::now();
        let held = live.hold(task_id, claimed_at);
        assert_eq!(live.snapshot(), (vec![task_id], vec![claimed_at]));
        drop(held);
        assert_eq!(live.snapshot(), (Vec::new(), Vec::new()));
    }

    /// Python `test_runner_heartbeat.py`: the fail-closed window is half the
    /// threshold, and it holds one interval plus the landing allowance at the
    /// smallest threshold the config accepts.
    #[test]
    fn the_fail_closed_window_is_half_the_threshold() {
        let recovery = RecoveryConfig {
            runner_heartbeat_interval_ms: 1_000,
            running_stale_threshold_ms:
                crate::core::config::recovery::fail_closed_threshold_floor_ms(1_000),
            ..RecoveryConfig::default()
        };
        let StaleRunningRecovery::On { threshold } = StaleRunningRecovery::from_config(&recovery)
        else {
            panic!("auto_fail_stale_running is on by default");
        };
        assert_eq!(
            fail_closed_window(threshold),
            Duration::from_millis(
                1_000 + crate::core::config::recovery::RUNNER_BEAT_LANDING_ALLOWANCE_MS
            ),
        );

        let off = RecoveryConfig {
            auto_fail_stale_running: false,
            ..recovery
        };
        assert_eq!(
            StaleRunningRecovery::from_config(&off),
            StaleRunningRecovery::Off
        );
    }

    /// A slow write moves its window start earlier, so the window still ends
    /// the kill margin before the threshold counted from the write's start.
    #[test]
    fn a_slow_beat_starts_its_window_early() {
        let threshold = Duration::from_secs(64);
        let started_at = Instant::now();
        let quick = started_at + Duration::from_millis(100);
        assert_eq!(window_start(threshold, started_at, quick), quick);

        let slow = started_at + Duration::from_secs(40);
        let start = window_start(threshold, started_at, slow);
        assert!(start < slow);
        assert_eq!(
            start + fail_closed_window(threshold),
            started_at + threshold - FAIL_CLOSED_KILL_MARGIN,
        );
    }

    #[test]
    fn retry_delay_doubles_up_to_its_bounds() {
        let interval = Duration::from_secs(30);
        assert_eq!(retry_delay(1, interval), Duration::from_millis(250));
        assert_eq!(retry_delay(2, interval), Duration::from_millis(500));
        assert_eq!(retry_delay(5, interval), Duration::from_secs(4));
        assert_eq!(retry_delay(6, interval), BEAT_RETRY_MAX_DELAY);
        assert_eq!(retry_delay(u32::MAX, interval), BEAT_RETRY_MAX_DELAY);
        assert_eq!(
            retry_delay(6, Duration::from_secs(1)),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn a_covered_pass_is_current_and_a_late_one_is_overdue() {
        let now = Instant::now();
        let coverage = SharedCoverage::new(now);
        let window = Duration::from_secs(10);
        assert_eq!(
            check_coverage(&coverage.read(), window, now + Duration::from_secs(9)),
            WatchVerdict::Current,
        );

        assert_eq!(coverage.record_failure("connection refused".to_owned()), 1);
        assert_eq!(coverage.record_failure("connection refused".to_owned()), 2);
        let verdict = check_coverage(&coverage.read(), window, now + Duration::from_secs(11));
        assert_eq!(
            verdict,
            WatchVerdict::Overdue {
                uncovered: Duration::from_secs(11),
                window,
                last_failure: LastFailure::WriteFailed("connection refused".to_owned()),
            },
        );
        assert!(describe(&verdict).contains("last error: connection refused"));

        assert_eq!(coverage.record_covered(now + Duration::from_secs(11)), 2);
        assert_eq!(
            check_coverage(&coverage.read(), window, now + Duration::from_secs(12)),
            WatchVerdict::Current,
        );
    }

    /// The watch ends with a fail-closed verdict once the window passes with
    /// no landed beat, and with `Stopped` when the worker stops it first.
    #[test]
    fn the_watch_fails_closed_when_no_beat_lands_within_the_window() {
        let coverage = SharedCoverage::new(Instant::now());
        let (_stop_sender, stop_received) = std::sync::mpsc::channel::<()>();
        let end = watch_until_end(Duration::from_millis(300), &coverage, &stop_received);
        assert!(matches!(
            end,
            WatchEnd::FailClosed(WatchVerdict::Overdue {
                last_failure: LastFailure::NoFailure,
                ..
            })
        ));

        let (stop_sender, stop_received) = std::sync::mpsc::channel::<()>();
        stop_sender.send(()).expect("watch receiver is alive");
        assert_eq!(
            watch_until_end(Duration::from_secs(3_600), &coverage, &stop_received),
            WatchEnd::Stopped,
        );
    }

    #[test]
    fn the_runner_beat_statement_is_fenced() {
        for fence in [
            "t.status = 'RUNNING'",
            "t.claimed_by_worker_id = $1::varchar",
            "t.claimed_at = d.claimed_at",
            "t.finalizing_at IS NULL",
        ] {
            assert!(INSERT_RUNNER_HEARTBEATS_SQL.contains(fence), "{fence}");
        }
    }

    /// A RUNNING row on the baseline schema, owned by `owner` at the claim
    /// generation `claimed_at`, with or without the finalizing marker.
    async fn seed_running_task(
        pool: &PgPool,
        task_id: Uuid,
        owner: &str,
        claimed_at: DateTime<Utc>,
        finalizing: bool,
    ) {
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, task_options, execution,
                retention_class_key, sent_at, created_at, command_fingerprint_version,
                command_fingerprint, idempotency_key_digest, status, enqueued_at,
                claimed_at, claimed_by_worker_id, claim_expires_at, started_at,
                finalizing_at, finalizing_by_worker_id, worker_pid, worker_hostname,
                worker_process_name, retry_count, max_retries, next_retry_at, good_until,
                is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id,
                rerun_root_task_id
            ) VALUES (
                $1, 'runner_beat_test', 'default', 100, '{}', NULL, 'ISOLATED',
                'standard_30d', NOW(), NOW(), 1,
                decode(repeat('00', 32), 'hex'), NULL, 'RUNNING', NOW(),
                $2, $3, NULL, NOW(),
                CASE WHEN $4 THEN NOW() END, CASE WHEN $4 THEN $3 END, 1, 'host',
                'worker', 0, 0, NULL, NULL,
                FALSE, NOW(), FALSE, NULL,
                NULL
            )",
        )
        .bind(task_id)
        .bind(claimed_at)
        .bind(owner)
        .bind(finalizing)
        .execute(pool)
        .await
        .unwrap();
    }

    /// Python `test_runner_heartbeat_writer.py`: one statement beats the
    /// held RUNNING rows of this worker at their claim generation, and no row
    /// of another generation, another worker, or with the finalizing marker.
    #[tokio::test]
    #[serial_test::serial]
    async fn one_statement_beats_only_rows_that_pass_the_fence() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        // The heartbeat table has no default partition: the current hour
        // needs its leaf, as at worker start.
        crate::core::history::maintenance::coverage::ensure_startup_coverage_in_pool(
            &pool,
            2,
            2,
            &[],
            &crate::core::history::reads::publisher::StagedLoaderPublisher,
        )
        .await
        .expect("partition coverage");
        let ids: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
        let claimed_at: DateTime<Utc> =
            sqlx::query_scalar("SELECT date_trunc('milliseconds', now())")
                .fetch_one(&pool)
                .await
                .unwrap();
        // ids[0]: passes. ids[1]: other worker. ids[2]: finalizing.
        // ids[3]: other claim generation.
        for (index, id) in ids.iter().enumerate() {
            let owner = match index {
                1 => "other-worker",
                _ => "beat-worker",
            };
            let generation = match index {
                3 => claimed_at - chrono::Duration::seconds(5),
                _ => claimed_at,
            };
            seed_running_task(&pool, *id, owner, generation, index == 2).await;
        }
        let sender = BeatSender {
            worker_id: "beat-worker".to_owned(),
            hostname: "beat-host".to_owned(),
            pid: 7,
        };
        let generations = vec![claimed_at; ids.len()];
        let rows = write_beats(&pool, &sender, &ids, &generations)
            .await
            .unwrap();
        assert_eq!(rows, 1);
        let beaten: Vec<Uuid> = sqlx::query_scalar(
            "SELECT task_id FROM horsies_heartbeats
             WHERE role = 'runner' AND sender_id = 'beat-worker' AND task_id = ANY($1)",
        )
        .bind(&ids)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(beaten, vec![ids[0]]);
        sqlx::query("DELETE FROM horsies_heartbeats WHERE task_id = ANY($1)")
            .bind(&ids)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_tasks WHERE id = ANY($1)")
            .bind(&ids)
            .execute(&pool)
            .await
            .unwrap();
    }
}

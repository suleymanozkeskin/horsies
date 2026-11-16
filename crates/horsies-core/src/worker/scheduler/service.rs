use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::broker::PostgresBroker;
use crate::core::config::{AppConfig, ScheduleConfig, TaskSchedule};

use super::calculator::{next_run_at, should_run_now};
use super::state;

/// Fixed namespace UUID for schedule-derived deterministic task IDs.
/// Matches Python's `_SCHEDULE_NAMESPACE`. Never change this value.
const SCHEDULE_NAMESPACE: Uuid = Uuid::from_bytes([
    0x3c, 0x01, 0xf3, 0xf5, 0xaf, 0xd6, 0x43, 0x63, 0xb7, 0x26, 0xa5, 0xda, 0xb5, 0x1a, 0x81, 0xc7,
]);

/// Wall-clock target between missing-row existence checks while every state
/// row is present and initialized. The check guards rare conditions (startup
/// init failure, external row deletion); running it every tick reads the whole
/// schedule-state table each tick for an answer that changes only on those
/// rare events. Denominated in seconds, not ticks, so the worst-case dormancy
/// bound for an externally deleted row does not scale with
/// `check_interval_seconds`. While any row is missing or a re-init failed, the
/// check reruns every tick until a fully healthy pass.
const EXISTENCE_CHECK_INTERVAL_S: u32 = 60;

/// Cadence gate for the missing-row existence check.
///
/// Reproduces Python's countdown: a check runs when the countdown hits zero;
/// a healthy pass re-arms the full interval, an unhealthy pass re-checks on
/// the very next tick.
struct ExistenceCheckCadence {
    /// Ticks between checks while healthy, derived once from the tick length.
    interval_ticks: u32,
    /// Zero means the next tick runs the check.
    ticks_until_check: u32,
}

impl ExistenceCheckCadence {
    fn new(check_interval_seconds: u32) -> Self {
        let seconds = check_interval_seconds.max(1);
        let interval_ticks =
            ((f64::from(EXISTENCE_CHECK_INTERVAL_S) / f64::from(seconds)).round() as u32).max(1);
        Self {
            interval_ticks,
            ticks_until_check: 0,
        }
    }

    /// True when this tick must run the existence check.
    fn should_check(&self) -> bool {
        self.ticks_until_check == 0
    }

    /// Record a completed check's health and start the next countdown.
    fn record(&mut self, healthy: bool) {
        self.ticks_until_check = if healthy { self.interval_ticks } else { 1 };
    }

    /// Advance one tick.
    fn tick(&mut self) {
        self.ticks_until_check = self.ticks_until_check.saturating_sub(1);
    }
}

/// Resolve the effective priority for a queue, mirroring [`Horsies::effective_priority()`].
///
/// Priority resolution:
/// 1. Queue priority from [`CustomQueueConfig`] (if Custom mode and the queue has a config entry)
/// 2. Default priority (100)
fn resolve_queue_priority(app_config: &AppConfig, queue_name: &str) -> i32 {
    if let Some(ref queues) = app_config.custom_queues {
        if let Some(q) = queues.iter().find(|q| q.name == queue_name) {
            return q.priority as i32;
        }
    }
    100
}

/// Why the scheduler refused to start. Nothing is written before a refusal.
#[derive(Debug, thiserror::Error)]
pub enum SchedulerStartError {
    /// The schedule section is invalid (pattern bounds, timezone, names).
    #[error("invalid schedule config: {0}")]
    InvalidScheduleConfig(String),
    /// An enabled schedule's pattern has no slot after the start time, so its
    /// `next_run_at` (NOT NULL) cannot be stored.
    #[error("schedule '{schedule}' has no run after {after}: its pattern never fires again")]
    NoNextSlot {
        schedule: String,
        after: DateTime<Utc>,
    },
}

/// Why one slot was not enqueued.
#[derive(Debug, thiserror::Error)]
enum ScheduleFireError {
    /// The schedule's input is over `payload.reject_bytes`.
    #[error(
        "payload for schedule '{schedule}' is {size} bytes, exceeding \
         payload.reject_bytes={limit}; slot not enqueued"
    )]
    PayloadTooLarge {
        schedule: String,
        size: usize,
        limit: u64,
    },
    /// The schedule's input does not encode to JSON text.
    #[error("input of schedule '{schedule}' does not encode to JSON: {source}")]
    InputEncoding {
        schedule: String,
        #[source]
        source: serde_json::Error,
    },
    /// The broker refused or failed the enqueue.
    #[error("enqueue for schedule '{schedule}' failed: {source}")]
    Broker {
        schedule: String,
        #[source]
        source: crate::broker::BrokerError,
    },
}

/// What one slot's enqueue did, as the fire rules read it.
enum SlotOutcome {
    /// The slot's task row was written, or already holds the same command.
    Enqueued(Uuid),
    /// The slot's task id already holds a row with a different command. The
    /// slot has its task, so it counts as fired (Python
    /// `_count_existing_slot_as_fired`).
    HeldByOtherCommand(Uuid),
    /// The enqueue failed with an error that may clear: the slot stays due
    /// and the next tick fires it once.
    Transient(ScheduleFireError),
    /// The enqueue failed with an error that repeats on every try: the
    /// schedule moves past this slot and records no run.
    Permanent(ScheduleFireError),
}

impl ScheduleFireError {
    /// True when the failure may clear on a later try: only a retryable
    /// broker error. Payload and encoding refusals repeat on every try.
    fn is_retryable(&self) -> bool {
        match self {
            Self::Broker { source, .. } => source.is_retryable(),
            Self::PayloadTooLarge { .. } | Self::InputEncoding { .. } => false,
        }
    }
}

impl SlotOutcome {
    /// Classify an enqueue result by Python's rules: a payload mismatch
    /// counts as fired; a retryable broker error is transient; every other
    /// error is permanent.
    fn from_result(result: Result<Uuid, ScheduleFireError>) -> Self {
        match result {
            Ok(task_id) => Self::Enqueued(task_id),
            Err(ScheduleFireError::Broker {
                source: crate::broker::BrokerError::PayloadMismatch { task_id },
                ..
            }) => Self::HeldByOtherCommand(task_id),
            Err(error) if error.is_retryable() => Self::Transient(error),
            Err(error) => Self::Permanent(error),
        }
    }
}

/// Checks that run before the scheduler writes anything: the schedule
/// section is valid and every enabled pattern has a slot after `now`.
fn check_schedules_at_start(
    schedule_config: &ScheduleConfig,
    now: DateTime<Utc>,
) -> Result<(), SchedulerStartError> {
    schedule_config
        .validate()
        .map_err(SchedulerStartError::InvalidScheduleConfig)?;
    for schedule in schedule_config.schedules.iter().filter(|s| s.enabled) {
        if next_run_at(&schedule.pattern, now, &schedule.timezone).is_none() {
            return Err(SchedulerStartError::NoNextSlot {
                schedule: schedule.name.clone(),
                after: now,
            });
        }
    }
    Ok(())
}

/// Spawn the scheduler service loop.
///
/// The scheduler:
/// 1. Refuses to start, writing nothing, when the schedule section is invalid
///    or an enabled pattern has no next slot. The join handle then yields the
///    [`SchedulerStartError`]; the host exits non-zero.
/// 2. Initializes schedule state for all configured schedules; one schedule's
///    failure is logged and the others go on (the existence heal retries it).
/// 3. Runs one tick at once, then one per `check_interval_seconds`: each tick
///    enqueues the due slots of each due schedule.
///
/// Multiple scheduler instances can coexist — each due schedule is guarded by
/// a per-schedule transaction-scoped advisory try-lock (matching Python's
/// `pg_advisory_xact_lock` key derivation), and enqueues are idempotent via
/// slot-based task ids.
///
/// The `app_config` is used to resolve queue priorities when enqueueing
/// scheduled tasks, matching Python's `effective_priority()` behaviour.
pub fn spawn_scheduler(
    broker: Arc<PostgresBroker>,
    schedule_config: ScheduleConfig,
    app_config: AppConfig,
    cancel: CancellationToken,
) -> JoinHandle<Result<(), SchedulerStartError>> {
    tokio::spawn(run_scheduler(broker, schedule_config, app_config, cancel))
}

/// The body of [`spawn_scheduler`]: start checks, state initialization, then
/// the tick loop until `cancel` fires.
async fn run_scheduler(
    broker: Arc<PostgresBroker>,
    schedule_config: ScheduleConfig,
    app_config: AppConfig,
    cancel: CancellationToken,
) -> Result<(), SchedulerStartError> {
    if !schedule_config.enabled || schedule_config.schedules.is_empty() {
        tracing::info!("scheduler disabled or no schedules configured");
        return Ok(());
    }

    check_schedules_at_start(&schedule_config, Utc::now())?;
    initialize_schedules(broker.pool(), &schedule_config.schedules, Utc::now()).await;

    let lock_pool = scheduler_lock_pool(broker.pool());
    tracing::info!("scheduler started");
    let check_interval = Duration::from_secs(u64::from(schedule_config.check_interval_seconds));
    let mut existence_cadence = ExistenceCheckCadence::new(schedule_config.check_interval_seconds);

    // Main loop: the first tick runs at once (Python `run_forever`).
    while !cancel.is_cancelled() {
        if let Err(e) = check_and_enqueue(
            &lock_pool,
            &broker,
            &schedule_config.schedules,
            schedule_config.check_interval_seconds,
            &app_config,
            &mut existence_cadence,
        )
        .await
        {
            tracing::error!(error = %e, "scheduler check failed");
        }

        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(check_interval) => {}
        }
    }

    lock_pool.close().await;
    tracing::info!("scheduler stopped");
    Ok(())
}

fn scheduler_lock_pool(runtime_pool: &sqlx::PgPool) -> sqlx::PgPool {
    runtime_pool
        .options()
        .clone()
        .min_connections(0)
        .max_connections(1)
        .connect_lazy_with((*runtime_pool.connect_options()).clone())
}

/// Initialize schedule states in the database.
///
/// For each enabled schedule, creates a state row if one doesn't exist, or
/// moves `next_run_at` when the pattern or timezone changed (the run record is
/// kept). Per-schedule isolation (Python `_initialize_schedules`): one
/// schedule's failure is logged and the rest proceed; a schedule left without
/// a row is recreated by the tick's existence heal.
async fn initialize_schedules(pool: &sqlx::PgPool, schedules: &[TaskSchedule], now: DateTime<Utc>) {
    // Schedule-state rows whose name is absent from this process's config are
    // left intact, not deleted: another scheduler (rolling deploy, shared DB)
    // may own them, and deleting would silently stop its schedule. Such rows are
    // inert here — get_due_schedules only considers configured, enabled names.
    // Warn for visibility. Parity with horsies PR #101 e26a0f55.
    let configured_names: std::collections::HashSet<&str> =
        schedules.iter().map(|s| s.name.as_str()).collect();

    match state::get_all_states(pool).await {
        Ok(all_states) => {
            for row in all_states {
                if !configured_names.contains(row.schedule_name.as_str()) {
                    tracing::warn!(
                        schedule = %row.schedule_name,
                        "schedule state present in DB but not in this config; \
                         leaving intact (may belong to another scheduler)",
                    );
                }
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch schedule states for inspection");
        }
    }

    let mut failed: Vec<&str> = Vec::new();
    for schedule in schedules.iter().filter(|s| s.enabled) {
        if let Err(e) = initialize_schedule(pool, schedule, now).await {
            tracing::error!(
                schedule = %schedule.name,
                error = %e,
                "failed to initialize schedule state",
            );
            failed.push(schedule.name.as_str());
        }
    }
    if !failed.is_empty() {
        tracing::error!(
            failed = ?failed,
            "{} schedule(s) failed to initialize; remaining schedules will proceed",
            failed.len(),
        );
    }
}

/// Why one schedule's state row was not initialized.
#[derive(Debug, thiserror::Error)]
enum ScheduleInitError {
    #[error("state read or write failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("the pattern has no run after {after}")]
    NoNextSlot { after: DateTime<Utc> },
}

/// Create the state row of one schedule, or store its changed pattern or
/// timezone. An unchanged schedule is not written.
async fn initialize_schedule(
    pool: &sqlx::PgPool,
    schedule: &TaskSchedule,
    now: DateTime<Utc>,
) -> Result<(), ScheduleInitError> {
    let config_hash = compute_config_hash(schedule);
    let existing = state::get_state(pool, &schedule.name).await?;
    if existing
        .as_ref()
        .is_some_and(|row| row.config_hash.as_slice() == config_hash.as_slice())
    {
        tracing::debug!(schedule = %schedule.name, "schedule state unchanged");
        return Ok(());
    }

    let next = next_run_at(&schedule.pattern, now, &schedule.timezone)
        .ok_or(ScheduleInitError::NoNextSlot { after: now })?;
    match existing {
        None => {
            let _inserted =
                state::insert_state_if_absent(pool, &schedule.name, next, &config_hash).await?;
            tracing::info!(schedule = %schedule.name, next_run_at = %next, "schedule initialized");
        }
        Some(_) => {
            let _written =
                state::update_schedule_config(pool, &schedule.name, next, &config_hash).await?;
            tracing::warn!(
                schedule = %schedule.name,
                next_run_at = %next,
                "schedule pattern or timezone changed; next_run_at recalculated, run record kept",
            );
        }
    }
    Ok(())
}

/// Recreate state rows for enabled schedules that have none.
///
/// Diffs enabled schedule names against existing state-row names (one PK-column
/// SELECT, no locks) and inserts the missing ones with `next_run_at` computed
/// from `now`. Per-schedule isolated (a failure logs and is retried on the next
/// check), idempotent, and race-safe via `insert_state_if_absent` (a concurrent
/// winner's row is preserved). Parity with horsies PR #123.
///
/// Runs on a cadence, not every tick (parity with horsies PR #206): while
/// healthy the caller re-checks roughly every [`EXISTENCE_CHECK_INTERVAL_S`]
/// seconds; an unhealthy pass makes the caller re-check every tick until a
/// fully healthy one. A missing row healed without error in the same pass
/// counts as healthy.
///
/// Returns `true` when every enabled schedule had a state row or was
/// re-initialized without error in this pass.
async fn ensure_states_exist(
    pool: &sqlx::PgPool,
    schedules: &[TaskSchedule],
    now: DateTime<Utc>,
) -> bool {
    let enabled: Vec<&TaskSchedule> = schedules.iter().filter(|s| s.enabled).collect();
    if enabled.is_empty() {
        return true;
    }

    let existing: std::collections::HashSet<String> = match state::get_existing_names(pool).await {
        Ok(names) => names.into_iter().collect(),
        Err(e) => {
            // A failed existence read this tick is not a regression: the due
            // query would fail the same way. Skip the heal; the unhealthy
            // verdict makes the next tick re-check.
            tracing::warn!(error = %e, "schedule self-heal: existence read failed, skipping tick");
            return false;
        }
    };

    let mut healthy = true;
    for schedule in enabled {
        if existing.contains(&schedule.name) {
            continue;
        }

        let config_hash = compute_config_hash(schedule);
        let Some(next) = next_run_at(&schedule.pattern, now, &schedule.timezone) else {
            healthy = false;
            tracing::error!(
                schedule = %schedule.name,
                after = %now,
                "schedule self-heal: the pattern has no next run; row not created",
            );
            continue;
        };

        match state::insert_state_if_absent(pool, &schedule.name, next, &config_hash).await {
            Ok(true) => tracing::info!(
                schedule = %schedule.name,
                next_run_at = %next,
                "self-healed missing schedule state",
            ),
            Ok(false) => {} // lost the check-then-insert race; winner's row stands
            Err(e) => {
                healthy = false;
                tracing::error!(
                    schedule = %schedule.name,
                    error = %e,
                    "schedule self-heal insert failed, will retry next tick",
                );
            }
        }
    }
    healthy
}

/// Check for due schedules and enqueue their tasks.
///
/// No tick-level lock (parity with Python's `_check_and_run_schedules`):
/// concurrent schedulers coordinate per schedule via
/// `try_acquire_schedule_lock`, and `process_schedule` re-reads state under
/// that lock before enqueueing. Returns the error of the due read; each due
/// schedule's own failure is logged and the other schedules go on.
async fn check_and_enqueue(
    lock_pool: &sqlx::PgPool,
    broker: &Arc<PostgresBroker>,
    schedules: &[TaskSchedule],
    check_interval_seconds: u32,
    app_config: &AppConfig,
    existence_cadence: &mut ExistenceCheckCadence,
) -> Result<(), sqlx::Error> {
    let now = Utc::now();

    // Only query for enabled schedules — filter at the DB level.
    let enabled_names: Vec<String> = schedules
        .iter()
        .filter(|s| s.enabled)
        .map(|s| s.name.clone())
        .collect();

    if enabled_names.is_empty() {
        return Ok(());
    }

    // Self-heal: recreate state rows for enabled schedules missing one (init
    // failure or external delete) so they are not silently dormant until the
    // next restart. Runs before the due query — recreated rows have a strictly
    // future next_run_at and so are not due this tick. Gated to a ~60s cadence
    // while healthy (parity with horsies PR #206); the due read below still
    // runs every tick.
    if existence_cadence.should_check() {
        let healthy = ensure_states_exist(broker.pool(), schedules, now).await;
        existence_cadence.record(healthy);
    }
    existence_cadence.tick();

    let due = state::get_due_schedules_filtered(broker.pool(), &enabled_names, now).await?;

    for row in due {
        // Defensive: skip any row that doesn't match a configured schedule.
        let Some(schedule) = schedules.iter().find(|s| s.name == row.schedule_name) else {
            tracing::warn!(schedule = %row.schedule_name, "due schedule not in config");
            continue;
        };

        let lock_tx = match state::try_acquire_schedule_lock(lock_pool, &schedule.name).await {
            Ok(Some(tx)) => tx,
            Ok(None) => {
                tracing::debug!(schedule = %schedule.name, "schedule lock busy, skipping");
                continue;
            }
            Err(e) => {
                tracing::error!(schedule = %schedule.name, error = %e, "schedule lock failed");
                continue;
            }
        };

        let schedule_result =
            process_schedule(broker, schedule, now, check_interval_seconds, app_config).await;

        if let Err(e) = state::release_schedule_lock(lock_tx).await {
            tracing::error!(schedule = %schedule.name, error = %e, "failed to release schedule lock");
        }

        if let Err(e) = schedule_result {
            tracing::error!(schedule = %schedule.name, error = %e, "schedule processing failed");
        }
    }

    Ok(())
}

/// Fire the due slots of one schedule and move its state row.
///
/// Returns the error of a state read or write. Enqueue failures do not
/// return: a transient one leaves the slot due, a permanent one moves past
/// the slot with no run recorded, and a payload mismatch counts as fired.
async fn process_schedule(
    broker: &Arc<PostgresBroker>,
    schedule: &TaskSchedule,
    now: chrono::DateTime<Utc>,
    _check_interval_seconds: u32,
    app_config: &AppConfig,
) -> Result<(), sqlx::Error> {
    let Some(state_row) = state::get_state(broker.pool(), &schedule.name).await? else {
        tracing::warn!(
            schedule = %schedule.name,
            "schedule state missing; reinitializing",
        );
        let config_hash = compute_config_hash(schedule);
        match next_run_at(&schedule.pattern, now, &schedule.timezone) {
            Some(next) => {
                let _inserted = state::insert_state_if_absent(
                    broker.pool(),
                    &schedule.name,
                    next,
                    &config_hash,
                )
                .await?;
            }
            None => tracing::error!(
                schedule = %schedule.name,
                after = %now,
                "the pattern has no next run; state row not created",
            ),
        }
        return Ok(());
    };

    if !should_run_now(Some(state_row.next_run_at), now) {
        return Ok(());
    }

    // With catch_up_missed on, the due slots fire in order; with it off, or
    // when no slot is due under the cap, only the latest due slot fires
    // (Python `_check_schedule`).
    let due_runs = match schedule.catch_up_missed {
        true => calculate_missed_runs(
            schedule,
            state_row.next_run_at,
            now,
            schedule.max_catch_up_runs,
        ),
        false => Vec::new(),
    };
    match due_runs.is_empty() {
        false => fire_catch_up(broker, schedule, &due_runs, now, app_config).await,
        true => fire_latest_slot(broker, schedule, &state_row, now, app_config).await,
    }
}

/// `catch_up_missed = true`: fire the due slots in order, at most
/// `max_catch_up_runs` per tick. A transient failure stops the tick and
/// keeps the progress of the slots before it; a permanent failure also moves
/// past its slot (Python `_check_schedule`, catch-up branch).
async fn fire_catch_up(
    broker: &Arc<PostgresBroker>,
    schedule: &TaskSchedule,
    due_runs: &[DateTime<Utc>],
    now: DateTime<Utc>,
    app_config: &AppConfig,
) -> Result<(), sqlx::Error> {
    if due_runs.len() > 1 {
        tracing::warn!(
            schedule = %schedule.name,
            due = due_runs.len(),
            max_catch_up = schedule.max_catch_up_runs,
            "catching up missed schedule runs",
        );
    }

    let mut last_recorded: Option<Uuid> = None;
    let mut progress_slot: Option<DateTime<Utc>> = None;
    for (index, slot) in due_runs.iter().enumerate() {
        let result = enqueue_scheduled_task(broker, schedule, app_config, *slot).await;
        let outcome = SlotOutcome::from_result(result);
        log_recorded_slot(schedule, *slot, &outcome);
        match outcome {
            SlotOutcome::Enqueued(task_id) | SlotOutcome::HeldByOtherCommand(task_id) => {
                last_recorded = Some(task_id);
                progress_slot = Some(*slot);
            }
            SlotOutcome::Transient(error) => {
                tracing::error!(
                    schedule = %schedule.name,
                    run = index + 1,
                    total = due_runs.len(),
                    error = %error,
                    "catch-up enqueue failed (transient); the slot stays due",
                );
                break;
            }
            SlotOutcome::Permanent(error) => {
                tracing::error!(
                    schedule = %schedule.name,
                    run = index + 1,
                    total = due_runs.len(),
                    slot = %slot,
                    error = %error,
                    "catch-up enqueue failed (permanent); skipping the slot",
                );
                progress_slot = Some(*slot);
                break;
            }
        }
    }

    // Nothing fired and nothing skipped: every slot stays due for the next tick.
    let Some(progress_slot) = progress_slot else {
        return Ok(());
    };
    let Some(next) = next_run_at(&schedule.pattern, progress_slot, &schedule.timezone) else {
        log_no_next_slot(schedule, progress_slot);
        return Ok(());
    };
    match last_recorded {
        Some(task_id) => state::record_run(broker.pool(), &schedule.name, now, next, task_id).await,
        None => state::advance_next_run(broker.pool(), &schedule.name, next).await,
    }
}

/// `catch_up_missed = false`: fire only the latest due slot. Older missed
/// slots (scheduler downtime) are dropped and the schedule resumes strictly
/// in the future (C5). `slot_time` stays slot-aligned so the slot-derived
/// task id stays deterministic (parity with horsies PR #46).
async fn fire_latest_slot(
    broker: &Arc<PostgresBroker>,
    schedule: &TaskSchedule,
    state_row: &state::ScheduleStateRow,
    now: DateTime<Utc>,
    app_config: &AppConfig,
) -> Result<(), sqlx::Error> {
    let (slot_time, next, skipped) =
        advance_to_latest_due_slot(schedule, state_row.next_run_at, now);
    if skipped > 0 {
        tracing::warn!(
            schedule = %schedule.name,
            skipped,
            slot = %slot_time,
            "missed run(s); skipping to latest due slot (catch_up_missed=false)",
        );
    }

    // next_run_at is NOT NULL: a slot with no successor is not fired, and the
    // row stays due (Python raises from calculate_next_run before the fire).
    let Some(next) = next else {
        log_no_next_slot(schedule, slot_time);
        return Ok(());
    };

    // Scan cap reached on a very deep backlog: persist progress without
    // firing a stale slot; the next tick continues advancing from here.
    if next <= now {
        return state::advance_next_run(broker.pool(), &schedule.name, next).await;
    }

    let result = enqueue_scheduled_task(broker, schedule, app_config, slot_time).await;
    let outcome = SlotOutcome::from_result(result);
    log_recorded_slot(schedule, slot_time, &outcome);
    match outcome {
        SlotOutcome::Enqueued(task_id) | SlotOutcome::HeldByOtherCommand(task_id) => {
            state::record_run(broker.pool(), &schedule.name, now, next, task_id).await
        }
        SlotOutcome::Transient(error) => {
            tracing::warn!(
                schedule = %schedule.name,
                error = %error,
                "schedule enqueue failed (transient); the slot stays due",
            );
            Ok(())
        }
        SlotOutcome::Permanent(error) => {
            // A refusal that repeats on every try (horsies-rust #77): move
            // past the slot so the schedule does not retry it every tick.
            tracing::error!(
                schedule = %schedule.name,
                slot = %slot_time,
                error = %error,
                "schedule enqueue failed (permanent); skipping the slot",
            );
            state::advance_next_run(broker.pool(), &schedule.name, next).await
        }
    }
}

/// Log a slot that counts as fired. Failed slots are logged by the caller,
/// which knows whether the slot stays due or is skipped.
fn log_recorded_slot(schedule: &TaskSchedule, slot: DateTime<Utc>, outcome: &SlotOutcome) {
    match outcome {
        SlotOutcome::Enqueued(task_id) => tracing::info!(
            schedule = %schedule.name,
            task_name = %schedule.task_name,
            task_id = %task_id,
            slot = %slot,
            "scheduled task enqueued",
        ),
        SlotOutcome::HeldByOtherCommand(task_id) => tracing::warn!(
            schedule = %schedule.name,
            task_id = %task_id,
            slot = %slot,
            "the slot's task id already holds a task with a different command (an \
             earlier version, or a change to the schedule's input or the task's \
             options); the slot counts as fired and is not enqueued again",
        ),
        SlotOutcome::Transient(_) | SlotOutcome::Permanent(_) => {}
    }
}

fn log_no_next_slot(schedule: &TaskSchedule, slot: DateTime<Utc>) {
    tracing::error!(
        schedule = %schedule.name,
        slot = %slot,
        "the pattern has no run after this slot; the state row is not moved",
    );
}

/// Bound the per-tick slot scan so a pathological backlog (tiny period, very
/// long downtime) cannot stall a tick; progress is persisted and the next tick
/// continues from where this one stopped. Parity with Python
/// `_MAX_SKIP_SCAN_PER_TICK`.
const MAX_SKIP_SCAN_PER_TICK: u32 = 100_000;

/// Advance through due slots to the latest one (catch_up_missed=false).
///
/// Returns `(latest_due_slot, next_run_after_slot, skipped)`. `next_run_after_slot`
/// is `None` when the pattern is unsatisfiable from the latest slot, and may still
/// be `Some(t)` with `t <= now` when the scan cap was reached — callers must not
/// fire a slot in that case. Mirrors Python `_advance_to_latest_due_slot`.
fn advance_to_latest_due_slot(
    schedule: &TaskSchedule,
    first_due: DateTime<Utc>,
    now: DateTime<Utc>,
) -> (DateTime<Utc>, Option<DateTime<Utc>>, u32) {
    let mut slot = first_due;
    let mut skipped: u32 = 0;
    let mut next_run = next_run_at(&schedule.pattern, slot, &schedule.timezone);
    while let Some(candidate) = next_run {
        if candidate > now || skipped >= MAX_SKIP_SCAN_PER_TICK {
            break;
        }
        // Non-monotonic guard: a non-advancing calculator would loop forever.
        if candidate <= slot {
            tracing::error!(
                schedule = %schedule.name,
                current = %slot,
                next = %candidate,
                "non-monotonic next_run_at — stopping skip-to-latest",
            );
            break;
        }
        slot = candidate;
        skipped += 1;
        next_run = next_run_at(&schedule.pattern, slot, &schedule.timezone);
    }
    (slot, next_run, skipped)
}

fn calculate_missed_runs(
    schedule: &TaskSchedule,
    first_due_run: DateTime<Utc>,
    now: DateTime<Utc>,
    max_runs: u32,
) -> Vec<DateTime<Utc>> {
    if first_due_run > now {
        return Vec::new();
    }

    let mut due_runs = Vec::new();
    let mut cursor = first_due_run;

    while cursor <= now && due_runs.len() < max_runs as usize {
        due_runs.push(cursor);

        let Some(next) = next_run_at(&schedule.pattern, cursor, &schedule.timezone) else {
            break;
        };

        // Non-monotonic guard: if the calculator returns a non-advancing time,
        // break to prevent an infinite loop.
        if next <= cursor {
            tracing::error!(
                schedule = %schedule.name,
                current = %cursor,
                next = %next,
                "non-monotonic next_run_at — stopping catch-up",
            );
            break;
        }

        cursor = next;
    }

    if cursor <= now && due_runs.len() >= max_runs as usize {
        tracing::warn!(
            schedule = %schedule.name,
            cap = max_runs,
            "catch-up cap reached, backlog remains",
        );
    }

    due_runs
}

/// Canonical UTC datetime string for fingerprint hashing.
/// Matches Python's `_canon_dt()`.
fn canon_dt(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string()
}

/// Deterministic task_id for a schedule name + slot time.
/// Same schedule + same slot -> same UUID5 -> idempotent on conflict.
fn schedule_slot_task_id(schedule_name: &str, slot_time: DateTime<Utc>) -> Uuid {
    Uuid::new_v5(
        &SCHEDULE_NAMESPACE,
        format!("{}:{}", schedule_name, canon_dt(slot_time)).as_bytes(),
    )
}

async fn enqueue_scheduled_task(
    broker: &Arc<PostgresBroker>,
    schedule: &TaskSchedule,
    app_config: &AppConfig,
    slot_time: DateTime<Utc>,
) -> Result<Uuid, ScheduleFireError> {
    // The schedule's input is one JSON value; absent is `null`.
    let input_json = serde_json::to_string(&schedule.input).map_err(|source| {
        ScheduleFireError::InputEncoding {
            schedule: schedule.name.clone(),
            source,
        }
    })?;

    // Schedule input is static config, so a violation repeats on every
    // fire — the warn rate-limit collapses that to one line, and the refusal
    // is permanent: the schedule moves past the slot. Parity with horsies
    // PR #208.
    let oversize = crate::core::config::payload::enforce_payload_policy(
        &app_config.payload,
        &schedule.task_name,
        crate::core::config::payload::PayloadKind::Kwargs,
        input_json.len(),
    );
    if let (Some(size), Some(limit)) = (oversize, app_config.payload.reject_bytes) {
        return Err(ScheduleFireError::PayloadTooLarge {
            schedule: schedule.name.clone(),
            size,
            limit,
        });
    }

    let queue = schedule.queue_name.as_deref().unwrap_or("default");
    let priority = resolve_queue_priority(app_config, queue);
    let retention_class_key = app_config.retention.resolve_queue_class(queue);

    // Deterministic task_id from schedule + slot (idempotent on retry).
    let task_id = schedule_slot_task_id(&schedule.name, slot_time);

    // sent_at is the logical slot time, not wall clock: a second fire of the
    // same slot carries the same command and sent_at, so the exact-id resend
    // check accepts it.
    broker
        .enqueue(
            &schedule.task_name,
            &input_json,
            queue,
            priority,
            Some(slot_time), // sent_at = slot time
            None,            // enqueued_at
            None,            // good_until
            None,            // task_options
            Some(task_id),   // predetermined deterministic task_id
            None,
            None,
            retention_class_key.as_deref(),
            None,
        )
        .await
        .map_err(|source| ScheduleFireError::Broker {
            schedule: schedule.name.clone(),
            source,
        })
}

/// The part of a schedule that decides its slots: pattern and timezone.
#[derive(serde::Serialize)]
struct ScheduleTiming<'a> {
    pattern: &'a crate::core::config::SchedulePattern,
    timezone: &'a str,
}

/// SHA-256 of the schedule's pattern and timezone, for change detection
/// (Python `_compute_config_hash`). A change of input, queue or catch-up
/// settings does not move `next_run_at`.
///
/// Stable across toolchain upgrades (C20): SHA-256 over serde_json text of a
/// struct with a fixed field order.
fn compute_config_hash(schedule: &TaskSchedule) -> [u8; 32] {
    use sha2::{Digest, Sha256};

    let timing = ScheduleTiming {
        pattern: &schedule.pattern,
        timezone: &schedule.timezone,
    };
    // The pattern types hold only strings, integers, enums and times: they
    // always serialize. A failure here is a defect, not an operational error.
    let text = serde_json::to_vec(&timing).expect("schedule pattern serializes to JSON");
    Sha256::digest(&text).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::{
        CustomQueueConfig, IntervalSchedule, PostgresConfig, QueueMode, RecoveryConfig,
        SchedulePattern, WorkerResilienceConfig,
    };
    use chrono::{TimeZone, Utc};
    use serial_test::serial;

    fn default_app_config() -> AppConfig {
        AppConfig {
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: PostgresConfig {
                database_url: "postgresql://localhost/test".to_owned(),
                session_database_url: None,
                pgbouncer_transaction_mode: false,
                pool_pre_ping: true,
                pool_size: 30,
                max_overflow: 30,
                retain_rerun_input_default: false,
                pool_timeout: 30,
                pool_recycle: 1800,
                echo: false,
            },
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            retention: crate::core::RetentionConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            schedule: None,
            resend_on_transient_err: false,
        }
    }

    fn custom_app_config() -> AppConfig {
        let mut config = default_app_config();
        config.queue_mode = QueueMode::Custom;
        config.custom_queues = Some(vec![
            CustomQueueConfig {
                name: "fast".to_owned(),
                priority: 1,
                max_concurrency: Some(10),
            },
            CustomQueueConfig {
                name: "slow".to_owned(),
                priority: 50,
                max_concurrency: Some(5),
            },
        ]);
        config
    }

    #[test]
    fn resolve_queue_priority_default_mode() {
        let config = default_app_config();
        // Default mode has no custom queues, should return 100.
        assert_eq!(resolve_queue_priority(&config, "default"), 100);
        assert_eq!(resolve_queue_priority(&config, "anything"), 100);
    }

    #[test]
    fn resolve_queue_priority_custom_mode_known_queue() {
        let config = custom_app_config();
        assert_eq!(resolve_queue_priority(&config, "fast"), 1);
        assert_eq!(resolve_queue_priority(&config, "slow"), 50);
    }

    #[test]
    fn resolve_queue_priority_custom_mode_unknown_queue() {
        let config = custom_app_config();
        // Unknown queue falls back to 100.
        assert_eq!(resolve_queue_priority(&config, "unknown"), 100);
    }

    #[test]
    fn compute_config_hash_deterministic() {
        let schedule = TaskSchedule {
            name: "test".to_owned(),
            task_name: "my_task".to_owned(),
            pattern: SchedulePattern::Interval(IntervalSchedule {
                seconds: Some(60),
                minutes: None,
                hours: None,
                days: None,
            }),
            input: serde_json::Value::Null,
            queue_name: None,
            enabled: true,
            timezone: "UTC".to_owned(),
            catch_up_missed: false,
            max_catch_up_runs: 100,
        };

        let hash1 = compute_config_hash(&schedule);
        let hash2 = compute_config_hash(&schedule);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn compute_config_hash_changes_on_different_config() {
        let schedule1 = TaskSchedule {
            name: "test".to_owned(),
            task_name: "my_task".to_owned(),
            pattern: SchedulePattern::Interval(IntervalSchedule {
                seconds: Some(60),
                minutes: None,
                hours: None,
                days: None,
            }),
            input: serde_json::Value::Null,
            queue_name: None,
            enabled: true,
            timezone: "UTC".to_owned(),
            catch_up_missed: false,
            max_catch_up_runs: 100,
        };

        let schedule2 = TaskSchedule {
            name: "test".to_owned(),
            task_name: "my_task".to_owned(),
            pattern: SchedulePattern::Interval(IntervalSchedule {
                seconds: Some(120), // different interval
                minutes: None,
                hours: None,
                days: None,
            }),
            input: serde_json::Value::Null,
            queue_name: None,
            enabled: true,
            timezone: "UTC".to_owned(),
            catch_up_missed: false,
            max_catch_up_runs: 100,
        };

        assert_ne!(
            compute_config_hash(&schedule1),
            compute_config_hash(&schedule2)
        );
    }

    #[test]
    fn compute_config_hash_is_stable_sha256() {
        // C20: the config hash must use a stable algorithm (SHA-256), not the
        // toolchain-dependent DefaultHasher. SHA-256 hex is 64 lowercase chars;
        // the old u64 DefaultHasher output was 16 — pinning the length and a
        // golden value guards against a regression to an unstable hasher.
        let schedule = TaskSchedule {
            name: "test".to_owned(),
            task_name: "my_task".to_owned(),
            pattern: SchedulePattern::Interval(IntervalSchedule {
                seconds: Some(60),
                minutes: None,
                hours: None,
                days: None,
            }),
            input: serde_json::Value::Null,
            queue_name: None,
            enabled: true,
            timezone: "UTC".to_owned(),
            catch_up_missed: false,
            max_catch_up_runs: 100,
        };
        let hash = compute_config_hash(&schedule);
        // Golden value pins the exact algorithm + serialization basis:
        // SHA-256 of {"pattern":{"type":"interval","seconds":60},"timezone":"UTC"}.
        let hex: String = hash.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(
            hex,
            "b896b67f1120471246f798d1f83ba3b4601931e97069ae773d2bf0c1880f705f",
        );
    }

    /// Python `_compute_config_hash` covers the pattern and the timezone
    /// only: a change of input, queue or catch-up settings does not move
    /// `next_run_at`.
    #[test]
    fn compute_config_hash_covers_pattern_and_timezone_only() {
        let base = interval_schedule_secs("hash_scope", 60);
        let hash = compute_config_hash(&base);

        let mut other_settings = base.clone();
        other_settings.task_name = "another_task".to_owned();
        other_settings.input = serde_json::json!({"changed": true});
        other_settings.queue_name = Some("bulk".to_owned());
        other_settings.catch_up_missed = true;
        other_settings.max_catch_up_runs = 7;
        assert_eq!(compute_config_hash(&other_settings), hash);

        let mut other_timezone = base.clone();
        other_timezone.timezone = "Europe/Istanbul".to_owned();
        assert_ne!(compute_config_hash(&other_timezone), hash);

        let other_pattern = interval_schedule_secs("hash_scope", 61);
        assert_ne!(compute_config_hash(&other_pattern), hash);
    }

    /// Python `_count_existing_slot_as_fired` and the retryable / permanent
    /// split of `_check_schedule`.
    #[test]
    fn slot_outcome_follows_the_fire_rules() {
        let task_id = Uuid::new_v4();
        let broker_error = |source: crate::broker::BrokerError| {
            Err(ScheduleFireError::Broker {
                schedule: "s".to_owned(),
                source,
            })
        };

        assert!(matches!(
            SlotOutcome::from_result(Ok(task_id)),
            SlotOutcome::Enqueued(id) if id == task_id
        ));
        assert!(matches!(
            SlotOutcome::from_result(broker_error(
                crate::broker::BrokerError::PayloadMismatch { task_id }
            )),
            SlotOutcome::HeldByOtherCommand(id) if id == task_id
        ));
        assert!(matches!(
            SlotOutcome::from_result(broker_error(crate::broker::BrokerError::ConnectionFailed(
                "reset".to_owned()
            ))),
            SlotOutcome::Transient(_)
        ));
        assert!(matches!(
            SlotOutcome::from_result(broker_error(
                crate::broker::BrokerError::EnqueueConflictUnverifiable {
                    task_id,
                    task_name: "t".to_owned(),
                }
            )),
            SlotOutcome::Permanent(_)
        ));
        assert!(matches!(
            SlotOutcome::from_result(Err(ScheduleFireError::PayloadTooLarge {
                schedule: "s".to_owned(),
                size: 10,
                limit: 5,
            })),
            SlotOutcome::Permanent(_)
        ));
    }

    /// The scheduler refuses to start, before any statement, when the
    /// schedule section is invalid (Python: start errors reach the CLI, exit 1).
    #[tokio::test]
    async fn start_refuses_an_invalid_schedule_section_before_any_statement() {
        // A pool that cannot connect: a statement would fail with a
        // connection error, not with the start refusal.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(100))
            .connect_lazy("postgresql://horsies@127.0.0.1:1/unreachable")
            .expect("lazy pool");
        let broker = Arc::new(PostgresBroker::from_pool(pool));
        let mut schedule = interval_schedule_secs("bad_tz", 60);
        schedule.timezone = "Not/A_Timezone".to_owned();

        let outcome = spawn_scheduler(
            broker,
            ScheduleConfig::new(vec![schedule]),
            default_app_config(),
            CancellationToken::new(),
        )
        .await
        .expect("scheduler task joins");
        assert!(matches!(
            outcome,
            Err(SchedulerStartError::InvalidScheduleConfig(ref message))
                if message.contains("Not/A_Timezone")
        ));
    }

    #[test]
    fn start_checks_accept_valid_schedules() {
        let config = ScheduleConfig::new(vec![interval_schedule_secs("fine", 60)]);
        assert!(check_schedules_at_start(&config, Utc::now()).is_ok());
    }

    /// Python `test_scheduler_service.py::test_normal_run_permanent_failure_skips_slot`:
    /// a refusal that repeats moves past the slot, records no run and writes
    /// no task (horsies-rust #77).
    #[tokio::test]
    #[serial]
    async fn normal_run_permanent_failure_skips_slot() {
        let pool = test_pool().await;
        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let name = format!("permanent_skip_{}", Uuid::new_v4().simple());
        let mut schedule = interval_schedule_secs(&name, 60);
        schedule.task_name = name.clone();
        schedule.input = serde_json::json!({"payload": "over the limit"});
        let mut app_config = default_app_config();
        app_config.payload.reject_bytes = Some(1);

        let slot = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
        state::seed_state(&pool, &name, slot).await.unwrap();
        let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 30).unwrap();
        process_schedule(&broker, &schedule, now, 1, &app_config)
            .await
            .unwrap();

        let row = state::get_state(&pool, &name).await.unwrap().unwrap();
        assert_eq!(
            row.next_run_at,
            Utc.with_ymd_and_hms(2025, 6, 1, 12, 1, 0).unwrap()
        );
        assert_eq!(row.run_count, 0);
        assert!(row.last_task_id.is_none());
        let tasks: i64 =
            sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE task_name = $1")
                .bind(&name)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(tasks, 0);
        state::delete_state(&pool, &name).await.unwrap();
    }

    /// Python `test_scheduler_service.py::test_catch_up_permanent_failure_skips_doomed_slot`:
    /// in catch-up mode a permanent refusal of the first due slot moves past
    /// that one slot only.
    #[tokio::test]
    #[serial]
    async fn catch_up_permanent_failure_skips_doomed_slot() {
        let pool = test_pool().await;
        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let name = format!("catch_up_skip_{}", Uuid::new_v4().simple());
        let mut schedule = interval_schedule_secs(&name, 60);
        schedule.task_name = name.clone();
        schedule.catch_up_missed = true;
        schedule.input = serde_json::json!({"payload": "over the limit"});
        let mut app_config = default_app_config();
        app_config.payload.reject_bytes = Some(1);

        let first_slot = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
        state::seed_state(&pool, &name, first_slot).await.unwrap();
        // Three slots are due: 12:00, 12:01, 12:02.
        let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 2, 30).unwrap();
        process_schedule(&broker, &schedule, now, 1, &app_config)
            .await
            .unwrap();

        let row = state::get_state(&pool, &name).await.unwrap().unwrap();
        assert_eq!(
            row.next_run_at,
            Utc.with_ymd_and_hms(2025, 6, 1, 12, 1, 0).unwrap()
        );
        assert_eq!(row.run_count, 0);
        state::delete_state(&pool, &name).await.unwrap();
    }

    #[test]
    fn schedule_slot_task_id_is_deterministic_for_the_same_slot() {
        let slot = Utc.with_ymd_and_hms(2025, 3, 29, 12, 0, 0).unwrap();

        let first = schedule_slot_task_id("nightly-reindex", slot);
        let second = schedule_slot_task_id("nightly-reindex", slot);

        assert_eq!(first, second);
    }

    #[test]
    fn schedule_slot_task_id_changes_when_slot_changes() {
        let first_slot = Utc.with_ymd_and_hms(2025, 3, 29, 12, 0, 0).unwrap();
        let second_slot = Utc.with_ymd_and_hms(2025, 3, 29, 12, 1, 0).unwrap();

        let first = schedule_slot_task_id("nightly-reindex", first_slot);
        let second = schedule_slot_task_id("nightly-reindex", second_slot);

        assert_ne!(first, second);
    }

    #[test]
    fn calculate_missed_runs_returns_consecutive_slots_up_to_cap() {
        let schedule = TaskSchedule {
            name: "nightly-reindex".to_owned(),
            task_name: "reindex".to_owned(),
            pattern: SchedulePattern::Interval(IntervalSchedule {
                seconds: Some(60),
                minutes: None,
                hours: None,
                days: None,
            }),
            input: serde_json::Value::Null,
            queue_name: None,
            enabled: true,
            timezone: "UTC".to_owned(),
            catch_up_missed: true,
            max_catch_up_runs: 3,
        };
        let first_due = Utc.with_ymd_and_hms(2025, 3, 29, 12, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2025, 3, 29, 12, 4, 30).unwrap();

        let missed = calculate_missed_runs(&schedule, first_due, now, 3);

        assert_eq!(
            missed,
            vec![
                Utc.with_ymd_and_hms(2025, 3, 29, 12, 0, 0).unwrap(),
                Utc.with_ymd_and_hms(2025, 3, 29, 12, 1, 0).unwrap(),
                Utc.with_ymd_and_hms(2025, 3, 29, 12, 2, 0).unwrap(),
            ]
        );
    }

    #[test]
    fn calculate_missed_runs_returns_empty_when_first_due_is_in_the_future() {
        let schedule = TaskSchedule {
            name: "nightly-reindex".to_owned(),
            task_name: "reindex".to_owned(),
            pattern: SchedulePattern::Interval(IntervalSchedule {
                seconds: Some(60),
                minutes: None,
                hours: None,
                days: None,
            }),
            input: serde_json::Value::Null,
            queue_name: None,
            enabled: true,
            timezone: "UTC".to_owned(),
            catch_up_missed: true,
            max_catch_up_runs: 3,
        };
        let first_due = Utc.with_ymd_and_hms(2025, 3, 29, 12, 5, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2025, 3, 29, 12, 4, 59).unwrap();

        let missed = calculate_missed_runs(&schedule, first_due, now, 3);

        assert!(missed.is_empty());
    }

    #[tokio::test]
    #[serial]
    async fn scheduled_enqueue_persists_mapped_class_and_shared_facts() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let task_name = "p6_scheduled_facts";
        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = $1")
            .bind(task_name)
            .execute(&pool)
            .await
            .unwrap();

        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let mut schedule = interval_schedule_secs("p6-scheduled-facts", 60);
        schedule.task_name = task_name.to_owned();
        schedule.queue_name = Some("bulk".to_owned());
        schedule.input = serde_json::json!({"value": 1, "named": true});
        let mut app_config = default_app_config();
        app_config
            .retention
            .queue_retention
            .insert("bulk".to_owned(), Some(chrono::Duration::days(2)));
        let slot = Utc::now() - chrono::Duration::seconds(1);

        let task_id = enqueue_scheduled_task(&broker, &schedule, &app_config, slot)
            .await
            .expect("enqueue scheduled task");
        let row: (String, String, i32, bool) = sqlx::query_as(
            "SELECT id::text, retention_class_key,
                    octet_length(command_fingerprint), retain_rerun_input
             FROM horsies_tasks WHERE id = $1::uuid",
        )
        .bind(&task_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.0, task_id.to_string());
        assert_eq!(row.1, "standard_2d");
        assert_eq!(row.2, 32);
        assert!(!row.3);
        assert_eq!(task_id.get_version_num(), 5);
    }

    // ---- DB-backed: slot-anchored advancement (parity with horsies PR #46) ----

    async fn test_pool() -> sqlx::PgPool {
        crate::broker::terminalization_matrix::migrated_pool().await
    }

    fn interval_schedule_secs(name: &str, seconds: u32) -> TaskSchedule {
        TaskSchedule {
            name: name.to_owned(),
            task_name: "my_task".to_owned(),
            pattern: SchedulePattern::Interval(IntervalSchedule {
                seconds: Some(seconds),
                minutes: None,
                hours: None,
                days: None,
            }),
            input: serde_json::Value::Null,
            queue_name: None,
            enabled: true,
            timezone: "UTC".to_owned(),
            catch_up_missed: false,
            max_catch_up_runs: 100,
        }
    }

    #[tokio::test]
    #[serial]
    async fn process_schedule_single_late_tick_anchors_to_slot() {
        // A late tick still within one period must fire the stored slot and
        // advance one period from it (slot-anchored, not wall-clock; horsies
        // PR #46). No skip occurs because the following slot is future.
        let pool = test_pool().await;
        let name = "pr46_slot_anchor";
        let task_name = "pr46_anchor_task";
        sqlx::query("DELETE FROM horsies_schedule_state WHERE schedule_name = $1")
            .bind(name)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = $1")
            .bind(task_name)
            .execute(&pool)
            .await
            .unwrap();

        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let mut schedule = interval_schedule_secs(name, 5);
        schedule.task_name = task_name.to_owned();
        let app_config = default_app_config();

        // Seed a due slot at 12:00:00; the tick arrives late at 12:00:03 —
        // within one 5s period, so the following slot (12:00:05) is future.
        let due_slot = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
        state::seed_state(&pool, name, due_slot).await.unwrap();

        let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 3).unwrap();
        process_schedule(&broker, &schedule, now, 1, &app_config)
            .await
            .unwrap();

        // next_run advances from the slot (12:00:05), not wall-clock (12:00:08).
        let state_row = state::get_state(&pool, name).await.unwrap().unwrap();
        assert_eq!(
            state_row.next_run_at,
            Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 5).unwrap(),
            "next_run must anchor to the due slot, not wall-clock now",
        );

        // The enqueued task's sent_at is the logical slot, not wall-clock now.
        let sent_at: chrono::DateTime<Utc> =
            sqlx::query_scalar("SELECT sent_at FROM horsies_tasks WHERE task_name = $1")
                .bind(task_name)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(sent_at, due_slot, "enqueue slot must be the due slot");

        // Cleanup.
        sqlx::query("DELETE FROM horsies_schedule_state WHERE schedule_name = $1")
            .bind(name)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = $1")
            .bind(task_name)
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn process_schedule_skips_to_latest_due_slot_when_catch_up_disabled() {
        // C5: catch_up_missed=false with a backlog more than one period deep must
        // fire ONLY the latest due slot and persist a strictly-future next_run,
        // not replay every missed slot one tick at a time. 5s interval, stored
        // slot 12:00:00, tick at 12:00:17 → missed 12:00:05/10/15 → fire 12:00:15,
        // next 12:00:20. Before the fix it fired 12:00:00 with next 12:00:05.
        let pool = test_pool().await;
        let name = "c5_skip_latest";
        let task_name = "c5_skip_task";
        sqlx::query("DELETE FROM horsies_schedule_state WHERE schedule_name = $1")
            .bind(name)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = $1")
            .bind(task_name)
            .execute(&pool)
            .await
            .unwrap();

        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let mut schedule = interval_schedule_secs(name, 5);
        schedule.task_name = task_name.to_owned();
        let app_config = default_app_config();

        let due_slot = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
        state::seed_state(&pool, name, due_slot).await.unwrap();

        let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 17).unwrap();
        process_schedule(&broker, &schedule, now, 1, &app_config)
            .await
            .unwrap();

        // next_run is strictly future (12:00:20), skipping the 05/10/15 backlog.
        let state_row = state::get_state(&pool, name).await.unwrap().unwrap();
        assert_eq!(
            state_row.next_run_at,
            Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 20).unwrap(),
            "next_run must skip past now to the latest slot's successor",
        );
        assert!(
            state_row.next_run_at > now,
            "persisted next_run must be strictly in the future",
        );

        // Exactly one task enqueued, at the latest due slot (12:00:15).
        let slots: Vec<chrono::DateTime<Utc>> = sqlx::query_scalar(
            "SELECT sent_at FROM horsies_tasks WHERE task_name = $1 ORDER BY sent_at",
        )
        .bind(task_name)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(slots.len(), 1, "only the latest due slot must fire");
        assert_eq!(
            slots[0],
            Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 15).unwrap(),
            "the fired slot must be the latest due slot",
        );

        // Cleanup.
        sqlx::query("DELETE FROM horsies_schedule_state WHERE schedule_name = $1")
            .bind(name)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = $1")
            .bind(task_name)
            .execute(&pool)
            .await
            .unwrap();
    }

    #[test]
    fn advance_to_latest_due_slot_skips_backlog() {
        // Pure-unit: 5s interval, stored slot 12:00:00, now 12:00:17 → latest
        // due slot 12:00:15, next 12:00:20, skipped 3.
        let schedule = interval_schedule_secs("unit_skip", 5);
        let first_due = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 17).unwrap();
        let (slot, next, skipped) = advance_to_latest_due_slot(&schedule, first_due, now);
        assert_eq!(slot, Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 15).unwrap());
        assert_eq!(
            next,
            Some(Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 20).unwrap())
        );
        assert_eq!(skipped, 3);
    }

    #[test]
    fn advance_to_latest_due_slot_no_skip_within_one_period() {
        // The following slot is future → no advance, skipped 0.
        let schedule = interval_schedule_secs("unit_noskip", 5);
        let first_due = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 3).unwrap();
        let (slot, next, skipped) = advance_to_latest_due_slot(&schedule, first_due, now);
        assert_eq!(slot, first_due);
        assert_eq!(
            next,
            Some(Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 5).unwrap())
        );
        assert_eq!(skipped, 0);
    }

    /// The self-heal creates a state row for an enabled schedule that has none
    /// (with a strictly-future next_run), leaves an existing row's next_run
    /// untouched, and skips disabled schedules. Parity with horsies PR #123.
    #[tokio::test]
    #[serial]
    async fn ensure_states_exist_heals_missing_and_preserves_existing() {
        let pool = test_pool().await;
        let missing = format!("qw123_missing_{}", uuid::Uuid::new_v4());
        let disabled = format!("qw123_disabled_{}", uuid::Uuid::new_v4());

        let mut disabled_schedule = interval_schedule_secs(&disabled, 60);
        disabled_schedule.enabled = false;
        let schedules = vec![interval_schedule_secs(&missing, 60), disabled_schedule];

        let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
        let healthy = ensure_states_exist(&pool, &schedules, now).await;
        assert!(healthy, "a pass that heals its missing row counts healthy");

        // Missing enabled schedule was healed with a strictly-future next_run.
        let healed = state::get_state(&pool, &missing).await.unwrap().unwrap();
        assert!(
            healed.next_run_at > now,
            "healed next_run must be strictly future"
        );

        // Disabled schedule is not created.
        assert!(state::get_state(&pool, &disabled).await.unwrap().is_none());

        // A later tick leaves the existing row's next_run untouched (no overwrite).
        let later = now + chrono::Duration::seconds(30);
        let healthy = ensure_states_exist(&pool, &schedules, later).await;
        assert!(healthy, "all rows present is a healthy pass");
        let after = state::get_state(&pool, &missing).await.unwrap().unwrap();
        assert_eq!(
            after.next_run_at, healed.next_run_at,
            "existing next_run must be preserved across ticks",
        );

        // Cleanup.
        sqlx::query("DELETE FROM horsies_schedule_state WHERE schedule_name = ANY($1)")
            .bind(vec![missing, disabled])
            .execute(&pool)
            .await
            .unwrap();
    }

    // --- Existence-check cadence (parity with horsies PR #206) ---

    /// The tick count preserves the ~60s wall-clock cadence across tick
    /// configs; a tick longer than the target floors at 1.
    #[test]
    fn existence_cadence_ticks_derived_from_check_interval() {
        for (seconds, expected_ticks) in [(1, 60), (10, 6), (60, 1), (120, 1)] {
            let cadence = ExistenceCheckCadence::new(seconds);
            assert_eq!(
                cadence.interval_ticks, expected_ticks,
                "check_interval_seconds={seconds}",
            );
        }
    }

    /// While healthy, only the first tick checks; subsequent ticks inside the
    /// interval skip, and the check runs again once the interval elapses.
    #[test]
    fn existence_cadence_healthy_skips_until_interval_elapses() {
        let mut cadence = ExistenceCheckCadence::new(10); // 6 ticks
        let mut checks = 0;
        for _ in 0..7 {
            if cadence.should_check() {
                checks += 1;
                cadence.record(true);
            }
            cadence.tick();
        }
        assert_eq!(checks, 2, "tick 0 and tick 6 check; ticks 1-5 skip");
    }

    /// An unhealthy pass re-checks every tick until a healthy one, then the
    /// cadence resumes.
    #[test]
    fn existence_cadence_unhealthy_rechecks_every_tick_until_healthy() {
        let mut cadence = ExistenceCheckCadence::new(10); // 6 ticks
        let health = [false, false, true]; // two failed passes, then healthy
        let mut checks = 0;
        for tick in 0..6 {
            if cadence.should_check() {
                cadence.record(health[checks.min(2)]);
                checks += 1;
                assert!(tick <= 2, "checks must be consecutive while unhealthy");
            }
            cadence.tick();
        }
        assert_eq!(
            checks, 3,
            "ticks 0/1/2 check (unhealthy, unhealthy, healthy); 3-5 skip",
        );
    }

    #[tokio::test]
    #[serial]
    async fn private_scheduler_pool_preserves_replay_cancellation_and_connection_hooks() {
        let admin = test_pool().await;
        let runtime = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_millis(200))
            .after_connect(|connection, _| {
                Box::pin(async move {
                    sqlx::query("SET application_name='test_scheduler_private'")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect_with((*admin.connect_options()).clone())
            .await
            .unwrap();
        let broker = Arc::new(PostgresBroker::from_pool(runtime.clone()));
        let make_lock_pool = || scheduler_lock_pool(&runtime);
        let lock_pool = make_lock_pool();
        let application: String = sqlx::query_scalar("SHOW application_name")
            .fetch_one(&lock_pool)
            .await
            .unwrap();
        assert_eq!(application, "test_scheduler_private");
        let config = default_app_config();
        for scenario in [
            "state_failure",
            "cancel_insert",
            "saturation",
            "competition",
        ] {
            let name = format!("test_private_{scenario}");
            let mut schedule = interval_schedule_secs(&name, 60);
            schedule.task_name = name.clone();
            state::seed_state(&admin, &name, Utc::now() - chrono::Duration::seconds(1))
                .await
                .unwrap();
            let schedules = vec![schedule];
            match scenario {
                "state_failure" => {
                    sqlx::raw_sql("CREATE FUNCTION test_fail_schedule_state() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected state write failure'; END $$; CREATE TRIGGER test_fail_schedule_state BEFORE UPDATE ON horsies_schedule_state FOR EACH ROW EXECUTE FUNCTION test_fail_schedule_state();").execute(&admin).await.unwrap();
                    let mut cadence = ExistenceCheckCadence::new(1);
                    cadence.record(true);
                    check_and_enqueue(&lock_pool, &broker, &schedules, 1, &config, &mut cadence)
                        .await
                        .unwrap();
                    assert_eq!(
                        state::get_state(&admin, &name)
                            .await
                            .unwrap()
                            .unwrap()
                            .run_count,
                        0
                    );
                    let tasks: i64 =
                        sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE task_name=$1")
                            .bind(&name)
                            .fetch_one(&admin)
                            .await
                            .unwrap();
                    assert_eq!(tasks, 1);
                    sqlx::raw_sql("DROP TRIGGER test_fail_schedule_state ON horsies_schedule_state; DROP FUNCTION test_fail_schedule_state();").execute(&admin).await.unwrap();
                }
                "cancel_insert" => {
                    let mut blocker = admin.begin().await.unwrap();
                    sqlx::query("LOCK TABLE horsies_tasks IN ACCESS EXCLUSIVE MODE")
                        .execute(&mut *blocker)
                        .await
                        .unwrap();
                    let pool = lock_pool.clone();
                    let broker = broker.clone();
                    let schedules = schedules.clone();
                    let config = config.clone();
                    let task = tokio::spawn(async move {
                        let mut cadence = ExistenceCheckCadence::new(1);
                        cadence.record(true);
                        check_and_enqueue(&pool, &broker, &schedules, 1, &config, &mut cadence)
                            .await
                            .unwrap();
                    });
                    tokio::time::timeout(Duration::from_secs(5),async {
                        loop {
                            let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='test_scheduler_private' AND wait_event_type='Lock')").fetch_one(&admin).await.unwrap();
                            if waiting { break; }
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    }).await.unwrap();
                    task.abort();
                    assert!(task.await.unwrap_err().is_cancelled());
                    blocker.rollback().await.unwrap();
                    tokio::time::timeout(Duration::from_secs(5), async {
                        loop {
                            match state::try_acquire_schedule_lock(&admin, &name)
                                .await
                                .unwrap()
                            {
                                Some(lock) => {
                                    drop(lock);
                                    break;
                                }
                                None => tokio::time::sleep(Duration::from_millis(10)).await,
                            }
                        }
                    })
                    .await
                    .unwrap();
                }
                "saturation" => {
                    let held = runtime.acquire().await.unwrap();
                    let mut cadence = ExistenceCheckCadence::new(1);
                    cadence.record(true);
                    assert!(check_and_enqueue(
                        &lock_pool,
                        &broker,
                        &schedules,
                        1,
                        &config,
                        &mut cadence
                    )
                    .await
                    .is_err());
                    drop(held);
                }
                "competition" => {
                    let mut calls = Vec::new();
                    for _ in 0..4 {
                        let pool = make_lock_pool();
                        let broker = broker.clone();
                        let schedules = schedules.clone();
                        let config = config.clone();
                        calls.push(tokio::spawn(async move {
                            let mut cadence = ExistenceCheckCadence::new(1);
                            cadence.record(true);
                            check_and_enqueue(&pool, &broker, &schedules, 1, &config, &mut cadence)
                                .await
                                .unwrap();
                            pool.close().await;
                        }));
                    }
                    for call in calls {
                        call.await.unwrap();
                    }
                }
                _ => unreachable!(),
            }
            let mut cadence = ExistenceCheckCadence::new(1);
            cadence.record(true);
            check_and_enqueue(&lock_pool, &broker, &schedules, 1, &config, &mut cadence)
                .await
                .unwrap();
            check_and_enqueue(&lock_pool, &broker, &schedules, 1, &config, &mut cadence)
                .await
                .unwrap();
            assert_eq!(
                state::get_state(&admin, &name)
                    .await
                    .unwrap()
                    .unwrap()
                    .run_count,
                1
            );
            let tasks: i64 =
                sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE task_name=$1")
                    .bind(&name)
                    .fetch_one(&admin)
                    .await
                    .unwrap();
            assert_eq!(tasks, 1);
            sqlx::query("DELETE FROM horsies_tasks WHERE task_name=$1")
                .bind(&name)
                .execute(&admin)
                .await
                .unwrap();
            state::delete_state(&admin, &name).await.unwrap();
        }
        lock_pool.close().await;
        assert!(lock_pool.is_closed());
        runtime.close().await;
    }

    #[tokio::test]
    #[serial]
    async fn spawned_scheduler_uses_one_runtime_connection_and_closes_its_lock_pool() {
        let admin = test_pool().await;
        let application = format!("scheduler_lifecycle_{}", uuid::Uuid::new_v4().simple());
        let hook_application = application.clone();
        let runtime = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(1))
            .after_connect(move |connection, _| {
                let name = hook_application.clone();
                Box::pin(async move {
                    sqlx::query("SELECT set_config('application_name', $1, false)")
                        .bind(name)
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect_with((*admin.connect_options()).clone())
            .await
            .unwrap();
        let broker = Arc::new(PostgresBroker::from_pool(runtime.clone()));
        let name = format!("scheduler_lifecycle_{}", uuid::Uuid::new_v4().simple());
        let mut schedule = interval_schedule_secs(&name, 60);
        schedule.task_name = name.clone();
        let cancel = CancellationToken::new();
        let service = spawn_scheduler(
            broker,
            ScheduleConfig {
                enabled: true,
                schedules: vec![schedule],
                check_interval_seconds: 1,
            },
            default_app_config(),
            cancel.clone(),
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if state::get_state(&admin, &name).await.unwrap().is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        sqlx::query("UPDATE horsies_schedule_state SET next_run_at=NOW()-interval '1 second' WHERE schedule_name=$1")
            .bind(&name).execute(&admin).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if state::get_state(&admin, &name)
                    .await
                    .unwrap()
                    .unwrap()
                    .run_count
                    == 1
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let tasks: i64 =
            sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE task_name=$1")
                .bind(&name)
                .fetch_one(&admin)
                .await
                .unwrap();
        assert_eq!(tasks, 1);
        let sessions: i64 =
            sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name=$1")
                .bind(&application)
                .fetch_one(&admin)
                .await
                .unwrap();
        assert_eq!(sessions, 2);
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), service)
            .await
            .unwrap()
            .unwrap()
            .expect("the scheduler started and stopped without a start error");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let sessions: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name=$1",
                )
                .bind(&application)
                .fetch_one(&admin)
                .await
                .unwrap();
                if sessions == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!runtime.is_closed());
        sqlx::query("DELETE FROM horsies_tasks WHERE task_name=$1")
            .bind(&name)
            .execute(&admin)
            .await
            .unwrap();
        state::delete_state(&admin, &name).await.unwrap();
        runtime.close().await;
    }
}

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

/// Spawn the scheduler service loop.
///
/// The scheduler:
/// 1. Initializes schedule state for all configured schedules
/// 2. On each tick, checks for due schedules and enqueues their tasks.
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
) -> JoinHandle<()> {
    tokio::spawn(async move {
        if !schedule_config.enabled || schedule_config.schedules.is_empty() {
            tracing::info!("scheduler disabled or no schedules configured");
            return;
        }

        // Initialize schedule states (one-time, outside the per-tick lock).
        if let Err(e) = initialize_schedules(broker.pool(), &schedule_config.schedules).await {
            tracing::error!(error = %e, "failed to initialize schedule states");
            return;
        }

        tracing::info!("scheduler started");
        let check_interval = Duration::from_secs(schedule_config.check_interval_seconds as u64);
        let mut existence_cadence =
            ExistenceCheckCadence::new(schedule_config.check_interval_seconds);

        // Main loop.
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(check_interval) => {}
            }

            if cancel.is_cancelled() {
                break;
            }

            if let Err(e) = check_and_enqueue(
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
        }

        tracing::info!("scheduler stopped");
    })
}

/// Initialize schedule states in the database.
///
/// For each configured schedule, creates a state row if one doesn't exist,
/// or updates the next_run_at if the config changed.
async fn initialize_schedules(
    pool: &sqlx::PgPool,
    schedules: &[TaskSchedule],
) -> Result<(), sqlx::Error> {
    let now = Utc::now();

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

    for schedule in schedules {
        if !schedule.enabled {
            continue;
        }

        let existing = state::get_state(pool, &schedule.name).await?;
        let config_hash = compute_config_hash(schedule);

        match existing {
            Some(row) if row.config_hash.as_deref() == Some(&config_hash) => {
                // Config unchanged, keep existing state.
                tracing::debug!(schedule = %schedule.name, "schedule state unchanged");
            }
            _ => {
                // New or changed schedule — compute next_run_at.
                let next = next_run_at(&schedule.pattern, now, &schedule.timezone);
                state::upsert_state(
                    pool,
                    &schedule.name,
                    None, // last_run_at
                    next,
                    None, // last_task_id
                    0,
                    Some(&config_hash),
                )
                .await?;
                tracing::info!(
                    schedule = %schedule.name,
                    next_run_at = ?next,
                    "schedule initialized",
                );
            }
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
        let next = next_run_at(&schedule.pattern, now, &schedule.timezone);

        match state::insert_state_if_absent(
            pool,
            &schedule.name,
            None,
            next,
            None,
            0,
            Some(&config_hash),
        )
        .await
        {
            Ok(true) => tracing::info!(
                schedule = %schedule.name,
                next_run_at = ?next,
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
/// that lock before enqueueing.
async fn check_and_enqueue(
    broker: &Arc<PostgresBroker>,
    schedules: &[TaskSchedule],
    check_interval_seconds: u32,
    app_config: &AppConfig,
    existence_cadence: &mut ExistenceCheckCadence,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

        let lock_tx = match state::try_acquire_schedule_lock(broker.pool(), &schedule.name).await {
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

async fn process_schedule(
    broker: &Arc<PostgresBroker>,
    schedule: &TaskSchedule,
    now: chrono::DateTime<Utc>,
    _check_interval_seconds: u32,
    app_config: &AppConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(state_row) = state::get_state(broker.pool(), &schedule.name).await? else {
        tracing::warn!(
            schedule = %schedule.name,
            "schedule state missing; reinitializing",
        );
        let config_hash = compute_config_hash(schedule);
        let next = next_run_at(&schedule.pattern, now, &schedule.timezone);
        state::upsert_state(
            broker.pool(),
            &schedule.name,
            None,
            next,
            None,
            0,
            Some(&config_hash),
        )
        .await?;
        return Ok(());
    };

    if !should_run_now(state_row.next_run_at, now) {
        return Ok(());
    }

    let mut missed_runs = Vec::new();
    if schedule.catch_up_missed {
        if let Some(last_scheduled_run) = state_row.next_run_at {
            missed_runs = calculate_missed_runs(
                schedule,
                last_scheduled_run,
                now,
                schedule.max_catch_up_runs,
            );
        }
    }

    if !missed_runs.is_empty() {
        tracing::warn!(
            schedule = %schedule.name,
            missed = missed_runs.len(),
            max_catch_up = schedule.max_catch_up_runs,
            "catching up missed schedule runs",
        );

        // Partial-progress: track successful enqueues. If one fails, persist
        // progress for the successful ones so they aren't retried next tick.
        let mut caught_up: Vec<chrono::DateTime<Utc>> = Vec::new();
        let mut last_task_id = None;
        for missed_time in &missed_runs {
            match enqueue_scheduled_task(broker, schedule, app_config, *missed_time).await {
                Ok(task_id) => {
                    tracing::info!(
                        schedule = %schedule.name,
                        task_id = %task_id,
                        missed_run_at = ?missed_time,
                        "catch-up run enqueued",
                    );
                    last_task_id = Some(task_id);
                    caught_up.push(*missed_time);
                }
                Err(e) => {
                    tracing::error!(
                        schedule = %schedule.name,
                        error = %e,
                        caught_up = caught_up.len(),
                        total = missed_runs.len(),
                        "catch-up enqueue failed, persisting partial progress",
                    );
                    break;
                }
            }
        }

        if !caught_up.is_empty() {
            let last_slot = *caught_up.last().expect("caught_up not empty");
            let next = next_run_at(&schedule.pattern, last_slot, &schedule.timezone);
            if next.is_none() {
                tracing::error!(
                    schedule = %schedule.name,
                    last_slot = %last_slot,
                    "next_run_at returned None; persisting NULL will stop this schedule",
                );
            }
            state::upsert_state(
                broker.pool(),
                &schedule.name,
                Some(now),
                next,
                last_task_id,
                state_row.run_count + 1,
                state_row.config_hash.as_deref(),
            )
            .await?;

            if caught_up.len() < missed_runs.len() {
                tracing::warn!(
                    schedule = %schedule.name,
                    caught_up = caught_up.len(),
                    total = missed_runs.len(),
                    "partially caught up — remainder will retry next tick",
                );
            }
        }
        // If nothing caught up, don't advance state — retry everything next tick.
    } else {
        // catch_up_missed=false: fire only the latest due slot. Older missed
        // slots (scheduler downtime) are dropped and the schedule resumes
        // strictly in the future, instead of replaying the whole backlog one
        // tick at a time (C5). `slot_time` stays slot-aligned so the enqueue
        // slot and slot-derived task_id remain deterministic (parity with
        // horsies PR #46). Falls back to `now` for the first run, where there
        // is no prior slot.
        let first_due = state_row.next_run_at.unwrap_or(now);
        let (slot_time, next, skipped) = advance_to_latest_due_slot(schedule, first_due, now);

        if skipped > 0 {
            tracing::warn!(
                schedule = %schedule.name,
                skipped,
                slot = %slot_time,
                "missed run(s); skipping to latest due slot (catch_up_missed=false)",
            );
        }

        // Scan cap reached on a very deep backlog: persist progress without
        // firing a stale slot; the next tick continues advancing from here.
        if let Some(next) = next {
            if next <= now {
                state::upsert_state(
                    broker.pool(),
                    &schedule.name,
                    state_row.last_run_at,
                    Some(next),
                    state_row.last_task_id,
                    state_row.run_count,
                    state_row.config_hash.as_deref(),
                )
                .await?;
                return Ok(());
            }
        }

        let task_id = enqueue_scheduled_task(broker, schedule, app_config, slot_time).await?;
        tracing::info!(
            schedule = %schedule.name,
            task_name = %schedule.task_name,
            task_id = %task_id,
            "scheduled task enqueued",
        );

        if next.is_none() {
            tracing::error!(
                schedule = %schedule.name,
                slot = %slot_time,
                "next_run_at returned None; persisting NULL will stop this schedule",
            );
        }
        state::upsert_state(
            broker.pool(),
            &schedule.name,
            Some(now),
            next,
            Some(task_id),
            state_row.run_count + 1,
            state_row.config_hash.as_deref(),
        )
        .await?;
    }

    Ok(())
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
) -> Result<Uuid, Box<dyn std::error::Error + Send + Sync>> {
    let args_json = if schedule.args == serde_json::Value::Null {
        None
    } else {
        Some(serde_json::to_string(&schedule.args)?)
    };

    let kwargs_json = if schedule.kwargs == serde_json::Value::Null {
        None
    } else {
        Some(serde_json::to_string(&schedule.kwargs)?)
    };

    // Schedule args/kwargs are static config, so a violation repeats on every
    // fire — the warn rate-limit collapses that to one line and the reject
    // surfaces per-slot through the schedule's normal enqueue-failure
    // logging. Parity with horsies PR #208.
    let encoded_len =
        args_json.as_deref().map_or(0, str::len) + kwargs_json.as_deref().map_or(0, str::len);
    if let Some(oversize) = crate::core::config::payload::enforce_payload_policy(
        &app_config.payload,
        &schedule.task_name,
        crate::core::config::payload::PayloadKind::Kwargs,
        encoded_len,
    ) {
        return Err(format!(
            "payload for schedule '{}' is {} bytes, exceeding payload.reject_bytes={:?}; \
             slot not enqueued",
            schedule.name, oversize, app_config.payload.reject_bytes,
        )
        .into());
    }

    let queue = schedule.queue_name.as_deref().unwrap_or("default");
    let priority = resolve_queue_priority(app_config, queue);
    let retention_class_key = app_config.retention.resolve_queue_class(queue);

    // Deterministic task_id from schedule + slot (idempotent on retry).
    let task_id = schedule_slot_task_id(&schedule.name, slot_time);

    // Deterministic fingerprint using slot_time as sent_at.
    let enqueue_sha = crate::broker::compute_enqueue_sha(
        &schedule.task_name,
        queue,
        priority,
        args_json.as_deref(),
        kwargs_json.as_deref(),
        slot_time, // sent_at = logical slot time, not wall clock
        None,      // good_until
        None,      // enqueue_delay_seconds
        None,      // task_options
    );

    let result_id = broker
        .enqueue(
            &schedule.task_name,
            args_json.as_deref(),
            kwargs_json.as_deref(),
            queue,
            priority,
            Some(slot_time), // sent_at = slot time
            None,            // enqueued_at
            None,            // good_until
            None,            // task_options
            &enqueue_sha,
            Some(task_id), // predetermined deterministic task_id
            None,
            None,
            retention_class_key.as_deref(),
            None,
        )
        .await?;

    Ok(result_id)
}

/// Compute a simple hash of the schedule config for change detection.
fn compute_config_hash(schedule: &TaskSchedule) -> String {
    use sha2::{Digest, Sha256};

    // Stable across toolchain upgrades. `DefaultHasher`'s algorithm is
    // explicitly unspecified between Rust releases: a shifted hash would make
    // `initialize_schedules` treat every schedule as changed and reset
    // `last_run_at`/`run_count`/`next_run_at`, dropping any pending catch-up
    // backlog and run history (C20). Upgrade note: deployments holding the old
    // 16-char DefaultHasher value in `config_hash` see every schedule counted as
    // changed on the first run after this change, incurring that reset exactly
    // once — the same symptom, paid one time to move onto the stable hash.
    let json = serde_json::to_string(schedule).unwrap_or_default();
    let digest = Sha256::digest(json.as_bytes());
    format!("{:x}", digest)
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
            args: serde_json::Value::Null,
            kwargs: serde_json::Value::Null,
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
            args: serde_json::Value::Null,
            kwargs: serde_json::Value::Null,
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
            args: serde_json::Value::Null,
            kwargs: serde_json::Value::Null,
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
            args: serde_json::Value::Null,
            kwargs: serde_json::Value::Null,
            queue_name: None,
            enabled: true,
            timezone: "UTC".to_owned(),
            catch_up_missed: false,
            max_catch_up_runs: 100,
        };
        let hash = compute_config_hash(&schedule);
        assert_eq!(hash.len(), 64, "expected 64-char SHA-256 hex");
        assert!(
            hash.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "hash must be lowercase hex: {hash}",
        );
        // Golden value pins the exact algorithm + serialization basis.
        assert_eq!(
            hash,
            "f017c54428c546a2d8ce73c45d1eafcf207d910b03ff1b73d29a4bd9407c1374",
        );
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
            args: serde_json::Value::Null,
            kwargs: serde_json::Value::Null,
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
            args: serde_json::Value::Null,
            kwargs: serde_json::Value::Null,
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
        schedule.args = serde_json::json!([1]);
        schedule.kwargs = serde_json::json!({"named": true});
        let mut app_config = default_app_config();
        app_config
            .retention
            .queue_retention
            .insert("bulk".to_owned(), Some(chrono::Duration::hours(36)));
        let slot = Utc::now() - chrono::Duration::seconds(1);

        let task_id = enqueue_scheduled_task(&broker, &schedule, &app_config, slot)
            .await
            .expect("enqueue scheduled task");
        let row: (String, String, String, i32, bool) = sqlx::query_as(
            "SELECT id::text, retention_class_key,
                    prepared_rerun_input_disposition,
                    octet_length(command_fingerprint), retain_rerun_input
             FROM horsies_tasks WHERE id = $1::uuid",
        )
        .bind(&task_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.0, task_id.to_string());
        assert_eq!(row.1, "q_bulk_36h");
        assert_eq!(row.2, "DECLINED_BY_POLICY");
        assert_eq!(row.3, 32);
        assert!(!row.4);
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
            args: serde_json::Value::Null,
            kwargs: serde_json::Value::Null,
            queue_name: None,
            enabled: true,
            timezone: "UTC".to_owned(),
            catch_up_missed: false,
            max_catch_up_runs: 100,
        }
    }

    #[tokio::test]
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
        state::upsert_state(&pool, name, None, Some(due_slot), None, 0, None)
            .await
            .unwrap();

        let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 3).unwrap();
        process_schedule(&broker, &schedule, now, 1, &app_config)
            .await
            .unwrap();

        // next_run advances from the slot (12:00:05), not wall-clock (12:00:08).
        let state_row = state::get_state(&pool, name).await.unwrap().unwrap();
        assert_eq!(
            state_row.next_run_at,
            Some(Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 5).unwrap()),
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
        state::upsert_state(&pool, name, None, Some(due_slot), None, 0, None)
            .await
            .unwrap();

        let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 17).unwrap();
        process_schedule(&broker, &schedule, now, 1, &app_config)
            .await
            .unwrap();

        // next_run is strictly future (12:00:20), skipping the 05/10/15 backlog.
        let state_row = state::get_state(&pool, name).await.unwrap().unwrap();
        assert_eq!(
            state_row.next_run_at,
            Some(Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 20).unwrap()),
            "next_run must skip past now to the latest slot's successor",
        );
        assert!(
            state_row.next_run_at.unwrap() > now,
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
        let next = healed.next_run_at.expect("next_run set");
        assert!(next > now, "healed next_run must be strictly future");

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
}

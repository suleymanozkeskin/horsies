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
/// 2. On each tick, acquires a transaction-scoped advisory lock (blocking),
///    checks for due schedules, enqueues tasks, and commits (releasing the lock).
///
/// Multiple scheduler instances can coexist — they block on the advisory lock
/// and take turns, matching Python's `pg_advisory_xact_lock` pattern.
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

    // Prune stale schedule states (schedules removed from config).
    // Errors are swallowed so one bad row doesn't block initialization.
    let configured_names: std::collections::HashSet<&str> =
        schedules.iter().map(|s| s.name.as_str()).collect();

    match state::get_all_states(pool).await {
        Ok(all_states) => {
            for row in all_states {
                if !configured_names.contains(row.schedule_name.as_str()) {
                    match state::delete_state(pool, &row.schedule_name).await {
                        Ok(_) => {
                            tracing::info!(
                                schedule = %row.schedule_name,
                                "pruned stale schedule state",
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                schedule = %row.schedule_name,
                                error = %e,
                                "failed to prune stale schedule state",
                            );
                        }
                    }
                }
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch schedule states for pruning");
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

/// Check for due schedules and enqueue their tasks.
///
/// Acquires a transaction-scoped advisory lock at the start of each tick
/// so concurrent schedulers serialize instead of conflicting.
async fn check_and_enqueue(
    broker: &Arc<PostgresBroker>,
    schedules: &[TaskSchedule],
    check_interval_seconds: u32,
    app_config: &AppConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Acquire transaction-scoped advisory lock (blocking).
    // Released automatically when the transaction commits at the end of this tick.
    let mut tx = broker.pool().begin().await?;
    state::acquire_scheduler_xact_lock(&mut tx).await?;
    tx.commit().await?;

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

    let due = state::get_due_schedules_filtered(broker.pool(), &enabled_names, now).await?;

    for row in due {
        // Defensive: skip any row that doesn't match a configured schedule.
        let Some(schedule) = schedules.iter().find(|s| s.name == row.schedule_name) else {
            tracing::warn!(schedule = %row.schedule_name, "due schedule not in config");
            continue;
        };

        let lock_conn = match state::try_acquire_schedule_lock(broker.pool(), &schedule.name).await
        {
            Ok(Some(conn)) => conn,
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

        if let Err(e) = state::release_schedule_lock(lock_conn, &schedule.name).await {
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
        let mut last_task_id = String::new();
        for missed_time in &missed_runs {
            match enqueue_scheduled_task(broker, schedule, app_config, *missed_time).await {
                Ok(task_id) => {
                    tracing::info!(
                        schedule = %schedule.name,
                        task_id = %task_id,
                        missed_run_at = ?missed_time,
                        "catch-up run enqueued",
                    );
                    last_task_id = task_id;
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
            state::upsert_state(
                broker.pool(),
                &schedule.name,
                Some(now),
                next,
                Some(&last_task_id),
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
        // Anchor to the logical due slot, not wall-clock `now`. A late tick must
        // keep the enqueue slot and next-run aligned to the schedule (no drift)
        // and the slot-based task_id idempotent. Falls back to `now` for the
        // first run, where there is no prior slot (parity with horsies PR #46).
        let slot_time = state_row.next_run_at.unwrap_or(now);

        let task_id = enqueue_scheduled_task(broker, schedule, app_config, slot_time).await?;
        tracing::info!(
            schedule = %schedule.name,
            task_name = %schedule.task_name,
            task_id = %task_id,
            "scheduled task enqueued",
        );

        let next = next_run_at(&schedule.pattern, slot_time, &schedule.timezone);
        state::upsert_state(
            broker.pool(),
            &schedule.name,
            Some(now),
            next,
            Some(&task_id),
            state_row.run_count + 1,
            state_row.config_hash.as_deref(),
        )
        .await?;
    }

    Ok(())
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
fn schedule_slot_task_id(schedule_name: &str, slot_time: DateTime<Utc>) -> String {
    Uuid::new_v5(
        &SCHEDULE_NAMESPACE,
        format!("{}:{}", schedule_name, canon_dt(slot_time)).as_bytes(),
    )
    .to_string()
}

async fn enqueue_scheduled_task(
    broker: &Arc<PostgresBroker>,
    schedule: &TaskSchedule,
    app_config: &AppConfig,
    slot_time: DateTime<Utc>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
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

    let queue = schedule.queue_name.as_deref().unwrap_or("default");
    let priority = resolve_queue_priority(app_config, queue);

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
            Some(&task_id), // predetermined deterministic task_id
        )
        .await?;

    Ok(result_id)
}

/// Compute a simple hash of the schedule config for change detection.
fn compute_config_hash(schedule: &TaskSchedule) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    // Hash the serialized form of relevant fields.
    let json = serde_json::to_string(schedule).unwrap_or_default();
    json.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::{
        CustomQueueConfig, IntervalSchedule, PostgresConfig, QueueMode, RecoveryConfig,
        SchedulePattern, WorkerResilienceConfig,
    };
    use chrono::{TimeZone, Utc};

    fn default_app_config() -> AppConfig {
        AppConfig {
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: PostgresConfig {
                database_url: "postgresql://localhost/test".to_owned(),
                session_database_url: None,
                pgbouncer_transaction_mode: false,
                pool_pre_ping: true,
                pool_size: 30,
                max_overflow: 30,
                pool_timeout: 30,
                pool_recycle: 1800,
                echo: false,
            },
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
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
                max_concurrency: 10,
            },
            CustomQueueConfig {
                name: "slow".to_owned(),
                priority: 50,
                max_concurrency: 5,
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

    // ---- DB-backed: slot-anchored advancement (parity with horsies PR #46) ----

    fn test_db_url() -> String {
        if let Ok(url) = std::env::var("DATABASE_URL") {
            return url;
        }
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let root = std::path::Path::new(manifest_dir)
            .ancestors()
            .find(|p| p.join(".env").exists());
        if let Some(root) = root {
            if let Ok(contents) = std::fs::read_to_string(root.join(".env")) {
                for line in contents.lines() {
                    let line = line.trim();
                    if line.is_empty() || line.starts_with('#') {
                        continue;
                    }
                    if let Some((key, value)) = line.split_once('=') {
                        if key.trim() == "DB_PASSWORD" {
                            return format!(
                                "postgresql://postgres:{}@localhost:5432/horsies-rust-port",
                                value.trim(),
                            );
                        }
                    }
                }
            }
        }
        panic!("database URL not found: set DATABASE_URL or add DB_PASSWORD to .env");
    }

    async fn test_pool() -> sqlx::PgPool {
        let pool = sqlx::PgPool::connect(&test_db_url())
            .await
            .expect("failed to connect");
        crate::broker::migrations::run_horsies_migrations(&pool)
            .await
            .expect("migrations failed");
        pool
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
    async fn process_schedule_advances_next_run_from_slot_not_now() {
        let pool = test_pool().await;
        let name = "pr46_slot_anchor";
        sqlx::query("DELETE FROM horsies_schedule_state WHERE schedule_name = $1")
            .bind(name)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = 'my_task'")
            .execute(&pool)
            .await
            .unwrap();

        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let schedule = interval_schedule_secs(name, 5);
        let app_config = default_app_config();

        // Seed a due slot at 12:00:00; the tick arrives late at 12:00:17.
        let due_slot = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
        state::upsert_state(&pool, name, None, Some(due_slot), None, 0, None)
            .await
            .unwrap();

        let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 17).unwrap();
        process_schedule(&broker, &schedule, now, 1, &app_config)
            .await
            .unwrap();

        // next_run advances from the slot (12:00:05), not the wall clock (12:00:22).
        let state_row = state::get_state(&pool, name).await.unwrap().unwrap();
        assert_eq!(
            state_row.next_run_at,
            Some(Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 5).unwrap()),
            "next_run must anchor to the due slot, not wall-clock now",
        );

        // The enqueued task's sent_at is the logical slot, not wall-clock now.
        let sent_at: chrono::DateTime<Utc> =
            sqlx::query_scalar("SELECT sent_at FROM horsies_tasks WHERE task_name = 'my_task'")
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
        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = 'my_task'")
            .execute(&pool)
            .await
            .unwrap();
    }
}

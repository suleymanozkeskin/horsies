use super::*;
use crate::core::app::ScheduleSendFacts;
use crate::core::config::{
    CustomQueueConfig, IntervalSchedule, PostgresConfig, QueueMode, RecoveryConfig,
    SchedulePattern, WorkerResilienceConfig,
};
use chrono::{TimeZone, Utc};
use serial_test::serial;
use uuid::Uuid;

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
            worker_pool_pre_ping: false,
            pool_size: 30,
            max_overflow: 30,
            retain_rerun_input_default: false,
            pool_timeout: 30,
            pool_recycle: crate::manifest::PoolRecycle::after_seconds(1800),
            echo: false,
            tcp_keepalives: Default::default(),
            tcp_user_timeout: Default::default(),
        },
        cluster_wide_cap: None,
        prefetch_buffer: 0,
        claim_lease_ms: None,
        max_claim_renew_age_ms: 180_000,
        recovery: RecoveryConfig::default(),
        retention: crate::core::RetentionConfig::default(),
        resilience: WorkerResilienceConfig::default(),
        worker_profiles: std::collections::BTreeMap::new(),
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

    let hash1 = compute_config_hash(&enabled(&schedule));
    let hash2 = compute_config_hash(&enabled(&schedule));
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
        compute_config_hash(&enabled(&schedule1)),
        compute_config_hash(&enabled(&schedule2))
    );
}

#[test]
fn compute_config_hash_is_stable_sha256() {
    // The config hash is SHA-256 of Python's digest text, not a
    // toolchain-dependent hasher; the golden value is the digest that
    // Python's `_compute_config_hash` stores for the same schedule.
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
    let hash = compute_config_hash(&enabled(&schedule));
    // SHA-256 of
    // {"pattern":{"type":"interval","seconds":60,"minutes":null,"hours":null,"days":null},"timezone":"UTC"}.
    let hex: String = hash.iter().map(|byte| format!("{byte:02x}")).collect();
    assert_eq!(
        hex,
        "a7bba31d521252fd1fb6e910224d2c9a13daa9d45b4c3a1c4569f7b76a2e92e1",
    );
}

/// Python `_compute_config_hash` covers the pattern and the timezone
/// only: a change of input, queue or catch-up settings does not move
/// `next_run_at`.
#[test]
fn compute_config_hash_covers_pattern_and_timezone_only() {
    let base = interval_schedule_secs("hash_scope", 60);
    let hash = compute_config_hash(&enabled(&base));

    let mut other_settings = base.clone();
    other_settings.task_name = "another_task".to_owned();
    other_settings.input = serde_json::json!({"changed": true});
    other_settings.queue_name = Some("bulk".to_owned());
    other_settings.catch_up_missed = true;
    other_settings.max_catch_up_runs = 7;
    assert_eq!(compute_config_hash(&enabled(&other_settings)), hash);

    let mut other_timezone = base.clone();
    other_timezone.timezone = "Europe/Istanbul".to_owned();
    assert_ne!(compute_config_hash(&enabled(&other_timezone)), hash);

    let other_pattern = interval_schedule_secs("hash_scope", 61);
    assert_ne!(compute_config_hash(&enabled(&other_pattern)), hash);
}

/// Python `_count_existing_slot_as_fired` and the retryable / permanent
/// split of `_check_schedule`.
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
        PreparedSchedules::new(),
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

/// A broker handle of the other client family is refused before the
/// first schedule-state write: the join handle yields
/// `SchedulerStartError::ClientFamily` with the mismatch, not retryable,
/// and no state row is written.
#[tokio::test]
#[serial]
async fn a_handle_of_the_other_family_starts_no_scheduler_and_writes_no_state() {
    use crate::broker::client_family::{ClientFamily, RecordedFamily};
    use crate::broker::BrokerError;

    let pool = test_pool().await;
    let broker =
        Arc::new(PostgresBroker::from_pool(pool.clone()).with_client_family(ClientFamily::Python));
    let name = format!("scheduler_family_{}", uuid::Uuid::new_v4().simple());
    let schedule = interval_schedule_secs(&name, 60);
    let prepared = prepared_for(std::slice::from_ref(&schedule));

    let outcome = spawn_scheduler(
        broker,
        ScheduleConfig::new(vec![schedule]),
        default_app_config(),
        prepared,
        CancellationToken::new(),
    )
    .await
    .expect("scheduler task joins");

    let refusal = match outcome {
        Err(SchedulerStartError::ClientFamily(refusal)) => refusal,
        other => panic!("expected the client family refusal, got {other:?}"),
    };
    assert!(matches!(
        refusal,
        BrokerError::ClientFamilyMismatch {
            stored: RecordedFamily::Recorded(ClientFamily::Rust),
            own: ClientFamily::Python,
        }
    ));
    assert!(!refusal.is_retryable());
    assert!(state::test_rows::state_row(&pool, &name).await.is_none());
}

#[test]
fn start_checks_accept_valid_schedules() {
    let config = ScheduleConfig::new(vec![interval_schedule_secs("fine", 60)]);
    assert!(check_schedules_at_start(&config, &prepared_for(&config.schedules)).is_ok());
    assert!(matches!(
        check_schedules_at_start(&config, &PreparedSchedules::new()),
        Err(SchedulerStartError::NotPrepared { ref schedule }) if schedule == "fine"
    ));
}

/// Python `test_scheduler_service.py::test_normal_run_permanent_failure_skips_slot`:
/// a refusal that repeats moves past the slot, records no run and writes
/// no task (horsies-rust #77).
/// Python `test_scheduler_service.py::test_normal_run_permanent_failure_skips_slot`:
/// a fire that the database refuses on every try moves the row past the
/// slot with a second fire of no slots, records no run and writes no task
/// (horsies-rust #77).
#[tokio::test]
#[serial]
async fn normal_run_permanent_failure_skips_slot() {
    let pool = test_pool().await;
    let name = format!("permanent_skip_{}", Uuid::new_v4().simple());
    let mut schedule = interval_schedule_secs(&name, 60);
    schedule.task_name = name.clone();
    let slot = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    state::test_rows::seed_state(&pool, &name, slot).await;
    refuse_slot_rows(&pool, &name).await;

    let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 30).unwrap();
    let fired = fire_once(
        &pool,
        &schedule,
        facts_for(&schedule),
        &default_app_config(),
        slot,
        now,
    )
    .await;
    drop_slot_refusal(&pool).await;

    fired.expect("a refused fire is handled by the skip");
    let row = state::test_rows::state_row(&pool, &name).await.unwrap();
    assert_eq!(
        row.next_run_at,
        Utc.with_ymd_and_hms(2025, 6, 1, 12, 1, 0).unwrap()
    );
    assert_eq!(row.run_count, 0);
    assert!(row.last_task_id.is_none());
    assert_eq!(task_count(&pool, &name).await, 0);
    cleanup(&pool, &name, &name).await;
}

/// Python `test_scheduler_service.py::test_catch_up_permanent_failure_skips_doomed_slot`:
/// in catch-up mode a refused fire of three due slots moves past the first
/// slot only, records no run and writes no task.
#[tokio::test]
#[serial]
async fn catch_up_permanent_failure_skips_doomed_slot() {
    let pool = test_pool().await;
    let name = format!("catch_up_skip_{}", Uuid::new_v4().simple());
    let mut schedule = interval_schedule_secs(&name, 60);
    schedule.task_name = name.clone();
    schedule.catch_up_missed = true;
    let first_slot = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    state::test_rows::seed_state(&pool, &name, first_slot).await;
    refuse_slot_rows(&pool, &name).await;

    // Three slots are due: 12:00, 12:01, 12:02.
    let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 2, 30).unwrap();
    let fired = fire_once(
        &pool,
        &schedule,
        facts_for(&schedule),
        &default_app_config(),
        first_slot,
        now,
    )
    .await;
    drop_slot_refusal(&pool).await;

    fired.expect("a refused fire is handled by the skip");
    let row = state::test_rows::state_row(&pool, &name).await.unwrap();
    assert_eq!(
        row.next_run_at,
        Utc.with_ymd_and_hms(2025, 6, 1, 12, 1, 0).unwrap()
    );
    assert_eq!(row.run_count, 0);
    assert_eq!(task_count(&pool, &name).await, 0);
    cleanup(&pool, &name, &name).await;
}

/// A schedule whose input is over `payload.reject_bytes` has no send
/// command: the start refuses it before any statement (Python
/// `prepare_schedules`, `CONFIG_INVALID_SCHEDULE`). Before, the refusal
/// came at each fire and skipped the slot.
#[tokio::test]
async fn an_input_over_the_reject_bound_refuses_the_start() {
    let lazy = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgresql://horsies@127.0.0.1:1/unreachable")
        .expect("lazy pool");
    let broker = PostgresBroker::from_pool(lazy);
    let mut schedule = interval_schedule_secs("over_the_limit", 60);
    schedule.input = serde_json::json!({"payload": "over the limit"});
    let mut app_config = default_app_config();
    app_config.payload.reject_bytes = Some(1);
    let config = ScheduleConfig::new(vec![schedule.clone()]);

    let built = build_commands(
        &broker,
        &config,
        &app_config,
        &prepared_for(std::slice::from_ref(&schedule)),
    );

    assert!(matches!(
        built,
        Err(SchedulerStartError::PayloadTooLarge { ref schedule, limit: 1, .. })
            if schedule == "over_the_limit"
    ));
}

/// Options that the broker refuses give no send command: the start refuses
/// the schedule (Python `_build_commands`, `CONFIG_INVALID_SCHEDULE`).
#[tokio::test]
async fn options_that_the_broker_refuses_refuse_the_start() {
    let lazy = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgresql://horsies@127.0.0.1:1/unreachable")
        .expect("lazy pool");
    let broker = PostgresBroker::from_pool(lazy);
    let schedule = interval_schedule_secs("bad_options", 60);
    let config = ScheduleConfig::new(vec![schedule.clone()]);
    let prepared = PreparedSchedules::from([(
        schedule.name.clone(),
        ScheduleSendFacts {
            queue_name: "default".to_owned(),
            task_options: Some("not json".to_owned()),
        },
    )]);

    let built = build_commands(&broker, &config, &default_app_config(), &prepared);

    assert!(matches!(
        built,
        Err(SchedulerStartError::CommandRefused { ref schedule, .. }) if schedule == "bad_options"
    ));
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

    let missed = calculate_missed_runs(&enabled(&schedule), first_due, now, 3);

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

    let missed = calculate_missed_runs(&enabled(&schedule), first_due, now, 3);

    assert!(missed.is_empty());
}

/// The main core pool on the test database of `pool`.
fn core_pool_of(pool: &sqlx::PgPool) -> CorePool {
    crate::broker::pools::core_pool_for_test_pool(pool)
}

/// The database time, read in its own statement.
async fn database_time(pool: &sqlx::PgPool) -> DateTime<Utc> {
    sqlx::query_scalar("SELECT statement_timestamp()")
        .fetch_one(pool)
        .await
        .expect("read the database time")
}

/// The send command of `schedule` with `facts`, as the start builds it.
fn command_for(
    pool: &sqlx::PgPool,
    schedule: &TaskSchedule,
    facts: ScheduleSendFacts,
    app_config: &AppConfig,
) -> SendCommand {
    let broker = PostgresBroker::from_pool(pool.clone());
    let config = ScheduleConfig::new(vec![schedule.clone()]);
    let prepared = PreparedSchedules::from([(schedule.name.clone(), facts)]);
    let mut commands =
        build_commands(&broker, &config, app_config, &prepared).expect("the send command builds");
    commands
        .remove(&schedule.name)
        .expect("one command per enabled schedule")
}

/// Fire `schedule` once as a tick does: its row is due at `first_due`, and
/// the due read ran at `database_now`.
async fn fire_once(
    pool: &sqlx::PgPool,
    schedule: &TaskSchedule,
    facts: ScheduleSendFacts,
    app_config: &AppConfig,
    first_due: DateTime<Utc>,
    database_now: DateTime<Utc>,
) -> Result<(), ScheduleCheckError> {
    let core = core_pool_of(pool);
    let command = command_for(pool, schedule, facts, app_config);
    let due = DueSchedule {
        schedule_name: schedule.name.clone(),
        next_run_at: first_due,
    };
    let outcome = fire_due_schedule(
        &core,
        StatementLog::Off,
        &enabled(schedule),
        &command,
        &due,
        database_now,
    )
    .await;
    core.close();
    outcome
}

/// The task rows of `task_name`.
async fn task_count(pool: &sqlx::PgPool, task_name: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE task_name = $1")
        .bind(task_name)
        .fetch_one(pool)
        .await
        .expect("count the task rows")
}

/// Delete the task rows of `task_name` and the state row of `schedule_name`.
async fn cleanup(pool: &sqlx::PgPool, task_name: &str, schedule_name: &str) {
    let _deleted = sqlx::query("DELETE FROM horsies_tasks WHERE task_name = $1")
        .bind(task_name)
        .execute(pool)
        .await
        .expect("delete the task rows");
    state::test_rows::delete_state(pool, schedule_name).await;
}

/// A trigger that refuses every task row of `task_name` with SQLSTATE
/// P0001: a database refusal of the fire that repeats on every try (as a
/// CHECK violation of the slot rows). [`drop_slot_refusal`] removes it.
async fn refuse_slot_rows(pool: &sqlx::PgPool, task_name: &str) {
    let _created = sqlx::raw_sql(&format!(
        "CREATE FUNCTION test_refuse_slot_rows() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.task_name = '{task_name}' THEN
                 RAISE EXCEPTION 'injected slot refusal';
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER test_refuse_slot_rows BEFORE INSERT ON horsies_tasks
             FOR EACH ROW EXECUTE FUNCTION test_refuse_slot_rows();"
    ))
    .execute(pool)
    .await
    .expect("create the refusal trigger");
}

async fn drop_slot_refusal(pool: &sqlx::PgPool) {
    let _dropped = sqlx::raw_sql(
        "DROP TRIGGER test_refuse_slot_rows ON horsies_tasks;
         DROP FUNCTION test_refuse_slot_rows();",
    )
    .execute(pool)
    .await
    .expect("drop the refusal trigger");
}

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

/// The enabled schedule of `schedule`, as the start reads it.
fn enabled(schedule: &TaskSchedule) -> EnabledSchedule {
    EnabledSchedule::of(schedule).expect("the test schedule is enabled")
}

/// Send facts of a schedule whose task declares no queue and no options.
fn facts_for(schedule: &TaskSchedule) -> ScheduleSendFacts {
    ScheduleSendFacts {
        queue_name: schedule
            .queue_name
            .clone()
            .unwrap_or_else(|| "default".to_owned()),
        task_options: None,
    }
}

fn prepared_for(schedules: &[TaskSchedule]) -> PreparedSchedules {
    schedules
        .iter()
        .map(|schedule| (schedule.name.clone(), facts_for(schedule)))
        .collect()
}

/// Python `test_scheduler_task_options.py::test_a_scheduled_run_stores_the_timeout_and_retry_policy_of_its_task`:
/// a fired slot stores the task's options text and the retry limit it
/// sets, as a send of the task does (#480). The policy names
/// `TASK_EXCEPTION`, as the Python task does: a policy with no code does
/// not validate as Python `RetryPolicy`, and the send refuses it.
#[tokio::test]
#[serial]
async fn scheduled_enqueue_persists_mapped_class_and_shared_facts() {
    let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
    let task_name = "p6_scheduled_facts";
    let mut schedule = interval_schedule_secs("p6-scheduled-facts", 60);
    schedule.task_name = task_name.to_owned();
    schedule.queue_name = Some("bulk".to_owned());
    schedule.input = serde_json::json!({"value": 1, "named": true});
    cleanup(&pool, task_name, &schedule.name).await;
    let mut app_config = default_app_config();
    app_config
        .retention
        .queue_retention
        .insert("bulk".to_owned(), Some(chrono::Duration::days(2)));
    let database_now = database_time(&pool).await;
    let slot = database_now - chrono::Duration::seconds(1);
    state::test_rows::seed_state(&pool, &schedule.name, slot).await;

    fire_once(
        &pool,
        &schedule,
        facts_for(&schedule),
        &app_config,
        slot,
        database_now,
    )
    .await
    .expect("the fire");
    let row: (String, String, i32, bool, String) = sqlx::query_as(
        "SELECT id::text, retention_class_key,
                octet_length(command_fingerprint), retain_rerun_input, input
         FROM horsies_tasks WHERE task_name = $1",
    )
    .bind(task_name)
    .fetch_one(&pool)
    .await
    .unwrap();
    let slot_id: Uuid = sqlx::query_scalar("SELECT horsies_schedule_slot_task_id($1, $2)")
        .bind(&schedule.name)
        .bind(slot)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.0, slot_id.to_string());
    assert_eq!(row.1, "standard_2d");
    assert_eq!(row.2, 32);
    assert!(!row.3);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&row.4).unwrap(),
        schedule.input
    );
    cleanup(&pool, task_name, &schedule.name).await;
}

/// Python `test_scheduler_task_options.py::test_a_scheduled_run_stores_the_timeout_and_retry_policy_of_its_task`:
/// a fired slot stores the task's options text and the retry limit it
/// sets, as a send of the task does (#480). The policy names
/// `TASK_EXCEPTION`, as the Python task does: a policy with no code does
/// not validate as Python `RetryPolicy`, and the send refuses it.
#[tokio::test]
#[serial]
async fn a_scheduled_run_stores_the_timeout_and_retry_policy_of_its_task() {
    let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
    let name = format!("fired_options_{}", Uuid::new_v4().simple());
    let mut schedule = interval_schedule_secs(&name, 60);
    schedule.task_name = name.clone();
    let options = crate::core::TaskOptions {
        task_name: name.clone(),
        queue_name: None,
        good_until: None,
        auto_retry_for: Some(vec![crate::core::TaskErrorCode::from(
            crate::core::OperationalErrorCode::TaskException,
        )]),
        retry_policy: Some(crate::core::RetryPolicy::fixed(vec![60, 120], true).unwrap()),
        timeout_ms: Some(5_000),
        execution: crate::core::task::Execution::Isolated,
        cancel_grace_ms: None,
    };
    let facts = ScheduleSendFacts {
        queue_name: "default".to_owned(),
        task_options: options.serialize_retry_options(),
    };
    let database_now = database_time(&pool).await;
    let slot = database_now - chrono::Duration::seconds(1);
    state::test_rows::seed_state(&pool, &name, slot).await;

    fire_once(
        &pool,
        &schedule,
        facts.clone(),
        &default_app_config(),
        slot,
        database_now,
    )
    .await
    .expect("the fire");
    let (stored, max_retries, queue): (Option<String>, i32, String) = sqlx::query_as(
        "SELECT task_options, max_retries, queue_name FROM horsies_tasks WHERE task_name = $1",
    )
    .bind(&name)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stored, facts.task_options);
    assert_eq!(max_retries, 2);
    assert_eq!(queue, "default");
    cleanup(&pool, &name, &name).await;
}

/// A late tick still within one period fires the stored slot and advances
/// one period from it (slot-anchored, not database-clock; horsies PR #46).
/// No skip occurs because the following slot is future.
#[tokio::test]
#[serial]
async fn a_single_late_tick_fires_the_stored_slot_and_anchors_the_next_run_to_it() {
    let pool = test_pool().await;
    let name = "pr46_slot_anchor";
    let task_name = "pr46_anchor_task";
    cleanup(&pool, task_name, name).await;
    let mut schedule = interval_schedule_secs(name, 5);
    schedule.task_name = task_name.to_owned();

    // A due slot at 12:00:00; the due read ran late at 12:00:03, within one
    // 5 s period, so the following slot (12:00:05) is future.
    let due_slot = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    state::test_rows::seed_state(&pool, name, due_slot).await;
    let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 3).unwrap();
    fire_once(
        &pool,
        &schedule,
        facts_for(&schedule),
        &default_app_config(),
        due_slot,
        now,
    )
    .await
    .expect("the fire");

    let row = state::test_rows::state_row(&pool, name).await.unwrap();
    assert_eq!(
        row.next_run_at,
        Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 5).unwrap(),
        "next_run must anchor to the due slot, not the database time",
    );
    assert_eq!(row.run_count, 1);
    let sent_at: DateTime<Utc> =
        sqlx::query_scalar("SELECT sent_at FROM horsies_tasks WHERE task_name = $1")
            .bind(task_name)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(sent_at, due_slot, "the fired slot is the due slot");
    cleanup(&pool, task_name, name).await;
}

/// C5: `catch_up_missed = false` with a backlog more than one period deep
/// fires only the latest due slot and stores a next run after the database
/// time of the due read. 5 s interval, stored slot 12:00:00, due read at
/// 12:00:17: missed 12:00:05/10/15, fire 12:00:15, next 12:00:20.
#[tokio::test]
#[serial]
async fn a_backlog_without_catch_up_fires_only_the_latest_due_slot() {
    let pool = test_pool().await;
    let name = "c5_skip_latest";
    let task_name = "c5_skip_task";
    cleanup(&pool, task_name, name).await;
    let mut schedule = interval_schedule_secs(name, 5);
    schedule.task_name = task_name.to_owned();

    let due_slot = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    state::test_rows::seed_state(&pool, name, due_slot).await;
    let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 17).unwrap();
    fire_once(
        &pool,
        &schedule,
        facts_for(&schedule),
        &default_app_config(),
        due_slot,
        now,
    )
    .await
    .expect("the fire");

    let row = state::test_rows::state_row(&pool, name).await.unwrap();
    assert_eq!(
        row.next_run_at,
        Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 20).unwrap(),
        "next_run must skip past the due read to the latest slot's successor",
    );
    assert!(row.next_run_at > now);
    let slots: Vec<DateTime<Utc>> = sqlx::query_scalar(
        "SELECT sent_at FROM horsies_tasks WHERE task_name = $1 ORDER BY sent_at",
    )
    .bind(task_name)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        slots,
        vec![Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 15).unwrap()],
        "only the latest due slot fires",
    );
    cleanup(&pool, task_name, name).await;
}

#[test]
fn advance_to_latest_due_slot_skips_backlog() {
    // Pure-unit: 5s interval, stored slot 12:00:00, now 12:00:17 → latest
    // due slot 12:00:15, next 12:00:20, skipped 3.
    let schedule = interval_schedule_secs("unit_skip", 5);
    let first_due = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 17).unwrap();
    let (slot, next, skipped) = advance_to_latest_due_slot(&enabled(&schedule), first_due, now);
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
    let (slot, next, skipped) = advance_to_latest_due_slot(&enabled(&schedule), first_due, now);
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
/// The self-heal creates a state row for an enabled schedule that has none
/// (with a strictly-future next_run), leaves an existing row's next_run
/// untouched, and skips disabled schedules. Parity with horsies PR #123.
#[tokio::test]
#[serial]
async fn ensure_states_exist_heals_missing_and_preserves_existing() {
    let pool = test_pool().await;
    let core = core_pool_of(&pool);
    let missing = format!("qw123_missing_{}", Uuid::new_v4());
    let disabled = format!("qw123_disabled_{}", Uuid::new_v4());

    let mut disabled_schedule = interval_schedule_secs(&disabled, 60);
    disabled_schedule.enabled = false;
    let schedules = vec![interval_schedule_secs(&missing, 60), disabled_schedule];

    let now = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    let healthy = ensure_states_exist(
        &core,
        StatementLog::Off,
        &enabled_schedules(&schedules),
        now,
    )
    .await;
    assert!(healthy, "a pass that heals its missing row counts healthy");

    let healed = state::test_rows::state_row(&pool, &missing).await.unwrap();
    assert!(
        healed.next_run_at > now,
        "healed next_run must be strictly future"
    );
    assert!(state::test_rows::state_row(&pool, &disabled)
        .await
        .is_none());

    // A later tick leaves the existing row's next_run untouched.
    let later = now + chrono::Duration::seconds(30);
    let healthy = ensure_states_exist(
        &core,
        StatementLog::Off,
        &enabled_schedules(&schedules),
        later,
    )
    .await;
    assert!(healthy, "all rows present is a healthy pass");
    let after = state::test_rows::state_row(&pool, &missing).await.unwrap();
    assert_eq!(
        after.next_run_at, healed.next_run_at,
        "existing next_run must be preserved across ticks",
    );

    core.close();
    for name in [missing, disabled] {
        state::test_rows::delete_state(&pool, &name).await;
    }
}

/// The start computes a new schedule's first run from the database time
/// of its read, and stores the schedule's digest (Python
/// `tests/integration/test_schedule_state_rows.py`,
/// `test_start_computes_the_first_run_from_the_database_time`).
#[tokio::test]
#[serial]
async fn initialize_schedules_computes_first_runs_from_the_database_time() {
    let pool = test_pool().await;
    let core = core_pool_of(&pool);
    let name = format!("qw728_start_{}", Uuid::new_v4());
    let schedules = vec![interval_schedule_secs(&name, 60)];

    let before = database_time(&pool).await;
    initialize_schedules(
        &core,
        StatementLog::Off,
        &HashSet::from([name.clone()]),
        &enabled_schedules(&schedules),
    )
    .await
    .expect("every pattern has a next slot");
    let after = database_time(&pool).await;

    let row = state::test_rows::state_row(&pool, &name).await.unwrap();
    assert!(row.next_run_at > before);
    assert!(row.next_run_at <= after + chrono::Duration::seconds(60));
    assert_eq!(
        row.config_hash,
        compute_config_hash(&enabled(&schedules[0])).to_vec()
    );
    assert_eq!(row.run_count, 0);

    core.close();
    state::test_rows::delete_state(&pool, &name).await;
}

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

#[cfg(test)]
mod fire_tests;

#[cfg(test)]
mod manifest_tests;

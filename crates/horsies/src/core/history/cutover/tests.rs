use chrono::Utc;
use clap::Parser;
use serial_test::serial;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, FromRow, PgConnection, PgPool};
use std::str::FromStr;
use uuid::Uuid;

use super::drain::DrainOutcome;
use super::identity::{AttemptIdentityNormalization, AttemptIdentityRestoration};
use super::ladder::{
    evaluate_rung, fit_run, BatchCommit, MeasuredRun, RungOutcome, LADDER, RUNG_FLOOR_DENOMINATOR,
    RUNG_FLOOR_NUMERATOR,
};
use super::preflight::{
    RelocationCoefficients, PLANNING_CEILING_DENOMINATOR, PLANNING_CEILING_NUMERATOR,
};
use super::program::{install_programs, rendered_statement_starting, ProgramInstallation};
use super::runner::{
    read_status, run_cutover, stage_drain, stage_install_programs, stage_normalize_identity,
    stage_preflight, stage_prepare, stage_relocate, stage_rollback_programs, stage_tighten,
    stage_validate, CutoverRunOptions,
};
use super::state::cutover_complete;
use super::tighten::{confirmation_phrase, TightenOutcome};
use super::validation::ValidationOutcome;
use crate::broker::migrations::{run_horsies_migrations, run_horsies_migrations_through};
use crate::broker::terminalization::terminalize;
use crate::broker::PostgresBroker;
use crate::core::history::archive::attempts::decode_attempt_snapshot;
use crate::core::history::maintenance::coverage::{
    ensure_startup_coverage, StartupCoverageOutcome,
};
use crate::core::history::phase2::consumption::{consume_phase2, Phase2DispositionKind};
use crate::core::history::reads::publisher::StagedLoaderPublisher;
use crate::core::lifecycle::{OwnedClaim, TerminalizationCommand, TerminalizationOutcome};
use crate::worker::cli::{Cli, Command};

fn commits(slope: f64, intercept: f64) -> Vec<BatchCommit> {
    (1..=4)
        .map(|index| BatchCommit {
            cumulative_rows: index * 250_000,
            elapsed_seconds: intercept + slope * index as f64 / 4.0,
        })
        .collect()
}

#[test]
fn ladder_fit_and_both_inclusive_bounds_are_exact() {
    assert_eq!((RUNG_FLOOR_NUMERATOR, RUNG_FLOOR_DENOMINATOR), (7, 10));
    assert_eq!(
        (PLANNING_CEILING_NUMERATOR, PLANNING_CEILING_DENOMINATOR),
        (5, 4)
    );
    assert_eq!(
        LADDER.map(|rung| (rung.rows, rung.contingent)),
        [(1_000_000, false), (10_000_000, false), (100_000_000, true),]
    );
    let fitted = fit_run(&MeasuredRun {
        rows: 1_000_000,
        seconds: 150.0,
        fixed_seconds: 120.0,
        preparation_seconds: 20.0,
        commits: commits(30.0, 7.5),
    })
    .unwrap();
    assert!((fitted.coefficients.seconds_per_million_rows() - 30.0).abs() < 1e-9);
    assert_eq!(fitted.coefficients.fixed_seconds(), 120.0);
    assert_eq!(
        fitted.coefficients.preparation_seconds_per_million_rows(),
        20.0
    );
    assert!((fitted.regression_intercept_seconds - 7.5).abs() < 1e-9);

    let coefficients = RelocationCoefficients::new(120.0, 30.0, 0.0).unwrap();
    for seconds in [105.0, 187.5] {
        assert!(matches!(
            evaluate_rung(
                LADDER[0],
                coefficients,
                &MeasuredRun {
                    rows: 1_000_000,
                    seconds,
                    fixed_seconds: 30.0,
                    preparation_seconds: 0.0,
                    commits: commits(120.0, 0.0),
                },
            )
            .unwrap(),
            RungOutcome::Passed { .. }
        ));
    }
    assert!(matches!(
        evaluate_rung(
            LADDER[0],
            coefficients,
            &MeasuredRun {
                rows: 1_000_000,
                seconds: 187.500_001,
                fixed_seconds: 30.0,
                preparation_seconds: 0.0,
                commits: Vec::new(),
            },
        )
        .unwrap(),
        RungOutcome::Busted { .. }
    ));
    assert!(matches!(
        evaluate_rung(
            LADDER[0],
            coefficients,
            &MeasuredRun {
                rows: 1_000_000,
                seconds: 104.999_999,
                fixed_seconds: 30.0,
                preparation_seconds: 0.0,
                commits: Vec::new(),
            },
        )
        .unwrap(),
        RungOutcome::Overpredicted { .. }
    ));
}

#[test]
fn ladder_refuses_fewer_than_two_distinct_commit_points() {
    for observations in [
        vec![BatchCommit {
            cumulative_rows: 1,
            elapsed_seconds: 1.0,
        }],
        vec![
            BatchCommit {
                cumulative_rows: 1,
                elapsed_seconds: 1.0,
            },
            BatchCommit {
                cumulative_rows: 1,
                elapsed_seconds: 2.0,
            },
        ],
    ] {
        assert!(fit_run(&MeasuredRun {
            rows: 1,
            seconds: 2.0,
            fixed_seconds: 0.0,
            preparation_seconds: 0.0,
            commits: observations,
        })
        .is_err());
    }
}

#[test]
fn cli_exposes_every_ratified_cutover_subcommand() {
    for subcommand in [
        "preflight",
        "ladder-evaluate",
        "drain",
        "install-programs",
        "prepare",
        "relocate",
        "tighten",
        "validate",
        "rollback-programs",
        "status",
        "run",
    ] {
        let result = Cli::try_parse_from(["horsies", "cutover", subcommand, "--help"]);
        assert!(result.is_err_and(|error| {
            error.kind() == clap::error::ErrorKind::DisplayHelp
                && error.to_string().contains(subcommand)
        }));
    }
    let parsed = Cli::try_parse_from([
        "horsies",
        "cutover",
        "--database-url",
        "postgresql://invalid/example",
        "status",
    ])
    .unwrap();
    assert!(matches!(parsed.command, Command::Cutover(_)));
}

const P9_DATABASE_PREFIX: &str = "horsies_p9_cutover_";

fn test_db_url() -> String {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return url;
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .find(|path| path.join(".env").exists());
    let password = root
        .and_then(|path| std::fs::read_to_string(path.join(".env")).ok())
        .and_then(|content| {
            content
                .lines()
                .filter_map(|line| line.trim().split_once('='))
                .find(|(key, _)| key.trim() == "DB_PASSWORD")
                .map(|(_, value)| value.trim().to_owned())
        })
        .unwrap_or_else(|| "W0rklane".to_owned());
    format!("postgresql://postgres:{password}@localhost:5432/postgres")
}

struct P9Database {
    name: String,
    pool: PgPool,
    base_options: PgConnectOptions,
}

impl P9Database {
    async fn create() -> Self {
        let base_options = PgConnectOptions::from_str(&test_db_url()).unwrap();
        let mut admin = PgConnection::connect_with(&base_options.clone().database("postgres"))
            .await
            .unwrap();
        sqlx::query("SELECT pg_advisory_lock(hashtext('horsies_p9_cutover_setup'))")
            .execute(&mut admin)
            .await
            .unwrap();
        let stale: Vec<String> = sqlx::query_scalar(
            "SELECT d.datname FROM pg_database AS d
             WHERE left(d.datname, length('horsies_p9_cutover_')) =
                   'horsies_p9_cutover_'
               AND NOT EXISTS (
                   SELECT 1 FROM pg_stat_activity AS a WHERE a.datname = d.datname
               )
             ORDER BY d.datname",
        )
        .fetch_all(&mut admin)
        .await
        .unwrap();
        for database in stale {
            let suffix = database.strip_prefix(P9_DATABASE_PREFIX).unwrap();
            assert!(
                suffix.len() == 32 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "refuse to drop non-generated P9 database {database:?}"
            );
            sqlx::query(&format!("DROP DATABASE \"{database}\""))
                .execute(&mut admin)
                .await
                .unwrap();
        }
        let name = format!("{P9_DATABASE_PREFIX}{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE DATABASE \"{name}\""))
            .execute(&mut admin)
            .await
            .unwrap();
        let pool = PgPoolOptions::new()
            .min_connections(1)
            .max_connections(5)
            .max_lifetime(None)
            .idle_timeout(None)
            .connect_with(base_options.clone().database(&name))
            .await
            .unwrap();
        let unlocked: bool =
            sqlx::query_scalar("SELECT pg_advisory_unlock(hashtext('horsies_p9_cutover_setup'))")
                .fetch_one(&mut admin)
                .await
                .unwrap();
        assert!(unlocked);
        Self {
            name,
            pool,
            base_options,
        }
    }

    async fn fresh_pool(&self) -> PgPool {
        PgPoolOptions::new()
            .min_connections(1)
            .max_connections(5)
            .max_lifetime(None)
            .idle_timeout(None)
            .connect_with(self.base_options.clone().database(&self.name))
            .await
            .unwrap()
    }

    async fn destroy(self) {
        self.pool.close().await;
        let mut admin = PgConnection::connect_with(&self.base_options.database("postgres"))
            .await
            .unwrap();
        let active: i64 =
            sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname = $1")
                .bind(&self.name)
                .fetch_one(&mut admin)
                .await
                .unwrap();
        assert_eq!(active, 0, "generated P9 database still has sessions");
        sqlx::query(&format!("DROP DATABASE \"{}\"", self.name))
            .execute(&mut admin)
            .await
            .unwrap();
    }
}

#[derive(Clone, Copy)]
struct SeededIds {
    recorded_terminal: Uuid,
    legacy_failed: Uuid,
    pending: Uuid,
    workflow_unconsumed_task: Uuid,
    workflow_consumed_task: Uuid,
    workflow_unconsumed: Uuid,
    workflow_consumed: Uuid,
    node_unconsumed: Uuid,
    node_consumed: Uuid,
    phase2_generation: Uuid,
}

#[derive(Debug, FromRow)]
struct Phase2Evidence {
    task_id: Uuid,
    workflow_id: Uuid,
    workflow_node_row_id: Uuid,
    terminal_status: String,
    terminal_at: chrono::DateTime<Utc>,
    terminalization_kind: String,
    recovery_source: String,
    history_class: Option<String>,
    history_anchor: Option<chrono::DateTime<Utc>>,
    history_schema_version: i16,
    result_digest: Vec<u8>,
    quarantine_task_id: Option<Uuid>,
    phase2_generation: Uuid,
    created_at: chrono::DateTime<Utc>,
    attempt_count: i32,
    last_attempt_at: Option<chrono::DateTime<Utc>>,
    last_failure_class: Option<String>,
}

async fn seed_v32_population(pool: &PgPool) -> SeededIds {
    run_horsies_migrations_through(pool, 32).await.unwrap();
    let ids = SeededIds {
        recorded_terminal: Uuid::new_v4(),
        legacy_failed: Uuid::new_v4(),
        pending: Uuid::new_v4(),
        workflow_unconsumed_task: Uuid::new_v4(),
        workflow_consumed_task: Uuid::new_v4(),
        workflow_unconsumed: Uuid::new_v4(),
        workflow_consumed: Uuid::new_v4(),
        node_unconsumed: Uuid::new_v4(),
        node_consumed: Uuid::new_v4(),
        phase2_generation: Uuid::new_v4(),
    };
    let terminal_at = Utc::now();
    for (id, status, kind, result, args, is_workflow) in [
        (
            ids.recorded_terminal,
            "COMPLETED",
            Some("COMPLETE_LOCKED"),
            Some("{\"ok\":1}"),
            Some("[1]"),
            false,
        ),
        (
            ids.legacy_failed,
            "FAILED",
            None,
            Some("{\"__type\":\"err\"}"),
            Some("{broken"),
            false,
        ),
        (
            ids.workflow_unconsumed_task,
            "COMPLETED",
            Some("COMPLETE_LOCKED"),
            Some("{\"ok\":2}"),
            Some("[2]"),
            true,
        ),
        (
            ids.workflow_consumed_task,
            "COMPLETED",
            None,
            Some("{\"ok\":3}"),
            Some("[3]"),
            true,
        ),
    ] {
        sqlx::query(
            "INSERT INTO horsies_tasks (
                 id, task_name, queue_name, priority, args, kwargs, status,
                 sent_at, enqueued_at, completed_at, failed_at, result,
                 failed_reason, claimed, retry_count, max_retries, task_options,
                 created_at, updated_at, enqueue_sha, is_workflow_task,
                 terminal_at, terminalization_kind, error_code
             ) VALUES (
                 $1, 'p9.legacy', 'default', 50, $2, '{}', $3,
                 $4, $4, CASE WHEN $3 = 'COMPLETED' THEN $4 END,
                 CASE WHEN $3 = 'FAILED' THEN $4 END, $5,
                 CASE WHEN $3 = 'FAILED' THEN 'legacy failure' END,
                 FALSE, 0, 0, '{}', $4, $4, repeat('a', 64), $6,
                 $4, $7, CASE WHEN $3 = 'FAILED' THEN 'LEGACY' END
             )",
        )
        .bind(id.to_string())
        .bind(args)
        .bind(status)
        .bind(terminal_at)
        .bind(result)
        .bind(is_workflow)
        .bind(kind)
        .execute(pool)
        .await
        .unwrap();
    }
    sqlx::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, args, kwargs, status,
             sent_at, enqueued_at, claimed, retry_count, max_retries,
             task_options, created_at, updated_at, enqueue_sha, is_workflow_task
         ) VALUES (
             $1, 'p9.pending', 'default', 50, '[9]', '{}', 'PENDING',
             $2, $2, FALSE, 0, 0, '{}', $2, $2, repeat('b', 64), FALSE
         )",
    )
    .bind(ids.pending.to_string())
    .bind(terminal_at)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO horsies_task_attempts (
             task_id, attempt, outcome, will_retry, started_at, finished_at,
             error_code, error_message, failed_reason, worker_id
         ) VALUES ($1, 1, 'FAILED', FALSE, $2, $2, 'LEGACY',
                   'failed', 'the recorded reason', 'old-worker')",
    )
    .bind(ids.legacy_failed.to_string())
    .bind(terminal_at)
    .execute(pool)
    .await
    .unwrap();
    for (workflow_id, node_id, task_id, task_index, node_status) in [
        (
            ids.workflow_unconsumed,
            ids.node_unconsumed,
            ids.workflow_unconsumed_task,
            0,
            "RUNNING",
        ),
        (
            ids.workflow_consumed,
            ids.node_consumed,
            ids.workflow_consumed_task,
            1,
            "COMPLETED",
        ),
    ] {
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, depth, root_workflow_id,
                 created_at, updated_at, sent_at
             ) VALUES ($1, 'p9 workflow', 'RUNNING', 'fail', 0, $1, $2, $2, $2)",
        )
        .bind(workflow_id.to_string())
        .bind(terminal_at)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                 id, workflow_id, task_index, node_id, task_name, queue_name,
                 priority, dependencies, allow_failed_deps, join_type, status,
                 task_id, is_subworkflow, created_at, completed_at
             ) VALUES ($1, $2, $3, $4, 'p9.legacy', 'default', 50, '{}',
                       FALSE, 'all', $7, $5, FALSE, $6,
                       CASE WHEN $7 = 'COMPLETED' THEN $6 END)",
        )
        .bind(node_id.to_string())
        .bind(workflow_id.to_string())
        .bind(task_index)
        .bind(format!("node-{task_index}"))
        .bind(task_id.to_string())
        .bind(terminal_at)
        .bind(node_status)
        .execute(pool)
        .await
        .unwrap();
    }
    sqlx::query(
        "INSERT INTO horsies_heartbeats (
             task_id, sender_id, role, sent_at, hostname, pid
         ) VALUES ($1, 'old-worker', 'worker', $2 - interval '5 minutes',
                   'legacy-host', 7)",
    )
    .bind(ids.pending.to_string())
    .bind(terminal_at)
    .execute(pool)
    .await
    .unwrap();

    run_horsies_migrations(pool).await.unwrap();
    sqlx::query(
        r#"INSERT INTO horsies_workflow_phase2_pending (
             task_id, workflow_id, workflow_node_row_id, terminal_status,
             terminal_at, terminalization_kind, recovery_source,
             history_class, history_anchor, history_schema_version,
             result_digest, phase2_generation, created_at, attempt_count
         ) VALUES (
             $1, $2, $3, 'COMPLETED', $4, 'COMPLETE_LOCKED', 'HISTORY',
             'forever', $4, 1, sha256(convert_to('{"ok":2}', 'UTF8')), $5, $4, 0
         )"#,
    )
    .bind(ids.workflow_unconsumed_task)
    .bind(ids.workflow_unconsumed)
    .bind(ids.node_unconsumed)
    .bind(terminal_at)
    .bind(ids.phase2_generation)
    .execute(pool)
    .await
    .unwrap();
    ids
}

fn coefficients() -> RelocationCoefficients {
    RelocationCoefficients::new(120.0, 30.0, 600.0).unwrap()
}

#[tokio::test]
#[serial]
async fn populated_v32_pipeline_reaches_attested_v35_and_completes_the_survivor() {
    let database = P9Database::create().await;
    let pool = database.pool.clone();
    let ids = seed_v32_population(&pool).await;

    let stored: i64 =
        sqlx::query_scalar("SELECT max(version) FROM horsies_migrations WHERE success")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(stored, crate::broker::migrations::expected_schema_version());
    let transitional: (bool, i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT atttypid = 'varchar'::regtype FROM pg_attribute
              WHERE attrelid = 'horsies_tasks'::regclass AND attname = 'id'),
             (SELECT count(*) FROM horsies_tasks),
             (SELECT count(*) FROM horsies_cutover_state)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(transitional, (true, 5, 0));

    let preflight = stage_preflight(&pool, coefficients()).await.unwrap();
    assert_eq!(preflight.terminal_live_rows, 4);
    assert_eq!(preflight.unrecorded_kind_rows, 2);
    assert_eq!(preflight.unprepared_envelope_rows, 4);
    assert_eq!(preflight.unclassified_rows, 4);
    assert_eq!(preflight.heartbeat_rows, 1);
    assert_eq!(preflight.estimate.coefficients, coefficients());
    assert_eq!(
        preflight.estimate.ceiling_seconds,
        preflight.estimate.total_seconds * 1.25
    );
    assert_eq!(preflight.advisories.len(), 1);
    assert!(preflight.advisories[0].contains("forever"));

    sqlx::query(
        "UPDATE horsies_tasks SET status = 'RUNNING', claimed = TRUE,
         claimed_by_worker_id = 'still-running', claimed_at = now()
         WHERE id = $1",
    )
    .bind(ids.pending.to_string())
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        stage_drain(&pool, 60.0).await.unwrap(),
        DrainOutcome::Blocked {
            running_rows: 1,
            ..
        }
    ));
    sqlx::query(
        "UPDATE horsies_tasks SET status = 'PENDING', claimed = FALSE,
         claimed_by_worker_id = NULL, claimed_at = NULL WHERE id = $1",
    )
    .bind(ids.pending.to_string())
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        stage_drain(&pool, 60.0).await.unwrap(),
        DrainOutcome::Verified { pending_rows: 1 }
    ));

    let mut refusal_tx = pool.begin().await.unwrap();
    assert!(matches!(
        install_programs(refusal_tx.as_mut()).await.unwrap(),
        ProgramInstallation::Refused { ref reasons }
            if reasons.iter().any(|reason| reason.contains("identity"))
    ));
    refusal_tx.rollback().await.unwrap();
    assert!(matches!(
        stage_tighten(&pool, "not-backed-up", "yes").await.unwrap(),
        TightenOutcome::Refused { ref reasons }
            if reasons.iter().any(|reason| reason.contains("confirmation"))
            && reasons.iter().any(|reason| reason.contains("terminal rows remain"))
            && reasons.iter().any(|reason| reason.contains("preparation incomplete"))
    ));

    sqlx::query("CREATE TABLE p9_attempt_refs (task_id varchar(36) PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO p9_attempt_refs (task_id) VALUES ($1)")
        .bind(ids.legacy_failed.to_string())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "ALTER TABLE horsies_task_attempts ADD CONSTRAINT p9_unexpected_attempt_fk
         FOREIGN KEY (task_id) REFERENCES p9_attempt_refs(task_id) ON DELETE CASCADE",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        stage_normalize_identity(&pool).await.unwrap(),
        AttemptIdentityNormalization::Refused { ref reasons }
            if reasons.iter().any(|reason| reason.contains("exactly the canonical"))
    ));
    let attempt_fks: Vec<String> = sqlx::query_scalar(
        "SELECT conname FROM pg_constraint
         WHERE conrelid = 'horsies_task_attempts'::regclass AND contype = 'f'
         ORDER BY conname",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        attempt_fks,
        [
            "horsies_task_attempts_task_id_fkey",
            "p9_unexpected_attempt_fk",
        ]
    );
    sqlx::query("ALTER TABLE horsies_task_attempts DROP CONSTRAINT p9_unexpected_attempt_fk")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DROP TABLE p9_attempt_refs")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        stage_normalize_identity(&pool).await.unwrap(),
        AttemptIdentityNormalization::Converted
    ));
    let installed_count = match stage_install_programs(&pool).await.unwrap() {
        ProgramInstallation::Installed {
            statements_executed,
        } => statements_executed,
        refused => panic!("unexpected program refusal: {refused:?}"),
    };
    assert_eq!(installed_count, 41);
    let rollback = stage_rollback_programs(&pool).await.unwrap();
    assert!(matches!(
        rollback,
        super::program::ProgramRollback::RolledBack {
            teardown_statements_executed: 12,
            attempt_identity: AttemptIdentityRestoration::Restored,
        }
    ));
    let restored_attempt_shape: (String, bool) = sqlx::query_as(
        "SELECT format_type(att.atttypid, att.atttypmod),
                EXISTS (
                    SELECT 1 FROM pg_constraint AS con
                    WHERE con.conrelid = 'horsies_task_attempts'::regclass
                      AND con.conname = 'horsies_task_attempts_task_id_fkey'
                      AND con.confrelid = 'horsies_tasks'::regclass
                      AND con.confdeltype = 'c'
                )
         FROM pg_attribute AS att
         WHERE att.attrelid = 'horsies_task_attempts'::regclass
           AND att.attname = 'task_id'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        restored_attempt_shape,
        ("character varying(36)".to_owned(), true)
    );
    assert!(sqlx::query_scalar::<_, bool>(
        "SELECT to_regprocedure(
             'horsies_move_task_to_history(uuid,text,text,timestamptz,text,text,text)'
         ) IS NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap());
    for signature in [
        "horsies_terminalization_miss(varchar,text[],text,timestamp with time zone)",
        "horsies_complete_locked_task(varchar,text,text)",
        "horsies_complete_task_fused(varchar,text,timestamp with time zone,text,text,text)",
        "horsies_fail_locked_task(varchar,text,text,text,text)",
        "horsies_fail_stale_task(varchar,integer,integer,text,text,text)",
        "horsies_expire_owned_claim(varchar,text,text,text)",
        "horsies_expire_pending_tasks(integer,text,text)",
        "horsies_cancel_locked_task(varchar,text[])",
        "horsies_cancel_owned_orphan(varchar,text,timestamp with time zone)",
        "horsies_cancel_orphaned_tasks(integer)",
        "horsies_abandon_owned_node(varchar,text,timestamp with time zone)",
        "horsies_abandon_owned_nodes(varchar[],timestamp with time zone[],text)",
        "horsies_abandon_nodes_of_paused_workflows(varchar[])",
        "horsies_cancel_owned_node(varchar,text,timestamp with time zone,boolean)",
        "horsies_cancel_owned_nodes(varchar[],timestamp with time zone[],text)",
        "horsies_cancel_nodes_of_cancelled_workflow(varchar[])",
    ] {
        let definition: Option<String> =
            sqlx::query_scalar("SELECT pg_get_functiondef(to_regprocedure($1))")
                .bind(signature)
                .fetch_optional(&pool)
                .await
                .unwrap()
                .flatten();
        assert!(
            definition.is_some_and(|definition| definition.contains("LANGUAGE plpgsql")),
            "R2 did not restore {signature}"
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT outcome FROM horsies_terminalization_miss(
                 $1::varchar, ARRAY['COMPLETE_LOCKED']::text[], NULL, NULL
             )",
        )
        .bind(Uuid::new_v4().to_string())
        .fetch_one(&pool)
        .await
        .unwrap(),
        "TASK_ABSENT"
    );
    let r2_task_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, args, kwargs, status,
             sent_at, enqueued_at, claimed, retry_count, max_retries,
             task_options, created_at, updated_at, enqueue_sha, is_workflow_task
         ) VALUES (
             $1, 'p9.r2', 'default', 50, '[]', '{}', 'PENDING',
             now(), now(), FALSE, 0, 0, '{}', now(), now(), repeat('d', 64), FALSE
         )",
    )
    .bind(r2_task_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    let r2_claimed_at: chrono::DateTime<Utc> = sqlx::query_scalar(
        "UPDATE horsies_tasks
         SET status = 'RUNNING', claimed = TRUE,
             claimed_by_worker_id = 'p9-r2-old-worker',
             claimed_at = clock_timestamp(), started_at = clock_timestamp()
         WHERE id = $1
         RETURNING claimed_at",
    )
    .bind(r2_task_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    let r2_outcome: (String, Option<String>) = sqlx::query_as(
        "SELECT outcome, terminalization_kind
         FROM horsies_complete_task_fused(
             $1::varchar, 'p9-r2-old-worker', $2, '{\"ok\":\"r2\"}',
             'p9_r2_done', $1::text
         )",
    )
    .bind(r2_task_id.to_string())
    .bind(r2_claimed_at)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        r2_outcome,
        ("APPLIED".to_owned(), Some("COMPLETE_FUSED".to_owned()))
    );
    let r2_attempt: (String, String, String) = sqlx::query_as(
        "SELECT task_id, outcome, worker_id
         FROM horsies_task_attempts WHERE task_id = $1",
    )
    .bind(r2_task_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        r2_attempt,
        (
            r2_task_id.to_string(),
            "COMPLETED".to_owned(),
            "p9-r2-old-worker".to_owned(),
        )
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM horsies_tasks WHERE id = $1")
            .bind(r2_task_id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap(),
        "COMPLETED"
    );
    let deleted = sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
        .bind(r2_task_id.to_string())
        .execute(&pool)
        .await
        .unwrap()
        .rows_affected();
    assert_eq!(deleted, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM horsies_task_attempts WHERE task_id = $1"
        )
        .bind(r2_task_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    assert!(matches!(
        stage_normalize_identity(&pool).await.unwrap(),
        AttemptIdentityNormalization::Converted
    ));
    assert!(matches!(
        stage_install_programs(&pool).await.unwrap(),
        ProgramInstallation::Installed { .. }
    ));

    let prepared = stage_prepare(&pool, true, 2).await.unwrap();
    assert_eq!(prepared.rows_prepared, 5);
    assert_eq!(prepared.live_rows_prepared, 1);
    assert_eq!(prepared.decode_failed_rows, 1);
    assert_eq!(prepared.batches_committed, 3);
    assert_eq!(
        stage_prepare(&pool, true, 2).await.unwrap().rows_prepared,
        0
    );

    sqlx::query(
        "ALTER TABLE horsies_cutover_relocation_ledger
         ADD CONSTRAINT p9_force_ledger_failure CHECK (batch_number < 0)",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(stage_relocate(&pool, 2).await.is_err());
    let rolled_back: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM horsies_tasks
              WHERE status NOT IN ('PENDING', 'CLAIMED', 'RUNNING')),
             (SELECT count(*) FROM horsies_task_attempts),
             (SELECT count(*) FROM horsies_task_history),
             (SELECT count(*) FROM horsies_cutover_relocation_ledger)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(rolled_back, (4, 1, 0, 0));
    sqlx::query(
        "ALTER TABLE horsies_cutover_relocation_ledger
         DROP CONSTRAINT p9_force_ledger_failure",
    )
    .execute(&pool)
    .await
    .unwrap();

    let relocated = stage_relocate(&pool, 2).await.unwrap();
    assert_eq!(relocated.rows_relocated, 4);
    assert_eq!(relocated.legacy_kind_rows, 2);
    assert_eq!(relocated.batches_committed, 2);
    assert_eq!(
        stage_relocate(&pool, 2).await.unwrap().rows_relocated,
        4,
        "resumed relocation reports the durable ledger total"
    );
    sqlx::query("UPDATE horsies_workflows SET root_workflow_id = 'not-a-uuid' WHERE id = $1")
        .bind(ids.workflow_unconsumed.to_string())
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        stage_tighten(
            &pool,
            "p9-disposable.dump",
            &confirmation_phrase("p9-disposable.dump"),
        )
        .await
        .unwrap(),
        TightenOutcome::Refused { ref reasons }
            if reasons == &["1 rows in horsies_workflows.root_workflow_id do not parse as uuid"]
    ));
    assert!(sqlx::query_scalar::<_, bool>(
        "SELECT atttypid = 'varchar'::regtype FROM pg_attribute
         WHERE attrelid = 'horsies_tasks'::regclass AND attname = 'id'",
    )
    .fetch_one(&pool)
    .await
    .unwrap());
    sqlx::query("UPDATE horsies_workflows SET root_workflow_id = id WHERE id = $1")
        .bind(ids.workflow_unconsumed.to_string())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "ALTER TABLE horsies_workflow_tasks
         DROP CONSTRAINT horsies_workflow_tasks_sub_workflow_id_fkey",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        stage_tighten(
            &pool,
            "p9-disposable.dump",
            &confirmation_phrase("p9-disposable.dump"),
        )
        .await
        .unwrap(),
        TightenOutcome::Refused { ref reasons }
            if reasons.iter().any(|reason| reason.contains("foreign-key topology drifted"))
    ));
    assert!(sqlx::query_scalar::<_, bool>(
        "SELECT atttypid = 'varchar'::regtype FROM pg_attribute
         WHERE attrelid = 'horsies_tasks'::regclass AND attname = 'id'",
    )
    .fetch_one(&pool)
    .await
    .unwrap());
    sqlx::query(
        "ALTER TABLE horsies_workflow_tasks
         ADD CONSTRAINT horsies_workflow_tasks_sub_workflow_id_fkey
         FOREIGN KEY (sub_workflow_id) REFERENCES horsies_workflows(id)
         ON DELETE SET NULL",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        stage_tighten(&pool, "p9-disposable.dump", "wrong")
            .await
            .unwrap(),
        TightenOutcome::Refused { ref reasons }
            if reasons.len() == 1 && reasons[0].contains("confirmation")
    ));
    assert!(matches!(
        stage_tighten(
            &pool,
            "p9-disposable.dump",
            &confirmation_phrase("p9-disposable.dump"),
        )
        .await
        .unwrap(),
        TightenOutcome::Complete { .. }
    ));
    assert!(!{
        let mut connection = pool.acquire().await.unwrap();
        cutover_complete(&mut connection).await.unwrap()
    });
    assert!(matches!(
        stage_validate(&pool).await.unwrap(),
        ValidationOutcome::Validated {
            history_rows: 4,
            ledger_rows: 4
        }
    ));
    assert!(matches!(
        stage_rollback_programs(&pool).await.unwrap(),
        super::program::ProgramRollback::Refused { ref reasons }
            if reasons.iter().any(|reason| reason.contains("point of no return"))
                && reasons.iter().any(|reason| reason.contains("attested"))
    ));
    assert!(sqlx::query_scalar::<_, bool>(
        "SELECT to_regprocedure(
             'horsies_move_task_to_history(uuid,text,text,timestamptz,text,text,text)'
         ) IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap());
    assert!({
        let mut connection = pool.acquire().await.unwrap();
        cutover_complete(&mut connection).await.unwrap()
    });

    let shapes: (bool, bool, String, i64, i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT count(*) = 10 AND bool_and(atttypid = 'uuid'::regtype)
              FROM (VALUES
                  ('horsies_tasks', 'id'),
                  ('horsies_task_attempts', 'task_id'),
                  ('horsies_workflows', 'id'),
                  ('horsies_workflows', 'parent_workflow_id'),
                  ('horsies_workflows', 'root_workflow_id'),
                  ('horsies_workflow_tasks', 'id'),
                  ('horsies_workflow_tasks', 'workflow_id'),
                  ('horsies_workflow_tasks', 'task_id'),
                  ('horsies_workflow_tasks', 'sub_workflow_id'),
                  ('horsies_heartbeats', 'task_id')
              ) AS expected(relation, column_name)
              JOIN pg_attribute ON attrelid = relation::regclass
                               AND attname = column_name),
             (SELECT relkind = 'p' FROM pg_class
              WHERE oid = 'horsies_heartbeats'::regclass),
             (SELECT pg_get_partkeydef('horsies_heartbeats'::regclass)),
             (SELECT count(*) FROM horsies_heartbeats),
             (SELECT count(*) FROM horsies_workflow_phase2_pending),
             (SELECT count(*) FROM horsies_task_attempts)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(shapes, (true, true, "RANGE (sent_at)".to_owned(), 0, 1, 0));
    let frozen_columns: Vec<(String, String, String, bool)> = sqlx::query_as(
        "SELECT column_name, data_type, udt_name, is_nullable = 'NO'
         FROM information_schema.columns
         WHERE table_schema = current_schema() AND table_name = 'horsies_heartbeats'
         ORDER BY ordinal_position",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        frozen_columns,
        [
            (
                "id".to_owned(),
                "bigint".to_owned(),
                "int8".to_owned(),
                true
            ),
            (
                "task_id".to_owned(),
                "uuid".to_owned(),
                "uuid".to_owned(),
                true
            ),
            (
                "sender_id".to_owned(),
                "character varying".to_owned(),
                "varchar".to_owned(),
                true
            ),
            (
                "role".to_owned(),
                "character varying".to_owned(),
                "varchar".to_owned(),
                true
            ),
            (
                "sent_at".to_owned(),
                "timestamp with time zone".to_owned(),
                "timestamptz".to_owned(),
                true
            ),
            (
                "hostname".to_owned(),
                "character varying".to_owned(),
                "varchar".to_owned(),
                false
            ),
            (
                "pid".to_owned(),
                "integer".to_owned(),
                "int4".to_owned(),
                false
            ),
        ]
    );
    let live_status_definition: String = sqlx::query_scalar(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint
         WHERE conrelid = 'horsies_tasks'::regclass
           AND conname = 'horsies_tasks_live_status_only'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        live_status_definition,
        "CHECK (((status)::text = ANY ((ARRAY['PENDING'::character varying, 'CLAIMED'::character varying, 'RUNNING'::character varying])::text[])))"
    );
    let history: Vec<(Uuid, String, String, String, Option<String>)> = sqlx::query_as(
        "SELECT task_id, status, terminalization_kind, retention_class_key,
                final_failed_reason
         FROM horsies_task_history ORDER BY task_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(history.len(), 4);
    assert!(history.iter().all(|row| row.3 == "forever"));
    assert!(history.iter().any(|row| {
        row.0 == ids.legacy_failed
            && row.2 == "LEGACY_TERMINAL"
            && row.4.as_deref() == Some("the recorded reason")
    }));
    assert!(history
        .iter()
        .any(|row| { row.0 == ids.workflow_consumed_task && row.2 == "LEGACY_TERMINAL" }));
    let attempt_archive: (i16, String, String, Vec<u8>, Vec<u8>) = sqlx::query_as(
        "SELECT attempt_archive_version, attempt_snapshot_codec,
                attempt_snapshot_content_type, attempt_snapshot,
                attempt_snapshot_digest
         FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(ids.legacy_failed)
    .fetch_one(&pool)
    .await
    .unwrap();
    let attempts = decode_attempt_snapshot(
        attempt_archive.0,
        &attempt_archive.1,
        &attempt_archive.2,
        &attempt_archive.3,
        &attempt_archive.4,
    )
    .unwrap();
    assert_eq!(attempts.len(), 1);
    let attempt = &attempts[0];
    assert_eq!(attempt.attempt(), 1);
    assert_eq!(attempt.outcome(), "FAILED");
    assert!(!attempt.will_retry());
    assert_eq!(attempt.error_code(), Some("LEGACY"));
    assert_eq!(attempt.error_message(), Some("failed"));
    assert_eq!(attempt.failed_reason(), Some("the recorded reason"));
    assert_eq!(attempt.worker_id(), Some("old-worker"));
    assert_eq!(attempt.worker_hostname(), None);
    assert_eq!(attempt.worker_pid(), None);
    assert_eq!(attempt.worker_process_name(), None);

    let pending_evidence: Vec<Phase2Evidence> = sqlx::query_as(
        "SELECT task_id, workflow_id, workflow_node_row_id, terminal_status,
                terminal_at, terminalization_kind, recovery_source,
                history_class, history_anchor, history_schema_version,
                result_digest, quarantine_task_id, phase2_generation,
                created_at, attempt_count, last_attempt_at, last_failure_class
         FROM horsies_workflow_phase2_pending ORDER BY task_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(pending_evidence.len(), 1);
    let evidence = &pending_evidence[0];
    assert_eq!(evidence.task_id, ids.workflow_unconsumed_task);
    assert_eq!(evidence.workflow_id, ids.workflow_unconsumed);
    assert_eq!(evidence.workflow_node_row_id, ids.node_unconsumed);
    assert_eq!(evidence.terminal_status, "COMPLETED");
    assert_eq!(evidence.terminalization_kind, "COMPLETE_LOCKED");
    assert_eq!(evidence.recovery_source, "HISTORY");
    assert_eq!(evidence.history_class.as_deref(), Some("forever"));
    assert_eq!(evidence.history_anchor, Some(evidence.terminal_at));
    assert_eq!(evidence.history_schema_version, 1);
    assert_eq!(evidence.result_digest.len(), 32);
    assert_eq!(
        evidence.result_digest,
        sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT result_digest FROM horsies_task_history WHERE task_id = $1",
        )
        .bind(ids.workflow_unconsumed_task)
        .fetch_one(&pool)
        .await
        .unwrap()
    );
    assert_eq!(evidence.quarantine_task_id, None);
    assert_eq!(evidence.phase2_generation, ids.phase2_generation);
    assert_eq!(evidence.created_at, evidence.terminal_at);
    assert_eq!(evidence.attempt_count, 0);
    assert_eq!(evidence.last_attempt_at, None);
    assert_eq!(evidence.last_failure_class, None);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM horsies_workflow_phase2_pending WHERE task_id = $1",
        )
        .bind(ids.workflow_consumed_task)
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    let mut consume_tx = pool.begin().await.unwrap();
    let disposition = consume_phase2(&mut consume_tx, ids.workflow_unconsumed_task, "COMPLETED")
        .await
        .unwrap();
    assert_eq!(
        disposition.disposition,
        Phase2DispositionKind::AppliedToNode
    );
    consume_tx.rollback().await.unwrap();

    // The stopped pre-cutover fleet is replaced by a new process after the
    // point of no return. Reconnect here so no session-local PL/pgSQL plan
    // compiled against the varchar-born row type can leak across that fleet
    // boundary.
    pool.close().await;
    let frozen_pool = database.fresh_pool().await;
    let broker = PostgresBroker::from_pool(frozen_pool.clone());
    broker.ensure_schema_initialized().await.unwrap();
    let mut coverage = frozen_pool.begin().await.unwrap();
    assert!(matches!(
        ensure_startup_coverage(coverage.as_mut(), 2, 2, &[], &StagedLoaderPublisher,)
            .await
            .unwrap(),
        StartupCoverageOutcome::Ready(_)
    ));
    coverage.commit().await.unwrap();
    let claimed = broker
        .claim("default", 1, "p9-new-worker", None)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, ids.pending);
    assert!(broker
        .set_running(
            ids.pending,
            "p9-new-worker",
            99,
            "p9-host",
            "p9-process",
            claimed[0].claimed_at,
        )
        .await
        .unwrap()
        .is_some());
    let completed = terminalize(
        broker.pool(),
        &TerminalizationCommand::CompleteTaskFused {
            task_id: ids.pending,
            fence: OwnedClaim {
                worker_id: "p9-new-worker".to_owned(),
                claimed_at: claimed[0].claimed_at,
            },
            result_json: "{\"ok\":true}".to_owned(),
            notify_channel: "p9_unused".to_owned(),
            notify_payload: ids.pending.to_string(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        completed.as_slice(),
        [TerminalizationOutcome::Applied { task_id, .. }] if *task_id == ids.pending
    ));
    let completed_location: (i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM horsies_tasks WHERE id = $1),
             (SELECT count(*) FROM horsies_task_history
              WHERE task_id = $1 AND status = 'COMPLETED')",
    )
    .bind(ids.pending)
    .fetch_one(&frozen_pool)
    .await
    .unwrap();
    assert_eq!(completed_location, (0, 1));

    sqlx::query("ALTER TABLE horsies_tasks DROP CONSTRAINT horsies_tasks_live_status_only")
        .execute(&frozen_pool)
        .await
        .unwrap();
    assert!(matches!(
        stage_validate(&frozen_pool).await.unwrap(),
        ValidationOutcome::Invalid { ref violations }
            if violations == &["the live-only status domain is absent".to_owned()]
    ));
    assert!(!{
        let mut connection = frozen_pool.acquire().await.unwrap();
        cutover_complete(&mut connection).await.unwrap()
    });
    sqlx::raw_sql(
        rendered_statement_starting(
            "ALTER TABLE horsies_tasks\n    ADD CONSTRAINT horsies_tasks_live_status_only",
        )
        .unwrap(),
    )
    .execute(&frozen_pool)
    .await
    .unwrap();
    assert!(matches!(
        stage_validate(&frozen_pool).await.unwrap(),
        ValidationOutcome::Validated { .. }
    ));
    let status = read_status(&frozen_pool).await.unwrap();
    assert!(status.attested && status.live_identity_uuid && status.attempts_identity_uuid);
    assert_eq!(status.terminal_live_rows, 0);
    assert_eq!(status.unprepared_live_rows, 0);

    drop(broker);
    frozen_pool.close().await;
    database.destroy().await;
}

#[tokio::test]
#[serial]
async fn ordered_run_driver_reports_every_stage_and_status() {
    let database = P9Database::create().await;
    let pool = database.pool.clone();
    let ids = seed_v32_population(&pool).await;
    let backup = "p9-run-driver.dump";
    let reports = run_cutover(
        &pool,
        &CutoverRunOptions {
            coefficients: coefficients(),
            heartbeat_quiet_seconds: 60.0,
            retain_rerun_input_default: false,
            preparation_batch_size: 3,
            relocation_batch_size: 3,
            backup_label: backup.to_owned(),
            operator_confirmation: confirmation_phrase(backup),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        reports
            .iter()
            .map(|report| report.stage)
            .collect::<Vec<_>>(),
        [
            "preflight",
            "drain",
            "normalize-identity",
            "install-programs",
            "prepare",
            "relocate",
            "tighten",
            "validate",
        ]
    );
    let identity_report = reports
        .iter()
        .find(|report| report.stage == "normalize-identity")
        .unwrap();
    assert_eq!(identity_report.detail, "attempt identity converted to uuid");
    assert!(!identity_report.detail.contains("Converted"));
    let status = read_status(&pool).await.unwrap();
    assert!(status.attested);
    assert_eq!(status.terminal_live_rows, 0);
    assert_eq!(status.history_rows, 4);
    assert_eq!(status.relocation_ledger_rows, Some(4));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT prepared_rerun_input_disposition FROM horsies_tasks WHERE id = $1",
        )
        .bind(ids.pending)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "DECLINED_BY_POLICY"
    );
    drop(pool);
    database.destroy().await;
}

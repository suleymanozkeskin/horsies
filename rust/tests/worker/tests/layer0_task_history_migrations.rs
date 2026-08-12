#![allow(clippy::unwrap_used)]

use horsies::{run_horsies_migrations, BrokerError, PostgresBroker};
use horsies_test_support::db;
use serial_test::serial;
use sqlx::{Executor, PgPool};

const VALIDATED_CUTOVER: &str = "task_history_v1_validated_v1";
const INCOMPLETE_CUTOVER_MESSAGE: &str = "schema migrations are current but the offline \
    task-history cutover is incomplete; run the documented cutover stages through tighten and \
    validation before starting this fleet";

const UUID_COLUMNS: &[(&str, &str)] = &[
    ("horsies_tasks", "id"),
    ("horsies_task_attempts", "task_id"),
    ("horsies_workflows", "id"),
    ("horsies_workflows", "parent_workflow_id"),
    ("horsies_workflows", "root_workflow_id"),
    ("horsies_workflow_tasks", "id"),
    ("horsies_workflow_tasks", "workflow_id"),
    ("horsies_workflow_tasks", "task_id"),
    ("horsies_workflow_tasks", "sub_workflow_id"),
    ("horsies_heartbeats", "task_id"),
];

const REQUIRED_FUNCTION_SIGNATURES: &[&str] = &[
    "horsies_key_reservation_claim(bytea,smallint,interval,smallint,bytea,uuid)",
    "horsies_key_reservation_terminalize(bytea,uuid,timestamp with time zone)",
    "horsies_key_reservation_terminalize_batch(bytea[],uuid[],timestamp with time zone)",
    "horsies_key_reservation_cleanup(integer)",
    "horsies_task_history_leaf_lock_key(text,timestamp with time zone)",
    "horsies_assert_archive_available()",
    "horsies_terminalization_miss(uuid,text[],text,timestamp with time zone)",
    "horsies_encode_task_attempts(uuid)",
    "horsies_move_task_to_history(uuid,text,text,timestamp with time zone,text,text,text)",
    "horsies_complete_locked_task(uuid,text,text)",
    "horsies_complete_task_fused(uuid,text,timestamp with time zone,text,text,text)",
    "horsies_fail_locked_task(uuid,text,text,text,text)",
    "horsies_fail_stale_task(uuid,integer,integer,text,text,text)",
    "horsies_expire_owned_claim(uuid,text,text,text)",
    "horsies_expire_pending_tasks(integer,text,text)",
    "horsies_cancel_locked_task(uuid,text[])",
    "horsies_cancel_owned_orphan(uuid,text,timestamp with time zone)",
    "horsies_cancel_orphaned_tasks(integer)",
    "horsies_abandon_owned_node(uuid,text,timestamp with time zone)",
    "horsies_cancel_owned_node(uuid,text,timestamp with time zone,boolean)",
    "horsies_abandon_owned_nodes(uuid[],timestamp with time zone[],text)",
    "horsies_cancel_owned_nodes(uuid[],timestamp with time zone[],text)",
    "horsies_abandon_nodes_of_paused_workflows(uuid[])",
    "horsies_cancel_nodes_of_cancelled_workflow(uuid[])",
    "horsies_phase2_consume(uuid,text)",
    "horsies_phase2_quarantine_one(uuid,text)",
    "horsies_archive_replacement_note_mutation()",
];

async fn relation_kind(pool: &PgPool, relation: &str) -> String {
    sqlx::query_scalar("SELECT relkind::text FROM pg_class WHERE oid = to_regclass($1)")
        .bind(relation)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn cutover_attested(pool: &PgPool) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM horsies_cutover_state WHERE cutover_name = $1)",
    )
    .bind(VALIDATED_CUTOVER)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn apply_through_v26(pool: &PgPool) {
    let migrator = sqlx::migrate!("../../horsies/migrations");
    sqlx::query(
        "CREATE TABLE horsies_migrations (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
            success BOOLEAN NOT NULL,
            checksum BYTEA NOT NULL,
            execution_time BIGINT NOT NULL
        )",
    )
    .execute(pool)
    .await
    .unwrap();

    for migration in migrator.iter().filter(|migration| migration.version <= 32) {
        pool.execute(migration.sql.as_ref()).await.unwrap();
        sqlx::query(
            "INSERT INTO horsies_migrations
                 (version, description, success, checksum, execution_time)
             VALUES ($1, $2, TRUE, $3, 0)",
        )
        .bind(migration.version)
        .bind(migration.description.as_ref())
        .bind(migration.checksum.as_ref())
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn seed_every_fork_relation(pool: &PgPool) {
    let task_id = "018f0000-0000-7000-8000-000000000001";
    let workflow_id = "018f0000-0000-7000-8000-000000000002";
    let node_id = "018f0000-0000-7000-8000-000000000003";

    sqlx::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, status, sent_at, enqueued_at,
             enqueue_sha, is_workflow_task
         ) VALUES ($1, 'seeded', 'default', 'PENDING', NOW(), NOW(),
                   'seeded-v26', TRUE)",
    )
    .bind(task_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO horsies_workflows (id, name, status, on_error, sent_at)
         VALUES ($1, 'seeded', 'PENDING', 'fail', NOW())",
    )
    .bind(workflow_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO horsies_workflow_tasks (
             id, workflow_id, task_index, task_name, task_id
         ) VALUES ($1, $2, 0, 'seeded', $3)",
    )
    .bind(node_id)
    .bind(workflow_id)
    .bind(task_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO horsies_task_attempts (
             task_id, attempt, outcome, started_at, finished_at
         ) VALUES ($1, 1, 'COMPLETED', NOW(), NOW())",
    )
    .bind(task_id)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
#[serial]
async fn fresh_database_is_born_at_validated_v35_posture() {
    let db_url = db::create_empty_database().await;
    let pool = PgPool::connect(&db_url).await.unwrap();

    run_horsies_migrations(&pool).await.unwrap();

    for &(relation, column) in UUID_COLUMNS {
        let is_uuid: bool = sqlx::query_scalar(
            "SELECT atttypid = 'uuid'::regtype
             FROM pg_attribute
             WHERE attrelid = $1::regclass AND attname = $2",
        )
        .bind(relation)
        .bind(column)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(is_uuid, "{relation}.{column} must be uuid");
    }

    assert!(cutover_attested(&pool).await);
    assert_eq!(relation_kind(&pool, "horsies_heartbeats").await, "p");
    assert_eq!(
        relation_kind(&pool, "horsies_task_history_forever").await,
        "p"
    );

    let current_forever_leaves: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM horsies_task_history_leaf_catalog
         WHERE parent_name = 'horsies_task_history_forever'
           AND class_key = 'forever'
           AND lower_anchor = date_trunc('day', statement_timestamp(), 'UTC')
           AND upper_anchor = date_trunc('day', statement_timestamp(), 'UTC')
                              + interval '1 day'
           AND min_birth_verified
           AND detached_at IS NULL
           AND dropped_at IS NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(current_forever_leaves, 1);

    let live_only_check: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM pg_constraint
             WHERE conrelid = 'horsies_tasks'::regclass
               AND conname = 'horsies_tasks_live_status_only'
         )",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(live_only_check);

    let superseded_status_checks: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM pg_constraint AS con
         WHERE con.conrelid = 'horsies_tasks'::regclass
           AND con.contype = 'c'
           AND (
               SELECT att.attnum
               FROM pg_attribute AS att
               WHERE att.attrelid = con.conrelid
                 AND att.attname = 'status'
           ) = ANY(con.conkey)
           AND con.conname <> 'horsies_tasks_live_status_only'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(superseded_status_checks, 0);

    let nullable_required_columns: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM pg_attribute
         WHERE attrelid = 'horsies_tasks'::regclass
           AND attname = ANY(ARRAY[
               'command_fingerprint_version', 'command_fingerprint',
               'retention_class_key', 'retain_rerun_input',
               'prepared_rerun_input_disposition'
           ])
           AND NOT attnotnull",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(nullable_required_columns, 0);

    for signature in REQUIRED_FUNCTION_SIGNATURES {
        let present: bool = sqlx::query_scalar("SELECT to_regprocedure($1) IS NOT NULL")
            .bind(signature)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(present, "missing function signature {signature}");
    }
    let old_in_place_signature: bool = sqlx::query_scalar(
        "SELECT to_regprocedure(
             'horsies_complete_locked_task(character varying,text,text)'
         ) IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!old_in_place_signature);

    let broker = PostgresBroker::from_pool(pool.clone());
    broker.ensure_schema_initialized().await.unwrap();

    pool.close().await;
    db::drop_database(&db_url).await;
}

#[tokio::test]
#[serial]
async fn populated_v26_database_stays_transitional_and_refuses_fleet_start() {
    let db_url = db::create_empty_database().await;
    let pool = PgPool::connect(&db_url).await.unwrap();

    apply_through_v26(&pool).await;
    seed_every_fork_relation(&pool).await;
    run_horsies_migrations(&pool).await.unwrap();

    let task_identity_is_varchar: bool = sqlx::query_scalar(
        "SELECT atttypid = 'character varying'::regtype
         FROM pg_attribute
         WHERE attrelid = 'horsies_tasks'::regclass AND attname = 'id'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(task_identity_is_varchar);

    let transitional_columns: (i64, i64) = sqlx::query_as(
        "SELECT count(*), count(*) FILTER (WHERE NOT attnotnull)
         FROM pg_attribute
         WHERE attrelid = 'horsies_tasks'::regclass
           AND attname = ANY(ARRAY[
               'command_fingerprint_version', 'command_fingerprint',
               'retention_class_key', 'input_digest', 'rerun_of_task_id',
               'rerun_root_task_id', 'idempotency_key_digest',
               'retain_rerun_input', 'prepared_rerun_input_disposition',
               'prepared_rerun_input_version', 'prepared_rerun_input_codec',
               'prepared_rerun_input_content_type', 'prepared_rerun_input_digest',
               'prepared_rerun_input_inline', 'prepared_rerun_input_reference'
           ])",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(transitional_columns, (15, 15));
    assert!(!cutover_attested(&pool).await);
    assert_eq!(relation_kind(&pool, "horsies_heartbeats").await, "r");

    let old_in_place_signature: bool = sqlx::query_scalar(
        "SELECT to_regprocedure(
             'horsies_complete_locked_task(character varying,text,text)'
         ) IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(old_in_place_signature);
    let move_signature: bool = sqlx::query_scalar(
        "SELECT to_regprocedure(
             'horsies_move_task_to_history(uuid,text,text,timestamp with time zone,text,text,text)'
         ) IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!move_signature);

    let broker = PostgresBroker::from_pool(pool.clone());
    let error = broker.ensure_schema_initialized().await.unwrap_err();
    assert!(matches!(error, BrokerError::IncompleteTaskHistoryCutover));
    assert_eq!(error.to_string(), INCOMPLETE_CUTOVER_MESSAGE);

    pool.close().await;
    db::drop_database(&db_url).await;
}

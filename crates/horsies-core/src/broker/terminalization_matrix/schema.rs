use std::panic::AssertUnwindSafe;

use futures::FutureExt;

use super::*;
use crate::broker::migrations::run_horsies_migrations_through;
use crate::core::history::cutover::program::{install_programs, ProgramInstallation};
use crate::core::history::cutover::runner::stage_install_programs;

const SCHEMA: &str = "horsies_schema_test";
const FUNCTIONS: [&str; 7] = [
    "horsies_move_task_to_history",
    "horsies_abandon_nodes_of_paused_workflows",
    "horsies_abandon_owned_nodes",
    "horsies_cancel_nodes_of_cancelled_workflow",
    "horsies_cancel_orphaned_tasks",
    "horsies_cancel_owned_nodes",
    "horsies_expire_pending_tasks",
];

struct SchemaDatabase {
    database: IsolatedTerminalizationTestDatabase,
    role: String,
}

impl SchemaDatabase {
    async fn create() -> Self {
        let mut database = IsolatedTerminalizationTestDatabase::create_empty().await;
        let role = format!("horsies_schema_role_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE ROLE \"{role}\" NOLOGIN"))
            .execute(&mut database.admin)
            .await
            .unwrap();
        sqlx::raw_sql(&format!(
            "REVOKE CREATE ON SCHEMA public FROM PUBLIC;
             CREATE SCHEMA {SCHEMA} AUTHORIZATION \"{role}\";"
        ))
        .execute(&database.pool)
        .await
        .unwrap();
        let hook_role = role.clone();
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .after_connect(move |connection, _| {
                let role_statement = format!("SET ROLE \"{hook_role}\"");
                Box::pin(async move {
                    sqlx::query(&role_statement)
                        .execute(&mut *connection)
                        .await?;
                    sqlx::query("SET search_path = horsies_schema_test, public")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect_with((*database.pool.connect_options()).clone())
            .await
            .unwrap();
        database.pool.close().await;
        database.pool = pool;
        Self { database, role }
    }

    async fn drop(self) {
        let role = self.role;
        let options = (*self.database.pool.connect_options()).clone();
        self.database.drop().await;
        let mut admin = PgConnection::connect_with(&options.database("postgres"))
            .await
            .unwrap();
        sqlx::query(&format!("DROP ROLE \"{role}\""))
            .execute(&mut admin)
            .await
            .unwrap();
    }
}

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct FunctionDefinition {
    oid: i64,
    name: String,
    arguments: String,
    result: String,
    definition: String,
}

async fn definitions(pool: &PgPool) -> Vec<FunctionDefinition> {
    let rows = sqlx::query_as(
        "SELECT p.oid::bigint AS oid, p.proname::text AS name,
                pg_get_function_identity_arguments(p.oid) AS arguments,
                pg_get_function_result(p.oid) AS result,
                pg_get_functiondef(p.oid) AS definition
         FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = $1 AND p.proname = ANY($2)
         ORDER BY p.proname",
    )
    .bind(SCHEMA)
    .bind(FUNCTIONS.as_slice())
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), FUNCTIONS.len());
    rows
}

async fn assert_schema_boundary(pool: &PgPool, role: &str) {
    let (user, schema, can_create_public): (String, String, bool) = sqlx::query_as(
        "SELECT current_user::text, current_schema()::text,
                has_schema_privilege(current_user, 'public', 'CREATE')",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(user, role);
    assert_eq!(schema, SCHEMA);
    assert!(!can_create_public);
    let public_functions: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = 'public' AND p.proname = ANY($1)",
    )
    .bind(FUNCTIONS.as_slice())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(public_functions, 0);
    for function in definitions(pool).await {
        let visible: bool = sqlx::query_scalar("SELECT pg_function_is_visible($1::bigint::oid)")
            .bind(function.oid)
            .fetch_one(pool)
            .await
            .unwrap();
        assert!(
            visible,
            "{} must resolve in the connection schema",
            function.name
        );
    }
}

#[derive(Clone, Copy)]
enum Installation {
    Migration,
    Cutover,
}

async fn check_schema_installation(installation: Installation) {
    let fixture = SchemaDatabase::create().await;
    let pool = &fixture.database.pool;
    let result = AssertUnwindSafe(async {
        run_horsies_migrations_through(pool, 49).await.unwrap();
        assert_schema_boundary(pool, &fixture.role).await;
        let before = definitions(pool).await;
        match installation {
            Installation::Migration => {
                run_horsies_migrations(pool).await.unwrap();
                let after = definitions(pool).await;
                for (old, new) in before.iter().zip(&after) {
                    assert_eq!(old.oid, new.oid);
                    assert_eq!(old.name, new.name);
                    assert_eq!(old.arguments, new.arguments);
                    assert_eq!(old.result, new.result);
                    assert_ne!(old.definition, new.definition);
                }
                run_horsies_migrations(pool).await.unwrap();
                assert_eq!(after, definitions(pool).await);
            }
            Installation::Cutover => {
                let mut transaction = pool.begin().await.unwrap();
                assert!(matches!(
                    install_programs(transaction.as_mut()).await.unwrap(),
                    ProgramInstallation::Installed { .. }
                ));
                transaction.rollback().await.unwrap();
                assert_eq!(before, definitions(pool).await);
                assert!(matches!(
                    stage_install_programs(pool).await.unwrap(),
                    ProgramInstallation::Installed { .. }
                ));
            }
        }
        assert_schema_boundary(pool, &fixture.role).await;
        let mut transaction = pool.begin().await.unwrap();
        let coverage =
            ensure_partition_coverage(&mut transaction, 2, 2, &[], &StagedLoaderPublisher)
                .await
                .unwrap();
        assert!(
            matches!(coverage, CoverageOutcome::Ensured(_)),
            "{coverage:?}"
        );
        transaction.commit().await.unwrap();
        assert_batch_snapshot_encoding(pool).await;
        assert_terminal_result_digests(pool).await;
        assert_schema_boundary(pool, &fixture.role).await;
    })
    .catch_unwind()
    .await;
    fixture.drop().await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[serial]
async fn terminal_migration_preserves_restricted_schema() {
    check_schema_installation(Installation::Migration).await;
}

#[tokio::test]
#[serial]
async fn terminal_cutover_preserves_restricted_schema() {
    check_schema_installation(Installation::Cutover).await;
}

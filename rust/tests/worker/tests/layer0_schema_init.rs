#![allow(clippy::unwrap_used)]

use std::time::Duration;

use horsies::{task, AppConfig, Horsies, TaskError, WorkflowSpecBuilder};
use horsies_test_support::{db, e2e::worker::start_worker};
use serial_test::serial;
use sqlx::PgPool;

#[task("schema_auto_send")]
async fn schema_auto_send(_: ()) -> Result<String, TaskError> {
    Ok("ok".to_owned())
}

#[task("schema_auto_wf_step")]
async fn schema_auto_wf_step(_: ()) -> Result<String, TaskError> {
    Ok("workflow-ok".to_owned())
}

async fn table_exists(pool: &PgPool, table_name: &str) -> bool {
    let regclass: Option<String> =
        sqlx::query_scalar("SELECT to_regclass(format('public.%s', $1))::text")
            .bind(table_name)
            .fetch_one(pool)
            .await
            .unwrap();
    regclass.is_some()
}

#[tokio::test]
#[serial]
async fn task_send_auto_initializes_empty_database() {
    let db_url = db::create_empty_database().await;

    let mut app = Horsies::new(AppConfig::for_database_url(&db_url)).unwrap();
    schema_auto_send::register(&mut app).unwrap();

    let handle = schema_auto_send::send(()).await.unwrap();
    let pool = PgPool::connect(&db_url).await.unwrap();

    assert!(table_exists(&pool, "horsies_tasks").await);

    let task_name: String = sqlx::query_scalar("SELECT task_name FROM horsies_tasks WHERE id = $1")
        .bind(handle.task_id())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(task_name, "schema_auto_send");

    pool.close().await;
    db::drop_database(&db_url).await;
}

#[tokio::test]
#[serial]
async fn workflow_start_auto_initializes_empty_database() {
    let db_url = db::create_empty_database().await;

    let mut app = Horsies::new(AppConfig::for_database_url(&db_url)).unwrap();
    schema_auto_wf_step::register(&mut app).unwrap();

    let mut builder = WorkflowSpecBuilder::new("schema_auto_workflow");
    let step = builder.task(schema_auto_wf_step::node().unwrap().node_id("step"));
    builder.output(step);
    let spec = builder.build().unwrap();

    let _handle = app.start::<String>(spec).await.unwrap();
    let pool = PgPool::connect(&db_url).await.unwrap();

    assert!(table_exists(&pool, "horsies_workflows").await);
    assert!(table_exists(&pool, "horsies_workflow_tasks").await);

    let workflow_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM horsies_workflows")
        .fetch_one(&pool)
        .await
        .unwrap();
    let workflow_task_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM horsies_workflow_tasks")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(workflow_count, 1);
    assert_eq!(workflow_task_count, 1);

    pool.close().await;
    db::drop_database(&db_url).await;
}

#[tokio::test]
#[serial]
async fn worker_start_auto_initializes_empty_database() {
    let db_url = db::create_empty_database().await;

    {
        let mut worker = start_worker(&db_url, &[], "worker started", Duration::from_secs(20));
        assert!(worker.is_running());
    }

    let pool = PgPool::connect(&db_url).await.unwrap();
    assert!(table_exists(&pool, "horsies_tasks").await);

    let completed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM horsies_task_history
         WHERE task_name = 'e2e_healthcheck' AND status = 'COMPLETED'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(completed >= 1);

    pool.close().await;
    db::drop_database(&db_url).await;
}

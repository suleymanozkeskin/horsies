//! PgBouncer contract-test helpers.
//!
//! The local fixture routes any database name through each PgBouncer pool-mode
//! endpoint, so tests can create an isolated database per case and then reuse
//! the same database name across direct, transaction, session, statement, and
//! prepared-statement endpoints.

use sqlx::{Connection, PgConnection, PgPool};
use url::Url;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PgbouncerUrls {
    pub db_name: String,
    pub direct: String,
    pub transaction: String,
    pub prepared: String,
    pub session: String,
    pub statement: String,
}

pub fn enabled() -> bool {
    std::env::var("HORSIES_PGBOUNCER_TEST").as_deref() == Ok("1")
}

pub fn template_urls() -> (String, String, String, String, String) {
    (
        std::env::var("HORSIES_TEST_DATABASE_URL_DIRECT")
            .unwrap_or_else(|_| default_url(15432, "horsies")),
        std::env::var("HORSIES_TEST_DATABASE_URL_TRANSACTION")
            .unwrap_or_else(|_| default_url(16432, "horsies")),
        std::env::var("HORSIES_TEST_DATABASE_URL_PREPARED")
            .unwrap_or_else(|_| default_url(16435, "horsies")),
        std::env::var("HORSIES_TEST_DATABASE_URL_SESSION")
            .unwrap_or_else(|_| default_url(16433, "horsies")),
        std::env::var("HORSIES_TEST_DATABASE_URL_STATEMENT")
            .unwrap_or_else(|_| default_url(16434, "horsies")),
    )
}

pub async fn create_isolated_database(prefix: &str) -> PgbouncerUrls {
    let (
        direct_template,
        transaction_template,
        prepared_template,
        session_template,
        statement_template,
    ) = template_urls();
    let db_name = format!("{}_{}", prefix, Uuid::new_v4().simple());
    let admin_url = replace_database(&direct_template, "postgres");

    let mut conn = PgConnection::connect(&admin_url)
        .await
        .expect("failed to connect to PgBouncer fixture admin database");
    sqlx::query(&format!("CREATE DATABASE \"{}\"", db_name))
        .execute(&mut conn)
        .await
        .expect("failed to create PgBouncer fixture database");

    PgbouncerUrls {
        db_name: db_name.clone(),
        direct: replace_database(&direct_template, &db_name),
        transaction: replace_database(&transaction_template, &db_name),
        prepared: replace_database(&prepared_template, &db_name),
        session: replace_database(&session_template, &db_name),
        statement: replace_database(&statement_template, &db_name),
    }
}

pub async fn drop_isolated_database(urls: &PgbouncerUrls) {
    let admin_url = replace_database(&urls.direct, "postgres");
    let mut conn = PgConnection::connect(&admin_url)
        .await
        .expect("failed to connect to PgBouncer fixture admin database");
    sqlx::query(
        "SELECT pg_terminate_backend(pid) \
         FROM pg_stat_activity \
         WHERE datname = $1 AND pid <> pg_backend_pid()",
    )
    .bind(&urls.db_name)
    .execute(&mut conn)
    .await
    .expect("failed to terminate PgBouncer fixture database connections");
    sqlx::query(&format!("DROP DATABASE IF EXISTS \"{}\"", urls.db_name))
        .execute(&mut conn)
        .await
        .expect("failed to drop PgBouncer fixture database");
}

pub async fn drop_horsies_schema(pool: &PgPool) {
    sqlx::query(
        "DROP TABLE IF EXISTS \
         horsies_workflow_tasks, \
         horsies_workflows, \
         horsies_tasks, \
         horsies_task_attempts, \
         horsies_heartbeats, \
         horsies_worker_states, \
         horsies_schedule_state, \
         horsies_migrations \
         CASCADE",
    )
    .execute(pool)
    .await
    .expect("failed to drop horsies tables");

    for function_name in [
        "horsies_notify_task_changes",
        "horsies_notify_workflow_changes",
        "notify_task_status_change",
        "notify_workflow_status_change",
        "notify_worker_state_insert",
        "notify_task_done",
    ] {
        sqlx::query(&format!(
            "DROP FUNCTION IF EXISTS {}() CASCADE",
            function_name
        ))
        .execute(pool)
        .await
        .expect("failed to drop horsies function");
    }
}

pub fn replace_database(url: &str, database: &str) -> String {
    let mut parsed = Url::parse(url).expect("invalid PgBouncer test database URL");
    parsed.set_path(&format!("/{}", database));
    parsed.to_string()
}

pub fn managed_provider_split_urls_from_env() -> Option<(String, String)> {
    if let (Ok(direct), Ok(transaction)) = (
        std::env::var("HORSIES_MANAGED_DATABASE_URL_DIRECT"),
        std::env::var("HORSIES_MANAGED_DATABASE_URL_TRANSACTION"),
    ) {
        return Some((direct, transaction));
    }

    let host = std::env::var("DATABASE_HOST").ok()?;
    let direct_port = std::env::var("DATABASE_PORT").ok()?;
    let transaction_port = std::env::var("DATABASE_PG_BOUNCER_PORT").ok()?;
    let username = std::env::var("DATABASE_USERNAME").ok()?;
    let password = std::env::var("DATABASE_PASSWORD").ok()?;
    let database = std::env::var("DATABASE").ok()?;
    let sslmode = std::env::var("DATABASE_SSLMODE").unwrap_or_else(|_| "require".to_owned());

    let direct = build_postgres_url(
        &host,
        &direct_port,
        &username,
        &password,
        &database,
        &sslmode,
    );
    let transaction = build_postgres_url(
        &host,
        &transaction_port,
        &username,
        &password,
        &database,
        &sslmode,
    );
    Some((direct, transaction))
}

fn default_url(port: u16, database: &str) -> String {
    let password = std::env::var("DB_PASSWORD").unwrap_or_else(|_| "testpassword".to_owned());
    format!(
        "postgresql://postgres:{}@localhost:{}/{}",
        password, port, database
    )
}

fn build_postgres_url(
    host: &str,
    port: &str,
    username: &str,
    password: &str,
    database: &str,
    sslmode: &str,
) -> String {
    let mut url = Url::parse("postgresql://localhost").expect("static URL should parse");
    url.set_host(Some(host)).expect("invalid database host");
    url.set_port(Some(port.parse::<u16>().expect("invalid database port")))
        .expect("invalid database port");
    url.set_username(username)
        .expect("invalid database username");
    url.set_password(Some(password))
        .expect("invalid database password");
    url.set_path(&format!("/{}", database));
    url.query_pairs_mut().append_pair("sslmode", sslmode);
    url.to_string()
}

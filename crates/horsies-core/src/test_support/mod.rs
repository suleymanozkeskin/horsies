//! Test support of the library's tests: the test databases
//! ([`database`]), the core handle on one of them, the terminal writes of
//! seeds ([`terminal_writes`]), the capture of the `tracing` events of a
//! test ([`log_capture`]), and the child spec source of the workflow tests
//! ([`child_specs`]).

pub(crate) mod child_specs;
mod database;
mod log_capture;
pub(crate) mod terminal_writes;

pub(crate) use database::{
    connect, database_name_prefix, database_of_this_test, is_generated_name, is_valid_prefix,
    server_url, with_database, TestDatabase, DATABASE_PREFIX_VARIABLE, DEFAULT_DATABASE_PREFIX,
};
pub(crate) use log_capture::{CapturedEvent, LogCapture};

use std::sync::{Arc, Mutex, PoisonError};

use tokio_postgres::AsyncMessage;

use crate::broker::client_family::ClientFamily;
use crate::broker::core_rows::StatementLog;
use crate::broker::migrations::{install_schema_on, InstallLockPlan};
use crate::broker::pools::{CoreHandle, PoolProfile};
use crate::core::PostgresConfig;
use crate::db::{
    open_connection, parse_connection_url, ConnectPlan, ConnectionEnvironment,
    DEFAULT_SOCKET_SETTINGS,
};

/// A core handle (producer pools) on the database of `url`. The caller
/// closes it ([`CoreHandle::close`]) before the database is dropped, or
/// leaves it to the forced drop at the end of the test.
pub(crate) fn core_handle(url: &str) -> CoreHandle {
    CoreHandle::open_with_environment(
        &PostgresConfig::from_url(url),
        PoolProfile::Producer,
        ConnectionEnvironment::capture().expect("read the connection environment of the test"),
    )
    .expect("the statement layer accepts the test database URL")
}

/// Opens one direct connection to `url` whose server notices are collected
/// in arrival order (for example the plans that `auto_explain` sends with
/// `log_level = notice`). Its driver runs as a task of the current runtime
/// and ends with it. The driver hands a notice to the collector before it
/// reads the messages after it, so the notices of a statement are in the
/// list when the call of that statement returns.
///
/// # Panics
///
/// When the URL is refused or the server cannot be reached: the test cannot
/// run.
pub(crate) async fn connect_collecting_notices(
    url: &str,
) -> (tokio_postgres::Client, Arc<Mutex<Vec<String>>>) {
    let environment =
        ConnectionEnvironment::capture().expect("read the connection environment of the test");
    let settings = parse_connection_url(url, &environment)
        .unwrap_or_else(|error| panic!("the test database URL is refused: {error}"));
    let plan = ConnectPlan::load(&settings, &environment, &DEFAULT_SOCKET_SETTINGS)
        .unwrap_or_else(|error| panic!("the test connection plan does not load: {error}"));
    let (client, mut connection) = open_connection(&plan)
        .await
        .unwrap_or_else(|error| panic!("connecting to the test server failed: {error}"));
    let notices = Arc::new(Mutex::new(Vec::new()));
    let collected = Arc::clone(&notices);
    drop(tokio::spawn(async move {
        while let Some(Ok(message)) =
            std::future::poll_fn(|context| connection.poll_message(context)).await
        {
            match message {
                AsyncMessage::Notice(notice) => collected
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(notice.message().to_owned()),
                _ => {}
            }
        }
    }));
    (client, notices)
}

/// A broker on the database of `pool` whose sqlx adapter pools are closed:
/// every call that borrows them fails. A path that runs to its end on this
/// broker runs on the core pools alone. The core pools connect at their first
/// checkout.
pub(crate) async fn broker_with_closed_sqlx_pools(
    pool: &sqlx::PgPool,
) -> crate::broker::PostgresBroker {
    let adapter =
        sqlx::postgres::PgPoolOptions::new().connect_lazy_with((*pool.connect_options()).clone());
    adapter.close().await;
    crate::broker::PostgresBroker::from_pool(adapter, ClientFamily::Rust)
}

/// Installs the schema into the template of this process: the
/// statement-layer install for the Rust client, then the handle is closed so
/// that the template has no session left.
async fn install_template(url: &str) {
    let handle = core_handle(url);
    let open = handle
        .pools()
        .expect("the statement layer accepts the template URL");
    install_schema_on(
        open.pools().session_work(),
        ClientFamily::Rust,
        InstallLockPlan::fleet(),
        StatementLog::Off,
    )
    .await
    .expect("install the schema into the test template");
    drop(open);
    let _closed = handle.close().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_prefix_rule_is_python_s() {
        for valid in ["horsies_test_", "pyexit_", "a_", "rs_run_2_"] {
            assert!(is_valid_prefix(valid), "{valid}");
        }
        for invalid in [
            "",
            "_",
            "Horsies_",
            "horsies",
            "9run_",
            "run-1_",
            "abcdefghijklmnopqrstuvwxy_",
        ] {
            assert!(!is_valid_prefix(invalid), "{invalid}");
        }
        assert!(is_valid_prefix("abcdefghijklmnopqrstuvw_"));
        assert_eq!(DEFAULT_DATABASE_PREFIX, "horsies_test_");
        assert_eq!(DATABASE_PREFIX_VARIABLE, "HORSIES_TEST_DATABASE_PREFIX");
    }

    #[test]
    fn only_generated_names_of_the_prefix_are_swept() {
        let hex = "0123456789abcdef0123456789abcdef";
        assert!(is_generated_name(
            &format!("horsies_test_rs_{hex}"),
            "horsies_test_"
        ));
        assert!(is_generated_name(
            &format!("horsies_test_rstpl_{hex}"),
            "horsies_test_"
        ));
        for other in [
            format!("horsies_test_{hex}"),
            format!("horsies_test_iso_{hex}"),
            format!("pyexit_rs_{hex}"),
            format!("horsies_test_rs_{}", &hex[..31]),
            format!("horsies_test_rs_{}", hex.to_uppercase()),
            "horsies-core".to_owned(),
        ] {
            assert!(!is_generated_name(&other, "horsies_test_"), "{other}");
        }
    }

    #[test]
    fn a_url_changes_only_its_database() {
        assert_eq!(
            with_database("postgresql://u:p@host:5432/base", "other"),
            "postgresql://u:p@host:5432/other"
        );
        assert_eq!(
            with_database("postgresql://u:p@host/base?sslmode=disable", "other"),
            "postgresql://u:p@host/other?sslmode=disable"
        );
        assert_eq!(
            with_database("postgresql://u:p@host", "other"),
            "postgresql://u:p@host/other"
        );
    }

    /// A copied database holds the installed schema and the Rust family; the
    /// shutdown of its test runtime drops it.
    #[test]
    fn a_copied_database_is_installed_and_dropped_with_its_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let name = runtime.block_on(async {
            let database = TestDatabase::create().await;
            let row = database
                .client()
                .query_one(
                    "SELECT (SELECT family FROM horsies_client_family), \
                            (SELECT max(version) FROM horsies_migrations)",
                    &[],
                )
                .await
                .expect("read the installed schema");
            let family: String = row.get(0);
            let version: i64 = row.get(1);
            assert_eq!(family, "RUST");
            assert_eq!(version, crate::broker::expected_schema_version());
            assert!(database.name().starts_with(&database_name_prefix()));
            database.name().to_owned()
        });
        drop(runtime);

        let check = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("check runtime");
        let exists = check.block_on(async {
            let admin = connect(&with_database(&server_url(), "postgres")).await;
            let row = admin
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)",
                    &[&name],
                )
                .await
                .expect("look the database up");
            row.get::<_, bool>(0)
        });
        assert!(!exists, "{name} is left after its runtime shut down");
    }

    /// Every call in one test runtime gets the same database; another
    /// runtime gets another one.
    #[test]
    fn each_test_runtime_has_one_database() {
        let first_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("first runtime");
        let (first, again) = first_runtime.block_on(async {
            let first = database_of_this_test().await.url().to_owned();
            let again = tokio::spawn(async { database_of_this_test().await.url().to_owned() })
                .await
                .expect("spawned lookup");
            (first, again)
        });
        drop(first_runtime);
        assert_eq!(first, again);

        let second_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("second runtime");
        let second =
            second_runtime.block_on(async { database_of_this_test().await.url().to_owned() });
        drop(second_runtime);
        assert_ne!(first, second);
    }

    /// An empty database has no horsies table; the explicit drop removes it
    /// while another session is still open on it.
    #[tokio::test]
    async fn an_empty_database_has_no_schema_and_drops_on_request() {
        let database = TestDatabase::create_empty().await;
        let row = database
            .client()
            .query_one("SELECT to_regclass('horsies_migrations') IS NULL", &[])
            .await
            .expect("read the catalog");
        assert!(row.get::<_, bool>(0));
        let name = database.name().to_owned();
        let other_session = database.connect().await;
        database.drop().await;
        drop(other_session);

        let admin = connect(&with_database(&server_url(), "postgres")).await;
        let row = admin
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)",
                &[&name],
            )
            .await
            .expect("look the database up");
        assert!(!row.get::<_, bool>(0));
    }
}

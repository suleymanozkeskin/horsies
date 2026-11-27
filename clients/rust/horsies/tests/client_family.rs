//! The client family that the Rust client records: the first operation of an
//! app on an empty database installs the schema and writes `RUST` as the
//! family row.
//!
//! Each test runs on an empty database of its own
//! (`core/src/test_support/database.rs`).

#![cfg(feature = "tokio-postgres")]

use horsies::{AppConfig, Horsies, ReadError, TaskInfoParts};
use horsies_core::PostgresBroker;
use uuid::Uuid;

/// The test databases of the core. This binary creates empty databases
/// only.
#[allow(dead_code)]
#[path = "../../../../core/src/test_support/database.rs"]
mod test_database;

use test_database::TestDatabase;

/// Installs the schema into a test template through the public API, with
/// the Rust client family.
async fn install_template(url: &str) {
    let broker = PostgresBroker::connect(url, horsies_core::ClientFamily::Rust)
        .await
        .expect("connect the template broker");
    broker
        .migrate()
        .await
        .expect("install the schema into the test template");
    broker.close().await;
}

/// The first read of an app on an empty database installs the schema with
/// the family row `RUST`; the read itself finds no task.
#[tokio::test]
async fn the_first_operation_of_an_app_records_the_rust_family() {
    let database = TestDatabase::create_empty().await;
    let app = Horsies::new(AppConfig::from_database_url(database.url())).expect("the app");

    match app
        .get_task_info(Uuid::new_v4(), TaskInfoParts::default())
        .await
    {
        Err(ReadError::TaskNotFound { .. }) => {}
        other => panic!(
            "a task id that no row holds is not found: {:?}",
            other.map(|_| ())
        ),
    }
    let recorded: String = database
        .client()
        .query_one("SELECT family FROM horsies_client_family", &[])
        .await
        .expect("the install wrote one family row")
        .get(0);
    assert_eq!(recorded, "RUST");
    database.drop().await;
}

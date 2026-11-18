//! A migrated database of its own for one monitoring read test.
//!
//! The worker list, the fleet check and the class report read every row of
//! their tables, so their tests do not share a database with other tests
//! (Python empties the tables instead: `tests/integration/test_class_coverage.py`,
//! `empty_fleet`).

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use crate::broker::PostgresBroker;
use crate::test_support::TestDatabase;

/// One copy of the installed test template (`crate::test_support`) with a
/// sqlx pool for the broker reads that still take one.
/// [`MonitoringTestDatabase::drop`] removes it; a test that panics before the
/// drop leaves it to the drop at the shutdown of its runtime.
pub(crate) struct MonitoringTestDatabase {
    pub(crate) pool: PgPool,
    database: TestDatabase,
}

impl MonitoringTestDatabase {
    pub(crate) async fn create() -> Self {
        let database = TestDatabase::create().await;
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(database.url())
            .await
            .expect("connect to the monitoring test database");
        Self { pool, database }
    }

    /// A broker on this database.
    pub(crate) fn broker(&self) -> PostgresBroker {
        PostgresBroker::from_pool(self.pool.clone(), crate::ClientFamily::Rust)
    }

    pub(crate) async fn drop(self) {
        self.pool.close().await;
        self.database.drop().await;
    }
}

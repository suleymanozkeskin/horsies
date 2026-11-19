//! A migrated database of its own for one monitoring read test.
//!
//! The worker list, the fleet check and the class report read every row of
//! their tables, so their tests do not share a database with other tests
//! (Python empties the tables instead: `clients/python/tests/integration/test_class_coverage.py`,
//! `empty_fleet`).

use crate::broker::PostgresBroker;
use crate::test_support::sql::{TestPool, TestPoolOptions};
use crate::test_support::TestDatabase;

/// One copy of the installed test template (`crate::test_support`) with a
/// test-client pool for the seeds and the probes of the tests.
/// [`MonitoringTestDatabase::drop`] removes it; a test that panics before the
/// drop leaves it to the drop at the shutdown of its runtime.
pub(crate) struct MonitoringTestDatabase {
    pub(crate) pool: TestPool,
    database: TestDatabase,
}

impl MonitoringTestDatabase {
    pub(crate) async fn create() -> Self {
        let database = TestDatabase::create().await;
        let pool = TestPoolOptions::new()
            .max_connections(4)
            .connect(database.url())
            .await
            .expect("connect to the monitoring test database");
        Self { pool, database }
    }

    /// A broker on this database.
    pub(crate) fn broker(&self) -> PostgresBroker {
        PostgresBroker::from_test_url(self.pool.url(), crate::ClientFamily::Rust)
    }

    pub(crate) async fn drop(self) {
        self.pool.close().await;
        self.database.drop().await;
    }
}

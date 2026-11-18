//! The sqlx side of the test fixtures that take either pool, for the test
//! modules that still hold a sqlx pool. This module goes when the last of
//! them moves to [`super::sql`].

use super::sql::TestDatabaseUrl;
use crate::broker::lifecycle_tests::support::UpstreamAddress;

impl TestDatabaseUrl for sqlx::PgPool {
    fn test_url(&self) -> String {
        crate::broker::pools::test_url_of(&self.connect_options())
    }
}

impl UpstreamAddress for sqlx::postgres::PgConnectOptions {
    fn host_and_port(&self) -> (String, u16) {
        (self.get_host().to_owned(), self.get_port())
    }
}

//! Fresh databases on the test server.
//!
//! The server URL comes from the first set variable of
//! [`SERVER_URL_VARIABLES`]. Each [`TestDatabase`] is a new, empty database
//! named `<prefix>rs_<32 hex>`; the first operation of a horsies app
//! installs the schema. The database is dropped when the value is dropped.
//! These helpers never create or drop a database of another name.
//!
//! The PgBouncer lane sets [`TRANSACTION_POOL_URL_VARIABLE`]: the apps then
//! connect through the transaction pool, with the direct server URL as
//! their session database (Python `clients/python/tests/e2e/tasks/instance_pgbouncer.py`).

use tokio_postgres::{Client, NoTls};
use url::Url;
use uuid::Uuid;

/// The variables that name the test server, read in this order.
pub const SERVER_URL_VARIABLES: [&str; 3] = [
    "HORSIES_TEST_DATABASE_URL",
    "HORSES_E2E_DB_URL",
    "DATABASE_URL",
];

/// The variable that sets the prefix of every database name (Python
/// `clients/python/tests/session_database.py`).
pub const DATABASE_PREFIX_VARIABLE: &str = "HORSIES_TEST_DATABASE_PREFIX";

/// The prefix when [`DATABASE_PREFIX_VARIABLE`] is unset or empty.
pub const DEFAULT_DATABASE_PREFIX: &str = "horsies_test_";

/// The URL of the PgBouncer transaction pool in front of the test server.
/// Set only by the PgBouncer lane (Python's variable of the same name).
pub const TRANSACTION_POOL_URL_VARIABLE: &str = "HORSIES_TEST_DATABASE_URL_TRANSACTION";

/// The most bytes of a prefix: `<prefix>rs_<32 hex>` then stays inside
/// PostgreSQL's 63-byte identifier limit.
const PREFIX_MAX_LENGTH: usize = 24;

/// The part of the name that marks a database of the Rust tests.
const RUST_INFIX: &str = "rs_";

/// The database that the create and the drop connect to.
const ADMIN_DATABASE: &str = "postgres";

/// How the apps of a test reach its database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Topology {
    /// The apps connect to the server directly.
    Direct,
    /// The apps connect through a PgBouncer transaction pool; LISTEN and the
    /// session statements use the direct URL.
    TransactionPool {
        /// The pool URL with the database path of the test.
        pool_url: String,
    },
}

/// A database of one test, dropped with this value.
#[derive(Debug)]
pub struct TestDatabase {
    name: String,
    server_url: String,
    url: String,
    topology: Topology,
}

impl TestDatabase {
    /// Create a new empty database on the test server.
    ///
    /// Panics when no server is configured, when the prefix is malformed, or
    /// when the create fails; each panic names the cause.
    pub async fn create() -> TestDatabase {
        let server_url = server_url();
        let name = format!(
            "{}{RUST_INFIX}{}",
            database_prefix(),
            Uuid::new_v4().simple()
        );
        let admin = connect(&url_with_database(&server_url, ADMIN_DATABASE)).await;
        admin
            .batch_execute(&format!("CREATE DATABASE \"{name}\""))
            .await
            .unwrap_or_else(|error| panic!("create the test database {name}: {error}"));
        let topology = match std::env::var(TRANSACTION_POOL_URL_VARIABLE) {
            Ok(pool) if !pool.is_empty() => Topology::TransactionPool {
                pool_url: url_with_database(&pool, &name),
            },
            Ok(_) | Err(_) => Topology::Direct,
        };
        TestDatabase {
            url: url_with_database(&server_url, &name),
            name,
            server_url,
            topology,
        }
    }

    /// The database name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The direct URL of the database.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// How the apps of the test reach the database.
    pub fn topology(&self) -> &Topology {
        &self.topology
    }

    /// A new direct connection to the database, driven on the current
    /// runtime. Panics when the connect fails.
    pub async fn connect(&self) -> Client {
        connect(&self.url).await
    }
}

impl Drop for TestDatabase {
    /// Drop the database with its open sessions. Runs on a thread of its
    /// own, so it works inside and outside a runtime. A failed drop is
    /// reported on stderr: a panic here would abort a test that is already
    /// failing.
    fn drop(&mut self) {
        let admin_url = url_with_database(&self.server_url, ADMIN_DATABASE);
        let statement = format!("DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)", self.name);
        let dropped = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| format!("the drop runtime did not start: {error}"))?;
            runtime.block_on(async move {
                let (admin, connection) = tokio_postgres::connect(&admin_url, NoTls)
                    .await
                    .map_err(|error| format!("connect: {error}"))?;
                let _driver = tokio::spawn(connection);
                admin
                    .batch_execute(&statement)
                    .await
                    .map_err(|error| format!("drop: {error}"))
            })
        })
        .join();
        match dropped {
            Ok(Ok(())) => {}
            Ok(Err(cause)) => eprintln!("the test database {} was not dropped: {cause}", self.name),
            Err(_panic) => eprintln!(
                "the test database {} was not dropped: the drop thread panicked",
                self.name
            ),
        }
    }
}

/// The URL of the test server: the first set variable of
/// [`SERVER_URL_VARIABLES`]. Panics when none is set.
pub fn server_url() -> String {
    SERVER_URL_VARIABLES
        .iter()
        .find_map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()))
        .unwrap_or_else(|| {
            panic!(
                "no test server: set one of {SERVER_URL_VARIABLES:?} to a PostgreSQL URL; \
                 the tests never fall back to a local default"
            )
        })
}

/// The prefix of the database names: [`DATABASE_PREFIX_VARIABLE`], or
/// [`DEFAULT_DATABASE_PREFIX`]. Panics for a value that is not a lowercase
/// identifier of at most 24 bytes ending in `_`.
pub fn database_prefix() -> String {
    match std::env::var(DATABASE_PREFIX_VARIABLE) {
        Err(_) => DEFAULT_DATABASE_PREFIX.to_owned(),
        Ok(value) if value.is_empty() => DEFAULT_DATABASE_PREFIX.to_owned(),
        Ok(value) if is_valid_prefix(&value) => value,
        Ok(value) => panic!(
            "{DATABASE_PREFIX_VARIABLE}={value:?} is not a lowercase identifier of at most \
             {PREFIX_MAX_LENGTH} bytes that ends in '_'"
        ),
    }
}

fn is_valid_prefix(value: &str) -> bool {
    let bytes = value.as_bytes();
    let starts_with_letter = bytes.first().is_some_and(u8::is_ascii_lowercase);
    let ends_with_underscore = bytes.last() == Some(&b'_');
    let allowed = bytes
        .iter()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_');
    bytes.len() <= PREFIX_MAX_LENGTH && starts_with_letter && ends_with_underscore && allowed
}

/// `server_url` with the database path `database`. Panics for a URL that
/// does not parse.
pub fn url_with_database(server_url: &str, database: &str) -> String {
    let mut url = Url::parse(server_url)
        .unwrap_or_else(|error| panic!("the test server URL does not parse: {error}"));
    url.set_path(&format!("/{database}"));
    url.to_string()
}

/// A connection to `url`, driven on the current runtime. Panics when the
/// connect fails; the message names the database, not the password.
pub async fn connect(url: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .unwrap_or_else(|error| panic!("connect to {}: {error}", database_of(url)));
    let _driver = tokio::spawn(connection);
    client
}

/// The database path of `url`, for a message.
fn database_of(url: &str) -> String {
    Url::parse(url)
        .map(|parsed| parsed.path().trim_start_matches('/').to_owned())
        .unwrap_or_else(|_| "an unparsable URL".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_prefix_is_a_lowercase_identifier_that_ends_in_an_underscore() {
        assert!(is_valid_prefix("horsies_test_"));
        assert!(is_valid_prefix("r9_"));
        assert!(!is_valid_prefix("R9_"));
        assert!(!is_valid_prefix("r9"));
        assert!(!is_valid_prefix("9r_"));
        assert!(!is_valid_prefix("a_name_that_is_far_too_long_"));
        assert!(!is_valid_prefix(""));
    }

    #[test]
    fn the_database_path_is_replaced_and_the_query_kept() {
        let url = url_with_database(
            "postgresql://postgres:secret@127.0.0.1:55432/postgres?sslmode=disable",
            "horsies_test_rs_00",
        );
        assert_eq!(
            url,
            "postgresql://postgres:secret@127.0.0.1:55432/horsies_test_rs_00?sslmode=disable"
        );
    }
}

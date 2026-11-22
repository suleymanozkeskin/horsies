//! The databases, the sessions and the case-file runner of the SQL tests.
//!
//! - [`BaselineDatabase::create`] creates an empty database for the test
//!   and applies the text of every migration of the chain, then each setup
//!   text, in one transaction that commits.
//! - A [`Session`] is one connection to that database. It keeps the notices
//!   and the notifications that the server sends to it, in arrival order.
//! - [`Session::run_script`] sends a text as one simple-query message, so
//!   every statement of the text runs, in order, as psql runs a file.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};

use tokio_postgres::{AsyncMessage, Client};

use crate::db::{
    open_connection, parse_connection_url, ConnectPlan, ConnectionEnvironment,
    DEFAULT_SOCKET_SETTINGS,
};
use crate::migration_sql_file_texts;
use crate::test_support::TestDatabase;

/// The setup text that adds dblink, for the case files that open peer
/// sessions.
pub(super) const CREATE_DBLINK_SQL: &str = "CREATE EXTENSION dblink";

/// The prefix of the notice of a passed case in the files that print
/// `ok: <case>`.
pub(super) const PASSED_PREFIX: &str = "ok: ";

/// The setting that names the database for the peer sessions of a case file.
const PEER_CONNECTION_SQL: &str = "SELECT set_config('horsies_test.peer_connstr', $1, false)";

/// The text of the migration chain: every file of every migration in run
/// order (migration 1000, then each later migration), concatenated with no
/// separator. A function that a later migration replaces has its last
/// definition in force, as on a database that the install migrated.
pub(super) fn chain_sql() -> String {
    migration_sql_file_texts()
        .into_iter()
        .map(|(_, _, text)| text)
        .collect()
}

/// The text of a server error: SQLSTATE, message, detail and context.
pub(super) fn describe(error: &tokio_postgres::Error) -> String {
    match error.as_db_error() {
        Some(database_error) => format!(
            "{} {}: {}; detail: {}; where: {}",
            database_error.severity(),
            database_error.code().code(),
            database_error.message(),
            database_error.detail().unwrap_or("none"),
            database_error.where_().unwrap_or("none"),
        ),
        None => error.to_string(),
    }
}

/// A database of one test that holds the migration chain.
pub(super) struct BaselineDatabase {
    database: TestDatabase,
}

impl BaselineDatabase {
    /// Creates an empty database and applies the text of the migration chain
    /// and then each text of `setup`, in one transaction that commits.
    ///
    /// # Panics
    ///
    /// When a statement fails: the test cannot run.
    pub(super) async fn create(setup: &[&str]) -> BaselineDatabase {
        BaselineDatabase::install(TestDatabase::create_empty().await, setup).await
    }

    /// [`BaselineDatabase::create`] on a database whose default collation
    /// ignores punctuation at the first level, as glibc's `en_US.UTF-8`
    /// does.
    ///
    /// # Panics
    ///
    /// When the server cannot create such a database, or when a statement
    /// fails: the test cannot run.
    pub(super) async fn create_ignoring_punctuation(setup: &[&str]) -> BaselineDatabase {
        BaselineDatabase::install(
            TestDatabase::create_empty_ignoring_punctuation().await,
            setup,
        )
        .await
    }

    /// Applies the text of the migration chain and then each text of `setup`
    /// to the empty `database`, in one transaction that commits.
    async fn install(database: TestDatabase, setup: &[&str]) -> BaselineDatabase {
        let client = database.client();
        run(client, "BEGIN", "BEGIN").await;
        run(client, "the migration chain", &chain_sql()).await;
        for text in setup {
            run(client, "a setup text", text).await;
        }
        run(client, "COMMIT", "COMMIT").await;
        BaselineDatabase { database }
    }

    /// A new session on the database, in autocommit.
    pub(super) async fn session(&self) -> Session {
        Session::open(self.database.url()).await
    }

    /// A new session whose setting `horsies_test.peer_connstr` is the URL of
    /// this database: the two-session case files open their peer sessions
    /// (dblink) with it.
    pub(super) async fn peer_session(&self) -> Session {
        let session = self.session().await;
        let _set = session
            .client()
            .execute(PEER_CONNECTION_SQL, &[&self.database.url()])
            .await
            .unwrap_or_else(|error| panic!("set the peer connection: {}", describe(&error)));
        session
    }
}

/// Runs `text` on `client` as one simple-query message.
///
/// # Panics
///
/// When a statement fails, with `name` and the server error.
async fn run(client: &Client, name: &str, text: &str) {
    client
        .batch_execute(text)
        .await
        .unwrap_or_else(|error| panic!("{name}: {}", describe(&error)));
}

/// What a session receives from the server outside the replies to its
/// statements, in arrival order.
#[derive(Default)]
struct Received {
    notices: Vec<String>,
    /// `(channel, payload)` of each notification.
    notifications: Vec<(String, String)>,
}

/// One connection to a test database, with its notices and notifications.
pub(super) struct Session {
    client: Client,
    received: Arc<Mutex<Received>>,
    backend_pid: i32,
}

impl Session {
    /// Opens a connection to `url`. Its driver runs as a task of the current
    /// runtime and ends with it. The driver hands a notice or a notification
    /// to the session before it reads the messages after it, so what a
    /// statement causes on its own session is kept when the call of that
    /// statement returns.
    ///
    /// # Panics
    ///
    /// When the URL is refused or the server cannot be reached.
    pub(super) async fn open(url: &str) -> Session {
        let environment =
            ConnectionEnvironment::capture().expect("read the connection environment of the test");
        let settings = parse_connection_url(url, &environment)
            .unwrap_or_else(|error| panic!("the test database URL is refused: {error}"));
        let plan = ConnectPlan::load(&settings, &environment, &DEFAULT_SOCKET_SETTINGS)
            .unwrap_or_else(|error| panic!("the test connection plan does not load: {error}"));
        let (client, mut connection) = open_connection(&plan)
            .await
            .unwrap_or_else(|error| panic!("connecting to the test server failed: {error}"));
        let received = Arc::new(Mutex::new(Received::default()));
        let collected = Arc::clone(&received);
        drop(tokio::spawn(async move {
            while let Some(Ok(message)) =
                std::future::poll_fn(|context| connection.poll_message(context)).await
            {
                let mut received = collected.lock().unwrap_or_else(PoisonError::into_inner);
                match message {
                    AsyncMessage::Notice(notice) => {
                        received.notices.push(notice.message().to_owned());
                    }
                    AsyncMessage::Notification(notification) => {
                        received.notifications.push((
                            notification.channel().to_owned(),
                            notification.payload().to_owned(),
                        ));
                    }
                    _ => {}
                }
            }
        }));
        let backend_pid: i32 = client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap_or_else(|error| panic!("read the backend pid: {}", describe(&error)))
            .get(0);
        Session {
            client,
            received,
            backend_pid,
        }
    }

    /// The connection of the session.
    pub(super) fn client(&self) -> &Client {
        &self.client
    }

    /// The process id of the server backend of the session.
    pub(super) fn backend_pid(&self) -> i32 {
        self.backend_pid
    }

    /// Runs `text` as one simple-query message.
    ///
    /// # Panics
    ///
    /// When a statement fails, with `name` and the server error.
    pub(super) async fn run_script(&self, name: &str, text: &str) {
        run(&self.client, name, text).await;
    }

    /// Runs `text` as one simple-query message in a transaction that is
    /// rolled back.
    ///
    /// # Panics
    ///
    /// When a statement fails, with `name` and the server error.
    pub(super) async fn run_script_rolled_back(&self, name: &str, text: &str) {
        run(&self.client, "BEGIN", "BEGIN").await;
        run(&self.client, name, text).await;
        run(&self.client, "ROLLBACK", "ROLLBACK").await;
    }

    /// The notices received so far whose message starts with `prefix`, in
    /// arrival order, with the prefix removed.
    pub(super) fn notices_after(&self, prefix: &str) -> Vec<String> {
        self.received
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .notices
            .iter()
            .filter_map(|notice| notice.strip_prefix(prefix))
            .map(str::to_owned)
            .collect()
    }

    /// Takes the notifications received so far, as `(channel, payload)` in
    /// arrival order.
    pub(super) fn take_notifications(&self) -> Vec<(String, String)> {
        std::mem::take(
            &mut self
                .received
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .notifications,
        )
    }
}

/// Asserts that `passed` holds `expected` case names and no name twice.
pub(super) fn assert_passed_cases(passed: &[String], expected: usize) {
    assert_eq!(passed.len(), expected, "{passed:?}");
    let distinct: HashSet<&String> = passed.iter().collect();
    assert_eq!(distinct.len(), expected, "two cases share a name");
}

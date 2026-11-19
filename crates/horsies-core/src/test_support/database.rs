//! Test databases on the statement layer (tokio-postgres), the same way for
//! every core test.
//!
//! - One database per test. [`TestDatabase::create`] copies the template of
//!   this process, which holds the installed schema (the whole migration
//!   chain, Rust family); [`TestDatabase::create_empty`] creates an empty one
//!   (install tests). [`database_of_this_test`] gives the tests of the
//!   shared-pool helpers one copied database per test runtime.
//! - The template is built once per process, by `super::install_template`,
//!   which the including module supplies: the library's tests install on the
//!   core pools; an integration test installs through its public API.
//! - Every name starts with one prefix, `HORSIES_TEST_DATABASE_PREFIX`
//!   (default `horsies_test_`), the variable of the Python suites
//!   (`tests/session_database.py`): `<prefix>rs_<32 hex>` for a test
//!   database and `<prefix>rstpl_<32 hex>` for a template. Runs that share
//!   one server set different prefixes.
//! - A test database is dropped (`DROP DATABASE ... WITH (FORCE)`) when the
//!   runtime of its test shuts down, after a panic too. Before its first
//!   creation, a process drops the inactive databases and templates of this
//!   prefix that an earlier process left. The sweep runs once and before any
//!   database of the process exists, so it never meets one of them.
//! - The server is `DATABASE_URL`, or the local server with `DB_PASSWORD` of
//!   the workspace `.env`.
//!
//! The library compiles this file into its tests; an integration test
//! includes it with `#[path]`. So it names the library `horsies_core` and
//! uses only its public API.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};

use horsies_core::db::{
    open_connection, parse_connection_url, ConnectPlan, ConnectionEnvironment,
    DEFAULT_SOCKET_SETTINGS,
};
use tokio::runtime::{Handle, Id as RuntimeId};
use tokio::sync::OnceCell;
use tokio_postgres::Client;
use uuid::Uuid;

/// The variable of the name prefix, shared with the Python suites.
pub(crate) const DATABASE_PREFIX_VARIABLE: &str = "HORSIES_TEST_DATABASE_PREFIX";

/// The prefix when the variable is unset or empty.
pub(crate) const DEFAULT_DATABASE_PREFIX: &str = "horsies_test_";

/// The longest prefix: `<prefix>rstpl_<32 hex>` then stays inside
/// PostgreSQL's 63-byte identifier limit (Python's bound too).
const MAX_PREFIX_BYTES: usize = 24;

/// The kind part of a test database name.
const TEST_DATABASE_KIND: &str = "rs_";

/// The kind part of a template name.
const TEMPLATE_KIND: &str = "rstpl_";

/// The database the admin connections use.
const ADMIN_DATABASE: &str = "postgres";

/// One creation or sweep at a time on the server, for every core test
/// process.
const SETUP_LOCK_SQL: &str =
    "SELECT pg_advisory_lock(hashtextextended('horsies_core_test_database_setup', 0))";
const SETUP_UNLOCK_SQL: &str =
    "SELECT pg_advisory_unlock(hashtextextended('horsies_core_test_database_setup', 0))";

/// The inactive databases whose name starts with `$1`.
const INACTIVE_DATABASES_SQL: &str = "SELECT d.datname::text
     FROM pg_database AS d
     WHERE starts_with(d.datname::text, $1)
       AND NOT EXISTS (SELECT 1 FROM pg_stat_activity AS a WHERE a.datname = d.datname)
     ORDER BY 1";

/// Set once this process swept the leftovers of earlier processes.
static SWEPT: OnceCell<()> = OnceCell::const_new();

/// The template of this process: its name.
static TEMPLATE: OnceCell<String> = OnceCell::const_new();

/// The database of each live test runtime ([`database_of_this_test`]).
static RUNTIME_DATABASES: LazyLock<Mutex<HashMap<RuntimeId, Arc<OnceCell<Arc<RuntimeDatabase>>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The prefix of every test database name.
///
/// # Panics
///
/// When `HORSIES_TEST_DATABASE_PREFIX` is not a lowercase identifier of at
/// most 24 bytes that ends in `_` (`[a-z][a-z0-9_]{0,22}_`), or is not
/// Unicode: the run is set up wrongly.
pub(crate) fn database_name_prefix() -> String {
    match std::env::var(DATABASE_PREFIX_VARIABLE) {
        Err(std::env::VarError::NotPresent) => DEFAULT_DATABASE_PREFIX.to_owned(),
        Ok(value) if value.is_empty() => DEFAULT_DATABASE_PREFIX.to_owned(),
        Ok(value) if is_valid_prefix(&value) => value,
        Ok(value) => panic!(
            "{DATABASE_PREFIX_VARIABLE}={value:?} is not a lowercase identifier of at most \
             {MAX_PREFIX_BYTES} bytes that ends in '_'"
        ),
        Err(std::env::VarError::NotUnicode(value)) => {
            panic!("{DATABASE_PREFIX_VARIABLE}={value:?} is not Unicode")
        }
    }
}

/// Whether `value` matches `[a-z][a-z0-9_]{0,22}_`.
pub(crate) fn is_valid_prefix(value: &str) -> bool {
    let bytes = value.as_bytes();
    (2..=MAX_PREFIX_BYTES).contains(&bytes.len())
        && bytes[0].is_ascii_lowercase()
        && bytes.ends_with(b"_")
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
}

/// Whether `name` is a test database or template name that this support
/// generated under `prefix`: the prefix, a kind, then 32 lowercase hex
/// digits. A sweep drops no other name.
pub(crate) fn is_generated_name(name: &str, prefix: &str) -> bool {
    let Some(rest) = name.strip_prefix(prefix) else {
        return false;
    };
    let suffix = match (
        rest.strip_prefix(TEMPLATE_KIND),
        rest.strip_prefix(TEST_DATABASE_KIND),
    ) {
        (Some(suffix), _) | (None, Some(suffix)) => suffix,
        (None, None) => return false,
    };
    suffix.len() == 32
        && suffix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// The URL of the test server.
pub(crate) fn server_url() -> String {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return url;
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .find(|path| path.join(".env").exists());
    let password = root
        .and_then(|path| std::fs::read_to_string(path.join(".env")).ok())
        .and_then(|contents| {
            contents
                .lines()
                .filter_map(|line| line.trim().split_once('='))
                .find(|(key, _)| key.trim() == "DB_PASSWORD")
                .map(|(_, value)| value.trim().to_owned())
        })
        .unwrap_or_else(|| "W0rklane".to_owned());
    format!("postgresql://postgres:{password}@localhost:5432/horsies-core")
}

/// `url` with its database replaced by `database`; the query is kept.
pub(crate) fn with_database(url: &str, database: &str) -> String {
    let (scheme, rest) = url.split_once("://").expect("the test URL has a scheme");
    let authority_end = rest
        .find(|character: char| character == '/' || character == '?')
        .unwrap_or(rest.len());
    let (authority, remainder) = rest.split_at(authority_end);
    match remainder.split_once('?') {
        Some((_, query)) => format!("{scheme}://{authority}/{database}?{query}"),
        None => format!("{scheme}://{authority}/{database}"),
    }
}

/// Opens one direct connection to `url`. Its driver runs as a task of the
/// current runtime and ends with it.
///
/// # Panics
///
/// When the URL is refused or the server cannot be reached: the test cannot
/// run.
pub(crate) async fn connect(url: &str) -> Client {
    let environment =
        ConnectionEnvironment::capture().expect("read the connection environment of the test");
    let settings = parse_connection_url(url, &environment)
        .unwrap_or_else(|error| panic!("the test database URL is refused: {error}"));
    let plan = ConnectPlan::load(&settings, &environment, &DEFAULT_SOCKET_SETTINGS)
        .unwrap_or_else(|error| panic!("the test connection plan does not load: {error}"));
    let (client, connection) = open_connection(&plan)
        .await
        .unwrap_or_else(|error| panic!("connecting to the test server failed: {error}"));
    drop(tokio::spawn(async move {
        let _ended = connection.await;
    }));
    client
}

async fn admin_connection() -> Client {
    connect(&with_database(&server_url(), ADMIN_DATABASE)).await
}

async fn run(client: &Client, sql: &str) {
    client
        .batch_execute(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

/// The template of this process, built at the first call.
async fn template_name() -> &'static str {
    TEMPLATE.get_or_init(build_template).await.as_str()
}

/// Creates the template and installs the schema into it. The installer's
/// connections are closed when it returns, so the template can be copied.
async fn build_template() -> String {
    sweep_once().await;
    let name = format!(
        "{}{TEMPLATE_KIND}{}",
        database_name_prefix(),
        Uuid::new_v4().simple()
    );
    let admin = admin_connection().await;
    run(&admin, SETUP_LOCK_SQL).await;
    run(&admin, &format!("CREATE DATABASE \"{name}\"")).await;
    run(&admin, SETUP_UNLOCK_SQL).await;
    super::install_template(&with_database(&server_url(), &name)).await;
    name
}

/// Drops, once per process and before its first creation, what earlier
/// processes of this prefix left.
async fn sweep_once() {
    SWEPT
        .get_or_init(|| async {
            let admin = admin_connection().await;
            run(&admin, SETUP_LOCK_SQL).await;
            drop_inactive_leftovers(&admin, &database_name_prefix()).await;
            run(&admin, SETUP_UNLOCK_SQL).await;
        })
        .await;
}

/// Drops every inactive database of `prefix` with a generated name. The
/// caller holds the setup lock.
async fn drop_inactive_leftovers(admin: &Client, prefix: &str) {
    let rows = admin
        .query(INACTIVE_DATABASES_SQL, &[&prefix])
        .await
        .unwrap_or_else(|error| panic!("list the inactive test databases: {error}"));
    for row in rows {
        let name: String = row.get(0);
        if is_generated_name(&name, prefix) {
            run(admin, &drop_sql(&name)).await;
        }
    }
}

fn drop_sql(name: &str) -> String {
    format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)")
}

/// What a new test database starts from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Source {
    /// A copy of the template of this process: the installed schema.
    Template,
    /// An empty database.
    Empty,
}

/// Creates a test database and schedules its drop at the shutdown of the
/// current runtime. Returns its name.
async fn create_database(source: Source, entry: RuntimeEntry) -> String {
    let create = match source {
        Source::Template => format!("TEMPLATE \"{}\"", template_name().await),
        Source::Empty => {
            sweep_once().await;
            String::new()
        }
    };
    let name = format!(
        "{}{TEST_DATABASE_KIND}{}",
        database_name_prefix(),
        Uuid::new_v4().simple()
    );
    let admin = admin_connection().await;
    run(&admin, SETUP_LOCK_SQL).await;
    run(&admin, &format!("CREATE DATABASE \"{name}\" {create}")).await;
    run(&admin, SETUP_UNLOCK_SQL).await;
    let guard = DropAtShutdown {
        name: name.clone(),
        entry,
    };
    drop(tokio::spawn(async move {
        let _guard = guard;
        std::future::pending::<()>().await;
    }));
    name
}

/// Which entry of [`RUNTIME_DATABASES`] a dropped database leaves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RuntimeEntry {
    /// The database of a test runtime.
    Of(RuntimeId),
    /// A database the test owns.
    NoEntry,
}

/// Held by a task that never ends: the shutdown of the test runtime drops
/// the task, and this drops the database.
struct DropAtShutdown {
    name: String,
    entry: RuntimeEntry,
}

impl Drop for DropAtShutdown {
    /// Drops the database on a thread of its own, with a runtime of its own:
    /// the test runtime is shutting down. A failure is printed, not raised (a
    /// drop may run during a panic); the next template build of this prefix
    /// drops what is left.
    fn drop(&mut self) {
        match self.entry {
            RuntimeEntry::Of(runtime) => {
                let _left = RUNTIME_DATABASES
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&runtime);
            }
            RuntimeEntry::NoEntry => {}
        }
        let name = std::mem::take(&mut self.name);
        let shown = name.clone();
        let dropped = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a runtime for the test database drop");
            runtime.block_on(async {
                let admin = admin_connection().await;
                admin.batch_execute(&drop_sql(&name)).await
            })
        })
        .join();
        match dropped {
            Ok(Ok(())) => {}
            Ok(Err(error)) => eprintln!("dropping test database {shown} failed: {error}"),
            Err(_panic) => eprintln!("dropping test database {shown} failed: the drop panicked"),
        }
    }
}

/// A database of one test. Dropped at the shutdown of the test runtime, or
/// earlier by [`TestDatabase::drop`].
pub(crate) struct TestDatabase {
    name: String,
    url: String,
    client: Client,
}

impl TestDatabase {
    /// A copy of the template: the installed schema, Rust family.
    pub(crate) async fn create() -> TestDatabase {
        TestDatabase::open(create_database(Source::Template, RuntimeEntry::NoEntry).await).await
    }

    /// An empty database.
    pub(crate) async fn create_empty() -> TestDatabase {
        TestDatabase::open(create_database(Source::Empty, RuntimeEntry::NoEntry).await).await
    }

    async fn open(name: String) -> TestDatabase {
        let url = with_database(&server_url(), &name);
        let client = connect(&url).await;
        TestDatabase { name, url, client }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    /// A connection to this database for setup statements and reads.
    pub(crate) fn client(&self) -> &Client {
        &self.client
    }

    /// Another connection to this database (a second session).
    pub(crate) async fn connect(&self) -> Client {
        connect(&self.url).await
    }

    /// Drops the database now, with the sessions still open on it.
    pub(crate) async fn drop(self) {
        let TestDatabase { name, client, .. } = self;
        drop(client);
        let admin = admin_connection().await;
        run(&admin, &drop_sql(&name)).await;
    }
}

/// The copied database of one test runtime, shared by every caller in it.
pub(crate) struct RuntimeDatabase {
    url: String,
    setup: OnceCell<()>,
}

impl RuntimeDatabase {
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    /// A setup that the first caller in the test runs once on this database
    /// (the coverage pass of the shared-pool helpers).
    pub(crate) fn setup(&self) -> &OnceCell<()> {
        &self.setup
    }
}

/// The database of the running test: a copy of the template, created at the
/// first call in this test runtime and shared by every later call in it
/// (spawned tasks included), dropped when the runtime shuts down.
pub(crate) async fn database_of_this_test() -> Arc<RuntimeDatabase> {
    let runtime = Handle::current().id();
    let cell = Arc::clone(
        RUNTIME_DATABASES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(runtime)
            .or_default(),
    );
    let database = cell
        .get_or_init(|| async move {
            let name = create_database(Source::Template, RuntimeEntry::Of(runtime)).await;
            Arc::new(RuntimeDatabase {
                url: with_database(&server_url(), &name),
                setup: OnceCell::new(),
            })
        })
        .await;
    Arc::clone(database)
}

//! The statements of the core's tests, on the statement layer's driver
//! (tokio-postgres): seeds, reads and assertions on a test database.
//!
//! A test statement is SQL text and its bound values. Each value declares its
//! PostgreSQL type from its Rust type ([`Param`]), and the statement runs as
//! one unnamed statement with those types (`query_typed`, `execute_typed`).
//! The declared type of a Rust type is the one sqlx declared for it (`i32`
//! int4, `i64` int8, `&str` and `String` text, `serde_json::Value` jsonb,
//! `Vec<u8>` bytea, `Vec<T>` the array of `T`, `Option<T>` the type of `T`),
//! so a test that moved from sqlx sends the same statement with the same
//! parameter types, and the server resolves every parameter as before.
//!
//! - [`TestPool`]: sessions on one database, at most `max_connections` open.
//!   A statement on the pool checks a session out for its duration. A
//!   checkout takes the last returned session whose driver still runs; it
//!   sends no ping.
//! - [`TestConnection`]: one session. [`TestConnection::begin`] opens a
//!   [`TestTransaction`] (`BEGIN`, or a savepoint inside an open one).
//! - [`query`], [`query_scalar`], [`query_as`]: one statement. `fetch_one`
//!   takes the first row and fails with [`Error::RowNotFound`] on none;
//!   `fetch_optional` takes the first row or none; `fetch_all` takes every
//!   row; `execute` returns the count of the command tag. [`raw_sql`]: a text
//!   of several statements in one simple-query message.
//! - [`TestListener`]: `LISTEN` on a session of its own, notifications in
//!   arrival order.
//!
//! This module is test code: the shipped core runs its typed statements
//! ([`crate::db::TypedStatement`]).

use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_postgres::error::DbError;
use tokio_postgres::types::{FromSqlOwned, ToSql, Type};
use tokio_postgres::{AsyncMessage, Client, Notification, Row, SimpleQueryMessage};
use uuid::Uuid;

use crate::db::{
    open_connection, parse_connection_url, ConnectError, ConnectPlan, ConnectPlanError,
    ConnectionEnvironment, DriverConnection, EnvironmentError, UrlError, DEFAULT_SOCKET_SETTINGS,
};

/// The sessions of a pool when the test names no bound (sqlx's default,
/// which the tests ran with before).
pub(crate) const DEFAULT_MAX_CONNECTIONS: u32 = 10;

/// The longest wait for a free session of a pool when the test names no
/// bound (sqlx's default).
pub(crate) const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

/// The deepest transaction nesting of one session: `BEGIN` and the
/// savepoints inside it.
pub(crate) const MAX_TRANSACTION_DEPTH: u32 = 16;

/// The notifications a listener keeps before its session waits for the test
/// to read them.
pub(crate) const LISTENER_QUEUE_CAPACITY: usize = 1024;

/// Why a test statement, a checkout or a connect failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    /// The server refused the statement: its SQLSTATE, message, detail and
    /// constraint.
    #[error("{0}")]
    Database(Box<DbError>),
    /// The driver failed: the session ended, or an I/O, TLS or protocol
    /// error.
    #[error("{0}")]
    Driver(tokio_postgres::Error),
    /// `fetch_one` met no row.
    #[error("the statement returned no row")]
    RowNotFound,
    /// A column did not decode into the Rust type that the test asked for.
    #[error("column {column}: {source}")]
    Decode {
        column: String,
        source: tokio_postgres::Error,
    },
    /// The core's URL parser refused the URL.
    #[error("the test database URL is refused: {0}")]
    Url(#[from] UrlError),
    /// The process environment of the connection could not be read.
    #[error("the connection environment is unreadable: {0}")]
    Environment(#[from] EnvironmentError),
    /// The connection plan (TLS files, password file) did not load.
    #[error("the connection plan does not load: {0}")]
    Plan(#[from] ConnectPlanError),
    /// The server could not be reached, or refused the session.
    #[error("{0}")]
    Connect(#[from] ConnectError),
    /// No session of the pool came free within its checkout bound.
    #[error("no session of the test pool came free within {0:?}")]
    PoolTimedOut(Duration),
    /// The pool was closed.
    #[error("the test pool is closed")]
    PoolClosed,
    /// A transaction was opened past [`MAX_TRANSACTION_DEPTH`].
    #[error("transactions nest deeper than {MAX_TRANSACTION_DEPTH} on one session")]
    TooDeep,
}

impl From<tokio_postgres::Error> for Error {
    /// A refusal of the server is [`Error::Database`]; any other driver error
    /// is [`Error::Driver`].
    fn from(error: tokio_postgres::Error) -> Error {
        match error.as_db_error() {
            Some(server) => Error::Database(Box::new(server.clone())),
            None => Error::Driver(error),
        }
    }
}

impl Error {
    /// The server's error, when the server refused the statement.
    pub(crate) fn as_database_error(&self) -> Option<&DbError> {
        match self {
            Error::Database(error) => Some(error.as_ref()),
            Error::Driver(_)
            | Error::Connect(_)
            | Error::RowNotFound
            | Error::Decode { .. }
            | Error::Url(_)
            | Error::Environment(_)
            | Error::Plan(_)
            | Error::PoolTimedOut(_)
            | Error::PoolClosed
            | Error::TooDeep => None,
        }
    }

    /// The SQLSTATE of the server's error, when the server refused the
    /// statement.
    pub(crate) fn sqlstate(&self) -> Option<&str> {
        self.as_database_error().map(|error| error.code().code())
    }
}

/// Why a test client of a SQL function failed.
#[derive(Debug)]
pub(crate) enum CallError {
    /// The server refused the statement, or the session failed.
    Database(Error),
    /// The function answered outside the vocabulary of its client.
    Contract(crate::core::history::errors::HistoryError),
}

/// A value that a test binds. Its declared PostgreSQL type follows from its
/// Rust type: the type that sqlx declared for the same Rust type.
pub(crate) trait Param: ToSql + Send + Sync {
    /// The declared type of the parameter.
    fn declared_type() -> Type;
}

/// A value that a test binds as an element of an array.
pub(crate) trait ArrayElement: Param {
    /// The declared type of an array of this element.
    fn array_type() -> Type;
}

macro_rules! declared_types {
    ($($rust:ty => $declared:expr, $array:expr;)*) => {$(
        impl Param for $rust {
            fn declared_type() -> Type {
                $declared
            }
        }

        impl ArrayElement for $rust {
            fn array_type() -> Type {
                $array
            }
        }
    )*};
}

declared_types! {
    bool => Type::BOOL, Type::BOOL_ARRAY;
    i8 => Type::CHAR, Type::CHAR_ARRAY;
    i16 => Type::INT2, Type::INT2_ARRAY;
    i32 => Type::INT4, Type::INT4_ARRAY;
    i64 => Type::INT8, Type::INT8_ARRAY;
    f32 => Type::FLOAT4, Type::FLOAT4_ARRAY;
    f64 => Type::FLOAT8, Type::FLOAT8_ARRAY;
    String => Type::TEXT, Type::TEXT_ARRAY;
    Uuid => Type::UUID, Type::UUID_ARRAY;
    DateTime<Utc> => Type::TIMESTAMPTZ, Type::TIMESTAMPTZ_ARRAY;
    NaiveDateTime => Type::TIMESTAMP, Type::TIMESTAMP_ARRAY;
    NaiveDate => Type::DATE, Type::DATE_ARRAY;
    serde_json::Value => Type::JSONB, Type::JSONB_ARRAY;
    Vec<u8> => Type::BYTEA, Type::BYTEA_ARRAY;
}

impl Param for &str {
    fn declared_type() -> Type {
        Type::TEXT
    }
}

impl ArrayElement for &str {
    fn array_type() -> Type {
        Type::TEXT_ARRAY
    }
}

impl Param for std::borrow::Cow<'_, str> {
    fn declared_type() -> Type {
        Type::TEXT
    }
}

impl Param for Box<str> {
    fn declared_type() -> Type {
        Type::TEXT
    }
}

impl Param for &[u8] {
    fn declared_type() -> Type {
        Type::BYTEA
    }
}

impl ArrayElement for &[u8] {
    fn array_type() -> Type {
        Type::BYTEA_ARRAY
    }
}

impl<T: Param> Param for Option<T> {
    fn declared_type() -> Type {
        T::declared_type()
    }
}

impl<T: ArrayElement> ArrayElement for Option<T> {
    fn array_type() -> Type {
        T::array_type()
    }
}

impl<T: Param> Param for &T {
    fn declared_type() -> Type {
        T::declared_type()
    }
}

impl<T: ArrayElement> Param for Vec<T> {
    fn declared_type() -> Type {
        T::array_type()
    }
}

impl<T: ArrayElement> Param for &[T] {
    fn declared_type() -> Type {
        T::array_type()
    }
}

/// A row that a test reads into one Rust value: a tuple of the columns in
/// order, or a struct with its own reader.
pub(crate) trait FromTestRow: Sized {
    /// Reads the row.
    ///
    /// Errors: [`Error::Decode`] naming the column that did not decode.
    fn from_row(row: &Row) -> Result<Self, Error>;
}

/// Column `index` of `row` as `T`.
///
/// Errors: [`Error::Decode`] when the column is absent or does not decode
/// into `T`.
pub(crate) fn column<T: FromSqlOwned>(row: &Row, index: usize) -> Result<T, Error> {
    row.try_get::<_, T>(index).map_err(|source| Error::Decode {
        column: index.to_string(),
        source,
    })
}

/// Column `name` of `row` as `T`.
///
/// Errors: [`Error::Decode`] when the column is absent or does not decode
/// into `T`.
pub(crate) fn named<T: FromSqlOwned>(row: &Row, name: &str) -> Result<T, Error> {
    row.try_get::<_, T>(name).map_err(|source| Error::Decode {
        column: name.to_owned(),
        source,
    })
}

macro_rules! tuple_rows {
    ($(($($name:ident $index:tt),+))*) => {$(
        impl<$($name: FromSqlOwned),+> FromTestRow for ($($name,)+) {
            fn from_row(row: &Row) -> Result<Self, Error> {
                Ok(($(column::<$name>(row, $index)?,)+))
            }
        }
    )*};
}

tuple_rows! {
    (A 0)
    (A 0, B 1)
    (A 0, B 1, C 2)
    (A 0, B 1, C 2, D 3)
    (A 0, B 1, C 2, D 3, E 4)
    (A 0, B 1, C 2, D 3, E 4, F 5)
    (A 0, B 1, C 2, D 3, E 4, F 5, G 6)
    (A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7)
    (A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8)
    (A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9)
    (A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10)
    (A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11)
    (A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11, M 12)
    (A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11, M 12, N 13)
    (A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11, M 12, N 13, O 14)
    (A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11, M 12, N 13, O 14, P 15)
}

/// The count of the command tag of an executed statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Executed {
    rows_affected: u64,
}

impl Executed {
    /// The rows that the statement inserted, updated, deleted or returned.
    pub(crate) fn rows_affected(&self) -> u64 {
        self.rows_affected
    }
}

/// Where a statement runs: a session that the pool checks out for it, the
/// test's own session (also the session of an open transaction), or a
/// driver client that a test holds outside this module.
pub(crate) enum Target<'t> {
    Pool(&'t TestPool),
    Connection(&'t mut TestConnection),
    Client(&'t Client),
}

impl<'t> From<&'t Client> for Target<'t> {
    fn from(client: &'t Client) -> Target<'t> {
        Target::Client(client)
    }
}

impl<'t> From<&'t TestPool> for Target<'t> {
    fn from(pool: &'t TestPool) -> Target<'t> {
        Target::Pool(pool)
    }
}

impl<'t> From<&'t mut TestConnection> for Target<'t> {
    fn from(connection: &'t mut TestConnection) -> Target<'t> {
        Target::Connection(connection)
    }
}

impl<'t> From<&'t mut PooledConnection> for Target<'t> {
    fn from(connection: &'t mut PooledConnection) -> Target<'t> {
        Target::Connection(connection.session())
    }
}

impl<'t, 'c> From<&'t mut TestTransaction<'c>> for Target<'t> {
    fn from(transaction: &'t mut TestTransaction<'c>) -> Target<'t> {
        Target::Connection(transaction.session())
    }
}

/// One bound value and its declared type.
struct Bound<'q> {
    value: Box<dyn ToSql + Send + Sync + 'q>,
    declared: Type,
}

impl Target<'_> {
    /// Runs the statement and returns its rows.
    async fn rows(self, sql: &str, params: &[Bound<'_>]) -> Result<Vec<Row>, Error> {
        match self {
            Target::Pool(pool) => {
                let mut connection = pool.acquire().await?;
                connection.session().rows(sql, params).await
            }
            Target::Connection(connection) => connection.rows(sql, params).await,
            Target::Client(client) => Ok(client.query_typed(sql, &typed_params(params)).await?),
        }
    }

    /// Runs the statement and returns the count of its command tag.
    async fn count(self, sql: &str, params: &[Bound<'_>]) -> Result<u64, Error> {
        match self {
            Target::Pool(pool) => {
                let mut connection = pool.acquire().await?;
                connection.session().count(sql, params).await
            }
            Target::Connection(connection) => connection.count(sql, params).await,
            Target::Client(client) => Ok(client.execute_typed(sql, &typed_params(params)).await?),
        }
    }

    /// Runs a text of several statements and returns the sum of their
    /// command tag counts.
    async fn script(self, sql: &str) -> Result<u64, Error> {
        match self {
            Target::Pool(pool) => {
                let mut connection = pool.acquire().await?;
                connection.session().script(sql).await
            }
            Target::Connection(connection) => connection.script(sql).await,
            Target::Client(client) => Ok(command_count(&client.simple_query(sql).await?)),
        }
    }
}

/// One statement whose rows the test reads as [`Row`]s.
pub(crate) struct Query<'q> {
    sql: &'q str,
    params: Vec<Bound<'q>>,
}

/// A statement of `sql`. Bind its parameters in order with
/// [`Query::bind`].
pub(crate) fn query(sql: &str) -> Query<'_> {
    Query {
        sql,
        params: Vec::new(),
    }
}

impl<'q> Query<'q> {
    /// Binds the next parameter, with the declared type of `T`.
    #[must_use]
    pub(crate) fn bind<T: Param + 'q>(mut self, value: T) -> Query<'q> {
        self.params.push(Bound {
            value: Box::new(value),
            declared: T::declared_type(),
        });
        self
    }

    /// Runs the statement; returns the count of its command tag.
    ///
    /// Errors: [`Error::Database`], [`Error::Driver`], or a checkout error of a pool.
    pub(crate) async fn execute<'t>(
        self,
        target: impl Into<Target<'t>>,
    ) -> Result<Executed, Error> {
        let rows_affected = target.into().count(self.sql, &self.params).await?;
        Ok(Executed { rows_affected })
    }

    /// The first row.
    ///
    /// Errors: [`Error::RowNotFound`] when there is none;
    /// [`Error::Database`], [`Error::Driver`], or a checkout error of a pool.
    pub(crate) async fn fetch_one<'t>(self, target: impl Into<Target<'t>>) -> Result<Row, Error> {
        self.fetch_optional(target).await?.ok_or(Error::RowNotFound)
    }

    /// The first row, or `None` when there is none.
    ///
    /// Errors: [`Error::Database`], [`Error::Driver`], or a checkout error of a pool.
    pub(crate) async fn fetch_optional<'t>(
        self,
        target: impl Into<Target<'t>>,
    ) -> Result<Option<Row>, Error> {
        Ok(self.fetch_all(target).await?.into_iter().next())
    }

    /// Every row, in the order the server sent them.
    ///
    /// Errors: [`Error::Database`], [`Error::Driver`], or a checkout error of a pool.
    pub(crate) async fn fetch_all<'t>(
        self,
        target: impl Into<Target<'t>>,
    ) -> Result<Vec<Row>, Error> {
        target.into().rows(self.sql, &self.params).await
    }
}

/// One statement whose rows the test reads as the first column, `T`.
pub(crate) struct QueryScalar<'q, T> {
    query: Query<'q>,
    output: PhantomData<fn() -> T>,
}

/// A statement of `sql` whose first column the test reads as `T`.
pub(crate) fn query_scalar<T: FromSqlOwned>(sql: &str) -> QueryScalar<'_, T> {
    QueryScalar {
        query: query(sql),
        output: PhantomData,
    }
}

impl<'q, T: FromSqlOwned> QueryScalar<'q, T> {
    /// Binds the next parameter, with the declared type of `V`.
    #[must_use]
    pub(crate) fn bind<V: Param + 'q>(self, value: V) -> QueryScalar<'q, T> {
        QueryScalar {
            query: self.query.bind(value),
            output: PhantomData,
        }
    }

    /// The first column of the first row.
    ///
    /// Errors: [`Error::RowNotFound`] when there is no row; [`Error::Decode`];
    /// [`Error::Database`], [`Error::Driver`], or a checkout error of a pool.
    pub(crate) async fn fetch_one<'t>(self, target: impl Into<Target<'t>>) -> Result<T, Error> {
        let row = self.query.fetch_one(target).await?;
        column::<T>(&row, 0)
    }

    /// The first column of the first row, or `None` when there is no row.
    ///
    /// Errors: [`Error::Decode`]; [`Error::Database`], [`Error::Driver`], or a checkout error of
    /// a pool.
    pub(crate) async fn fetch_optional<'t>(
        self,
        target: impl Into<Target<'t>>,
    ) -> Result<Option<T>, Error> {
        match self.query.fetch_optional(target).await? {
            Some(row) => Ok(Some(column::<T>(&row, 0)?)),
            None => Ok(None),
        }
    }

    /// The first column of every row.
    ///
    /// Errors: [`Error::Decode`]; [`Error::Database`], [`Error::Driver`], or a checkout error of
    /// a pool.
    pub(crate) async fn fetch_all<'t>(
        self,
        target: impl Into<Target<'t>>,
    ) -> Result<Vec<T>, Error> {
        let rows = self.query.fetch_all(target).await?;
        rows.iter().map(|row| column::<T>(row, 0)).collect()
    }
}

/// One statement whose rows the test reads as `T` ([`FromTestRow`]).
pub(crate) struct QueryAs<'q, T> {
    query: Query<'q>,
    output: PhantomData<fn() -> T>,
}

/// A statement of `sql` whose rows the test reads as `T`.
pub(crate) fn query_as<T: FromTestRow>(sql: &str) -> QueryAs<'_, T> {
    QueryAs {
        query: query(sql),
        output: PhantomData,
    }
}

impl<'q, T: FromTestRow> QueryAs<'q, T> {
    /// Binds the next parameter, with the declared type of `V`.
    #[must_use]
    pub(crate) fn bind<V: Param + 'q>(self, value: V) -> QueryAs<'q, T> {
        QueryAs {
            query: self.query.bind(value),
            output: PhantomData,
        }
    }

    /// The first row.
    ///
    /// Errors: [`Error::RowNotFound`] when there is no row; [`Error::Decode`];
    /// [`Error::Database`], [`Error::Driver`], or a checkout error of a pool.
    pub(crate) async fn fetch_one<'t>(self, target: impl Into<Target<'t>>) -> Result<T, Error> {
        let row = self.query.fetch_one(target).await?;
        T::from_row(&row)
    }

    /// The first row, or `None` when there is no row.
    ///
    /// Errors: [`Error::Decode`]; [`Error::Database`], [`Error::Driver`], or a checkout error of
    /// a pool.
    pub(crate) async fn fetch_optional<'t>(
        self,
        target: impl Into<Target<'t>>,
    ) -> Result<Option<T>, Error> {
        match self.query.fetch_optional(target).await? {
            Some(row) => Ok(Some(T::from_row(&row)?)),
            None => Ok(None),
        }
    }

    /// Every row.
    ///
    /// Errors: [`Error::Decode`]; [`Error::Database`], [`Error::Driver`], or a checkout error of
    /// a pool.
    pub(crate) async fn fetch_all<'t>(
        self,
        target: impl Into<Target<'t>>,
    ) -> Result<Vec<T>, Error> {
        let rows = self.query.fetch_all(target).await?;
        rows.iter().map(T::from_row).collect()
    }
}

/// A text of statements with no parameters, sent as one simple-query
/// message: every statement runs, in order, as psql runs a file.
pub(crate) struct RawSql<'q> {
    sql: &'q str,
}

/// The text `sql`, run as one simple-query message.
pub(crate) fn raw_sql(sql: &str) -> RawSql<'_> {
    RawSql { sql }
}

impl RawSql<'_> {
    /// Runs the text; returns the sum of the command tag counts of its
    /// statements.
    ///
    /// Errors: [`Error::Database`] at the first statement that fails (the
    /// statements before it ran), or a checkout error of a pool.
    pub(crate) async fn execute<'t>(
        self,
        target: impl Into<Target<'t>>,
    ) -> Result<Executed, Error> {
        let rows_affected = target.into().script(self.sql).await?;
        Ok(Executed { rows_affected })
    }
}

/// A test database that a fixture reaches by its URL.
pub(crate) trait TestDatabaseUrl {
    /// The URL of the database.
    fn test_url(&self) -> String;
}

impl TestDatabaseUrl for TestPool {
    fn test_url(&self) -> String {
        self.url().to_owned()
    }
}

impl<T: TestDatabaseUrl + ?Sized> TestDatabaseUrl for &T {
    fn test_url(&self) -> String {
        (**self).test_url()
    }
}

/// The host and the port of a test database URL
/// (`postgresql://user:password@host:port/database?query`): the port is
/// 5432 when the URL names none.
///
/// # Panics
///
/// When the URL has no scheme or its port is not a number: a defect of the
/// test.
pub(crate) fn endpoint_of(url: &str) -> (String, u16) {
    let (_scheme, rest) = url.split_once("://").expect("the test URL has a scheme");
    let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let host_port = authority
        .rsplit_once('@')
        .map_or(authority, |(_, after)| after);
    let (host, port) = match host_port.rsplit_once(':') {
        Some((host, port)) if !host.ends_with(':') && !port.ends_with(']') => (host, port),
        Some(_) | None => (host_port, ""),
    };
    let port = match port {
        "" => 5432,
        digits => digits
            .parse()
            .expect("the port of the test URL is a number"),
    };
    (host.trim_matches(['[', ']']).to_owned(), port)
}

/// `url` with its host and port replaced; the user, the password, the
/// database and the query are kept.
///
/// # Panics
///
/// When the URL has no scheme: a defect of the test.
pub(crate) fn with_endpoint(url: &str, host: &str, port: u16) -> String {
    let (scheme, rest) = url.split_once("://").expect("the test URL has a scheme");
    let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
    let (authority, remainder) = rest.split_at(authority_end);
    match authority.rsplit_once('@') {
        Some((user_info, _)) => format!("{scheme}://{user_info}@{host}:{port}{remainder}"),
        None => format!("{scheme}://{host}:{port}{remainder}"),
    }
}

/// Opens one session to `url` through the core's URL parser and connect
/// path. Its driver runs as a task of the current runtime.
async fn open_session(url: &str) -> Result<(Client, DriverConnection), Error> {
    let environment = ConnectionEnvironment::capture()?;
    let settings = parse_connection_url(url, &environment)?;
    let plan = ConnectPlan::load(&settings, &environment, &DEFAULT_SOCKET_SETTINGS)?;
    Ok(open_connection(&plan).await?)
}

/// What a session owes before its next statement: the end of each
/// transaction that the test dropped open, innermost first.
#[derive(Debug, Default)]
struct DroppedEnds {
    statements: Vec<String>,
}

/// One session to a test database.
pub(crate) struct TestConnection {
    client: Client,
    /// Open transactions: 0 outside a transaction, 1 inside `BEGIN`, one
    /// more for each savepoint.
    depth: u32,
    dropped: DroppedEnds,
}

impl TestConnection {
    /// Opens a session to `url`.
    ///
    /// Errors: [`Error::Url`], [`Error::Environment`], [`Error::Plan`],
    /// [`Error::Connect`].
    pub(crate) async fn connect(url: &str) -> Result<TestConnection, Error> {
        let (client, connection) = open_session(url).await?;
        drop(tokio::spawn(async move {
            let _ended = connection.await;
        }));
        Ok(TestConnection {
            client,
            depth: 0,
            dropped: DroppedEnds::default(),
        })
    }

    /// Ends the session.
    pub(crate) async fn close(self) {
        drop(self);
    }

    /// Opens a transaction on this session: `BEGIN`, or a savepoint when a
    /// transaction is open.
    ///
    /// Errors: [`Error::TooDeep`] past [`MAX_TRANSACTION_DEPTH`];
    /// [`Error::Database`].
    pub(crate) async fn begin(&mut self) -> Result<TestTransaction<'_>, Error> {
        self.open_transaction().await?;
        Ok(TestTransaction {
            held: HeldSession::Borrowed(self),
            end: TransactionEnd::Open,
        })
    }

    async fn open_transaction(&mut self) -> Result<(), Error> {
        self.send_dropped_ends().await?;
        if self.depth >= MAX_TRANSACTION_DEPTH {
            return Err(Error::TooDeep);
        }
        let statement = match self.depth {
            0 => "BEGIN".to_owned(),
            depth => format!("SAVEPOINT horsies_test_savepoint_{depth}"),
        };
        self.client.batch_execute(&statement).await?;
        self.depth += 1;
        Ok(())
    }

    /// The statement that commits (`release`) or rolls back the innermost
    /// open transaction.
    fn end_statement(&self, end: TransactionClose) -> String {
        let savepoint = self.depth - 1;
        match (end, self.depth) {
            (TransactionClose::Commit, 1) => "COMMIT".to_owned(),
            (TransactionClose::Rollback, 1) => "ROLLBACK".to_owned(),
            (TransactionClose::Commit, _) => {
                format!("RELEASE SAVEPOINT horsies_test_savepoint_{savepoint}")
            }
            (TransactionClose::Rollback, _) => {
                format!("ROLLBACK TO SAVEPOINT horsies_test_savepoint_{savepoint}")
            }
        }
    }

    async fn close_transaction(&mut self, end: TransactionClose) -> Result<(), Error> {
        self.send_dropped_ends().await?;
        let statement = self.end_statement(end);
        self.depth -= 1;
        self.client.batch_execute(&statement).await?;
        Ok(())
    }

    /// Records the rollback of the innermost transaction, which the test
    /// dropped open; it is sent before the next statement of the session.
    fn drop_transaction(&mut self) {
        let statement = self.end_statement(TransactionClose::Rollback);
        self.depth -= 1;
        self.dropped.statements.push(statement);
    }

    /// Sends the ends of the transactions that the test dropped open.
    async fn send_dropped_ends(&mut self) -> Result<(), Error> {
        let statements = std::mem::take(&mut self.dropped.statements);
        for statement in statements {
            self.client.batch_execute(&statement).await?;
        }
        Ok(())
    }

    async fn rows(&mut self, sql: &str, params: &[Bound<'_>]) -> Result<Vec<Row>, Error> {
        self.send_dropped_ends().await?;
        let typed = typed_params(params);
        Ok(self.client.query_typed(sql, &typed).await?)
    }

    async fn count(&mut self, sql: &str, params: &[Bound<'_>]) -> Result<u64, Error> {
        self.send_dropped_ends().await?;
        let typed = typed_params(params);
        Ok(self.client.execute_typed(sql, &typed).await?)
    }

    async fn script(&mut self, sql: &str) -> Result<u64, Error> {
        self.send_dropped_ends().await?;
        let messages = self.client.simple_query(sql).await?;
        Ok(command_count(&messages))
    }
}

/// The sum of the command tag counts of a simple query's messages.
fn command_count(messages: &[SimpleQueryMessage]) -> u64 {
    messages
        .iter()
        .map(|message| match message {
            SimpleQueryMessage::CommandComplete(count) => *count,
            _ => 0,
        })
        .sum()
}

/// The values of `params` with their declared types, as the driver takes
/// them.
fn typed_params<'p>(params: &'p [Bound<'_>]) -> Vec<(&'p (dyn ToSql + Sync), Type)> {
    params
        .iter()
        .map(|bound| {
            let value: &(dyn ToSql + Sync) = &*bound.value;
            (value, bound.declared.clone())
        })
        .collect()
}

/// How the test ends a transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransactionClose {
    Commit,
    Rollback,
}

/// Whether the test ended a transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransactionEnd {
    Open,
    Ended,
}

/// The session of a transaction: a session that the transaction checked out
/// of a pool, or the test's own session.
enum HeldSession<'c> {
    Pooled(Box<PooledConnection>),
    Borrowed(&'c mut TestConnection),
}

/// An open transaction. [`TestTransaction::commit`] and
/// [`TestTransaction::rollback`] end it. A transaction that the test drops
/// open is rolled back: a session of a pool is closed (the server rolls the
/// transaction back), the test's own session sends the rollback before its
/// next statement.
pub(crate) struct TestTransaction<'c> {
    held: HeldSession<'c>,
    end: TransactionEnd,
}

impl TestTransaction<'_> {
    fn session(&mut self) -> &mut TestConnection {
        match &mut self.held {
            HeldSession::Pooled(connection) => connection.session(),
            HeldSession::Borrowed(connection) => connection,
        }
    }

    /// Commits the transaction (`COMMIT`, or the release of its savepoint).
    ///
    /// Errors: [`Error::Database`]; the outcome is then unknown.
    pub(crate) async fn commit(mut self) -> Result<(), Error> {
        self.end = TransactionEnd::Ended;
        self.session()
            .close_transaction(TransactionClose::Commit)
            .await
    }

    /// Rolls the transaction back (`ROLLBACK`, or the rollback to its
    /// savepoint).
    ///
    /// Errors: [`Error::Database`].
    pub(crate) async fn rollback(mut self) -> Result<(), Error> {
        self.end = TransactionEnd::Ended;
        self.session()
            .close_transaction(TransactionClose::Rollback)
            .await
    }
}

impl Deref for TestTransaction<'_> {
    type Target = TestConnection;

    fn deref(&self) -> &TestConnection {
        match &self.held {
            HeldSession::Pooled(connection) => connection,
            HeldSession::Borrowed(connection) => connection,
        }
    }
}

impl DerefMut for TestTransaction<'_> {
    fn deref_mut(&mut self) -> &mut TestConnection {
        self.session()
    }
}

impl Drop for TestTransaction<'_> {
    fn drop(&mut self) {
        match (self.end, &mut self.held) {
            (TransactionEnd::Ended, _) => {}
            (TransactionEnd::Open, HeldSession::Pooled(connection)) => connection.discard(),
            (TransactionEnd::Open, HeldSession::Borrowed(connection)) => {
                connection.drop_transaction();
            }
        }
    }
}

/// The settings of a [`TestPool`].
#[derive(Clone, Debug)]
pub(crate) struct TestPoolOptions {
    max_connections: u32,
    acquire_timeout: Duration,
    session_setup: Vec<String>,
}

impl Default for TestPoolOptions {
    fn default() -> TestPoolOptions {
        TestPoolOptions::new()
    }
}

impl TestPoolOptions {
    /// [`DEFAULT_MAX_CONNECTIONS`] sessions, [`DEFAULT_ACQUIRE_TIMEOUT`], no
    /// session setup.
    pub(crate) fn new() -> TestPoolOptions {
        TestPoolOptions {
            max_connections: DEFAULT_MAX_CONNECTIONS,
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
            session_setup: Vec::new(),
        }
    }

    /// At most `max_connections` open sessions.
    #[must_use]
    pub(crate) fn max_connections(mut self, max_connections: u32) -> TestPoolOptions {
        self.max_connections = max_connections;
        self
    }

    /// The longest wait for a free session.
    #[must_use]
    pub(crate) fn acquire_timeout(mut self, acquire_timeout: Duration) -> TestPoolOptions {
        self.acquire_timeout = acquire_timeout;
        self
    }

    /// A text that each new session runs once, before its first statement
    /// (for example `SET TIME ZONE 'Asia/Tokyo'`).
    #[must_use]
    pub(crate) fn session_setup(mut self, sql: impl Into<String>) -> TestPoolOptions {
        self.session_setup.push(sql.into());
        self
    }

    /// A pool on `url` that has opened one session, so a refused URL or an
    /// unreachable server fails here.
    ///
    /// Errors: the connect errors of [`TestConnection::connect`];
    /// [`Error::Database`] from the session setup.
    pub(crate) async fn connect(self, url: &str) -> Result<TestPool, Error> {
        let pool = self.connect_lazy(url);
        let first = pool.acquire().await?;
        drop(first);
        Ok(pool)
    }

    /// A pool on `url` that opens its first session at its first checkout.
    pub(crate) fn connect_lazy(self, url: &str) -> TestPool {
        TestPool {
            shared: Arc::new(PoolShared {
                url: url.to_owned(),
                idle: Mutex::new(Vec::new()),
                permits: Arc::new(Semaphore::new(self.max_connections as usize)),
                closed: AtomicBool::new(false),
                options: self,
            }),
        }
    }
}

struct PoolShared {
    url: String,
    /// Returned sessions, the last returned at the end. At most
    /// `max_connections` (one per permit).
    idle: Mutex<Vec<TestConnection>>,
    /// One permit per checked-out session. Closed when the pool has closed.
    permits: Arc<Semaphore>,
    /// Set by [`TestPool::close`]: checkouts are refused from then on.
    closed: AtomicBool,
    options: TestPoolOptions,
}

/// Sessions on one test database. Cloning shares the sessions.
#[derive(Clone)]
pub(crate) struct TestPool {
    shared: Arc<PoolShared>,
}

impl TestPool {
    /// A pool on `url` with the default options, that has opened one
    /// session.
    ///
    /// Errors: see [`TestPoolOptions::connect`].
    pub(crate) async fn connect(url: &str) -> Result<TestPool, Error> {
        TestPoolOptions::new().connect(url).await
    }

    /// The URL of the pool's database.
    pub(crate) fn url(&self) -> &str {
        &self.shared.url
    }

    /// Checks a session out until the returned guard drops.
    ///
    /// Errors: [`Error::PoolTimedOut`] when no session came free within the
    /// checkout bound; [`Error::PoolClosed`]; the connect errors of
    /// [`TestConnection::connect`] and [`Error::Database`] of the session
    /// setup when a new session opens.
    pub(crate) async fn acquire(&self) -> Result<PooledConnection, Error> {
        if self.is_closed() {
            return Err(Error::PoolClosed);
        }
        let bound = self.shared.options.acquire_timeout;
        let permit =
            match tokio::time::timeout(bound, Arc::clone(&self.shared.permits).acquire_owned())
                .await
            {
                Err(_elapsed) => return Err(Error::PoolTimedOut(bound)),
                Ok(Err(_closed)) => return Err(Error::PoolClosed),
                Ok(Ok(permit)) => permit,
            };
        if self.is_closed() {
            return Err(Error::PoolClosed);
        }
        let session = match self.take_idle() {
            Some(session) => session,
            None => self.open_new_session().await?,
        };
        Ok(PooledConnection {
            session: PooledSession::Held(session),
            shared: Arc::clone(&self.shared),
            _permit: permit,
        })
    }

    /// The last returned session whose driver still runs. Sessions whose
    /// driver ended are dropped. Bounded by the idle list, at most
    /// `max_connections` long.
    fn take_idle(&self) -> Option<TestConnection> {
        let mut idle = self
            .shared
            .idle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        while let Some(session) = idle.pop() {
            match session.client.is_closed() {
                false => return Some(session),
                true => drop(session),
            }
        }
        None
    }

    async fn open_new_session(&self) -> Result<TestConnection, Error> {
        let mut session = TestConnection::connect(&self.shared.url).await?;
        for sql in &self.shared.options.session_setup {
            let _ran = session.script(sql).await?;
        }
        Ok(session)
    }

    /// Opens a transaction on a session checked out for it.
    ///
    /// Errors: the errors of [`TestPool::acquire`]; [`Error::Database`].
    pub(crate) async fn begin(&self) -> Result<TestTransaction<'static>, Error> {
        let mut connection = self.acquire().await?;
        connection.session().open_transaction().await?;
        Ok(TestTransaction {
            held: HeldSession::Pooled(Box::new(connection)),
            end: TransactionEnd::Open,
        })
    }

    /// Closes the pool, as sqlx's close did: later checkouts fail with
    /// [`Error::PoolClosed`], the idle sessions end now, and the call
    /// returns once every checked-out session was returned (and ended). A
    /// caller that may hold a session bounds the wait itself.
    pub(crate) async fn close(&self) {
        self.shared.closed.store(true, Ordering::SeqCst);
        drop(self.take_all_idle());
        let max = self.shared.options.max_connections;
        let returned = Arc::clone(&self.shared.permits)
            .acquire_many_owned(max)
            .await;
        drop(returned);
        self.shared.permits.close();
        drop(self.take_all_idle());
    }

    fn take_all_idle(&self) -> Vec<TestConnection> {
        std::mem::take(
            &mut *self
                .shared
                .idle
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    /// Whether [`TestPool::close`] ran.
    pub(crate) fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::SeqCst)
    }

    /// The open sessions: checked out and idle.
    pub(crate) fn size(&self) -> u32 {
        let checked_out = self
            .shared
            .options
            .max_connections
            .saturating_sub(self.shared.permits.available_permits() as u32);
        checked_out.saturating_add(self.num_idle() as u32)
    }

    /// The idle sessions.
    pub(crate) fn num_idle(&self) -> usize {
        self.shared
            .idle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

/// Whether a checked-out session goes back to its pool.
enum PooledSession {
    /// The session, returned to the pool when the guard drops.
    Held(TestConnection),
    /// The session was closed: a transaction was dropped open on it.
    Discarded,
}

/// A session checked out of a [`TestPool`]. Returned to the pool when it
/// drops, unless the pool was closed or the session's driver ended.
pub(crate) struct PooledConnection {
    session: PooledSession,
    shared: Arc<PoolShared>,
    _permit: OwnedSemaphorePermit,
}

impl PooledConnection {
    fn session(&mut self) -> &mut TestConnection {
        match &mut self.session {
            PooledSession::Held(session) => session,
            PooledSession::Discarded => {
                panic!("a discarded test session was used: a defect of the test client")
            }
        }
    }

    /// Closes the session instead of returning it.
    fn discard(&mut self) {
        self.session = PooledSession::Discarded;
    }
}

impl Deref for PooledConnection {
    type Target = TestConnection;

    fn deref(&self) -> &TestConnection {
        match &self.session {
            PooledSession::Held(session) => session,
            PooledSession::Discarded => {
                panic!("a discarded test session was used: a defect of the test client")
            }
        }
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut TestConnection {
        self.session()
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        let session = std::mem::replace(&mut self.session, PooledSession::Discarded);
        match session {
            PooledSession::Discarded => {}
            PooledSession::Held(session) => {
                match self.shared.closed.load(Ordering::SeqCst) || session.client.is_closed() {
                    true => drop(session),
                    false => self
                        .shared
                        .idle
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .push(session),
                }
            }
        }
    }
}

/// `LISTEN` on a session of its own. Notifications arrive in order; a
/// notification sent before [`TestListener::listen`] returned for its
/// channel is not received.
pub(crate) struct TestListener {
    client: Client,
    notifications: mpsc::Receiver<Notification>,
}

impl TestListener {
    /// Opens the listener's session on the database of `pool`.
    ///
    /// Errors: the connect errors of [`TestConnection::connect`].
    pub(crate) async fn connect_with(pool: &TestPool) -> Result<TestListener, Error> {
        TestListener::connect(pool.url()).await
    }

    /// Opens the listener's session on `url`.
    ///
    /// Errors: the connect errors of [`TestConnection::connect`].
    pub(crate) async fn connect(url: &str) -> Result<TestListener, Error> {
        let (client, mut connection) = open_session(url).await?;
        let (sender, notifications) = mpsc::channel(LISTENER_QUEUE_CAPACITY);
        drop(tokio::spawn(async move {
            // Runs while the session is open: the driver returns `None` or an
            // error when the session ends, and the send fails when the test
            // dropped the listener.
            while let Some(Ok(message)) =
                std::future::poll_fn(|context| connection.poll_message(context)).await
            {
                // A notice, or a message of a later driver version, is skipped.
                let AsyncMessage::Notification(notification) = message else {
                    continue;
                };
                if sender.send(notification).await.is_err() {
                    break;
                }
            }
        }));
        Ok(TestListener {
            client,
            notifications,
        })
    }

    /// `LISTEN` on `channel` (quoted as an identifier).
    ///
    /// Errors: [`Error::Database`].
    pub(crate) async fn listen(&mut self, channel: &str) -> Result<(), Error> {
        let quoted = channel.replace('"', "\"\"");
        self.client
            .batch_execute(&format!("LISTEN \"{quoted}\""))
            .await?;
        Ok(())
    }

    /// `LISTEN` on each of `channels`.
    ///
    /// Errors: [`Error::Database`] at the first channel that fails.
    pub(crate) async fn listen_all(
        &mut self,
        channels: impl IntoIterator<Item = &str>,
    ) -> Result<(), Error> {
        for channel in channels {
            self.listen(channel).await?;
        }
        Ok(())
    }

    /// The next notification; waits for one.
    ///
    /// Errors: [`Error::Driver`] (closed) when the session ended.
    pub(crate) async fn recv(&mut self) -> Result<Notification, Error> {
        match self.notifications.recv().await {
            Some(notification) => Ok(notification),
            None => Err(Error::from(self.closed_error().await)),
        }
    }

    /// The next notification, or `None` when the session ended; waits for
    /// one.
    pub(crate) async fn try_recv(&mut self) -> Result<Option<Notification>, Error> {
        Ok(self.notifications.recv().await)
    }

    /// The driver's error for a statement on the ended session.
    async fn closed_error(&self) -> tokio_postgres::Error {
        match self.client.batch_execute("").await {
            Err(error) => error,
            Ok(()) => panic!("the listener's session answered after its driver ended"),
        }
    }
}

#[cfg(test)]
mod tests;

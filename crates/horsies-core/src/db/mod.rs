//! The statement layer of the core: tokio-postgres with deadpool, behind one
//! module.
//!
//! Path of a statement:
//! 1. [`parse_connection_url`] turns the URL text into [`ConnectionSettings`]
//!    (pure). It behaves as libpq, or refuses a setting by name; it never
//!    drops a setting that libpq would apply.
//! 2. [`ConnectPlan::load`] reads the TLS files (named, or libpq's defaults
//!    under `~/.postgresql/`), `~/.pgpass` and the process user name once,
//!    and applies the core's [`SocketSettings`].
//! 3. [`CorePool::new`] builds one pool per [`PoolRole`], with its reserve,
//!    idle ping and [`StatementProtocol`].
//! 4. A [`TypedStatement`] is bound with [`TypedStatement::bind`] and run with
//!    [`CorePool::query`] / [`CorePool::execute`], or on a checked-out
//!    [`PooledConnection`].
//! 5. A failure is one of the three outcomes of [`StatementFailure`], each
//!    with a [`Transience`].
//! 6. [`PooledConnection::begin`] opens a [`Transaction`]; its `COMMIT` has
//!    the same three outcomes. A [`SqlScript`] (a migration text: several
//!    statements, no parameters) runs in a transaction with
//!    [`Transaction::run_script`].
//! 7. A [`PartitionDdl`] (a leaf detach, built only from checked
//!    [`SqlIdentifier`]s) runs in autocommit with
//!    [`PooledConnection::run_partition_ddl`].
//!
//! The listener ([`Listener`]) holds one dedicated connection on the session
//! URL for every LISTEN of the process. A lock session ([`LockSession`])
//! holds one dedicated connection with a session advisory lock.

mod connect;
mod ddl;
mod environment;
mod failure;
mod listener;
mod pool;
mod pooling_check;
mod session_lock;
mod statement;
mod tls;
mod transaction;
mod url;

pub use connect::{
    open_connection, ConnectPlan, ConnectPlanError, DriverConnection, Keepalives, SocketSettings,
    TcpUserTimeout, DEFAULT_KEEPALIVES_IDLE, DEFAULT_KEEPALIVES_INTERVAL,
    DEFAULT_KEEPALIVES_PROBES, DEFAULT_SOCKET_SETTINGS, DEFAULT_TCP_USER_TIMEOUT,
};
pub use ddl::{PartitionDdl, SqlIdentifier, SqlIdentifierError, MAX_IDENTIFIER_BYTES};
pub use environment::{
    lookup_password, parse_password_file, read_password_file, variable_rule, ConnectionEnvironment,
    EnvironmentError, EnvironmentPassword, HomeDirectory, PasswordFileEntry, PasswordFileError,
    PasswordFileMatch, PasswordFileRead, PasswordLookup, VariableRule, LIBPQ_VARIABLES,
    PASSWORD_VARIABLE,
};
pub use failure::{
    error_scope, sqlstate_code_transience, sqlstate_transience, CancelRequestOutcome,
    CheckoutFailure, ConnectError, ConnectFailure, DbErrorText, ErrorScope, NotSentCause,
    PoolingCheckFailure, RefusalCause, StatementFailure, Transience, UnknownOutcomeCause,
    TRANSIENT_SQLSTATE_CLASSES,
};
pub use listener::{
    ChannelName, ChannelNameError, Listener, ListenerClose, ListenerStartFailure, PayloadEvent,
    PayloadFilter, PayloadSubscription, QueueCapacity, QueueCapacityError, SubscribeError,
    WakeEvent, WakeSubscription, DEFAULT_PAYLOAD_QUEUE_CAPACITY, MAX_CHANNEL_NAME_BYTES,
    MAX_SUBSCRIPTIONS,
};
pub use pool::{
    CheckoutClass, ConnectionAge, CorePool, IdlePing, KeptOpen, PoolPlan, PoolRole, PoolSizing,
    PoolSizingError, PoolStatus, PooledConnection, Reserve, DEFAULT_IDLE_PING_AFTER,
    IDLE_PING_BOUND, KEPT_OPEN_CHECK_INTERVAL, KEPT_SESSION_SETUP_SQL, POSTGRES_MAX_BACKENDS,
    WORKER_RESERVED_CONNECTIONS,
};
pub use pooling_check::STATEMENT_POOLING_CHECK_BOUND;
pub use session_lock::{
    LockSession, LockTake, SessionEnd, SessionLockFailure, SESSION_LOCK_RECONNECT_FIRST,
    SESSION_LOCK_RECONNECT_LIMIT, SESSION_LOCK_STATEMENT_BOUND,
};
pub use statement::{
    BoundStatement, ParameterBinder, ParameterType, SqlScript, StatementBound, StatementProtocol,
    TypedStatement, CANCEL_GRACE, CANCEL_REQUEST_BOUND,
};
pub use tls::{
    plan_tls, probe_default_tls_files, AttemptOrder, ClientFile, ClientIdentity, DefaultFile,
    DefaultTlsFiles, FileRoots, RootCertificates, RootSource, ServerVerification, SslMode,
    SslModeSource, StrictRoots, TlsFile, TlsLoadError, TlsMode, TlsNegotiation, TlsPlan, TlsPolicy,
    TlsPolicyError, TlsSettings, DEFAULT_CLIENT_CERTIFICATE_FILE, DEFAULT_CLIENT_KEY_FILE,
    DEFAULT_ROOT_CERTIFICATE_FILE, DEFAULT_ROOT_CRL_FILE,
};
pub use transaction::{DiscardReason, IsolationLevel, RollbackOutcome, Transaction};
pub use url::{
    parse_connection_url, ChannelBindingPolicy, ConnectionSettings, DatabaseName, DatabasePassword,
    Endpoint, HostAddress, HostOrder, MaskedUrl, TargetSession, UnsupportedReason, UrlComponent,
    UrlError, UrlPassword, UserName, DEFAULT_CONNECT_TIMEOUT, DEFAULT_PORT, MAX_HOSTS,
};

/// Re-exported driver types that statement callers read.
pub use tokio_postgres::types::{FromSql, ToSql};
pub use tokio_postgres::Row;

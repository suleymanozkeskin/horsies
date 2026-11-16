//! The statement layer of the core: tokio-postgres with deadpool, behind one
//! module.
//!
//! Path of a statement:
//! 1. [`parse_connection_url`] turns the URL text into [`ConnectionSettings`]
//!    (pure; every unsupported setting is refused by name).
//! 2. [`ConnectPlan::load`] reads the TLS files, `~/.pgpass` and the process
//!    user name once, and applies the core's [`SocketSettings`].
//! 3. [`CorePool::new`] builds one pool per [`PoolRole`], with its reserve,
//!    idle ping and [`StatementProtocol`].
//! 4. A [`TypedStatement`] is bound with [`TypedStatement::bind`] and run with
//!    [`CorePool::query`] / [`CorePool::execute`], or on a checked-out
//!    [`PooledConnection`].
//! 5. A failure is one of the three outcomes of [`StatementFailure`], each
//!    with a [`Transience`].
//!
//! The listener ([`Listener`]) holds one dedicated connection on the session
//! URL for every LISTEN of the process.
//!
//! The module stands next to the sqlx broker; call sites move to it module by
//! module.

mod connect;
mod environment;
mod failure;
mod listener;
mod pool;
mod pooling_check;
mod statement;
mod tls;
mod url;

pub use connect::{
    open_connection, ConnectPlan, ConnectPlanError, DriverConnection, Keepalives, SocketSettings,
    TcpUserTimeout, DEFAULT_KEEPALIVES_IDLE, DEFAULT_KEEPALIVES_INTERVAL,
    DEFAULT_KEEPALIVES_PROBES, DEFAULT_SOCKET_SETTINGS, DEFAULT_TCP_USER_TIMEOUT,
};
pub use environment::{
    lookup_password, parse_password_file, read_password_file, ConnectionEnvironment,
    EnvironmentError, EnvironmentPassword, HomeDirectory, PasswordFileEntry, PasswordFileError,
    PasswordFileMatch, PasswordFileRead, PasswordLookup, PASSWORD_VARIABLE, REFUSED_VARIABLES,
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
    CheckoutClass, ConnectionAge, CorePool, IdlePing, PoolPlan, PoolRole, PoolSizing,
    PoolSizingError, PoolStatus, PooledConnection, Reserve, DEFAULT_IDLE_PING_AFTER,
    IDLE_PING_BOUND, POSTGRES_MAX_BACKENDS, WORKER_RESERVED_CONNECTIONS,
};
pub use pooling_check::STATEMENT_POOLING_CHECK_BOUND;
pub use statement::{
    BoundStatement, ParameterBinder, ParameterType, StatementBound, StatementProtocol,
    TypedStatement, CANCEL_GRACE,
};
pub use tls::{
    ClientIdentity, RootCertificates, RootSource, ServerVerification, SslMode, SslModeSource,
    TlsFile, TlsLoadError, TlsNegotiation, TlsPolicy, TlsPolicyError, TlsSettings,
};
pub use url::{
    parse_connection_url, ChannelBindingPolicy, ConnectionSettings, DatabaseName, DatabasePassword,
    Endpoint, HostAddress, HostOrder, MaskedUrl, TargetSession, UnsupportedReason, UrlComponent,
    UrlError, UrlPassword, UserName, DEFAULT_CONNECT_TIMEOUT, DEFAULT_PORT, MAX_HOSTS,
};

/// Re-exported driver types that statement callers read.
pub use tokio_postgres::types::{FromSql, ToSql};
pub use tokio_postgres::Row;

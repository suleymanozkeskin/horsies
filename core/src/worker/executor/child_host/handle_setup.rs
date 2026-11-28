//! The core handle of a Python child, from its init frame
//! (`CoreChildHandle.from_init`): the producer manifest of the child's pool
//! and its pool profile.
//!
//! The pool is the one the Rust child host connects from the same frame
//! (`host::child_broker_config`, `host::kept_open_of`): the URL and the
//! connection settings of the parent's config, up to `max_size` connections
//! with no overflow, `min_size` kept open, `check` as the ping at checkout,
//! Python's checkout wait and connection age of a child pool, and no schema
//! work. The database URL never becomes a Python value of the child.

use super::host::{
    kept_open_of, pool_of, OwnWritesPool, CHILD_POOL_CHECKOUT_WAIT_SECONDS, CHILD_POOL_MAX_AGE,
};
use crate::broker::PoolProfile;
use crate::core::history::identity::keys::IDEMPOTENCY_WINDOW_DEFAULT_MICROS;
use crate::manifest::{
    DatabaseManifest, DatabaseUrl, ManifestVersion, PoolConnections, PoolSize, ProducerManifest,
    ReservationWindowMicros, SchemaMode, SendManifest, SessionDatabase, WholeSeconds,
};
use crate::worker::executor::protocol::{InitFrame, ServerSessions};

/// What the core handle of a worker's child connects with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildHandleSetup {
    /// The producer manifest of the child's pool. Its send section is the
    /// producer default: the child handle sends no task.
    pub manifest: ProducerManifest,
    /// The pool of the child kind: `min_size` connections kept open, and
    /// with `check` a ping of every pooled connection at checkout.
    pub profile: PoolProfile,
}

/// The init frame of a handler child: it has no core handle, since it runs
/// no task and writes nothing through one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a handler child has no core handle: it runs no task")]
pub struct HandlerChildHasNoHandle;

/// The core handle setup of the task child that `init` boots. Pure.
///
/// Errors: [`HandlerChildHasNoHandle`] for the init frame of a handler
/// child.
pub fn child_handle_setup(init: &InitFrame) -> Result<ChildHandleSetup, HandlerChildHasNoHandle> {
    let pool = match pool_of(init.child_kind) {
        OwnWritesPool::Pool(pool) => pool,
        OwnWritesPool::NoOwnWrites => return Err(HandlerChildHasNoHandle),
    };
    let connections = u32::try_from(pool.max_size.get())
        .ok()
        .and_then(|count| PoolSize::try_from(count).ok())
        .expect("a child pool max_size is from 1 to MAX_CHILD_POOL_CONNECTIONS");
    let no_overflow = PoolConnections::try_from(0).expect("0 is a connection count");
    let (session_database, pgbouncer_transaction_mode) = match &init.connection.server_sessions {
        ServerSessions::Kept => (SessionDatabase::SameAsDatabaseUrl, false),
        ServerSessions::PgBouncerTransactionMode {
            session_database_url,
        } => (
            SessionDatabase::Separate {
                url: DatabaseUrl::new(session_database_url.as_str().to_owned()),
            },
            true,
        ),
    };
    let window = u64::try_from(IDEMPOTENCY_WINDOW_DEFAULT_MICROS)
        .ok()
        .and_then(|micros| ReservationWindowMicros::try_from(micros).ok())
        .expect("the default reservation window is inside its bounds");
    Ok(ChildHandleSetup {
        manifest: ProducerManifest {
            manifest_version: ManifestVersion::CURRENT,
            database: DatabaseManifest {
                database_url: DatabaseUrl::new(init.database_url.as_str().to_owned()),
                session_database,
                pgbouncer_transaction_mode,
                pool_pre_ping: pool.check,
                pool_size: connections,
                max_overflow: no_overflow,
                pool_timeout_seconds: WholeSeconds::from_secs(CHILD_POOL_CHECKOUT_WAIT_SECONDS),
                pool_recycle: CHILD_POOL_MAX_AGE,
                echo: false,
                tcp_keepalives: init.connection.tcp_keepalives,
                tcp_user_timeout: init.connection.tcp_user_timeout,
            },
            schema: SchemaMode::AssumeInitialized,
            send: SendManifest {
                retain_rerun_input_default: false,
                idempotency_reservation_window: window,
            },
        },
        profile: PoolProfile::Child {
            kept_open: kept_open_of(pool),
            check: pool.check,
        },
    })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;
    use crate::core::config::socket::{TcpKeepalivesConfig, TcpUserTimeoutConfig};
    use crate::db::KeptOpen;
    use crate::worker::executor::protocol::{
        ChildConnection, ChildKind, ChildLogLevel, ChildPool, ChildProgram, DatabaseUrl as InitUrl,
        HandlerChild, IsolatedChild, MemoryLimit, PoolMaxSize, PoolMinSize, SharedThreadChild,
        TaskSendPool, ThreadSeats,
    };

    fn init(child_kind: ChildKind, connection: ChildConnection) -> InitFrame {
        InitFrame {
            program: ChildProgram::Rust,
            log_level: ChildLogLevel::Info,
            database_url: InitUrl::new("postgresql://user:secret@db:5432/app".to_owned()),
            connection,
            process_name: "horsies-child".to_owned(),
            child_kind,
        }
    }

    fn pool(min_size: u64, max_size: u64, check: bool) -> ChildPool {
        ChildPool {
            min_size: PoolMinSize::new(min_size).expect("a min_size inside its bounds"),
            max_size: PoolMaxSize::new(max_size).expect("a max_size inside its bounds"),
            check,
        }
    }

    /// The pool of the child kind sizes the handle: `max_size` connections
    /// with no overflow, `min_size` kept open, `check` as the ping; the
    /// connection settings of the frame, Python's checkout wait and age of
    /// a child pool, and no schema work.
    #[test]
    fn the_handle_takes_the_pool_of_the_child_kind_and_the_connection_of_the_frame() {
        let setup = child_handle_setup(&init(
            ChildKind::Isolated(IsolatedChild {
                pool: pool(1, 3, true),
                task_send_pool: TaskSendPool::new(2).expect("a task-send pool"),
            }),
            ChildConnection::default(),
        ))
        .expect("an isolated child has a core handle");
        let database = &setup.manifest.database;
        assert_eq!(
            database.database_url.expose(),
            "postgresql://user:secret@db:5432/app"
        );
        assert_eq!(
            database.session_database,
            SessionDatabase::SameAsDatabaseUrl
        );
        assert!(!database.pgbouncer_transaction_mode);
        assert!(database.pool_pre_ping);
        assert_eq!(database.pool_size.get(), 3);
        assert_eq!(database.max_overflow.get(), 0);
        assert_eq!(
            database.pool_timeout_seconds.get(),
            CHILD_POOL_CHECKOUT_WAIT_SECONDS
        );
        assert_eq!(database.pool_recycle, CHILD_POOL_MAX_AGE);
        assert_eq!(database.tcp_keepalives, TcpKeepalivesConfig::default());
        assert_eq!(database.tcp_user_timeout, TcpUserTimeoutConfig::default());
        assert_eq!(setup.manifest.schema, SchemaMode::AssumeInitialized);
        assert_eq!(
            setup.profile,
            PoolProfile::Child {
                kept_open: KeptOpen::Connections(NonZeroU32::new(1).expect("1 is above 0")),
                check: true,
            }
        );
    }

    /// In PgBouncer transaction mode the handle names the session URL of
    /// the parent's config; a pool with no kept connection keeps none.
    #[test]
    fn transaction_mode_names_the_session_url_and_a_zero_min_size_keeps_none() {
        let connection = ChildConnection {
            tcp_keepalives: TcpKeepalivesConfig::Off,
            tcp_user_timeout: TcpUserTimeoutConfig::SystemDefault,
            server_sessions: ServerSessions::PgBouncerTransactionMode {
                session_database_url: InitUrl::new(
                    "postgresql://user:secret@direct/app".to_owned(),
                ),
            },
        };
        let setup = child_handle_setup(&init(
            ChildKind::SharedThread(SharedThreadChild {
                seats: ThreadSeats::new(4).expect("thread seats"),
                pool: pool(0, 2, false),
                task_send_pool: TaskSendPool::new(1).expect("a task-send pool"),
                recycle_above: MemoryLimit::Off,
            }),
            connection,
        ))
        .expect("a shared thread child has a core handle");
        let database = &setup.manifest.database;
        assert!(database.pgbouncer_transaction_mode);
        match &database.session_database {
            SessionDatabase::Separate { url } => {
                assert_eq!(url.expose(), "postgresql://user:secret@direct/app");
            }
            SessionDatabase::SameAsDatabaseUrl => panic!("transaction mode names its session URL"),
        }
        assert_eq!(database.tcp_keepalives, TcpKeepalivesConfig::Off);
        assert_eq!(
            database.tcp_user_timeout,
            TcpUserTimeoutConfig::SystemDefault
        );
        assert!(!database.pool_pre_ping);
        assert_eq!(
            setup.profile,
            PoolProfile::Child {
                kept_open: KeptOpen::NoConnections,
                check: false,
            }
        );
    }

    /// A handler child runs no task: its init frame gives no core handle.
    #[test]
    fn a_handler_child_has_no_core_handle() {
        let refused = child_handle_setup(&init(
            ChildKind::Handler(HandlerChild {
                task_send_pool: TaskSendPool::new(1).expect("a task-send pool"),
            }),
            ChildConnection::default(),
        ));
        assert_eq!(refused, Err(HandlerChildHasNoHandle));
    }
}

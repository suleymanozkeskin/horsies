//! The producer handle of a child: task code that sends a task or starts a
//! workflow sends through it (Python: `Horsies.set_task_send_pool` at the
//! child's start, then `get_broker()` in task code).
//!
//! The handle has the app's own producer settings, with the pool sized by
//! the `task_send_pool` of the child's kind from the `init` frame: up to that
//! many connections, no overflow (Python `PostgresConfig.with_exact_pool`).
//! The child host publishes it once, before the start hook and the first
//! run; it connects at its first operation.

use std::sync::{Arc, OnceLock};

use crate::broker::client_family::ClientFamily;
use crate::broker::PoolProfile;
use crate::handle::CoreConnection;
use crate::manifest::{PoolConnections, PoolSize, ProducerManifest};
use crate::worker::executor::protocol::{ChildKind, TaskSendPool};

static CHILD_PRODUCER: OnceLock<Arc<CoreConnection>> = OnceLock::new();

/// The producer handle of this process, if it is a worker child.
pub enum ChildProducer {
    /// The child host published the handle: task code sends through it.
    InChild(Arc<CoreConnection>),
    /// No child host runs in this process: the app sends through its own
    /// handle.
    NotInAChild,
}

/// The producer handle that the child host published in this process. Pure
/// read; starts nothing.
pub fn child_producer() -> ChildProducer {
    match CHILD_PRODUCER.get() {
        Some(handle) => ChildProducer::InChild(Arc::clone(handle)),
        None => ChildProducer::NotInAChild,
    }
}

/// The task-send pool of a child kind.
pub(super) fn task_send_pool_of(kind: ChildKind) -> TaskSendPool {
    match kind {
        ChildKind::Isolated(child) => child.task_send_pool,
        ChildKind::SharedAsync(child) => child.task_send_pool,
        ChildKind::SharedThread(child) => child.task_send_pool,
        ChildKind::Handler(child) => child.task_send_pool,
    }
}

/// `app` with a pool of exactly `connections`: `pool_size = connections`,
/// `max_overflow = 0`; every other setting kept. Pure.
pub(super) fn task_send_manifest(
    app: &ProducerManifest,
    connections: TaskSendPool,
) -> ProducerManifest {
    let size = u32::try_from(connections.get())
        .ok()
        .and_then(|count| PoolSize::try_from(count).ok())
        .expect("a task-send pool is from 1 to 1,000 connections, inside the pool size bound");
    let no_overflow = PoolConnections::try_from(0).expect("0 is a connection count");
    let mut manifest = app.clone();
    manifest.database.pool_size = size;
    manifest.database.max_overflow = no_overflow;
    manifest
}

/// Publish the producer handle of this child: `manifest` with the producer
/// pools, writing as `family`, the client family of the binary. Starts
/// nothing. The child host publishes once per process; a second publish is a
/// defect of the host and crashes loud.
pub(super) fn publish_child_producer(manifest: ProducerManifest, family: ClientFamily) {
    let handle = Arc::new(CoreConnection::new(manifest, PoolProfile::Producer, family));
    if CHILD_PRODUCER.set(handle).is_err() {
        panic!("the producer handle of this child was published twice");
    }
}

/// A complete producer manifest for tests of the child host.
#[cfg(test)]
pub(crate) fn test_producer_manifest() -> ProducerManifest {
    crate::manifest::parse_producer_manifest(
        r#"{
            "manifest_version": 1,
            "database": {
                "database_url": "postgresql://user:secret@127.0.0.1:1/none",
                "session_database": {"kind": "SAME_AS_DATABASE_URL"},
                "pgbouncer_transaction_mode": false,
                "pool_pre_ping": true,
                "pool_size": 5,
                "max_overflow": 10,
                "pool_timeout_seconds": 30,
                "pool_recycle": {"kind": "AFTER_SECONDS", "seconds": 1800},
                "echo": false,
                "tcp_keepalives": {
                    "kind": "ON",
                    "idle_seconds": 30,
                    "interval_seconds": 10,
                    "probes": 3
                },
                "tcp_user_timeout": {"kind": "AFTER", "milliseconds": 30000}
            },
            "schema": "MIGRATE_AND_VALIDATE",
            "send": {
                "retain_rerun_input_default": true,
                "idempotency_reservation_window": 86400000000
            }
        }"#,
    )
    .expect("the test manifest is complete")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Python `with_exact_pool`: the task-send pool size, no overflow, and
    /// every other setting of the app kept.
    #[test]
    fn the_task_send_manifest_is_the_app_s_with_an_exact_pool() {
        let app = test_producer_manifest();
        let connections = TaskSendPool::new(3).expect("3 is a task-send pool");
        let sized = task_send_manifest(&app, connections);
        assert_eq!(sized.database.pool_size.get(), 3);
        assert_eq!(sized.database.max_overflow.get(), 0);
        let mut expected = app.clone();
        expected.database.pool_size = sized.database.pool_size;
        expected.database.max_overflow = sized.database.max_overflow;
        assert_eq!(sized, expected);
        assert!(sized.send.retain_rerun_input_default);
    }

    /// No child host runs in a test process, so nothing is published.
    #[test]
    fn a_process_with_no_child_host_has_no_child_producer() {
        assert!(matches!(child_producer(), ChildProducer::NotInAChild));
    }
}

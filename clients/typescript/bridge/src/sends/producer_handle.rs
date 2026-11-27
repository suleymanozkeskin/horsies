//! The handle that a send or a task read goes through.
//!
//! - In a task child, task code sends and reads through the producer handle
//!   that the child host published (`horsies_core::child_producer`): the
//!   app's producer settings with a pool of the child's `task_send_pool`
//!   connections and no overflow. So the connections of the children stay
//!   inside the bound that the worker sets.
//! - In every other process, a call goes through the handle that it gives.
//! - The Rust client chooses its handle the same way for its sends, reads
//!   and workflow starts.

use std::sync::Arc;

use horsies_core::handle::CoreConnection as LazyHandle;
use horsies_core::{child_producer, ChildProducer};

use crate::handle::core_handle::{BrokerConnection, CoreHandle};

/// The handle that one send or task read goes through.
pub enum ProducerHandle {
    /// The producer handle of the child host: this process is a task child.
    ChildHost(Arc<LazyHandle>),
    /// The handle that the call gives: no child host runs in this process.
    Given(BrokerConnection),
}

impl ProducerHandle {
    /// The handle of a call on `given` in this process. Reads the core's
    /// slot of the child producer; starts nothing.
    pub fn of_process(given: &CoreHandle) -> Self {
        Self::chosen(child_producer(), given)
    }

    /// The child host's handle when `child` holds one, else the handle of
    /// `given`. Pure.
    pub fn chosen(child: ChildProducer, given: &CoreHandle) -> Self {
        match child {
            ChildProducer::InChild(handle) => Self::ChildHost(handle),
            ChildProducer::NotInAChild => Self::Given(given.connection()),
        }
    }

    /// The core's lazy handle of the choice.
    pub fn lazy_handle(&self) -> &LazyHandle {
        match self {
            Self::ChildHost(handle) => handle,
            Self::Given(connection) => connection.lazy_handle(),
        }
    }
}

#[cfg(test)]
mod tests {
    use horsies_core::manifest::parse_producer_manifest;
    use horsies_core::{ClientFamily, PoolProfile};

    use super::*;
    use crate::call::thrown::Envelope;
    use crate::handle::core_handle::{build_core_handle, HandleProfile};

    /// The producer manifest of the app: 5 connections and 10 more under
    /// load.
    const APP_MANIFEST: &str = r#"{
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
            "retain_rerun_input_default": false,
            "idempotency_reservation_window": 86400000000
        }
    }"#;

    fn app_handle() -> CoreHandle {
        match build_core_handle(APP_MANIFEST.to_owned(), HandleProfile::Producer) {
            Envelope::Value(handle) => handle,
            Envelope::Thrown(thrown) => panic!("the manifest is complete, got {thrown:?}"),
        }
    }

    /// A handle as the child host builds it: the app's settings with a pool
    /// of 2 connections and no overflow.
    fn child_host_handle() -> Arc<LazyHandle> {
        let manifest = parse_producer_manifest(
            &APP_MANIFEST
                .replace("\"pool_size\": 5", "\"pool_size\": 2")
                .replace("\"max_overflow\": 10", "\"max_overflow\": 0"),
        )
        .expect("the child manifest is complete");
        Arc::new(LazyHandle::new(
            manifest,
            PoolProfile::Producer,
            ClientFamily::TypeScript,
        ))
    }

    #[test]
    fn in_a_task_child_a_call_goes_through_the_child_host_s_handle() {
        let app = app_handle();
        let child = child_host_handle();
        let chosen = ProducerHandle::chosen(ChildProducer::InChild(Arc::clone(&child)), &app);
        assert!(matches!(
            &chosen,
            ProducerHandle::ChildHost(handle) if Arc::ptr_eq(handle, &child)
        ));
        assert!(std::ptr::eq(chosen.lazy_handle(), Arc::as_ptr(&child)));
        assert!(!std::ptr::eq(
            chosen.lazy_handle(),
            app.connection().lazy_handle()
        ));
    }

    #[test]
    fn outside_a_task_child_a_call_goes_through_the_given_handle() {
        let app = app_handle();
        let chosen = ProducerHandle::chosen(ChildProducer::NotInAChild, &app);
        assert!(matches!(chosen, ProducerHandle::Given(_)));
        assert!(std::ptr::eq(
            chosen.lazy_handle(),
            app.connection().lazy_handle()
        ));
    }

    /// No child host runs in a test process, so the core's slot is empty.
    #[test]
    fn a_process_with_no_child_host_goes_through_the_given_handle() {
        let app = app_handle();
        let chosen = ProducerHandle::of_process(&app);
        assert!(matches!(chosen, ProducerHandle::Given(_)));
        assert!(std::ptr::eq(
            chosen.lazy_handle(),
            app.connection().lazy_handle()
        ));
    }
}

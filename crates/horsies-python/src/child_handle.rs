//! `CoreChildHandle`: the handle of a worker's child process, for the
//! writes of the child.
//!
//! A Python child builds it once, in the child, after its start: from the
//! producer manifest of its child pool (the database URL and the pool size of
//! its child kind) and the two settings that only a child pool has, the
//! connections it keeps open and the ping at checkout (Python
//! `_initialize_worker_pool` and `open_shared_async_pool`, `min_size` and
//! `check_on_checkout`). Building it starts nothing: the first call starts
//! the core of the child process and connects. A process forked without
//! `exec` after the core started refuses every call (`ForkedAfterCoreStart`),
//! so the handle is never built before the fork.

use std::num::NonZeroU32;

use chrono::{DateTime, Utc};
use horsies_core::db::KeptOpen;
use horsies_core::manifest::parse_producer_manifest;
use horsies_core::PoolProfile;
use pyo3::prelude::*;
use uuid::Uuid;

use crate::bridge::blocking::call_blocking;
use crate::bridge::completion_queue::call_async;
use crate::bridge::CallKind;
use crate::errors::ManifestRefused;
use crate::handle::{checked_core, BrokerConnection};
use crate::operations::arguments::utc_instant;
use crate::operations::finalizing::{mark_finalizing_operation, marker_fence};
use crate::operations::start::{start_attempt_operation, start_command, start_window};

/// The pool profile of a child: `kept_open` connections kept open (0 keeps
/// none) and, with `check_at_checkout`, a ping of every pooled connection at
/// checkout. Pure.
fn child_profile(kept_open: u32, check_at_checkout: bool) -> PoolProfile {
    PoolProfile::Child {
        kept_open: match NonZeroU32::new(kept_open) {
            Some(kept) => KeptOpen::Connections(kept),
            None => KeptOpen::NoConnections,
        },
        check: check_at_checkout,
    }
}

/// The core handle of one worker's child process.
#[pyclass(module = "horsies._core", name = "CoreChildHandle", frozen)]
pub struct CoreChildHandle {
    connection: BrokerConnection,
}

#[pymethods]
impl CoreChildHandle {
    /// Build a child handle from the JSON text of the producer manifest of
    /// the child pool, the connections the pool keeps open (`kept_open`,
    /// Python `min_size`) and whether it pings each connection at checkout
    /// (`check_at_checkout`). Starts nothing. A refused manifest raises
    /// `ManifestRefused`. A `kept_open` above the pool size is refused at
    /// the first call, when the pool is built: `StartFailed`, not
    /// retryable.
    #[new]
    #[pyo3(signature = (manifest_json, *, kept_open, check_at_checkout))]
    fn new(manifest_json: &str, kept_open: u32, check_at_checkout: bool) -> PyResult<Self> {
        let manifest = parse_producer_manifest(manifest_json)
            .map_err(|refused| ManifestRefused::new_err((refused.field_path, refused.cause)))?;
        Ok(Self {
            connection: BrokerConnection::new(
                manifest,
                child_profile(kept_open, check_at_checkout),
            ),
        })
    }

    /// Start one claimed task (`horsies_start_attempt`): `AttemptStarted`,
    /// `AttemptMissed`, `DeadlineBeforeStart`, or `StartFailed`. Writes, in
    /// one statement, checked out as reserved work. `deadline_in_ms` is the
    /// time left before the deadline of the run at the call, or `None` for a
    /// run with no deadline: when it has passed by the time the pool gives
    /// the connection, no statement runs. Blocking, for the isolated child
    /// and the seats of a shared thread child. A naive `claimed_at` raises
    /// `TypeError`.
    #[pyo3(signature = (
        task_id,
        *,
        worker_id,
        claimed_at,
        worker_pid,
        worker_hostname,
        deadline_in_ms
    ))]
    #[allow(clippy::too_many_arguments)]
    fn start_attempt(
        &self,
        py: Python<'_>,
        task_id: Uuid,
        worker_id: String,
        claimed_at: &Bound<'_, PyAny>,
        worker_pid: i32,
        worker_hostname: String,
        deadline_in_ms: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let claimed_at: DateTime<Utc> = utc_instant(claimed_at)?;
        let window = start_window(deadline_in_ms);
        let command = start_command(task_id, worker_id, claimed_at, worker_pid, worker_hostname);
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreChildHandle.start_attempt",
            CallKind::Write,
            start_attempt_operation(self.connection.clone(), command, window),
        )
    }

    /// The awaitable form of `start_attempt`, for a shared async child.
    #[pyo3(signature = (
        task_id,
        *,
        worker_id,
        claimed_at,
        worker_pid,
        worker_hostname,
        deadline_in_ms
    ))]
    #[allow(clippy::too_many_arguments)]
    fn start_attempt_async<'py>(
        &self,
        py: Python<'py>,
        task_id: Uuid,
        worker_id: String,
        claimed_at: &Bound<'_, PyAny>,
        worker_pid: i32,
        worker_hostname: String,
        deadline_in_ms: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let claimed_at: DateTime<Utc> = utc_instant(claimed_at)?;
        let window = start_window(deadline_in_ms);
        let command = start_command(task_id, worker_id, claimed_at, worker_pid, worker_hostname);
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreChildHandle.start_attempt_async",
            CallKind::Write,
            start_attempt_operation(self.connection.clone(), command, window),
        )
    }

    /// Write the finalizing marker of a run whose body ended
    /// (`horsies_mark_finalizing`): `FinalizingMarked`, `FinalizingNotHeld`,
    /// or `MarkFinalizingFailed`. Writes, in one statement, checked out as
    /// reserved work; the wait for the connection and the statement have the
    /// pool's bounds. Blocking, for the isolated child and the seats of a
    /// shared thread child. A naive `claimed_at` raises `TypeError`.
    #[pyo3(signature = (task_id, *, worker_id, claimed_at))]
    fn mark_finalizing(
        &self,
        py: Python<'_>,
        task_id: Uuid,
        worker_id: String,
        claimed_at: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let fence = marker_fence(worker_id, utc_instant(claimed_at)?);
        let core = checked_core(py)?;
        call_blocking(
            py,
            core,
            "CoreChildHandle.mark_finalizing",
            CallKind::Write,
            mark_finalizing_operation(self.connection.clone(), task_id, fence),
        )
    }

    /// The awaitable form of `mark_finalizing`, for a shared async child.
    #[pyo3(signature = (task_id, *, worker_id, claimed_at))]
    fn mark_finalizing_async<'py>(
        &self,
        py: Python<'py>,
        task_id: Uuid,
        worker_id: String,
        claimed_at: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let fence = marker_fence(worker_id, utc_instant(claimed_at)?);
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "CoreChildHandle.mark_finalizing_async",
            CallKind::Write,
            mark_finalizing_operation(self.connection.clone(), task_id, fence),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_child_pool_keeps_open_what_it_was_given_and_none_for_zero() {
        assert_eq!(
            child_profile(0, false),
            PoolProfile::Child {
                kept_open: KeptOpen::NoConnections,
                check: false
            }
        );
        assert_eq!(
            child_profile(2, true),
            PoolProfile::Child {
                kept_open: KeptOpen::Connections(NonZeroU32::new(2).expect("2 is above 0")),
                check: true
            }
        );
    }
}

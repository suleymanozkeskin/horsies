//! Fail-fatal supervision for worker-lifetime service loops.
//!
//! The claimer heartbeat, reaper, workflow-recovery, and worker-state loops
//! contain DB-error handling and exit only on the worker's cancellation
//! token — so the only ways one dies early are a panic or a bug that returns
//! from the loop. Before this module, each loop's `JoinHandle` was discarded:
//! a dead loop left one panic-hook line while the worker kept claiming — with
//! leases no longer renewing, no reaper passes, and stale worker-state rows
//! advertising a healthy worker (C11-inverse; Python records the same gap
//! with a fail-fatal recommendation).
//!
//! A watchdog per service awaits its loop's `JoinHandle`. An exit without a
//! shutdown request is fatal: the watchdog reports the dead service, fires
//! the worker's cancellation token (stopping the claim loop and the other
//! services), and `Worker::run` returns an error so the process exits
//! non-zero.

use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

/// Report of a service loop that died without a shutdown request.
#[derive(Debug)]
pub(crate) struct ServiceExit {
    pub service: &'static str,
    pub reason: String,
}

/// Human-readable reason for a service task's `JoinError`.
fn join_error_reason(error: JoinError) -> String {
    if error.is_panic() {
        let payload = error.into_panic();
        let message = payload
            .downcast_ref::<&'static str>()
            .map(|s| (*s).to_owned())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "non-string panic payload".to_owned());
        format!("panicked: {message}")
    } else {
        "was aborted".to_owned()
    }
}

/// Watch a service loop's `JoinHandle` and escalate an unexpected exit.
///
/// If the handle completes while `cancel` has fired, shutdown was requested
/// and the exit is normal. Otherwise — the loop returned, panicked, or was
/// aborted — the watchdog logs the death, sends a [`ServiceExit`] on
/// `fatal_tx`, and fires `cancel` so the worker stops claiming immediately.
pub(crate) fn spawn_service_watchdog(
    service: &'static str,
    handle: JoinHandle<()>,
    cancel: CancellationToken,
    fatal_tx: mpsc::UnboundedSender<ServiceExit>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let result = handle.await;
        if cancel.is_cancelled() {
            return;
        }
        let reason = match result {
            Ok(()) => "exited without a shutdown request".to_owned(),
            Err(join_error) => join_error_reason(join_error),
        };
        tracing::error!(
            service,
            reason = %reason,
            "service loop died with the worker still running; stopping worker (fail-fatal)",
        );
        let _ = fatal_tx.send(ServiceExit { service, reason });
        cancel.cancel();
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::Utc;
    use sqlx::PgPool;

    use crate::core::config::app::AppConfig;
    use crate::core::config::{PostgresConfig, QueueMode, RecoveryConfig, WorkerResilienceConfig};
    use crate::core::registry::workflow::WorkflowSpecRegistry;
    use crate::worker::config::WorkerConfig;
    use crate::worker::heartbeat::spawn_claimer_heartbeat;
    use crate::worker::recovery::{spawn_reaper, spawn_workflow_recovery};
    use crate::worker::worker_state::spawn_worker_state_loop;

    fn test_db_url() -> String {
        if let Ok(url) = std::env::var("DATABASE_URL") {
            return url;
        }
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let root = std::path::Path::new(manifest_dir)
            .ancestors()
            .find(|p| p.join(".env").exists());
        let pw = root
            .and_then(|r| std::fs::read_to_string(r.join(".env")).ok())
            .and_then(|c| {
                c.lines()
                    .filter_map(|l| l.trim().split_once('='))
                    .find(|(k, _)| k.trim() == "DB_PASSWORD")
                    .map(|(_, v)| v.trim().to_owned())
            })
            .unwrap_or_else(|| "W0rklane".to_owned());
        format!("postgresql://postgres:{pw}@localhost:5432/horsies-rust-port")
    }

    fn test_app_config() -> AppConfig {
        AppConfig {
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: PostgresConfig {
                database_url: test_db_url(),
                session_database_url: None,
                pgbouncer_transaction_mode: false,
                pool_pre_ping: true,
                pool_size: 30,
                max_overflow: 30,
                pool_timeout: 30,
                pool_recycle: 1800,
                echo: false,
            },
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            schedule: None,
            resend_on_transient_err: false,
        }
    }

    /// Await the watchdog and return the escalation it produced, if any.
    async fn watchdog_outcome(
        watchdog: JoinHandle<()>,
        mut fatal_rx: mpsc::UnboundedReceiver<ServiceExit>,
    ) -> Option<ServiceExit> {
        tokio::time::timeout(Duration::from_secs(5), watchdog)
            .await
            .expect("watchdog must settle promptly")
            .expect("watchdog must not panic");
        fatal_rx.try_recv().ok()
    }

    /// A panicking service task must escalate: report sent, token fired.
    #[tokio::test]
    async fn watchdog_escalates_on_panic() {
        let cancel = CancellationToken::new();
        let (fatal_tx, fatal_rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(async { panic!("boom") });

        let watchdog = spawn_service_watchdog("panicking", handle, cancel.clone(), fatal_tx);
        let exit = watchdog_outcome(watchdog, fatal_rx)
            .await
            .expect("panic must escalate");
        assert_eq!(exit.service, "panicking");
        assert!(exit.reason.contains("panicked: boom"), "reason: {}", exit.reason);
        assert!(cancel.is_cancelled(), "worker must stop claiming");
    }

    /// A service loop that returns without a shutdown request must escalate.
    #[tokio::test]
    async fn watchdog_escalates_on_unexpected_return() {
        let cancel = CancellationToken::new();
        let (fatal_tx, fatal_rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(async {});

        let watchdog = spawn_service_watchdog("returning", handle, cancel.clone(), fatal_tx);
        let exit = watchdog_outcome(watchdog, fatal_rx)
            .await
            .expect("unexpected return must escalate");
        assert_eq!(exit.service, "returning");
        assert!(cancel.is_cancelled());
    }

    /// A loop exiting because shutdown was requested must NOT escalate.
    #[tokio::test]
    async fn watchdog_silent_on_graceful_shutdown() {
        let cancel = CancellationToken::new();
        let (fatal_tx, fatal_rx) = mpsc::unbounded_channel();
        let loop_cancel = cancel.clone();
        let handle = tokio::spawn(async move { loop_cancel.cancelled().await });

        let watchdog = spawn_service_watchdog("graceful", handle, cancel.clone(), fatal_tx);
        cancel.cancel();
        assert!(
            watchdog_outcome(watchdog, fatal_rx).await.is_none(),
            "graceful shutdown must not be reported as a service death",
        );
    }

    /// C11-inverse regression, claimer heartbeat: a dead heartbeat loop
    /// (leases silently stop renewing) must stop the worker.
    #[tokio::test]
    async fn claimer_heartbeat_death_stops_worker() {
        let pool = PgPool::connect(&test_db_url()).await.expect("connect");
        let cancel = CancellationToken::new();
        let handle = spawn_claimer_heartbeat(
            pool,
            "supervision-w1".to_owned(),
            "host-1".to_owned(),
            1,
            Duration::from_secs(3600),
            Some(60_000),
            180_000,
            cancel.clone(),
        );
        run_loop_death_case("claimer-heartbeat", cancel, handle).await;
    }

    /// C11-inverse regression, reaper: a dead reaper loop (no stale-task
    /// recovery, no retention) must stop the worker.
    #[tokio::test]
    async fn reaper_death_stops_worker() {
        let pool = PgPool::connect(&test_db_url()).await.expect("connect");
        let cancel = CancellationToken::new();
        let config = RecoveryConfig {
            check_interval_ms: 3_600_000,
            ..RecoveryConfig::default()
        };
        let handle = spawn_reaper(pool, config, cancel.clone());
        run_loop_death_case("reaper", cancel, handle).await;
    }

    /// C11-inverse regression, workflow recovery: a dead recovery loop must
    /// stop the worker.
    #[tokio::test]
    async fn workflow_recovery_death_stops_worker() {
        let pool = PgPool::connect(&test_db_url()).await.expect("connect");
        let cancel = CancellationToken::new();
        let config = RecoveryConfig {
            check_interval_ms: 3_600_000,
            ..RecoveryConfig::default()
        };
        let handle = spawn_workflow_recovery(
            pool,
            Arc::new(WorkflowSpecRegistry::new()),
            config,
            crate::core::config::payload::PayloadPolicy::default(),
            cancel.clone(),
        );
        run_loop_death_case("workflow-recovery", cancel, handle).await;
    }

    /// C11-inverse regression, worker-state snapshot: a dead snapshot loop
    /// (stale rows advertising a healthy worker) must stop the worker.
    #[tokio::test]
    async fn worker_state_loop_death_stops_worker() {
        let pool = PgPool::connect(&test_db_url()).await.expect("connect");
        let cancel = CancellationToken::new();
        let handle = spawn_worker_state_loop(
            pool,
            "supervision-w1".to_owned(),
            "host-1".to_owned(),
            std::process::id() as i32,
            WorkerConfig::default(),
            test_app_config(),
            Arc::new(tokio::sync::Semaphore::new(1)),
            Utc::now(),
            cancel.clone(),
        );
        run_loop_death_case("worker-state", cancel, handle).await;
    }

    /// Shared body for the per-loop regression tests: supervise the real
    /// loop, kill its task without firing the token, and assert escalation.
    async fn run_loop_death_case(
        service: &'static str,
        cancel: CancellationToken,
        handle: JoinHandle<()>,
    ) {
        let (fatal_tx, fatal_rx) = mpsc::unbounded_channel();
        let abort = handle.abort_handle();
        let watchdog = spawn_service_watchdog(service, handle, cancel.clone(), fatal_tx);
        // Give the loop a beat to reach its interval sleep, then kill it.
        tokio::time::sleep(Duration::from_millis(50)).await;
        abort.abort();
        let exit = watchdog_outcome(watchdog, fatal_rx)
            .await
            .unwrap_or_else(|| panic!("{service} death must escalate"));
        assert_eq!(exit.service, service);
        assert!(exit.reason.contains("aborted"), "reason: {}", exit.reason);
        assert!(
            cancel.is_cancelled(),
            "{service} death must stop the worker from claiming",
        );
    }
}

//! What the database tests share: the opt-in database, and a worker of the
//! `acme` binary.
//!
//! A Rust worker runs every task body in a child that is its own binary in
//! child mode, so a test cannot run a worker in its own process: it starts
//! the `acme` binary, which serves child mode, as the worker.

// Each test binary uses a part of this module.
#![allow(dead_code)]

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use acme_showcase::app::{ShowcaseApp, WORKER_PROFILE};
use acme_showcase::settings::{resolve_database_settings, DatabaseSettings, ACME_URL_VARIABLE};
use acme_showcase::store::{ensure_database, Store};

/// The longest wait for the result of a task or a workflow: the worker
/// starts its children first.
pub const RESULT_WAIT: Duration = Duration::from_secs(90);

/// The pause between two looks at a stopping worker.
const STOP_POLL: Duration = Duration::from_millis(100);

/// The looks at a stopping worker before it is killed: 30 s in all.
const STOP_POLLS: u32 = 300;

/// The showcase database and its store, when `ACME_DATABASE_URL` is set.
/// The database tests are opt-in: without the variable they pass with no
/// check.
pub async fn database() -> Option<(DatabaseSettings, Store)> {
    std::env::var(ACME_URL_VARIABLE).ok()?;
    let settings = resolve_database_settings().expect("showcase settings");
    ensure_database(&settings).await.expect("showcase database");
    let store = Store::connect(&settings).await.expect("showcase store");
    store.ensure_schema().await.expect("showcase schema");
    Some((settings, store))
}

/// A worker of the `acme` binary with the Acme profile. Dropping it sends
/// `SIGTERM` and waits for the drain; a worker that has not exited after
/// the bound is killed.
pub struct WorkerProcess {
    child: Child,
}

impl WorkerProcess {
    /// Installs the schema through the handle of `showcase` (its first read
    /// runs the migrations and the coverage pass), then starts the worker.
    /// A worker start that finds the partition coverage gate held by another
    /// process, with no heartbeat leaf for the present instant, waits with
    /// its backoff; on a new database the first send of the test would hold
    /// that gate.
    pub async fn start(showcase: &ShowcaseApp) -> Self {
        let _ping = showcase
            .app
            .ping_database()
            .await
            .expect("the schema installs before the worker starts");
        let child = Command::new(env!("CARGO_BIN_EXE_acme"))
            .args(["worker", "--profile", WORKER_PROFILE])
            .stdin(Stdio::null())
            .spawn()
            .expect("the acme worker starts");
        Self { child }
    }
}

impl Drop for WorkerProcess {
    fn drop(&mut self) {
        let terminated = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status();
        if !matches!(terminated, Ok(status) if status.success()) {
            let _killed = self.child.kill();
        }
        for _ in 0..STOP_POLLS {
            match self.child.try_wait() {
                Ok(Some(_status)) => return,
                Ok(None) => std::thread::sleep(STOP_POLL),
                Err(_unreadable) => break,
            }
        }
        let _killed = self.child.kill();
        let _reaped = self.child.wait();
    }
}

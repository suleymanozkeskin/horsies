/// Worker process lifecycle helpers for e2e tests.
///
/// Mirrors Python's `tests/e2e/helpers/worker.py`.
///
/// Spawns `horsies-test-worker` as a child process, waits for functional
/// readiness (a real healthcheck task completes), and kills it on drop.
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read};
use std::process::{Child, Command, Stdio};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

/// Number of active managed worker contexts. Prevents stale-worker cleanup
/// from killing intentionally running workers in nested contexts.
static ACTIVE_WORKER_CONTEXTS: AtomicUsize = AtomicUsize::new(0);

/// How many recent log lines to keep for error diagnostics.
const LOG_RING_SIZE: usize = 50;

/// Path to the test worker binary. Built by `cargo build --bin horsies-test-worker`.
fn test_worker_binary() -> String {
    // Check for explicit override first.
    if let Ok(path) = std::env::var("HORSIES_TEST_WORKER_BIN") {
        return path;
    }

    // Default: look in target/debug (assumes `cargo build` was run).
    // CARGO_MANIFEST_DIR = <workspace>/tests/support, so go up twice.
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_owned());
    let workspace_root = std::path::Path::new(&manifest_dir)
        .parent()
        .and_then(|p| p.parent())
        .unwrap_or(std::path::Path::new("."));

    workspace_root
        .join("target/debug/horsies-test-worker")
        .to_string_lossy()
        .into_owned()
}

/// ERE for `pgrep -f`/`pkill -f` that matches only a spawned worker binary's
/// command line — argv[0] is a path ending in the binary name — and never a
/// command line that merely mentions the name as an argument.
///
/// The unanchored name matched `cargo test -p horsies-test-worker --test
/// pgbouncer_contract ...` itself: procps (Linux) matches `-f` patterns
/// against every process's full argv, including the caller's own ancestors,
/// which BSD pkill (macOS) excludes by default. On CI the stale-worker sweep
/// SIGTERMed cargo mid-run — the step exited 143 ("Terminated") while the
/// orphaned test binary finished and reported all tests passed.
const WORKER_PROCESS_PATTERN: &str = "(^|/)horsies-test-worker( |$)";

/// Kill any leftover horsies-test-worker processes from previous tests.
///
/// SIGTERM first (graceful), SIGKILL after 3s if needed.
pub fn kill_stale_workers() {
    let result = Command::new("pgrep")
        .args(["-f", WORKER_PROCESS_PATTERN])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    if result.map(|s| s.success()).unwrap_or(false) {
        // Graceful SIGTERM first.
        let _ = Command::new("pkill")
            .args(["-TERM", "-f", WORKER_PROCESS_PATTERN])
            .status();
        std::thread::sleep(Duration::from_secs(1));

        // Check if still running.
        let still_running = Command::new("pgrep")
            .args(["-f", WORKER_PROCESS_PATTERN])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);

        if still_running {
            let _ = Command::new("pkill")
                .args(["-9", "-f", WORKER_PROCESS_PATTERN])
                .status();
            std::thread::sleep(Duration::from_secs(1));
        }
    }
}

/// Shared ring buffer for recent log lines from the worker process.
type LogRing = Arc<Mutex<VecDeque<String>>>;

/// Spawn a background thread that drains a stream and keeps the last N lines.
fn spawn_drain_thread(
    stream: impl Read + Send + 'static,
    ring: LogRing,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let reader = BufReader::new(stream);
        for line in reader.lines() {
            let Ok(text) = line else { break };
            let mut buf = ring.lock().unwrap();
            if buf.len() >= LOG_RING_SIZE {
                buf.pop_front();
            }
            buf.push_back(text);
        }
    })
}

/// A managed worker process that kills itself on drop.
pub struct ManagedWorker {
    child: Option<Child>,
    _drain_handles: Vec<std::thread::JoinHandle<()>>,
    log_ring: LogRing,
}

impl ManagedWorker {
    /// Get the process ID (if still running).
    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(|c| c.id())
    }

    /// Check if the process is still running.
    pub fn is_running(&mut self) -> bool {
        self.child
            .as_mut()
            .map(|c| c.try_wait().ok().flatten().is_none())
            .unwrap_or(false)
    }

    /// Get recent log lines for diagnostics.
    pub fn recent_logs(&self) -> Vec<String> {
        self.log_ring.lock().unwrap().iter().cloned().collect()
    }

    /// Kill the worker process. Called automatically on drop.
    pub fn kill(&mut self) {
        let Some(ref mut child) = self.child else {
            return;
        };

        if child.try_wait().ok().flatten().is_some() {
            return; // already exited
        }

        // SIGTERM first.
        #[cfg(unix)]
        {
            unsafe {
                libc::kill(child.id() as i32, libc::SIGTERM);
            }
        }
        #[cfg(not(unix))]
        {
            let _ = child.kill();
        }

        // Wait up to 10s for graceful shutdown.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if child.try_wait().ok().flatten().is_some() {
                return;
            }
            if Instant::now() > deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        // Force SIGKILL.
        let _ = child.kill();
        let _ = child.wait();
    }
}

impl Drop for ManagedWorker {
    fn drop(&mut self) {
        self.kill();
        let prev = ACTIVE_WORKER_CONTEXTS.fetch_sub(1, Ordering::SeqCst);
        if prev <= 1 {
            // Outermost context — reap any orphans.
            kill_stale_workers();
        }
    }
}

/// Start a worker process and wait for functional readiness.
///
/// Functional readiness = a real `e2e_healthcheck` task enqueued via the DB
/// reaches COMPLETED status. This matches Python's `_make_ready_check` pattern.
///
/// Both stdout and stderr are continuously drained by background threads
/// to prevent the child from blocking on full pipe buffers.
///
/// # Arguments
/// * `config_path` — path to a JSON/TOML config file or a database URL
/// * `extra_args` — additional CLI arguments (e.g., `["--concurrency", "4"]`)
/// * `ready_marker` — IGNORED (kept for backward compat). Readiness is determined
///   by functional healthcheck, not a log line.
/// * `timeout` — how long to wait for the worker to become ready
#[allow(clippy::zombie_processes)]
pub fn start_worker(
    config_path: &str,
    extra_args: &[&str],
    _ready_marker: &str,
    timeout: Duration,
) -> ManagedWorker {
    if ACTIVE_WORKER_CONTEXTS.fetch_add(1, Ordering::SeqCst) == 0 {
        kill_stale_workers();
    }

    let binary = test_worker_binary();
    let mut cmd = Command::new(&binary);
    cmd.arg("worker")
        .arg(config_path)
        .args(extra_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    // Start in new process group so we can kill the whole group.
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }

    let mut child = cmd
        .spawn()
        .unwrap_or_else(|e| panic!("failed to spawn test worker ({}): {}", binary, e));

    let label = format!("worker[pid={}]", child.id());

    // Drain both stdout and stderr in background threads.
    let log_ring: LogRing = Arc::new(Mutex::new(VecDeque::with_capacity(LOG_RING_SIZE)));

    let stdout = child.stdout.take().expect("stdout not captured");
    let stdout_ring = Arc::clone(&log_ring);
    let stdout_handle = spawn_drain_thread(stdout, stdout_ring);

    let stderr = child.stderr.take().expect("stderr not captured");
    let stderr_ring = Arc::clone(&log_ring);
    let stderr_handle = spawn_drain_thread(stderr, stderr_ring);

    // Determine the queue to use for the healthcheck task.
    // If --queues or -q is specified, use the first queue; otherwise default.
    let healthcheck_queue = extra_args
        .iter()
        .zip(extra_args.iter().skip(1))
        .find(|(&flag, _)| flag == "--queues" || flag == "-q")
        .and_then(|(_, &val)| val.split(',').next().map(|s| s.to_owned()))
        .unwrap_or_else(|| "default".to_owned());

    // Resolve the DB URL — config_path may be a direct URL or a JSON config file.
    let (db_url, pgbouncer_transaction_mode) =
        if config_path.starts_with("postgresql://") || config_path.starts_with("postgres://") {
            (config_path.to_owned(), false)
        } else {
            // Parse broker.database_url from the JSON config file.
            std::fs::read_to_string(config_path)
                .ok()
                .and_then(|contents| serde_json::from_str::<serde_json::Value>(&contents).ok())
                .and_then(|v| {
                    let broker = v.get("broker")?;
                    let url = broker.get("database_url")?.as_str()?.to_owned();
                    let pgbouncer_transaction_mode = broker
                        .get("pgbouncer_transaction_mode")
                        .and_then(|value| value.as_bool())
                        .unwrap_or(false);
                    Some((url, pgbouncer_transaction_mode))
                })
                .unwrap_or_else(|| (crate::db::db_url(), false))
        };

    // Functional readiness: enqueue a healthcheck task and poll until COMPLETED.
    let deadline = Instant::now() + timeout;
    let mut healthcheck_id: Option<String> = None;

    while Instant::now() < deadline {
        // Check if the process exited.
        if let Some(status) = child.try_wait().ok().flatten() {
            let logs = log_ring.lock().unwrap().iter().cloned().collect::<Vec<_>>();
            panic!(
                "{}: exited before ready (code={:?})\nRecent logs:\n{}",
                label,
                status.code(),
                logs.join("\n"),
            );
        }

        // Try to enqueue or poll the healthcheck task.
        if healthcheck_id.is_none() {
            // Try to connect and enqueue. May fail if the DB isn't ready or
            // the task table doesn't exist yet.
            if let Ok(id) =
                try_enqueue_healthcheck(&db_url, pgbouncer_transaction_mode, &healthcheck_queue)
            {
                healthcheck_id = Some(id);
            }
        }

        if let Some(ref hc_id) = healthcheck_id {
            if is_task_completed(&db_url, pgbouncer_transaction_mode, hc_id) {
                return ManagedWorker {
                    child: Some(child),
                    _drain_handles: vec![stdout_handle, stderr_handle],
                    log_ring,
                };
            }
        }

        std::thread::sleep(Duration::from_millis(200));
    }

    let logs = log_ring.lock().unwrap().iter().cloned().collect::<Vec<_>>();
    let _ = child.kill();
    let _ = child.wait();
    panic!(
        "{}: timed out waiting for functional readiness ({}s)\nhealthcheck_id={:?}\nRecent logs:\n{}",
        label,
        timeout.as_secs(),
        healthcheck_id,
        logs.join("\n"),
    );
}

/// Run an async block on a dedicated thread with its own tokio runtime.
/// Avoids "cannot start a runtime from within a runtime" when called from
/// an async test context.
fn run_on_dedicated_thread<F, T>(f: F) -> Result<T, ()>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let handle = std::thread::spawn(f);
    handle.join().map_err(|_| ())
}

/// Try to enqueue a healthcheck task. Returns Ok(task_id) or Err on any failure.
fn try_enqueue_healthcheck(
    db_url: &str,
    pgbouncer_transaction_mode: bool,
    queue: &str,
) -> Result<String, ()> {
    let url = db_url.to_owned();
    let queue = queue.to_owned();
    run_on_dedicated_thread(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| ())?;

        rt.block_on(async {
            let pool = connect_readiness_pool(&url, pgbouncer_transaction_mode).await?;
            let task_id = uuid::Uuid::new_v4().to_string();
            let sha = format!("healthcheck-{}", task_id);
            sqlx::query(
                "INSERT INTO horsies_tasks (
                    id, task_name, queue_name, priority, args, kwargs,
                    status, sent_at, enqueue_sha, created_at, updated_at
                ) VALUES ($1, 'e2e_healthcheck', $3, 100, NULL, '{}',
                          'PENDING', NOW(), $2, NOW(), NOW())",
            )
            .bind(&task_id)
            .bind(&sha)
            .bind(&queue)
            .execute(&pool)
            .await
            .map_err(|_| ())?;

            pool.close().await;
            Ok(task_id)
        })
    })?
}

/// Check if a task has reached COMPLETED status.
fn is_task_completed(db_url: &str, pgbouncer_transaction_mode: bool, task_id: &str) -> bool {
    let url = db_url.to_owned();
    let id = task_id.to_owned();
    run_on_dedicated_thread(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .ok();

        let Some(rt) = rt else { return false };

        rt.block_on(async {
            let Ok(pool) = connect_readiness_pool(&url, pgbouncer_transaction_mode).await else {
                return false;
            };
            let status: Option<String> =
                sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
                    .bind(&id)
                    .fetch_optional(&pool)
                    .await
                    .ok()
                    .flatten();
            pool.close().await;
            status.as_deref() == Some("COMPLETED")
        })
    })
    .unwrap_or(false)
}

async fn connect_readiness_pool(
    db_url: &str,
    pgbouncer_transaction_mode: bool,
) -> Result<sqlx::PgPool, ()> {
    if pgbouncer_transaction_mode {
        let options = PgConnectOptions::from_str(db_url)
            .map_err(|_| ())?
            .statement_cache_capacity(0);
        PgPoolOptions::new()
            .max_connections(2)
            .connect_with(options)
            .await
            .map_err(|_| ())
    } else {
        PgPoolOptions::new()
            .max_connections(2)
            .connect(db_url)
            .await
            .map_err(|_| ())
    }
}

/// Start a scheduler process with the given config file.
pub fn start_scheduler(config_path: &str, ready_marker: &str, timeout: Duration) -> ManagedWorker {
    if ACTIVE_WORKER_CONTEXTS.fetch_add(1, Ordering::SeqCst) == 0 {
        kill_stale_workers();
    }

    let binary = test_worker_binary();
    let mut cmd = Command::new(&binary);
    cmd.arg("scheduler")
        .arg(config_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }

    let mut child = cmd
        .spawn()
        .unwrap_or_else(|e| panic!("failed to spawn test scheduler ({}): {}", binary, e));

    let label = format!("scheduler[pid={}]", child.id());

    let log_ring: LogRing = Arc::new(Mutex::new(VecDeque::with_capacity(LOG_RING_SIZE)));

    let stdout = child.stdout.take().expect("stdout not captured");
    let stdout_handle = spawn_drain_thread(stdout, Arc::clone(&log_ring));

    let stderr = child.stderr.take().expect("stderr not captured");
    let stderr_ring = Arc::clone(&log_ring);
    let stderr_handle = spawn_drain_thread(stderr, stderr_ring);

    // For the scheduler, we still use the log marker since there's no
    // equivalent "scheduler healthcheck task".
    let deadline = Instant::now() + timeout;

    loop {
        if Instant::now() > deadline {
            let logs = log_ring.lock().unwrap().iter().cloned().collect::<Vec<_>>();
            let _ = child.kill();
            panic!(
                "{}: timed out waiting for ready marker {:?}\nRecent logs:\n{}",
                label,
                ready_marker,
                logs.join("\n"),
            );
        }

        if let Some(status) = child.try_wait().ok().flatten() {
            let logs = log_ring.lock().unwrap().iter().cloned().collect::<Vec<_>>();
            panic!(
                "{}: exited before ready (code={:?})\nRecent logs:\n{}",
                label,
                status.code(),
                logs.join("\n"),
            );
        }

        // Check if the ready marker appeared in recent logs.
        let found = {
            let buf = log_ring.lock().unwrap();
            buf.iter().any(|line| line.contains(ready_marker))
        };
        if found {
            return ManagedWorker {
                child: Some(child),
                _drain_handles: vec![stdout_handle, stderr_handle],
                log_ring,
            };
        }

        std::thread::sleep(Duration::from_millis(100));
    }
}

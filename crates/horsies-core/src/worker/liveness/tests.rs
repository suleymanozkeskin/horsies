//! The worker liveness lock and the reaper's owner-gone step together.
//!
//! - A worker process is a probe process: this test binary started again
//!   with the probe variables, which runs only [`liveness_probe_process`].
//!   It takes the liveness lock of its worker id with the production holder
//!   and watchdog ([`start_liveness_lock`]), holds one task in its own
//!   process, and sleeps. A test kills it, stops it, cuts its connection, or
//!   waits for it to fail closed.
//! - The reaper is [`owner_gone_step`] on the test database, called every
//!   [`PASS_INTERVAL`] with a span that fits the probe's regain bound.
//! - Each test has its database of its own, so the liveness read sees only
//!   the rows of its test.

use std::process::{Child, Command, Stdio};

use chrono::Utc;
use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;
use crate::broker::core_rows::StatementLog;
use crate::broker::pools::core_pool_for_test_pool;
use crate::broker::terminalization_matrix::{migrated_database_url, migrated_pool};
use crate::db::{CorePool, DEFAULT_SOCKET_SETTINGS};
use crate::worker::recovery::owner_gone::OwnerGoneStep;
use crate::worker::recovery::{owner_gone_step, UnheldOwners};
use crate::worker::runner_beats::DispatchRunner;

/// The longest wait for an event that a test expects.
const WAIT: Duration = Duration::from_secs(20);

/// How often a test reads the lock table or runs a reaper pass.
const PASS_INTERVAL: Duration = Duration::from_millis(100);

/// The span between the two reads that judge a killed probe gone.
const TEST_SPAN: Duration = Duration::from_secs(1);

/// The time the rows of a gone owner take to be recovered after the span:
/// a few passes and their statements.
const RECOVERY_SLACK: Duration = Duration::from_secs(5);

/// The probe interval of the probes: well below every bound of the tests.
const PROBE_INTERVAL: Duration = Duration::from_millis(100);

/// How long a probe sleeps when no test ends it.
const PROBE_LIFETIME: Duration = Duration::from_secs(120);

const PROBE_URL_ENV: &str = "HORSIES_TEST_LIVENESS_PROBE_URL";
const PROBE_WORKER_ENV: &str = "HORSIES_TEST_LIVENESS_PROBE_WORKER";
const PROBE_BOUND_ENV: &str = "HORSIES_TEST_LIVENESS_PROBE_BOUND_MS";
const PROBE_TEST: &str = "worker::liveness::tests::liveness_probe_process";

/// The backend that holds the liveness lock of `$1` in this database.
const HOLDER_SQL: &str = "\
SELECT l.pid
FROM pg_locks l
WHERE l.locktype = 'advisory'
  AND l.granted
  AND l.objsubid = 1
  AND l.database = (SELECT d.oid FROM pg_database d WHERE d.datname = current_database())
  AND ((l.classid::bigint << 32) | l.objid::bigint) = horsies_worker_liveness_key($1)";

/// A RUNNING row of worker `$2`, claimed 1 s ago and started now.
const SEED_RUNNING_SQL: &str = "\
INSERT INTO horsies_tasks (
    id, task_name, queue_name, priority, input, task_options, execution,
    retention_class_key, sent_at, created_at,
    command_fingerprint_version, command_fingerprint, idempotency_key_digest,
    status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
    started_at, finalizing_at, finalizing_by_worker_id,
    worker_pid, worker_hostname, worker_process_name,
    retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
    updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
) VALUES (
    $1, 'liveness_test', 'q_liveness', 100, 'null', NULL, 'ISOLATED',
    'standard_30d', NOW(), NOW(),
    1, decode(repeat('00', 32), 'hex'), NULL,
    'RUNNING', NOW(), NOW() - interval '1 second', $2, NULL,
    NOW(), NULL, NULL,
    7, 'host-a', 'worker-7',
    0, 0, NULL, NULL, FALSE,
    NOW(), FALSE, NULL, NULL
)";

/// A CLAIMED row of worker `$2`, claimed 1 s ago with a live lease.
const SEED_CLAIMED_SQL: &str = "\
INSERT INTO horsies_tasks (
    id, task_name, queue_name, priority, input, task_options, execution,
    retention_class_key, sent_at, created_at,
    command_fingerprint_version, command_fingerprint, idempotency_key_digest,
    status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
    started_at, finalizing_at, finalizing_by_worker_id,
    worker_pid, worker_hostname, worker_process_name,
    retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
    updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
) VALUES (
    $1, 'liveness_test', 'q_liveness', 100, 'null', NULL, 'ISOLATED',
    'standard_30d', NOW(), NOW(),
    1, decode(repeat('00', 32), 'hex'), NULL,
    'CLAIMED', NOW(), NOW() - interval '1 second', $2, NOW() + interval '1 minute',
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    0, 0, NULL, NULL, FALSE,
    NOW(), FALSE, NULL, NULL
)";

// ---------------------------------------------------------------------------
// The rule
// ---------------------------------------------------------------------------

fn rule(recovery: LivenessRecovery) -> LivenessRule {
    LivenessRule {
        regain_bound: Duration::from_secs(60),
        probe_interval: LIVENESS_PROBE_INTERVAL,
        recovery,
    }
}

/// The watchdog acts only past the regain bound, and fails closed only with
/// stale running recovery on and a task held; otherwise nothing is at risk.
#[test]
fn the_watch_fails_closed_only_past_the_bound_with_a_task_at_risk() {
    let within = Duration::from_secs(60);
    let past = Duration::from_millis(60_001);
    let fail_closed = rule(LivenessRecovery::FailClosed);
    let log_only = rule(LivenessRecovery::LogOnly);

    assert_eq!(
        watch_verdict(within, fail_closed, TasksAtRisk::Held),
        WatchVerdict::WithinBound
    );
    assert_eq!(
        watch_verdict(past, fail_closed, TasksAtRisk::Held),
        WatchVerdict::FailClosed
    );
    assert_eq!(
        watch_verdict(past, fail_closed, TasksAtRisk::NoneHeld),
        WatchVerdict::PastBoundRunOn
    );
    assert_eq!(
        watch_verdict(past, log_only, TasksAtRisk::Held),
        WatchVerdict::PastBoundRunOn
    );
}

/// The worker fails closed by the same flag with which its peers recover
/// the RUNNING rows of a gone owner; the production bounds hold the probe
/// interval well inside the regain bound, and the peers' span is the bound
/// plus the margin.
#[test]
fn the_rule_of_the_config_follows_stale_running_recovery() {
    let on = RecoveryConfig::default();
    let off = RecoveryConfig {
        auto_fail_stale_running: false,
        ..RecoveryConfig::default()
    };

    assert_eq!(
        LivenessRule::of_config(&on).recovery,
        LivenessRecovery::FailClosed
    );
    assert_eq!(
        LivenessRule::of_config(&off).recovery,
        LivenessRecovery::LogOnly
    );
    assert_eq!(
        LivenessRule::of_config(&on).regain_bound,
        LIVENESS_REGAIN_BOUND
    );
    assert!(
        LIVENESS_PROBE_INTERVAL + SESSION_LOCK_STATEMENT_BOUND < LIVENESS_REGAIN_BOUND,
        "a probe and its statement fit in the regain bound"
    );
    assert_eq!(OWNER_GONE_SPAN, LIVENESS_REGAIN_BOUND + OWNER_GONE_MARGIN);
    assert_ne!(
        LIVENESS_LOCK_LOST_EXIT_CODE,
        crate::worker::runner_beats::RUNNER_HEARTBEAT_LOST_EXIT_CODE
    );
}

// ---------------------------------------------------------------------------
// The probe process
// ---------------------------------------------------------------------------

/// The connection plan of a test URL, with an isolated environment (test URLs
/// carry their password).
fn test_plan(url: &str) -> ConnectPlan {
    let environment = ConnectionEnvironment::isolated();
    let settings = parse_connection_url(url, &environment).expect("the test URL parses");
    ConnectPlan::load(&settings, &environment, &DEFAULT_SOCKET_SETTINGS)
        .expect("the test connection plan loads")
}

/// The body of a probe process: the liveness lock of the worker id that the
/// test names, with its regain bound and the probe interval of the tests,
/// stale running recovery on, and one task held in the process. Without the
/// probe variables this test returns at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn liveness_probe_process() {
    let Some(url) = std::env::var_os(PROBE_URL_ENV) else {
        return;
    };
    let url = url.into_string().expect("the probe URL is UTF-8");
    let worker_id = std::env::var(PROBE_WORKER_ENV).expect("the probe names its worker");
    let bound_ms: u64 = std::env::var(PROBE_BOUND_ENV)
        .expect("the probe names its bound")
        .parse()
        .expect("the bound is whole milliseconds");
    let live = LiveDispatches::default();
    let _held = live
        .hold(Uuid::new_v4(), Utc::now(), DispatchRunner::WorkerProcess)
        .expect("the registry is open");
    let probe_rule = LivenessRule {
        regain_bound: Duration::from_millis(bound_ms),
        probe_interval: PROBE_INTERVAL,
        recovery: LivenessRecovery::FailClosed,
    };
    let started = start_liveness_lock(Arc::new(test_plan(&url)), &worker_id, probe_rule, live)
        .await
        .expect("the probe takes its lock");
    tokio::time::sleep(PROBE_LIFETIME).await;
    // Reached only when no test ended the process.
    started.running.stop().await;
    drop(started.holder);
}

/// A probe process that the test owns: killed and reaped when dropped.
struct Probe(Child);

impl Probe {
    fn start(url: &str, worker_id: &str, bound: Duration) -> Probe {
        let child = Command::new(std::env::current_exe().expect("the test binary"))
            .args(["--exact", PROBE_TEST, "--nocapture", "--test-threads=1"])
            .env(PROBE_URL_ENV, url)
            .env(PROBE_WORKER_ENV, worker_id)
            .env(PROBE_BOUND_ENV, bound.as_millis().to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("the probe process starts");
        Probe(child)
    }

    fn pid(&self) -> libc::pid_t {
        libc::pid_t::try_from(self.0.id()).expect("a process id fits pid_t")
    }

    /// `SIGKILL`, then reap.
    fn kill(&mut self) {
        self.0.kill().expect("the probe is killed");
        let _status = self.0.wait().expect("the probe is reaped");
    }

    /// `SIGSTOP`: the process keeps its connections and runs nothing.
    fn stop(&self) {
        // SAFETY: kill(2) on the pid of a child that this test started and
        // has not reaped.
        let sent = unsafe { libc::kill(self.pid(), libc::SIGSTOP) };
        assert_eq!(sent, 0, "SIGSTOP reaches the probe");
    }

    fn is_running(&mut self) -> bool {
        matches!(self.0.try_wait(), Ok(None))
    }

    /// The exit code, once the probe ended within `bound`.
    async fn exit_code_within(&mut self, bound: Duration) -> Option<i32> {
        let until = Instant::now() + bound;
        loop {
            if let Some(status) = self.0.try_wait().expect("read the probe's state") {
                return status.code();
            }
            assert!(
                Instant::now() < until,
                "the probe did not end within {bound:?}"
            );
            tokio::time::sleep(PASS_INTERVAL).await;
        }
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        // The probe may have ended already; then both calls fail.
        let _killed = self.0.kill();
        let _reaped = self.0.wait();
    }
}

// ---------------------------------------------------------------------------
// The database
// ---------------------------------------------------------------------------

/// The backend that holds the liveness lock of `worker_id`, if any.
async fn holder_backend(pool: &PgPool, worker_id: &str) -> Option<i32> {
    sqlx::query_scalar(HOLDER_SQL)
        .bind(worker_id)
        .fetch_optional(pool)
        .await
        .expect("read the lock table")
}

/// Wait until a backend holds the liveness lock of `worker_id` (other than
/// `not`, when given), and return it.
async fn wait_for_holder(pool: &PgPool, worker_id: &str, not: Option<i32>) -> i32 {
    let until = Instant::now() + WAIT;
    loop {
        match holder_backend(pool, worker_id).await {
            Some(pid) if Some(pid) != not => return pid,
            Some(_) | None => {}
        }
        assert!(
            Instant::now() < until,
            "no backend took the lock of {worker_id}"
        );
        tokio::time::sleep(PASS_INTERVAL).await;
    }
}

/// One RUNNING and one CLAIMED row of `worker_id`.
async fn seed_rows(pool: &PgPool, worker_id: &str) -> (Uuid, Uuid) {
    let running = Uuid::new_v4();
    let claimed = Uuid::new_v4();
    for (sql, task_id) in [(SEED_RUNNING_SQL, running), (SEED_CLAIMED_SQL, claimed)] {
        sqlx::query(sql)
            .bind(task_id)
            .bind(worker_id)
            .execute(pool)
            .await
            .expect("seed a row of the worker");
    }
    (running, claimed)
}

/// Where a seeded row is now.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RowState {
    /// The live row, with its status.
    Live(String),
    /// The history row, with its status and the kind of its move.
    Moved {
        status: String,
        kind: String,
    },
    Absent,
}

async fn row_state(pool: &PgPool, task_id: Uuid) -> RowState {
    let live: Option<String> =
        sqlx::query_scalar("SELECT status::text FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .fetch_optional(pool)
            .await
            .expect("read the live row");
    if let Some(status) = live {
        return RowState::Live(status);
    }
    let moved: Option<(String, String)> = sqlx::query_as(
        "SELECT status::text, terminalization_kind::text FROM horsies_task_history \
         WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await
    .expect("read the history row");
    match moved {
        Some((status, kind)) => RowState::Moved { status, kind },
        None => RowState::Absent,
    }
}

/// The rows of a gone owner after their recovery: the RUNNING row failed
/// (no retry policy), the CLAIMED row back in its queue.
fn recovered(running: &RowState, claimed: &RowState) -> bool {
    *running
        == RowState::Moved {
            status: "FAILED".to_owned(),
            kind: "FAIL_STALE".to_owned(),
        }
        && *claimed == RowState::Live("PENDING".to_owned())
}

async fn pass(core: &CorePool, memory: &mut UnheldOwners, span: Duration) -> OwnerGoneStep {
    owner_gone_step(
        core,
        StatementLog::Off,
        &RecoveryConfig::default(),
        memory,
        span,
    )
    .await
    .expect("the owner-gone step reads the lock table")
}

/// Run passes every [`PASS_INTERVAL`] for `window`; panics at a pass that
/// judges an owner gone.
async fn passes_judge_no_owner(
    core: &CorePool,
    memory: &mut UnheldOwners,
    span: Duration,
    window: Duration,
) {
    let until = Instant::now() + window;
    while Instant::now() < until {
        let step = pass(core, memory, span).await;
        assert_eq!(step.gone_owners, 0, "{step:?}");
        tokio::time::sleep(PASS_INTERVAL).await;
    }
}

// ---------------------------------------------------------------------------
// The four cases
// ---------------------------------------------------------------------------

/// A worker process that is killed with RUNNING and CLAIMED rows: its
/// connection ends with it, so its lock is free, and the reaper recovers
/// both rows within the span and a few passes, far inside the stale
/// thresholds. While the process lived, no pass judged it.
#[tokio::test]
#[serial]
async fn a_killed_worker_process_is_recovered_within_the_detection_time() {
    let pool = migrated_pool().await;
    let url = migrated_database_url().await;
    let worker_id = Uuid::new_v4().to_string();
    let mut probe = Probe::start(&url, &worker_id, Duration::from_secs(60));
    let _holder = wait_for_holder(&pool, &worker_id, None).await;
    let (running, claimed) = seed_rows(&pool, &worker_id).await;
    let core = core_pool_for_test_pool(&pool);
    let mut memory = UnheldOwners::default();

    // A held lock is never read unheld, with no span at all.
    let held = pass(&core, &mut memory, Duration::ZERO).await;
    assert_eq!(held.gone_owners, 0);

    probe.kill();
    let killed_at = Instant::now();
    let recovered_at = loop {
        let _step = pass(&core, &mut memory, TEST_SPAN).await;
        let states = (
            row_state(&pool, running).await,
            row_state(&pool, claimed).await,
        );
        if recovered(&states.0, &states.1) {
            break Instant::now();
        }
        assert!(
            killed_at.elapsed() < TEST_SPAN + RECOVERY_SLACK,
            "the rows of the killed worker were not recovered: {states:?}"
        );
        tokio::time::sleep(PASS_INTERVAL).await;
    };
    core.close();

    let detection = recovered_at.saturating_duration_since(killed_at);
    assert!(
        detection >= TEST_SPAN,
        "{detection:?}: two reads a span apart judge it"
    );
    let stale_threshold =
        Duration::from_millis(RecoveryConfig::default().claimed_stale_threshold_ms);
    assert!(detection < stale_threshold, "{detection:?}");
}

/// A worker that is alive but stalled keeps its connection, so it keeps its
/// lock: no pass judges it gone, and its rows stay.
#[tokio::test]
#[serial]
async fn a_stalled_worker_that_keeps_its_lock_is_never_recovered() {
    let pool = migrated_pool().await;
    let url = migrated_database_url().await;
    let worker_id = Uuid::new_v4().to_string();
    let mut probe = Probe::start(&url, &worker_id, Duration::from_secs(60));
    let _holder = wait_for_holder(&pool, &worker_id, None).await;
    let (running, claimed) = seed_rows(&pool, &worker_id).await;
    let core = core_pool_for_test_pool(&pool);
    let mut memory = UnheldOwners::default();

    probe.stop();
    passes_judge_no_owner(&core, &mut memory, TEST_SPAN, 3 * TEST_SPAN).await;
    core.close();

    assert_eq!(
        row_state(&pool, running).await,
        RowState::Live("RUNNING".to_owned())
    );
    assert_eq!(
        row_state(&pool, claimed).await,
        RowState::Live("CLAIMED".to_owned())
    );
    assert!(
        holder_backend(&pool, &worker_id).await.is_some(),
        "the stalled worker holds its lock"
    );
    probe.kill();
}

/// A connection lost and the lock taken again within the regain bound: the
/// passes, with the span of that bound plus a margin, judge nothing, and the
/// worker runs on with a task held.
#[tokio::test]
#[serial]
async fn a_lock_lost_and_regained_within_the_bound_causes_no_recovery() {
    let pool = migrated_pool().await;
    let url = migrated_database_url().await;
    let worker_id = Uuid::new_v4().to_string();
    let bound = Duration::from_secs(3);
    let span = bound + Duration::from_secs(1);
    let mut probe = Probe::start(&url, &worker_id, bound);
    let first = wait_for_holder(&pool, &worker_id, None).await;
    let (running, claimed) = seed_rows(&pool, &worker_id).await;
    let core = core_pool_for_test_pool(&pool);
    let mut memory = UnheldOwners::default();

    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(first)
        .fetch_one(&pool)
        .await
        .expect("end the probe's lock session");
    assert!(terminated);
    passes_judge_no_owner(&core, &mut memory, span, 2 * span).await;
    core.close();

    let again = wait_for_holder(&pool, &worker_id, Some(first)).await;
    assert_ne!(again, first);
    assert!(
        probe.is_running(),
        "the worker took its lock again and runs on"
    );
    assert_eq!(
        row_state(&pool, running).await,
        RowState::Live("RUNNING".to_owned())
    );
    assert_eq!(
        row_state(&pool, claimed).await,
        RowState::Live("CLAIMED".to_owned())
    );
    probe.kill();
}

/// A connection lost and no new one possible: past its regain bound, with a
/// task held, the worker fails closed and its process ends with
/// [`LIVENESS_LOCK_LOST_EXIT_CODE`].
#[tokio::test]
#[serial]
async fn a_lock_lost_past_the_bound_makes_the_worker_fail_closed() {
    let pool = migrated_pool().await;
    let url = migrated_database_url().await;
    let worker_id = Uuid::new_v4().to_string();
    let bound = Duration::from_secs(1);
    let mut probe = Probe::start(&url, &worker_id, bound);
    let holder = wait_for_holder(&pool, &worker_id, None).await;
    let database: String = sqlx::query_scalar("SELECT current_database()::text")
        .fetch_one(&pool)
        .await
        .expect("read the test database name");
    let admin =
        crate::test_support::connect(&crate::test_support::with_database(&url, "postgres")).await;
    let close_sql = format!("ALTER DATABASE \"{database}\" ALLOW_CONNECTIONS false");
    let open_sql = format!("ALTER DATABASE \"{database}\" ALLOW_CONNECTIONS true");

    admin
        .batch_execute(&close_sql)
        .await
        .expect("refuse new connections");
    admin
        .query_one("SELECT pg_terminate_backend($1)", &[&holder])
        .await
        .expect("end the probe's lock session");
    let lost_at = Instant::now();
    let code = probe.exit_code_within(WAIT).await;
    let ended_after = lost_at.elapsed();
    admin
        .batch_execute(&open_sql)
        .await
        .expect("accept connections again");

    assert_eq!(code, Some(LIVENESS_LOCK_LOST_EXIT_CODE));
    // The bound counts from the start of the last confirmation, at most two
    // probe intervals before the loss.
    assert!(
        ended_after + 2 * PROBE_INTERVAL >= bound,
        "the worker ended {ended_after:?} after the loss, before its bound of {bound:?}"
    );
}

/// The stop of a running liveness lock closes its connection: the server
/// releases the lock.
#[tokio::test]
#[serial]
async fn a_stopped_liveness_lock_is_released() {
    let pool = migrated_pool().await;
    let url = migrated_database_url().await;
    let worker_id = Uuid::new_v4().to_string();
    let started = start_liveness_lock(
        Arc::new(test_plan(&url)),
        &worker_id,
        rule(LivenessRecovery::LogOnly),
        LiveDispatches::default(),
    )
    .await
    .expect("the lock is taken");
    let _holder = wait_for_holder(&pool, &worker_id, None).await;

    started.running.stop().await;

    let until = Instant::now() + WAIT;
    while holder_backend(&pool, &worker_id).await.is_some() {
        assert!(Instant::now() < until, "the stopped lock was not released");
        tokio::time::sleep(PASS_INTERVAL).await;
    }
    drop(started.holder);
}

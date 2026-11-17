//! The runner beats with real children, writes that fail on demand, and the
//! isolated slots that hold their tasks (Python
//! `tests/unit/test_runner_heartbeat.py`).
//!
//! - A write fails on demand on a pool whose server refuses the connection.
//! - A write does not return within a test on a pool whose server accepts
//!   the connection and never answers (the connect ends at
//!   [`BEAT_CONNECT_TIMEOUT`]).
//! - A write fails, then lands, on a pool that reaches the test database
//!   through a gate that closes every connection until a deadline.
//! - Children are `/bin/sh` processes that the spawner thread starts.

use std::net::TcpListener;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU32, Ordering};

use chrono::TimeZone;

use super::*;
use crate::broker::attempt_writes::RetryPlacement;
use crate::core::config::recovery::RecoveryConfigError;
use crate::test_support::{CapturedEvent, LogCapture};
use crate::worker::executor::placement::ChildCpus;
use crate::worker::executor::protocol::{
    encode_child_frame, encode_parent_frame, ChildConnection, ChildFrame, ChildKind, ChildLogLevel,
    ChildPool, ChildProgram, DatabaseUrl, InitFrame, IsolatedChild, IsolatedWork, NoLeftWork,
    ParentFrame, PoolMaxSize, PoolMinSize, ReadyFrame, ResultEnvelopeText, ResultFrame, RssReading,
    RunEnd, RunFrame, TaskInputText, TaskSendPool, TaskTimeoutMs,
};
use crate::worker::executor::slots::{
    AttemptEnd, ChildLayout, ClaimedTask, ExecutionClass, ExitNotSeen, IsolatedDeadline,
    IsolatedPool, IsolatedPoolConfig, MemoryLimit, NotRunCause, PoolBoard, RetryFacts, Submitted,
    TaskKind, TaskLimit, TimeoutCancel,
};
use crate::worker::executor::supervisor::channel::KillMark;
use crate::worker::executor::supervisor::{
    wait_for_exit, ChildCommand, ChildExit, ExitState, ReapedExit, SpawnedChild, Spawner,
    CHILD_KILL_WAIT,
};
use crate::worker::retry::read_stored_options;

/// The longest wait for an event a test expects.
const WAIT: Duration = Duration::from_secs(10);
/// The start of the watchdog's line for late beats with recovery off.
const LATE_BEATS_LINE: &str = "runner heartbeats are late; the worker runs on";
/// The start of the writer's line for a failed write.
const FAILED_WRITE_LINE: &str = "Runner heartbeat write failed (attempt ";
/// The beat interval of the tests.
const INTERVAL: Duration = Duration::from_millis(50);
/// A fail-closed window that passes within a test.
const SHORT_WINDOW: Duration = Duration::from_millis(500);
/// Connections that one test gate accepts before it stops.
const GATE_CONNECTION_LIMIT: u32 = 1_000;

/// Stale running recovery whose fail-closed window is `window`.
fn recovery_on(window: Duration) -> StaleRunningRecovery {
    StaleRunningRecovery::On {
        threshold: window * FAIL_CLOSED_WINDOW_DIVISOR as u32,
    }
}

/// A pool on a port where no server listens: every write fails at its
/// connect.
fn refusing_pool() -> Arc<CorePool> {
    test_beat_pool("postgresql://horsies@127.0.0.1:1/horsies")
}

/// A pool on `silent`, which takes the connection and never answers: a
/// write does not return before [`BEAT_CONNECT_TIMEOUT`].
fn silent_pool(silent: &TcpListener) -> Arc<CorePool> {
    let port = silent
        .local_addr()
        .expect("the silent listener has a port")
        .port();
    test_beat_pool(&format!("postgresql://horsies@127.0.0.1:{port}/horsies"))
}

/// A TCP gate in front of the test database: until its deadline it closes
/// each connection it accepts, so the connect fails; from then on it relays
/// each connection to the database. It counts both.
struct ClosingGate {
    port: u16,
    refused: Arc<AtomicU32>,
    admitted: Arc<AtomicU32>,
    accept_loop: JoinHandle<()>,
}

impl ClosingGate {
    /// Listen on a free local port in front of `host`:`port` until
    /// `refuse_until`, then relay.
    async fn start(host: String, port: u16, refuse_until: Instant) -> Self {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind the gate");
        let gate_port = listener.local_addr().expect("the gate has a port").port();
        let refused = Arc::new(AtomicU32::new(0));
        let admitted = Arc::new(AtomicU32::new(0));
        let counts = (Arc::clone(&refused), Arc::clone(&admitted));
        let accept_loop = tokio::spawn(async move {
            let (refused, admitted) = counts;
            for _ in 0..GATE_CONNECTION_LIMIT {
                let Ok((mut client, _)) = listener.accept().await else {
                    return;
                };
                match Instant::now() < refuse_until {
                    true => {
                        refused.fetch_add(1, Ordering::SeqCst);
                        drop(client);
                    }
                    false => {
                        admitted.fetch_add(1, Ordering::SeqCst);
                        let host = host.clone();
                        let _relay = tokio::spawn(async move {
                            if let Ok(mut upstream) =
                                tokio::net::TcpStream::connect((host.as_str(), port)).await
                            {
                                let _ended =
                                    tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
                            }
                        });
                    }
                }
            }
        });
        Self {
            port: gate_port,
            refused,
            admitted,
            accept_loop,
        }
    }
}

impl Drop for ClosingGate {
    fn drop(&mut self) {
        self.accept_loop.abort();
    }
}

fn sender() -> BeatSender {
    BeatSender {
        worker_id: "beat-test-worker".to_owned(),
        hostname: "beat-test-host".to_owned(),
        pid: 7,
    }
}

/// The beats of one test: the watchdog, the writer, and the tokens the
/// worker holds.
struct Launched {
    watch: RunnerBeatWatch,
    writer: JoinHandle<()>,
    beats_cancel: CancellationToken,
    worker_stop: CancellationToken,
    fail_closed_stop: FailClosedStop,
}

fn launch(
    pool: Arc<CorePool>,
    live: &LiveDispatches,
    stale_running: StaleRunningRecovery,
) -> Launched {
    let worker_stop = CancellationToken::new();
    let beats_cancel = CancellationToken::new();
    let fail_closed_stop = FailClosedStop::new(worker_stop.clone());
    let (watch, writer) = launch_runner_beats(
        pool,
        sender(),
        INTERVAL,
        stale_running,
        live.clone(),
        beats_cancel.clone(),
        fail_closed_stop.clone(),
    )
    .expect("the watchdog thread starts");
    Launched {
        watch,
        writer,
        beats_cancel,
        worker_stop,
        fail_closed_stop,
    }
}

impl Launched {
    /// End the writer (a writer stuck in a write is aborted) and the
    /// watchdog, as the worker does after its drain.
    async fn stop(self) {
        self.beats_cancel.cancel();
        self.writer.abort();
        let _ended = self.writer.await;
        let watch = self.watch;
        tokio::time::timeout(WAIT, tokio::task::spawn_blocking(move || watch.stop()))
            .await
            .expect("the watchdog stops in time")
            .expect("the watchdog thread joins");
    }

    /// Wait until the watchdog fails closed and asks the worker to stop;
    /// returns the cause.
    async fn failed_closed(&self) -> String {
        tokio::time::timeout(WAIT, self.worker_stop.cancelled())
            .await
            .expect("the watchdog fails closed and asks the worker to stop");
        match self.fail_closed_stop.state() {
            FailClosedState::FailedClosed { cause } => cause,
            FailClosedState::Watching => panic!("the worker stopped with no fail-closed cause"),
        }
    }

    /// The worker was not asked to stop.
    fn still_watching(&self) {
        assert_eq!(self.fail_closed_stop.state(), FailClosedState::Watching);
        assert!(!self.worker_stop.is_cancelled());
    }
}

/// A child that sleeps: what a held dispatch runs on.
async fn sleeping_child(spawner: &Spawner) -> (SpawnedChild, HeldChild) {
    let spawned = spawner
        .handle()
        .spawn(ChildCommand {
            program: "/bin/sh".into(),
            args: vec!["-c".into(), "exec sleep 60".into()],
        })
        .await
        .expect("spawn a sleeping child");
    let child = HeldChild {
        id: spawned.id,
        pid: spawned.pid,
        spawner: spawner.handle(),
        kill_mark: WorkerKillMark::default(),
    };
    (spawned, child)
}

/// Kill the child if it still runs, then stop the spawner.
async fn end_child(spawner: Spawner, mut spawned: SpawnedChild) {
    let _outcome = spawner.handle().kill_session(spawned.id).await;
    let _exit = wait_for_exit(&mut spawned.exit, WAIT).await;
    let _report = spawner.stop(WAIT);
}

fn killed_by_sigkill() -> ChildExit {
    ChildExit::Reaped(ReapedExit::KilledBySignal(libc::SIGKILL))
}

// ---------------------------------------------------------------------------
// Which tasks get a beat
// ---------------------------------------------------------------------------

/// Python `test_an_idle_worker_writes_nothing_and_never_fails_closed`: with
/// no held task no write is sent, so a database that refuses every write
/// cannot fail the worker closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_worker_never_fails_closed() {
    let live = LiveDispatches::default();
    let launched = launch(
        refusing_pool(),
        &live,
        recovery_on(Duration::from_millis(300)),
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    launched.still_watching();
    launched.stop().await;
}

// ---------------------------------------------------------------------------
// Fail closed
// ---------------------------------------------------------------------------

/// Python `test_writes_failing_past_the_window_stop_the_worker`: the
/// watchdog names the last failure, kills the held child with the mark of a
/// kill by the worker, refuses later holds, and asks the worker to stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_failing_past_the_window_stop_the_worker() {
    let spawner = Spawner::start().expect("the spawner starts");
    let (mut spawned, child) = sleeping_child(&spawner).await;
    let kill_mark = child.kill_mark.clone();
    let live = LiveDispatches::default();
    let claimed_at = Utc::now();
    let _held = live
        .hold(Uuid::new_v4(), claimed_at, DispatchRunner::Child(child))
        .expect("the registry is open");

    let launched = launch(refusing_pool(), &live, recovery_on(SHORT_WINDOW));
    let cause = launched.failed_closed().await;

    assert!(
        cause.starts_with("no runner heartbeat landed for"),
        "{cause}"
    );
    assert!(cause.contains("past 0.5s"), "{cause}");
    assert!(cause.contains("; last error: "), "{cause}");
    assert_eq!(
        wait_for_exit(&mut spawned.exit, WAIT).await,
        killed_by_sigkill()
    );
    assert_eq!(kill_mark.read(), KillMark::KilledByParent);
    assert!(matches!(
        live.hold(Uuid::new_v4(), claimed_at, DispatchRunner::WorkerProcess),
        Err(RegistryClosed)
    ));
    launched.stop().await;
    end_child(spawner, spawned).await;
}

/// Python `test_a_write_that_never_returns_stops_the_worker`: the watchdog
/// does not wait for a writer stuck in I/O.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_that_never_returns_stops_the_worker() {
    let silent = TcpListener::bind("127.0.0.1:0").expect("bind a silent listener");
    let spawner = Spawner::start().expect("the spawner starts");
    let (mut spawned, child) = sleeping_child(&spawner).await;
    let live = LiveDispatches::default();
    let _held = live
        .hold(Uuid::new_v4(), Utc::now(), DispatchRunner::Child(child))
        .expect("the registry is open");

    let launched = launch(silent_pool(&silent), &live, recovery_on(SHORT_WINDOW));
    let cause = launched.failed_closed().await;

    assert!(
        cause.ends_with("the last write has not returned"),
        "{cause}"
    );
    assert_eq!(
        wait_for_exit(&mut spawned.exit, WAIT).await,
        killed_by_sigkill()
    );
    launched.stop().await;
    end_child(spawner, spawned).await;
    drop(silent);
}

/// Python `test_a_dead_beat_thread_stops_the_worker`: a writer that ended
/// lands no beat, so the window passes and the held child is killed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_writer_stops_the_worker() {
    let spawner = Spawner::start().expect("the spawner starts");
    let (mut spawned, child) = sleeping_child(&spawner).await;
    let live = LiveDispatches::default();
    let _held = live
        .hold(Uuid::new_v4(), Utc::now(), DispatchRunner::Child(child))
        .expect("the registry is open");

    let launched = launch(refusing_pool(), &live, recovery_on(SHORT_WINDOW));
    launched.writer.abort();
    let cause = launched.failed_closed().await;

    assert!(
        cause.ends_with("the last write has not returned"),
        "{cause}"
    );
    assert_eq!(
        wait_for_exit(&mut spawned.exit, WAIT).await,
        killed_by_sigkill()
    );
    launched.stop().await;
    end_child(spawner, spawned).await;
}

/// Python `test_a_failed_write_inside_the_window_recovers`: writes fail for
/// 0.7 s, well inside the 2 s window; the beat that lands then restarts the
/// window, and the worker runs on past it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn a_failed_write_inside_the_window_recovers() {
    let database = crate::broker::terminalization_matrix::migrated_pool().await;
    let options = database.connect_options();
    let refuse_until = Instant::now() + Duration::from_millis(700);
    let gate = ClosingGate::start(
        options.get_host().to_owned(),
        options.get_port(),
        refuse_until,
    )
    .await;
    let pool = test_beat_pool(&crate::broker::pools::test_url_of(
        &(*options).clone().host("127.0.0.1").port(gate.port),
    ));
    let spawner = Spawner::start().expect("the spawner starts");
    let (spawned, child) = sleeping_child(&spawner).await;
    let live = LiveDispatches::default();
    let _held = live
        .hold(Uuid::new_v4(), Utc::now(), DispatchRunner::Child(child))
        .expect("the registry is open");

    let launched = launch(pool, &live, recovery_on(Duration::from_secs(2)));
    tokio::time::sleep(Duration::from_secs(3)).await;

    assert!(gate.refused.load(Ordering::SeqCst) > 0, "no write failed");
    assert!(gate.admitted.load(Ordering::SeqCst) > 0, "no write landed");
    launched.still_watching();
    assert_eq!(*spawned.exit.borrow(), ExitState::Running);
    launched.stop().await;
    end_child(spawner, spawned).await;
}

/// Python `test_beats_that_land_within_the_window_never_stop_the_worker`:
/// writes take 0.9 s, and the 1.5 s window counts from each landing, so the
/// next landing is inside it. Counted from the start of the write, 1.8 s
/// would pass, and the worker would stop while every beat lands.
#[test]
fn a_slow_beat_counts_from_its_landing() {
    let threshold = Duration::from_secs(3);
    let window = fail_closed_window(threshold);
    let started_at = Instant::now();
    let landed_at = started_at + Duration::from_millis(900);
    let coverage = Coverage {
        covered_at: window_start(threshold, started_at, landed_at),
        consecutive_failures: 0,
        last_failure: LastFailure::NoFailure,
    };
    assert_eq!(coverage.covered_at, landed_at);

    let next_landing = landed_at + Duration::from_millis(900);
    assert_eq!(
        check_coverage(&coverage, window, next_landing),
        WatchVerdict::Current
    );
    let from_the_start = Coverage {
        covered_at: started_at,
        ..coverage.clone()
    };
    assert!(matches!(
        check_coverage(&from_the_start, window, next_landing),
        WatchVerdict::Overdue { .. }
    ));
}

/// Python `test_stop_ends_both_threads`: the writer ends at its cancel and
/// closes its pool; the watchdog thread ends at its stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_ends_the_writer_and_the_watchdog() {
    let pool = refusing_pool();
    let launched = launch(
        Arc::clone(&pool),
        &LiveDispatches::default(),
        recovery_on(Duration::from_secs(30)),
    );
    let Launched {
        watch,
        writer,
        beats_cancel,
        ..
    } = launched;
    beats_cancel.cancel();
    tokio::time::timeout(WAIT, writer)
        .await
        .expect("the writer ends at its cancel")
        .expect("the writer does not panic");
    assert!(
        matches!(
            pool.checkout(CheckoutClass::Reserved).await,
            Err(crate::db::CheckoutFailure::PoolClosed { .. })
        ),
        "the writer closes its pool at its end"
    );
    tokio::time::timeout(WAIT, tokio::task::spawn_blocking(move || watch.stop()))
        .await
        .expect("the watchdog stops in time")
        .expect("the watchdog thread joins");
}

// ---------------------------------------------------------------------------
// Exit of the process
// ---------------------------------------------------------------------------

/// Set only in the process that [`task_code_in_the_worker_process_ends_the_process_at_once`]
/// starts.
const EXIT_PROBE_ENV: &str = "HORSIES_TEST_RUNNER_BEATS_EXIT_PROBE";
const EXIT_PROBE_TEST: &str =
    "worker::runner_beats::fail_closed_tests::exit_probe_task_code_in_the_worker_process";

/// The body of the probe process: a dispatch held by the worker process
/// itself, and writes that fail. The watchdog ends the process; without the
/// probe variable this test returns at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exit_probe_task_code_in_the_worker_process() {
    let Some(_probe) = std::env::var_os(EXIT_PROBE_ENV) else {
        return;
    };
    let live = LiveDispatches::default();
    let _held = live
        .hold(Uuid::new_v4(), Utc::now(), DispatchRunner::WorkerProcess)
        .expect("the registry is open");
    let launched = launch(refusing_pool(), &live, recovery_on(SHORT_WINDOW));
    tokio::time::sleep(WAIT).await;
    // Reached only when the watchdog did not end the process.
    launched.stop().await;
}

/// Python `test_a_closed_loop_exits_without_the_grace`: task code that runs
/// in the worker process stops only with the process, so the watchdog exits
/// with code 77 at once, without the grace for the worker's stop.
#[test]
fn task_code_in_the_worker_process_ends_the_process_at_once() {
    let started = Instant::now();
    let status = std::process::Command::new(std::env::current_exe().expect("the test binary"))
        .args([
            "--exact",
            EXIT_PROBE_TEST,
            "--nocapture",
            "--test-threads=1",
        ])
        .env(EXIT_PROBE_ENV, "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .expect("the probe process runs");
    assert_eq!(status.code(), Some(RUNNER_HEARTBEAT_LOST_EXIT_CODE));
    assert!(started.elapsed() < WAIT, "{:?}", started.elapsed());
}

// ---------------------------------------------------------------------------
// Stale running recovery off: the worker runs on
// ---------------------------------------------------------------------------

/// Python `test_with_recovery_off_failing_writes_never_stop_the_worker`: no
/// reaper fails the row, so a stop would protect nothing. And
/// `test_failed_writes_are_logged_at_most_once_per_log_interval`, the lines:
/// writes that fail for 1.5 s, inside one log interval
/// (`BEAT_FAILURE_LOG_INTERVAL`, 60 s), give one line, for the first
/// failure, which names the consequence of recovery off; the watchdog's
/// late line names the last error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn with_recovery_off_failing_writes_never_stop_the_worker() {
    let capture = LogCapture::new();
    let _default = capture.set_default();
    let spawner = Spawner::start().expect("the spawner starts");
    let (spawned, child) = sleeping_child(&spawner).await;
    let live = LiveDispatches::default();
    let _held = live
        .hold(Uuid::new_v4(), Utc::now(), DispatchRunner::Child(child))
        .expect("the registry is open");

    let launched = launch(refusing_pool(), &live, StaleRunningRecovery::Off);
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let failed = capture.lines_starting(tracing::Level::ERROR, FAILED_WRITE_LINE);
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert_eq!(failed[0].field("attempt"), Some("1"));
    assert!(
        failed[0]
            .message
            .contains("; no reaper fails a RUNNING row, so the worker runs on): "),
        "{}",
        failed[0].message
    );
    let late = one_late_line(&capture);
    let cause = late.field("cause").expect("the late line names its cause");
    assert!(cause.contains("last error: "), "{cause}");
    launched.still_watching();
    assert_eq!(*spawned.exit.borrow(), ExitState::Running);
    assert!(live
        .hold(Uuid::new_v4(), Utc::now(), DispatchRunner::WorkerProcess)
        .is_ok());
    launched.stop().await;
    end_child(spawner, spawned).await;
}

/// The writer of a dead-writer test: a defect at its first poll (Python
/// `_ScriptedWriter(defect)`).
async fn defective_writer() {
    panic!("scripted defect in the beat writer");
}

/// The beats of a dead-writer test: the watchdog and the supervised task of
/// [`launch_with_writer`], whose writer is [`defective_writer`].
fn launch_defective(live: &LiveDispatches, stale_running: StaleRunningRecovery) -> Launched {
    let worker_stop = CancellationToken::new();
    let beats_cancel = CancellationToken::new();
    let fail_closed_stop = FailClosedStop::new(worker_stop.clone());
    let (watch, writer) = launch_with_writer(
        INTERVAL,
        stale_running,
        live.clone(),
        beats_cancel.clone(),
        fail_closed_stop.clone(),
        |_coverage| defective_writer(),
    )
    .expect("the watchdog thread starts");
    Launched {
        watch,
        writer,
        beats_cancel,
        worker_stop,
        fail_closed_stop,
    }
}

/// Wait until the watchdog logs a dead writer; returns its lines so far.
async fn dead_writer_lines(capture: &LogCapture) -> Vec<CapturedEvent> {
    let deadline = Instant::now() + WAIT;
    loop {
        let lines = capture.lines_starting(tracing::Level::ERROR, DEAD_WRITER_LINE);
        match lines.is_empty() {
            false => return lines,
            true => assert!(
                Instant::now() < deadline,
                "the watchdog logged no dead writer in time"
            ),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Python `test_with_recovery_off_a_dead_beat_thread_is_logged_and_the_worker_runs`:
/// with stale running recovery off, a writer that dies is logged once by the
/// watchdog, with Python's text, and the watchdog ends. The task that the
/// worker supervises does not end before the beats stop, so its supervision
/// stops nothing: the worker is not asked to stop, the held child runs on,
/// and the registry takes new holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn with_recovery_off_a_dead_writer_is_logged_once_and_the_worker_runs() {
    let capture = LogCapture::new();
    let _default = capture.set_default();
    let spawner = Spawner::start().expect("the spawner starts");
    let (spawned, child) = sleeping_child(&spawner).await;
    let live = LiveDispatches::default();
    let _held = live
        .hold(Uuid::new_v4(), Utc::now(), DispatchRunner::Child(child))
        .expect("the registry is open");

    let Launched {
        watch,
        writer,
        beats_cancel,
        worker_stop,
        fail_closed_stop,
    } = launch_defective(&live, StaleRunningRecovery::Off);
    let (fatal_tx, mut fatal_rx) = tokio::sync::mpsc::unbounded_channel();
    let supervision = crate::worker::supervision::spawn_service_watchdog(
        "runner-heartbeat",
        writer,
        worker_stop.clone(),
        fatal_tx,
    );
    let _first = dead_writer_lines(&capture).await;
    // Four watch intervals more: a second line would come within them.
    tokio::time::sleep(WATCH_INTERVAL * 4).await;

    let lines = capture.lines_starting(tracing::Level::ERROR, DEAD_WRITER_LINE);
    let [line] = lines.as_slice() else {
        panic!("one dead-writer line, got {lines:?}");
    };
    assert_eq!(
        line.message,
        "the runner heartbeat writer died: panicked: scripted defect in the beat writer. The \
         worker runs on without runner heartbeats, because no reaper fails a RUNNING row \
         (auto_fail_stale_running is off, or no recovery config)"
    );
    assert_eq!(
        line.field("cause"),
        Some("panicked: scripted defect in the beat writer")
    );
    assert!(
        watch.watch.is_finished(),
        "the watchdog ends after the line"
    );
    assert_eq!(fail_closed_stop.state(), FailClosedState::Watching);
    assert!(
        !worker_stop.is_cancelled(),
        "the worker is not asked to stop"
    );
    assert!(!supervision.is_finished(), "the supervised task runs on");
    assert!(
        matches!(
            fatal_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ),
        "no service death is reported"
    );
    assert_eq!(*spawned.exit.borrow(), ExitState::Running);
    assert!(live
        .hold(Uuid::new_v4(), Utc::now(), DispatchRunner::WorkerProcess)
        .is_ok());

    // The stop of the worker: its token, then the beats after the drain.
    worker_stop.cancel();
    beats_cancel.cancel();
    tokio::time::timeout(WAIT, supervision)
        .await
        .expect("the supervised task ends at the stop")
        .expect("the supervision does not panic");
    assert!(fatal_rx.try_recv().is_err(), "the stop is no service death");
    tokio::time::timeout(WAIT, tokio::task::spawn_blocking(move || watch.stop()))
        .await
        .expect("the watchdog stops in time")
        .expect("the watchdog thread joins");
    end_child(spawner, spawned).await;
}

/// With stale running recovery on, a writer that dies stays fatal (Python
/// fails closed on a dead beat thread): the supervised task is the writer
/// itself, so its supervision reports the panic and stops the worker. The
/// watchdog logs no dead-writer line.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn with_recovery_on_a_dead_writer_stops_the_worker_through_its_supervision() {
    let capture = LogCapture::new();
    let _default = capture.set_default();
    let live = LiveDispatches::default();
    let Launched {
        watch,
        writer,
        beats_cancel,
        worker_stop,
        fail_closed_stop,
    } = launch_defective(&live, recovery_on(Duration::from_secs(30)));
    let (fatal_tx, mut fatal_rx) = tokio::sync::mpsc::unbounded_channel();
    let supervision = crate::worker::supervision::spawn_service_watchdog(
        "runner-heartbeat",
        writer,
        worker_stop.clone(),
        fatal_tx,
    );

    tokio::time::timeout(WAIT, supervision)
        .await
        .expect("the supervision sees the writer end")
        .expect("the supervision does not panic");
    let exit = fatal_rx
        .try_recv()
        .expect("the dead writer is a service death");
    assert_eq!(exit.service, "runner-heartbeat");
    assert_eq!(exit.reason, "panicked: scripted defect in the beat writer");
    assert!(
        worker_stop.is_cancelled(),
        "the supervision stops the worker"
    );
    assert_eq!(fail_closed_stop.state(), FailClosedState::Watching);
    assert!(capture
        .lines_starting(tracing::Level::ERROR, DEAD_WRITER_LINE)
        .is_empty());

    beats_cancel.cancel();
    tokio::time::timeout(WAIT, tokio::task::spawn_blocking(move || watch.stop()))
        .await
        .expect("the watchdog stops in time")
        .expect("the watchdog thread joins");
}

/// Python `test_with_recovery_off_a_late_beat_is_logged`: a write that never
/// returns is no failure yet; after two intervals the watch reads it as late
/// and names the unreturned write, and the worker runs on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn with_recovery_off_a_late_beat_names_the_unreturned_write() {
    let late_after = INTERVAL * LATE_BEAT_INTERVALS;
    let started_at = Instant::now();
    let coverage = Coverage {
        covered_at: started_at,
        consecutive_failures: 0,
        last_failure: LastFailure::NoFailure,
    };
    let verdict = check_coverage(&coverage, late_after, started_at + late_after * 2);
    assert!(
        matches!(verdict, WatchVerdict::Overdue { .. }),
        "{verdict:?}"
    );
    assert!(describe(&verdict).ends_with("the last write has not returned"));

    let silent = TcpListener::bind("127.0.0.1:0").expect("bind a silent listener");
    let spawner = Spawner::start().expect("the spawner starts");
    let (spawned, child) = sleeping_child(&spawner).await;
    let live = LiveDispatches::default();
    let _held = live
        .hold(Uuid::new_v4(), Utc::now(), DispatchRunner::Child(child))
        .expect("the registry is open");
    let capture = LogCapture::new();
    let _default = capture.set_default();
    let launched = launch(silent_pool(&silent), &live, StaleRunningRecovery::Off);
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let late = one_late_line(&capture);
    let cause = late.field("cause").expect("the late line names its cause");
    assert!(
        cause.ends_with("the last write has not returned"),
        "{cause}"
    );
    assert!(capture
        .lines_starting(tracing::Level::ERROR, FAILED_WRITE_LINE)
        .is_empty());
    launched.still_watching();
    assert_eq!(*spawned.exit.borrow(), ExitState::Running);
    launched.stop().await;
    end_child(spawner, spawned).await;
    drop(silent);
}

/// Python `test_failed_writes_are_logged_at_most_once_per_log_interval`:
/// writes that fail 20 times a second for 1.2 s give one log line per 0.5 s
/// of log interval; a landed beat resets the bound.
#[test]
fn failed_writes_are_logged_at_most_once_per_log_interval() {
    let start = Instant::now();
    let mut gate = LogGate::new(Duration::from_millis(500));
    let failures_at = (0..24_u32).map(|step| start + Duration::from_millis(50) * step);
    let logged = failures_at.filter(|at| gate.due(*at)).count();
    assert_eq!(logged, 3);

    gate.reset();
    assert!(gate.due(start + Duration::from_millis(1_250)));
    assert!(!gate.due(start + Duration::from_millis(1_300)));

    let mut every_minute = LogGate::new(BEAT_FAILURE_LOG_INTERVAL);
    assert!(every_minute.due(start));
    assert!(!every_minute.due(start + BEAT_FAILURE_LOG_INTERVAL - Duration::from_millis(1)));
    assert!(every_minute.due(start + BEAT_FAILURE_LOG_INTERVAL));
}

/// The one late-beat line of the watchdog: a warning, logged once per log
/// interval (`BEAT_FAILURE_LOG_INTERVAL`, 60 s, longer than any test).
fn one_late_line(capture: &LogCapture) -> CapturedEvent {
    let late = capture.lines_starting(tracing::Level::WARN, LATE_BEATS_LINE);
    match <[CapturedEvent; 1]>::try_from(late) {
        Ok([line]) => line,
        Err(lines) => panic!("one late-beat line, got {lines:?}"),
    }
}

/// Python `test_kill_child_session_reports_a_refused_kill`: a kill of a
/// child that held tasks, which the OS refuses (`KillOutcome::NotPermitted`,
/// errno `EPERM`), is logged with the child and its tasks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_kill_is_logged_with_the_child_and_its_tasks() {
    let capture = LogCapture::new();
    let _default = capture.set_default();
    let spawner = Spawner::start().expect("the spawner starts");
    let (spawned, child) = sleeping_child(&spawner).await;
    let pid = child.pid.get().to_string();
    let task_id = Uuid::new_v4();

    log_kill(
        &ChildToKill {
            child,
            task_ids: vec![task_id],
        },
        KillOutcome::NotPermitted,
    );

    let refused =
        capture.lines_starting(tracing::Level::ERROR, "cannot kill a child that held tasks");
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert_eq!(refused[0].field("pid"), Some(pid.as_str()));
    assert_eq!(refused[0].field("outcome"), Some("NotPermitted"));
    let tasks = refused[0].field("tasks").expect("the line names the tasks");
    assert!(tasks.contains(&task_id.to_string()), "{tasks}");
    end_child(spawner, spawned).await;
}

/// Set only in the process that [`a_worker_that_does_not_stop_within_the_grace_is_exited`]
/// starts.
const GRACE_PROBE_ENV: &str = "HORSIES_TEST_RUNNER_BEATS_GRACE_PROBE";
const GRACE_PROBE_TEST: &str =
    "worker::runner_beats::fail_closed_tests::grace_probe_a_worker_that_does_not_stop";

/// The body of the probe process: a child holds a task, writes fail, and
/// nothing stops the watchdog after it asks the worker to stop. The watchdog
/// ends the process after the grace; without the probe variable this test
/// returns at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grace_probe_a_worker_that_does_not_stop() {
    let Some(_probe) = std::env::var_os(GRACE_PROBE_ENV) else {
        return;
    };
    let spawner = Spawner::start().expect("the spawner starts");
    let (_spawned, child) = sleeping_child(&spawner).await;
    let live = LiveDispatches::default();
    let _held = live
        .hold(Uuid::new_v4(), Utc::now(), DispatchRunner::Child(child))
        .expect("the registry is open");
    let launched = launch(refusing_pool(), &live, recovery_on(SHORT_WINDOW));
    tokio::time::sleep(FAIL_CLOSED_EXIT_GRACE + WAIT).await;
    // Reached only when the watchdog did not end the process.
    launched.stop().await;
}

/// Python `test_a_worker_that_does_not_stop_is_exited`: after it fails
/// closed, the watchdog asks the worker to stop; a worker that has not
/// stopped within `FAIL_CLOSED_EXIT_GRACE` (120 s) is ended with code 77.
/// The test waits the whole grace.
#[test]
fn a_worker_that_does_not_stop_within_the_grace_is_exited() {
    let started = Instant::now();
    let status = std::process::Command::new(std::env::current_exe().expect("the test binary"))
        .args([
            "--exact",
            GRACE_PROBE_TEST,
            "--nocapture",
            "--test-threads=1",
        ])
        .env(GRACE_PROBE_ENV, "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .expect("the probe process runs");
    let elapsed = started.elapsed();
    assert_eq!(status.code(), Some(RUNNER_HEARTBEAT_LOST_EXIT_CODE));
    assert!(elapsed >= FAIL_CLOSED_EXIT_GRACE, "{elapsed:?}");
    assert!(elapsed < FAIL_CLOSED_EXIT_GRACE + WAIT, "{elapsed:?}");
}

// ---------------------------------------------------------------------------
// Parts
// ---------------------------------------------------------------------------

/// Python `test_only_a_reaper_that_fails_stale_running_rows_makes_beats_fail_closed`:
/// the default config fails stale RUNNING rows at 300 s, so the beats fail
/// closed; with `auto_fail_stale_running` off they only log.
#[test]
fn only_a_reaper_that_fails_stale_running_rows_makes_beats_fail_closed() {
    assert_eq!(
        StaleRunningRecovery::from_config(&RecoveryConfig::default()),
        StaleRunningRecovery::On {
            threshold: Duration::from_millis(300_000)
        }
    );
    let off = RecoveryConfig {
        auto_fail_stale_running: false,
        ..RecoveryConfig::default()
    };
    assert_eq!(
        StaleRunningRecovery::from_config(&off),
        StaleRunningRecovery::Off
    );
}

/// Python `test_the_kill_margin_is_above_one_watch_and_below_one_interval`:
/// above one watch interval, so the watchdog sees the end of the window in
/// time; below the smallest beat interval the config accepts, so a beat
/// that lands on time never starts its window early.
#[test]
fn the_kill_margin_is_above_one_watch_and_below_the_smallest_interval() {
    let zero_interval = RecoveryConfig {
        runner_heartbeat_interval_ms: 0,
        ..RecoveryConfig::default()
    };
    let smallest_interval_ms = zero_interval
        .validate()
        .into_iter()
        .find_map(|error| match error {
            RecoveryConfigError::BelowMinimum {
                field: "runner_heartbeat_interval_ms",
                min,
                ..
            } => Some(min),
            _ => None,
        })
        .expect("the config refuses a beat interval of 0");
    assert!(WATCH_INTERVAL < FAIL_CLOSED_KILL_MARGIN);
    assert!(FAIL_CLOSED_KILL_MARGIN < Duration::from_millis(smallest_interval_ms));
}

/// Python `test_an_unreachable_database_is_a_failed_write`: each write to an
/// unreachable server returns its failure.
#[tokio::test]
async fn an_unreachable_database_is_a_failed_write() {
    let pool = refusing_pool();
    let task_ids = vec![Uuid::new_v4()];
    let generations = vec![Utc::now()];
    assert!(write_beats(&pool, &sender(), &task_ids, &generations)
        .await
        .is_err());
    assert!(write_beats(&pool, &sender(), &task_ids, &generations)
        .await
        .is_err());
}

/// Python `test_each_cause_names_what_failed`, the cause of an overdue beat:
/// the time without a landed beat, the window, and the last failure or the
/// write that has not returned.
#[test]
fn each_cause_names_what_failed() {
    let overdue = WatchVerdict::Overdue {
        uncovered: Duration::from_secs(151),
        window: Duration::from_secs(150),
        last_failure: LastFailure::WriteFailed("OperationalError: refused".to_owned()),
    };
    assert_eq!(
        describe(&overdue),
        "no runner heartbeat landed for 151.0s, past 150.0s (half of \
         running_stale_threshold_ms); last error: OperationalError: refused"
    );
    let unreturned = WatchVerdict::Overdue {
        uncovered: Duration::from_secs(2),
        window: Duration::from_secs(1),
        last_failure: LastFailure::NoFailure,
    };
    assert!(describe(&unreturned).ends_with("the last write has not returned"));
}

// ---------------------------------------------------------------------------
// The isolated slots hold their tasks
// ---------------------------------------------------------------------------

const TASK: &str = "0190f5a2-7b1c-7d3e-8f00-0000000000bb";
const WORKER: &str = "worker-1";

fn claimed_task() -> ClaimedTask {
    ClaimedTask {
        task_id: Uuid::parse_str(TASK).expect("a valid task id"),
        task_name: "app.tasks.beat".to_owned(),
        queue_name: "default".to_owned(),
        claimed_at: Utc
            .with_ymd_and_hms(2026, 11, 16, 9, 30, 0)
            .single()
            .expect("a valid claim time"),
        execution: ExecutionClass::Isolated,
        kind: TaskKind::Plain,
        retry: RetryFacts {
            retry_count: 0,
            max_retries: 0,
        },
        options: read_stored_options(Some("{}")),
    }
}

fn input() -> TaskInputText {
    TaskInputText::new("{}".to_owned())
}

fn init_frame() -> InitFrame {
    InitFrame {
        program: ChildProgram::Rust,
        log_level: ChildLogLevel::Info,
        database_url: DatabaseUrl::new("postgresql://u@h/db".to_owned()),
        connection: ChildConnection::default(),
        process_name: "horsies-test-child".to_owned(),
        child_kind: ChildKind::Isolated(IsolatedChild {
            pool: ChildPool {
                min_size: PoolMinSize::new(0).expect("a valid pool size"),
                max_size: PoolMaxSize::new(1).expect("a valid pool size"),
                check: true,
            },
            task_send_pool: TaskSendPool::new(1).expect("a valid pool size"),
        }),
    }
}

/// `printf` text that writes `bytes`: one octal escape per byte.
fn printf_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("\\{byte:03o}")).collect()
}

/// Shell step that reads and drops the next `count` bytes of the socket.
/// `dd` with one-byte blocks reads one byte per `read`, so it never takes a
/// byte past `count`. Not `head -c`: BSD `head` (macOS) reads a full stdio
/// buffer from the socket and drops the bytes it does not write.
fn drop_socket_bytes(count: usize) -> String {
    format!(r#"dd bs=1 count={count} <&"$0" >/dev/null 2>&1"#)
}

/// The bytes of the run frame the pool sends for `claimed`.
fn run_frame_bytes(claimed: &ClaimedTask) -> Vec<u8> {
    encode_parent_frame(&ParentFrame::Run(RunFrame {
        task_id: claimed.task_id,
        task_name: claimed.task_name.clone(),
        worker_id: WORKER.to_owned(),
        claimed_at: claimed.claimed_at,
        orphan_self_heal: crate::broker::OrphanSelfHeal::Off,
        work: IsolatedWork::Run,
        input: input(),
    }))
    .expect("the run frame encodes")
    .to_bytes()
}

/// A child script: take the init frame, send ready, take the run frame,
/// then do `after_run`.
fn scripted_child(claimed: &ClaimedTask, after_run: &str) -> ChildCommand {
    let init = encode_parent_frame(&ParentFrame::Init(init_frame()))
        .expect("the init frame encodes")
        .to_bytes();
    let ready = encode_child_frame(&ChildFrame::Ready(ReadyFrame {
        pid: ProcessId::new(1).expect("a valid pid"),
        rss: RssReading::Unreadable,
    }))
    .expect("the ready frame encodes")
    .to_bytes();
    let script = format!(
        r#"{take_init}; printf '{ready}' >&"$0"; {take_run}; {after_run}"#,
        take_init = drop_socket_bytes(init.len()),
        ready = printf_bytes(&ready),
        take_run = drop_socket_bytes(run_frame_bytes(claimed).len()),
    );
    ChildCommand {
        program: "/bin/sh".into(),
        args: vec!["-c".into(), script.into()],
    }
}

/// The child script that answers `claimed` with an ok result after a
/// second, then waits for the stop frame.
fn answer_after_a_second(claimed: &ClaimedTask) -> String {
    let result = encode_child_frame(&ChildFrame::Result(ResultFrame {
        task_id: claimed.task_id,
        end: RunEnd::Result(ResultEnvelopeText::new(
            r#"{"__h_task_result__":true,"ok":3,"err":null}"#.to_owned(),
        )),
        rss: RssReading::Unreadable,
        task_loop: NoLeftWork::NoTaskLoop,
    }))
    .expect("the result frame encodes")
    .to_bytes();
    format!(
        r#"sleep 1; printf '{}' >&"$0"; {}; exit 0"#,
        printf_bytes(&result),
        drop_socket_bytes(27),
    )
}

/// One isolated slot whose children run `command`, on the registry `live`.
fn start_pool(spawner: &Spawner, live: &LiveDispatches, command: ChildCommand) -> IsolatedPool {
    let board = Arc::new(PoolBoard::new(ChildLayout {
        isolated: 1,
        shared_async: 0,
        shared_thread: 0,
    }));
    IsolatedPool::start(
        IsolatedPoolConfig {
            command,
            init: init_frame(),
            worker_id: WORKER.to_owned(),
            orphan_self_heal: crate::broker::OrphanSelfHeal::Off,
            pending_limit: NonZeroUsize::new(4).expect("a positive pending limit"),
            tasks_per_child: TaskLimit::Off,
            memory_per_child: MemoryLimit::Off,
            exit_not_seen: ExitNotSeen::LeaveToRecovery,
            child_cpus: ChildCpus::Inherited,
            kill_wait: CHILD_KILL_WAIT,
        },
        board,
        spawner.handle(),
        live.clone(),
    )
}

fn queued(submitted: Submitted) -> oneshot::Receiver<AttemptEnd> {
    match submitted {
        Submitted::Queued(receiver) => receiver,
        Submitted::NotDispatched(cause) => panic!("not dispatched: {cause}"),
    }
}

async fn answer(receiver: oneshot::Receiver<AttemptEnd>) -> AttemptEnd {
    tokio::time::timeout(WAIT, receiver)
        .await
        .expect("the slot answers in time")
        .expect("the slot sends an answer")
}

/// Wait until the registry holds a dispatch; returns what it holds.
async fn until_held(live: &LiveDispatches) -> (Vec<Uuid>, Vec<DateTime<Utc>>) {
    let deadline = Instant::now() + WAIT;
    loop {
        let held = live.snapshot();
        match held.0.is_empty() {
            false => return held,
            true => assert!(Instant::now() < deadline, "no dispatch was held in time"),
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn nothing_held() -> (Vec<Uuid>, Vec<DateTime<Utc>>) {
    (Vec::new(), Vec::new())
}

/// Python `test_the_pool_holds_a_task_from_the_send_to_the_answer`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_pool_holds_a_task_from_the_send_to_the_answer() {
    let claimed = claimed_task();
    let spawner = Spawner::start().expect("the spawner starts");
    let live = LiveDispatches::default();
    let pool = start_pool(
        &spawner,
        &live,
        scripted_child(&claimed, &answer_after_a_second(&claimed)),
    );
    let receiver = queued(pool.submit(
        &claimed,
        input(),
        IsolatedWork::Run,
        IsolatedDeadline::NoDeadline,
    ));

    assert_eq!(
        until_held(&live).await,
        (vec![claimed.task_id], vec![claimed.claimed_at])
    );
    let end = answer(receiver).await;
    assert!(
        matches!(end, AttemptEnd::Reported(RunEnd::Result(_))),
        "{end:?}"
    );
    assert_eq!(live.snapshot(), nothing_held());
    let _report = pool.stop(Duration::from_secs(5)).await;
    let _spawner = spawner.stop(WAIT);
}

/// Python `test_a_kill_releases_the_task_before_the_child_exits`, the part
/// this test can see: a task killed at its deadline is no longer held when
/// its end comes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_killed_at_its_deadline_is_no_longer_held() {
    let claimed = claimed_task();
    let spawner = Spawner::start().expect("the spawner starts");
    let live = LiveDispatches::default();
    let pool = start_pool(&spawner, &live, scripted_child(&claimed, "exec sleep 60"));
    let timeout = TaskTimeoutMs::new(1_000).expect("a valid timeout");
    let receiver = queued(pool.submit(
        &claimed,
        input(),
        IsolatedWork::Run,
        IsolatedDeadline::After(timeout),
    ));

    let _held = until_held(&live).await;
    assert_eq!(
        answer(receiver).await,
        AttemptEnd::TimedOut {
            timeout,
            retry: RetryPlacement::SameClass,
            cancel: TimeoutCancel::NoSignal,
        }
    );
    assert_eq!(live.snapshot(), nothing_held());
    let _report = pool.stop(Duration::from_secs(5)).await;
    let _spawner = spawner.stop(WAIT);
}

/// Python `test_a_closed_registry_kills_the_child_that_takes_a_task`: after
/// a fail-closed stop no beat would cover a task, so the slot does not send
/// it, the task ends not dispatched (its row goes back to the queue), and
/// the child is ended.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_closed_registry_refuses_the_task_at_its_child() {
    let claimed = claimed_task();
    let spawner = Spawner::start().expect("the spawner starts");
    let live = LiveDispatches::default();
    assert!(live.close().is_empty());
    let pool = start_pool(&spawner, &live, scripted_child(&claimed, "exec sleep 60"));
    let receiver = queued(pool.submit(
        &claimed,
        input(),
        IsolatedWork::Run,
        IsolatedDeadline::NoDeadline,
    ));

    assert_eq!(
        answer(receiver).await,
        AttemptEnd::NotDispatched(NotRunCause::RunnerBeatsFailedClosed)
    );
    assert_eq!(live.snapshot(), nothing_held());
    let _report = pool.stop(Duration::from_secs(5)).await;
    let _spawner = spawner.stop(WAIT);
}

/// Python `test_a_fail_closed_kill_reads_as_a_kill_by_the_worker`: enter at
/// the watchdog, read the loss at the slot: the kill is the worker's, not a
/// death of the child.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fail_closed_kill_reads_as_a_kill_by_the_worker() {
    let claimed = claimed_task();
    let spawner = Spawner::start().expect("the spawner starts");
    let live = LiveDispatches::default();
    let pool = start_pool(&spawner, &live, scripted_child(&claimed, "exec sleep 60"));
    let receiver = queued(pool.submit(
        &claimed,
        input(),
        IsolatedWork::Run,
        IsolatedDeadline::NoDeadline,
    ));
    let _held = until_held(&live).await;

    let launched = launch(refusing_pool(), &live, recovery_on(SHORT_WINDOW));
    match answer(receiver).await {
        AttemptEnd::Crashed { reason } => {
            assert!(
                reason.as_str().starts_with("killed by the worker"),
                "{reason}"
            );
        }
        other => panic!("expected the crash path of a kill by the worker, got {other:?}"),
    }
    let cause = launched.failed_closed().await;
    assert!(cause.contains("; last error: "), "{cause}");
    launched.stop().await;
    let _report = pool.stop(Duration::from_secs(5)).await;
    let _spawner = spawner.stop(WAIT);
}

//! Ports of Python `clients/python/tests/unit/test_child_placement.py`: each child gets
//! its CPUs before its init frame, a child whose CPUs the OS refuses is
//! killed and its start fails, and the start places the worker's threads
//! after its children started. These tests set real CPU masks: Linux only.
//! A worker thread placement touches only a thread that the test owns.

use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;

use super::scripted::{
    idle_child, isolated_config, isolated_init, process_ended, read_frame, send, serving, shell,
    start_isolated, take_init, until, Shown, TestDir, SPAWNER_STOP_WAIT, STATE_WAIT,
};
use super::*;
use crate::worker::executor::placement::{
    allowed_cpus, parse_cpu_list, plan_own_core, ChildCpus, CpuIds, CpuPlacement, WorkerThreads,
    MAX_CPU_ID,
};
use crate::worker::executor::protocol::{ChildFrame, ProcessId, ReadyFrame, RssReading};
use crate::worker::executor::supervisor::{BootFailureCause, Spawner, CHILD_BOOT_ATTEMPTS};

const START_WAIT: Duration = Duration::from_secs(30);

fn cpus(ids: &[u32]) -> CpuIds {
    CpuIds::new(ids.iter().copied().collect()).unwrap()
}

/// The `Cpus_allowed_list` of a status file.
fn allowed_list(status_path: &str) -> CpuIds {
    let status = std::fs::read_to_string(status_path).unwrap();
    let line = status
        .lines()
        .find(|line| line.starts_with("Cpus_allowed_list:"))
        .expect("the status names the allowed CPUs");
    parse_cpu_list(line.split_once(':').unwrap().1).unwrap()
}

async fn failure_of(board: &PoolBoard) -> PoolFailure {
    until(
        "the pool failed",
        STATE_WAIT,
        &[Shown::Board(board)],
        || matches!(board.health(), PoolHealth::Failed(_)),
    )
    .await;
    match board.health() {
        PoolHealth::Failed(failure) => failure,
        other => panic!("expected a failed pool, got {other:?}"),
    }
}

fn ready() -> ChildFrame {
    ChildFrame::Ready(ReadyFrame {
        pid: ProcessId::new(1).unwrap(),
        rss: RssReading::Unreadable,
    })
}

/// A child that reads its init frame, writes its allowed CPUs to `file`,
/// reports ready, and exits at the stop frame.
fn child_that_reports_its_cpus(file: &str) -> String {
    format!(
        "{}grep Cpus_allowed_list /proc/$$/status > {file}; {}{}exit 3",
        take_init(),
        send(&[ready()]),
        read_frame(),
    )
}

fn cpus_in(file: &str) -> CpuIds {
    let text = std::fs::read_to_string(file).unwrap();
    parse_cpu_list(text.split_once(':').unwrap().1).unwrap()
}

/// Python `test_a_refused_child_placement_kills_the_child_and_fails_its_start`:
/// a child whose CPUs the OS refuses never gets its init frame, is killed,
/// and its start fails as `SpawnFailed` with its pid in the detail. The
/// same placement fails again, so the boot attempts run out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_child_placement_kills_the_child_and_fails_its_start() {
    let dir = TestDir::new();
    let inited = dir.file("inited");
    let script = format!("{}echo $$ >> {inited}; exec sleep 60", take_init(),);
    let config = IsolatedPoolConfig {
        child_cpus: ChildCpus::Set(cpus(&[MAX_CPU_ID])),
        ..isolated_config(shell(&script))
    };
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, config);

    let failure = failure_of(&board).await;

    let last = match failure {
        PoolFailure::BootAttemptsExhausted { attempts, last } => {
            assert_eq!(attempts, CHILD_BOOT_ATTEMPTS);
            last
        }
        other => panic!("expected the boot attempts to run out, got {other:?}"),
    };
    assert_eq!(last.cause, BootFailureCause::SpawnFailed);
    let detail = last.detail.as_str();
    let pid: libc::pid_t = detail
        .strip_prefix("the CPUs of child ")
        .and_then(|rest| rest.split_once(' '))
        .and_then(|(pid, _)| pid.parse().ok())
        .unwrap_or_else(|| panic!("the detail names the child: {detail}"));
    assert!(
        process_ended(pid),
        "child {pid} outlived its refused placement"
    );
    assert!(
        dir.lines("inited").is_empty(),
        "a child read its init frame"
    );
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// Python `test_a_child_is_placed_before_its_init_frame`: the child runs on
/// its CPUs when it reads its init frame.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_is_placed_before_its_init_frame() {
    let dir = TestDir::new();
    let file = dir.file("cpus");
    let lowest = cpus(&[allowed_cpus().unwrap().lowest()]);
    let config = IsolatedPoolConfig {
        child_cpus: ChildCpus::Set(lowest.clone()),
        ..isolated_config(shell(&child_that_reports_its_cpus(&file)))
    };
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, config);

    let _ = serving(&board, 0).await;

    assert_eq!(cpus_in(&file), lowest);
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// Python `test_shared_cpus_place_no_child`: a child with inherited CPUs
/// keeps the CPUs of the worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inherited_cpus_place_no_child() {
    let dir = TestDir::new();
    let file = dir.file("cpus");
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(
        &spawner,
        1,
        isolated_config(shell(&child_that_reports_its_cpus(&file))),
    );

    let _ = serving(&board, 0).await;

    assert_eq!(cpus_in(&file), allowed_cpus().unwrap());
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// A thread that the test owns, listed alone for the worker placement.
struct OwnThread {
    thread_id: u32,
    end: mpsc::Sender<()>,
    handle: std::thread::JoinHandle<()>,
    dir: PathBuf,
}

impl OwnThread {
    fn start() -> Self {
        let (id_tx, id_rx) = mpsc::channel();
        let (end, end_rx) = mpsc::channel::<()>();
        let handle = std::thread::spawn(move || {
            // SAFETY: gettid takes no argument.
            let thread_id = unsafe { libc::syscall(libc::SYS_gettid) };
            id_tx.send(u32::try_from(thread_id).unwrap()).unwrap();
            let _ = end_rx.recv();
        });
        let thread_id = id_rx.recv().unwrap();
        let dir =
            std::env::temp_dir().join(format!("horsies-threads-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(dir.join(thread_id.to_string())).unwrap();
        Self {
            thread_id,
            end,
            handle,
            dir,
        }
    }

    fn listing(&self) -> WorkerThreads {
        WorkerThreads::listed_in(self.dir.clone())
    }

    fn cpus(&self) -> CpuIds {
        allowed_list(&format!("/proc/self/task/{}/status", self.thread_id))
    }

    fn end(self) {
        self.end.send(()).unwrap();
        self.handle.join().unwrap();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Python `test_the_start_places_the_worker_after_its_children_started` and
/// `test_real_children_start_on_the_children_cpus`: on this host's own
/// split, two children start on the children's CPUs, and the start then
/// places the worker's threads on its core. Skipped on a host that cannot
/// split its CPUs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_children_start_on_the_children_cpus_and_the_worker_on_its_core() {
    let own = match plan_own_core() {
        Ok(own) => own,
        Err(refused) => {
            eprintln!("skipped: this host cannot split its CPUs: {refused}");
            return;
        }
    };
    let placement = CpuPlacement::OwnCore(own.clone());
    let config = IsolatedPoolConfig {
        child_cpus: placement.child_cpus(),
        ..isolated_config(shell(&idle_child(&isolated_init())))
    };
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 2, config);
    let thread = OwnThread::start();
    let off = BaselineLimits {
        isolated: MemoryLimit::Off,
        shared_async: MemoryLimit::Off,
        shared_thread: MemoryLimit::Off,
    };

    let started = tokio::time::timeout(
        START_WAIT,
        start_children(&board, &placement, &thread.listing(), &off),
    )
    .await
    .unwrap();

    let baselines = match started {
        Ok(ChildrenStart::Started { baselines }) => baselines,
        other => panic!("expected a start, got {other:?}"),
    };
    assert_eq!(baselines.len(), 2);
    for baseline in &baselines {
        let status = format!("/proc/{}/status", baseline.pid.get());
        assert_eq!(&allowed_list(&status), own.children());
    }
    assert_eq!(&thread.cpus(), own.parent());
    thread.end();
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// Python `test_shared_cpus_place_no_worker_thread`: with shared CPUs the
/// start places no thread of the worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_cpus_place_no_worker_thread() {
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(
        &spawner,
        1,
        isolated_config(shell(&idle_child(&isolated_init()))),
    );
    let thread = OwnThread::start();
    let before = thread.cpus();
    let off = BaselineLimits {
        isolated: MemoryLimit::Off,
        shared_async: MemoryLimit::Off,
        shared_thread: MemoryLimit::Off,
    };

    let started = tokio::time::timeout(
        START_WAIT,
        start_children(&board, &CpuPlacement::SharedCpus, &thread.listing(), &off),
    )
    .await
    .unwrap();

    assert!(
        matches!(started, Ok(ChildrenStart::Started { .. })),
        "{started:?}"
    );
    assert_eq!(thread.cpus(), before);
    thread.end();
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

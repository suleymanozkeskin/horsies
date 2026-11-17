//! Tests of the preload fork path, Linux only, with a scripted preload
//! process: a `python3` program that reads the init frame, writes preload
//! frames, and answers each spawn request by a real double fork whose
//! child runs a `/bin/sh` script on the socket it was given.
//!
//! The subreaper flag and the orphan sweep act on the whole process, and a
//! test process holds many spawners, so each test runs in a child process
//! of the test binary, alone.

use std::io::Read;
use std::time::{Duration, Instant};

use super::*;
use crate::worker::executor::protocol::{
    ChildConnection, ChildKind, ChildLogLevel, ChildPool, ChildProgram, DatabaseUrl, IsolatedChild,
    PoolMaxSize, PoolMinSize, TaskSendPool,
};
use crate::worker::executor::supervisor::exit::{ChildExit, ReapedExit};
use crate::worker::executor::supervisor::spawner::{wait_for_exit, Spawner};

/// The child test to run, set in the environment of a child process.
const CHILD_TEST_ENV: &str = "HORSIES_PRELOAD_CHILD_TEST";

/// The longest wait of a test for a child's bytes or exit.
const TEST_WAIT: Duration = Duration::from_secs(20);

/// The scripted preload process. Arguments after the program: the
/// behaviour, then what the spawner appends (socket descriptor, worker pid,
/// mode). Behaviours:
/// - `fork`: ready, then a double fork per request.
/// - `refuse`: refuse at the start, exit 1.
/// - `never_ready`: read the init frame and sleep.
/// - `fork_fails`: ready, then `fork_failed` per request.
/// - `silent`: ready, then never answer a request.
/// - `exit_once:<dir>`: as `fork`, but the first process of the test
///   exits at its first request.
/// - `orphan:<file>`: as `fork`, and each middle process leaves an exited
///   orphan in its own session, whose pid goes to `<file>`.
const SCRIPTED_PRELOAD: &str = r#"
import ctypes, json, os, signal, socket, struct, sys, time

behaviour, fd, worker_pid, mode = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
ctrl = socket.socket(fileno=fd)

def send(frame):
    meta = json.dumps(frame, separators=(',', ':')).encode()
    ctrl.sendall(struct.pack('>IQ', len(meta), 0) + meta)

def read_exact(count):
    data = b''
    while len(data) < count:
        chunk = ctrl.recv(count - len(data))
        if not chunk:
            os._exit(0)
        data += chunk
    return data

meta_bytes, body_bytes = struct.unpack('>IQ', read_exact(12))
init = json.loads(read_exact(meta_bytes))
read_exact(body_bytes)
assert init['kind'] == 'init', init
if behaviour == 'refuse':
    send({'kind': 'preload_refused', 'reason': 'import failed'})
    os._exit(1)
if behaviour == 'never_ready':
    time.sleep(3600)
    os._exit(0)
exit_now = False
if behaviour.startswith('exit_once:'):
    marker = os.path.join(behaviour.split(':', 1)[1], 'exited')
    exit_now = not os.path.exists(marker)
    if exit_now:
        open(marker, 'w').close()
send({'kind': 'preload_ready', 'pid': os.getpid()})
CHILD = 'printf forked >&"$0"; dd bs=1 count=1 <&"$0" >/dev/null 2>&1; exit 7'
while True:
    message, fds, _flags, _address = socket.recv_fds(ctrl, 1, 1)
    if not message:
        os._exit(0)
    child_fd = fds[0]
    if exit_now:
        os._exit(5)
    if behaviour == 'fork_fails':
        os.close(child_fd)
        send({'kind': 'fork_failed', 'reason': 'no fork today'})
        continue
    if behaviour == 'silent':
        os.close(child_fd)
        continue
    read_end, write_end = os.pipe()
    middle = os.fork()
    if middle == 0:
        if behaviour.startswith('orphan:'):
            stray = os.fork()
            if stray == 0:
                os.setsid()
                os._exit(0)
            os.waitid(os.P_PID, stray, os.WEXITED | os.WNOWAIT)
            with open(behaviour.split(':', 1)[1], 'w') as out:
                out.write(str(stray))
        child = os.fork()
        if child == 0:
            ctrl.close()
            os.close(read_end)
            os.close(write_end)
            os.setsid()
            deadline = time.monotonic() + 5
            while os.getppid() != worker_pid:
                if time.monotonic() > deadline:
                    os._exit(76)
                time.sleep(0.0002)
            ctypes.CDLL(None).prctl(1, signal.SIGKILL, 0, 0, 0)
            os.set_inheritable(child_fd, True)
            os.execv('/bin/sh', ['/bin/sh', '-c', CHILD, str(child_fd), str(worker_pid)])
        os.write(write_end, str(child).encode())
        os._exit(0)
    os.close(write_end)
    reported = os.read(read_end, 32)
    os.close(read_end)
    os.waitpid(middle, 0)
    os.close(child_fd)
    send({'kind': 'child_forked', 'pid': int(reported)})
"#;

/// An `exec` child: it writes `exec__`, then exits 8 at the EOF of its
/// socket.
const EXEC_CHILD: &str = r#"printf exec__ >&"$0"; dd bs=1 count=1 <&"$0" >/dev/null 2>&1; exit 8"#;

/// The bytes a forked child of the scripted preload writes first.
const FORKED_MARK: &[u8; 6] = b"forked";
/// The bytes an `exec` child writes first.
const EXEC_MARK: &[u8; 6] = b"exec__";

fn exec_command() -> ChildCommand {
    ChildCommand {
        program: "/bin/sh".into(),
        args: vec!["-c".into(), EXEC_CHILD.into()],
    }
}

fn init() -> InitFrame {
    InitFrame {
        program: ChildProgram::Rust,
        log_level: ChildLogLevel::Info,
        database_url: DatabaseUrl::new("postgresql://preload@localhost/test".to_owned()),
        connection: ChildConnection::default(),
        process_name: "horsies-child".to_owned(),
        child_kind: ChildKind::Isolated(IsolatedChild {
            pool: ChildPool {
                min_size: PoolMinSize::new(0).unwrap(),
                max_size: PoolMaxSize::new(1).unwrap(),
                check: false,
            },
            task_send_pool: TaskSendPool::new(1).unwrap(),
        }),
    }
}

fn plan(behaviour: &str) -> PreloadPlan {
    PreloadPlan {
        command: ChildCommand {
            program: "python3".into(),
            args: vec!["-c".into(), SCRIPTED_PRELOAD.into(), behaviour.into()],
        },
        mode: PreloadMode::Library,
        init: init(),
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// The full libtest name of a test of this module.
fn test_path(name: &str) -> String {
    let module = module_path!()
        .split_once("::")
        .map_or(module_path!(), |(_, rest)| rest);
    format!("{module}::{name}")
}

/// Run `body` when this process is the child of `name`; else run `name` in
/// a child process of the test binary and check that it passed.
fn in_own_process(name: &str, body: impl FnOnce()) {
    let path = test_path(name);
    match std::env::var(CHILD_TEST_ENV) {
        Ok(running) if running == path => body(),
        _ => {
            let binary = std::env::current_exe().unwrap();
            let output = std::process::Command::new(binary)
                .args([path.as_str(), "--exact", "--nocapture", "--test-threads=1"])
                .env(CHILD_TEST_ENV, &path)
                .output()
                .unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(output.status.success(), "{stdout}\n{stderr}");
            assert!(stdout.contains(&format!("test {path} ... ok")), "{stdout}");
        }
    }
}

/// The first six bytes the child wrote on its socket.
fn first_bytes(child: &SpawnedChild) -> [u8; 6] {
    let mut socket = child.socket.try_clone().unwrap();
    socket.set_read_timeout(Some(TEST_WAIT)).unwrap();
    let mut mark = [0_u8; 6];
    socket.read_exact(&mut mark).unwrap();
    mark
}

/// Close the parent end, so the child exits, and return its exit.
async fn close_and_wait(child: SpawnedChild) -> ChildExit {
    let SpawnedChild {
        socket, mut exit, ..
    } = child;
    drop(socket);
    wait_for_exit(&mut exit, TEST_WAIT).await
}

/// Whether `pid` names a live process or an unreaped zombie.
fn process_exists(pid: ProcessId) -> bool {
    // SAFETY: kill with signal 0 only checks the pid.
    unsafe { libc::kill(pid.as_pid_t(), 0) == 0 }
}

/// The parent pid of `pid`, from `/proc/<pid>/stat`.
fn parent_of(pid: ProcessId) -> u32 {
    let stat = std::fs::read_to_string(format!("/proc/{}/stat", pid.get())).unwrap();
    let after_name = &stat[stat.rfind(')').unwrap() + 2..];
    after_name
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap()
}

fn ready_pid(start: PreloadStart) -> ProcessId {
    match start {
        PreloadStart::Ready { pid } => pid,
        other => panic!("the preload start ended with {other:?}"),
    }
}

#[test]
fn a_ready_preload_forks_each_child_and_the_spawner_reaps_it() {
    in_own_process(
        "a_ready_preload_forks_each_child_and_the_spawner_reaps_it",
        || {
            let spawner = Spawner::start().unwrap();
            let handle = spawner.handle();
            runtime().block_on(async {
                let preload = ready_pid(handle.start_preload(plan("fork")).await);
                for _ in 0..2 {
                    let child = handle.spawn(exec_command()).await.unwrap();
                    assert_eq!(&first_bytes(&child), FORKED_MARK);
                    assert_ne!(child.pid, preload);
                    assert_eq!(parent_of(child.pid), std::process::id());
                    assert_eq!(
                        close_and_wait(child).await,
                        ChildExit::Reaped(ReapedExit::ExitedWith(7))
                    );
                }
            });
            assert_eq!(
                spawner.stop(TEST_WAIT),
                crate::worker::executor::supervisor::SpawnerStopReport::AllReaped
            );
        },
    );
}

#[test]
fn a_refusal_at_the_start_is_answered_and_every_spawn_uses_exec() {
    in_own_process(
        "a_refusal_at_the_start_is_answered_and_every_spawn_uses_exec",
        || {
            let spawner = Spawner::start().unwrap();
            let handle = spawner.handle();
            runtime().block_on(async {
                assert_eq!(
                    handle.start_preload(plan("refuse")).await,
                    PreloadStart::Refused {
                        reason: "import failed".to_owned()
                    }
                );
                let child = handle.spawn(exec_command()).await.unwrap();
                assert_eq!(&first_bytes(&child), EXEC_MARK);
                let _exit = close_and_wait(child).await;
            });
            let _report = spawner.stop(TEST_WAIT);
        },
    );
}

/// The design point of the path: the 120 s wait for `preload_ready` is a
/// state of the spawner loop, so an exit is reaped and published during
/// it.
#[test]
fn the_wait_for_ready_does_not_stop_the_reaping() {
    in_own_process("the_wait_for_ready_does_not_stop_the_reaping", || {
        let spawner = Spawner::start().unwrap();
        let handle = spawner.handle();
        let runtime = runtime();
        let start = {
            let handle = handle.clone();
            runtime.spawn(async move { handle.start_preload(plan("never_ready")).await })
        };
        runtime.block_on(async {
            let child = handle.spawn(exec_command()).await.unwrap();
            assert_eq!(&first_bytes(&child), EXEC_MARK);
            let began = Instant::now();
            assert_eq!(
                close_and_wait(child).await,
                ChildExit::Reaped(ReapedExit::ExitedWith(8))
            );
            assert!(began.elapsed() < PRELOAD_READY_TIMEOUT);
            assert!(!start.is_finished());
        });
        let _report = spawner.stop(TEST_WAIT);
        assert_eq!(
            runtime.block_on(start).unwrap(),
            PreloadStart::SpawnerStopped
        );
    });
}

#[test]
fn a_failed_fork_starts_the_child_with_exec_and_keeps_the_preload() {
    in_own_process(
        "a_failed_fork_starts_the_child_with_exec_and_keeps_the_preload",
        || {
            let spawner = Spawner::start().unwrap();
            let handle = spawner.handle();
            runtime().block_on(async {
                let preload = ready_pid(handle.start_preload(plan("fork_fails")).await);
                for _ in 0..2 {
                    let child = handle.spawn(exec_command()).await.unwrap();
                    assert_eq!(&first_bytes(&child), EXEC_MARK);
                    assert!(process_exists(preload));
                    let _exit = close_and_wait(child).await;
                }
            });
            let _report = spawner.stop(TEST_WAIT);
        },
    );
}

#[test]
fn a_fork_with_no_answer_ends_at_its_bound_with_exec_and_a_killed_preload() {
    in_own_process(
        "a_fork_with_no_answer_ends_at_its_bound_with_exec_and_a_killed_preload",
        || {
            let spawner = Spawner::start().unwrap();
            let handle = spawner.handle();
            runtime().block_on(async {
                let preload = ready_pid(handle.start_preload(plan("silent")).await);
                let began = Instant::now();
                let child = handle.spawn(exec_command()).await.unwrap();
                assert!(began.elapsed() >= PRELOAD_FORK_TIMEOUT);
                assert_eq!(&first_bytes(&child), EXEC_MARK);
                let deadline = Instant::now() + TEST_WAIT;
                while process_exists(preload) && Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                assert!(!process_exists(preload), "the silent preload still runs");
                let _exit = close_and_wait(child).await;
            });
            let _report = spawner.stop(TEST_WAIT);
        },
    );
}

#[test]
fn a_dead_preload_restarts_after_the_delay() {
    in_own_process("a_dead_preload_restarts_after_the_delay", || {
        let marker_dir =
            std::env::temp_dir().join(format!("horsies-preload-{}", std::process::id()));
        std::fs::create_dir_all(&marker_dir).unwrap();
        let behaviour = format!("exit_once:{}", marker_dir.display());
        let spawner = Spawner::start().unwrap();
        let handle = spawner.handle();
        runtime().block_on(async {
            let first = ready_pid(handle.start_preload(plan(&behaviour)).await);
            let child = handle.spawn(exec_command()).await.unwrap();
            assert_eq!(&first_bytes(&child), EXEC_MARK);
            let _exit = close_and_wait(child).await;
            tokio::time::sleep(PRELOAD_RESTART_DELAY + Duration::from_secs(3)).await;
            let child = handle.spawn(exec_command()).await.unwrap();
            assert_eq!(&first_bytes(&child), FORKED_MARK);
            assert!(!process_exists(first));
            let _exit = close_and_wait(child).await;
        });
        let _report = spawner.stop(TEST_WAIT);
        std::fs::remove_dir_all(&marker_dir).unwrap();
    });
}

#[test]
fn the_sweep_reaps_a_landed_orphan_and_leaves_known_children_to_the_spawner() {
    in_own_process(
        "the_sweep_reaps_a_landed_orphan_and_leaves_known_children_to_the_spawner",
        || {
            let pid_file =
                std::env::temp_dir().join(format!("horsies-orphan-{}", std::process::id()));
            let behaviour = format!("orphan:{}", pid_file.display());
            let spawner = Spawner::start().unwrap();
            let handle = spawner.handle();
            runtime().block_on(async {
                let _preload = ready_pid(handle.start_preload(plan(&behaviour)).await);
                let child = handle.spawn(exec_command()).await.unwrap();
                assert_eq!(&first_bytes(&child), FORKED_MARK);
                let orphan: libc::pid_t = std::fs::read_to_string(&pid_file)
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                let mut status: libc::c_int = 0;
                // SAFETY: waitpid of one pid, writing into a live local.
                let waited = unsafe { libc::waitpid(orphan, &mut status, libc::WNOHANG) };
                assert_eq!(waited, -1, "the landed orphan was not reaped by the sweep");
                assert_eq!(
                    close_and_wait(child).await,
                    ChildExit::Reaped(ReapedExit::ExitedWith(7))
                );
            });
            let _report = spawner.stop(TEST_WAIT);
            std::fs::remove_file(&pid_file).unwrap();
        },
    );
}

/// Executor risk 1: a child of each path, with its parent-death signal set,
/// survives the end of a thread that is neither the spawner thread nor the
/// main thread.
#[test]
fn children_of_both_paths_survive_the_end_of_another_thread() {
    in_own_process(
        "children_of_both_paths_survive_the_end_of_another_thread",
        || {
            let spawner = Spawner::start().unwrap();
            let handle = spawner.handle();
            let runtime = runtime();
            let (forked, exec) = runtime.block_on(async {
                // Before the preload start, every spawn uses exec.
                let exec = handle.spawn(exec_command()).await.unwrap();
                assert_eq!(&first_bytes(&exec), EXEC_MARK);
                let _ready = ready_pid(handle.start_preload(plan("fork")).await);
                let forked = handle.spawn(exec_command()).await.unwrap();
                assert_eq!(&first_bytes(&forked), FORKED_MARK);
                (forked, exec)
            });
            std::thread::spawn(|| ()).join().unwrap();
            std::thread::sleep(Duration::from_millis(200));
            assert!(process_exists(forked.pid));
            assert!(process_exists(exec.pid));
            runtime.block_on(async {
                assert_eq!(
                    close_and_wait(forked).await,
                    ChildExit::Reaped(ReapedExit::ExitedWith(7))
                );
                assert_eq!(
                    close_and_wait(exec).await,
                    ChildExit::Reaped(ReapedExit::ExitedWith(8))
                );
            });
            let _report = spawner.stop(TEST_WAIT);
        },
    );
}

/// Executor risk 6: one thread reaps forked and `exec` children whose
/// subprocesses exit at other times, and no exit is lost or taken twice.
#[test]
fn every_exit_of_both_paths_is_reaped_once() {
    in_own_process("every_exit_of_both_paths_is_reaped_once", || {
        let spawner = Spawner::start().unwrap();
        let handle = spawner.handle();
        runtime().block_on(async {
            let mut children = Vec::new();
            for index in 0..4_u32 {
                let command = ChildCommand {
                    program: "/bin/sh".into(),
                    args: vec![
                        "-c".into(),
                        format!(
                            r#"printf exec__ >&"$0"; (sleep 0.{index}) & (sleep 0.{}) & dd bs=1 count=1 <&"$0" >/dev/null 2>&1; exit 8"#,
                            4 - index
                        )
                        .into(),
                    ],
                };
                children.push(handle.spawn(command).await.unwrap());
            }
            let _ready = ready_pid(handle.start_preload(plan("fork")).await);
            for _ in 0..4 {
                children.push(handle.spawn(exec_command()).await.unwrap());
            }
            for child in children {
                let forked = first_bytes(&child) == *FORKED_MARK;
                let expected = match forked {
                    true => 7,
                    false => 8,
                };
                assert_eq!(
                    close_and_wait(child).await,
                    ChildExit::Reaped(ReapedExit::ExitedWith(expected))
                );
            }
        });
        assert_eq!(
            spawner.stop(TEST_WAIT),
            crate::worker::executor::supervisor::SpawnerStopReport::AllReaped
        );
    });
}

#[test]
fn the_wait_bound_never_passes_the_next_preload_deadline() {
    let now = Instant::now();
    assert_eq!(
        super::super::wait_bound(false, None, now),
        Duration::from_secs(1)
    );
    assert_eq!(
        super::super::wait_bound(false, Some(now + Duration::from_millis(300)), now),
        Duration::from_millis(300)
    );
    assert_eq!(
        super::super::wait_bound(true, Some(now + Duration::from_secs(5)), now),
        crate::worker::executor::supervisor::POLLED_EXIT_INTERVAL
    );
    assert_eq!(
        super::super::wait_bound(false, Some(now - Duration::from_millis(1)), now),
        Duration::ZERO
    );
}

#[test]
fn the_preload_mode_argument_is_the_python_value() {
    assert_eq!(PreloadMode::Library.argument(), "library");
    assert_eq!(PreloadMode::App.argument(), "app");
}

//! Tests of the preload fork path, Linux only, with a scripted preload
//! process: a `python3` program that reads the init frame, writes preload
//! frames, and answers each spawn request by a real double fork whose
//! child runs a bash script on the socket it was given.
//!
//! The subreaper flag and the orphan sweep act on the whole process, and a
//! test process holds many spawners, so each test runs in a child process
//! of the test binary, alone.

use std::collections::{HashMap, VecDeque};
use std::io::{self, Read};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::*;
use crate::worker::executor::protocol::{
    ChildConnection, ChildKind, ChildLogLevel, ChildPool, ChildProgram, DatabaseUrl, IsolatedChild,
    PoolMaxSize, PoolMinSize, TaskSendPool,
};
use crate::worker::executor::supervisor::exit::{ChildExit, ReapedExit};
use crate::worker::executor::supervisor::spawner::{
    wait_for_exit, ExitWatch, Intake, KillOutcome, Queue, Shared, Spawner, SpawnerStopReport, Waker,
};

/// The child test to run, set in the environment of a child process.
const CHILD_TEST_ENV: &str = "HORSIES_PRELOAD_CHILD_TEST";

/// The longest wait of a test for a child's bytes or exit.
const TEST_WAIT: Duration = Duration::from_secs(20);

/// The scripted preload process. Arguments after the program: the
/// behaviour, then what the spawner appends (socket descriptor, worker pid,
/// mode). Behaviours:
/// - `fork`: ready, then a double fork per request.
/// - `refuse`: refuse at the start, exit 1.
/// - `never_ready`: read the init frame and sleep. `never_ready:<file>`
///   also writes its pid to `<file>`.
/// - `fork_fails`: ready, then `fork_failed` per request.
/// - `silent`: ready, then never answer a request.
/// - `exit_once:<dir>`: as `fork`, but the first process of the test
///   exits at its first request.
/// - `orphan:<file>`: as `fork`, and each middle process leaves an exited
///   orphan in its own session, whose pid goes to `<file>`.
/// - `exit_at_once`: as `fork`, but the forked child exits 9 after its
///   `setsid`, and the middle process reports it only after that exit.
/// - `late:<file>`: as `fork`, but the pid of the forked child goes to
///   `<file>`, and the answer comes one second after the fork bound.
/// - `close_on_request`: ready, then at a request it closes its socket and
///   sleeps.
/// - `ready_again`: ready, then at a request it sends `preload_ready` again
///   and sleeps.
/// - `thread_pid:<file>`: ready, then at a request it starts a helper
///   process in its own session with a second thread, writes `<helper pid>
///   <thread id>` to `<file>`, and reports the thread id as the forked
///   child. A pidfd of a thread that leads no process cannot open.
///
/// `{script_shell}` stands for [`crate::test_support::SCRIPT_SHELL`], the
/// program of the forked child; [`plan`] puts it in.
const SCRIPTED_PRELOAD: &str = r#"
import ctypes, json, os, signal, socket, struct, sys, threading, time

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

def argument():
    return behaviour.split(':', 1)[1]

def write_whole(path, text):
    with open(path + '.part', 'w') as out:
        out.write(text)
    os.replace(path + '.part', path)

meta_bytes, body_bytes = struct.unpack('>IQ', read_exact(12))
init = json.loads(read_exact(meta_bytes))
read_exact(body_bytes)
assert init['kind'] == 'init', init
if behaviour == 'refuse':
    send({'kind': 'preload_refused', 'reason': 'import failed'})
    os._exit(1)
if behaviour.startswith('never_ready'):
    if behaviour.startswith('never_ready:'):
        write_whole(argument(), str(os.getpid()))
    time.sleep(3600)
    os._exit(0)
exit_now = False
if behaviour.startswith('exit_once:'):
    marker = os.path.join(argument(), 'exited')
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
    if behaviour == 'close_on_request':
        os.close(child_fd)
        ctrl.close()
        time.sleep(3600)
        os._exit(0)
    if behaviour == 'ready_again':
        os.close(child_fd)
        send({'kind': 'preload_ready', 'pid': os.getpid()})
        time.sleep(3600)
        os._exit(0)
    if behaviour.startswith('thread_pid:'):
        os.close(child_fd)
        helper = os.fork()
        if helper == 0:
            ctrl.close()
            os.setsid()
            def hold():
                write_whole(argument(), str(os.getpid()) + ' ' + str(threading.get_native_id()))
                time.sleep(3600)
            threading.Thread(target=hold, daemon=True).start()
            time.sleep(3600)
            os._exit(0)
        while not os.path.exists(argument()):
            time.sleep(0.01)
        with open(argument()) as found:
            thread_id = int(found.read().split()[1])
        send({'kind': 'child_forked', 'pid': thread_id})
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
            with open(argument(), 'w') as out:
                out.write(str(stray))
        child = os.fork()
        if child == 0:
            ctrl.close()
            os.close(read_end)
            os.close(write_end)
            os.setsid()
            if behaviour == 'exit_at_once':
                os._exit(9)
            deadline = time.monotonic() + 5
            while os.getppid() != worker_pid:
                if time.monotonic() > deadline:
                    os._exit(76)
                time.sleep(0.0002)
            ctypes.CDLL(None).prctl(1, signal.SIGKILL, 0, 0, 0)
            os.set_inheritable(child_fd, True)
            os.execv('{script_shell}', ['{script_shell}', '-c', CHILD, str(child_fd), str(worker_pid)])
        if behaviour == 'exit_at_once':
            os.waitid(os.P_PID, child, os.WEXITED | os.WNOWAIT)
        os.write(write_end, str(child).encode())
        os._exit(0)
    os.close(write_end)
    reported = os.read(read_end, 32)
    os.close(read_end)
    os.waitpid(middle, 0)
    os.close(child_fd)
    if behaviour.startswith('late:'):
        write_whole(argument(), reported.decode())
        time.sleep(6)
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
        program: crate::test_support::SCRIPT_SHELL.into(),
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
            args: vec![
                "-c".into(),
                SCRIPTED_PRELOAD
                    .replace("{script_shell}", crate::test_support::SCRIPT_SHELL)
                    .into(),
                behaviour.into(),
            ],
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
                    program: crate::test_support::SCRIPT_SHELL.into(),
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

// ---------------------------------------------------------------------------
// The rules of Python `clients/python/tests/unit/test_preload.py` on the core
// ---------------------------------------------------------------------------

/// How long the `late:<file>` behaviour waits before it answers a request.
const LATE_ANSWER: Duration = Duration::from_secs(6);

/// The longest wait of a test for a process to end.
const END_WAIT: Duration = Duration::from_secs(20);

/// A file under the temporary directory, named after the test process.
fn scratch_file(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("horsies-{name}-{}", std::process::id()))
}

/// The pids that a scripted process wrote to `path`, in order.
fn read_pids(path: &Path) -> Vec<libc::pid_t> {
    std::fs::read_to_string(path)
        .unwrap()
        .split_whitespace()
        .map(|pid| pid.parse().unwrap())
        .collect()
}

/// The `PR_SET_CHILD_SUBREAPER` flag of this process: 0 or 1.
fn subreaper_flag() -> libc::c_int {
    let mut value: libc::c_int = -1;
    let unused: libc::c_ulong = 0;
    // SAFETY: PR_GET_CHILD_SUBREAPER writes one int to the pointer, which
    // points at a live local.
    let result = unsafe {
        libc::prctl(
            libc::PR_GET_CHILD_SUBREAPER,
            std::ptr::addr_of_mut!(value),
            unused,
            unused,
            unused,
        )
    };
    assert_eq!(result, 0, "{}", io::Error::last_os_error());
    value
}

/// Whether `pid` is gone or a zombie that no one reaped yet.
fn process_ended(pid: libc::pid_t) -> bool {
    // SAFETY: signal 0 checks the process and sends nothing.
    let alive = unsafe { libc::kill(pid, 0) } == 0;
    match alive {
        false => true,
        true => std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .map(|stat| {
                stat.rsplit(')')
                    .next()
                    .is_some_and(|rest| rest.trim_start().starts_with('Z'))
            })
            .unwrap_or(false),
    }
}

/// Wait until `pid` is gone or a zombie.
async fn wait_until_ended(pid: libc::pid_t) {
    let deadline = Instant::now() + END_WAIT;
    while !process_ended(pid) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(process_ended(pid), "process {pid} still runs");
}

/// Wait until `pid` is gone: ended and reaped.
async fn wait_until_gone(pid: ProcessId) {
    let deadline = Instant::now() + END_WAIT;
    while process_exists(pid) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!process_exists(pid), "process {} still exists", pid.get());
}

/// `SIGKILL` to `pid` alone.
fn kill_process(pid: ProcessId) {
    // SAFETY: kill sends a signal; it touches no memory of this process.
    let sent = unsafe { libc::kill(pid.as_pid_t(), libc::SIGKILL) };
    assert_eq!(sent, 0, "{}", io::Error::last_os_error());
}

/// Wait until `pid`, a child of this process, exited, and leave it
/// unreaped.
fn wait_until_exited(pid: libc::pid_t) {
    let id = libc::id_t::try_from(pid).unwrap();
    // SAFETY: an all-zero siginfo_t is a valid value.
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    // SAFETY: waitid writes into a live local siginfo_t.
    let result = unsafe { libc::waitid(libc::P_PID, id, &mut info, libc::WEXITED | libc::WNOWAIT) };
    assert_eq!(result, 0, "{}", io::Error::last_os_error());
}

/// Whether `waitpid` reaps `pid` now; the exit status when it does.
fn reap_now(pid: libc::pid_t) -> Option<libc::c_int> {
    let mut status: libc::c_int = 0;
    // SAFETY: waitpid of one pid, writing into a live local.
    let reaped = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
    match reaped == pid {
        true => Some(status),
        false => None,
    }
}

/// The `pre_exec` hook of a child that leads its own session.
fn lead_a_new_session() -> io::Result<()> {
    // SAFETY: setsid takes no argument.
    match unsafe { libc::setsid() } {
        -1 => Err(io::Error::last_os_error()),
        _ => Ok(()),
    }
}

/// A spawner loop with no thread, for the rules of the preload path that
/// need no process.
fn spawner_loop() -> SpawnerLoop {
    let waker = Waker::new().unwrap();
    let watch = ExitWatch::new(waker.read.as_raw_fd()).unwrap();
    let shared = Arc::new(Shared {
        queue: Mutex::new(Queue {
            requests: VecDeque::new(),
            intake: Intake::Open,
        }),
        waker,
    });
    SpawnerLoop::new(shared, watch)
}

/// A plan whose program does not exist: every start of it fails.
fn missing_program_plan() -> PreloadPlan {
    PreloadPlan {
        command: ChildCommand {
            program: "/nonexistent/horsies-preload".into(),
            args: Vec::new(),
        },
        mode: PreloadMode::Library,
        init: init(),
    }
}

/// Python `TestSupport::test_a_kernel_with_no_pidfd_uses_exec` and
/// `::test_a_blocked_prctl_uses_exec`: a kernel that refuses a pidfd or the
/// subreaper flag has no preload path, and the answer names the call.
#[test]
fn a_kernel_without_pidfd_or_the_subreaper_flag_has_no_preload_path() {
    assert_eq!(kernel_support(Ok(()), || Ok(())), Ok(()));
    assert_eq!(
        kernel_support(
            Err(io::Error::from_raw_os_error(libc::ENOSYS)),
            || -> io::Result<()> { panic!("the flag is tried only after a pidfd opened") },
        ),
        Err(PreloadUnsupported::MissingKernelSupport {
            reason: format!("pidfd_open: {}", io::Error::from_raw_os_error(libc::ENOSYS))
        })
    );
    assert_eq!(
        kernel_support(Ok(()), || Err(io::Error::from_raw_os_error(libc::EPERM))),
        Err(PreloadUnsupported::MissingKernelSupport {
            reason: format!(
                "prctl is refused: {}",
                io::Error::from_raw_os_error(libc::EPERM)
            )
        })
    );
}

/// Python `TestOrphanSweep::test_a_child_in_this_session_keeps_its_exit_status`:
/// an exited child in the worker's own session is no landed orphan; the
/// sweep stops at it and leaves its status to its own waiter.
#[test]
fn the_sweep_leaves_an_exited_child_of_the_worker_session_to_its_waiter() {
    in_own_process(
        "the_sweep_leaves_an_exited_child_of_the_worker_session_to_its_waiter",
        || {
            #[expect(
                clippy::zombie_processes,
                reason = "the orphan sweep under test must reap this child; a wait here would take its exit status first"
            )]
            let child = std::process::Command::new("/bin/sh")
                .args(["-c", "exit 3"])
                .spawn()
                .unwrap();
            let pid = libc::pid_t::try_from(child.id()).unwrap();
            wait_until_exited(pid);

            sweep_landed_orphans(&HashMap::new());

            let status = reap_now(pid).expect("the sweep took the exit status of the child");
            assert!(libc::WIFEXITED(status));
            assert_eq!(libc::WEXITSTATUS(status), 3);
        },
    );
}

/// Python `TestOrphanSweep::test_the_sweep_is_bounded`: one sweep reaps at
/// most [`ORPHAN_SWEEP_LIMIT`] landed orphans.
#[test]
fn one_sweep_reaps_at_most_its_bound_of_landed_orphans() {
    in_own_process(
        "one_sweep_reaps_at_most_its_bound_of_landed_orphans",
        || {
            let count = ORPHAN_SWEEP_LIMIT + 2;
            let mut pids = Vec::new();
            for _ in 0..count {
                let mut command = std::process::Command::new("/bin/sh");
                command.args(["-c", "exit 0"]);
                // SAFETY: the hook runs in the forked child before `exec` and
                // calls only setsid.
                unsafe {
                    command.pre_exec(lead_a_new_session);
                }
                #[expect(
                    clippy::zombie_processes,
                    reason = "the orphan sweep under test must reap this child; a wait here would take its exit status first"
                )]
                let child = command.spawn().unwrap();
                pids.push(libc::pid_t::try_from(child.id()).unwrap());
            }
            for &pid in &pids {
                wait_until_exited(pid);
            }

            sweep_landed_orphans(&HashMap::new());

            let left = pids.iter().filter(|&&pid| reap_now(pid).is_some()).count();
            assert_eq!(left, 2, "one sweep reaped {} of {count}", pids.len() - left);
        },
    );
}

/// Python `TestForkReplies::test_a_new_child_is_registered_before_the_sweep`:
/// a forked child that exited before its pid was reported is a landed
/// orphan at the sweep. It enters the registry first, so the sweep leaves
/// its exit to the spawner.
#[test]
fn a_forked_child_that_exits_at_once_keeps_its_exit_for_the_spawner() {
    in_own_process(
        "a_forked_child_that_exits_at_once_keeps_its_exit_for_the_spawner",
        || {
            let spawner = Spawner::start().unwrap();
            let handle = spawner.handle();
            runtime().block_on(async {
                let _preload = ready_pid(handle.start_preload(plan("exit_at_once")).await);
                let child = handle.spawn(exec_command()).await.unwrap();
                assert_eq!(
                    close_and_wait(child).await,
                    ChildExit::Reaped(ReapedExit::ExitedWith(9))
                );
            });
            assert_eq!(spawner.stop(TEST_WAIT), SpawnerStopReport::AllReaped);
        },
    );
}

/// Python `TestForkReplies::test_a_failed_request_ends_the_parent_and_clears_the_flag`:
/// the channel may hold half a frame, so a preload process whose socket
/// fails during a request is killed; the subreaper flag is cleared, and the
/// child starts with `exec`.
#[test]
fn a_request_whose_channel_closes_ends_the_preload_and_clears_the_flag() {
    in_own_process(
        "a_request_whose_channel_closes_ends_the_preload_and_clears_the_flag",
        || {
            let spawner = Spawner::start().unwrap();
            let handle = spawner.handle();
            runtime().block_on(async {
                let preload = ready_pid(handle.start_preload(plan("close_on_request")).await);
                let child = handle.spawn(exec_command()).await.unwrap();
                assert_eq!(&first_bytes(&child), EXEC_MARK);
                assert_eq!(subreaper_flag(), 0);
                wait_until_gone(preload).await;
                let _exit = close_and_wait(child).await;
            });
            assert_eq!(spawner.stop(TEST_WAIT), SpawnerStopReport::AllReaped);
        },
    );
}

/// Python `TestForkReplies::test_an_unexpected_frame_ends_the_parent`.
#[test]
fn an_unexpected_frame_during_a_request_ends_the_preload() {
    in_own_process(
        "an_unexpected_frame_during_a_request_ends_the_preload",
        || {
            let spawner = Spawner::start().unwrap();
            let handle = spawner.handle();
            runtime().block_on(async {
                let preload = ready_pid(handle.start_preload(plan("ready_again")).await);
                let child = handle.spawn(exec_command()).await.unwrap();
                assert_eq!(&first_bytes(&child), EXEC_MARK);
                assert_eq!(subreaper_flag(), 0);
                wait_until_gone(preload).await;
                let _exit = close_and_wait(child).await;
            });
            assert_eq!(spawner.stop(TEST_WAIT), SpawnerStopReport::AllReaped);
        },
    );
}

/// Python `TestForkReplies::test_a_pid_that_cannot_be_watched_is_no_child`:
/// no exit can be seen for a reported pid whose pidfd does not open, so
/// that process is killed, its pid leaves the registry, the child starts
/// with `exec`, and the preload process stays.
#[test]
fn a_forked_pid_that_cannot_be_watched_is_killed_and_the_spawn_uses_exec() {
    in_own_process(
        "a_forked_pid_that_cannot_be_watched_is_killed_and_the_spawn_uses_exec",
        || {
            let report = scratch_file("thread-pid");
            let behaviour = format!("thread_pid:{}", report.display());
            let spawner = Spawner::start().unwrap();
            let handle = spawner.handle();
            runtime().block_on(async {
                let preload = ready_pid(handle.start_preload(plan(&behaviour)).await);
                let child = handle.spawn(exec_command()).await.unwrap();
                assert_eq!(&first_bytes(&child), EXEC_MARK);
                let pids = read_pids(&report);
                let (helper, thread) = (pids[0], pids[1]);
                assert_ne!(helper, thread);
                wait_until_ended(helper).await;
                assert!(process_exists(preload));
                let _exit = close_and_wait(child).await;
            });
            // The stop ends only when the registry is empty.
            assert_eq!(spawner.stop(TEST_WAIT), SpawnerStopReport::AllReaped);
            std::fs::remove_file(&report).unwrap();
        },
    );
}

/// Python `TestForkedChild::test_the_subreaper_flag_is_off_between_spawns`.
#[test]
fn the_subreaper_flag_is_off_between_spawns() {
    in_own_process("the_subreaper_flag_is_off_between_spawns", || {
        let spawner = Spawner::start().unwrap();
        let handle = spawner.handle();
        runtime().block_on(async {
            let _preload = ready_pid(handle.start_preload(plan("fork")).await);
            assert_eq!(subreaper_flag(), 0);
            let mut children = Vec::new();
            for _ in 0..2 {
                let child = handle.spawn(exec_command()).await.unwrap();
                assert_eq!(&first_bytes(&child), FORKED_MARK);
                assert_eq!(subreaper_flag(), 0);
                children.push(child);
            }
            for child in children {
                let _exit = close_and_wait(child).await;
            }
        });
        assert_eq!(spawner.stop(TEST_WAIT), SpawnerStopReport::AllReaped);
    });
}

/// Python `TestPreloadParentDeath::test_running_tasks_finish_and_new_children_use_exec`:
/// the worker is the parent of a forked child, so the death of the preload
/// process does not touch it; until the restart, new children use `exec`.
#[test]
fn a_forked_child_outlives_its_preload_and_new_children_use_exec() {
    in_own_process(
        "a_forked_child_outlives_its_preload_and_new_children_use_exec",
        || {
            let spawner = Spawner::start().unwrap();
            let handle = spawner.handle();
            runtime().block_on(async {
                let preload = ready_pid(handle.start_preload(plan("fork")).await);
                let forked = handle.spawn(exec_command()).await.unwrap();
                assert_eq!(&first_bytes(&forked), FORKED_MARK);

                kill_process(preload);
                wait_until_gone(preload).await;

                let after = handle.spawn(exec_command()).await.unwrap();
                assert_eq!(&first_bytes(&after), EXEC_MARK);
                assert!(process_exists(forked.pid));
                assert_eq!(
                    close_and_wait(forked).await,
                    ChildExit::Reaped(ReapedExit::ExitedWith(7))
                );
                assert_eq!(
                    close_and_wait(after).await,
                    ChildExit::Reaped(ReapedExit::ExitedWith(8))
                );
            });
            assert_eq!(spawner.stop(TEST_WAIT), SpawnerStopReport::AllReaped);
        },
    );
}

/// Python `TestPreloadParentDeath::test_no_preload_parent_and_no_exec_is_a_pool_failure`,
/// the spawner half: with the preload process down, a child that cannot
/// `exec` is a spawn failure, which the pool counts as a boot attempt.
#[test]
fn with_the_preload_down_a_child_that_cannot_exec_is_a_spawn_failure() {
    in_own_process(
        "with_the_preload_down_a_child_that_cannot_exec_is_a_spawn_failure",
        || {
            let spawner = Spawner::start().unwrap();
            let handle = spawner.handle();
            runtime().block_on(async {
                let preload = ready_pid(handle.start_preload(plan("fork")).await);
                kill_process(preload);
                wait_until_gone(preload).await;
                let missing = ChildCommand {
                    program: "/nonexistent/horsies-child".into(),
                    args: Vec::new(),
                };
                assert!(matches!(
                    handle.spawn(missing).await,
                    Err(SpawnError::SpawnFailed { .. })
                ));
            });
            assert_eq!(spawner.stop(TEST_WAIT), SpawnerStopReport::AllReaped);
        },
    );
}

/// Python `TestPreloadParentDeath::test_a_failed_restart_spends_one_attempt_only`:
/// a restart that does not start uses one attempt and plans the next one.
#[test]
fn a_failed_restart_uses_one_attempt_and_plans_the_next() {
    let mut spawner = spawner_loop();
    spawner.preload.plan = Some(missing_program_plan());

    spawner.restart(0);
    assert!(
        matches!(
            spawner.preload.state,
            PreloadState::Restarting { restarts: 1, .. }
        ),
        "{:?}",
        spawner.preload.state
    );
    spawner.restart(1);
    assert!(
        matches!(
            spawner.preload.state,
            PreloadState::Restarting { restarts: 2, .. }
        ),
        "{:?}",
        spawner.preload.state
    );
}

/// Python `TestPreloadParentDeath::test_restarts_are_bounded`: after
/// [`PRELOAD_RESTART_LIMIT`] restarts, no restart is planned and every
/// child uses `exec`, also when the last restart did not start.
#[test]
fn restarts_stop_at_the_limit() {
    let mut spawner = spawner_loop();
    let last = PRELOAD_RESTART_LIMIT - 1;
    spawner.schedule_restart(last);
    assert!(
        matches!(spawner.preload.state, PreloadState::Restarting { restarts, .. } if restarts == last),
        "{:?}",
        spawner.preload.state
    );
    spawner.schedule_restart(PRELOAD_RESTART_LIMIT);
    assert!(
        matches!(spawner.preload.state, PreloadState::ExecOnly),
        "{:?}",
        spawner.preload.state
    );

    spawner.preload.plan = Some(missing_program_plan());
    spawner.restart(last);
    assert!(
        matches!(spawner.preload.state, PreloadState::ExecOnly),
        "{:?}",
        spawner.preload.state
    );
}

/// Python `TestPreloadParentDeath::test_a_bad_fork_never_shares_a_socket_with_an_exec_child`:
/// the fork is answered after the bound. The forked child holds the first
/// socket pair; the `exec` child gets a new one, and the forked child ends
/// at the close of its pair.
#[test]
fn a_late_fork_answer_leaves_the_exec_child_its_own_socket() {
    assert!(PRELOAD_FORK_TIMEOUT < LATE_ANSWER);
    in_own_process(
        "a_late_fork_answer_leaves_the_exec_child_its_own_socket",
        || {
            let pid_file = scratch_file("late-fork");
            let behaviour = format!("late:{}", pid_file.display());
            let spawner = Spawner::start().unwrap();
            let handle = spawner.handle();
            runtime().block_on(async {
                let preload = ready_pid(handle.start_preload(plan(&behaviour)).await);
                let began = Instant::now();
                let child = handle.spawn(exec_command()).await.unwrap();
                assert!(began.elapsed() >= PRELOAD_FORK_TIMEOUT);
                assert_eq!(&first_bytes(&child), EXEC_MARK);
                let late = read_pids(&pid_file)[0];
                assert_ne!(late, child.pid.as_pid_t());
                wait_until_ended(late).await;
                wait_until_gone(preload).await;
                let _exit = close_and_wait(child).await;
            });
            assert_eq!(spawner.stop(TEST_WAIT), SpawnerStopReport::AllReaped);
            std::fs::remove_file(&pid_file).unwrap();
        },
    );
}

/// Python `TestPreloadParentDeath::test_a_cancelled_start_ends_the_preload_parent`:
/// a stop while the start waits for `preload_ready` ends the preload
/// process, and the start answers that the spawner stopped.
#[test]
fn a_stop_during_the_preload_start_ends_the_preload_process() {
    in_own_process(
        "a_stop_during_the_preload_start_ends_the_preload_process",
        || {
            let pid_file = scratch_file("never-ready");
            let behaviour = format!("never_ready:{}", pid_file.display());
            let spawner = Spawner::start().unwrap();
            let handle = spawner.handle();
            let runtime = runtime();
            let start = {
                let handle = handle.clone();
                runtime.spawn(async move { handle.start_preload(plan(&behaviour)).await })
            };
            let preload = runtime.block_on(async {
                let deadline = Instant::now() + END_WAIT;
                while !pid_file.exists() {
                    assert!(
                        Instant::now() < deadline,
                        "the preload process did not start"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                read_pids(&pid_file)[0]
            });
            assert!(!start.is_finished());

            assert_eq!(spawner.stop(TEST_WAIT), SpawnerStopReport::AllReaped);

            assert!(process_ended(preload));
            assert_eq!(
                runtime.block_on(start).unwrap(),
                PreloadStart::SpawnerStopped
            );
            std::fs::remove_file(&pid_file).unwrap();
        },
    );
}

/// Python `TestKillBeforeSession::test_a_child_with_no_group_is_killed_alone`:
/// a forked child before its `setsid` leads no session; the kill reaches it
/// by its pid.
#[test]
fn a_child_that_leads_no_session_is_killed_by_its_pid() {
    let mut child = std::process::Command::new("/bin/sh")
        .args(["-c", "exec sleep 60"])
        .spawn()
        .unwrap();
    let pid = ProcessId::new(child.id()).unwrap();

    assert_eq!(kill_session(pid), KillOutcome::Sent);

    assert_eq!(child.wait().unwrap().signal(), Some(libc::SIGKILL));
}

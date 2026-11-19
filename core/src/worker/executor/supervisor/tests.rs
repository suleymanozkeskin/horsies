//! Tests of the supervisor: the pure rules, then real child processes
//! started through the spawner thread (`/bin/sh` scripts).

use std::io::Read;
use std::num::{NonZeroU32, NonZeroUsize};
use std::os::fd::AsRawFd;
use std::time::Duration;

use super::*;
use crate::worker::executor::protocol::{
    encode_child_frame, BootDetail, BootErrorName, BootFailedFrame, ChildExitCode, ChildFrame,
    ProcessId, ReadyFrame, ReceivedProtocol, ReportedBootCause, RssReading, ShortageErrno,
};

fn pid() -> ProcessId {
    ProcessId::new(4242).unwrap()
}

fn lost(cause: LossCause, exit: ChildExit) -> ChildLost {
    ChildLost {
        cause,
        exit,
        partial: PartialFrame::NoneAtHand,
        pid: pid(),
    }
}

fn exited(code: i32) -> ChildExit {
    ChildExit::Reaped(ReapedExit::ExitedWith(code))
}

fn killed() -> ChildExit {
    ChildExit::Reaped(ReapedExit::KilledBySignal(libc::SIGKILL))
}

fn cause_of(end: BootEnd) -> BootFailureCause {
    match end {
        BootEnd::Failed(failure) => failure.cause,
        BootEnd::ChildHookFailed => panic!("expected a boot failure, got a hook failure"),
    }
}

const POOL_ALONE_AT_START: PoolAtFailure = PoolAtFailure {
    first_generation_in: false,
    another_child_serves: false,
    every_other_slot_down: true,
};

const POOL_SERVING: PoolAtFailure = PoolAtFailure {
    first_generation_in: true,
    another_child_serves: true,
    every_other_slot_down: false,
};

// ---------------------------------------------------------------------------
// Exits
// ---------------------------------------------------------------------------

/// A Python child closes its task loop after the stop frame within 7 s:
/// `LOOP_CLOSE_TIMEOUT_S` (5 s), then `ASYNCGEN_SHUTDOWN_MIN_TIMEOUT_S` and
/// `EXECUTOR_SHUTDOWN_MIN_TIMEOUT_S` (1 s each) of
/// `clients/python/horsies/core/worker/task_loop.py`. The Python test
/// `clients/python/tests/unit/test_task_loop.py::TestClose::test_close_steps_end_before_the_parent_kills_the_child`
/// pins that sum. The parent waits longer for the exit, so it does not kill
/// a child that is still closing.
#[test]
fn the_stop_wait_outlasts_the_close_of_a_python_child() {
    let python_child_close = Duration::from_secs(7);
    assert!(python_child_close < CHILD_STOP_TIMEOUT);
}

#[test]
fn exit_codes_are_read_in_the_protocol() {
    assert_eq!(ReapedExit::ExitedWith(0).meaning(), ExitMeaning::Clean);
    assert_eq!(
        ReapedExit::ExitedWith(173).meaning(),
        ExitMeaning::Protocol(ChildExitCode::ChildHookFailed)
    );
    assert_eq!(
        ReapedExit::ExitedWith(78).meaning(),
        ExitMeaning::Protocol(ChildExitCode::ProtocolMismatch)
    );
    assert_eq!(
        ReapedExit::ExitedWith(3).meaning(),
        ExitMeaning::OtherCode(3)
    );
    assert_eq!(
        ReapedExit::KilledBySignal(libc::SIGSEGV).meaning(),
        ExitMeaning::Signal(libc::SIGSEGV)
    );
    assert_eq!(
        ReapedExit::KilledBySignal(libc::SIGKILL).to_string(),
        format!("signal {} (SIGKILL)", libc::SIGKILL)
    );
    assert!(ReapedExit::KilledBySignal(libc::SIGKILL).is_sigkill());
    assert!(!ReapedExit::ExitedWith(libc::SIGKILL).is_sigkill());
    assert_eq!(
        ReapedExit::ExitedWith(80).meaning(),
        ExitMeaning::Protocol(ChildExitCode::CoreStopped)
    );
}

/// A child that held no task and ended with the core-stopped code is
/// replaced with an error log; every other end keeps its path.
#[test]
fn an_end_without_a_task_names_a_stopped_core_only() {
    assert_eq!(
        EndWithoutTask::of(exited(ChildExitCode::CoreStopped.code())),
        EndWithoutTask::CoreStopped
    );
    let others = [
        exited(0),
        exited(1),
        exited(ChildExitCode::ParentGone.code()),
        exited(ChildExitCode::BootFailed.code()),
        exited(ChildExitCode::ChildHookFailed.code()),
        killed(),
        ChildExit::Reaped(ReapedExit::KilledBySignal(libc::SIGSEGV)),
        ChildExit::ReapedElsewhere,
        ChildExit::ExitUnknown,
    ];
    for exit in others {
        assert_eq!(EndWithoutTask::of(exit), EndWithoutTask::Other, "{exit}");
    }
}

/// A child that dies with the core-stopped code before its ready frame is a
/// boot failure lost before ready, as the other codes of a child.
#[test]
fn a_core_stopped_exit_before_ready_is_lost_before_ready() {
    let exit = exited(ChildExitCode::CoreStopped.code());
    let cause = cause_of(boot_end_of(BootReply::Lost(lost(
        LossCause::ChildDied,
        exit,
    ))));
    assert_eq!(
        cause,
        BootFailureCause::LostBeforeReady {
            loss: LossCause::ChildDied,
            exit,
        }
    );
}

// ---------------------------------------------------------------------------
// Boot causes
// ---------------------------------------------------------------------------

#[test]
fn a_boot_failure_frame_names_its_cause() {
    let end = boot_end_of(BootReply::BootFailed(BootFailedFrame {
        cause: ReportedBootCause::ResourceShortage(ShortageErrno::Emfile),
        error_name: BootErrorName::new("OSError".to_owned()).unwrap(),
        detail: BootDetail::new("too many open files".to_owned()).unwrap(),
    }));
    match end {
        BootEnd::Failed(failure) => {
            assert_eq!(
                failure.cause,
                BootFailureCause::ResourceShortage(ShortageErrno::Emfile)
            );
            assert_eq!(failure.detail.as_str(), "OSError: too many open files");
            assert_eq!(failure.cause.kind(), CauseKind::CanClear);
        }
        BootEnd::ChildHookFailed => panic!("expected a boot failure"),
    }
}

#[test]
fn a_child_without_a_frame_is_read_by_its_exit() {
    assert_eq!(
        boot_end_of(BootReply::Lost(lost(LossCause::ChildDied, exited(173)))),
        BootEnd::ChildHookFailed
    );
    assert_eq!(
        cause_of(boot_end_of(BootReply::Lost(lost(
            LossCause::ChildDied,
            exited(76)
        )))),
        BootFailureCause::SpawnFailed
    );
    assert_eq!(
        cause_of(boot_end_of(BootReply::Lost(lost(
            LossCause::ChildDied,
            exited(78)
        )))),
        BootFailureCause::ProtocolMismatch(MismatchSeen::ChildRefusedInit)
    );
    assert_eq!(
        cause_of(boot_end_of(BootReply::Lost(lost(
            LossCause::ChildDied,
            exited(1)
        )))),
        BootFailureCause::LostBeforeReady {
            loss: LossCause::ChildDied,
            exit: exited(1),
        }
    );
    assert_eq!(
        cause_of(boot_end_of(BootReply::NotReadyInTime)),
        BootFailureCause::NotReadyInTime
    );
}

/// The text of a boot failure: a child that ended before
/// its ready frame names the cause without its loss, and the detail names
/// the loss and the exit once. The line of a failed attempt and the error
/// of a pool whose attempts ran out carry this text.
#[test]
fn a_child_lost_before_ready_names_its_loss_once() {
    let failure = match boot_end_of(BootReply::Lost(lost(LossCause::ChildDied, exited(1)))) {
        BootEnd::Failed(failure) => failure,
        BootEnd::ChildHookFailed => panic!("expected a boot failure, got a hook failure"),
    };

    assert_eq!(
        failure.cause.to_string(),
        "the child ended before it was ready"
    );
    assert_eq!(
        failure.to_string(),
        "the child ended before it was ready: child died: exit code 1"
    );
}

/// A forked child that did not reach the worker is a spawn failure, and the
/// detail names the reparent failure before the loss and the exit. The line
/// of a failed attempt and the error of a pool whose attempts ran out carry
/// this text.
#[test]
fn a_forked_child_that_did_not_reach_the_worker_names_the_reparent_failure() {
    let failure = match boot_end_of(BootReply::Lost(lost(LossCause::ChildDied, exited(76)))) {
        BootEnd::Failed(failure) => failure,
        BootEnd::ChildHookFailed => panic!("expected a boot failure, got a hook failure"),
    };

    assert_eq!(failure.cause, BootFailureCause::SpawnFailed);
    assert_eq!(
        failure.to_string(),
        "the OS refused to start the child process: the forked child did not reach the \
         worker: child died: exit code 76"
    );
}

#[test]
fn a_ready_frame_of_another_version_is_a_protocol_mismatch() {
    let reply = BootReply::Lost(lost(
        LossCause::ProtocolMismatch(ReceivedProtocol::Named(3)),
        killed(),
    ));
    assert_eq!(
        cause_of(boot_end_of(reply)),
        BootFailureCause::ProtocolMismatch(MismatchSeen::ReadyNamed(ReceivedProtocol::Named(3)))
    );
    assert_eq!(
        BootFailureCause::ProtocolMismatch(MismatchSeen::ChildRefusedInit).kind(),
        CauseKind::Repeats
    );
}

// ---------------------------------------------------------------------------
// Restart rules
// ---------------------------------------------------------------------------

/// Python `_boot_child`: a cause that repeats counts every attempt; the
/// last counted attempt is recorded as a failed attempt before the pool
/// fails.
#[test]
fn a_cause_that_repeats_counts_up_to_the_bound() {
    let mut counts = BootCounts::default();
    let cause = BootFailureCause::AppLoadFailed;
    assert_eq!(
        counts.after_failure(cause, POOL_ALONE_AT_START, 0),
        BootDecision::Retry {
            counted: 1,
            wait: Duration::from_millis(500)
        }
    );
    assert_eq!(
        counts.after_failure(cause, POOL_ALONE_AT_START, 0),
        BootDecision::Retry {
            counted: 2,
            wait: Duration::from_secs(1)
        }
    );
    assert_eq!(
        counts.after_failure(cause, POOL_ALONE_AT_START, 0),
        BootDecision::Exhausted {
            attempts: CHILD_BOOT_ATTEMPTS,
            last: LastRecord::AttemptFailed,
        }
    );
}

#[test]
fn a_cause_that_can_clear_is_spared_while_another_slot_serves() {
    let mut counts = BootCounts::default();
    let cause = BootFailureCause::ThreadStartRefused;
    for failed_before in 0..10_u32 {
        let failed_boots = NonZeroU32::new(failed_before + 1).unwrap();
        assert_eq!(
            counts.after_failure(cause, POOL_SERVING, failed_before),
            BootDecision::SlotDown {
                counted: 0,
                failed_boots,
                wait: slot_down_delay(failed_boots),
            }
        );
    }
}

/// Python `TestBoot::test_a_task_loop_failure_on_the_only_slot_uses_every_attempt`:
/// a cause that can clear counts while every other slot is down. The last
/// counted attempt is recorded as a slot down, with its failed boots and its
/// wait, before the pool fails.
#[test]
fn a_cause_that_can_clear_counts_when_every_other_slot_is_down() {
    let mut counts = BootCounts::default();
    let cause = BootFailureCause::ResourceShortage(ShortageErrno::Enomem);
    assert!(matches!(
        counts.after_failure(cause, POOL_ALONE_AT_START, 0),
        BootDecision::SlotDown { counted: 1, .. }
    ));
    assert!(matches!(
        counts.after_failure(cause, POOL_ALONE_AT_START, 1),
        BootDecision::SlotDown { counted: 2, .. }
    ));
    let third = NonZeroU32::new(3).unwrap();
    assert_eq!(
        counts.after_failure(cause, POOL_ALONE_AT_START, 2),
        BootDecision::Exhausted {
            attempts: 3,
            last: LastRecord::SlotDown {
                failed_boots: third,
                wait: Duration::from_secs(4),
            },
        }
    );
}

#[test]
fn a_spared_attempt_starts_the_count_again() {
    let mut counts = BootCounts::default();
    assert!(matches!(
        counts.after_failure(BootFailureCause::NotReadyInTime, POOL_SERVING, 0),
        BootDecision::Retry { counted: 1, .. }
    ));
    assert!(matches!(
        counts.after_failure(BootFailureCause::ThreadStartRefused, POOL_SERVING, 0),
        BootDecision::SlotDown { counted: 0, .. }
    ));
    assert!(matches!(
        counts.after_failure(BootFailureCause::NotReadyInTime, POOL_SERVING, 0),
        BootDecision::Retry { counted: 1, .. }
    ));
}

#[test]
fn an_outside_kill_is_spared_only_while_another_child_serves() {
    let killed_before_ready = BootFailureCause::LostBeforeReady {
        loss: LossCause::ChildDied,
        exit: killed(),
    };
    let mut counts = BootCounts::default();
    for kills in 1..=12_u32 {
        assert_eq!(
            counts.after_failure(killed_before_ready, POOL_SERVING, 0),
            BootDecision::OutsideKillSpared {
                kills_in_a_row: kills,
                wait: outside_kill_delay(kills),
            }
        );
    }
    let mut at_start = BootCounts::default();
    assert!(matches!(
        at_start.after_failure(killed_before_ready, POOL_ALONE_AT_START, 0),
        BootDecision::Retry { counted: 1, .. }
    ));
    let mut no_other = BootCounts::default();
    let no_other_child = PoolAtFailure {
        first_generation_in: true,
        another_child_serves: false,
        every_other_slot_down: true,
    };
    assert!(matches!(
        no_other.after_failure(killed_before_ready, no_other_child, 0),
        BootDecision::Retry { counted: 1, .. }
    ));
}

#[test]
fn waits_grow_and_stay_bounded() {
    let down: Vec<u64> = (1..=9_u32)
        .map(|boots| slot_down_delay(NonZeroU32::new(boots).unwrap()).as_secs())
        .collect();
    assert_eq!(down, vec![1, 2, 4, 8, 16, 32, 60, 60, 60]);
    assert_eq!(outside_kill_delay(1), Duration::from_millis(500));
    assert_eq!(outside_kill_delay(4), Duration::from_secs(2));
    assert_eq!(outside_kill_delay(10), Duration::from_secs(5));
    assert_eq!(outside_kill_delay(u32::MAX), Duration::from_secs(5));
}

#[test]
fn the_mailbox_holds_four_frames_per_seat_and_two() {
    assert_eq!(MailboxBound::for_seats(NonZeroUsize::MIN).get(), 6);
    assert_eq!(
        MailboxBound::for_seats(NonZeroUsize::new(1000).unwrap()).get(),
        4002
    );
}

// ---------------------------------------------------------------------------
// Real children through the spawner thread
// ---------------------------------------------------------------------------

/// A `/bin/sh` child. The spawner appends the socket descriptor and the
/// worker pid, so the script reads the descriptor as `$0`.
fn shell(script: &str) -> ChildCommand {
    ChildCommand {
        program: "/bin/sh".into(),
        args: vec!["-c".into(), script.into()],
    }
}

const TEST_WAIT: Duration = Duration::from_secs(10);

fn stop(spawner: Spawner) {
    assert_eq!(spawner.stop(TEST_WAIT), SpawnerStopReport::AllReaped);
}

#[tokio::test]
async fn the_spawner_reaps_a_child_and_publishes_its_exit() {
    let spawner = Spawner::start().unwrap();
    let mut child = spawner.handle().spawn(shell("exit 3")).await.unwrap();
    assert_eq!(
        wait_for_exit(&mut child.exit, TEST_WAIT).await,
        ChildExit::Reaped(ReapedExit::ExitedWith(3))
    );
    assert_eq!(
        spawner.handle().kill_session(child.id).await,
        KillOutcome::SessionGone
    );
    stop(spawner);
}

#[tokio::test]
async fn the_child_gets_its_socket_end_and_the_buffer_sizes() {
    let spawner = Spawner::start().unwrap();
    let mut child = spawner
        .handle()
        .spawn(shell(r#"printf hi >&"$0""#))
        .await
        .unwrap();
    for option in [libc::SO_SNDBUF, libc::SO_RCVBUF] {
        let size = socket::buffer_size(child.socket.as_raw_fd(), option).unwrap();
        assert!(size >= SOCKET_BUFFER_BYTES, "option {option}: {size}");
    }
    child.socket.set_read_timeout(Some(TEST_WAIT)).unwrap();
    let mut text = String::new();
    child.socket.read_to_string(&mut text).unwrap();
    assert_eq!(text, "hi");
    assert_eq!(
        wait_for_exit(&mut child.exit, TEST_WAIT).await,
        ChildExit::Reaped(ReapedExit::ExitedWith(0))
    );
    stop(spawner);
}

#[tokio::test]
async fn a_program_that_does_not_exist_is_a_spawn_failure() {
    let spawner = Spawner::start().unwrap();
    let command = ChildCommand {
        program: "/nonexistent/horsies-child".into(),
        args: Vec::new(),
    };
    assert!(matches!(
        spawner.handle().spawn(command).await,
        Err(SpawnError::SpawnFailed { .. })
    ));
    stop(spawner);
}

/// Whether `pid` is gone or a zombie that no one reaps yet.
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

#[tokio::test]
async fn a_kill_reaches_the_subprocesses_of_the_child() {
    let spawner = Spawner::start().unwrap();
    let mut child = spawner
        .handle()
        .spawn(shell(r#"sleep 60 & echo $! >&"$0"; wait"#))
        .await
        .unwrap();
    child.socket.set_read_timeout(Some(TEST_WAIT)).unwrap();
    let mut line = Vec::new();
    let mut byte = [0_u8; 1];
    while byte[0] != b'\n' {
        assert_eq!(child.socket.read(&mut byte).unwrap(), 1);
        line.push(byte[0]);
    }
    let grandchild: libc::pid_t = String::from_utf8(line).unwrap().trim().parse().unwrap();
    assert_eq!(
        spawner.handle().kill_session(child.id).await,
        KillOutcome::Sent
    );
    assert_eq!(
        wait_for_exit(&mut child.exit, TEST_WAIT).await,
        ChildExit::Reaped(ReapedExit::KilledBySignal(libc::SIGKILL))
    );
    let mut ended = false;
    for _ in 0..100 {
        ended = process_ended(grandchild);
        if ended {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ended, "the subprocess {grandchild} outlived the kill");
    stop(spawner);
}

/// Read one line of a pid that a child wrote to its socket.
fn read_pid_line(socket: &mut std::os::unix::net::UnixStream) -> libc::pid_t {
    socket.set_read_timeout(Some(TEST_WAIT)).unwrap();
    let mut line = Vec::new();
    let mut byte = [0_u8; 1];
    while byte[0] != b'\n' {
        assert_eq!(socket.read(&mut byte).unwrap(), 1);
        line.push(byte[0]);
    }
    String::from_utf8(line).unwrap().trim().parse().unwrap()
}

/// Python `ChildProcess.kill_group`: the kill of a reaped child still
/// reaches the subprocesses left in its session.
#[tokio::test]
async fn a_kill_after_the_reap_reaches_the_subprocesses_left_in_the_session() {
    let spawner = Spawner::start().unwrap();
    let mut child = spawner
        .handle()
        .spawn(shell(r#"sleep 60 & echo $! >&"$0"; exit 0"#))
        .await
        .unwrap();
    let grandchild = read_pid_line(&mut child.socket);
    assert_eq!(
        wait_for_exit(&mut child.exit, TEST_WAIT).await,
        ChildExit::Reaped(ReapedExit::ExitedWith(0))
    );
    assert!(!process_ended(grandchild));

    assert_eq!(
        spawner.handle().kill_session(child.id).await,
        KillOutcome::SentToReapedSession
    );

    let mut ended = false;
    for _ in 0..100 {
        ended = process_ended(grandchild);
        if ended {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ended, "the subprocess {grandchild} outlived the kill");
    stop(spawner);
}

/// A kill of a reaped child whose pid a live child of the spawner holds
/// sends no signal: the live child keeps running.
#[tokio::test]
async fn a_kill_after_the_reap_refuses_a_pid_that_a_live_child_holds() {
    let spawner = Spawner::start().unwrap();
    let mut live = spawner
        .handle()
        .spawn(shell("exec sleep 60"))
        .await
        .unwrap();
    let reaped = ChildId::reaped_child_with_pid(live.pid);

    assert_eq!(
        spawner.handle().kill_session(reaped).await,
        KillOutcome::PidReused
    );

    assert_eq!(
        wait_for_exit(&mut live.exit, Duration::from_millis(500)).await,
        ChildExit::ExitUnknown
    );
    assert_eq!(
        spawner.handle().kill_session(live.id).await,
        KillOutcome::Sent
    );
    assert_eq!(wait_for_exit(&mut live.exit, TEST_WAIT).await, killed());
    stop(spawner);
}

/// Python `TestKillBeforeSession::test_a_reaped_child_is_left_alone`: the
/// kill of a reaped child signals its session only, never the bare pid. A
/// process outside the spawner that holds the pid and leads no session
/// keeps running.
#[tokio::test]
async fn a_kill_after_the_reap_leaves_a_process_that_holds_the_pid_alone() {
    let spawner = Spawner::start().unwrap();
    let mut holder = std::process::Command::new("/bin/sh")
        .args(["-c", "exec sleep 60"])
        .spawn()
        .unwrap();
    let reaped = ChildId::reaped_child_with_pid(ProcessId::new(holder.id()).unwrap());

    assert_eq!(
        spawner.handle().kill_session(reaped).await,
        KillOutcome::SessionGone
    );

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(holder.try_wait().unwrap().is_none());
    holder.kill().unwrap();
    let _status = holder.wait().unwrap();
    stop(spawner);
}

async fn channel(spawner: &Spawner, script: &str) -> ChildChannel {
    let spawned = spawner.handle().spawn(shell(script)).await.unwrap();
    ChildChannel::attach(
        spawned,
        spawner.handle(),
        MailboxBound::for_seats(NonZeroUsize::MIN),
        CHILD_KILL_WAIT,
    )
    .unwrap()
}

fn ready_frame() -> ChildFrame {
    ChildFrame::Ready(ReadyFrame {
        pid: pid(),
        rss: RssReading::Unreadable,
    })
}

#[tokio::test]
async fn a_frame_from_the_child_is_delivered() {
    let spawner = Spawner::start().unwrap();
    let mut child = channel(&spawner, r#"exec cat <&"$0" >&"$0""#).await;
    let bytes = encode_child_frame(&ready_frame()).unwrap().to_bytes();
    child.send_raw(&bytes).await.unwrap();
    assert_eq!(
        child.await_frame(Some(TEST_WAIT)).await,
        FrameWait::Frame(ready_frame())
    );
    assert_eq!(
        child.await_frame(Some(Duration::from_millis(50))).await,
        FrameWait::Expired
    );
    assert_eq!(child.kill_for_caller().await, KillOutcome::Sent);
    assert_eq!(
        child.await_frame(Some(TEST_WAIT)).await,
        FrameWait::Lost(ChildLost {
            cause: LossCause::KilledByParent,
            exit: killed(),
            partial: PartialFrame::NoneAtHand,
            pid: child.pid(),
        })
    );
    let _ = child.dispose().await;
    stop(spawner);
}

/// A complete frame wins over the death that follows it.
#[tokio::test]
async fn a_frame_written_before_the_exit_wins() {
    let spawner = Spawner::start().unwrap();
    let bytes = encode_child_frame(&ready_frame()).unwrap().to_bytes();
    let script = format!(
        r#"dd bs=1 count={} <&"$0" 2>/dev/null >&"$0"; exit 0"#,
        bytes.len()
    );
    let mut child = channel(&spawner, &script).await;
    child.send_raw(&bytes).await.unwrap();
    assert_eq!(
        child.await_frame(Some(TEST_WAIT)).await,
        FrameWait::Frame(ready_frame())
    );
    assert_eq!(
        child.await_frame(Some(TEST_WAIT)).await,
        FrameWait::Lost(ChildLost {
            cause: LossCause::ChildDied,
            exit: exited(0),
            partial: PartialFrame::NoneAtHand,
            pid: child.pid(),
        })
    );
    let _ = child.dispose().await;
    stop(spawner);
}

#[tokio::test]
async fn a_death_with_no_frame_is_a_loss() {
    let spawner = Spawner::start().unwrap();
    let mut child = channel(&spawner, "exit 5").await;
    assert_eq!(
        child.await_frame(Some(TEST_WAIT)).await,
        FrameWait::Lost(ChildLost {
            cause: LossCause::ChildDied,
            exit: exited(5),
            partial: PartialFrame::NoneAtHand,
            pid: child.pid(),
        })
    );
    let _ = child.dispose().await;
    stop(spawner);
}

#[tokio::test]
async fn a_header_over_the_bounds_kills_the_child() {
    let spawner = Spawner::start().unwrap();
    let script =
        r#"printf '\000\000\000\001\000\000\000\000\000\000\000\000x' >&"$0"; exec sleep 60"#;
    let mut child = channel(&spawner, script).await;
    match child.await_frame(Some(TEST_WAIT)).await {
        FrameWait::Lost(lost) => {
            assert_eq!(lost.cause, LossCause::ProtocolViolation);
            assert_eq!(lost.exit, killed());
        }
        other => panic!("expected a loss, got {other:?}"),
    }
    let _ = child.dispose().await;
    stop(spawner);
}

#[tokio::test]
async fn an_eof_with_a_live_child_kills_it_after_the_grace() {
    let spawner = Spawner::start().unwrap();
    let mut child = channel(&spawner, r#"eval "exec $0>&-"; exec sleep 60"#).await;
    assert_eq!(
        child.await_frame(Some(TEST_WAIT)).await,
        FrameWait::Lost(ChildLost {
            cause: LossCause::ChannelLost,
            exit: killed(),
            partial: PartialFrame::NoneAtHand,
            pid: child.pid(),
        })
    );
    let _ = child.dispose().await;
    stop(spawner);
}

#[tokio::test]
async fn retire_sends_the_stop_frame_and_waits_for_the_exit() {
    let spawner = Spawner::start().unwrap();
    // Reads the stop frame, then exits by itself.
    let obeys = channel(
        &spawner,
        r#"dd bs=1 count=27 <&"$0" >/dev/null 2>&1; exit 0"#,
    )
    .await;
    assert_eq!(obeys.retire().await, exited(0));
    stop(spawner);
}

/// A channel dropped before its disposal (its slot task panicked or was
/// aborted) kills its child, as Python's `_clear_slot` does.
#[tokio::test]
async fn a_dropped_channel_kills_its_child() {
    let spawner = Spawner::start().unwrap();
    let child = channel(&spawner, "exec sleep 60").await;
    let mut exit = child.exit_watch();

    drop(child);

    assert_eq!(wait_for_exit(&mut exit, TEST_WAIT).await, killed());
    stop(spawner);
}

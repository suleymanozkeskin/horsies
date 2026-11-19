//! One child process: how its exit reads and is
//! described, the arguments it gets, a frame that reaches it whole, and a
//! frame cut by its death.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use uuid::Uuid;

use super::channel::JOINED_WRITE_BODY_BYTES;
use super::*;
use crate::worker::executor::protocol::{
    encode_child_frame, encode_parent_frame, ChildFrame, IsolatedWork, ParentFrame, ProcessId,
    ReadyFrame, RssReading, RunFrame, TaskInputText,
};

const TEST_WAIT: Duration = Duration::from_secs(10);

fn shell(script: &str) -> ChildCommand {
    ChildCommand {
        program: crate::test_support::SCRIPT_SHELL.into(),
        args: vec!["-c".into(), script.into()],
    }
}

/// A file path of one test in the temporary directory of the OS.
fn test_file(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("horsies-{name}-{}", Uuid::new_v4().simple()))
}

/// Python `TestExitDescription::test_returncode`: an exit status reads as
/// the exit code, or as the signal that ended the child.
#[test]
fn a_wait_status_reads_as_an_exit_code_or_a_signal() {
    assert_eq!(
        ReapedExit::from_wait_status(0),
        Some(ReapedExit::ExitedWith(0))
    );
    assert_eq!(
        ReapedExit::from_wait_status(3 << 8),
        Some(ReapedExit::ExitedWith(3))
    );
    assert_eq!(
        ReapedExit::from_wait_status(libc::SIGKILL),
        Some(ReapedExit::KilledBySignal(libc::SIGKILL))
    );
}

/// Python `TestExitDescription::test_text_names_the_signal`,
/// `::test_text_for_an_unknown_signal_number`, `::test_text_for_an_exit_code`,
/// `::test_text_for_an_unknown_exit`.
#[test]
fn an_exit_is_described_by_its_code_or_its_signal() {
    assert_eq!(
        ReapedExit::KilledBySignal(libc::SIGSEGV).to_string(),
        format!("signal {} (SIGSEGV)", libc::SIGSEGV)
    );
    assert_eq!(ReapedExit::KilledBySignal(250).to_string(), "signal 250");
    assert_eq!(ReapedExit::ExitedWith(2).to_string(), "exit code 2");
    assert!(ChildExit::ExitUnknown.to_string().contains("no exit"));
}

/// Python `TestBoot::test_child_program_receives_socket_and_parent_pid`,
/// and the forwarding half of `TestInterpreterFlags`: the child gets the
/// arguments of its command as given, then its socket descriptor and the
/// worker pid. (Here `sh -c` reads the first argument after the script as
/// `$0`.)
#[tokio::test]
async fn the_child_gets_its_command_arguments_then_its_socket_and_the_worker_pid() {
    let path = test_file("argv");
    let script = format!(r#"printf '%s %s %s' "$0" "$#" "$2" > "{}""#, path.display());
    let command = ChildCommand {
        program: crate::test_support::SCRIPT_SHELL.into(),
        args: vec!["-c".into(), script.into(), "-O".into()],
    };
    let spawner = Spawner::start().unwrap();

    let mut child = spawner.handle().spawn(command).await.unwrap();

    assert_eq!(
        wait_for_exit(&mut child.exit, TEST_WAIT).await,
        ChildExit::Reaped(ReapedExit::ExitedWith(0))
    );
    let seen = std::fs::read_to_string(&path).unwrap();
    let _ = std::fs::remove_file(&path);
    assert_eq!(seen, format!("-O 2 {}", std::process::id()));
    assert_eq!(spawner.stop(TEST_WAIT), SpawnerStopReport::AllReaped);
}

/// Python `TestChildSend::test_a_frame_with_a_small_body_is_one_write` and
/// `::test_a_large_body_goes_in_a_write_of_its_own`: a frame reaches the
/// child byte for byte, with its body joined to the header or written
/// apart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_frame_reaches_the_child_whole_with_a_small_or_a_large_body() {
    for body_bytes in [64, JOINED_WRITE_BODY_BYTES + 10] {
        let frame = ParentFrame::Run(RunFrame {
            task_id: Uuid::from_u128(1),
            task_name: "app.tasks.send".to_owned(),
            worker_id: "worker-1".to_owned(),
            claimed_at: Utc.with_ymd_and_hms(2026, 11, 16, 9, 30, 0).unwrap(),
            orphan_self_heal: crate::broker::OrphanSelfHeal::Off,
            work: IsolatedWork::Run,
            input: TaskInputText::new(format!("\"{}\"", "a".repeat(body_bytes))),
        });
        let bytes = encode_parent_frame(&frame).unwrap().to_bytes();
        let path = test_file("frame");
        // `dd` with one-byte blocks takes exactly the frame; BSD `head -c`
        // (macOS) reads a full stdio buffer and drops what it does not write.
        let script = format!(
            r#"dd bs=1 count={} <&"$0" 2>/dev/null > "{}""#,
            bytes.len(),
            path.display()
        );
        let spawner = Spawner::start().unwrap();
        let spawned = spawner.handle().spawn(shell(&script)).await.unwrap();
        let mut channel = ChildChannel::attach(
            spawned,
            spawner.handle(),
            MailboxBound::for_seats(NonZeroUsize::MIN),
            CHILD_KILL_WAIT,
        )
        .unwrap();

        channel.send(&frame).await.unwrap();

        assert_eq!(
            channel.wait_exit(TEST_WAIT).await,
            ChildExit::Reaped(ReapedExit::ExitedWith(0))
        );
        let received = std::fs::read(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        assert!(
            received == bytes,
            "a body of {body_bytes} bytes arrived changed"
        );
        let _ = channel.dispose().await;
        assert_eq!(spawner.stop(TEST_WAIT), SpawnerStopReport::AllReaped);
    }
}

/// Python `TestVerdicts::test_half_a_frame_still_unread_is_reported_partial`:
/// a child that dies after half a frame is lost, and the loss says that a
/// partial frame was discarded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn half_a_frame_before_the_death_is_reported_partial() {
    let ready = ChildFrame::Ready(ReadyFrame {
        pid: ProcessId::new(1).unwrap(),
        rss: RssReading::Unreadable,
    });
    let bytes = encode_child_frame(&ready).unwrap().to_bytes();
    let half: String = bytes[..bytes.len() - 1]
        .iter()
        .map(|byte| format!("\\{byte:03o}"))
        .collect();
    let script = format!(r#"printf '{half}' >&"$0"; exit 9"#);
    let spawner = Spawner::start().unwrap();
    let spawned = spawner.handle().spawn(shell(&script)).await.unwrap();
    let mut channel = ChildChannel::attach(
        spawned,
        spawner.handle(),
        MailboxBound::for_seats(NonZeroUsize::MIN),
        CHILD_KILL_WAIT,
    )
    .unwrap();

    let wait = channel.await_frame(Some(TEST_WAIT)).await;

    assert_eq!(
        wait,
        FrameWait::Lost(ChildLost {
            cause: LossCause::ChildDied,
            exit: ChildExit::Reaped(ReapedExit::ExitedWith(9)),
            partial: PartialFrame::Discarded,
            pid: channel.pid(),
        })
    );
    let _ = channel.dispose().await;
    assert_eq!(spawner.stop(TEST_WAIT), SpawnerStopReport::AllReaped);
}

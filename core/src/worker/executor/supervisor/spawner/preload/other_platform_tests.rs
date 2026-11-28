//! The preload path off Linux: no preload process starts, and every spawn
//! uses `exec` (Python `preload_support` on other platforms).

use std::io::Read;
use std::time::Duration;

use super::*;
use crate::worker::executor::protocol::{
    ChildConnection, ChildKind, ChildLogLevel, ChildPool, ChildProgram, DatabaseUrl, IsolatedChild,
    PoolMaxSize, PoolMinSize, TaskSendPool,
};
use crate::worker::executor::supervisor::exit::{ChildExit, ReapedExit};
use crate::worker::executor::supervisor::spawner::{wait_for_exit, Spawner, SpawnerStopReport};

/// The longest wait of the test for a child's bytes or exit.
const TEST_WAIT: Duration = Duration::from_secs(20);

fn init() -> InitFrame {
    InitFrame {
        program: ChildProgram::Host,
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

/// Python `TestSupport::test_other_platforms_use_exec`: the start of a
/// preload process answers that the platform has none, and the next spawn
/// starts its child with `exec`.
#[test]
fn other_platforms_have_no_preload_path_and_spawn_with_exec() {
    let not_linux = PreloadUnsupported::NotLinux {
        platform: std::env::consts::OS,
    };
    assert_eq!(preload_support(), Err(not_linux.clone()));

    let spawner = Spawner::start().unwrap();
    let handle = spawner.handle();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let plan = PreloadPlan {
            command: ChildCommand {
                program: "python3".into(),
                args: vec!["-c".into(), "raise SystemExit(3)".into()],
            },
            mode: PreloadMode::App,
            init: init(),
        };
        assert_eq!(
            handle.start_preload(plan).await,
            PreloadStart::Unsupported(not_linux)
        );
        let command = ChildCommand {
            program: crate::test_support::SCRIPT_SHELL.into(),
            args: vec![
                "-c".into(),
                r#"printf exec__ >&"$0"; dd bs=1 count=1 <&"$0" >/dev/null 2>&1; exit 8"#.into(),
            ],
        };
        let SpawnedChild {
            mut socket,
            mut exit,
            ..
        } = handle.spawn(command).await.unwrap();
        socket.set_read_timeout(Some(TEST_WAIT)).unwrap();
        let mut mark = [0_u8; 6];
        socket.read_exact(&mut mark).unwrap();
        assert_eq!(&mark, b"exec__");
        // The child exits 8 at the EOF of its socket.
        drop(socket);
        assert_eq!(
            wait_for_exit(&mut exit, TEST_WAIT).await,
            ChildExit::Reaped(ReapedExit::ExitedWith(8))
        );
    });
    assert_eq!(spawner.stop(TEST_WAIT), SpawnerStopReport::AllReaped);
}

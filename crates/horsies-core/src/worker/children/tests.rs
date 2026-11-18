//! The pools of a worker from its profile: the init frame of each child
//! kind, the recycle limits, the pending limit and the layout of the board
//! (Python `tests/unit/test_child_pool_wiring.py`,
//! `tests/unit/test_worker_helpers.py::TestSlotPoolSetup`,
//! `tests/unit/test_shared_dispatch.py::TestCapacityAndConfig`,
//! `tests/unit/test_thread_dispatch.py::TestCapacityAndConfig`).

use std::num::NonZeroU32;
use std::path::PathBuf;

use super::*;
use crate::core::config::child_sizing::{
    shared_async_executor_threads, shared_async_pool_connections, shared_thread_pool_connections,
    DEFAULT_TASK_SEND_POOL_CONNECTIONS,
};
use crate::worker::executor::placement::ChildCpus;
use crate::worker::executor::protocol::PythonProgram;

fn profile(document: serde_json::Value) -> WorkerProfile {
    serde_json::from_value(document).expect("a valid test profile")
}

fn launch() -> ChildLaunch {
    ChildLaunch {
        command: ChildCommand {
            program: PathBuf::from("/usr/bin/python3"),
            args: vec!["-m".into(), "horsies.core.worker.child_main".into()],
        },
        preload: crate::worker::children::PreloadProgram::NoPreloadProgram,
        program: ChildProgram::Python(PythonProgram {
            app_locator: "app.main:app".to_owned(),
            imports: vec!["app.tasks".to_owned()],
            sys_path_roots: vec!["/srv/app".to_owned()],
            parent_sys_path: vec!["/srv/app".to_owned()],
            parent_argv: vec!["horsies".to_owned(), "worker".to_owned()],
        }),
        log_level: ChildLogLevel::Info,
        database_url: DatabaseUrl::new("postgresql://u:p@localhost/db".to_owned()),
        connection: ChildConnection::default(),
    }
}

fn facts(pending: usize) -> PoolFacts {
    PoolFacts {
        worker_id: "worker-1".to_owned(),
        orphan_self_heal: crate::broker::OrphanSelfHeal::Off,
        pending_limit: NonZeroUsize::new(pending).expect("a test pending limit above 0"),
        exit_not_seen: ExitNotSeen::LeaveToRecovery,
        placement: CpuPlacement::SharedCpus,
    }
}

fn pool(min_size: u64, max_size: u64, check: bool) -> ChildPool {
    ChildPool {
        min_size: PoolMinSize::new(min_size).expect("a test min_size"),
        max_size: PoolMaxSize::new(max_size).expect("a test max_size"),
        check,
    }
}

fn task_send(connections: u64) -> TaskSendPool {
    TaskSendPool::new(connections).expect("a test task_send_pool")
}

fn seats(seats: u32) -> NonZeroU32 {
    NonZeroU32::new(seats).expect("test seats above 0")
}

const ALL_KINDS: &str = r#"{
    "isolated": {"kind": "isolated_children", "count": 1,
                 "pool": {"min_size": 1, "max_size": {"kind": "pool_size", "connections": 3}}},
    "shared_async": {"kind": "shared_async_children", "count": 1, "seats": 25,
                     "pool": {"min_size": 1},
                     "executor": {"kind": "executor_threads", "threads": 6}},
    "shared_thread": {"kind": "shared_thread_children", "count": 1, "seats": 8}
}"#;

/// Python `test_child_pool_wiring.py::test_each_kind_gets_the_resolved_pool_of_its_setting`.
#[test]
fn each_kind_gets_the_resolved_pool_of_its_setting() {
    let profile = profile(serde_json::from_str(ALL_KINDS).expect("test JSON"));
    let facts = facts(8);

    let isolated = isolated_pool_config(&profile, &launch(), &facts).expect("isolated children");
    let shared_async =
        shared_async_pool_config(&profile, &launch(), &facts).expect("shared async children");
    let shared_thread =
        shared_thread_pool_config(&profile, &launch(), &facts).expect("shared thread children");

    assert_eq!(
        isolated.init.child_kind,
        ChildKind::Isolated(IsolatedChild {
            pool: pool(1, 3, false),
            task_send_pool: task_send(u64::from(DEFAULT_TASK_SEND_POOL_CONNECTIONS)),
        })
    );
    assert_eq!(
        shared_async.init.child_kind,
        ChildKind::SharedAsync(SharedAsyncChild {
            seats: AsyncSeats::new(25).expect("25 seats"),
            pool: pool(
                1,
                u64::from(shared_async_pool_connections(seats(25)).get()),
                false
            ),
            executor_threads: ExecutorThreads::new(6).expect("6 threads"),
            task_send_pool: task_send(u64::from(DEFAULT_TASK_SEND_POOL_CONNECTIONS)),
        })
    );
    assert_eq!(
        shared_thread.init.child_kind,
        ChildKind::SharedThread(SharedThreadChild {
            seats: ThreadSeats::new(8).expect("8 seats"),
            pool: pool(
                0,
                u64::from(shared_thread_pool_connections(seats(8)).get()),
                false
            ),
            task_send_pool: task_send(u64::from(DEFAULT_TASK_SEND_POOL_CONNECTIONS)),
        })
    );
}

/// Python `test_child_pool_wiring.py::test_every_child_of_a_kind_gets_the_same_pool`:
/// one pool config per kind, whose init frame every slot of the kind sends.
#[test]
fn every_child_of_a_kind_gets_the_same_pool() {
    let profile = profile(serde_json::json!({
        "shared_async": {"kind": "shared_async_children", "count": 3, "seats": 40},
        "shared_thread": {"kind": "shared_thread_children", "count": 2, "seats": 4,
                          "pool": {"max_size": {"kind": "pool_size", "connections": 7}}}
    }));
    let facts = facts(8);

    assert!(isolated_pool_config(&profile, &launch(), &facts).is_none());
    let shared_async = shared_async_pool_config(&profile, &launch(), &facts).expect("async");
    let shared_thread = shared_thread_pool_config(&profile, &launch(), &facts).expect("thread");
    assert_eq!(
        shared_async.init.child_kind,
        ChildKind::SharedAsync(SharedAsyncChild {
            seats: AsyncSeats::new(40).expect("40 seats"),
            pool: pool(
                0,
                u64::from(shared_async_pool_connections(seats(40)).get()),
                false
            ),
            executor_threads: ExecutorThreads::new(u64::from(
                shared_async_executor_threads(seats(40)).get()
            ))
            .expect("derived threads"),
            task_send_pool: task_send(u64::from(DEFAULT_TASK_SEND_POOL_CONNECTIONS)),
        })
    );
    assert_eq!(
        shared_thread.init.child_kind,
        ChildKind::SharedThread(SharedThreadChild {
            seats: ThreadSeats::new(4).expect("4 seats"),
            pool: pool(0, 7, false),
            task_send_pool: task_send(u64::from(DEFAULT_TASK_SEND_POOL_CONNECTIONS)),
        })
    );
    assert_eq!(
        child_layout(&profile),
        ChildLayout {
            isolated: 0,
            shared_async: 3,
            shared_thread: 2,
        }
    );
}

/// Python `test_child_pool_wiring.py::test_the_isolated_default_is_a_pool_of_one_without_a_check`.
#[test]
fn the_isolated_default_is_a_pool_of_one_without_a_check() {
    let profile = profile(serde_json::json!({
        "isolated": {"kind": "isolated_children", "count": 2}
    }));
    let isolated = isolated_pool_config(&profile, &launch(), &facts(4)).expect("isolated");
    assert_eq!(
        isolated.init.child_kind,
        ChildKind::Isolated(IsolatedChild {
            pool: pool(0, 1, false),
            task_send_pool: task_send(u64::from(DEFAULT_TASK_SEND_POOL_CONNECTIONS)),
        })
    );
    assert_eq!(child_layout(&profile).isolated, 2);
}

/// Python `test_child_pool_wiring.py::test_each_kind_gets_the_task_send_pool_of_its_setting`.
#[test]
fn each_kind_gets_the_task_send_pool_of_its_setting() {
    let profile = profile(serde_json::json!({
        "isolated": {"kind": "isolated_children", "count": 1, "task_send_pool": {"connections": 3}},
        "shared_async": {"kind": "shared_async_children", "count": 1, "seats": 4,
                         "task_send_pool": {"connections": 5}},
        "shared_thread": {"kind": "shared_thread_children", "count": 1, "seats": 2,
                          "task_send_pool": {"connections": 7}}
    }));
    let facts = facts(8);
    let sends = [
        isolated_pool_config(&profile, &launch(), &facts).map(|config| config.init.child_kind),
        shared_async_pool_config(&profile, &launch(), &facts).map(|config| config.init.child_kind),
        shared_thread_pool_config(&profile, &launch(), &facts).map(|config| config.init.child_kind),
    ]
    .map(|kind| match kind {
        Some(ChildKind::Isolated(child)) => child.task_send_pool.get(),
        Some(ChildKind::SharedAsync(child)) => child.task_send_pool.get(),
        Some(ChildKind::SharedThread(child)) => child.task_send_pool.get(),
        None => panic!("each kind has a pool"),
    });
    assert_eq!(sends, [3, 5, 7]);
}

/// Python `test_worker_helpers.py::TestSlotPoolSetup::test_config_carries_what_a_child_needs`,
/// `test_shared_dispatch.py::TestCapacityAndConfig::test_the_pool_config_carries_the_shared_children`
/// and `test_thread_dispatch.py::TestCapacityAndConfig::test_the_pool_config_carries_the_thread_children`:
/// each pool config carries the launch of the host, the worker id, the
/// recycle limits of its kind, the pending limit and the kill wait.
#[test]
fn the_pool_config_carries_what_a_child_needs() {
    let profile = profile(serde_json::json!({
        "isolated": {"kind": "isolated_children", "count": 2,
                     "recycle": {"after": {"kind": "task_limit", "tasks": 50},
                                 "above": {"kind": "memory_limit", "mb": 512}}},
        "shared_async": {"kind": "shared_async_children", "count": 1, "seats": 10,
                         "recycle": {"after_per_seat": {"kind": "no_task_limit"},
                                     "above": {"kind": "memory_limit", "mb": 900}}},
        "shared_thread": {"kind": "shared_thread_children", "count": 1, "seats": 4}
    }));
    let facts = facts(32);
    let launch = launch();

    let isolated = isolated_pool_config(&profile, &launch, &facts).expect("isolated");
    assert_eq!(isolated.command, launch.command);
    assert_eq!(isolated.init.program, launch.program);
    assert_eq!(isolated.init.log_level, ChildLogLevel::Info);
    assert_eq!(isolated.init.database_url, launch.database_url);
    assert_eq!(isolated.init.connection, launch.connection);
    assert_eq!(isolated.init.process_name, CHILD_PROCESS_NAME);
    assert_eq!(isolated.worker_id, "worker-1");
    assert_eq!(isolated.pending_limit.get(), 32);
    assert_eq!(
        isolated.tasks_per_child,
        TaskLimit::After(NonZeroU64::new(50).expect("50"))
    );
    assert_eq!(
        isolated.memory_per_child,
        MemoryLimit::AtMb(NonZeroU64::new(512).expect("512"))
    );
    assert_eq!(isolated.exit_not_seen, ExitNotSeen::LeaveToRecovery);
    assert_eq!(isolated.child_cpus, ChildCpus::Inherited);
    assert_eq!(isolated.kill_wait, CHILD_KILL_WAIT);

    let shared_async = shared_async_pool_config(&profile, &launch, &facts).expect("async");
    assert_eq!(shared_async.tasks_per_seat, TaskLimit::Off);
    assert_eq!(
        shared_async.memory_per_child,
        MemoryLimit::AtMb(NonZeroU64::new(900).expect("900"))
    );
    let shared_thread = shared_thread_pool_config(&profile, &launch, &facts).expect("thread");
    assert_eq!(
        shared_thread.tasks_per_seat,
        TaskLimit::After(NonZeroU64::new(100).expect("the default recycle"))
    );
    assert_eq!(shared_thread.memory_per_child, MemoryLimit::Off);

    assert_eq!(
        baseline_limits(&profile),
        BaselineLimits {
            isolated: MemoryLimit::AtMb(NonZeroU64::new(512).expect("512")),
            shared_async: MemoryLimit::AtMb(NonZeroU64::new(900).expect("900")),
            shared_thread: MemoryLimit::Off,
        }
    );
}

/// Python `test_worker_helpers.py::TestSlotPoolSetup::test_pending_limit_covers_the_claim_budget`,
/// `test_shared_dispatch.py::TestCapacityAndConfig::test_the_pending_limit_counts_the_seats`
/// and the pending-limit half of
/// `test_thread_dispatch.py::TestCapacityAndConfig::test_the_seats_and_the_pending_limit_count_the_thread_seats`:
/// `2 * max(seats + prefetch_buffer, claimed_per_worker)`.
#[test]
fn the_pending_limit_covers_the_claim_budget() {
    let isolated = profile(serde_json::json!({
        "isolated": {"kind": "isolated_children", "count": 4}
    }));
    assert_eq!(pending_task_limit(&isolated, 0).get(), 8);
    assert_eq!(pending_task_limit(&isolated, 3).get(), 14);

    let claimed_limit = profile(serde_json::json!({
        "isolated": {"kind": "isolated_children", "count": 4},
        "claim": {"claimed_per_worker": {"kind": "task_limit", "tasks": 20}}
    }));
    assert_eq!(pending_task_limit(&claimed_limit, 3).get(), 40);

    let shared = profile(serde_json::json!({
        "isolated": {"kind": "isolated_children", "count": 1},
        "shared_async": {"kind": "shared_async_children", "count": 2, "seats": 10},
        "shared_thread": {"kind": "shared_thread_children", "count": 1, "seats": 4}
    }));
    assert_eq!(pending_task_limit(&shared, 0).get(), 2 * (1 + 20 + 4));
}

/// Stale RUNNING recovery on: a slot writes nothing for a child whose exit
/// was not seen; off: it waits for the exit.
#[test]
fn the_exit_rule_follows_stale_running_recovery() {
    let mut recovery = RecoveryConfig::default();
    recovery.auto_fail_stale_running = true;
    assert_eq!(exit_not_seen(&recovery), ExitNotSeen::LeaveToRecovery);
    recovery.auto_fail_stale_running = false;
    assert_eq!(exit_not_seen(&recovery), ExitNotSeen::WaitForExit);
}

/// Python `test_child_placement.py::test_a_refused_placement_stops_the_worker_start`
/// (the plan): `SHARED` places nothing; `OWN_CORE` off Linux is refused.
#[test]
fn shared_cpus_place_nothing_and_own_core_needs_linux() {
    assert_eq!(
        plan_placement(ParentCpu::Shared).expect("shared CPUs need no plan"),
        CpuPlacement::SharedCpus
    );
    #[cfg(not(target_os = "linux"))]
    assert!(matches!(
        plan_placement(ParentCpu::OwnCore),
        Err(PlacementRefused::NotLinux { .. })
    ));
}

/// Python `SlotPool.stop` reports the killed tasks of its one pool; the
/// worker joins the reports of its three pools: the worst stop, with the
/// killed tasks of every pool in pool order.
#[test]
fn the_stop_reports_of_the_pools_join_their_killed_tasks() {
    let isolated = Uuid::from_u128(1);
    let shared = [Uuid::from_u128(2), Uuid::from_u128(3)];

    assert_eq!(
        join_stop_reports([
            PoolStopReport::KilledAtDrainDeadline {
                killed: vec![isolated]
            },
            PoolStopReport::Drained,
            PoolStopReport::KilledAtDrainDeadline {
                killed: shared.to_vec()
            },
        ]),
        PoolStopReport::KilledAtDrainDeadline {
            killed: vec![isolated, shared[0], shared[1]]
        }
    );
    assert_eq!(
        join_stop_reports([
            PoolStopReport::KilledAtDrainDeadline {
                killed: vec![isolated]
            },
            PoolStopReport::AbortedAfterDrain { killed: Vec::new() },
            PoolStopReport::Drained,
        ]),
        PoolStopReport::AbortedAfterDrain {
            killed: vec![isolated]
        }
    );
    assert_eq!(
        join_stop_reports([
            PoolStopReport::Drained,
            PoolStopReport::Drained,
            PoolStopReport::Drained,
        ]),
        PoolStopReport::Drained
    );
}

fn preload_command() -> ChildCommand {
    ChildCommand {
        program: PathBuf::from("/usr/bin/python3"),
        args: vec!["-m".into(), "horsies.core.worker.preload_main".into()],
    }
}

fn launch_with_preload() -> ChildLaunch {
    ChildLaunch {
        preload: PreloadProgram::Command(preload_command()),
        ..launch()
    }
}

/// Python `SlotPool._start_preload`: the mode of the profile, the preload
/// command of the child program, and the init frame of a child.
#[test]
fn the_preload_plan_takes_the_mode_the_command_and_the_init_frame_of_the_first_kind() {
    let mut document: serde_json::Value = serde_json::from_str(ALL_KINDS).expect("test JSON");
    document["preload"] = serde_json::json!("library");
    let all_kinds = profile(document);
    let plan = preload_plan(&all_kinds, &launch_with_preload()).expect("a library preload");
    assert_eq!(plan.mode, PreloadMode::Library);
    assert_eq!(plan.command, preload_command());
    let isolated = isolated_pool_config(&all_kinds, &launch_with_preload(), &facts(8))
        .expect("isolated children");
    assert_eq!(plan.init, isolated.init);

    let threads_only = profile(serde_json::json!({
        "shared_thread": {"kind": "shared_thread_children", "count": 1, "seats": 8},
        "preload": "app",
    }));
    let plan = preload_plan(&threads_only, &launch_with_preload()).expect("an app preload");
    assert_eq!(plan.mode, PreloadMode::App);
    let threads = shared_thread_pool_config(&threads_only, &launch_with_preload(), &facts(8))
        .expect("shared thread children");
    assert_eq!(plan.init, threads.init);
}

#[test]
fn no_preload_runs_for_a_profile_without_one_or_a_child_program_without_one() {
    let no_preload = profile(serde_json::json!({
        "isolated": {"kind": "isolated_children", "count": 1},
        "preload": "none",
    }));
    assert!(preload_plan(&no_preload, &launch_with_preload()).is_none());
    let library = profile(serde_json::json!({
        "isolated": {"kind": "isolated_children", "count": 1},
        "preload": "library",
    }));
    assert!(preload_plan(&library, &launch()).is_none());
}

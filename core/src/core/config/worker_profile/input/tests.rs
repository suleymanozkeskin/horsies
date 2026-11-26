//! The worker profile as the config holds it: plain numbers, and the check
//! into the checked profile (Python `test_worker_profile.py`, the bounds
//! classes, where pydantic refuses a value when the config is built).

use serde_json::json;

use super::*;
use crate::core::config::worker_profile as checked;
use crate::core::config::worker_profile::{
    MAX_CHILD_POOL_CONNECTIONS, MAX_EXECUTOR_THREADS, MAX_ISOLATED_CHILDREN, MAX_MEMORY_LIMIT_MB,
    MAX_SEATS_PER_SHARED_ASYNC_CHILD, MAX_SEATS_PER_SHARED_THREAD_CHILD, MAX_SHARED_ASYNC_CHILDREN,
    MAX_SHARED_THREAD_CHILDREN, MAX_TASK_LIMIT,
};
use crate::core::error::HorsiesErrorCode;

fn bounded<const MIN: u32, const MAX: u32>(value: u32) -> BoundedU32<MIN, MAX> {
    BoundedU32::try_from(value).expect("a test value is inside its bounds")
}

fn isolated_with(change: impl FnOnce(&mut IsolatedChildren)) -> WorkerProfile {
    let mut children = IsolatedChildren::new(1);
    change(&mut children);
    WorkerProfile {
        isolated: children.into(),
        ..WorkerProfile::default()
    }
}

fn shared_async_with(change: impl FnOnce(&mut SharedAsyncChildren)) -> WorkerProfile {
    let mut children = SharedAsyncChildren::new(1, 1);
    change(&mut children);
    WorkerProfile {
        shared_async: children.into(),
        ..WorkerProfile::default()
    }
}

fn shared_thread_with(change: impl FnOnce(&mut SharedThreadChildren)) -> WorkerProfile {
    let mut children = SharedThreadChildren::new(1, 1);
    change(&mut children);
    WorkerProfile {
        shared_thread: children.into(),
        ..WorkerProfile::default()
    }
}

/// A bounded value: its field path in the profile, its lowest and highest
/// value, and the profile that carries a value.
type BoundCase = (&'static str, u32, u32, fn(u32) -> WorkerProfile);

/// Every bounded value of a profile.
fn bound_cases() -> [BoundCase; 24] {
    [
        ("isolated.count", 1, MAX_ISOLATED_CHILDREN, |value| {
            isolated_with(|children| children.count = value)
        }),
        ("isolated.recycle.after.tasks", 1, MAX_TASK_LIMIT, |value| {
            isolated_with(|children| {
                children.recycle.after = TaskLimitSetting::TaskLimit { tasks: value }
            })
        }),
        (
            "isolated.recycle.above.mb",
            1,
            MAX_MEMORY_LIMIT_MB,
            |value| {
                isolated_with(|children| {
                    children.recycle.above = MemoryLimitSetting::MemoryLimit { mb: value }
                })
            },
        ),
        (
            "isolated.pool.min_size",
            0,
            MAX_CHILD_POOL_CONNECTIONS,
            |value| isolated_with(|children| children.pool.min_size = value),
        ),
        (
            "isolated.pool.max_size.connections",
            1,
            MAX_CHILD_POOL_CONNECTIONS,
            |value| isolated_with(|children| children.pool.max_size = PoolSize::new(value)),
        ),
        (
            "isolated.task_send_pool.connections",
            1,
            MAX_CHILD_POOL_CONNECTIONS,
            |value| isolated_with(|children| children.task_send_pool = PoolSize::new(value)),
        ),
        (
            "shared_async.count",
            1,
            MAX_SHARED_ASYNC_CHILDREN,
            |value| shared_async_with(|children| children.count = value),
        ),
        (
            "shared_async.seats",
            1,
            MAX_SEATS_PER_SHARED_ASYNC_CHILD,
            |value| shared_async_with(|children| children.seats = value),
        ),
        (
            "shared_async.recycle.after_per_seat.tasks",
            1,
            MAX_TASK_LIMIT,
            |value| {
                shared_async_with(|children| {
                    children.recycle.after_per_seat = TaskLimitSetting::TaskLimit { tasks: value }
                })
            },
        ),
        (
            "shared_async.recycle.above.mb",
            1,
            MAX_MEMORY_LIMIT_MB,
            |value| {
                shared_async_with(|children| {
                    children.recycle.above = MemoryLimitSetting::MemoryLimit { mb: value }
                })
            },
        ),
        (
            "shared_async.pool.min_size",
            0,
            MAX_CHILD_POOL_CONNECTIONS,
            |value| shared_async_with(|children| children.pool.min_size = value),
        ),
        (
            "shared_async.pool.max_size.connections",
            1,
            MAX_CHILD_POOL_CONNECTIONS,
            |value| {
                shared_async_with(|children| {
                    children.pool.max_size = PoolSizeSetting::PoolSize { connections: value }
                })
            },
        ),
        (
            "shared_async.executor.threads",
            1,
            MAX_EXECUTOR_THREADS,
            |value| {
                shared_async_with(|children| {
                    children.executor = ExecutorThreadsSetting::ExecutorThreads { threads: value }
                })
            },
        ),
        (
            "shared_async.task_send_pool.connections",
            1,
            MAX_CHILD_POOL_CONNECTIONS,
            |value| shared_async_with(|children| children.task_send_pool = PoolSize::new(value)),
        ),
        (
            "shared_thread.count",
            1,
            MAX_SHARED_THREAD_CHILDREN,
            |value| shared_thread_with(|children| children.count = value),
        ),
        (
            "shared_thread.seats",
            1,
            MAX_SEATS_PER_SHARED_THREAD_CHILD,
            |value| shared_thread_with(|children| children.seats = value),
        ),
        (
            "shared_thread.recycle.after_per_seat.tasks",
            1,
            MAX_TASK_LIMIT,
            |value| {
                shared_thread_with(|children| {
                    children.recycle.after_per_seat = TaskLimitSetting::TaskLimit { tasks: value }
                })
            },
        ),
        (
            "shared_thread.recycle.above.mb",
            1,
            MAX_MEMORY_LIMIT_MB,
            |value| {
                shared_thread_with(|children| {
                    children.recycle.above = MemoryLimitSetting::MemoryLimit { mb: value }
                })
            },
        ),
        (
            "shared_thread.pool.min_size",
            0,
            MAX_CHILD_POOL_CONNECTIONS,
            |value| shared_thread_with(|children| children.pool.min_size = value),
        ),
        (
            "shared_thread.pool.max_size.connections",
            1,
            MAX_CHILD_POOL_CONNECTIONS,
            |value| {
                shared_thread_with(|children| {
                    children.pool.max_size = PoolSizeSetting::PoolSize { connections: value }
                })
            },
        ),
        (
            "shared_thread.task_send_pool.connections",
            1,
            MAX_CHILD_POOL_CONNECTIONS,
            |value| shared_thread_with(|children| children.task_send_pool = PoolSize::new(value)),
        ),
        (
            "pool.max_size.connections",
            1,
            MAX_CHILD_POOL_CONNECTIONS,
            |value| WorkerProfile {
                pool: ParentPool {
                    max_size: PoolSizeSetting::PoolSize { connections: value },
                },
                ..WorkerProfile::default()
            },
        ),
        (
            "claim.per_queue_per_pass.tasks",
            1,
            MAX_TASK_LIMIT,
            |value| WorkerProfile {
                claim: ClaimLimits {
                    per_queue_per_pass: TaskLimitSetting::TaskLimit { tasks: value },
                    ..ClaimLimits::default()
                },
                ..WorkerProfile::default()
            },
        ),
        (
            "claim.claimed_per_worker.tasks",
            1,
            MAX_TASK_LIMIT,
            |value| WorkerProfile {
                claim: ClaimLimits {
                    claimed_per_worker: TaskLimitSetting::TaskLimit { tasks: value },
                    ..ClaimLimits::default()
                },
                ..WorkerProfile::default()
            },
        ),
    ]
}

/// Python `TestIsolatedChildrenBounds`, `TestSharedAsyncChildrenBounds`,
/// `TestSharedThreadChildrenBounds`, `TestTaskLimitBounds`,
/// `TestMemoryLimitBounds`, `TestPoolSizeBounds`, `TestExecutorThreadsBounds`
/// and `TestChildPoolMinSizeBounds`: the lowest and the highest value pass
/// with the same value in the checked profile; one step outside each bound
/// is the one refusal of the profile, with its path and its bounds.
#[test]
fn every_bounded_value_passes_its_ends_and_is_refused_outside_with_its_path() {
    for (field, min, max, profile_of) in bound_cases() {
        for accepted in [min, max] {
            let profile = profile_of(accepted);
            let checked = profile
                .checked("p")
                .unwrap_or_else(|refused| panic!("{field} {accepted} passes: {refused:?}"));
            assert_eq!(WorkerProfile::from(checked), profile, "{field} {accepted}");
        }
        let outside = [min.checked_sub(1), max.checked_add(1)];
        for refused in outside.into_iter().flatten() {
            assert_eq!(
                profile_of(refused).checked("p"),
                Err(vec![ProfileValueRefused::OutOfBounds {
                    path: format!("worker_profiles['p'].{field}"),
                    refused: OutOfBounds {
                        value: refused,
                        min,
                        max,
                    },
                }]),
                "{field} {refused}",
            );
        }
    }
}

/// Python `TestDrainTimeoutBounds`: above 0 and at most a day passes; 0 and
/// one nanosecond above a day are refused with the path.
#[test]
fn a_drain_timeout_passes_above_zero_up_to_a_day_and_is_refused_outside() {
    let with_timeout = |drain_timeout| WorkerProfile {
        drain_timeout,
        ..WorkerProfile::default()
    };
    for accepted in [Duration::from_nanos(1), MAX_DRAIN_TIMEOUT] {
        let checked = with_timeout(accepted)
            .checked("p")
            .expect("inside the bounds");
        assert_eq!(checked.drain_timeout.get(), accepted);
    }
    let path = "worker_profiles['p'].drain_timeout".to_owned();
    assert_eq!(
        with_timeout(Duration::ZERO).checked("p"),
        Err(vec![ProfileValueRefused::DrainTimeout {
            path: path.clone(),
            refused: DrainTimeoutRefused::Zero,
        }]),
    );
    let above = MAX_DRAIN_TIMEOUT + Duration::from_nanos(1);
    assert_eq!(
        with_timeout(above).checked("p"),
        Err(vec![ProfileValueRefused::DrainTimeout {
            path,
            refused: DrainTimeoutRefused::AboveMaximum(above),
        }]),
    );
}

/// Every value outside its bound is refused at once, in field order, and
/// the name is in each path.
#[test]
fn every_refused_value_of_a_profile_is_reported_at_once() {
    let profile = WorkerProfile {
        isolated: IsolatedChildren::new(0).into(),
        shared_async: SharedAsyncChildren::new(2, MAX_SEATS_PER_SHARED_ASYNC_CHILD + 1).into(),
        drain_timeout: Duration::ZERO,
        ..WorkerProfile::default()
    };

    let refusals = profile
        .checked("batch")
        .expect_err("three values are outside");

    let paths: Vec<&str> = refusals.iter().map(ProfileValueRefused::path).collect();
    assert_eq!(
        paths,
        [
            "worker_profiles['batch'].isolated.count",
            "worker_profiles['batch'].shared_async.seats",
            "worker_profiles['batch'].drain_timeout",
        ],
    );
}

/// The checked profile has every value of the profile, and the config form
/// of it is the profile again.
#[test]
fn the_checked_profile_keeps_every_value() {
    let profile = WorkerProfile {
        isolated: IsolatedChildren {
            recycle: IsolatedRecycle {
                after: TaskLimitSetting::NoTaskLimit {},
                above: MemoryLimitSetting::MemoryLimit { mb: 512 },
            },
            pool: IsolatedPool {
                min_size: 1,
                max_size: PoolSize::new(3),
                check: true,
            },
            task_send_pool: PoolSize::new(4),
            ..IsolatedChildren::new(2)
        }
        .into(),
        shared_async: SharedAsyncChildren {
            executor: ExecutorThreadsSetting::ExecutorThreads { threads: 8 },
            ..SharedAsyncChildren::new(4, 25)
        }
        .into(),
        shared_thread: SharedThreadChildren::new(1, 4).into(),
        pool: ParentPool {
            max_size: PoolSizeSetting::PoolSize { connections: 12 },
        },
        claim: ClaimLimits {
            per_queue_per_pass: TaskLimitSetting::TaskLimit { tasks: 2 },
            claimed_per_worker: TaskLimitSetting::NoTaskLimit {},
        },
        drain_timeout: Duration::from_secs(120),
        preload: ChildPreload::NoPreload,
        parent_cpu: ParentCpu::OwnCore,
    };

    let result = profile
        .checked("p")
        .expect("every value is inside its bound");

    let checked::IsolatedSetting::IsolatedChildren(isolated) = result.isolated else {
        panic!("the isolated children are kept");
    };
    assert_eq!(isolated.count.get(), 2);
    assert_eq!(
        isolated.recycle.above,
        checked::MemoryLimitSetting::MemoryLimit { mb: bounded(512) },
    );
    assert_eq!(
        (
            isolated.pool.min_size.get(),
            isolated.pool.max_size.connections.get()
        ),
        (1, 3),
    );
    assert!(isolated.pool.check);
    assert_eq!(isolated.task_send_pool.connections.get(), 4);
    let checked::SharedAsyncSetting::SharedAsyncChildren(shared_async) = result.shared_async else {
        panic!("the shared async children are kept");
    };
    assert_eq!(
        (shared_async.count.get(), shared_async.seats.get()),
        (4, 25)
    );
    assert_eq!(shared_async.resolved_executor_threads().get(), 8);
    assert_eq!(result.seat_count(), 2 + 100 + 4);
    assert_eq!(result.resolved_parent_pool().get(), 12);
    assert_eq!(result.drain_timeout.get(), Duration::from_secs(120));
    assert_eq!(result.parent_cpu, ParentCpu::OwnCore);
    assert_eq!(WorkerProfile::from(result), profile);
}

/// `From` builds each setting from its children, so a profile reads
/// `IsolatedChildren::new(2).into()`.
#[test]
fn a_setting_is_built_from_its_children_with_into() {
    let profile = WorkerProfile {
        isolated: IsolatedChildren::new(2).into(),
        shared_async: SharedAsyncChildren::new(4, 25).into(),
        shared_thread: SharedThreadChildren::new(1, 4).into(),
        ..WorkerProfile::default()
    };

    assert_eq!(
        profile.isolated,
        IsolatedSetting::IsolatedChildren(IsolatedChildren::new(2)),
    );
    assert_eq!(
        profile.shared_async,
        SharedAsyncSetting::SharedAsyncChildren(SharedAsyncChildren::new(4, 25)),
    );
    assert_eq!(
        profile.shared_thread,
        SharedThreadSetting::SharedThreadChildren(SharedThreadChildren::new(1, 4)),
    );
}

/// The defaults are the defaults of the checked profile: an empty JSON
/// object, `WorkerProfile::default()` and the children of `new`.
#[test]
fn the_defaults_are_the_defaults_of_the_checked_profile() {
    let parsed: WorkerProfile = serde_json::from_value(json!({})).expect("an empty profile");
    assert_eq!(parsed, WorkerProfile::default());
    assert_eq!(
        WorkerProfile::default().checked("p"),
        Ok(checked::WorkerProfile::default()),
    );
    assert_eq!(
        IsolatedChildren::new(1),
        IsolatedChildren::from(checked::IsolatedChildren::new(bounded(1))),
    );
    assert_eq!(
        SharedAsyncChildren::new(1, 40),
        SharedAsyncChildren::from(checked::SharedAsyncChildren::new(bounded(1), bounded(40))),
    );
    assert_eq!(
        SharedThreadChildren::new(1, 40),
        SharedThreadChildren::from(checked::SharedThreadChildren::new(bounded(1), bounded(40))),
    );
}

/// The JSON form is the form of the checked profile. A count outside its
/// bound parses here, and the parse of the checked profile refuses it.
#[test]
fn the_json_form_is_the_form_of_the_checked_profile() {
    let document = json!({
        "isolated": {"kind": "isolated_children", "count": 2,
            "recycle": {"after": {"kind": "no_task_limit"},
                        "above": {"kind": "memory_limit", "mb": 512}},
            "task_send_pool": {"kind": "pool_size", "connections": 6}},
        "shared_async": {"kind": "shared_async_children", "count": 1, "seats": 4,
            "pool": {"max_size": {"kind": "pool_size", "connections": 3}},
            "executor": {"kind": "executor_threads", "threads": 8}},
        "shared_thread": {"kind": "no_shared_thread_children"},
        "pool": {"max_size": {"kind": "pool_size", "connections": 12}},
        "claim": {"per_queue_per_pass": {"kind": "task_limit", "tasks": 2}},
        "drain_timeout": {"secs": 2, "nanos": 500_000_000},
        "preload": "none",
        "parent_cpu": "OWN_CORE",
    });
    let profile: WorkerProfile =
        serde_json::from_value(document.clone()).expect("the config form parses");
    let parsed_checked: checked::WorkerProfile =
        serde_json::from_value(document).expect("the checked form parses");

    assert_eq!(profile.checked("p"), Ok(parsed_checked));
    assert_eq!(
        serde_json::to_value(profile).expect("the config form serializes"),
        serde_json::to_value(parsed_checked).expect("the checked form serializes"),
    );

    let zero = json!({"isolated": {"kind": "isolated_children", "count": 0}});
    let parsed: WorkerProfile = serde_json::from_value(zero.clone()).expect("0 parses");
    assert_eq!(
        parsed.isolated,
        IsolatedSetting::from(IsolatedChildren::new(0))
    );
    assert!(serde_json::from_value::<checked::WorkerProfile>(zero).is_err());
    assert!(
        serde_json::from_value::<WorkerProfile>(json!({"extra": 1})).is_err(),
        "an unknown key is refused",
    );
}

/// The refusal of a value is a profile refusal: `CONFIG_INVALID_WORKER_PROFILE`,
/// the path and the bound in the message, and the bound in the help.
#[test]
fn a_refusal_is_a_profile_refusal_with_its_bound() {
    let out_of_bounds = ProfileValueRefused::OutOfBounds {
        path: "worker_profiles['p'].isolated.count".to_owned(),
        refused: OutOfBounds {
            value: 0,
            min: 1,
            max: 1024,
        },
    }
    .to_horsies_error();
    assert_eq!(
        out_of_bounds.message,
        "worker_profiles['p'].isolated.count: 0 is outside the bounds 1 to 1024",
    );
    assert_eq!(
        out_of_bounds.code,
        Some(HorsiesErrorCode::ConfigInvalidWorkerProfile)
    );
    assert_eq!(
        out_of_bounds.help_text.as_deref(),
        Some("set a value from 1 to 1024")
    );

    let path = "worker_profiles['p'].drain_timeout".to_owned();
    let zero = ProfileValueRefused::DrainTimeout {
        path: path.clone(),
        refused: DrainTimeoutRefused::Zero,
    }
    .to_horsies_error();
    assert_eq!(
        zero.message,
        "worker_profiles['p'].drain_timeout: a drain timeout of 0 is refused; a stopping \
         worker needs a deadline",
    );
    assert_eq!(zero.code, Some(HorsiesErrorCode::ConfigInvalidWorkerProfile));
    assert_eq!(
        zero.help_text.as_deref(),
        Some("set a drain timeout above 0 and at most 86400s")
    );
    let above = ProfileValueRefused::DrainTimeout {
        path,
        refused: DrainTimeoutRefused::AboveMaximum(Duration::from_secs(86_401)),
    }
    .to_horsies_error();
    assert_eq!(
        above.message,
        "worker_profiles['p'].drain_timeout: 86401s is above the maximum of 86400s",
    );
}

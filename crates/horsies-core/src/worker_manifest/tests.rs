//! The parse of a worker manifest, the profile it resolves and the broker
//! config of its worker process.

use std::time::Duration;

use serde_json::{json, Value};

use super::*;
use crate::core::config::child_sizing::parent_pool_connections;
use crate::core::error::{ErrorCode, HorsiesError};

/// A profile with every setting of every kind of child.
fn full_profile() -> Value {
    json!({
        "isolated": {
            "kind": "CHILDREN",
            "count": 2,
            "recycle": {
                "after": {"kind": "TASKS", "tasks": 50},
                "above": {"kind": "MEGABYTES", "mb": 512}
            },
            "pool": {"min_size": 0, "max_size": 3, "check": true},
            "task_send_pool": 2
        },
        "shared_async": {
            "kind": "CHILDREN",
            "count": 1,
            "seats": 20,
            "recycle": {
                "after_per_seat": {"kind": "NO_LIMIT"},
                "above": {"kind": "NO_LIMIT"}
            },
            "pool": {"min_size": 1, "max_size": {"kind": "FROM_SEATS"}, "check": false},
            "executor": {"kind": "THREADS", "threads": 4},
            "task_send_pool": 3
        },
        "shared_thread": {"kind": "NO_CHILDREN"},
        "pool": {"max_size": {"kind": "FROM_SEATS"}},
        "claim": {
            "per_queue_per_pass": {"kind": "TASKS", "tasks": 5},
            "claimed_per_worker": {"kind": "NO_LIMIT"}
        },
        "drain_timeout_ms": 1500,
        "preload": "NO_PRELOAD",
        "parent_cpu": "SHARED"
    })
}

/// A profile of `count` isolated children. Its per-pass claim limit is
/// `count` tasks: the claim check refuses a limit above the capacity of the
/// worker (Python `profile_claim_errors`,
/// `python/horsies/core/models/worker_profile.py:1012-1039`), and the 5 tasks of
/// [`full_profile`] are above a capacity of fewer than 5 seats.
fn isolated_profile(count: u32) -> Value {
    let mut profile = full_profile();
    profile["isolated"]["count"] = json!(count);
    profile["shared_async"] = json!({"kind": "NO_CHILDREN"});
    profile["claim"]["per_queue_per_pass"] = json!({"kind": "TASKS", "tasks": count});
    profile
}

fn manifest(profiles: Value, request: Value) -> Value {
    json!({
        "manifest_version": 1,
        "database": {
            "database_url": "postgresql://user:secret@localhost:5432/db",
            "session_database": {"kind": "SAME_AS_DATABASE_URL"},
            "pgbouncer_transaction_mode": false,
            "pool_pre_ping": true,
            "worker_pool_pre_ping": false,
            "pool_timeout": 30,
            "pool_recycle": {"kind": "AFTER_SECONDS", "seconds": 1800},
            "echo": false,
            "tcp_keepalives": {
                "kind": "ON",
                "idle_seconds": 30,
                "interval_seconds": 10,
                "probes": 3
            },
            "tcp_user_timeout": {"kind": "AFTER", "milliseconds": 30000}
        },
        "send": {
            "retain_rerun_input_default": true,
            "idempotency_reservation_window": 60_000_000
        },
        "profiles": profiles,
        "profile": request,
        "claim": {
            "queues": {
                "kind": "CUSTOM",
                "queues": [
                    {"name": "high", "priority": 1, "max_concurrency": {"kind": "TASKS", "tasks": 0}},
                    {"name": "low", "priority": 100, "max_concurrency": {"kind": "NO_LIMIT"}}
                ]
            },
            "prefetch_buffer": 0,
            "claim_lease": {"kind": "WORKER_DEFAULT"},
            "max_claim_renew_age_ms": 180_000,
            "cluster_wide_cap": {"kind": "TASKS", "tasks": 20}
        },
        "tasks": {
            "app.tasks.add": "ISOLATED",
            "app.tasks.fetch": "SHARED_ASYNC",
            "app.tasks.resize": "SHARED_THREAD"
        },
        "child_launch": {
            "command": {"program": "/usr/bin/python3", "args": ["-m", "horsies.core.worker.child_main"]},
            "program": {
                "kind": "PYTHON",
                "app_locator": "app.main:app",
                "imports": ["app.tasks"],
                "sys_path_roots": ["/srv/app"],
                "parent_sys_path": ["/srv/app", "/usr/lib/python3.13"],
                "parent_argv": ["horsies", "worker", "app.main:app", "--profile", "batch"]
            },
            "log_level": "INFO"
        }
    })
}

fn named(name: &str) -> Value {
    json!({"kind": "NAMED", "name": name})
}

fn parsed(document: &Value) -> WorkerManifest {
    parse_worker_manifest(&document.to_string())
        .unwrap_or_else(|refused| panic!("the manifest parses: {refused}"))
}

fn refused_path(document: &Value) -> String {
    parse_worker_manifest(&document.to_string())
        .expect_err("the manifest is refused")
        .field_path
}

fn bounded<const MIN: u32, const MAX: u32>(value: u32) -> BoundedU32<MIN, MAX> {
    BoundedU32::try_from(value).expect("the test value is inside its bounds")
}

#[test]
fn a_complete_manifest_parses() {
    let manifest = parsed(&manifest(json!({"batch": full_profile()}), named("batch")));
    assert_eq!(manifest.manifest_version, WorkerManifestVersion::CURRENT);
    assert_eq!(
        manifest.profile,
        ProfileRequest::Named {
            name: "batch".to_owned()
        }
    );
    assert_eq!(
        manifest.tasks.get("app.tasks.fetch"),
        Some(&Execution::SharedAsync)
    );
    assert_eq!(manifest.child_launch.log_level, WorkerLogLevel::Info);
    match &manifest.claim.queues {
        Queues::Custom { queues } => {
            let names: Vec<&str> = queues.iter().map(|queue| queue.name.as_str()).collect();
            assert_eq!(names, ["high", "low"]);
        }
        Queues::Default => panic!("the fixture has custom queues"),
    }
}

/// Every setting of the manifest profile reaches the config profile.
#[test]
fn a_profile_maps_to_the_config_profile_field_by_field() {
    let manifest = parsed(&manifest(json!({"batch": full_profile()}), named("batch")));
    let profile = manifest.profiles["batch"].to_profile();
    let expected = WorkerProfile {
        isolated: IsolatedSetting::IsolatedChildren(IsolatedChildren {
            count: bounded(2),
            recycle: IsolatedRecycle {
                after: TaskLimitSetting::TaskLimit { tasks: bounded(50) },
                above: MemoryLimitSetting::MemoryLimit { mb: bounded(512) },
            },
            pool: IsolatedPool {
                min_size: bounded(0),
                max_size: PoolSize::new(bounded(3)),
                check: true,
            },
            task_send_pool: PoolSize::new(bounded(2)),
        }),
        shared_async: SharedAsyncSetting::SharedAsyncChildren(SharedAsyncChildren {
            count: bounded(1),
            seats: bounded(20),
            recycle: SharedRecycle {
                after_per_seat: TaskLimitSetting::NoTaskLimit {},
                above: MemoryLimitSetting::NoMemoryLimit {},
            },
            pool: SharedPool {
                min_size: bounded(1),
                max_size: PoolSizeSetting::PoolSizeFromSeats {},
                check: false,
            },
            executor: ExecutorThreadsSetting::ExecutorThreads {
                threads: bounded(4),
            },
            task_send_pool: PoolSize::new(bounded(3)),
        }),
        shared_thread: SharedThreadSetting::NoSharedThreadChildren {},
        pool: ParentPool {
            max_size: PoolSizeSetting::PoolSizeFromSeats {},
        },
        claim: ClaimLimits {
            per_queue_per_pass: TaskLimitSetting::TaskLimit { tasks: bounded(5) },
            claimed_per_worker: TaskLimitSetting::NoTaskLimit {},
        },
        drain_timeout: DrainTimeout::try_from(Duration::from_millis(1500))
            .expect("1.5 s is a drain timeout"),
        preload: ChildPreload::NoPreload,
        parent_cpu: ParentCpu::Shared,
    };
    assert_eq!(profile, expected);
}

#[test]
fn a_drain_timeout_is_whole_milliseconds_from_one_to_a_day() {
    for (milliseconds, duration) in [
        (1, Duration::from_millis(1)),
        (MAX_DRAIN_TIMEOUT_MS, MAX_DRAIN_TIMEOUT),
    ] {
        let mut profile = full_profile();
        profile["drain_timeout_ms"] = json!(milliseconds);
        let manifest = parsed(&manifest(json!({"batch": profile}), named("batch")));
        assert_eq!(
            manifest.profiles["batch"].to_profile().drain_timeout.get(),
            duration
        );
    }
    for outside in [
        json!(0),
        json!(MAX_DRAIN_TIMEOUT_MS + 1),
        json!(1.5),
        json!({"secs": 1, "nanos": 0}),
    ] {
        let mut profile = full_profile();
        profile["drain_timeout_ms"] = outside;
        assert_eq!(
            refused_path(&manifest(json!({"batch": profile}), named("batch"))),
            "profiles.batch.drain_timeout_ms"
        );
    }
}

#[test]
fn an_unknown_field_or_a_missing_one_is_refused_with_its_path() {
    let mut profile = full_profile();
    profile["pool"]["min_size"] = json!(1);
    assert_eq!(
        refused_path(&manifest(json!({"batch": profile}), named("batch"))),
        "profiles.batch.pool.min_size"
    );
    let mut profile = full_profile();
    let removed = profile
        .as_object_mut()
        .expect("a profile is an object")
        .remove("preload");
    assert!(removed.is_some());
    assert!(
        refused_path(&manifest(json!({"batch": profile}), named("batch")))
            .starts_with("profiles.batch")
    );
}

#[test]
fn another_manifest_version_is_refused() {
    let mut document = manifest(json!({"batch": full_profile()}), named("batch"));
    document["manifest_version"] = json!(2);
    assert_eq!(refused_path(&document), "manifest_version");
}

#[test]
fn a_refusal_never_holds_the_url_password() {
    let mut document = manifest(json!({"batch": full_profile()}), named("batch"));
    document["claim"]["prefetch_buffer"] = json!("postgresql://u:secret@h/d");
    let refused = parse_worker_manifest(&document.to_string()).expect_err("text is not a count");
    assert_eq!(refused.field_path, "claim.prefetch_buffer");
    assert!(!refused.to_string().contains("secret"));
    let manifest = parsed(&manifest(json!({"batch": full_profile()}), named("batch")));
    assert!(!format!("{manifest:?}").contains("secret"));
}

#[test]
fn the_named_profile_is_selected() {
    let manifest = parsed(&manifest(
        json!({"batch": full_profile(), "small": isolated_profile(1)}),
        named("small"),
    ));
    let selected = manifest
        .selected_profile()
        .expect("small is a profile of the set");
    assert_eq!(selected.name, "small");
    assert_eq!(selected.profile, manifest.profiles["small"].to_profile());
}

/// Python `test_cli.py` profile selection: no name and an unknown name are
/// refused with the defined names, sorted, and Python's text.
#[test]
fn no_name_and_an_unknown_name_are_refused_with_the_defined_names() {
    let profiles = json!({"small": isolated_profile(1), "batch": full_profile()});
    let not_given = parsed(&manifest(profiles.clone(), json!({"kind": "NOT_GIVEN"})))
        .selected_profile()
        .expect_err("no profile was named");
    let ProfileSelectionRefused::NotGiven { defined } = &not_given else {
        panic!("expected NotGiven, got {not_given:?}");
    };
    assert_eq!(defined, &["batch", "small"]);
    let [error]: [HorsiesError; 1] = not_given.errors().try_into().expect("one error");
    assert_eq!(error.message, "--profile is required");
    assert_eq!(error.code, Some(ErrorCode::CliInvalidArgs));
    assert_eq!(
        error.notes,
        [
            "a worker takes its settings from a named profile in AppConfig.worker_profiles; \
             there is no default profile",
            "defined profiles: batch, small",
        ]
    );
    assert_eq!(
        error.help_text.as_deref(),
        Some("start the worker with: horsies worker <app> --profile batch")
    );

    let unknown = parsed(&manifest(profiles, named("large")))
        .selected_profile()
        .expect_err("large is not a profile of the set");
    let [error]: [HorsiesError; 1] = unknown.errors().try_into().expect("one error");
    assert_eq!(error.message, "unknown worker profile 'large'");
    assert_eq!(error.notes, ["defined profiles: batch, small"]);
}

#[test]
fn an_app_with_no_profiles_is_told_how_to_add_one() {
    let refused = parsed(&manifest(json!({}), named("default")))
        .selected_profile()
        .expect_err("the set is empty");
    let [error]: [HorsiesError; 1] = refused.errors().try_into().expect("one error");
    assert_eq!(
        error.notes,
        ["the app defines no worker profiles: AppConfig.worker_profiles is empty"]
    );
    assert_eq!(
        error.help_text.as_deref(),
        Some(
            "add a profile to the app config, for example:\n  AppConfig(..., worker_profiles={\n      \
             'default': WorkerProfile(isolated=IsolatedChildren(count=4)),\n  })\nthen start the \
             worker with: horsies worker <app> --profile default"
        )
    );
}

/// The set is checked as AppConfig validation checks it, also the profiles
/// that the start does not name.
#[test]
fn a_profile_of_the_set_that_validation_refuses_stops_the_selection() {
    let mut idle = full_profile();
    idle["isolated"] = json!({"kind": "NO_CHILDREN"});
    idle["shared_async"] = json!({"kind": "NO_CHILDREN"});
    let refused = parsed(&manifest(
        json!({"batch": full_profile(), "idle": idle}),
        named("batch"),
    ))
    .selected_profile()
    .expect_err("idle starts no children");
    let ProfileSelectionRefused::InvalidProfiles { errors } = &refused else {
        panic!("expected InvalidProfiles, got {refused:?}");
    };
    let messages: Vec<&str> = errors.iter().map(|error| error.message.as_str()).collect();
    assert_eq!(
        messages,
        ["worker_profiles['idle']: the profile starts no children"]
    );
}

/// The claim check takes the prefetch buffer of the manifest.
#[test]
fn a_claim_limit_above_the_capacity_with_the_prefetch_buffer_is_refused() {
    let mut profile = isolated_profile(1);
    profile["claim"]["claimed_per_worker"] = json!({"kind": "TASKS", "tasks": 3});
    let mut document = manifest(json!({"small": profile}), named("small"));
    document["claim"]["prefetch_buffer"] = json!(2);
    assert!(parsed(&document).selected_profile().is_ok());
    document["claim"]["prefetch_buffer"] = json!(1);
    let refused = parsed(&document)
        .selected_profile()
        .expect_err("3 tasks is above 1 seat and a prefetch buffer of 1");
    assert!(matches!(
        refused,
        ProfileSelectionRefused::InvalidProfiles { .. }
    ));
}

/// Python `PostgresConfig.worker_process_config`: the pool of the worker
/// process is the resolved pool of its profile, with no overflow and the
/// worker pre-ping.
#[test]
fn the_worker_process_pool_is_the_resolved_pool_of_the_profile() {
    let manifest = parsed(&manifest(json!({"batch": full_profile()}), named("batch")));
    let selected = manifest
        .selected_profile()
        .expect("batch is a valid profile");
    let config = manifest.worker_process_config(&selected);
    // 2 isolated seats and 20 shared async seats.
    assert_eq!(config.pool_size, parent_pool_connections(22).get());
    assert_eq!(config.max_overflow, 0);
    assert!(!config.pool_pre_ping);
    assert!(!config.worker_pool_pre_ping);
    assert!(config.retain_rerun_input_default);
    assert_eq!(config.session_database_url, None);
    assert_eq!(
        config.database_url,
        "postgresql://user:secret@localhost:5432/db"
    );
    assert_eq!(config.socket_settings(), crate::db::DEFAULT_SOCKET_SETTINGS);
}

#[test]
fn keepalives_off_and_the_system_timeout_reach_the_worker_process_config() {
    let mut document = manifest(json!({"batch": full_profile()}), named("batch"));
    document["database"]["tcp_keepalives"] = json!({"kind": "OFF"});
    document["database"]["tcp_user_timeout"] = json!({"kind": "SYSTEM_DEFAULT"});
    let manifest = parsed(&document);
    let selected = manifest
        .selected_profile()
        .expect("batch is a valid profile");
    let settings = manifest.worker_process_config(&selected).socket_settings();
    assert_eq!(settings.keepalives, crate::db::Keepalives::Off);
    assert_eq!(
        settings.tcp_user_timeout,
        crate::db::TcpUserTimeout::SystemDefault
    );
}

/// Both socket settings are required, as in the producer manifest.
#[test]
fn a_missing_or_out_of_bound_socket_setting_is_refused() {
    for field in ["tcp_keepalives", "tcp_user_timeout"] {
        let mut document = manifest(json!({"batch": full_profile()}), named("batch"));
        let removed = document["database"]
            .as_object_mut()
            .expect("the database section is an object")
            .remove(field);
        assert!(removed.is_some());
        assert!(refused_path(&document).starts_with("database"), "{field}");
    }
    let mut document = manifest(json!({"batch": full_profile()}), named("batch"));
    document["database"]["tcp_keepalives"]["probes"] = json!(0);
    // An internally tagged enum reads its content after its tag, so the path
    // ends at the enum.
    assert!(refused_path(&document).starts_with("database.tcp_keepalives"));
}

#[test]
fn each_choice_parses_from_its_wire_name() {
    for (wire, level) in [
        ("DEBUG", WorkerLogLevel::Debug),
        ("INFO", WorkerLogLevel::Info),
        ("WARNING", WorkerLogLevel::Warning),
        ("ERROR", WorkerLogLevel::Error),
        ("CRITICAL", WorkerLogLevel::Critical),
    ] {
        let mut document = manifest(json!({"batch": full_profile()}), named("batch"));
        document["child_launch"]["log_level"] = json!(wire);
        assert_eq!(parsed(&document).child_launch.log_level, level);
    }
    for (wire, preload) in [
        ("LIBRARY", ChildPreload::Library),
        ("APP", ChildPreload::App),
        ("NO_PRELOAD", ChildPreload::NoPreload),
    ] {
        let mut profile = full_profile();
        profile["preload"] = json!(wire);
        let manifest = parsed(&manifest(json!({"batch": profile}), named("batch")));
        assert_eq!(manifest.profiles["batch"].to_profile().preload, preload);
    }
    let mut document = manifest(json!({"batch": full_profile()}), named("batch"));
    document["claim"]["claim_lease"] = json!({"kind": "MILLISECONDS", "ms": 30_000});
    document["claim"]["queues"] = json!({"kind": "DEFAULT"});
    document["child_launch"]["program"] = json!({"kind": "RUST"});
    let manifest = parsed(&document);
    assert_eq!(manifest.claim.queues, Queues::Default);
    assert_eq!(manifest.child_launch.program, ChildProgram::Rust);
    assert!(matches!(
        manifest.claim.claim_lease,
        ClaimLease::Milliseconds { ms } if ms.0.get() == 30_000
    ));
}

#[test]
fn a_queue_priority_outside_one_to_a_hundred_is_refused() {
    for priority in [0, MAX_QUEUE_PRIORITY + 1] {
        let mut document = manifest(json!({"batch": full_profile()}), named("batch"));
        document["claim"]["queues"]["queues"][0]["priority"] = json!(priority);
        // An internally tagged enum reads its content after its tag, so the
        // path ends at the enum.
        assert!(refused_path(&document).starts_with("claim.queues"));
    }
}

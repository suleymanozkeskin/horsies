//! The parse of a worker manifest, the profile it resolves and the broker
//! config of its worker process.

use std::time::Duration;

use serde_json::{json, Value};

use super::*;
use crate::core::config::child_sizing::parent_pool_connections;
use crate::core::error::{ErrorCode, HorsiesError};

/// A profile with every setting of every kind of child.
pub(crate) fn full_profile() -> Value {
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
/// `clients/python/horsies/core/models/worker_profile.py:1012-1039`), and the 5 tasks of
/// [`full_profile`] are above a capacity of fewer than 5 seats.
fn isolated_profile(count: u32) -> Value {
    let mut profile = full_profile();
    profile["isolated"]["count"] = json!(count);
    profile["shared_async"] = json!({"kind": "NO_CHILDREN"});
    profile["claim"]["per_queue_per_pass"] = json!({"kind": "TASKS", "tasks": count});
    profile
}

pub(crate) fn manifest(profiles: &Value, request: &Value) -> Value {
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
        "metrics_endpoint": {"kind": "NO_METRICS_ENDPOINT"},
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
        "recovery": recovery_section(),
        "retention": retention_section(),
        "resilience": {
            "db_retry_initial_ms": 250,
            "db_retry_max_ms": 20_000,
            "db_retry_max_attempts": {"kind": "ATTEMPTS", "attempts": 7},
            "notify_poll_interval_ms": 4_000
        },
        "payload": {
            "warn_bytes": {"kind": "BYTES", "bytes": 1_048_576},
            "reject_bytes": {"kind": "NO_LIMIT"}
        },
        "tasks": {
            "app.tasks.add": {"execution": "ISOLATED", "error_codes": []},
            "app.tasks.fetch": {
                "execution": "SHARED_ASYNC",
                "error_codes": ["RATE_LIMITED", "NOT_FOUND"]
            },
            "app.tasks.resize": {"execution": "SHARED_THREAD", "error_codes": []}
        },
        "handlers": {"kind": "NO_HANDLERS"},
        "worker_ended_spans": "NO_SPANS",
        "child_launch": {
            "command": {
                "program": "/usr/bin/python3",
                "args": ["-X", "dev", "-m", "horsies.core.worker.child_main"]
            },
            "preload": {
                "kind": "COMMAND",
                "command": {
                    "program": "/usr/bin/python3",
                    "args": ["-X", "dev", "-m", "horsies.core.worker.preload_main"]
                }
            },
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

/// A recovery section with a value of its own in every field.
fn recovery_section() -> Value {
    json!({
        "auto_requeue_stale_claimed": true,
        "claimed_stale_threshold_ms": 120_000,
        "auto_terminate_orphaned_workflow_tasks": false,
        "auto_fail_stale_running": true,
        "running_stale_threshold_ms": 300_000,
        "finalizing_stale_threshold_ms": 400_000,
        "crashed_worker_recovery_grace_ms": 0,
        "phase2_quarantine_after_attempts": 25,
        "workflow_recovery_failure_limit": 30,
        "check_interval_ms": 30_000,
        "orphan_task_audit_interval_ms": 60_000,
        "runner_heartbeat_interval_ms": 20_000,
        "claimer_heartbeat_interval_ms": 25_000,
        "worker_state_snapshot_interval_ms": 15_000
    })
}

/// A retention section with a value of its own in every field.
fn retention_section() -> Value {
    json!({
        "worker_state_retention": {"kind": "HOURS", "hours": 168},
        "terminal_record_retention": {"kind": "NO_PRUNING"},
        "paused_workflow_auto_cancel_after": {"kind": "AFTER", "micros": 86_400_000_001_u64},
        "history_leaf_horizon_days": 3,
        "heartbeat_leaf_horizon_hours": 6,
        "retention_periods": [7, 90],
        "queue_retention": {
            "high": {"kind": "DAYS", "days": 14},
            "low": {"kind": "FOREVER"}
        },
        "partition_maintenance_interval_s": 900,
        "retention_sweep_interval_s": 300,
        "retention_delete_batch_size": 500
    })
}

pub(crate) fn named(name: &str) -> Value {
    json!({"kind": "NAMED", "name": name})
}

pub(crate) fn parsed(document: &Value) -> WorkerManifest {
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
    let manifest = parsed(&manifest(
        &json!({"batch": full_profile()}),
        &named("batch"),
    ));
    assert_eq!(manifest.manifest_version, WorkerManifestVersion::CURRENT);
    assert_eq!(
        manifest.profile,
        ProfileRequest::Named {
            name: "batch".to_owned()
        }
    );
    let fetch = &manifest.tasks["app.tasks.fetch"];
    assert_eq!(fetch.execution, Execution::SharedAsync);
    let codes: Vec<&str> = fetch.error_codes.iter().map(|code| code.as_str()).collect();
    assert_eq!(codes, ["NOT_FOUND", "RATE_LIMITED"]);
    assert!(manifest.tasks["app.tasks.add"].error_codes.is_empty());
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
    let manifest = parsed(&manifest(
        &json!({"batch": full_profile()}),
        &named("batch"),
    ));
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
        let manifest = parsed(&manifest(&json!({"batch": profile}), &named("batch")));
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
            refused_path(&manifest(&json!({"batch": profile}), &named("batch"))),
            "profiles.batch.drain_timeout_ms"
        );
    }
}

#[test]
fn an_unknown_field_or_a_missing_one_is_refused_with_its_path() {
    let mut profile = full_profile();
    profile["pool"]["min_size"] = json!(1);
    assert_eq!(
        refused_path(&manifest(&json!({"batch": profile}), &named("batch"))),
        "profiles.batch.pool.min_size"
    );
    let mut profile = full_profile();
    let removed = profile
        .as_object_mut()
        .expect("a profile is an object")
        .remove("preload");
    assert!(removed.is_some());
    assert!(
        refused_path(&manifest(&json!({"batch": profile}), &named("batch")))
            .starts_with("profiles.batch")
    );
}

/// The parser applies the rules of a declared set
/// (`DeclaredErrorCodes::parse`) and names the field of the task.
#[test]
fn a_bad_set_of_declared_codes_is_refused_at_the_task_field() {
    for codes in [
        json!(["RATE_LIMITED", "RATE_LIMITED"]),
        json!(["rate_limited"]),
        json!(["TimeoutError"]),
        json!([""]),
        json!(["TASK_TIMEOUT"]),
        json!(["WORKFLOW_STOPPED"]),
        json!([7]),
        json!("RATE_LIMITED"),
    ] {
        let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
        document["tasks"]["app.tasks.fetch"]["error_codes"] = codes.clone();
        let path = refused_path(&document);
        assert!(
            path.starts_with("tasks.app.tasks.fetch.error_codes"),
            "{codes}: {path}"
        );
    }
}

/// The metrics endpoint is an address with a host and a port in 1 to
/// 65535, or no endpoint; the field has no default.
#[test]
fn a_metrics_endpoint_is_an_address_in_bounds_or_none() {
    for (endpoint, host, port) in [
        (
            json!({"kind": "LISTEN_ADDRESS", "host": "0.0.0.0", "port": 9100}),
            "0.0.0.0",
            9100,
        ),
        (
            json!({"kind": "LISTEN_ADDRESS", "host": "::1", "port": 1}),
            "::1",
            1,
        ),
        (
            json!({"kind": "LISTEN_ADDRESS", "host": "metrics.internal", "port": 65_535}),
            "metrics.internal",
            65_535,
        ),
    ] {
        let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
        document["metrics_endpoint"] = endpoint;
        match parsed(&document).metrics_endpoint {
            MetricsEndpoint::ListenAddress {
                host: parsed_host,
                port: parsed_port,
            } => {
                assert_eq!(parsed_host.as_str(), host);
                assert_eq!(parsed_port.get(), port);
            }
            MetricsEndpoint::NoMetricsEndpoint => panic!("an address is an endpoint"),
        }
    }
    let document = manifest(&json!({"batch": full_profile()}), &named("batch"));
    assert_eq!(
        parsed(&document).metrics_endpoint,
        MetricsEndpoint::NoMetricsEndpoint
    );

    for endpoint in [
        json!({"kind": "LISTEN_ADDRESS", "host": "", "port": 9100}),
        json!({"kind": "LISTEN_ADDRESS", "host": "a".repeat(254), "port": 9100}),
        json!({"kind": "LISTEN_ADDRESS", "host": "0.0.0.0", "port": 0}),
        json!({"kind": "LISTEN_ADDRESS", "host": "0.0.0.0", "port": 65_536}),
        json!({"kind": "LISTEN_ADDRESS", "host": "0.0.0.0"}),
        json!({"kind": "LISTEN_ADDRESS", "port": 9100}),
        json!({"kind": "LISTEN_ADDRESS", "host": "0.0.0.0", "port": 9100, "path": "/m"}),
        json!({"kind": "NO_METRICS_ENDPOINT", "port": 9100}),
        json!({"kind": "DEFAULT"}),
        json!("0.0.0.0:9100"),
    ] {
        let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
        document["metrics_endpoint"] = endpoint.clone();
        let path = refused_path(&document);
        assert!(path.starts_with("metrics_endpoint"), "{endpoint}: {path}");
    }
    let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
    let _removed = document
        .as_object_mut()
        .expect("a manifest is an object")
        .remove("metrics_endpoint");
    assert!(parse_worker_manifest(&document.to_string()).is_err());
}

/// A task record holds both facts and nothing else.
#[test]
fn a_task_record_without_its_codes_or_with_another_field_is_refused() {
    let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
    document["tasks"]["app.tasks.add"] = json!({"execution": "ISOLATED"});
    assert!(refused_path(&document).starts_with("tasks."));
    let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
    document["tasks"]["app.tasks.add"] = json!("ISOLATED");
    assert!(refused_path(&document).starts_with("tasks."));
    let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
    document["tasks"]["app.tasks.add"]["queue"] = json!("default");
    assert!(refused_path(&document).starts_with("tasks."));
}

/// The handlers of the app parse with their names and types, in order;
/// both span switches parse from their wire names.
#[test]
fn the_handlers_and_the_span_switch_parse() {
    let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
    document["handlers"] = json!({
        "kind": "HANDLERS",
        "handlers": [
            {"name": "app.hooks.on_end", "event_type": "AttemptEnded"},
            {"name": "app.hooks.on_claim", "event_type": "TaskClaimed"}
        ]
    });
    document["worker_ended_spans"] = json!("MAKE_SPANS");

    let manifest = parsed(&document);

    let EventHandlers::Handlers { handlers } = &manifest.handlers else {
        panic!("the manifest has handlers: {:?}", manifest.handlers);
    };
    let parsed_handlers: Vec<(&str, EventTypeName)> = handlers
        .handlers()
        .iter()
        .map(|handler| (handler.name.as_str(), handler.event_type))
        .collect();
    assert_eq!(
        parsed_handlers,
        [
            ("app.hooks.on_end", EventTypeName::AttemptEnded),
            ("app.hooks.on_claim", EventTypeName::TaskClaimed),
        ]
    );
    assert_eq!(manifest.worker_ended_spans, WorkerEndedSpans::MakeSpans);
    let complete = parsed(&manifest_with_no_handler());
    assert_eq!(complete.handlers, EventHandlers::NoHandlers);
    assert_eq!(complete.worker_ended_spans, WorkerEndedSpans::NoSpans);
}

fn manifest_with_no_handler() -> Value {
    manifest(&json!({"batch": full_profile()}), &named("batch"))
}

/// A handler list is refused at its field when it is empty, holds more than
/// `MAX_EVENT_HANDLERS` handlers or a name twice, has a name that is empty,
/// too long or `(unknown)`, or names no event type of the contract.
#[test]
fn a_bad_handler_list_is_refused_at_the_handlers_field() {
    let handler = |name: &str, event_type: &str| json!({"name": name, "event_type": event_type});
    let too_many: Vec<Value> = (0..=MAX_EVENT_HANDLERS)
        .map(|index| handler(&format!("app.hooks.h{index}"), "TaskClaimed"))
        .collect();
    let longest = "h".repeat(MAX_HANDLER_NAME_CHARS);
    let too_long = "h".repeat(MAX_HANDLER_NAME_CHARS + 1);
    for handlers in [
        json!([]),
        Value::Array(too_many),
        json!([
            handler("app.hooks.a", "TaskClaimed"),
            handler("app.hooks.a", "AttemptEnded")
        ]),
        json!([handler("", "TaskClaimed")]),
        json!([handler("(unknown)", "TaskClaimed")]),
        json!([handler(&too_long, "TaskClaimed")]),
        json!([handler("app.hooks.a", "TaskStarted")]),
        json!([handler("app.hooks.a", "task_claimed")]),
        json!([{"name": "app.hooks.a"}]),
        json!([{"name": "app.hooks.a", "event_type": "TaskClaimed", "queue": "default"}]),
    ] {
        let mut document = manifest_with_no_handler();
        document["handlers"] = json!({"kind": "HANDLERS", "handlers": handlers.clone()});
        let path = refused_path(&document);
        assert!(path.starts_with("handlers"), "{handlers}: {path}");
    }
    let mut document = manifest_with_no_handler();
    document["handlers"] = json!({"kind": "HANDLERS", "handlers": [handler(&longest, "SlotDown")]});
    assert!(matches!(
        parsed(&document).handlers,
        EventHandlers::Handlers { .. }
    ));
}

/// The span switch has two wire names, and both fields are required.
#[test]
fn the_handler_fields_are_required_and_closed() {
    let mut document = manifest_with_no_handler();
    document["worker_ended_spans"] = json!("SPANS");
    assert!(refused_path(&document).starts_with("worker_ended_spans"));
    let mut document = manifest_with_no_handler();
    document["handlers"] = json!({"kind": "NO_HANDLERS", "handlers": []});
    assert!(refused_path(&document).starts_with("handlers"));
    for field in ["handlers", "worker_ended_spans"] {
        let mut document = manifest_with_no_handler();
        let removed = document
            .as_object_mut()
            .expect("a manifest is an object")
            .remove(field);
        assert!(removed.is_some(), "{field}");
        assert!(
            parse_worker_manifest(&document.to_string()).is_err(),
            "a manifest without {field} is refused"
        );
    }
}

/// The declared codes of a parsed task give the label of each error code.
#[test]
fn the_declared_codes_of_a_task_give_its_error_code_labels() {
    use crate::core::task::{
        error_code_label, ErrorCodeLabel, OperationalErrorCode, TaskErrorCode, UserCode,
    };
    let manifest = parsed(&manifest(
        &json!({"batch": full_profile()}),
        &named("batch"),
    ));
    let fetch = &manifest.tasks["app.tasks.fetch"].error_codes;
    let declared = TaskErrorCode::User(UserCode::parse("RATE_LIMITED").expect("a user code"));
    let other = TaskErrorCode::User(UserCode::parse("GONE").expect("a user code"));
    let built_in = TaskErrorCode::from(OperationalErrorCode::BrokerError);
    assert_eq!(error_code_label(fetch, &declared).value(), "RATE_LIMITED");
    assert_eq!(error_code_label(fetch, &other), ErrorCodeLabel::Undeclared);
    assert_eq!(error_code_label(fetch, &built_in).value(), "BROKER_ERROR");
}

#[test]
fn another_manifest_version_is_refused() {
    let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
    document["manifest_version"] = json!(2);
    assert_eq!(refused_path(&document), "manifest_version");
}

#[test]
fn a_refusal_never_holds_the_url_password() {
    let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
    document["claim"]["prefetch_buffer"] = json!("postgresql://u:secret@h/d");
    let refused = parse_worker_manifest(&document.to_string()).expect_err("text is not a count");
    assert_eq!(refused.field_path, "claim.prefetch_buffer");
    assert!(!refused.to_string().contains("secret"));
    let manifest = parsed(&manifest(
        &json!({"batch": full_profile()}),
        &named("batch"),
    ));
    assert!(!format!("{manifest:?}").contains("secret"));
}

#[test]
fn the_named_profile_is_selected() {
    let manifest = parsed(&manifest(
        &json!({"batch": full_profile(), "small": isolated_profile(1)}),
        &named("small"),
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
    let not_given = parsed(&manifest(&profiles, &json!({"kind": "NOT_GIVEN"})))
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

    let unknown = parsed(&manifest(&profiles, &named("large")))
        .selected_profile()
        .expect_err("large is not a profile of the set");
    let [error]: [HorsiesError; 1] = unknown.errors().try_into().expect("one error");
    assert_eq!(error.message, "unknown worker profile 'large'");
    assert_eq!(error.notes, ["defined profiles: batch, small"]);
}

#[test]
fn an_app_with_no_profiles_is_told_how_to_add_one() {
    let refused = parsed(&manifest(&json!({}), &named("default")))
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
        &json!({"batch": full_profile(), "idle": idle}),
        &named("batch"),
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
    let mut document = manifest(&json!({"small": profile}), &named("small"));
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
    let manifest = parsed(&manifest(
        &json!({"batch": full_profile()}),
        &named("batch"),
    ));
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
    let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
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
        let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
        let removed = document["database"]
            .as_object_mut()
            .expect("the database section is an object")
            .remove(field);
        assert!(removed.is_some());
        assert!(refused_path(&document).starts_with("database"), "{field}");
    }
    let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
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
        let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
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
        let manifest = parsed(&manifest(&json!({"batch": profile}), &named("batch")));
        assert_eq!(manifest.profiles["batch"].to_profile().preload, preload);
    }
    let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
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
        let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
        document["claim"]["queues"]["queues"][0]["priority"] = json!(priority);
        // An internally tagged enum reads its content after its tag, so the
        // path ends at the enum.
        assert!(refused_path(&document).starts_with("claim.queues"));
    }
}

/// Every field of the recovery section reaches the config type.
#[test]
fn the_recovery_section_maps_to_the_config_field_by_field() {
    let manifest = parsed(&manifest(
        &json!({"batch": full_profile()}),
        &named("batch"),
    ));
    let config = manifest.recovery.to_config();
    assert!(config.auto_requeue_stale_claimed);
    assert_eq!(config.claimed_stale_threshold_ms, 120_000);
    assert!(!config.auto_terminate_orphaned_workflow_tasks);
    assert!(config.auto_fail_stale_running);
    assert_eq!(config.running_stale_threshold_ms, 300_000);
    assert_eq!(config.finalizing_stale_threshold_ms, 400_000);
    assert_eq!(config.crashed_worker_recovery_grace_ms, 0);
    assert_eq!(config.phase2_quarantine_after_attempts, 25);
    assert_eq!(config.workflow_recovery_failure_limit, 30);
    assert_eq!(config.check_interval_ms, 30_000);
    assert_eq!(config.orphan_task_audit_interval_ms, 60_000);
    assert_eq!(config.runner_heartbeat_interval_ms, 20_000);
    assert_eq!(config.claimer_heartbeat_interval_ms, 25_000);
    assert_eq!(config.worker_state_snapshot_interval_ms, 15_000);
    assert!(config.validate().is_empty(), "{:?}", config.validate());
}

/// Every field of the retention section reaches the config type; a period
/// is whole days and the paused age is exact to the microsecond.
#[test]
fn the_retention_section_maps_to_the_config_field_by_field() {
    let manifest = parsed(&manifest(
        &json!({"batch": full_profile()}),
        &named("batch"),
    ));
    let config = manifest.retention.to_config();
    assert_eq!(config.worker_state_retention_hours, Some(168));
    assert_eq!(config.terminal_record_retention_hours, None);
    assert_eq!(
        config
            .paused_workflow_auto_cancel_after
            .map(crate::core::config::PausedWorkflowAge::micros),
        Some(86_400_000_001)
    );
    assert_eq!(config.history_leaf_horizon_days, 3);
    assert_eq!(config.heartbeat_leaf_horizon_hours, 6);
    assert_eq!(
        config.retention_periods,
        [chrono::Duration::days(7), chrono::Duration::days(90)]
    );
    assert_eq!(config.queue_retention.len(), 2);
    assert_eq!(
        config.queue_retention.get("high"),
        Some(&Some(chrono::Duration::days(14)))
    );
    assert_eq!(config.queue_retention.get("low"), Some(&None));
    assert_eq!(config.partition_maintenance_interval_s, 900);
    assert_eq!(config.retention_sweep_interval_s, 300);
    assert_eq!(config.retention_delete_batch_size, 500);
    assert!(config.validate().is_empty(), "{:?}", config.validate());
}

/// The other variant of each retention choice.
#[test]
fn a_retention_rule_that_is_off_maps_to_none() {
    let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
    document["retention"]["worker_state_retention"] = json!({"kind": "NO_PRUNING"});
    document["retention"]["terminal_record_retention"] = json!({"kind": "HOURS", "hours": 43_800});
    document["retention"]["paused_workflow_auto_cancel_after"] = json!({"kind": "NO_EXPIRY"});
    let config = parsed(&document).retention.to_config();
    assert_eq!(config.worker_state_retention_hours, None);
    assert_eq!(config.terminal_record_retention_hours, Some(43_800));
    assert_eq!(config.paused_workflow_auto_cancel_after, None);
}

/// A period outside 1 to 1825 days, a paused age of 0 and a paused age
/// above the bound of the retention config are refused; the bound itself
/// reaches the config.
#[test]
fn a_retention_value_outside_its_bounds_is_refused() {
    for days in [
        0,
        u32::from(crate::core::history::periods::MAX_RETENTION_DAYS) + 1,
    ] {
        let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
        document["retention"]["retention_periods"] = json!([days]);
        assert!(
            refused_path(&document).starts_with("retention.retention_periods"),
            "{days}"
        );
    }
    let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
    document["retention"]["paused_workflow_auto_cancel_after"] =
        json!({"kind": "AFTER", "micros": 0});
    assert!(refused_path(&document).starts_with("retention.paused_workflow_auto_cancel_after"));
    let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
    document["retention"]["paused_workflow_auto_cancel_after"] =
        json!({"kind": "AFTER", "micros": MAX_PAUSED_AGE_MICROS + 1});
    assert!(refused_path(&document).starts_with("retention.paused_workflow_auto_cancel_after"));
    let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
    document["retention"]["paused_workflow_auto_cancel_after"] =
        json!({"kind": "AFTER", "micros": MAX_PAUSED_AGE_MICROS});
    let config = parsed(&document).retention.to_config();
    assert_eq!(
        config
            .paused_workflow_auto_cancel_after
            .map(crate::core::config::PausedWorkflowAge::micros),
        Some(crate::core::config::PausedWorkflowAge::MAX_MICROS)
    );
}

/// The resilience section reaches the config type; no attempt limit is the
/// config's 0.
#[test]
fn the_resilience_section_maps_to_the_config_and_no_limit_is_zero() {
    let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
    let config = parsed(&document).resilience.to_config();
    assert_eq!(config.db_retry_initial_ms, 250);
    assert_eq!(config.db_retry_max_ms, 20_000);
    assert_eq!(config.db_retry_max_attempts, 7);
    assert_eq!(config.notify_poll_interval_ms, 4_000);
    document["resilience"]["db_retry_max_attempts"] = json!({"kind": "NO_LIMIT"});
    assert_eq!(
        parsed(&document)
            .resilience
            .to_config()
            .db_retry_max_attempts,
        0
    );
    document["resilience"]["db_retry_max_attempts"] = json!({"kind": "ATTEMPTS", "attempts": 0});
    assert!(refused_path(&document).starts_with("resilience.db_retry_max_attempts"));
}

/// Each payload threshold maps to the config's `Option`; a threshold of 0
/// bytes is refused.
#[test]
fn the_payload_section_maps_to_the_config_and_refuses_zero_bytes() {
    let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
    let config = parsed(&document).payload.to_config();
    assert_eq!(config.warn_bytes, Some(1_048_576));
    assert_eq!(config.reject_bytes, None);
    document["payload"] = json!({
        "warn_bytes": {"kind": "NO_LIMIT"},
        "reject_bytes": {"kind": "BYTES", "bytes": MAX_PAYLOAD_BYTES}
    });
    let config = parsed(&document).payload.to_config();
    assert_eq!(config.warn_bytes, None);
    assert_eq!(config.reject_bytes, Some(MAX_PAYLOAD_BYTES));
    document["payload"]["reject_bytes"] = json!({"kind": "BYTES", "bytes": 0});
    assert!(refused_path(&document).starts_with("payload.reject_bytes"));
}

/// A missing section is refused with its name: no section has a default.
#[test]
fn a_missing_section_is_refused() {
    for section in ["recovery", "retention", "resilience", "payload"] {
        let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
        let removed = document
            .as_object_mut()
            .expect("a manifest is an object")
            .remove(section);
        assert!(removed.is_some(), "{section}");
        assert!(
            parse_worker_manifest(&document.to_string()).is_err(),
            "a manifest without {section} is refused"
        );
    }
}

/// The preload command keeps its interpreter flags; a child program with no
/// preload process parses too.
#[test]
fn the_preload_launch_parses_in_both_forms() {
    let mut document = manifest(&json!({"batch": full_profile()}), &named("batch"));
    let preload = parsed(&document).child_launch.preload;
    match preload {
        PreloadLaunch::Command { command } => {
            assert_eq!(command.program, "/usr/bin/python3");
            assert_eq!(
                command.args,
                ["-X", "dev", "-m", "horsies.core.worker.preload_main"]
            );
        }
        PreloadLaunch::NoPreloadProgram => panic!("the fixture has a preload command"),
    }
    document["child_launch"]["preload"] = json!({"kind": "NO_PRELOAD_PROGRAM"});
    assert_eq!(
        parsed(&document).child_launch.preload,
        PreloadLaunch::NoPreloadProgram
    );
    let removed = document["child_launch"]
        .as_object_mut()
        .expect("the child launch is an object")
        .remove("preload");
    assert!(removed.is_some());
    assert!(refused_path(&document).starts_with("child_launch"));
}

// --- the profile set of the live check ----------------------------------------

/// The profile set that the Python live check sends is read as the
/// `profiles` member of a worker manifest is.
#[test]
fn a_profile_set_parses_as_the_profiles_of_a_manifest() {
    let profiles = json!({"batch": full_profile(), "small": isolated_profile(1)});

    let set = parse_worker_profiles(&profiles.to_string()).expect("a valid profile set");

    let from_manifest = parsed(&manifest(&profiles, &named("batch"))).profile_set();
    assert_eq!(set, from_manifest);
}

/// An empty object is the set of an app with no profile.
#[test]
fn an_empty_profile_set_parses_to_no_profile() {
    let set = parse_worker_profiles("{}").expect("an empty profile set");
    assert!(set.is_empty());
}

/// A profile with a field the manifest does not have is refused with its
/// path, as in a worker manifest.
#[test]
fn a_profile_set_with_an_unknown_field_is_refused() {
    let mut profile = isolated_profile(1);
    profile["children"] = json!(3);

    let refused = parse_worker_profiles(&json!({"small": profile}).to_string())
        .expect_err("an unknown field is refused");

    assert!(refused.field_path.starts_with("small"), "{refused}");
}

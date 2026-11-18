//! The worker plan of a worker manifest (Python `cli.py`, `worker_command`
//! and `run_worker`).

use std::collections::HashMap;

use serde_json::json;

use super::*;
use crate::core::config::worker_profile::ProfileSelectionRefused;
use crate::worker_manifest::parse_worker_manifest;
use crate::worker_manifest::tests::{full_profile, manifest, named, parsed};

fn batch_plan() -> WorkerPlan {
    let manifest = parsed(&manifest(json!({"batch": full_profile()}), named("batch")));
    match WorkerPlan::of_manifest(&manifest) {
        Ok(plan) => plan,
        Err(refused) => panic!("the batch manifest gives a plan: {refused}"),
    }
}

#[test]
fn the_plan_takes_the_queues_and_claim_settings_of_the_claim_section() {
    let plan = batch_plan();
    let app = &plan.app_config;
    assert_eq!(app.queue_mode, QueueMode::Custom);
    let queues = app.custom_queues.as_ref().expect("custom queues");
    assert_eq!(
        queues
            .iter()
            .map(|queue| (queue.name.as_str(), queue.priority, queue.max_concurrency))
            .collect::<Vec<_>>(),
        vec![("high", 1, Some(0)), ("low", 100, None)]
    );
    assert_eq!(app.cluster_wide_cap, Some(20));
    assert_eq!(app.prefetch_buffer, 0);
    assert_eq!(app.claim_lease_ms, None);
    assert_eq!(app.max_claim_renew_age_ms, 180_000);
    assert_eq!(plan.worker_config.queues, vec!["high", "low"]);
    assert_eq!(
        plan.worker_config.queue_priorities,
        HashMap::from([("high".to_owned(), 1), ("low".to_owned(), 100)])
    );
    assert_eq!(
        plan.worker_config.queue_max_concurrency,
        HashMap::from([("high".to_owned(), 0)])
    );
}

#[test]
fn the_plan_takes_the_child_launch_and_the_registered_classes() {
    let plan = batch_plan();
    let launch = &plan.launch;
    assert_eq!(
        launch.command,
        ChildCommand {
            program: PathBuf::from("/usr/bin/python3"),
            args: ["-X", "dev", "-m", "horsies.core.worker.child_main"]
                .map(OsString::from)
                .to_vec(),
        }
    );
    assert_eq!(
        launch.preload,
        PreloadProgram::Command(ChildCommand {
            program: PathBuf::from("/usr/bin/python3"),
            args: ["-X", "dev", "-m", "horsies.core.worker.preload_main"]
                .map(OsString::from)
                .to_vec(),
        })
    );
    match &launch.program {
        ChildProgram::Python(program) => {
            assert_eq!(program.app_locator, "app.main:app");
            assert_eq!(program.imports, vec!["app.tasks"]);
        }
        ChildProgram::Rust => panic!("the manifest names a Python child"),
    }
    assert_eq!(launch.log_level, ChildLogLevel::Info);
    let mut names: Vec<&str> = plan.registered.task_names().collect();
    names.sort_unstable();
    assert_eq!(
        names,
        vec!["app.tasks.add", "app.tasks.fetch", "app.tasks.resize"]
    );
    assert_eq!(plan.profile.name, "batch");
}

/// The worker process pool keeps the worker pre-ping; the children and the
/// runner heartbeat pool read the app's settings.
#[test]
fn the_worker_process_and_the_app_broker_configs_differ_only_where_python_does() {
    let plan = batch_plan();
    assert!(!plan.broker_config.pool_pre_ping);
    assert!(plan.app_config.broker.pool_pre_ping);
    assert_eq!(plan.broker_config.max_overflow, 0);
    assert_eq!(
        plan.broker_config.database_url,
        plan.app_config.broker.database_url
    );
}

#[test]
fn the_default_queue_mode_serves_the_one_default_queue() {
    let mut document = manifest(json!({"batch": full_profile()}), named("batch"));
    document["claim"]["queues"] = json!({"kind": "DEFAULT"});
    let plan = WorkerPlan::of_manifest(&parsed(&document)).expect("a plan");
    assert_eq!(plan.app_config.queue_mode, QueueMode::Default);
    assert!(plan.app_config.custom_queues.is_none());
    assert_eq!(plan.worker_config.queues, vec!["default"]);
}

#[test]
fn an_unknown_profile_gives_no_plan() {
    let manifest = parsed(&manifest(json!({"batch": full_profile()}), named("other")));
    assert!(matches!(
        WorkerPlan::of_manifest(&manifest),
        Err(WorkerPlanRefused::Profile(
            ProfileSelectionRefused::Unknown { .. }
        ))
    ));
}

#[test]
fn transaction_mode_with_no_session_url_is_a_refused_connection_setting() {
    let mut document = manifest(json!({"batch": full_profile()}), named("batch"));
    document["database"]["pgbouncer_transaction_mode"] = json!(true);
    let manifest = parse_worker_manifest(&document.to_string()).expect("the manifest parses");
    assert!(matches!(
        WorkerPlan::of_manifest(&manifest),
        Err(WorkerPlanRefused::ConnectionSettings { .. })
    ));
}

/// The broker of the worker process belongs to the client family of its
/// caller: the Python CLI's worker records and checks `PYTHON`, the Rust
/// client's `RUST`. Nothing connects.
#[tokio::test]
async fn the_worker_broker_is_of_the_family_of_its_caller() {
    let plan = batch_plan();
    for family in [ClientFamily::Python, ClientFamily::Rust] {
        let broker = match worker_broker(&plan.broker_config, family).await {
            Ok(broker) => broker,
            Err(refused) => panic!("the batch plan's broker config is valid: {refused}"),
        };
        assert_eq!(broker.client_family(), family);
    }
}

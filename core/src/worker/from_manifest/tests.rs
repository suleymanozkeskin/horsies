//! The worker plan of a worker manifest (Python `cli.py`, `worker_command`
//! and `run_worker`).

use std::collections::HashMap;

use serde_json::json;

use super::*;
use crate::core::config::worker_profile::ProfileSelectionRefused;
use crate::test_support::TestDatabase;
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

/// The install of the worker process's broker on an empty database records
/// the family of its caller: `PYTHON` for the Python CLI's worker, `RUST` for
/// the Rust client's. A worker broker that took a default family instead
/// recorded `RUST` for the Python worker.
#[tokio::test]
#[serial_test::serial]
async fn the_install_of_the_worker_broker_records_the_family_of_its_caller() {
    for family in [ClientFamily::Python, ClientFamily::Rust] {
        let database = TestDatabase::create_empty().await;
        let broker = match worker_broker(&PostgresConfig::from_url(database.url()), family).await {
            Ok(broker) => broker,
            Err(refused) => panic!("the config of a test database is valid: {refused}"),
        };
        if let Err(error) = broker.migrate().await {
            panic!("the install on an empty database: {error}");
        }
        let recorded: String = database
            .client()
            .query_one("SELECT family FROM horsies_client_family", &[])
            .await
            .expect("the install wrote one family row")
            .get(0);
        assert_eq!(recorded, family.as_str());
        broker.close().await;
        drop(broker);
        database.drop().await;
    }
}

/// The manifest of the batch profile with the metrics endpoint `endpoint`.
fn manifest_with_endpoint(endpoint: serde_json::Value) -> serde_json::Value {
    let mut document = manifest(json!({"batch": full_profile()}), named("batch"));
    document["metrics_endpoint"] = endpoint;
    document
}

#[test]
fn no_metrics_endpoint_plans_none() {
    assert_eq!(
        batch_plan().metrics_endpoint,
        MetricsEndpointPlan::NoMetricsEndpoint
    );
}

#[test]
fn a_listen_address_plans_the_endpoint_with_the_codes_of_each_task() {
    let document = manifest_with_endpoint(json!({
        "kind": "LISTEN_ADDRESS", "host": "::1", "port": 9100
    }));
    let manifest = parsed(&document);
    let plan = WorkerPlan::of_manifest(&manifest).expect("a plan");
    let MetricsEndpointPlan::ListenAddress {
        address,
        task_codes,
    } = plan.metrics_endpoint
    else {
        panic!("an address plans an endpoint");
    };
    assert_eq!(address.to_string(), "[::1]:9100");
    assert_eq!(
        task_codes,
        manifest
            .tasks
            .iter()
            .map(|(name, task)| (name.clone(), task.error_codes.clone()))
            .collect::<BTreeMap<String, DeclaredErrorCodes>>()
    );
    assert_eq!(task_codes["app.tasks.fetch"].iter().count(), 2);
}

/// With an endpoint, the worker of the plan builds the registry of its
/// tasks and queues; a queue named as the fallback of the `queue` label is
/// a config error that names the flag. Nothing connects.
#[cfg(feature = "metrics-endpoint")]
#[tokio::test]
async fn the_worker_of_an_endpoint_plan_builds_and_a_fallback_queue_name_is_refused() {
    let endpoint = json!({"kind": "LISTEN_ADDRESS", "host": "127.0.0.1", "port": 9100});
    let plan = WorkerPlan::of_manifest(&parsed(&manifest_with_endpoint(endpoint.clone())))
        .expect("a plan");
    let built = Worker::from_plan(
        plan,
        crate::test_support::child_specs::no_child_specs(),
        ClientFamily::Rust,
    )
    .await;
    assert!(built.is_ok(), "{:?}", built.err());

    let mut document = manifest_with_endpoint(endpoint);
    document["claim"]["queues"]["queues"][0]["name"] = json!("(unknown)");
    let plan = WorkerPlan::of_manifest(&parsed(&document)).expect("a plan");
    match Worker::from_plan(
        plan,
        crate::test_support::child_specs::no_child_specs(),
        ClientFamily::Rust,
    )
    .await
    {
        Err(WorkerError::Config(message)) => {
            assert!(
                message.starts_with("--metrics-listen 127.0.0.1:9100: "),
                "{message}"
            );
            assert!(message.contains("(unknown)"), "{message}");
        }
        Err(other) => panic!("expected a config error, got {other}"),
        Ok(_worker) => panic!("a queue named (unknown) is not a label value"),
    }
}

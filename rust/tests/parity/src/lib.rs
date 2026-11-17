//! The Rust side of the parity runs (`tests/parity/runner.py`).
//!
//! The app has the task names, input field names, codes, messages, options,
//! profiles and workflow of the Python parity app (`tests/parity/app.py`),
//! and [`SCENARIOS`] holds the scenarios of `tests/parity/scenarios.py`.
//! The runner sets [`DATABASE_URL_ENV`] for the worker, its children and the
//! send process.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::time::Duration;

use horsies::{
    blocking_task, task, AppConfig, ChildPreload, DrainTimeout, Horsies, HorsiesError,
    IsolatedChildCount, IsolatedChildren, IsolatedSetting, RetryPolicy, SharedAsyncChildCount,
    SharedAsyncChildren, SharedAsyncSeats, SharedAsyncSetting, SharedThreadChildCount,
    SharedThreadChildren, SharedThreadSeats, SharedThreadSetting, TaskError, TaskOutcome,
    TaskRuntime, TaskSendOptions, UserErrorCode, WorkerProfile, WorkflowDefConfig,
    WorkflowDefinition, WorkflowFunction, WorkflowSpecBuilder,
};

/// The database of this side of the run.
pub const DATABASE_URL_ENV: &str = "HORSIES_PARITY_DATABASE_URL";

pub const ISOLATED: &str = "isolated-1";
pub const SHARED_ASYNC: &str = "shared-async-1x4";
pub const SHARED_THREAD: &str = "shared-thread-1x4";

/// How long a task that must time out sleeps: far past its 1 s deadline.
const PAST_THE_DEADLINE: Duration = Duration::from_secs(30);

/// How long a stopping worker waits for its tasks.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

pub const CHAIN_NAME: &str = "parity_chain";
pub const CHAIN_DEFINITION_KEY: &str = "tests.parity.chain.v1";

/// The codes of the parity tasks' own errors.
#[derive(Debug, Clone, PartialEq, UserErrorCode)]
pub enum ParityCode {
    Refused,
    Transient,
    UpstreamFailed,
}

/// Two retries one second apart for the code `TRANSIENT`.
fn retry_policy() -> RetryPolicy {
    RetryPolicy::fixed(vec![1, 1], false).expect("a fixed policy of 1 s intervals is valid")
}

// ---- Isolated ----

/// A plain success.
#[task("parity.add")]
pub async fn add(a: i64, b: i64) -> Result<i64, TaskError<Infallible>> {
    Ok(a + b)
}

/// A task error with no retry.
#[task("parity.refuse")]
pub async fn refuse(value: i64) -> Result<i64, TaskError<ParityCode>> {
    Err(TaskError::user(
        ParityCode::Refused,
        format!("refused {value}"),
    ))
}

/// A task error that the policy retries until the retries run out.
#[task("parity.transient", retry_policy = retry_policy(), auto_retry_for = ["TRANSIENT"])]
pub async fn transient(value: i64) -> Result<i64, TaskError<ParityCode>> {
    Err(TaskError::user(
        ParityCode::Transient,
        format!("transient {value}"),
    ))
}

/// An isolated task that the parent kills at its deadline.
#[blocking_task("parity.sleep_past_deadline", timeout_ms = 1_000)]
pub fn sleep_past_deadline() -> Result<String, TaskError<Infallible>> {
    std::thread::sleep(PAST_THE_DEADLINE);
    Ok("late".to_owned())
}

/// Twice the value of its upstream node.
#[task("parity.double")]
pub async fn double(upstream: TaskOutcome<i64, Infallible>) -> Result<i64, TaskError<ParityCode>> {
    match upstream {
        TaskOutcome::Succeeded(value) => Ok(value * 2),
        TaskOutcome::Failed(_) => Err(TaskError::user(
            ParityCode::UpstreamFailed,
            "the upstream failed",
        )),
    }
}

// ---- Shared async ----

#[task("parity.add_async", execution = shared_async, timeout_ms = 30_000)]
pub async fn add_async(a: i64, b: i64) -> Result<i64, TaskError<Infallible>> {
    Ok(a + b)
}

#[task("parity.refuse_async", execution = shared_async, timeout_ms = 30_000)]
pub async fn refuse_async(value: i64) -> Result<i64, TaskError<ParityCode>> {
    Err(TaskError::user(
        ParityCode::Refused,
        format!("refused {value}"),
    ))
}

#[task(
    "parity.transient_async",
    execution = shared_async,
    timeout_ms = 30_000,
    retry_policy = retry_policy(),
    auto_retry_for = ["TRANSIENT"]
)]
pub async fn transient_async(value: i64) -> Result<i64, TaskError<ParityCode>> {
    Err(TaskError::user(
        ParityCode::Transient,
        format!("transient {value}"),
    ))
}

/// Ends at the cancel signal of its deadline, within the grace:
/// `cancel_end` is `RETURNED_AFTER_SIGNAL`, as the Python task whose sleep
/// the cancel ends.
#[task("parity.async_past_deadline", execution = shared_async, timeout_ms = 1_000)]
pub async fn async_past_deadline(rt: TaskRuntime) -> Result<String, TaskError<Infallible>> {
    tokio::select! {
        () = tokio::time::sleep(PAST_THE_DEADLINE) => Ok("late".to_owned()),
        () = rt.cancelled() => Ok("late".to_owned()),
    }
}

// ---- Shared thread ----

#[blocking_task("parity.add_thread", execution = shared_thread, timeout_ms = 30_000)]
pub fn add_thread(a: i64, b: i64) -> Result<i64, TaskError<Infallible>> {
    Ok(a + b)
}

#[blocking_task("parity.refuse_thread", execution = shared_thread, timeout_ms = 30_000)]
pub fn refuse_thread(value: i64) -> Result<i64, TaskError<ParityCode>> {
    Err(TaskError::user(
        ParityCode::Refused,
        format!("refused {value}"),
    ))
}

#[blocking_task(
    "parity.transient_thread",
    execution = shared_thread,
    timeout_ms = 30_000,
    retry_policy = retry_policy(),
    auto_retry_for = ["TRANSIENT"]
)]
pub fn transient_thread(value: i64) -> Result<i64, TaskError<ParityCode>> {
    Err(TaskError::user(
        ParityCode::Transient,
        format!("transient {value}"),
    ))
}

/// A thread seat that cannot stop at its deadline: the child is killed
/// when no run of it has time left.
#[blocking_task("parity.thread_past_deadline", execution = shared_thread, timeout_ms = 1_000)]
pub fn thread_past_deadline() -> Result<String, TaskError<Infallible>> {
    std::thread::sleep(PAST_THE_DEADLINE);
    Ok("late".to_owned())
}

// ---- Workflow ----

/// Two nodes: the second takes the result of the first (Python
/// `parity_chain`).
pub struct ParityChain;

impl WorkflowDefinition for ParityChain {
    type Output = i64;
    type Params = ();

    fn name() -> &'static str {
        CHAIN_NAME
    }

    fn definition_key() -> &'static str {
        CHAIN_DEFINITION_KEY
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let first = builder.task(
            add::node()?
                .node_id("add")
                .set(add::params::a(), 2)?
                .set(add::params::b(), 3)?,
        );
        let second = builder.task(
            double::node()?
                .node_id("double")
                .arg_from(double::params::upstream(), first),
        );
        Ok(WorkflowDefConfig::new().output(second))
    }
}

// ---- The app ----

/// The app of this side on `database_url`, with every parity task and the
/// workflow. No I/O.
///
/// Errors: the config refusal, or the first registration refusal.
pub fn build_app(database_url: &str) -> Result<(Horsies, WorkflowFunction<i64>), HorsiesError> {
    let mut config = AppConfig::for_database_url(database_url);
    config.broker.pool_size = 5;
    config.broker.max_overflow = 5;
    config.worker_profiles = profiles();
    let mut app = Horsies::new(config)?;
    let _add = add::register(&mut app)?;
    let _refuse = refuse::register(&mut app)?;
    let _transient = transient::register(&mut app)?;
    let _sleep_past_deadline = sleep_past_deadline::register(&mut app)?;
    let _double = double::register(&mut app)?;
    let _add_async = add_async::register(&mut app)?;
    let _refuse_async = refuse_async::register(&mut app)?;
    let _transient_async = transient_async::register(&mut app)?;
    let _async_past_deadline = async_past_deadline::register(&mut app)?;
    let _add_thread = add_thread::register(&mut app)?;
    let _refuse_thread = refuse_thread::register(&mut app)?;
    let _transient_thread = transient_thread::register(&mut app)?;
    let _thread_past_deadline = thread_past_deadline::register(&mut app)?;
    let chain = app.register_workflow_definition::<ParityChain>()?;
    Ok((app, chain))
}

fn profiles() -> BTreeMap<String, WorkerProfile> {
    let base = WorkerProfile {
        isolated: IsolatedSetting::NoIsolatedChildren {},
        shared_async: SharedAsyncSetting::NoSharedAsyncChildren {},
        shared_thread: SharedThreadSetting::NoSharedThreadChildren {},
        preload: ChildPreload::NoPreload,
        drain_timeout: DrainTimeout::try_from(DRAIN_TIMEOUT)
            .expect("5 s is above 0 and below 24 hours"),
        ..WorkerProfile::default()
    };
    BTreeMap::from([
        (
            ISOLATED.to_owned(),
            WorkerProfile {
                isolated: IsolatedSetting::IsolatedChildren(IsolatedChildren::new(
                    IsolatedChildCount::from_constant(1),
                )),
                ..base
            },
        ),
        (
            SHARED_ASYNC.to_owned(),
            WorkerProfile {
                shared_async: SharedAsyncSetting::SharedAsyncChildren(SharedAsyncChildren::new(
                    SharedAsyncChildCount::from_constant(1),
                    SharedAsyncSeats::from_constant(4),
                )),
                ..base
            },
        ),
        (
            SHARED_THREAD.to_owned(),
            WorkerProfile {
                shared_thread: SharedThreadSetting::SharedThreadChildren(
                    SharedThreadChildren::new(
                        SharedThreadChildCount::from_constant(1),
                        SharedThreadSeats::from_constant(4),
                    ),
                ),
                ..base
            },
        ),
    ])
}

// ---- The scenarios ----

/// One send: the task name, its input members, and its idempotency key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Send {
    pub task_name: &'static str,
    pub kwargs: &'static [(&'static str, i64)],
    pub idempotency_key: Option<&'static str>,
}

/// The profile of the worker, the sends in order, then the workflow starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scenario {
    pub name: &'static str,
    pub profile: &'static str,
    pub sends: &'static [Send],
    pub workflows: &'static [&'static str],
}

const fn send(task_name: &'static str, kwargs: &'static [(&'static str, i64)]) -> Send {
    Send {
        task_name,
        kwargs,
        idempotency_key: None,
    }
}

/// The scenarios of `tests/parity/scenarios.py`, in its order.
pub const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "isolated_outcomes",
        profile: ISOLATED,
        sends: &[
            send("parity.add", &[("a", 2), ("b", 3)]),
            send("parity.refuse", &[("value", 7)]),
            send("parity.transient", &[("value", 1)]),
        ],
        workflows: &[],
    },
    Scenario {
        name: "shared_async_outcomes",
        profile: SHARED_ASYNC,
        sends: &[
            send("parity.add_async", &[("a", 2), ("b", 3)]),
            send("parity.refuse_async", &[("value", 7)]),
            send("parity.transient_async", &[("value", 1)]),
        ],
        workflows: &[],
    },
    Scenario {
        name: "shared_thread_outcomes",
        profile: SHARED_THREAD,
        sends: &[
            send("parity.add_thread", &[("a", 2), ("b", 3)]),
            send("parity.refuse_thread", &[("value", 7)]),
            send("parity.transient_thread", &[("value", 1)]),
        ],
        workflows: &[],
    },
    Scenario {
        name: "isolated_timeout",
        profile: ISOLATED,
        sends: &[send("parity.sleep_past_deadline", &[])],
        workflows: &[],
    },
    Scenario {
        name: "shared_async_timeout_cancel_grace",
        profile: SHARED_ASYNC,
        sends: &[send("parity.async_past_deadline", &[])],
        workflows: &[],
    },
    Scenario {
        name: "shared_thread_timeout",
        profile: SHARED_THREAD,
        sends: &[send("parity.thread_past_deadline", &[])],
        workflows: &[],
    },
    Scenario {
        name: "idempotency_key",
        profile: ISOLATED,
        sends: &[
            Send {
                task_name: "parity.add",
                kwargs: &[("a", 1), ("b", 1)],
                idempotency_key: Some("parity-key"),
            },
            Send {
                task_name: "parity.add",
                kwargs: &[("a", 1), ("b", 1)],
                idempotency_key: Some("parity-key"),
            },
        ],
        workflows: &[],
    },
    Scenario {
        name: "workflow_chain",
        profile: ISOLATED,
        sends: &[],
        workflows: &[CHAIN_NAME],
    },
];

/// The scenario of `name`.
pub fn scenario_named(name: &str) -> Option<&'static Scenario> {
    SCENARIOS.iter().find(|scenario| scenario.name == name)
}

/// Why a send of a scenario failed; the text names the send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendRefused(pub String);

fn member(send: &Send, name: &str) -> Result<i64, SendRefused> {
    send.kwargs
        .iter()
        .find(|(key, _)| *key == name)
        .map(|(_, value)| *value)
        .ok_or_else(|| SendRefused(format!("{} has no input {name}", send.task_name)))
}

/// The id of the task of `send`, sent through the typed function of `app`.
async fn send_one(app: &Horsies, send: &Send) -> Result<String, SendRefused> {
    let runtime = app.task_runtime();
    let options = match send.idempotency_key {
        Some(key) => TaskSendOptions::new().idempotency_key(key),
        None => TaskSendOptions::new(),
    };
    let refused = |error: &dyn std::fmt::Display| {
        SendRefused(format!(
            "the send of {} was refused: {error}",
            send.task_name
        ))
    };
    let missing = |error: horsies::TaskFunctionNotFound| refused(&error);
    let task_id = match send.task_name {
        "parity.add" => {
            let input = add::Input {
                a: member(send, "a")?,
                b: member(send, "b")?,
            };
            add::handle(&runtime)
                .map_err(missing)?
                .send_with_options(options, input)
                .await
                .map(|h| h.task_id())
        }
        "parity.refuse" => {
            let input = refuse::Input {
                value: member(send, "value")?,
            };
            refuse::handle(&runtime)
                .map_err(missing)?
                .send_with_options(options, input)
                .await
                .map(|h| h.task_id())
        }
        "parity.transient" => {
            let input = transient::Input {
                value: member(send, "value")?,
            };
            transient::handle(&runtime)
                .map_err(missing)?
                .send_with_options(options, input)
                .await
                .map(|h| h.task_id())
        }
        "parity.sleep_past_deadline" => sleep_past_deadline::handle(&runtime)
            .map_err(missing)?
            .send_with_options(options, ())
            .await
            .map(|h| h.task_id()),
        "parity.add_async" => {
            let input = add_async::Input {
                a: member(send, "a")?,
                b: member(send, "b")?,
            };
            add_async::handle(&runtime)
                .map_err(missing)?
                .send_with_options(options, input)
                .await
                .map(|h| h.task_id())
        }
        "parity.refuse_async" => {
            let input = refuse_async::Input {
                value: member(send, "value")?,
            };
            refuse_async::handle(&runtime)
                .map_err(missing)?
                .send_with_options(options, input)
                .await
                .map(|h| h.task_id())
        }
        "parity.transient_async" => {
            let input = transient_async::Input {
                value: member(send, "value")?,
            };
            transient_async::handle(&runtime)
                .map_err(missing)?
                .send_with_options(options, input)
                .await
                .map(|h| h.task_id())
        }
        "parity.async_past_deadline" => async_past_deadline::handle(&runtime)
            .map_err(missing)?
            .send_with_options(options, ())
            .await
            .map(|h| h.task_id()),
        "parity.add_thread" => {
            let input = add_thread::Input {
                a: member(send, "a")?,
                b: member(send, "b")?,
            };
            add_thread::handle(&runtime)
                .map_err(missing)?
                .send_with_options(options, input)
                .await
                .map(|h| h.task_id())
        }
        "parity.refuse_thread" => {
            let input = refuse_thread::Input {
                value: member(send, "value")?,
            };
            refuse_thread::handle(&runtime)
                .map_err(missing)?
                .send_with_options(options, input)
                .await
                .map(|h| h.task_id())
        }
        "parity.transient_thread" => {
            let input = transient_thread::Input {
                value: member(send, "value")?,
            };
            transient_thread::handle(&runtime)
                .map_err(missing)?
                .send_with_options(options, input)
                .await
                .map(|h| h.task_id())
        }
        "parity.thread_past_deadline" => thread_past_deadline::handle(&runtime)
            .map_err(missing)?
            .send_with_options(options, ())
            .await
            .map(|h| h.task_id()),
        other => return Err(SendRefused(format!("the parity app has no task {other}"))),
    };
    task_id
        .map(|id| id.to_string())
        .map_err(|error| refused(&error))
}

/// Send the tasks of `scenario`, then start its workflows, in order. The
/// result is one JSON line per send and start, as `tests/parity/send.py`
/// prints them.
///
/// Errors: the first refused send or start.
pub async fn run_sends(
    app: &Horsies,
    chain: &WorkflowFunction<i64>,
    scenario: &Scenario,
) -> Result<Vec<String>, SendRefused> {
    let mut lines = Vec::with_capacity(scenario.sends.len() + scenario.workflows.len());
    for (index, send) in scenario.sends.iter().enumerate() {
        let task_id = send_one(app, send).await?;
        lines.push(serde_json::json!({"kind": "task", "index": index, "id": task_id}).to_string());
    }
    for (index, workflow) in scenario.workflows.iter().enumerate() {
        let workflow_id = match *workflow {
            CHAIN_NAME => chain
                .start()
                .await
                .map(|handle| handle.workflow_id())
                .map_err(|error| {
                    SendRefused(format!("the start of {workflow} was refused: {error}"))
                })?,
            other => {
                return Err(SendRefused(format!(
                    "the parity app has no workflow {other}"
                )))
            }
        };
        lines.push(
            serde_json::json!({"kind": "workflow", "index": index, "id": workflow_id.to_string()})
                .to_string(),
        );
    }
    Ok(lines)
}

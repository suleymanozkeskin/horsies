//! The Rust side of the parity runs (`crossclient/parity/runner.py`).
//!
//! The app has the task names, input field names, codes, messages, options,
//! profiles, schedule and workflows of the Python parity app
//! (`crossclient/parity/app.py`), and [`SCENARIOS`] holds the scenarios of
//! `crossclient/parity/scenarios.py`. The runner sets [`DATABASE_URL_ENV`] for the
//! worker, its children, the scheduler and the send process.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::time::Duration;

use horsies::rerun::{RerunEnqueuePolicy, RerunOutcome, RerunTask};
use horsies::{
    blocking_task, task, AppConfig, ChildPreload, Horsies, HorsiesError, IntervalSchedule,
    IsolatedChildren, IsolatedSetting, PingTarget, RetryPolicy, ScheduleConfig, SchedulePattern,
    SharedAsyncChildren, SharedAsyncSetting, SharedThreadChildren, SharedThreadSetting,
    SubWorkflowParams, TaskError, TaskErrorCode, TaskOutcome, TaskRuntime, TaskSchedule,
    TaskSendError, TaskSendOptions, UserErrorCode, WaitLimit, WorkerPong, WorkerProfile,
    WorkflowDefConfig, WorkflowDefinition, WorkflowFunction, WorkflowHandle, WorkflowSpecBuilder,
};
use uuid::Uuid;

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
pub const CHAIN_DEFINITION_KEY: &str = "crossclient.parity.chain.v1";
pub const CHILD_NAME: &str = "parity_child";
pub const CHILD_DEFINITION_KEY: &str = "crossclient.parity.child.v1";
pub const PARENT_NAME: &str = "parity_parent";
pub const PARENT_DEFINITION_KEY: &str = "crossclient.parity.parent.v1";

/// The retries of a send of the form [`SendForm::RetryOfAsyncContext`].
const SEND_RETRIES: usize = 2;

pub const SCHEDULE_NAME: &str = "parity_slots";
/// The task of each slot of the schedule.
pub const SLOT_TASK_NAME: &str = "parity.add";
/// The interval of the schedule, in hours: longer than any run, so a slot
/// is due only after the runner moves the schedule back.
const SCHEDULE_INTERVAL_HOURS: u32 = 1;
const SCHEDULE_MAX_CATCH_UP_RUNS: u32 = 2;
const SCHEDULE_CHECK_INTERVAL_S: u32 = 1;

/// The longest wait for the end of a rerun source.
const RESULT_WAIT: Duration = Duration::from_secs(30);

/// The window of one worker ping.
const PING_WINDOW: Duration = Duration::from_secs(2);

/// The pings of every worker before the send process gives up: a worker
/// that has just started may not listen yet.
const PING_ATTEMPTS: usize = 10;

/// The codes of the parity tasks' own errors.
#[derive(Debug, Clone, PartialEq, UserErrorCode)]
pub enum ParityCode {
    Refused,
    Transient,
    UpstreamFailed,
}

/// Two retries one second apart for the code `TRANSIENT`, with jitter, as
/// Python's `RetryPolicy.fixed` gives by default (`crossclient/parity/app.py`).
fn retry_policy() -> RetryPolicy {
    RetryPolicy::fixed(vec![1, 1], true).expect("a fixed policy of 1 s intervals is valid")
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
#[task("parity.transient", retry_policy = retry_policy(), retry_for = ["TRANSIENT"])]
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
    retry_for = ["TRANSIENT"]
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
    retry_for = ["TRANSIENT"]
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
            add::node()
                .node_id("add")
                .set(add::fields::a(), 2)?
                .set(add::fields::b(), 3)?,
        )?;
        let second = builder.task(
            double::node()
                .node_id("double")
                .arg_from(double::fields::upstream(), first),
        )?;
        Ok(WorkflowDefConfig::new().output(second))
    }
}

/// The child of `parity_parent`: one node, its output (Python
/// `ParityChild`).
pub struct ParityChild;

impl WorkflowDefinition for ParityChild {
    type Output = i64;
    type Params = ();

    fn name() -> &'static str {
        CHILD_NAME
    }

    fn definition_key() -> &'static str {
        CHILD_DEFINITION_KEY
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let added = builder.task(
            add::node()
                .node_id("add")
                .set(add::fields::a(), 2)?
                .set(add::fields::b(), 3)?,
        )?;
        Ok(WorkflowDefConfig::new().output(added))
    }
}

/// A sub-workflow node of [`ParityChild`], then a node that takes the
/// child's output (Python `parity_parent`).
pub struct ParityParent;

impl WorkflowDefinition for ParityParent {
    type Output = i64;
    type Params = ();

    fn name() -> &'static str {
        PARENT_NAME
    }

    fn definition_key() -> &'static str {
        PARENT_DEFINITION_KEY
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let child =
            builder.sub_workflow(ParityChild::node(SubWorkflowParams::NotGiven)?.node_id("child"));
        let doubled = builder.task(
            double::node()
                .node_id("double")
                .arg_from(double::fields::upstream(), child),
        )?;
        Ok(WorkflowDefConfig::new().output(doubled))
    }
}

// ---- The app ----

/// The app of this side and its started workflows.
pub struct ParityApp {
    pub app: Horsies,
    pub chain: WorkflowFunction<i64>,
    pub parent: WorkflowFunction<i64>,
}

/// The app of this side on `database_url`, with every parity task,
/// workflow and the schedule. Every send retains its input, so a failed
/// task can be rerun. No I/O.
///
/// Errors: the config refusal, or the first registration refusal.
pub fn build_app(database_url: &str) -> Result<ParityApp, HorsiesError> {
    let mut config = AppConfig::for_database_url(database_url);
    config.broker.pool_size = 5;
    config.broker.max_overflow = 5;
    config.broker.retain_rerun_input_default = true;
    config.worker_profiles = profiles();
    config.schedule = Some(schedules());
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
    let _child = app.register_workflow_definition::<ParityChild>()?;
    let parent = app.register_workflow_definition::<ParityParent>()?;
    Ok(ParityApp { app, chain, parent })
}

/// The schedule `parity_slots` (Python `app.py`): `parity.add` (2 + 3)
/// every hour, missed runs caught up, at most two slots per fire. Only the
/// scheduler process of the `schedule_slots` scenario runs it.
fn schedules() -> ScheduleConfig {
    let every_hour = SchedulePattern::Interval(IntervalSchedule {
        hours: Some(SCHEDULE_INTERVAL_HOURS),
        ..IntervalSchedule::default()
    });
    ScheduleConfig::new(vec![TaskSchedule::new(
        SCHEDULE_NAME,
        SLOT_TASK_NAME,
        every_hour,
    )
    .input(&serde_json::json!({"a": 2, "b": 3}))
    .expect("an object of two integers writes as JSON")
    .timezone("UTC")
    .catch_up_missed(true)
    .max_catch_up_runs(SCHEDULE_MAX_CATCH_UP_RUNS)])
    .check_interval_seconds(SCHEDULE_CHECK_INTERVAL_S)
}

fn profiles() -> BTreeMap<String, WorkerProfile> {
    let base = WorkerProfile {
        isolated: IsolatedSetting::NoIsolatedChildren {},
        shared_async: SharedAsyncSetting::NoSharedAsyncChildren {},
        shared_thread: SharedThreadSetting::NoSharedThreadChildren {},
        preload: ChildPreload::NoPreload,
        drain_timeout: DRAIN_TIMEOUT,
        ..WorkerProfile::default()
    };
    BTreeMap::from([
        (
            ISOLATED.to_owned(),
            WorkerProfile {
                isolated: IsolatedChildren::new(1).into(),
                ..base
            },
        ),
        (
            SHARED_ASYNC.to_owned(),
            WorkerProfile {
                shared_async: SharedAsyncChildren::new(1, 4).into(),
                ..base
            },
        ),
        (
            SHARED_THREAD.to_owned(),
            WorkerProfile {
                shared_thread: SharedThreadChildren::new(1, 4).into(),
                ..base
            },
        ),
    ])
}

// ---- The scenarios ----

/// How a send is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendForm {
    /// One send.
    Direct,
    /// A blocking send on a thread of the running tokio runtime, refused
    /// with `ASYNC_CONTEXT` before any statement; then two `retry_send`
    /// calls with its error, which write one task under the prepared id.
    RetryOfAsyncContext,
}

/// One send: the task name, its input members, its idempotency key and its
/// form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Send {
    pub task_name: &'static str,
    pub kwargs: &'static [(&'static str, i64)],
    pub idempotency_key: Option<&'static str>,
    pub form: SendForm,
}

/// What an action does to a started workflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionKind {
    Pause,
    Resume,
    Cancel,
}

/// An action on the started workflow at `index`. A pause and a resume must
/// answer true.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkflowAction {
    pub kind: ActionKind,
    pub index: usize,
}

/// The rerun of the task of task line `source`: a send, or an earlier
/// rerun. The send process waits for the end of that task, then reruns it
/// with [`RerunEnqueuePolicy::standard`] and the input retained. The new
/// task is a task line after the lines of the sends, in the order of the
/// reruns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rerun {
    pub source: usize,
}

/// Whether the send process pings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Health {
    NotPinged,
    /// After the actions: `ping_database`; `ping_workers` until each
    /// worker of the scenario answered, at most [`PING_ATTEMPTS`] pings of
    /// [`PING_WINDOW`]; then a ping of the first worker that answered, by
    /// its id. The send process prints one ping line with the counts of the
    /// answers.
    Pinged,
}

/// When the workers of a scenario start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStart {
    BeforeSends,
    /// The steps run on a database with no worker: no claim can race a
    /// pause, a resume or a cancel.
    AfterActions,
}

/// One worker per profile, the sends in order, the reruns in order, the
/// workflow starts in order, the actions in order, then the pings. The
/// runner of `crossclient/parity` reads the profiles, the worker start and the
/// scheduler of the Python copy; the send process reads the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scenario {
    pub name: &'static str,
    pub profiles: &'static [&'static str],
    pub sends: &'static [Send],
    pub workflows: &'static [&'static str],
    pub actions: &'static [WorkflowAction],
    pub worker_start: WorkerStart,
    pub reruns: &'static [Rerun],
    pub health: Health,
}

const fn send(task_name: &'static str, kwargs: &'static [(&'static str, i64)]) -> Send {
    Send {
        task_name,
        kwargs,
        idempotency_key: None,
        form: SendForm::Direct,
    }
}

/// The scenarios of `crossclient/parity/scenarios.py`, in its order.
pub const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "isolated_outcomes",
        profiles: &[ISOLATED],
        sends: &[
            send("parity.add", &[("a", 2), ("b", 3)]),
            send("parity.refuse", &[("value", 7)]),
            send("parity.transient", &[("value", 1)]),
        ],
        workflows: &[],
        actions: &[],
        worker_start: WorkerStart::BeforeSends,
        reruns: &[],
        health: Health::NotPinged,
    },
    Scenario {
        name: "shared_async_outcomes",
        profiles: &[SHARED_ASYNC],
        sends: &[
            send("parity.add_async", &[("a", 2), ("b", 3)]),
            send("parity.refuse_async", &[("value", 7)]),
            send("parity.transient_async", &[("value", 1)]),
        ],
        workflows: &[],
        actions: &[],
        worker_start: WorkerStart::BeforeSends,
        reruns: &[],
        health: Health::NotPinged,
    },
    Scenario {
        name: "shared_thread_outcomes",
        profiles: &[SHARED_THREAD],
        sends: &[
            send("parity.add_thread", &[("a", 2), ("b", 3)]),
            send("parity.refuse_thread", &[("value", 7)]),
            send("parity.transient_thread", &[("value", 1)]),
        ],
        workflows: &[],
        actions: &[],
        worker_start: WorkerStart::BeforeSends,
        reruns: &[],
        health: Health::NotPinged,
    },
    Scenario {
        name: "isolated_timeout",
        profiles: &[ISOLATED],
        sends: &[send("parity.sleep_past_deadline", &[])],
        workflows: &[],
        actions: &[],
        worker_start: WorkerStart::BeforeSends,
        reruns: &[],
        health: Health::NotPinged,
    },
    Scenario {
        name: "shared_async_timeout_cancel_grace",
        profiles: &[SHARED_ASYNC],
        sends: &[send("parity.async_past_deadline", &[])],
        workflows: &[],
        actions: &[],
        worker_start: WorkerStart::BeforeSends,
        reruns: &[],
        health: Health::NotPinged,
    },
    Scenario {
        name: "shared_thread_timeout",
        profiles: &[SHARED_THREAD],
        sends: &[send("parity.thread_past_deadline", &[])],
        workflows: &[],
        actions: &[],
        worker_start: WorkerStart::BeforeSends,
        reruns: &[],
        health: Health::NotPinged,
    },
    Scenario {
        name: "idempotency_key",
        profiles: &[ISOLATED],
        sends: &[
            Send {
                task_name: "parity.add",
                kwargs: &[("a", 1), ("b", 1)],
                idempotency_key: Some("parity-key"),
                form: SendForm::Direct,
            },
            Send {
                task_name: "parity.add",
                kwargs: &[("a", 1), ("b", 1)],
                idempotency_key: Some("parity-key"),
                form: SendForm::Direct,
            },
        ],
        workflows: &[],
        actions: &[],
        worker_start: WorkerStart::BeforeSends,
        reruns: &[],
        health: Health::NotPinged,
    },
    Scenario {
        name: "workflow_chain",
        profiles: &[ISOLATED],
        sends: &[],
        workflows: &[CHAIN_NAME],
        actions: &[],
        worker_start: WorkerStart::BeforeSends,
        reruns: &[],
        health: Health::NotPinged,
    },
    // Layer 2: two workers, and each task has one possible worker. A worker
    // with isolated children also serves a shared class that it has no
    // children for, through its ISOLATED budget; a worker with no isolated
    // children serves only the classes of its own children (the core's
    // `ClaimPlan::route`). So neither worker here serves the class of the
    // other.
    Scenario {
        name: "several_workers",
        profiles: &[SHARED_ASYNC, SHARED_THREAD],
        sends: &[
            send("parity.add_async", &[("a", 2), ("b", 3)]),
            send("parity.add_thread", &[("a", 2), ("b", 3)]),
        ],
        workflows: &[],
        actions: &[],
        worker_start: WorkerStart::BeforeSends,
        reruns: &[],
        health: Health::NotPinged,
    },
    Scenario {
        name: "send_retry",
        profiles: &[ISOLATED],
        sends: &[Send {
            task_name: "parity.add",
            kwargs: &[("a", 4), ("b", 5)],
            idempotency_key: None,
            form: SendForm::RetryOfAsyncContext,
        }],
        workflows: &[],
        actions: &[],
        worker_start: WorkerStart::BeforeSends,
        reruns: &[],
        health: Health::NotPinged,
    },
    // Layer 6: the first chain is paused and resumed, the second cancelled,
    // before any worker runs.
    Scenario {
        name: "workflow_pause_resume_cancel",
        profiles: &[ISOLATED],
        sends: &[],
        workflows: &[CHAIN_NAME, CHAIN_NAME],
        actions: &[
            WorkflowAction {
                kind: ActionKind::Pause,
                index: 0,
            },
            WorkflowAction {
                kind: ActionKind::Resume,
                index: 0,
            },
            WorkflowAction {
                kind: ActionKind::Cancel,
                index: 1,
            },
        ],
        worker_start: WorkerStart::AfterActions,
        reruns: &[],
        health: Health::NotPinged,
    },
    // Layer 7: a child workflow whose output a later node of the parent
    // takes.
    Scenario {
        name: "sub_workflow",
        profiles: &[ISOLATED],
        sends: &[],
        workflows: &[PARENT_NAME],
        actions: &[],
        worker_start: WorkerStart::BeforeSends,
        reruns: &[],
        health: Health::NotPinged,
    },
    // Layer 3: the scheduler process and the move of the schedule are steps
    // of the runner; the send process sends nothing.
    Scenario {
        name: "schedule_slots",
        profiles: &[ISOLATED],
        sends: &[],
        workflows: &[],
        actions: &[],
        worker_start: WorkerStart::BeforeSends,
        reruns: &[],
        health: Health::NotPinged,
    },
    // Layer 8: the database and the two workers answer.
    Scenario {
        name: "worker_health",
        profiles: &[ISOLATED, SHARED_ASYNC],
        sends: &[],
        workflows: &[],
        actions: &[],
        worker_start: WorkerStart::BeforeSends,
        reruns: &[],
        health: Health::Pinged,
    },
    // A failed task, then its rerun: a new task with the source as its
    // rerun parent and root.
    Scenario {
        name: "rerun",
        profiles: &[ISOLATED],
        sends: &[send("parity.refuse", &[("value", 7)])],
        workflows: &[],
        actions: &[],
        worker_start: WorkerStart::BeforeSends,
        reruns: &[Rerun { source: 0 }],
        health: Health::NotPinged,
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

/// The id of the task that two retries of a refused blocking send wrote.
/// Only `parity.add` has this form.
async fn send_retry(app: &Horsies, send: &Send) -> Result<Uuid, SendRefused> {
    let runtime = app.task_runtime();
    let function = match send.task_name {
        "parity.add" => {
            add::function(&runtime).map_err(|missing| SendRefused(missing.to_string()))?
        }
        other => return Err(SendRefused(format!("the retry form has no task {other}"))),
    };
    let input = add::Input {
        a: member(send, "a")?,
        b: member(send, "b")?,
    };
    // The send process runs its sends inside a tokio runtime: the blocking
    // view refuses before any statement and keeps the prepared payload.
    let refused = match function.blocking().send(input) {
        Err(error @ TaskSendError::AsyncContext { .. }) => error,
        Err(other) => {
            return Err(SendRefused(format!(
                "the blocking send of {} answered {other}",
                send.task_name
            )))
        }
        Ok(handle) => {
            return Err(SendRefused(format!(
                "the blocking send of {} wrote task {}",
                send.task_name,
                handle.task_id()
            )))
        }
    };
    let mut ids = Vec::with_capacity(SEND_RETRIES);
    for _attempt in 0..SEND_RETRIES {
        let handle = function.retry_send(&refused).await.map_err(|error| {
            SendRefused(format!(
                "the retry of {} was refused: {error}",
                send.task_name
            ))
        })?;
        ids.push(handle.task_id());
    }
    match ids.as_slice() {
        [first, second] if first == second => Ok(*first),
        _ => Err(SendRefused(format!(
            "the retries of {} wrote other tasks: {ids:?}",
            send.task_name
        ))),
    }
}

/// The id of the task of `send`, sent through the typed function of `app`.
async fn send_one(app: &Horsies, send: &Send) -> Result<Uuid, SendRefused> {
    match send.form {
        SendForm::Direct => {}
        SendForm::RetryOfAsyncContext => return send_retry(app, send).await,
    }
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
            add::function(&runtime)
                .map_err(missing)?
                .send_with_options(options, input)
                .await
                .map(|h| h.task_id())
        }
        "parity.refuse" => {
            let input = refuse::Input {
                value: member(send, "value")?,
            };
            refuse::function(&runtime)
                .map_err(missing)?
                .send_with_options(options, input)
                .await
                .map(|h| h.task_id())
        }
        "parity.transient" => {
            let input = transient::Input {
                value: member(send, "value")?,
            };
            transient::function(&runtime)
                .map_err(missing)?
                .send_with_options(options, input)
                .await
                .map(|h| h.task_id())
        }
        "parity.sleep_past_deadline" => sleep_past_deadline::function(&runtime)
            .map_err(missing)?
            .send_with_options(options, ())
            .await
            .map(|h| h.task_id()),
        "parity.add_async" => {
            let input = add_async::Input {
                a: member(send, "a")?,
                b: member(send, "b")?,
            };
            add_async::function(&runtime)
                .map_err(missing)?
                .send_with_options(options, input)
                .await
                .map(|h| h.task_id())
        }
        "parity.refuse_async" => {
            let input = refuse_async::Input {
                value: member(send, "value")?,
            };
            refuse_async::function(&runtime)
                .map_err(missing)?
                .send_with_options(options, input)
                .await
                .map(|h| h.task_id())
        }
        "parity.transient_async" => {
            let input = transient_async::Input {
                value: member(send, "value")?,
            };
            transient_async::function(&runtime)
                .map_err(missing)?
                .send_with_options(options, input)
                .await
                .map(|h| h.task_id())
        }
        "parity.async_past_deadline" => async_past_deadline::function(&runtime)
            .map_err(missing)?
            .send_with_options(options, ())
            .await
            .map(|h| h.task_id()),
        "parity.add_thread" => {
            let input = add_thread::Input {
                a: member(send, "a")?,
                b: member(send, "b")?,
            };
            add_thread::function(&runtime)
                .map_err(missing)?
                .send_with_options(options, input)
                .await
                .map(|h| h.task_id())
        }
        "parity.refuse_thread" => {
            let input = refuse_thread::Input {
                value: member(send, "value")?,
            };
            refuse_thread::function(&runtime)
                .map_err(missing)?
                .send_with_options(options, input)
                .await
                .map(|h| h.task_id())
        }
        "parity.transient_thread" => {
            let input = transient_thread::Input {
                value: member(send, "value")?,
            };
            transient_thread::function(&runtime)
                .map_err(missing)?
                .send_with_options(options, input)
                .await
                .map(|h| h.task_id())
        }
        "parity.thread_past_deadline" => thread_past_deadline::function(&runtime)
            .map_err(missing)?
            .send_with_options(options, ())
            .await
            .map(|h| h.task_id()),
        other => return Err(SendRefused(format!("the parity app has no task {other}"))),
    };
    task_id.map_err(|error| refused(&error))
}

/// The id of the new task of `rerun`, after the end of its source.
async fn rerun_one(app: &Horsies, rerun: &Rerun, sent: &[Uuid]) -> Result<Uuid, SendRefused> {
    let source = *sent
        .get(rerun.source)
        .ok_or_else(|| SendRefused(format!("{rerun:?} names no earlier task line")))?;
    let limit = WaitLimit::Within(RESULT_WAIT);
    match app.get_result::<i64, ParityCode>(source, limit).await {
        Ok(TaskOutcome::Failed(error))
            if *error.code() == TaskErrorCode::User(ParityCode::Refused) => {}
        other => {
            return Err(SendRefused(format!(
                "the rerun source {source} did not end with REFUSED: {other:?}"
            )))
        }
    }
    let command = RerunTask::new(source, None, None);
    match app
        .rerun_task(command, RerunEnqueuePolicy::standard(true))
        .await
    {
        Ok(RerunOutcome::Enqueued { new_task_id, .. }) => Ok(new_task_id),
        Ok(other) => Err(SendRefused(format!(
            "the rerun of {source} answered {other:?}"
        ))),
        Err(error) => Err(SendRefused(format!(
            "the rerun of {source} was refused: {error}"
        ))),
    }
}

/// The answers to a ping of every worker, once `expected` workers answered
/// one ping.
async fn every_worker(app: &Horsies, expected: usize) -> Result<Vec<WorkerPong>, SendRefused> {
    let mut answered: Vec<WorkerPong> = Vec::new();
    for _attempt in 0..PING_ATTEMPTS {
        answered = app
            .ping_workers(
                PingTarget::EveryWorker,
                PING_WINDOW,
                NonZeroUsize::new(expected),
            )
            .await
            .map_err(|refused| SendRefused(format!("ping_workers was refused: {refused}")))?;
        if answered.len() >= expected {
            return Ok(answered);
        }
    }
    Err(SendRefused(format!(
        "{} of {expected} workers answered the last of {PING_ATTEMPTS} pings",
        answered.len()
    )))
}

/// The ping line: the database answers, then each of the `expected`
/// workers, then the first of them by its id.
async fn ping(app: &Horsies, expected: usize) -> Result<String, SendRefused> {
    let _latency = app
        .ping_database()
        .await
        .map_err(|refused| SendRefused(format!("ping_database was refused: {refused}")))?;
    let answered = every_worker(app, expected).await?;
    let first = answered
        .first()
        .ok_or_else(|| SendRefused("no worker answered".to_owned()))?;
    let targeted = app
        .ping_workers(
            PingTarget::Worker(first.worker_id.clone()),
            PING_WINDOW,
            None,
        )
        .await
        .map_err(|refused| {
            SendRefused(format!(
                "the ping of worker {} was refused: {refused}",
                first.worker_id
            ))
        })?;
    Ok(serde_json::json!({
        "kind": "ping",
        "index": 0,
        "workers": answered.len(),
        "targeted": targeted.len(),
    })
    .to_string())
}

/// Run the action `action` on its started workflow. A pause and a resume
/// must answer true.
async fn act(action: &WorkflowAction, handles: &[WorkflowHandle<i64>]) -> Result<(), SendRefused> {
    let handle = handles
        .get(action.index)
        .ok_or_else(|| SendRefused(format!("{action:?} names no started workflow")))?;
    let answered = match action.kind {
        ActionKind::Pause => handle.pause().await,
        ActionKind::Resume => handle.resume().await,
        ActionKind::Cancel => {
            return handle
                .cancel()
                .await
                .map_err(|refused| SendRefused(format!("{action:?} was refused: {refused:?}")))
        }
    };
    match answered {
        Ok(true) => Ok(()),
        Ok(false) => Err(SendRefused(format!("{action:?} answered false"))),
        Err(refused) => Err(SendRefused(format!("{action:?} was refused: {refused:?}"))),
    }
}

/// Send the tasks of `scenario`, rerun the tasks of its reruns, start its
/// workflows, run its actions on the started workflows, then ping, in
/// order. The result is one JSON line per send, rerun, start and ping, as
/// `crossclient/parity/send.py` prints them.
///
/// Errors: the first refused send, rerun, start, action or ping.
pub async fn run_sends(
    parity: &ParityApp,
    scenario: &Scenario,
) -> Result<Vec<String>, SendRefused> {
    let mut sent: Vec<Uuid> = Vec::with_capacity(scenario.sends.len() + scenario.reruns.len());
    for send in scenario.sends {
        sent.push(send_one(&parity.app, send).await?);
    }
    for rerun in scenario.reruns {
        let new_task_id = rerun_one(&parity.app, rerun, &sent).await?;
        sent.push(new_task_id);
    }
    let mut lines = Vec::with_capacity(sent.len() + scenario.workflows.len() + 1);
    for (index, task_id) in sent.iter().enumerate() {
        lines.push(
            serde_json::json!({"kind": "task", "index": index, "id": task_id.to_string()})
                .to_string(),
        );
    }
    let mut handles: Vec<WorkflowHandle<i64>> = Vec::with_capacity(scenario.workflows.len());
    for (index, workflow) in scenario.workflows.iter().enumerate() {
        let function = match *workflow {
            CHAIN_NAME => &parity.chain,
            PARENT_NAME => &parity.parent,
            other => {
                return Err(SendRefused(format!(
                    "the parity app has no workflow {other}"
                )))
            }
        };
        let handle = function.start().await.map_err(|error| {
            SendRefused(format!("the start of {workflow} was refused: {error}"))
        })?;
        lines.push(
            serde_json::json!({"kind": "workflow", "index": index, "id": handle.workflow_id().to_string()})
                .to_string(),
        );
        handles.push(handle);
    }
    for action in scenario.actions {
        act(action, &handles).await?;
    }
    match scenario.health {
        Health::Pinged => lines.push(ping(&parity.app, scenario.profiles.len()).await?),
        Health::NotPinged => {}
    }
    Ok(lines)
}

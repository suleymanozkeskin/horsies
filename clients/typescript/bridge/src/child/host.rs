//! The child host entry: `childArguments` and `serveChild`.
//!
//! - `childArguments(arguments)`: pure. The core's read of the child-mode
//!   arguments (`child_mode`), the program name first, so the package can
//!   print the cause of bad arguments and end the process with 73 before it
//!   builds anything.
//! - `serveChild(plan, post, hook)`: the child app of the plan on the core's
//!   child host, until the stop frame or the end of the parent. On the
//!   JavaScript thread it reads the arguments, registers the post dispatcher
//!   and the hook dispatcher, builds the task registry (one body form and one
//!   context use per task), the start hooks, the producer manifest and the
//!   loop, and starts the host thread (`exit`), which keeps a duplicate of
//!   the child's socket until the process ends. It resolves with the exit
//!   code when the host returns; the host thread's backstop waits for the
//!   plan's flush bound.
//! - The child app: family `TYPESCRIPT`; the bridge's log setup at the
//!   level of the init frame; the body scope of the plan's body facts; the
//!   handler child of the handler slot; the loop of the JavaScript main
//!   thread (`EventLoopWatch::Host`); the immediate exit with the output
//!   drain only, so a child at the end of its parent keeps the 1 s exit rule
//!   (the core's other exits wait for no host drain either).
//! - Nothing starts before the call: the pure functions of the bridge never
//!   shape the process runtime, so the child host shapes it for its kind.
//! - One serve per process: a second one is `CLIENT_DEFECT`.

use std::ffi::OsString;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use horsies_core::core::task::fn_trait::{RegisteredTask, TaskMeta};
use horsies_core::core::TaskRegistry;
use horsies_core::manifest::parse_producer_manifest;
use horsies_core::{
    child_mode, run_child_host, ChildApp, ChildArgs, ChildHostExit, ChildMode, ClientFamily,
    EventLoopWatch, HostLoop,
};
use napi::bindgen_prelude::{BigInt, FnArgs, Function, Object};
use napi::{Env, JsDeferred};
use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::operation::{guarded_start, resolve, EnvelopeResolver};
use crate::call::thrown::{Envelope, NativeThrown};
use crate::child::body::{ChildBodies, ChildPost, ChildPoster, JsAsyncBody, JsBlockingBody};
use crate::child::body_facts::{BodyFacts, BodyFactsMode};
use crate::child::exit::{child_process_exit, hold_socket, spawn_host_thread, FlushBound};
use crate::child::handler_slot::take_handler_child;
use crate::child::loop_watch::make_host_loop;
use crate::child::start_hooks::{start_hooks, HookDispatcher, StartHookCall};
use crate::logging::bridge::child_log_setup;

/// The program name in front of the arguments that the core reads: a
/// Node-API library has no argument list of its own.
const PROGRAM_PLACEHOLDER: &str = "horsies-child";

/// The subject of the serve in its refusals.
const SERVE_SUBJECT: &str = "serve_child";

/// Whether a serve ran in this process.
static SERVED: AtomicBool = AtomicBool::new(false);

/// How the child host runs the body of a task.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BodyForm {
    /// An `ISOLATED` or a `SHARED_ASYNC` task: the core's async form.
    Async,
    /// A `SHARED_THREAD` task: the core's blocking form.
    Blocking,
}

/// Whether the child host writes the workflow context into the input of a
/// task (`build_task_input`, `accepts_workflow_context`).
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskContextUse {
    /// A task with `workflowContext: true`: the core writes the
    /// `workflow_context` member for a node that names `workflowContextFrom`.
    TakesContext,
    /// Any other task: the core removes the member.
    NoContext,
}

/// One task of the child, by its name.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildTaskEntry {
    pub name: String,
    pub form: BodyForm,
    pub context: TaskContextUse,
}

/// What the package gives the child host.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildHostPlan {
    /// The implementations, each by its task name.
    pub tasks: Vec<ChildTaskEntry>,
    /// The name of each start hook in run order: the providers, then the
    /// `onChildStart` hooks. Hook `index` of the dispatcher is the name at
    /// `index`.
    pub start_hooks: Vec<String>,
    /// The app's producer manifest, as the config check of the core wrote
    /// it.
    pub producer_manifest_json: String,
    /// The host name written with each start.
    pub hostname: String,
    pub body_facts: BodyFactsMode,
    /// The package's longest wait for its telemetry flush before it ends
    /// the process, in milliseconds: at most 8,000. The backstop of the host
    /// thread comes 1 s after it.
    pub exit_flush_bound_ms: u32,
    /// The process arguments after the entry script (`process.argv`
    /// from index 2): the child flag, the socket descriptor, the worker pid.
    pub process_arguments: Vec<String>,
}

/// The read of the child-mode arguments.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChildArgumentsCheck {
    /// The arguments start a child.
    Child,
    /// The arguments cannot be used: the child prints `cause` and ends with
    /// `code` (73).
    BadArguments { cause: String, code: i32 },
}

/// How the child host ended, as the core names it.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChildExitName {
    Stopped,
    ParentGone,
    BadParentFrame,
    BadArguments,
    BootFailed,
    ProtocolMismatch,
    ChildHookFailed,
}

impl ChildExitName {
    /// The name of the core's exit. Pure.
    fn of(exit: ChildHostExit) -> Self {
        match exit {
            ChildHostExit::Stopped => Self::Stopped,
            ChildHostExit::ParentGone => Self::ParentGone,
            ChildHostExit::BadParentFrame => Self::BadParentFrame,
            ChildHostExit::BadArguments => Self::BadArguments,
            ChildHostExit::BootFailed => Self::BootFailed,
            ChildHostExit::ProtocolMismatch => Self::ProtocolMismatch,
            ChildHostExit::ChildHookFailed => Self::ChildHookFailed,
        }
    }
}

/// The end of the serve: the exit code of the process and its name.
#[napi(object, object_from_js = false)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChildServeEnd {
    pub code: i32,
    pub exit: ChildExitName,
}

/// The core's read of child-mode arguments.
#[derive(Debug)]
enum ArgumentsRead {
    Child(ChildArgs),
    Bad { cause: String },
}

/// The core's read of `arguments` (the process arguments after the entry
/// script). Not child mode is a defect of the package, which calls this
/// only when the first argument is the child flag.
fn read_arguments(arguments: &[String]) -> Result<ArgumentsRead, NativeThrown> {
    let program_first = std::iter::once(OsString::from(PROGRAM_PLACEHOLDER))
        .chain(arguments.iter().map(OsString::from));
    match child_mode(program_first) {
        ChildMode::Child(args) => Ok(ArgumentsRead::Child(args)),
        ChildMode::BadArguments(error) => Ok(ArgumentsRead::Bad {
            cause: error.to_string(),
        }),
        ChildMode::Worker => Err(NativeThrown::ClientDefect {
            subject: "arguments".to_owned(),
            rule: "child mode starts with the child flag".to_owned(),
        }),
    }
}

/// Whether `processArguments` (the process arguments after the entry
/// script) start a child. Pure: starts nothing.
#[napi(ts_return_type = "NativeEnvelope<ChildArgumentsCheck>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn child_arguments(process_arguments: Vec<String>) -> Envelope<ChildArgumentsCheck> {
    sync_call("child_arguments", || {
        match read_arguments(&process_arguments) {
            Ok(ArgumentsRead::Child(_args)) => Envelope::Value(ChildArgumentsCheck::Child),
            Ok(ArgumentsRead::Bad { cause }) => {
                Envelope::Value(ChildArgumentsCheck::BadArguments {
                    cause,
                    code: ChildHostExit::BadArguments.exit_code(),
                })
            }
            Err(thrown) => Envelope::Thrown(thrown),
        }
    })
}

/// Serve as a task or handler child with `plan` until the stop frame or the
/// end of the parent; resolve with the exit code. `post` runs the bodies,
/// the cancels and the stop requests of spawned work; `hook` runs the start
/// hook of an index. Bad arguments, a manifest that the core refuses, a
/// repeated task name and a second serve resolve at once with their thrown
/// code; nothing starts then.
#[napi(
    ts_args_type = "plan: ChildHostPlan, post: (post: ChildPost) => void, hook: (callId: bigint, call: StartHookCall) => void",
    ts_return_type = "Promise<NativeEnvelope<ChildServeEnd>>"
)]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn serve_child<'env>(
    env: &'env Env,
    plan: ChildHostPlan,
    post: Function<ChildPost, ()>,
    hook: Function<FnArgs<(BigInt, StartHookCall)>, ()>,
) -> napi::Result<Object<'env>> {
    let (deferred, promise) =
        env.create_deferred::<Envelope<ChildServeEnd>, EnvelopeResolver<ChildServeEnd>>()?;
    match guarded_start(SERVE_SUBJECT, || served_app(plan, &post, &hook)) {
        Err(thrown) => resolve(deferred, Envelope::Thrown(thrown)),
        Ok(served) => start_host(deferred, served),
    }
    Ok(promise)
}

/// The deferred of the serve, taken once: by the host thread at the exit,
/// or by the start when the OS refuses the thread.
type ServeDeferred = JsDeferred<Envelope<ChildServeEnd>, EnvelopeResolver<ChildServeEnd>>;

/// Resolve the deferred in `slot` with `envelope`, if no one took it.
fn resolve_once(slot: &Mutex<Option<ServeDeferred>>, envelope: Envelope<ChildServeEnd>) {
    let taken = slot.lock().unwrap_or_else(PoisonError::into_inner).take();
    if let Some(deferred) = taken {
        resolve(deferred, envelope);
    }
}

/// Start the host thread with a duplicate of the child's socket; a socket
/// that the OS refuses to duplicate or a thread that the OS refuses resolves
/// with `RUNTIME_START_FAILED`, and nothing runs.
fn start_host(deferred: ServeDeferred, served: ServedApp) {
    let ServedApp {
        app,
        args,
        flush_bound,
    } = served;
    let slot = Arc::new(Mutex::new(Some(deferred)));
    let socket = match hold_socket(args.socket_fd()) {
        Ok(socket) => socket,
        Err(error) => {
            resolve_once(
                &slot,
                Envelope::Thrown(NativeThrown::RuntimeStartFailed {
                    cause: format!("the child socket could not be kept open: {error}"),
                }),
            );
            return;
        }
    };
    let thread_slot = Arc::clone(&slot);
    let started = spawn_host_thread(
        move || run_child_host(app, args),
        move |exit| resolve_once(&thread_slot, Envelope::Value(serve_end(exit))),
        flush_bound,
        socket,
    );
    if let Err(error) = started {
        resolve_once(
            &slot,
            Envelope::Thrown(NativeThrown::RuntimeStartFailed {
                cause: format!("the child host thread did not start: {error}"),
            }),
        );
    }
}

/// The end record of the host's exit. Pure.
fn serve_end(exit: ChildHostExit) -> ChildServeEnd {
    ChildServeEnd {
        code: exit.exit_code(),
        exit: ChildExitName::of(exit),
    }
}

/// What the serve starts: the child app, its arguments, and the flush bound
/// of the package.
struct ServedApp {
    app: ChildApp,
    args: ChildArgs,
    flush_bound: FlushBound,
}

/// The child app of `plan`, its arguments and its flush bound, on the
/// JavaScript thread.
fn served_app(
    plan: ChildHostPlan,
    post: &Function<ChildPost, ()>,
    hook: &Function<FnArgs<(BigInt, StartHookCall)>, ()>,
) -> Result<ServedApp, NativeThrown> {
    let flush_bound = FlushBound::of_millis(plan.exit_flush_bound_ms).map_err(|_too_long| {
        client_defect("the exit flush bound of a plan is at most 8,000 milliseconds")
    })?;
    let args = match read_arguments(&plan.process_arguments)? {
        ArgumentsRead::Child(args) => args,
        ArgumentsRead::Bad { cause } => {
            return Err(client_defect(&format!(
                "the serve got bad child-mode arguments: {cause}"
            )))
        }
    };
    if SERVED.swap(true, Ordering::AcqRel) {
        return Err(client_defect("a process serves as a child once"));
    }
    let poster = Arc::new(ChildPoster::register(post).map_err(|error| {
        client_defect(&format!("Node-API refused the post dispatcher: {error}"))
    })?);
    let dispatcher: Arc<HookDispatcher> =
        Arc::new(HookDispatcher::register(hook).map_err(|error| {
            client_defect(&format!("Node-API refused the hook dispatcher: {error}"))
        })?);
    let producer_manifest =
        parse_producer_manifest(&plan.producer_manifest_json).map_err(|refused| {
            NativeThrown::ManifestRefused {
                field_path: refused.field_path,
                cause: refused.cause,
            }
        })?;
    let bodies = Arc::new(ChildBodies {
        poster,
        facts: BodyFacts::of(plan.body_facts),
    });
    let host_loop =
        make_host_loop().map_err(|_made| client_defect("a process serves as a child once"))?;
    let app = ChildApp {
        registry: Arc::new(task_registry(&plan.tasks, &bodies)?),
        hostname: plan.hostname,
        start_hook: start_hooks(plan.start_hooks, &dispatcher),
        producer_manifest,
        log_setup: child_log_setup(),
        family: ClientFamily::TypeScript,
        body_scope: bodies.facts.scope(),
        handler_child: take_handler_child(),
        event_loop: host_loop_watch(host_loop),
        exit: child_process_exit(),
    };
    Ok(ServedApp {
        app,
        args,
        flush_bound,
    })
}

/// The loop watch of a JavaScript child: the JavaScript main thread's loop.
fn host_loop_watch(host_loop: Arc<HostLoop>) -> EventLoopWatch {
    EventLoopWatch::Host(host_loop)
}

/// The registry of `tasks`: one body form and one context use per task. A
/// repeated or empty name is a defect of the package, which gives one entry
/// per implementation.
fn task_registry(
    tasks: &[ChildTaskEntry],
    bodies: &Arc<ChildBodies>,
) -> Result<TaskRegistry, NativeThrown> {
    let mut registry = TaskRegistry::new();
    for task in tasks {
        let registered = match task.form {
            BodyForm::Async => RegisteredTask::Async {
                task: Arc::new(JsAsyncBody {
                    task_name: task.name.clone(),
                    bodies: Arc::clone(bodies),
                }),
                meta: TaskMeta::default(),
            },
            BodyForm::Blocking => RegisteredTask::Blocking {
                task: Arc::new(JsBlockingBody {
                    task_name: task.name.clone(),
                    bodies: Arc::clone(bodies),
                }),
                meta: TaskMeta::default(),
            },
        };
        registry
            .register(
                task.name.clone(),
                with_context_use(registered, task.context),
            )
            .map_err(|error| client_defect(&format!("the task {}: {error}", task.name)))?;
    }
    Ok(registry)
}

/// `registered` with the context use of its entry: a task that takes the
/// context accepts the `workflow_context` member; another task keeps the
/// default, which removes it. Pure.
fn with_context_use(registered: RegisteredTask, context: TaskContextUse) -> RegisteredTask {
    match context {
        TaskContextUse::TakesContext => registered.with_workflow_context(),
        TaskContextUse::NoContext => registered,
    }
}

fn client_defect(rule: &str) -> NativeThrown {
    NativeThrown::ClientDefect {
        subject: SERVE_SUBJECT.to_owned(),
        rule: rule.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use horsies_core::core::task::fn_trait::{BlockingTaskFn, RawTaskResult};
    use horsies_core::TaskResult;

    use super::*;

    fn arguments(texts: &[&str]) -> Vec<String> {
        texts.iter().map(|text| (*text).to_owned()).collect()
    }

    /// A body that gives its input back.
    struct EchoBody;

    impl BlockingTaskFn for EchoBody {
        fn execute(&self, args: &[u8]) -> RawTaskResult {
            TaskResult::Ok(args.to_vec())
        }
    }

    fn echo_task() -> RegisteredTask {
        RegisteredTask::Blocking {
            task: Arc::new(EchoBody),
            meta: TaskMeta::default(),
        }
    }

    #[test]
    fn a_task_that_takes_the_context_accepts_the_workflow_context() {
        assert!(
            with_context_use(echo_task(), TaskContextUse::TakesContext).accepts_workflow_context()
        );
    }

    #[test]
    fn a_task_that_takes_no_context_does_not_accept_the_workflow_context() {
        assert!(
            !with_context_use(echo_task(), TaskContextUse::NoContext).accepts_workflow_context()
        );
    }

    #[test]
    fn the_child_flag_with_a_descriptor_and_a_pid_starts_a_child() {
        assert!(matches!(
            read_arguments(&arguments(&["--horsies-child", "3", "4242"])),
            Ok(ArgumentsRead::Child(_))
        ));
    }

    #[test]
    fn each_bad_argument_gives_its_cause_and_the_code_73() {
        for (texts, part) in [
            (&["--horsies-child"][..], "needs the socket descriptor"),
            (&["--horsies-child", "x", "4242"][..], "not a descriptor"),
            (&["--horsies-child", "3", "pid"][..], "not a pid"),
            (&["--horsies-child", "3", "4242", "extra"][..], "one more"),
        ] {
            match child_arguments(arguments(texts)) {
                Envelope::Value(ChildArgumentsCheck::BadArguments { cause, code }) => {
                    assert!(cause.contains(part), "{cause}");
                    assert_eq!(code, 73);
                }
                other => panic!("bad arguments, got {other:?}"),
            }
        }
    }

    #[test]
    fn arguments_with_no_child_flag_are_a_defect() {
        assert!(matches!(
            read_arguments(&arguments(&["worker"])),
            Err(NativeThrown::ClientDefect { .. })
        ));
    }

    #[test]
    fn each_exit_has_its_name_and_its_code() {
        assert_eq!(
            serve_end(ChildHostExit::Stopped),
            ChildServeEnd {
                code: 0,
                exit: ChildExitName::Stopped,
            }
        );
        assert_eq!(serve_end(ChildHostExit::ParentGone).code, 71);
        assert_eq!(
            serve_end(ChildHostExit::ChildHookFailed).exit,
            ChildExitName::ChildHookFailed
        );
        assert_eq!(serve_end(ChildHostExit::ChildHookFailed).code, 173);
    }
}

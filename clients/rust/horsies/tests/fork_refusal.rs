//! Every public call of the Rust client that runs on the core panics in a
//! process that was forked without `exec` after the core's process runtime
//! started in its parent: one call of each family, async and blocking.
//! Python raises `ForkedAfterCoreStart` in the same state.
//!
//! The parent builds an app, starts the process runtime and forks. The
//! child runs each call under `catch_unwind` and writes one line for each
//! call that did not panic with the refusal to a socket; the parent reads
//! the lines and asserts that there are none. No call reaches a database:
//! each refusal comes before the connect. An async call runs to its first
//! poll only, where the refusal comes. This binary holds this one test, so
//! the other threads of the parent at the fork are the harness and the idle
//! runtime workers, and the child uses neither.

#![cfg(unix)]

use std::any::Any;
use std::convert::Infallible;
use std::future::Future;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use horsies::monitoring::{cancel_task, list_workflow_names, pause_workflow_action};
use horsies::rerun::{RerunCommand, RerunEnqueuePolicy};
use horsies::{
    task, AppConfig, Horsies, TaskError, TaskInfoParts, WaitLimit, WorkflowHandle, WorkflowSpec,
};
use horsies_core::process_runtime::process_runtime;
use uuid::Uuid;

/// The text that every refusal panic holds.
const REFUSAL: &str = "is a fork of it without exec; nothing ran";

/// A task id and a workflow id that no database holds: no call reads them.
const TASK_ID: Uuid = Uuid::from_u128(0x0198_c0de_0000_7000_8000_0000_0000_19c1);
const WORKFLOW_ID: Uuid = Uuid::from_u128(0x0198_c0de_0000_7000_8000_0000_0000_19c2);

/// Exit code of a child whose report could not be written.
const REPORT_NOT_WRITTEN: i32 = 3;

#[task("fork_refusal_echo")]
async fn echo(value: u32) -> Result<u32, TaskError<Infallible>> {
    Ok(value)
}

#[task("fork_refusal_answer")]
async fn answer() -> Result<u32, TaskError<Infallible>> {
    Ok(1)
}

/// The text of a panic payload.
fn panic_text(payload: &(dyn Any + Send)) -> String {
    match (
        payload.downcast_ref::<String>(),
        payload.downcast_ref::<&str>(),
    ) {
        (Some(text), _) => text.clone(),
        (None, Some(text)) => (*text).to_owned(),
        (None, None) => "a panic without a text".to_owned(),
    }
}

/// `None` when `call` panicked with the refusal; else what it did instead.
fn refusal_of(name: &str, call: impl FnOnce()) -> Option<String> {
    match catch_unwind(AssertUnwindSafe(call)) {
        Ok(()) => Some(format!("{name}: returned; expected the fork panic")),
        Err(payload) => {
            let text = panic_text(&*payload);
            match text.contains(REFUSAL) {
                true => None,
                false => Some(format!("{name}: panicked with another text: {text}")),
            }
        }
    }
}

/// Poll `future` once with a waker that does nothing: the refusal of an
/// async call comes in its first poll, before any await.
fn first_poll<F: Future>(future: F) -> Poll<F::Output> {
    let mut future = pin!(future);
    future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
}

/// A workflow of one node whose output is `answer`.
fn one_node_spec(app: &mut Horsies) -> WorkflowSpec {
    let answer_function = answer::register(app).expect("register the workflow task");
    let mut builder = app.workflow_builder("fork_refusal_flow");
    builder.definition_key("tests.fork_refusal.flow.v1");
    let output = builder
        .task(answer_function.node())
        .expect("the app registers answer");
    builder.output(output);
    builder.build().expect("the spec builds")
}

/// One call of each family, in the fork. Returns a line for each call that
/// did not panic with the refusal.
fn child_report(app: &mut Horsies) -> Vec<String> {
    let echo_function = echo::register(app).expect("register the task");
    let spec = one_node_spec(app);
    let task_handle = app.task_handle::<u32, Infallible>(TASK_ID);
    let workflow: WorkflowHandle<u32> = app.workflow_handle(WORKFLOW_ID);
    let app = &*app;
    let calls: [Option<String>; 18] = [
        refusal_of("TaskFunction::send", || {
            let _never = first_poll(echo_function.send(echo::Input { value: 1 }));
        }),
        refusal_of("blocking TaskFunction::send", || {
            let _never = echo_function.blocking().send(echo::Input { value: 1 });
        }),
        refusal_of("Horsies::get_task_info", || {
            let _never = first_poll(app.get_task_info(TASK_ID, TaskInfoParts::default()));
        }),
        refusal_of("Horsies::get_raw_result", || {
            let _never = first_poll(app.get_raw_result(TASK_ID, WaitLimit::NoLimit));
        }),
        refusal_of("TaskHandle::get", || {
            let _never = first_poll(task_handle.get(WaitLimit::NoLimit));
        }),
        refusal_of("blocking TaskHandle::info", || {
            let _never = task_handle.blocking().info(TaskInfoParts::default());
        }),
        refusal_of("Horsies::ping_database", || {
            let _never = first_poll(app.ping_database());
        }),
        refusal_of("blocking Horsies::list_worker_states", || {
            let _never = app.blocking().list_worker_states();
        }),
        refusal_of("Horsies::check_live", || {
            let _never = first_poll(app.check_live());
        }),
        refusal_of("blocking Horsies::check_live", || {
            let _never = app.blocking().check_live();
        }),
        refusal_of("Horsies::start", || {
            let _never = first_poll(app.start::<u32>(&spec));
        }),
        refusal_of("blocking Horsies::start", || {
            let _never = app.blocking().start::<u32>(&spec);
        }),
        refusal_of("WorkflowHandle::status", || {
            let _never = first_poll(workflow.status());
        }),
        refusal_of("blocking WorkflowHandle::get", || {
            let _never = workflow.blocking().get(WaitLimit::NoLimit);
        }),
        refusal_of("monitoring::list_workflow_names", || {
            let _never = first_poll(list_workflow_names(app));
        }),
        refusal_of("monitoring::cancel_task", || {
            let _never = first_poll(cancel_task(app, TASK_ID, false));
        }),
        refusal_of("monitoring::pause_workflow_action", || {
            let _never = first_poll(pause_workflow_action(app, WORKFLOW_ID));
        }),
        refusal_of("Horsies::rerun_task", || {
            let _never = first_poll(app.rerun_task(
                RerunCommand::new(TASK_ID, None, None),
                RerunEnqueuePolicy::standard(false),
            ));
        }),
    ];
    calls.into_iter().flatten().collect()
}

/// The exit code of the child: 0 when it wrote its report.
fn run_child(mut app: Horsies, mut report: UnixStream) -> i32 {
    let lines = child_report(&mut app);
    match report.write_all(lines.join("\n").as_bytes()) {
        Ok(()) => 0,
        Err(_) => REPORT_NOT_WRITTEN,
    }
}

#[test]
fn every_call_family_panics_in_a_fork() {
    let app = Horsies::new(AppConfig::from_database_url(
        "postgresql://127.0.0.1:1/fork_refusal",
    ))
    .expect("build the app");
    let _runtime = process_runtime().expect("start the core's process runtime");
    let (mut parent_end, child_end) = UnixStream::pair().expect("a socket pair");
    // SAFETY: the child runs only the code under test and `_exit`; it uses
    // no lock that another thread of the parent may hold at the fork, and
    // the runtime of the parent, which it would block on, is refused first.
    let pid = unsafe { libc::fork() };
    match pid {
        -1 => panic!("fork failed: {}", std::io::Error::last_os_error()),
        0 => {
            drop(parent_end);
            let code = run_child(app, child_end);
            // SAFETY: `_exit` ends the child without the destructors and
            // exit handlers of state that the parent owns.
            unsafe { libc::_exit(code) }
        }
        child => {
            drop(child_end);
            let mut report = String::new();
            let _read = parent_end
                .read_to_string(&mut report)
                .expect("read the child's report");
            let mut status: libc::c_int = 0;
            // SAFETY: `child` is the process id of this process's child.
            let waited = unsafe { libc::waitpid(child, &mut status, 0) };
            assert_eq!(waited, child, "waitpid of the child");
            assert!(
                libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
                "the child did not end with exit code 0: status {status}"
            );
            assert_eq!(
                report, "",
                "calls in the fork that did not panic with the refusal:\n{report}"
            );
        }
    }
}

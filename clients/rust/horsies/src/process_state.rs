//! The state of the core's process runtime, for every call of the client
//! that runs on the core.
//!
//! The pools and the listener of an app drive their connections on the
//! core's process runtime ([`horsies_core::process_runtime`]). A process
//! that was forked without `exec` after that runtime started in its parent
//! has none of its threads, so no call can run on the core there. The fork
//! is a state of the process, never the result of one call: every public
//! call that runs on the core panics in such a process before it runs
//! anything, in its async and its blocking form, as every Python core call
//! raises `ForkedAfterCoreStart`. The Rust client has no stopped-core
//! state: a panic in a core task of a Rust program is the program's panic.
//!
//! A runtime that the OS refused to start is the failure of one call
//! ([`RuntimeStartFailed`]): a later use tries again.

use std::fmt;

use horsies_core::process_runtime::{
    process_runtime, process_runtime_status, OsErrorCode, ProcessRuntime, ProcessRuntimeRefusal,
    ProcessRuntimeStatus,
};

/// The OS refused the threads of the core's process runtime. Nothing ran;
/// a later use tries the start again.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the core runtime could not start: {cause}")]
pub struct RuntimeStartFailed {
    /// The cause that the OS gave.
    pub cause: String,
    /// The OS error code of the cause, when it was one.
    pub os_error: OsErrorCode,
}

/// Panics when this process is a fork, without `exec`, of the process
/// that started the core's process runtime ([`forked`]). Starts nothing.
/// Every async call that runs on the core calls it before its first core
/// operation; `call` names the call for the panic.
pub(crate) fn refuse_in_a_fork(call: fmt::Arguments<'_>) {
    match process_runtime_status() {
        ProcessRuntimeStatus::NotStarted | ProcessRuntimeStatus::Running => {}
        ProcessRuntimeStatus::Inherited => match process_runtime() {
            Err(ProcessRuntimeRefusal::ForkedAfterStart { core_pid, this_pid }) => {
                forked(call, core_pid, this_pid)
            }
            Ok(_) | Err(ProcessRuntimeRefusal::StartFailed { .. }) => {
                unreachable!("the runtime of a parent process is refused as a fork")
            }
        },
    }
}

/// The core's process runtime for the call `call`, started at its first
/// use: the place where a blocking call or a process entry point runs.
///
/// Errors: [`RuntimeStartFailed`] when the OS refused the runtime's
/// threads.
///
/// # Panics
///
/// In a fork, without `exec`, of the process that started the runtime
/// ([`forked`]).
pub(crate) fn runtime_for(
    call: fmt::Arguments<'_>,
) -> Result<&'static ProcessRuntime, RuntimeStartFailed> {
    runtime_of(call, process_runtime())
}

/// The runtime of `outcome`, the answer of `process_runtime` for the call
/// `call`. Pure but for the panic.
///
/// # Panics
///
/// For a fork ([`forked`]).
fn runtime_of(
    call: fmt::Arguments<'_>,
    outcome: Result<&'static ProcessRuntime, ProcessRuntimeRefusal>,
) -> Result<&'static ProcessRuntime, RuntimeStartFailed> {
    match outcome {
        Ok(runtime) => Ok(runtime),
        Err(ProcessRuntimeRefusal::ForkedAfterStart { core_pid, this_pid }) => {
            forked(call, core_pid, this_pid)
        }
        Err(ProcessRuntimeRefusal::StartFailed { cause, os_error }) => {
            Err(RuntimeStartFailed { cause, os_error })
        }
    }
}

/// The panic of the call `call` in process `this_pid`, a fork without
/// `exec` of process `core_pid`, where the core's process runtime started.
/// The fork is a state of the process: no call in it can run on the core,
/// so it is never an error value of a call.
fn forked(call: fmt::Arguments<'_>, core_pid: u32, this_pid: u32) -> ! {
    panic!(
        "{call} refused: the core runtime started in process {core_pid}, and this process \
         ({this_pid}) is a fork of it without exec; nothing ran. Start the child process with \
         exec (for example std::process::Command), or fork before the parent's first call that \
         runs on the core"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A call in a fork panics with the call, both processes and the fix.
    #[test]
    #[should_panic(
        expected = "send of task 'send_invoice' refused: the core runtime started in process 10, \
                    and this process (11) is a fork of it without exec; nothing ran. Start the \
                    child process with exec"
    )]
    fn a_call_in_a_fork_panics_with_both_processes_and_the_fix() {
        forked(format_args!("send of task '{}'", "send_invoice"), 10, 11);
    }

    /// The test process is not a fork of the process that started the
    /// runtime, so the fork check lets a call through.
    #[test]
    fn the_fork_check_lets_a_call_through_outside_a_fork() {
        refuse_in_a_fork(format_args!("Horsies::ping_database"));
    }

    /// A blocking call or an entry point in a fork panics; it never answers
    /// an error value.
    #[test]
    #[should_panic(
        expected = "blocking Horsies::ping_database() refused: the core runtime started in \
                    process 1, and this process (2) is a fork of it without exec"
    )]
    fn a_runtime_in_a_fork_panics() {
        let _never = runtime_of(
            format_args!("blocking Horsies::ping_database()"),
            Err(ProcessRuntimeRefusal::ForkedAfterStart {
                core_pid: 1,
                this_pid: 2,
            }),
        );
    }

    /// A start that the OS refused is the failure of the call, with the
    /// cause and the OS error code.
    #[test]
    fn a_runtime_that_did_not_start_is_the_failure_of_the_call() {
        let Err(failed) = runtime_of(
            format_args!("blocking Horsies::ping_database()"),
            Err(ProcessRuntimeRefusal::StartFailed {
                cause: "EAGAIN".to_owned(),
                os_error: OsErrorCode::Code(11),
            }),
        ) else {
            panic!("a refused start is the failure of the call");
        };
        assert_eq!(
            failed,
            RuntimeStartFailed {
                cause: "EAGAIN".to_owned(),
                os_error: OsErrorCode::Code(11),
            }
        );
        assert_eq!(
            failed.to_string(),
            "the core runtime could not start: EAGAIN"
        );
    }
}

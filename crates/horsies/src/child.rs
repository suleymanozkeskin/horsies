//! Child mode of the user's binary: `child_mode`, `Horsies::serve_child`,
//! and the start hooks of each child (`Horsies::on_child_start`).
//!
//! The worker starts this binary again with `--horsies-child <fd> <pid>` for
//! every child, whatever the execution class of its tasks. Every line of
//! `main` before the hand-over runs in each child too, as a Python child
//! imports the app again: build the app, then hand over to horsies
//! (`horsies::cli::run(app)`, or `child_mode` and `serve_child`).

use std::future::Future;
use std::process::ExitCode;
use std::sync::Arc;

use horsies_core::{
    run_child_host, ChildApp, ChildHookFailure, ChildLogSetup, ChildStartFuture, ChildStartHook,
};

pub use horsies_core::{
    ChildArgs, ChildArgsError, ChildHostExit, ChildMode, CHILD_MODE_ARGUMENT,
    CHILD_START_HOOK_BOUND,
};

use crate::app::ChildStartFn;
use crate::Horsies;

/// How this process was started, read from its arguments:
/// `ChildMode::Child` when a worker started it as a child,
/// `ChildMode::BadArguments` for a child start with arguments that cannot
/// be used, else `ChildMode::Worker`. Reads the process arguments only.
pub fn child_mode() -> ChildMode {
    horsies_core::child_mode(std::env::args_os())
}

impl Horsies {
    /// Run `hook` once in every worker child, a replacement child included
    /// (Python `on_child_process_start`, `on_shared_async_child_start`).
    ///
    /// - It runs on the child's runtime, after the child connected its pool
    ///   and before it takes its first task. Hooks run in registration
    ///   order, and the first failure ends the rest.
    /// - The hooks of one child run for at most [`CHILD_START_HOOK_BOUND`]
    ///   (10 s) together.
    /// - A hook that returns an error, panics or runs past the bound ends
    ///   the child with exit code 173, and the worker stops with
    ///   `CHILD_HOOK_FAILED`. The error's text is the logged cause.
    /// - Work that a hook starts and leaves running belongs to no task.
    ///
    /// Only children run the hooks: the worker process, the scheduler and a
    /// producer never call them.
    pub fn on_child_start<F, Fut, E>(&mut self, hook: F)
    where
        F: FnOnce() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: std::fmt::Display,
    {
        self.add_child_start_hook(Box::new(move || -> ChildStartFuture {
            Box::pin(async move {
                hook().await.map_err(|error| ChildHookFailure {
                    cause: error.to_string(),
                })
            })
        }));
    }

    /// Serve as a worker child until the worker's stop frame or the end of
    /// the worker, for a binary that owns its `main`: call it when
    /// [`child_mode`] answers `ChildMode::Child`, after `main` built the
    /// app. `horsies::cli::run` calls it.
    ///
    /// Returns the child's exit code: 0 at the stop frame; otherwise the
    /// code of [`ChildHostExit`] (71 to 78 for a parent or protocol failure,
    /// 173 for a failed start hook). The child keeps the log setup that the
    /// binary installed before this call.
    pub fn serve_child(self, args: ChildArgs) -> ExitCode {
        self.serve_child_with_logs(args, ChildLogSetup::NoSetup)
    }

    /// [`Self::serve_child`] with `log_setup`, which the child calls with
    /// the log level of its init frame.
    pub(crate) fn serve_child_with_logs(
        self,
        args: ChildArgs,
        log_setup: ChildLogSetup,
    ) -> ExitCode {
        let parts = self.into_child_parts();
        let app = ChildApp {
            registry: Arc::new(parts.tasks),
            hostname: host_name(),
            start_hook: start_hook_of(parts.start_hooks),
            producer_manifest: parts.producer_manifest,
            log_setup,
        };
        ExitCode::from(exit_byte(run_child_host(app, args).code()))
    }
}

/// The core's one start hook: `hooks` in registration order; the first
/// failure ends it. No hook is `NoHook`. Pure.
fn start_hook_of(hooks: Vec<ChildStartFn>) -> ChildStartHook {
    match hooks.is_empty() {
        true => ChildStartHook::NoHook,
        false => ChildStartHook::Run(Box::new(move || -> ChildStartFuture {
            Box::pin(async move {
                for hook in hooks {
                    hook().await?;
                }
                Ok(())
            })
        })),
    }
}

/// The host name that the child writes with each start. A name that is not
/// UTF-8 keeps its readable part.
fn host_name() -> String {
    gethostname::gethostname().to_string_lossy().into_owned()
}

/// The process exit status of a child exit code. The core's codes are 0 to
/// 255 by definition (`protocol/exit_codes.rs`); another value is a defect.
pub(crate) fn exit_byte(code: i32) -> u8 {
    u8::try_from(code).unwrap_or_else(|_| panic!("exit code {code} is outside 0 to 255"))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::AppConfig;

    fn app() -> Horsies {
        Horsies::new(AppConfig::for_database_url(
            "postgresql://localhost/horsies",
        ))
        .unwrap()
    }

    /// Runs the core hook that `hooks` give, with the calls in `calls`.
    async fn run_hooks(hooks: Vec<ChildStartFn>) -> Result<(), ChildHookFailure> {
        match start_hook_of(hooks) {
            ChildStartHook::NoHook => Ok(()),
            ChildStartHook::Run(hook) => hook().await,
        }
    }

    fn recording_hook(
        calls: &Arc<Mutex<Vec<&'static str>>>,
        name: &'static str,
        fails: bool,
    ) -> ChildStartFn {
        let calls = Arc::clone(calls);
        Box::new(move || -> ChildStartFuture {
            Box::pin(async move {
                calls.lock().unwrap().push(name);
                match fails {
                    true => Err(ChildHookFailure {
                        cause: format!("{name} failed"),
                    }),
                    false => Ok(()),
                }
            })
        })
    }

    #[test]
    fn an_app_with_no_hook_gives_the_core_no_hook() {
        assert!(matches!(start_hook_of(Vec::new()), ChildStartHook::NoHook));
    }

    #[tokio::test]
    async fn hooks_run_in_registration_order_and_the_first_failure_ends_the_rest() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let failure = run_hooks(vec![
            recording_hook(&calls, "first", false),
            recording_hook(&calls, "second", true),
            recording_hook(&calls, "third", false),
        ])
        .await
        .unwrap_err();
        assert_eq!(failure.cause, "second failed");
        assert_eq!(*calls.lock().unwrap(), vec!["first", "second"]);
    }

    #[tokio::test]
    async fn on_child_start_keeps_the_text_of_the_hook_error() {
        let mut app = app();
        app.on_child_start(|| async { Err::<(), _>("the cache did not open") });
        app.on_child_start(|| async { Ok::<(), std::convert::Infallible>(()) });
        assert_eq!(app.child_start_hook_count(), 2);
        let parts = app.into_child_parts();
        let failure = run_hooks(parts.start_hooks).await.unwrap_err();
        assert_eq!(failure.cause, "the cache did not open");
    }

    #[test]
    fn child_parts_hold_the_tasks_and_the_producer_manifest_of_the_app() {
        let app = app();
        let manifest = app.handle().manifest().clone();
        let parts = app.into_child_parts();
        assert!(parts.tasks.is_empty());
        assert!(parts.start_hooks.is_empty());
        assert_eq!(parts.producer_manifest, manifest);
    }

    #[test]
    fn every_exit_code_of_the_core_is_a_process_exit_status() {
        for exit in [
            ChildHostExit::Stopped,
            ChildHostExit::ParentGone,
            ChildHostExit::BadParentFrame,
            ChildHostExit::BadArguments,
            ChildHostExit::BootFailed,
            ChildHostExit::ProtocolMismatch,
            ChildHostExit::ChildHookFailed,
        ] {
            assert_eq!(i32::from(exit_byte(exit.code())), exit.code());
        }
        assert_eq!(exit_byte(ChildHostExit::ChildHookFailed.code()), 173);
    }
}

//! Child mode of the user's binary: `child_mode`, `Horsies::serve_child`,
//! and the start hooks of each child (`Horsies::on_child_start`).
//!
//! The worker starts this binary again with `--horsies-child <fd> <pid>` for
//! every child, whatever the execution class of its tasks, and for its
//! handler child, which runs the app's event handlers (`horsies::events`).
//! Every line of
//! `main` before the hand-over runs in each child too, as a Python child
//! imports the app again: build the app, then hand over to horsies
//! (`horsies::cli::run(app)`, or `child_mode` and `serve_child`).

use std::future::Future;
use std::process::ExitCode;
use std::sync::Arc;

use horsies_core::{
    run_child_host, ChildApp, ChildHookFailure, ChildLogSetup, ChildStartFuture, ChildStartHook,
    ClientFamily, HandlerChildApp, NamedChildStartHook,
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
    /// - Each hook runs for at most [`CHILD_START_HOOK_BOUND`] (10 s), as
    ///   each Python hook does.
    /// - A hook that returns an error, panics or runs past its bound ends
    ///   the child with exit code 173, and the worker stops with
    ///   `CHILD_HOOK_FAILED`. The logged cause names the hook by its
    ///   position and the type name of `hook` (for a closure, the path of
    ///   the function that defines it), and gives the error's text.
    /// - Work that a hook starts and leaves running belongs to no task.
    ///
    /// Only task children run the hooks: the worker process, the handler
    /// child ([`Self::on_handler_child_start`]), the scheduler and a
    /// producer never call them.
    pub fn on_child_start<F, Fut, E>(&mut self, hook: F)
    where
        F: FnOnce() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: std::fmt::Display,
    {
        self.add_child_start_hook(named_start_hook(hook));
    }

    /// Serve as a worker child until the worker's stop frame or the end of
    /// the worker, for a binary that owns its `main`: call it when
    /// [`child_mode`] answers `ChildMode::Child`, after `main` built the
    /// app. `horsies::cli::run` calls it.
    ///
    /// Returns the child's exit code: 0 at the stop frame; otherwise the
    /// code of [`ChildHostExit`] (71 to 78 for a parent or protocol failure,
    /// 173 for a failed start hook). The child keeps the log setup that the
    /// binary installed before this call. With the feature `otel`, each task
    /// body runs inside its attempt span (`horsies::otel`).
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
            family: ClientFamily::Rust,
            body_scope: crate::telemetry::body_scope(),
            handler_child: HandlerChildApp {
                handlers: parts.event_handlers,
                start_hook: start_hook_of(parts.handler_child_start_hooks),
            },
        };
        ExitCode::from(exit_byte(run_child_host(app, args).code()))
    }
}

/// The start hook of `hook`, named by its type name (for a closure, the path
/// of the function that defines it), whose error's text is the cause of a
/// failure. Pure.
pub(crate) fn named_start_hook<F, Fut, E>(hook: F) -> NamedChildStartHook
where
    F: FnOnce() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display,
{
    NamedChildStartHook {
        name: std::any::type_name::<F>().to_owned(),
        start: Box::new(move || -> ChildStartFuture {
            Box::pin(async move {
                hook().await.map_err(|error| ChildHookFailure {
                    cause: error.to_string(),
                })
            })
        }),
    }
}

/// The core's start hooks: `hooks` in registration order, each bounded by
/// the core alone. No hook is `NoHook`. Pure.
pub(crate) fn start_hook_of(hooks: Vec<ChildStartFn>) -> ChildStartHook {
    match hooks.is_empty() {
        true => ChildStartHook::NoHook,
        false => ChildStartHook::Run(hooks),
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

    /// The hooks that the core gets from `hooks`, in its order.
    fn core_hooks(hooks: Vec<ChildStartFn>) -> Vec<NamedChildStartHook> {
        match start_hook_of(hooks) {
            ChildStartHook::NoHook => Vec::new(),
            ChildStartHook::Run(hooks) => hooks,
        }
    }

    fn recording_hook(calls: &Arc<Mutex<Vec<&'static str>>>, name: &'static str) -> ChildStartFn {
        let calls = Arc::clone(calls);
        NamedChildStartHook {
            name: name.to_owned(),
            start: Box::new(move || -> ChildStartFuture {
                Box::pin(async move {
                    calls.lock().unwrap().push(name);
                    Ok(())
                })
            }),
        }
    }

    #[test]
    fn an_app_with_no_hook_gives_the_core_no_hook() {
        assert!(matches!(start_hook_of(Vec::new()), ChildStartHook::NoHook));
    }

    #[tokio::test]
    async fn the_core_gets_each_hook_in_registration_order() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let hooks = core_hooks(vec![
            recording_hook(&calls, "first"),
            recording_hook(&calls, "second"),
        ]);
        let names: Vec<&str> = hooks.iter().map(|hook| hook.name.as_str()).collect();
        assert_eq!(names, vec!["first", "second"]);
        for hook in hooks {
            (hook.start)().await.unwrap();
        }
        assert_eq!(*calls.lock().unwrap(), vec!["first", "second"]);
    }

    async fn cache_hook() -> Result<(), &'static str> {
        Err("the cache did not open")
    }

    #[tokio::test]
    async fn on_child_start_names_the_hook_and_keeps_the_text_of_its_error() {
        let mut app = app();
        app.on_child_start(cache_hook);
        app.on_child_start(|| async { Ok::<(), std::convert::Infallible>(()) });
        assert_eq!(app.child_start_hook_count(), 2);
        let mut hooks = core_hooks(app.into_child_parts().start_hooks).into_iter();
        let first = hooks.next().unwrap();
        assert!(first.name.ends_with("::cache_hook"), "{}", first.name);
        let failure = (first.start)().await.unwrap_err();
        assert_eq!(failure.cause, "the cache did not open");
        let second = hooks.next().unwrap();
        assert!(second.name.contains("{{closure}}"), "{}", second.name);
        (second.start)().await.unwrap();
    }

    #[test]
    fn child_parts_hold_the_tasks_and_the_producer_manifest_of_the_app() {
        let app = app();
        let manifest = app.handle().manifest().clone();
        let parts = app.into_child_parts();
        assert!(parts.tasks.is_empty());
        assert!(parts.start_hooks.is_empty());
        assert_eq!(parts.producer_manifest, manifest);
        assert!(parts.event_handlers.is_empty());
        assert!(parts.handler_child_start_hooks.is_empty());
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

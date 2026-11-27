//! The static check and the live check of the app (Python `Horsies.check`,
//! `horsies check [--live]`).
//!
//! The check runs in phases and stops at the first phase that refuses, as
//! Python's does. The config and each registration were checked when the
//! app was built; the check adds what needs the whole app:
//!
//! 1. Workflows: each workflow definition without params and each check
//!    case of a workflow builder, built and checked against the registered
//!    definitions and the app's queues, and walked into its sub-workflows;
//!    a nesting is refused only when it reaches the depth limit for certain
//!    (Python's phases 3.1 and 3.2).
//! 2. Task options: the rules of each registered task's options that the
//!    task attribute checks at compile time, for a task registered by hand,
//!    and Python's text rule of each `retry_for` code
//!    (`check_error_code_text`).
//! 3. Schedules (not for a worker, which never sends a scheduled task): per
//!    enabled schedule, its task, its queue, its input read back with the
//!    task's input type, and the input size.
//!
//! The live check adds, after a passing static check:
//!
//! 4. Connectivity, the core's rule for both clients
//!    (`PostgresBroker::check_connectivity`): `SELECT 1` on the main URL,
//!    then on the session URL when it differs, then, in PgBouncer
//!    transaction mode, a LISTEN probe on the session URL.
//! 5. Worker connections: each worker profile against the server's
//!    connection limit (none through a transaction pooler).

use std::collections::BTreeMap;

use horsies_core::core::task::error::check_error_code_text;
use horsies_core::handle::{CoreConnection, HandleClose};
use horsies_core::{
    check_worker_connections, ClientFamily, ConnectivityRefusal, Execution, PoolProfile,
    RegisteredTask, TaskOptions,
};

use crate::config::producer_manifest;
use crate::process_state::refuse_in_a_fork;
use crate::{
    AppConfig, AppRole, Horsies, HorsiesError, HorsiesErrorCode, SchemaMode, ValidationReport,
};

/// The check passed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckPassed {
    task_count: usize,
    declared_error_codes: BTreeMap<String, &'static [&'static str]>,
}

impl CheckPassed {
    /// The number of registered tasks.
    pub fn task_count(&self) -> usize {
        self.task_count
    }

    /// The error codes that each task declares (`CustomErrorCode::CODES` of
    /// its code type), by task name, for each task that declares at least
    /// one. Each one is a value of the `error_code` metric label of its
    /// task; another custom code of the task counts as `undeclared`.
    pub fn declared_error_codes(&self) -> &BTreeMap<String, &'static [&'static str]> {
        &self.declared_error_codes
    }
}

impl std::fmt::Display for CheckPassed {
    /// Python's success text of `horsies check`: the task count, then the
    /// declared error codes of each task that declares some, in task name
    /// order, or `none`.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "ok: all validations passed\n  {} task(s) registered",
            self.task_count
        )?;
        match self.declared_error_codes.is_empty() {
            true => write!(formatter, "\n  declared error codes: none"),
            false => {
                write!(formatter, "\n  declared error codes:")?;
                for (task_name, codes) in &self.declared_error_codes {
                    write!(formatter, "\n    {task_name}: {}", codes.join(", "))?;
                }
                Ok(())
            }
        }
    }
}

/// The phase name of `horsies check` (Python's report name).
pub(crate) const CHECK: &str = "check";
/// The phase name of the check before a worker starts.
pub(crate) const WORKER_STARTUP: &str = "worker-startup";
/// The phase name of the check before a scheduler starts.
pub(crate) const SCHEDULER_STARTUP: &str = "scheduler-startup";

/// Which phases a static check runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckScope {
    /// Every phase.
    WithSchedules,
    /// Every phase but the schedules: a worker never sends a scheduled
    /// task.
    WithoutSchedules,
}

impl CheckScope {
    /// The scope of a process of `role` (Python `_check_schedules`).
    fn of_role(role: AppRole) -> Self {
        match role {
            AppRole::Worker => Self::WithoutSchedules,
            AppRole::Producer | AppRole::Scheduler => Self::WithSchedules,
        }
    }
}

impl Horsies {
    /// The static check: the phases of the module docs for the role of
    /// this process (a worker skips the schedules). No I/O.
    ///
    /// Errors: [`ValidationReport`] with the refusals of the first phase that
    /// refused.
    pub fn check(&self) -> Result<CheckPassed, ValidationReport> {
        self.check_scope(CheckScope::of_role(self.role()), CHECK)
    }

    /// The static check, then the connectivity and the worker connections
    /// (Python `check(live=True)`). Connects a handle of its own with the
    /// schema mode [`SchemaMode::ObserveOnly`], whatever the mode of the
    /// app's handle, and closes it at the end; the app's handle does not
    /// connect. Writes nothing but the LISTEN probe's one NOTIFY in
    /// PgBouncer transaction mode. Installs no schema.
    ///
    /// Errors: [`ValidationReport`] with the refusals of the first phase that
    /// refused.
    ///
    /// # Panics
    ///
    /// After the static check passed, in a process that was forked without
    /// `exec` after the core's process runtime started. Nothing connected.
    pub async fn check_live(&self) -> Result<CheckPassed, ValidationReport> {
        let passed = self.check()?;
        refuse_in_a_fork(format_args!("Horsies::check_live"));
        let handle = live_check_handle(self.config())?;
        let outcome = run_live_phases(self.config(), &handle).await;
        let _closed: HandleClose = handle.close().await;
        outcome.map(|()| passed)
    }

    /// The static check with the phases of `scope`; a refusal names
    /// `phase`.
    pub(crate) fn check_scope(
        &self,
        scope: CheckScope,
        phase: &'static str,
    ) -> Result<CheckPassed, ValidationReport> {
        let workflow_errors = self.workflow_check_findings();
        if !workflow_errors.is_empty() {
            return Err(ValidationReport::of(phase, workflow_errors));
        }
        let option_errors = task_option_refusals(self.tasks().iter());
        if !option_errors.is_empty() {
            return Err(ValidationReport::of(phase, option_errors));
        }
        let schedule_errors = match (scope, &self.config().schedule) {
            (CheckScope::WithoutSchedules, _) | (CheckScope::WithSchedules, None) => Vec::new(),
            (CheckScope::WithSchedules, Some(schedule)) => self.schedule_refusals(schedule),
        };
        match schedule_errors.is_empty() {
            true => Ok(CheckPassed {
                task_count: self.tasks().len(),
                declared_error_codes: declared_error_codes(self.tasks().iter()),
            }),
            false => Err(ValidationReport::of(phase, schedule_errors)),
        }
    }
}

/// The core handle of the live check: built from `config.database` with the
/// schema mode `ObserveOnly` (no schema step, no write but the probe's
/// NOTIFY), whatever the mode of the app. Nothing connects. Pure.
///
/// Errors: a [`ValidationReport`] of the phase `check` that holds the
/// refused database setting; `config` passed validation when the app was
/// built, so a refusal here is a setting that the manifest bounds refuse
/// and the config validation does not.
fn live_check_handle(config: &AppConfig) -> Result<CoreConnection, ValidationReport> {
    let manifest = producer_manifest(&config.database, SchemaMode::ObserveOnly)
        .map_err(|refused| ValidationReport::of(CHECK, vec![refused.to_horsies_error()]))?;
    Ok(CoreConnection::new(
        manifest,
        PoolProfile::Producer,
        ClientFamily::Rust,
    ))
}

/// The live phases on `handle`: the connectivity check, then the worker
/// connections. Connects `handle` and reads; leaves it open.
///
/// Errors: the report of the first phase that refused.
async fn run_live_phases(
    config: &AppConfig,
    handle: &CoreConnection,
) -> Result<(), ValidationReport> {
    let broker = match handle.broker().await {
        Ok(broker) => broker,
        Err(error) => return Err(connectivity_report(&error.to_string())),
    };
    if let Err(refused) = broker.check_connectivity().await {
        return Err(refusal_report(&refused));
    }
    match check_worker_connections(config, &broker).await {
        errors if errors.is_empty() => Ok(()),
        errors => Err(ValidationReport::of(CHECK, errors)),
    }
}

/// The declared error codes of each task that declares at least one, by
/// task name. The registration checked each set. Pure.
fn declared_error_codes<'a>(
    tasks: impl Iterator<Item = (&'a str, &'a RegisteredTask)>,
) -> BTreeMap<String, &'static [&'static str]> {
    tasks
        .filter(|(_name, task)| !task.error_codes().is_empty())
        .map(|(name, task)| (name.to_owned(), task.error_codes()))
        .collect()
}

/// The report of a failed connect or ping (Python
/// `_check_broker_connectivity`). Pure.
pub(crate) fn connectivity_report(cause: &str) -> ValidationReport {
    ValidationReport::of(
        CHECK,
        vec![HorsiesError::new("broker connectivity check failed")
            .with_code(HorsiesErrorCode::DatabaseInvalidUrl)
            .with_note(cause.to_owned())
            .with_help("check database_url in PostgresConfig")],
    )
}

/// The report of a refused connectivity step, with Python's two answers:
/// the listener probe's, and the connectivity failure's for every other
/// step. Pure.
fn refusal_report(refused: &ConnectivityRefusal) -> ValidationReport {
    match refused {
        ConnectivityRefusal::ListenerNotCapable(failure) => listener_report(&failure.to_string()),
        ConnectivityRefusal::PoolSettings(_)
        | ConnectivityRefusal::MainUnreachable(_)
        | ConnectivityRefusal::SessionUnreachable(_) => connectivity_report(&refused.to_string()),
    }
}

/// The report of a session URL that does not deliver a NOTIFY to a LISTEN
/// in PgBouncer transaction mode (Python `_run_listener_probe`), with the
/// probe's cause as a second note. Pure.
fn listener_report(cause: &str) -> ValidationReport {
    ValidationReport::of(
        CHECK,
        vec![HorsiesError::new("broker listener capability check failed")
            .with_code(HorsiesErrorCode::DatabaseInvalidUrl)
            .with_note(
                "the session_database URL appears to be transaction-pooled; LISTEN \
                 notification was not delivered",
            )
            .with_note(cause.to_owned())
            .with_help("set session_database to a separate direct/session-capable Postgres URL")],
    )
}

/// The refusals of the registered tasks' options, in task name order (Python
/// `_check_runtime_policy_safety`, the rules a hand registration can break).
/// Pure.
fn task_option_refusals<'a>(
    tasks: impl Iterator<Item = (&'a str, &'a RegisteredTask)>,
) -> Vec<HorsiesError> {
    let mut named: Vec<(&str, &TaskOptions)> = tasks
        .filter_map(|(name, task)| task.task_options().map(|options| (name, options)))
        .collect();
    named.sort_by_key(|(name, _)| *name);
    named
        .into_iter()
        .flat_map(|(name, options)| options_refusals(name, options))
        .collect()
}

/// The refusals of one task's options. Pure.
fn options_refusals(name: &str, options: &TaskOptions) -> Vec<HorsiesError> {
    let mut refusals = Vec::new();
    if options.task_name != name {
        refusals.push(option_error(
            name,
            format!(
                "task_options.task_name '{}' does not match the registered name",
                options.task_name
            ),
            "set task_options.task_name to the name the task is registered with",
        ));
    }
    if let Some(Err(error)) = options
        .retry_policy
        .as_ref()
        .map(|policy| policy.validate())
    {
        refusals.push(option_error(
            name,
            format!("invalid retry policy: {error}"),
            "give the retry policy a valid backoff strategy and intervals",
        ));
    }
    match (&options.retry_policy, &options.retry_for) {
        (_, Some(codes)) if codes.is_empty() => refusals.push(option_error(
            name,
            "retry_for must not be empty".to_owned(),
            "list at least one error code that retries the task",
        )),
        (Some(_), None) => refusals.push(option_error(
            name,
            "retry_policy requires retry_for".to_owned(),
            "list the error codes that retry the task in retry_for",
        )),
        (None, None) | (_, Some(_)) => {}
    }
    refusals.extend(retry_code_refusals(name, options));
    refusals.extend(class_refusal(name, options));
    refusals
}

/// The refusal of each `retry_for` code whose text the retry code rule
/// refuses (`check_error_code_text`), with Python's text. Python checks the
/// text of each code, a built-in code's value included, when the
/// `RetryPolicy` is built (`validate_error_code_fields`). Pure.
fn retry_code_refusals(name: &str, options: &TaskOptions) -> Vec<HorsiesError> {
    options
        .retry_for
        .iter()
        .flatten()
        .filter_map(|code| {
            let text = code.to_string();
            check_error_code_text(&text).err().map(|refusal| {
                option_error(
                    name,
                    refusal.message("retry_for", &text),
                    "name each retry_for code in UPPER_SNAKE_CASE, as the codes of a \
                     CustomErrorCode type are named",
                )
            })
        })
        .collect()
}

/// The refusal of a class without its settings: a shared class needs
/// `timeout_ms`; `SHARED_ASYNC` carries `cancel_grace_ms`, and it must; an
/// `ISOLATED` task may carry one (the kill after an operator cancel);
/// `SHARED_THREAD` carries none, since a thread cannot be stopped. Pure.
fn class_refusal(name: &str, options: &TaskOptions) -> Option<HorsiesError> {
    let class = options.execution.as_str();
    match (
        options.execution,
        options.timeout_ms,
        options.cancel_grace_ms,
    ) {
        (Execution::SharedAsync | Execution::SharedThread, None, _) => Some(option_error(
            name,
            format!("a {class} task needs timeout_ms"),
            "set timeout_ms: a shared child holds the deadline of each of its runs",
        )),
        (Execution::SharedAsync, Some(_), None) => Some(option_error(
            name,
            format!("a {class} task needs cancel_grace_ms"),
            "set cancel_grace_ms, 100 to 10,000 ms (the task attribute sets 1,000)",
        )),
        (Execution::SharedThread, Some(_), Some(_)) => Some(option_error(
            name,
            format!(
                "cancel_grace_ms is for SHARED_ASYNC and ISOLATED tasks; this task is {class}, \
                 whose thread cannot be stopped"
            ),
            "remove cancel_grace_ms, or run the task as SHARED_ASYNC or ISOLATED",
        )),
        (Execution::SharedAsync, Some(_), Some(_))
        | (Execution::Isolated, _, _)
        | (Execution::SharedThread, Some(_), None) => None,
    }
}

fn option_error(name: &str, message: String, help: &str) -> HorsiesError {
    HorsiesError::new(message)
        .with_code(HorsiesErrorCode::TaskInvalidOptions)
        .with_note(format!("task '{name}'"))
        .with_help(help)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use horsies_core::core::task::fn_trait::{BlockingTaskFn, RawTaskResult, TaskMetadata};
    use horsies_core::{
        CustomCode, RetryPolicy, TaskErrorCode, TaskResult, DEFAULT_CANCEL_GRACE_MS,
    };

    use super::*;
    use crate::AppConfig;

    struct EchoTask;

    impl BlockingTaskFn for EchoTask {
        fn execute(&self, input: &[u8]) -> RawTaskResult {
            TaskResult::Ok(input.to_vec())
        }
    }

    fn options(name: &str) -> TaskOptions {
        TaskOptions {
            task_name: name.to_owned(),
            queue: None,
            good_until: None,
            retry_for: None,
            retry_policy: None,
            timeout_ms: None,
            execution: Execution::Isolated,
            cancel_grace_ms: None,
        }
    }

    fn task(options: TaskOptions) -> RegisteredTask {
        RegisteredTask::Blocking {
            task: Arc::new(EchoTask),
            metadata: TaskMetadata::default(),
        }
        .with_task_options(options)
    }

    fn refusal_texts(name: &str, options: &TaskOptions) -> Vec<String> {
        options_refusals(name, options)
            .into_iter()
            .map(|error| error.message)
            .collect()
    }

    #[test]
    fn valid_options_give_no_refusal() {
        assert!(refusal_texts("echo", &options("echo")).is_empty());
        let shared = TaskOptions {
            execution: Execution::SharedAsync,
            timeout_ms: Some(5_000),
            cancel_grace_ms: Some(DEFAULT_CANCEL_GRACE_MS),
            ..options("echo")
        };
        assert!(refusal_texts("echo", &shared).is_empty());
    }

    /// Python `check(live=True)` runs the static check first: its refusal
    /// returns before any connect, so an address with no server gives the
    /// refusal of the static check, not the connectivity refusal.
    #[tokio::test]
    async fn the_live_check_refuses_with_the_static_check_before_any_connect() {
        let mut app = Horsies::new(AppConfig::from_database_url(
            "postgresql://postgres@127.0.0.1:1/horsies",
        ))
        .unwrap();
        let policy_only = TaskOptions {
            retry_policy: Some(RetryPolicy::fixed(vec![1], false).unwrap()),
            ..options("bad_retry")
        };
        app.register("bad_retry", task(policy_only)).unwrap();
        let report = app.check_live().await.unwrap_err();
        assert_eq!(report.phase_name(), CHECK);
        let messages: Vec<&str> = report
            .refusals()
            .iter()
            .map(|error| error.message.as_str())
            .collect();
        assert!(
            messages.contains(&"retry_policy requires retry_for"),
            "{messages:?}"
        );
        assert!(
            report
                .refusals()
                .iter()
                .all(|error| error.code != Some(HorsiesErrorCode::DatabaseInvalidUrl)),
            "{report}"
        );
    }

    #[test]
    fn a_task_name_that_differs_from_the_registration_is_refused() {
        let texts = refusal_texts("echo", &options("other"));
        assert_eq!(texts.len(), 1);
        assert!(texts[0].contains("'other' does not match"));
    }

    #[test]
    fn retry_codes_and_policy_go_together() {
        let empty = TaskOptions {
            retry_for: Some(Vec::new()),
            ..options("echo")
        };
        assert_eq!(
            refusal_texts("echo", &empty),
            vec!["retry_for must not be empty"]
        );
        let policy_only = TaskOptions {
            retry_policy: Some(RetryPolicy::fixed(vec![1], false).unwrap()),
            ..options("echo")
        };
        assert_eq!(
            refusal_texts("echo", &policy_only),
            vec!["retry_policy requires retry_for"]
        );
        let both = TaskOptions {
            retry_for: Some(vec![TaskErrorCode::from(
                horsies_core::OutcomeCode::TaskTimeout,
            )]),
            ..policy_only
        };
        assert!(refusal_texts("echo", &both).is_empty());
    }

    /// A custom code accepts any text that is not a built-in or reserved name
    /// (`CustomCode::parse`, Python's `TaskError` rule); `retry_for`
    /// takes only the codes of the retry code rule, as Python's
    /// `RetryPolicy` does.
    #[test]
    fn a_retry_code_outside_the_retry_code_rule_is_refused_with_python_s_text() {
        let codes: Vec<TaskErrorCode> = ["my_error", "RateLimitError", "", "RATE_LIMITED"]
            .into_iter()
            .map(|text| TaskErrorCode::Custom(CustomCode::parse(text).expect("a custom code")))
            .chain([TaskErrorCode::from(horsies_core::OutcomeCode::TaskTimeout)])
            .collect();
        let refused = TaskOptions {
            retry_policy: Some(RetryPolicy::fixed(vec![1], false).unwrap()),
            retry_for: Some(codes),
            ..options("echo")
        };
        assert_eq!(
            refusal_texts("echo", &refused),
            vec![
                "retry_for 'my_error' is invalid; expected UPPER_SNAKE_CASE (e.g. \
                 RATE_LIMITED)",
                "retry_for 'RateLimitError' looks like an exception class name; retry \
                 matching is error-code-only, use UPPER_SNAKE_CASE code names",
                "retry_for must be a non-empty string, got ''",
            ]
        );
        let error = &options_refusals("echo", &refused)[0];
        assert_eq!(error.code, Some(HorsiesErrorCode::TaskInvalidOptions));
        assert_eq!(error.notes, vec!["task 'echo'"]);
    }

    #[test]
    fn each_class_needs_its_settings() {
        let cases = [
            (Execution::SharedThread, None, None, "needs timeout_ms"),
            (Execution::SharedAsync, None, None, "needs timeout_ms"),
            (
                Execution::SharedAsync,
                Some(1_000),
                None,
                "needs cancel_grace_ms",
            ),
            (
                Execution::SharedThread,
                Some(1_000),
                Some(DEFAULT_CANCEL_GRACE_MS),
                "is for SHARED_ASYNC and ISOLATED tasks",
            ),
        ];
        for (execution, timeout_ms, cancel_grace_ms, expected) in cases {
            let refused = TaskOptions {
                execution,
                timeout_ms,
                cancel_grace_ms,
                ..options("echo")
            };
            let texts = refusal_texts("echo", &refused);
            assert_eq!(texts.len(), 1, "{execution:?}");
            assert!(texts[0].contains(expected), "{}", texts[0]);
        }
    }

    /// An `ISOLATED` task takes a cancel grace (the kill after an operator
    /// cancel), or none.
    #[test]
    fn an_isolated_task_takes_a_cancel_grace_or_none() {
        for cancel_grace_ms in [Some(DEFAULT_CANCEL_GRACE_MS), None] {
            let accepted = TaskOptions {
                execution: Execution::Isolated,
                timeout_ms: None,
                cancel_grace_ms,
                ..options("echo")
            };
            assert!(
                refusal_texts("echo", &accepted).is_empty(),
                "{cancel_grace_ms:?}"
            );
        }
    }

    #[test]
    fn the_refusals_of_several_tasks_come_in_name_order() {
        let tasks = [
            ("zeta", task(options("wrong"))),
            ("alpha", task(options("also_wrong"))),
            ("plain", task(options("plain"))),
        ];
        let refusals = task_option_refusals(tasks.iter().map(|(name, task)| (*name, task)));
        let notes: Vec<&str> = refusals
            .iter()
            .map(|error| error.notes[0].as_str())
            .collect();
        assert_eq!(notes, vec!["task 'alpha'", "task 'zeta'"]);
    }

    #[test]
    fn an_app_with_no_task_passes_and_counts_none() {
        let app = Horsies::new(AppConfig::from_database_url(
            "postgresql://localhost/horsies",
        ))
        .unwrap();
        let passed = app.check().unwrap();
        assert_eq!(passed.task_count(), 0);
        assert!(passed.declared_error_codes().is_empty());
        assert_eq!(
            passed.to_string(),
            "ok: all validations passed\n  0 task(s) registered\n  declared error codes: none"
        );
    }

    #[test]
    fn the_check_reports_the_declared_codes_of_each_task_in_name_order() {
        let mut app = Horsies::new(AppConfig::from_database_url(
            "postgresql://localhost/horsies",
        ))
        .unwrap();
        let declaring = |codes: &'static [&'static str]| RegisteredTask::Blocking {
            task: Arc::new(EchoTask),
            metadata: TaskMetadata::default().with_error_codes(codes),
        };
        app.register("resize", declaring(&[])).unwrap();
        app.register("send_mail", declaring(&["BOUNCED"])).unwrap();
        app.register("charge", declaring(&["RATE_LIMITED", "CARD_DECLINED"]))
            .unwrap();
        let passed = app.check().unwrap();
        assert_eq!(passed.task_count(), 3);
        assert_eq!(
            passed.to_string(),
            "ok: all validations passed\n  3 task(s) registered\n  declared error codes:\n    \
             charge: RATE_LIMITED, CARD_DECLINED\n    send_mail: BOUNCED"
        );
    }

    #[test]
    fn a_report_names_its_phase_and_each_refusal() {
        let report = ValidationReport::of(
            "worker-startup",
            vec![HorsiesError::new("first"), HorsiesError::new("second")],
        );
        assert_eq!(report.phase_name(), "worker-startup");
        assert_eq!(report.refusals().len(), 2);
        assert!(report
            .to_string()
            .ends_with("aborting due to 2 previous errors"));
    }

    #[test]
    fn the_scope_follows_the_role() {
        assert_eq!(
            CheckScope::of_role(AppRole::Worker),
            CheckScope::WithoutSchedules
        );
        assert_eq!(
            CheckScope::of_role(AppRole::Producer),
            CheckScope::WithSchedules
        );
    }

    #[test]
    fn a_listener_that_does_not_deliver_is_python_s_listener_refusal() {
        let refused = ConnectivityRefusal::ListenerNotCapable(
            horsies_core::ListenProbeFailure::Interrupted(horsies_core::CheckInterruption::Stopped),
        );
        let report = refusal_report(&refused);
        let error = &report.refusals()[0];
        assert_eq!(error.message, "broker listener capability check failed");
        assert_eq!(error.code, Some(HorsiesErrorCode::DatabaseInvalidUrl));
        assert!(error.notes[0].contains("transaction-pooled"));
        assert_eq!(error.notes[1], "the listener stopped during the probe");
    }

    #[test]
    fn the_live_check_handle_is_observe_only_whatever_the_mode_of_the_app() {
        let config = AppConfig::from_database_url("postgresql://localhost/horsies");
        let app =
            Horsies::with_schema_mode(config.clone(), SchemaMode::MigrateAndValidate).unwrap();
        assert_eq!(app.handle().schema_mode(), SchemaMode::MigrateAndValidate);
        let handle = live_check_handle(app.config()).unwrap();
        assert_eq!(handle.schema_mode(), SchemaMode::ObserveOnly);
    }

    #[test]
    fn a_failed_connect_is_python_s_connectivity_refusal() {
        let report = connectivity_report("connection refused");
        assert_eq!(
            report.refusals()[0].code,
            Some(HorsiesErrorCode::DatabaseInvalidUrl)
        );
        assert_eq!(report.refusals()[0].notes, vec!["connection refused"]);
    }
}

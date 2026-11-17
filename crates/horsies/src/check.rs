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
//!    task attribute checks at compile time, for a task registered by hand.
//! 3. Schedules (not for a worker, which never sends a scheduled task): per
//!    enabled schedule, its task, its queue, its input read back with the
//!    task's input type, and the input size.
//!
//! The live check adds, after a passing static check:
//!
//! 4. Connectivity: a connect of the app's handle and one `SELECT 1`.
//! 5. Worker connections: each worker profile against the server's
//!    connection limit (none through a transaction pooler).

use horsies_core::{check_worker_connections, Execution, RegisteredTask, TaskOptions};

use crate::{AppRole, ErrorCode, Horsies, HorsiesError, ValidationReport};

/// The check passed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckPassed {
    task_count: usize,
}

impl CheckPassed {
    /// The number of registered tasks.
    pub fn task_count(&self) -> usize {
        self.task_count
    }
}

impl std::fmt::Display for CheckPassed {
    /// Python's success text of `horsies check`.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "ok: all validations passed\n  {} task(s) registered",
            self.task_count
        )
    }
}

/// The refusals of the first phase of the check that refused, each with
/// its code, notes and help.
#[derive(Debug, Clone)]
pub struct CheckReport {
    report: ValidationReport,
}

impl CheckReport {
    /// A report of `errors`, which hold at least one refusal.
    pub(crate) fn of(phase: &str, errors: Vec<HorsiesError>) -> Self {
        assert!(!errors.is_empty(), "a check report holds a refusal");
        let mut report = ValidationReport::new(phase);
        for error in errors {
            report.add(error);
        }
        Self { report }
    }

    /// The refusals, in the order the phase found them.
    pub fn errors(&self) -> &[HorsiesError] {
        &self.report.errors
    }

    /// The phase that refused: `check`, `worker-startup` or
    /// `scheduler-startup`.
    pub fn phase(&self) -> &str {
        &self.report.phase_name
    }
}

impl std::fmt::Display for CheckReport {
    /// Each refusal, then the count (Python `format_rust_style`).
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.report)
    }
}

impl std::error::Error for CheckReport {}

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
    /// Errors: [`CheckReport`] with the refusals of the first phase that
    /// refused.
    pub fn check(&self) -> Result<CheckPassed, CheckReport> {
        self.check_scope(CheckScope::of_role(self.role()), CHECK)
    }

    /// The static check, then the connectivity and the worker connections
    /// (Python `check(live=True)`). Connects the app's handle and reads;
    /// writes nothing.
    ///
    /// Errors: [`CheckReport`] with the refusals of the first phase that
    /// refused.
    pub async fn check_live(&self) -> Result<CheckPassed, CheckReport> {
        let passed = self.check()?;
        let broker = match self.handle().broker().await {
            Ok(broker) => broker,
            Err(error) => return Err(connectivity_report(&error.to_string())),
        };
        if let Err(error) = broker.ping_database().await {
            return Err(connectivity_report(&error.to_string()));
        }
        match check_worker_connections(self.config(), &broker).await {
            errors if errors.is_empty() => Ok(passed),
            errors => Err(CheckReport::of(CHECK, errors)),
        }
    }

    /// The static check with the phases of `scope`; a refusal names
    /// `phase`.
    pub(crate) fn check_scope(
        &self,
        scope: CheckScope,
        phase: &'static str,
    ) -> Result<CheckPassed, CheckReport> {
        let workflow_errors = self.workflow_check_findings();
        if !workflow_errors.is_empty() {
            return Err(CheckReport::of(phase, workflow_errors));
        }
        let option_errors = task_option_refusals(self.tasks().iter());
        if !option_errors.is_empty() {
            return Err(CheckReport::of(phase, option_errors));
        }
        let schedule_errors = match (scope, &self.config().schedule) {
            (CheckScope::WithoutSchedules, _) | (CheckScope::WithSchedules, None) => Vec::new(),
            (CheckScope::WithSchedules, Some(schedule)) => self.schedule_refusals(schedule),
        };
        match schedule_errors.is_empty() {
            true => Ok(CheckPassed {
                task_count: self.tasks().len(),
            }),
            false => Err(CheckReport::of(phase, schedule_errors)),
        }
    }
}

/// The report of a failed connect or ping (Python
/// `_check_broker_connectivity`). Pure.
pub(crate) fn connectivity_report(cause: &str) -> CheckReport {
    CheckReport::of(
        CHECK,
        vec![HorsiesError::new("broker connectivity check failed")
            .with_code(ErrorCode::BrokerInvalidUrl)
            .with_note(cause.to_owned())
            .with_help("check database_url in PostgresConfig")],
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
    match (&options.retry_policy, &options.auto_retry_for) {
        (_, Some(codes)) if codes.is_empty() => refusals.push(option_error(
            name,
            "auto_retry_for must not be empty".to_owned(),
            "list at least one error code that retries the task",
        )),
        (Some(_), None) => refusals.push(option_error(
            name,
            "retry_policy requires auto_retry_for".to_owned(),
            "list the error codes that retry the task in auto_retry_for",
        )),
        (None, None) | (_, Some(_)) => {}
    }
    refusals.extend(class_refusal(name, options));
    refusals
}

/// The refusal of a class without its settings: a shared class needs
/// `timeout_ms`; only `SHARED_ASYNC` carries `cancel_grace_ms`, and it
/// must. Pure.
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
        (Execution::Isolated | Execution::SharedThread, _, Some(_)) => Some(option_error(
            name,
            format!("cancel_grace_ms is for SHARED_ASYNC tasks; this task is {class}"),
            "remove cancel_grace_ms, or run the task as SHARED_ASYNC",
        )),
        (Execution::SharedAsync, Some(_), Some(_))
        | (Execution::Isolated, _, None)
        | (Execution::SharedThread, Some(_), None) => None,
    }
}

fn option_error(name: &str, message: String, help: &str) -> HorsiesError {
    HorsiesError::new(message)
        .with_code(ErrorCode::TaskInvalidOptions)
        .with_note(format!("task '{name}'"))
        .with_help(help)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use horsies_core::core::task::fn_trait::{BlockingTaskFn, RawTaskResult, TaskMeta};
    use horsies_core::{CancelGraceMs, RetryPolicy, TaskErrorCode, TaskResult};

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
            queue_name: None,
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
            timeout_ms: None,
            execution: Execution::Isolated,
            cancel_grace_ms: None,
        }
    }

    fn task(options: TaskOptions) -> RegisteredTask {
        RegisteredTask::Blocking {
            task: Arc::new(EchoTask),
            meta: TaskMeta::default(),
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
            cancel_grace_ms: Some(CancelGraceMs::DEFAULT),
            ..options("echo")
        };
        assert!(refusal_texts("echo", &shared).is_empty());
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
            auto_retry_for: Some(Vec::new()),
            ..options("echo")
        };
        assert_eq!(
            refusal_texts("echo", &empty),
            vec!["auto_retry_for must not be empty"]
        );
        let policy_only = TaskOptions {
            retry_policy: Some(RetryPolicy::fixed(vec![1], false).unwrap()),
            ..options("echo")
        };
        assert_eq!(
            refusal_texts("echo", &policy_only),
            vec!["retry_policy requires auto_retry_for"]
        );
        let both = TaskOptions {
            auto_retry_for: Some(vec![TaskErrorCode::from(
                horsies_core::OutcomeCode::TaskTimeout,
            )]),
            ..policy_only
        };
        assert!(refusal_texts("echo", &both).is_empty());
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
                Execution::Isolated,
                None,
                Some(CancelGraceMs::DEFAULT),
                "is for SHARED_ASYNC tasks",
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
        let app = Horsies::new(AppConfig::for_database_url(
            "postgresql://localhost/horsies",
        ))
        .unwrap();
        let passed = app.check().unwrap();
        assert_eq!(passed.task_count(), 0);
        assert_eq!(
            passed.to_string(),
            "ok: all validations passed\n  0 task(s) registered"
        );
    }

    #[test]
    fn a_report_names_its_phase_and_each_refusal() {
        let report = CheckReport::of(
            "worker-startup",
            vec![HorsiesError::new("first"), HorsiesError::new("second")],
        );
        assert_eq!(report.phase(), "worker-startup");
        assert_eq!(report.errors().len(), 2);
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
    fn a_failed_connect_is_python_s_connectivity_refusal() {
        let report = connectivity_report("connection refused");
        assert_eq!(report.errors()[0].code, Some(ErrorCode::BrokerInvalidUrl));
        assert_eq!(report.errors()[0].notes, vec!["connection refused"]);
    }
}

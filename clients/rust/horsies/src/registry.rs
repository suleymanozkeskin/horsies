//! The registration rules of the app's tasks and queues.
//!
//! A task name is unique. The queue of a task follows the queue mode: in
//! `Default` mode a task names no queue and runs on `default`; in `Custom`
//! mode it names one of the configured queues (Python
//! `Horsies.validate_queue_name`). Each registered task carries its resolved
//! queue and priority, which sends and workflow nodes inherit.
//!
//! The custom error codes that a task declares (`CustomErrorCode::CODES` of its
//! code type, which the task macros fill) follow the rules of a declared
//! set (`DeclaredErrorCodes::parse`). The cancel grace of a task's options is
//! checked here, once, against its bounds (`check_cancel_grace_ms`).
//!
//! Every function is pure: it reads the config and changes only the registry
//! it is given.

use horsies_core::core::task::error::CodeTextRefusal;
use horsies_core::{
    check_cancel_grace_ms, DeclaredCodeRefusal, DeclaredErrorCodes, RegisteredTask, TaskRegistry,
};

use crate::{AppConfig, ErrorCode, HorsiesError, QueueMode, ValidationReport};

/// The one queue of `Default` mode.
pub(crate) const DEFAULT_QUEUE: &str = "default";

/// The priority of the default queue, and of a task that resolves no queue
/// priority (Python's `100`).
pub(crate) const DEFAULT_PRIORITY: u32 = 100;

/// Where a registration takes the queue of its task from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueueSource<'a> {
    /// The `queue` of the task's options; no options name no queue.
    TaskOptions,
    /// The queue that the registration call names.
    Named(&'a str),
}

/// Register `task` as `name` in `tasks`, with the queue that `source` gives
/// and its priority.
///
/// Refuses: a definition-time `good_until` (`TaskInvalidOptions`); a
/// `cancel_grace_ms` outside its bounds (`TaskCancelGraceOutOfRange`); a set
/// of declared error codes that breaks a rule of a declared set
/// (`CheckReservedCodeCollision`, `TaskInvalidErrorCode`,
/// `TaskDuplicateErrorCode`; [`refuse_bad_error_codes`]); a queue that the
/// queue mode does not admit (`ConfigInvalidQueueMode`, `TaskInvalidQueue`);
/// an empty or repeated name (the core registry's refusals). After a
/// refusal `tasks` is unchanged.
pub(crate) fn register_task(
    tasks: &mut TaskRegistry,
    config: &AppConfig,
    name: String,
    mut task: RegisteredTask,
    source: QueueSource<'_>,
) -> Result<(), HorsiesError> {
    refuse_definition_deadline(&name, &task)?;
    refuse_cancel_grace_out_of_range(&name, &task)?;
    refuse_bad_error_codes(&name, &task)?;
    let requested = match source {
        QueueSource::TaskOptions => task
            .task_options()
            .and_then(|options| options.queue.clone()),
        QueueSource::Named(queue) => Some(queue.to_owned()),
    };
    let queue = resolve_task_queue(config, requested.as_deref())?;
    let priority = queue_priority(config, &queue);
    task.set_queue(queue);
    task.set_priority(priority);
    tasks.register(name, task)
}

/// The queue of a task that asks for `requested` (Python
/// `Horsies.validate_queue_name`).
///
/// - `Default` mode: no queue is `default`; any named queue is refused with
///   `ConfigInvalidQueueMode`, `default` too, as in Python.
/// - `Custom` mode: no queue is refused with `ConfigInvalidQueueMode`; a
///   queue that is not configured is refused with `TaskInvalidQueue`.
pub(crate) fn resolve_task_queue(
    config: &AppConfig,
    requested: Option<&str>,
) -> Result<String, HorsiesError> {
    match (config.queue_mode, requested) {
        (QueueMode::Default, None) => Ok(DEFAULT_QUEUE.to_owned()),
        (QueueMode::Default, Some(queue)) => Err(HorsiesError::new(
            "cannot specify a queue in Default queue mode",
        )
        .with_code(ErrorCode::ConfigInvalidQueueMode)
        .with_note(format!("queue '{queue}' was specified"))
        .with_note("the app is configured with QueueMode::Default")
        .with_help("either remove the queue or switch to QueueMode::Custom")),
        (QueueMode::Custom, None) => Err(HorsiesError::new(
            "a queue is required in Custom queue mode",
        )
        .with_code(ErrorCode::ConfigInvalidQueueMode)
        .with_note("the app is configured with QueueMode::Custom")
        .with_help("name one of the configured queues")),
        (QueueMode::Custom, Some(queue)) => match is_configured_queue(config, queue) {
            true => Ok(queue.to_owned()),
            false => Err(unknown_queue(config, queue)),
        },
    }
}

/// The queue names that the app admits: `default` in `Default` mode, the
/// configured queues in `Custom` mode.
pub(crate) fn valid_queue_names(config: &AppConfig) -> Vec<String> {
    match config.queue_mode {
        QueueMode::Default => vec![DEFAULT_QUEUE.to_owned()],
        QueueMode::Custom => configured_queues(config)
            .map(|queue| queue.name.clone())
            .collect(),
    }
}

/// Whether the app admits `queue` as the queue of a send. Refuses with
/// `TaskInvalidQueue` and the valid names.
pub(crate) fn check_queue(config: &AppConfig, queue: &str) -> Result<(), HorsiesError> {
    let admitted = match config.queue_mode {
        QueueMode::Default => queue == DEFAULT_QUEUE,
        QueueMode::Custom => is_configured_queue(config, queue),
    };
    match admitted {
        true => Ok(()),
        false => Err(unknown_queue(config, queue)),
    }
}

/// The priority of `queue`: its configured priority in `Custom` mode, else
/// [`DEFAULT_PRIORITY`].
pub(crate) fn queue_priority(config: &AppConfig, queue: &str) -> u32 {
    configured_queues(config)
        .find(|configured| configured.name == queue)
        .map_or(DEFAULT_PRIORITY, |configured| configured.priority)
}

fn configured_queues(config: &AppConfig) -> impl Iterator<Item = &crate::CustomQueueConfig> {
    config.custom_queues.iter().flatten()
}

fn is_configured_queue(config: &AppConfig, queue: &str) -> bool {
    configured_queues(config).any(|configured| configured.name == queue)
}

fn unknown_queue(config: &AppConfig, queue: &str) -> HorsiesError {
    HorsiesError::new(format!("invalid queue '{queue}'"))
        .with_code(ErrorCode::TaskInvalidQueue)
        .with_note(format!("valid queues: {:?}", valid_queue_names(config)))
        .with_help("use one of the configured queue names")
}

/// A task deadline belongs to a send or a workflow node, never to the task
/// definition: a deadline fixed at registration would be in the past for
/// every later send.
fn refuse_definition_deadline(name: &str, task: &RegisteredTask) -> Result<(), HorsiesError> {
    match task.task_options().and_then(|options| options.good_until) {
        None => Ok(()),
        Some(_) => Err(HorsiesError::new(format!(
            "task '{name}' sets good_until in its definition"
        ))
        .with_code(ErrorCode::TaskInvalidOptions)
        .with_help(
            "set good_until per send with TaskSendOptions, or per workflow node with \
             good_until on the node",
        )),
    }
}

/// The cancel grace of `task`'s options is from `MIN_CANCEL_GRACE_MS` to
/// `MAX_CANCEL_GRACE_MS` (Python `_check_cancel_grace`). A task with no
/// options or no grace passes. The task attribute checks the bounds at
/// compile time; a hand-built `TaskOptions` is checked here.
///
/// Errors: `TaskCancelGraceOutOfRange` (`HRS-111`), with the bounds.
fn refuse_cancel_grace_out_of_range(name: &str, task: &RegisteredTask) -> Result<(), HorsiesError> {
    let Some(options) = task.task_options() else {
        return Ok(());
    };
    match options.cancel_grace_ms.map(check_cancel_grace_ms) {
        None | Some(Ok(_)) => Ok(()),
        Some(Err(refused)) => Err(HorsiesError::new("cancel_grace_ms is out of range")
            .with_code(ErrorCode::TaskCancelGraceOutOfRange)
            .with_note(format!("task '{name}'"))
            .with_note(refused.to_string())
            .with_note(format!("execution is {}", options.execution))
            .with_help("set a value in the range, or remove cancel_grace_ms")),
    }
}

/// The error codes that `task` declares follow the rules of a declared
/// set: the code-text rule, no built-in or reserved name, each code once.
/// A derived code type keeps the last two rules at compile time; a code
/// type whose variant name does not give an `UPPER_SNAKE_CASE` code (for
/// example a leading `_`) or a hand-written impl can break one.
///
/// Errors: one error per refused code, with the HRS code of its refusal
/// (`DeclaredCodeRefusal::error_code`); several refusals come in one error
/// (`ValidationReport::into_result`).
fn refuse_bad_error_codes(name: &str, task: &RegisteredTask) -> Result<(), HorsiesError> {
    match DeclaredErrorCodes::parse(task.error_codes()) {
        Ok(_declared) => Ok(()),
        Err(refused) => {
            let mut report = ValidationReport::new("task registration");
            for refusal in refused.refusals() {
                report.add(declared_code_error(name, refusal));
            }
            report.into_result()
        }
    }
}

/// The registration error of one refused declared code.
fn declared_code_error(name: &str, refusal: &DeclaredCodeRefusal) -> HorsiesError {
    let help = match refusal {
        DeclaredCodeRefusal::Text {
            refusal: CodeTextRefusal::ReservedName,
            ..
        }
        | DeclaredCodeRefusal::BuiltInName { .. } => {
            "rename the code: a built-in or reserved name is not a custom code; derive the code \
             type with #[derive(horsies::CustomErrorCode)]"
        }
        DeclaredCodeRefusal::Text {
            refusal:
                CodeTextRefusal::Empty
                | CodeTextRefusal::ExceptionClassName
                | CodeTextRefusal::NotUpperSnakeCase,
            ..
        } => "give the code an UPPER_SNAKE_CASE name, for example RATE_LIMITED",
        DeclaredCodeRefusal::Repeated { .. } => "list each code once in CustomErrorCode::CODES",
    };
    HorsiesError::new(format!("task '{name}': {refusal}"))
        .with_code(refusal.error_code())
        .with_note("the error codes of a task are CustomErrorCode::CODES of its code type")
        .with_help(help)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use horsies_core::core::task::fn_trait::{BlockingTaskFn, RawTaskResult, TaskMeta};
    use horsies_core::core::task::result::TaskResult;
    use horsies_core::TaskOptions;

    use super::*;
    use crate::CustomQueueConfig;

    struct EchoTask;

    impl BlockingTaskFn for EchoTask {
        fn execute(&self, input: &[u8]) -> RawTaskResult {
            TaskResult::Ok(input.to_vec())
        }
    }

    fn plain_task() -> RegisteredTask {
        RegisteredTask::Blocking {
            task: Arc::new(EchoTask),
            meta: TaskMeta::default(),
        }
    }

    fn task_with_options(queue: Option<&str>, good_until: bool) -> RegisteredTask {
        plain_task().with_task_options(TaskOptions {
            task_name: "echo".to_owned(),
            queue: queue.map(str::to_owned),
            good_until: good_until.then(chrono::Utc::now),
            retry_for: None,
            retry_policy: None,
            timeout_ms: None,
            execution: horsies_core::Execution::Isolated,
            cancel_grace_ms: None,
        })
    }

    fn default_config() -> AppConfig {
        AppConfig::for_database_url("postgresql://localhost/horsies")
    }

    fn custom_config() -> AppConfig {
        AppConfig {
            queue_mode: QueueMode::Custom,
            custom_queues: Some(vec![
                CustomQueueConfig {
                    name: "critical".to_owned(),
                    priority: 1,
                    max_concurrency: None,
                },
                CustomQueueConfig {
                    name: "bulk".to_owned(),
                    priority: 90,
                    max_concurrency: Some(4),
                },
            ]),
            ..default_config()
        }
    }

    #[test]
    fn default_mode_without_a_queue_resolves_default_with_priority_100() {
        let mut tasks = TaskRegistry::new();
        register_task(
            &mut tasks,
            &default_config(),
            "echo".to_owned(),
            plain_task(),
            QueueSource::TaskOptions,
        )
        .unwrap();
        let task = tasks.get("echo").unwrap();
        assert_eq!(task.queue(), Some(DEFAULT_QUEUE));
        assert_eq!(task.priority(), Some(DEFAULT_PRIORITY));
    }

    #[test]
    fn default_mode_refuses_any_named_queue() {
        for queue in ["default", "critical"] {
            let refused = resolve_task_queue(&default_config(), Some(queue)).unwrap_err();
            assert_eq!(
                refused.code,
                Some(ErrorCode::ConfigInvalidQueueMode),
                "{queue}"
            );
        }
    }

    #[test]
    fn custom_mode_refuses_a_task_without_a_queue() {
        let refused = resolve_task_queue(&custom_config(), None).unwrap_err();
        assert_eq!(refused.code, Some(ErrorCode::ConfigInvalidQueueMode));
    }

    #[test]
    fn custom_mode_refuses_an_unknown_queue_and_names_the_valid_ones() {
        let refused = resolve_task_queue(&custom_config(), Some("other")).unwrap_err();
        assert_eq!(refused.code, Some(ErrorCode::TaskInvalidQueue));
        assert!(refused.notes.iter().any(|note| note.contains("critical")));
    }

    #[test]
    fn custom_mode_takes_the_queue_of_the_task_options_and_its_priority() {
        let mut tasks = TaskRegistry::new();
        register_task(
            &mut tasks,
            &custom_config(),
            "echo".to_owned(),
            task_with_options(Some("bulk"), false),
            QueueSource::TaskOptions,
        )
        .unwrap();
        let task = tasks.get("echo").unwrap();
        assert_eq!(task.queue(), Some("bulk"));
        assert_eq!(task.priority(), Some(90));
    }

    #[test]
    fn a_named_queue_replaces_the_queue_of_the_task_options() {
        let mut tasks = TaskRegistry::new();
        register_task(
            &mut tasks,
            &custom_config(),
            "echo".to_owned(),
            task_with_options(Some("bulk"), false),
            QueueSource::Named("critical"),
        )
        .unwrap();
        assert_eq!(tasks.get("echo").unwrap().queue(), Some("critical"));
    }

    #[test]
    fn a_definition_deadline_is_refused_and_nothing_is_registered() {
        let mut tasks = TaskRegistry::new();
        let refused = register_task(
            &mut tasks,
            &default_config(),
            "echo".to_owned(),
            task_with_options(None, true),
            QueueSource::TaskOptions,
        )
        .unwrap_err();
        assert_eq!(refused.code, Some(ErrorCode::TaskInvalidOptions));
        assert!(tasks.is_empty());
    }

    fn task_with_grace(execution: horsies_core::Execution, grace: u32) -> RegisteredTask {
        plain_task().with_task_options(TaskOptions {
            task_name: "echo".to_owned(),
            queue: None,
            good_until: None,
            retry_for: None,
            retry_policy: None,
            timeout_ms: Some(5_000),
            execution,
            cancel_grace_ms: Some(grace),
        })
    }

    /// The registration checks the cancel grace once: a grace outside 100 to
    /// 10,000 ms is `HRS-111`, and nothing is registered.
    #[test]
    fn a_cancel_grace_outside_its_bounds_is_refused_and_nothing_is_registered() {
        for (execution, grace) in [
            (horsies_core::Execution::SharedAsync, 99),
            (horsies_core::Execution::Isolated, 10_001),
        ] {
            let mut tasks = TaskRegistry::new();
            let refused = register_task(
                &mut tasks,
                &default_config(),
                "echo".to_owned(),
                task_with_grace(execution, grace),
                QueueSource::TaskOptions,
            )
            .unwrap_err();
            assert_eq!(
                refused.code,
                Some(ErrorCode::TaskCancelGraceOutOfRange),
                "{execution} {grace}"
            );
            assert!(tasks.is_empty());
        }
    }

    #[test]
    fn a_cancel_grace_at_its_bounds_registers() {
        for grace in [100, 10_000] {
            let mut tasks = TaskRegistry::new();
            register_task(
                &mut tasks,
                &default_config(),
                "echo".to_owned(),
                task_with_grace(horsies_core::Execution::SharedAsync, grace),
                QueueSource::TaskOptions,
            )
            .unwrap();
            assert_eq!(
                tasks
                    .get("echo")
                    .unwrap()
                    .task_options()
                    .and_then(|options| options.cancel_grace_ms),
                Some(grace)
            );
        }
    }

    #[test]
    fn a_repeated_name_is_refused() {
        let mut tasks = TaskRegistry::new();
        let config = default_config();
        register_task(
            &mut tasks,
            &config,
            "echo".to_owned(),
            plain_task(),
            QueueSource::TaskOptions,
        )
        .unwrap();
        let refused = register_task(
            &mut tasks,
            &config,
            "echo".to_owned(),
            plain_task(),
            QueueSource::TaskOptions,
        )
        .unwrap_err();
        assert_eq!(refused.code, Some(ErrorCode::TaskDuplicateName));
    }

    fn task_declaring(codes: &'static [&'static str]) -> RegisteredTask {
        RegisteredTask::Blocking {
            task: Arc::new(EchoTask),
            meta: TaskMeta::default().with_error_codes(codes),
        }
    }

    fn register_declaring(
        codes: &'static [&'static str],
    ) -> (TaskRegistry, Result<(), HorsiesError>) {
        let mut tasks = TaskRegistry::new();
        let registered = register_task(
            &mut tasks,
            &default_config(),
            "charge".to_owned(),
            task_declaring(codes),
            QueueSource::TaskOptions,
        );
        (tasks, registered)
    }

    #[test]
    fn a_valid_set_of_declared_codes_registers() {
        let (tasks, registered) = register_declaring(&["CARD_DECLINED", "RATE_LIMITED"]);
        assert!(registered.is_ok());
        assert_eq!(
            tasks.get("charge").unwrap().error_codes(),
            ["CARD_DECLINED", "RATE_LIMITED"]
        );
    }

    #[test]
    fn a_built_in_or_reserved_name_is_refused_with_hrs_212_and_nothing_is_registered() {
        let cases: [&'static [&'static str]; 2] =
            [&["FINE", "BROKER_ERROR"], &["WORKFLOW_STOPPED"]];
        for codes in cases {
            let (tasks, registered) = register_declaring(codes);
            let refused = registered.unwrap_err();
            assert_eq!(
                refused.code,
                Some(ErrorCode::CheckReservedCodeCollision),
                "{codes:?}"
            );
            assert!(
                refused.message.contains("task 'charge'"),
                "{}",
                refused.message
            );
            assert!(tasks.is_empty());
        }
    }

    #[test]
    fn a_code_outside_the_code_text_rule_is_refused_with_hrs_112() {
        let cases: [&'static [&'static str]; 4] =
            [&["_INTERNAL"], &["\u{c4}RGER"], &["TimeoutError"], &[""]];
        for codes in cases {
            let (tasks, registered) = register_declaring(codes);
            let refused = registered.unwrap_err();
            assert_eq!(
                refused.code,
                Some(ErrorCode::TaskInvalidErrorCode),
                "{codes:?}"
            );
            assert!(tasks.is_empty());
        }
    }

    #[test]
    fn a_repeated_code_is_refused_with_hrs_113() {
        let (tasks, registered) = register_declaring(&["RATE_LIMITED", "RATE_LIMITED"]);
        let refused = registered.unwrap_err();
        assert_eq!(refused.code, Some(ErrorCode::TaskDuplicateErrorCode));
        assert!(
            refused.message.contains("RATE_LIMITED"),
            "{}",
            refused.message
        );
        assert!(tasks.is_empty());
    }

    #[test]
    fn every_refused_code_is_named_in_one_error() {
        let (_tasks, registered) = register_declaring(&["bad", "BROKER_ERROR", "OK", "OK"]);
        let refused = registered.unwrap_err();
        assert_eq!(refused.notes.len(), 3, "{refused}");
        assert_eq!(refused.code, None);
        for code in ["HRS-112", "HRS-212", "HRS-113"] {
            assert!(
                refused.notes.iter().any(|note| note.contains(code)),
                "{code}: {refused}"
            );
        }
    }

    #[test]
    fn a_task_that_declares_no_code_registers() {
        let (tasks, registered) = register_declaring(&[]);
        assert!(registered.is_ok());
        assert!(tasks.get("charge").unwrap().error_codes().is_empty());
    }

    #[test]
    fn valid_queue_names_follow_the_queue_mode() {
        assert_eq!(valid_queue_names(&default_config()), vec!["default"]);
        assert_eq!(
            valid_queue_names(&custom_config()),
            vec!["critical", "bulk"]
        );
    }

    #[test]
    fn check_queue_admits_only_the_valid_names() {
        assert!(check_queue(&default_config(), "default").is_ok());
        assert_eq!(
            check_queue(&default_config(), "bulk").unwrap_err().code,
            Some(ErrorCode::TaskInvalidQueue)
        );
        assert!(check_queue(&custom_config(), "bulk").is_ok());
        assert!(check_queue(&custom_config(), "default").is_err());
    }

    #[test]
    fn queue_priority_is_the_configured_one_else_100() {
        assert_eq!(queue_priority(&custom_config(), "critical"), 1);
        assert_eq!(queue_priority(&custom_config(), "bulk"), 90);
        assert_eq!(
            queue_priority(&custom_config(), "unknown"),
            DEFAULT_PRIORITY
        );
        assert_eq!(
            queue_priority(&default_config(), "default"),
            DEFAULT_PRIORITY
        );
    }
}

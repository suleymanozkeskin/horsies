//! The scheduler run of the app (Python `horsies scheduler`), and the check
//! of each schedule before the start: its task, its queue, its input read
//! back with the task's input type (#573), and the input size.
//!
//! - [`Horsies::scheduler`]: the checks and the scheduler manifest, with no
//!   I/O. Its [`PreparedScheduler`] runs on the caller's runtime and stops
//!   at a request through its [`SchedulerStop`]: for a service that runs
//!   several loops in one binary.
//! - [`Horsies::run_scheduler`]: the same, stopped by `SIGTERM` or
//!   `SIGINT`.
//!
//! Several schedulers can run against one database: each fire locks its
//! schedule's state row and compares the slot it computed from, so a second
//! scheduler writes nothing for a slot that the first one fired.

use std::sync::Arc;

use horsies_core::handle::CoreHandle;
use horsies_core::scheduler_manifest::SchedulerManifest;
use horsies_core::{RegisteredTask, SchedulerService, SchedulerStartError, SchedulerStarted};

pub use horsies_core::SchedulerStop;

use crate::check::{CheckReport, CheckScope, SCHEDULER_STARTUP};
use crate::config::{schedule_input_text, scheduler_manifest};
use crate::process_state::{refuse_in_a_fork, runtime_for, RuntimeStartFailed};
use crate::registry::resolve_task_queue;
use crate::signals::stop_on_signals;
use crate::{Horsies, HorsiesError, HorsiesErrorCode, ScheduleConfig, TaskSchedule};

/// The scheduler ran until a stop request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerStopped;

/// Why a scheduler did not run until a stop request. Python's `horsies
/// scheduler` exits 1 for each.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SchedulerRunError {
    /// The app config has no schedule section. Nothing started.
    #[error("schedule configuration not found in app config")]
    NotConfigured,
    /// The schedule section's master switch is off. Nothing started.
    #[error("the scheduler is disabled in config (schedule.enabled is false)")]
    Disabled,
    /// The static check refused the app: a task's options, or a schedule
    /// (Python's `scheduler-startup` report). Nothing started.
    #[error("{0}")]
    CheckRefused(CheckReport),
    /// The core's parser refused the scheduler manifest: a value outside
    /// its bound, a pattern that breaks its rules, or a repeated schedule
    /// name. `cause` names the field, never a value. Nothing started.
    #[error("the scheduler manifest is refused: {cause}")]
    ManifestRefused { cause: String },
    /// The start refused `schedule` before any state write: a zone that the
    /// core does not know, a send that the command rules refuse, or a
    /// pattern with no slot after the database time.
    #[error("{reason}")]
    StartRefused { schedule: String, reason: String },
    /// The connect, the schema install or the client family check failed.
    /// No schedule state was written. `retryable` tells whether a later
    /// start may pass.
    #[error("the scheduler start failed: {message}")]
    StartFailed { message: String, retryable: bool },
    /// The OS refused the threads of the core's process runtime, which
    /// listens for the stop signals. Nothing started; a later run tries the
    /// start again.
    #[error("{0}")]
    RuntimeUnavailable(RuntimeStartFailed),
}

/// A checked scheduler of the app's schedule section, not started.
pub struct PreparedScheduler {
    handle: Arc<CoreHandle>,
    manifest: SchedulerManifest,
    stop: SchedulerStop,
}

impl std::fmt::Debug for PreparedScheduler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedScheduler")
            .field("schedules", &self.manifest.schedules.len())
            .finish_non_exhaustive()
    }
}

impl PreparedScheduler {
    /// The stop request of this scheduler. A request before the run ends
    /// the run after its start; a fire in flight runs to its end.
    pub fn stop_handle(&self) -> SchedulerStop {
        self.stop.clone()
    }

    /// Connect and install the schema once per handle, start the schedule
    /// state, then fire one tick at once and one per
    /// `check_interval_seconds` until a stop request. A failed tick is
    /// logged and the next one runs; a schedule section with no schedule
    /// waits for the stop request.
    ///
    /// Errors: `StartRefused`, `StartFailed`.
    ///
    /// # Panics
    ///
    /// In a process that was forked without `exec` after the core's
    /// process runtime started. Nothing started.
    pub async fn run(self) -> Result<SchedulerStopped, SchedulerRunError> {
        refuse_in_a_fork(format_args!("PreparedScheduler::run"));
        let broker = self.handle.initialized_broker().await.map_err(|error| {
            SchedulerRunError::StartFailed {
                message: error.to_string(),
                retryable: error.is_retryable(),
            }
        })?;
        let started = SchedulerService::from_manifest(broker, self.manifest, &self.stop)
            .start()
            .await
            .map_err(|error| start_error(&error))?;
        match started {
            SchedulerStarted::Active(active) => active.run().await,
            SchedulerStarted::Idle => self.stop.requested().await,
        }
        Ok(SchedulerStopped)
    }
}

impl Horsies {
    /// The scheduler of the app's schedule section, checked and ready to
    /// run. No I/O.
    ///
    /// In order: a schedule section with its master switch on; the static
    /// check with the schedules; the scheduler manifest, which the core's
    /// parser checks.
    ///
    /// Errors: `NotConfigured`, `Disabled`, `CheckRefused`,
    /// `ManifestRefused`.
    pub fn scheduler(&self) -> Result<PreparedScheduler, SchedulerRunError> {
        let schedule = match &self.config().schedule {
            None => return Err(SchedulerRunError::NotConfigured),
            Some(schedule) if !schedule.enabled => return Err(SchedulerRunError::Disabled),
            Some(schedule) => schedule,
        };
        self.check_scope(CheckScope::WithSchedules, SCHEDULER_STARTUP)
            .map_err(SchedulerRunError::CheckRefused)?;
        let manifest =
            scheduler_manifest(self.config(), schedule, self.tasks()).map_err(|refused| {
                SchedulerRunError::ManifestRefused {
                    cause: refused.to_string(),
                }
            })?;
        Ok(PreparedScheduler {
            handle: Arc::clone(self.handle()),
            manifest,
            stop: SchedulerStop::new(),
        })
    }

    /// Run the scheduler of [`Self::scheduler`] until `SIGTERM` or `SIGINT`
    /// (Python `horsies scheduler`).
    ///
    /// Errors: [`SchedulerRunError`]. The run returns `Ok` only at a stop
    /// request.
    ///
    /// # Panics
    ///
    /// After the checks, in a process that was forked without `exec` after
    /// the core's process runtime started. Nothing started.
    pub async fn run_scheduler(&self) -> Result<SchedulerStopped, SchedulerRunError> {
        let prepared = self.scheduler()?;
        let stop = prepared.stop_handle();
        let signals = runtime_for(format_args!("Horsies::run_scheduler"))
            .map_err(SchedulerRunError::RuntimeUnavailable)?
            .handle()
            .spawn(stop_on_signals("scheduler", move || stop.request_stop()));
        let end = prepared.run().await;
        signals.abort();
        end
    }

    /// One refusal per enabled schedule that fails a check, in config
    /// order; the first failing check of a schedule wins (Python
    /// `collect_schedule_errors`). Pure.
    pub(crate) fn schedule_refusals(&self, schedule: &ScheduleConfig) -> Vec<HorsiesError> {
        schedule
            .schedules
            .iter()
            .filter(|task_schedule| task_schedule.enabled)
            .filter_map(|task_schedule| self.schedule_refusal(task_schedule))
            .collect()
    }

    /// The first failing check of one enabled schedule: the task, the
    /// queue, the input read back with the task's input type, the input
    /// size. Pure.
    fn schedule_refusal(&self, schedule: &TaskSchedule) -> Option<HorsiesError> {
        let Ok(task) = self.tasks().get(&schedule.task_name) else {
            return Some(self.unregistered_task(schedule));
        };
        if let Err(cause) = schedule_queue(self, task, schedule) {
            return Some(
                HorsiesError::new(format!(
                    "invalid queue configuration for schedule '{}'",
                    schedule.name
                ))
                .with_code(HorsiesErrorCode::ConfigInvalidSchedule)
                .with_note(format!("underlying error: {}", cause.message))
                .with_help("name a queue of the app config in the schedule, or none"),
            );
        }
        let input = schedule_input_text(schedule);
        if let Err(refused) = task.validate_input(input.as_bytes()) {
            return Some(
                HorsiesError::new(format!(
                    "schedule '{}' input does not read back as the input of task '{}'",
                    schedule.name, schedule.task_name
                ))
                .with_code(HorsiesErrorCode::ConfigInvalidSchedule)
                .with_note(format!(
                    "input error: {}",
                    refused.message.as_deref().unwrap_or("no message")
                ))
                .with_help(
                    "write the schedule's input as the task's input type writes it: its \
                     fields, with values of their types",
                ),
            );
        }
        oversized_input(self, schedule, input.len())
    }

    fn unregistered_task(&self, schedule: &TaskSchedule) -> HorsiesError {
        let mut available: Vec<&str> = self.tasks().iter().map(|(name, _)| name).collect();
        available.sort_unstable();
        let available_note = match available.is_empty() {
            true => "no tasks registered".to_owned(),
            false => format!("available tasks: {available:?}"),
        };
        HorsiesError::new(format!(
            "scheduled task '{}' not registered",
            schedule.task_name
        ))
        .with_code(HorsiesErrorCode::TaskNotRegistered)
        .with_note(format!(
            "schedule '{}' references task '{}'",
            schedule.name, schedule.task_name
        ))
        .with_note(available_note)
        .with_help(format!(
            "register the task with {}::register(&mut app) in main",
            schedule.task_name
        ))
    }
}

/// The queue of each slot, checked with the queue rule of a registration:
/// the schedule's queue, else the task's (Python `_resolve_queue_error`).
/// Pure.
fn schedule_queue(
    app: &Horsies,
    task: &RegisteredTask,
    schedule: &TaskSchedule,
) -> Result<String, HorsiesError> {
    match (schedule.queue_name.as_deref(), task.queue_name()) {
        (Some(queue), _) => resolve_task_queue(app.config(), Some(queue)),
        (None, Some(task_queue)) => Ok(task_queue.to_owned()),
        (None, None) => resolve_task_queue(app.config(), None),
    }
}

/// The refusal of an input over `payload.reject_bytes`: every slot sends
/// the same input, so every enqueue would be refused. Pure.
fn oversized_input(app: &Horsies, schedule: &TaskSchedule, size: usize) -> Option<HorsiesError> {
    let limit = app.config().payload.reject_bytes?;
    let size = u64::try_from(size).unwrap_or(u64::MAX);
    (size > limit).then(|| {
        HorsiesError::new(format!(
            "schedule '{}' input is {size} bytes, over payload.reject_bytes={limit}",
            schedule.name
        ))
        .with_code(HorsiesErrorCode::ConfigInvalidSchedule)
        .with_note("every slot of the schedule sends the same input, so every enqueue is refused")
        .with_help("make the schedule's input smaller, or raise payload.reject_bytes")
    })
}

/// The error of a start that the core refused or that failed. Pure.
fn start_error(error: &SchedulerStartError) -> SchedulerRunError {
    match error {
        SchedulerStartError::UnknownTimezone { schedule, .. }
        | SchedulerStartError::CommandRefused { schedule, .. }
        | SchedulerStartError::NoNextSlot { schedule, .. } => SchedulerRunError::StartRefused {
            schedule: schedule.clone(),
            reason: error.to_string(),
        },
        SchedulerStartError::ClientFamily(cause) => SchedulerRunError::StartFailed {
            message: error.to_string(),
            retryable: cause.is_retryable(),
        },
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::*;
    use crate::{AppConfig, IntervalSchedule, PayloadPolicy, SchedulePattern, TaskError};

    #[derive(Debug, Serialize, Deserialize)]
    struct CountInput {
        count: u32,
    }

    #[allow(clippy::needless_pass_by_value)]
    fn count_task(input: CountInput) -> Result<u32, TaskError<std::convert::Infallible>> {
        Ok(input.count)
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct StepInput {
        count: u32,
        options: StepOptions,
    }

    /// A nested input type that reads an unknown key without an error.
    #[derive(Debug, Serialize, Deserialize)]
    struct StepOptions {
        #[serde(default)]
        step: u32,
    }

    #[allow(clippy::needless_pass_by_value)]
    fn step_task(input: StepInput) -> Result<u32, TaskError<std::convert::Infallible>> {
        Ok(input.count + input.options.step)
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct WideInput {
        n: u128,
    }

    #[allow(clippy::needless_pass_by_value)]
    fn wide_task(input: WideInput) -> Result<u128, TaskError<std::convert::Infallible>> {
        Ok(input.n)
    }

    /// A schedule of `task_name` every 30 seconds, with no input.
    fn schedule_without_input(name: &str, task_name: &str) -> TaskSchedule {
        TaskSchedule::new(
            name,
            task_name,
            SchedulePattern::Interval(IntervalSchedule {
                seconds: Some(30),
                minutes: None,
                hours: None,
                days: None,
            }),
        )
    }

    /// A schedule of `task_name` every 30 seconds with the input `input`,
    /// written by the builder.
    fn schedule(name: &str, task_name: &str, input: &serde_json::Value) -> TaskSchedule {
        schedule_without_input(name, task_name)
            .input(input)
            .unwrap()
    }

    fn app_with(schedules: Vec<TaskSchedule>, enabled: bool) -> Horsies {
        let mut config = AppConfig::for_database_url("postgresql://localhost/horsies");
        config.schedule = Some(ScheduleConfig {
            enabled,
            schedules,
            ..ScheduleConfig::default()
        });
        let mut app = Horsies::new(config).unwrap();
        app.register("count", crate::blocking_task_fn!(count_task, CountInput))
            .unwrap();
        app
    }

    fn messages(app: &Horsies) -> Vec<String> {
        let schedule = app.config().schedule.clone().unwrap();
        app.schedule_refusals(&schedule)
            .into_iter()
            .map(|error| error.message)
            .collect()
    }

    #[test]
    fn a_schedule_whose_input_reads_back_passes() {
        let app = app_with(vec![schedule("every", "count", &json!({"count": 5}))], true);
        assert!(messages(&app).is_empty());
        assert!(app.scheduler().is_ok());
    }

    /// #573: a schedule input of another type is refused before any
    /// connection, with the schedule and the task named.
    #[test]
    fn an_input_of_another_type_is_refused_before_the_start() {
        let app = app_with(
            vec![schedule("every", "count", &json!({"count": "five"}))],
            true,
        );
        let texts = messages(&app);
        assert_eq!(texts.len(), 1);
        assert!(texts[0].contains("schedule 'every' input does not read back"));
        let Err(SchedulerRunError::CheckRefused(report)) = app.scheduler() else {
            panic!("the scheduler starts with an input of another type");
        };
        assert_eq!(report.phase(), "scheduler-startup");
        assert_eq!(
            report.errors()[0].code,
            Some(HorsiesErrorCode::ConfigInvalidSchedule)
        );
        assert!(!report.to_string().contains("five"));
    }

    /// Python's `sig.bind` refuses a keyword that the task does not take.
    #[test]
    fn an_input_member_that_the_type_does_not_read_is_refused() {
        let app = app_with(
            vec![schedule("every", "count", &json!({"count": 5, "cuont": 6}))],
            true,
        );
        let schedule = app.config().schedule.clone().unwrap();
        let refusals = app.schedule_refusals(&schedule);
        assert_eq!(refusals.len(), 1);
        assert!(refusals[0].notes[0].contains("'cuont'"), "{}", refusals[0]);
    }

    /// #571, Python `_validate_input` with `extra='forbid'`: a key inside a
    /// nested object of the input that binds to no field is refused before
    /// any connection, with its key path.
    #[test]
    fn a_nested_input_member_that_the_type_does_not_read_is_refused() {
        let mut app = app_with(
            vec![schedule(
                "every",
                "step",
                &json!({"count": 5, "options": {"stpe": 2}}),
            )],
            true,
        );
        app.register("step", crate::blocking_task_fn!(step_task, StepInput))
            .unwrap();
        let schedule = app.config().schedule.clone().unwrap();
        let refusals = app.schedule_refusals(&schedule);
        assert_eq!(refusals.len(), 1);
        assert_eq!(
            refusals[0].code,
            Some(HorsiesErrorCode::ConfigInvalidSchedule)
        );
        assert!(
            refusals[0].notes[0].contains("'options.stpe'"),
            "{}",
            refusals[0]
        );
    }

    /// #1892: a schedule input of `u128::MAX`, written by the builder from
    /// the task's input type, keeps every digit and reads back with that
    /// type: the check passes, and the slot's text decodes to `u128::MAX`.
    #[test]
    fn an_input_of_u128_max_reads_back_as_the_task_s_input() {
        let wide = schedule_without_input("wide", "wide")
            .input(&WideInput { n: u128::MAX })
            .unwrap();
        let mut app = app_with(vec![wide], true);
        app.register("wide", crate::blocking_task_fn!(wide_task, WideInput))
            .unwrap();
        assert!(messages(&app).is_empty());
        let schedule = app.config().schedule.clone().unwrap();
        let text = schedule_input_text(&schedule.schedules[0]);
        assert_eq!(text, r#"{"n":340282366920938463463374607431768211455}"#);
        let read: WideInput = crate::task::body::decode_input(text.as_bytes()).unwrap();
        assert_eq!(read.n, u128::MAX);
    }

    #[test]
    fn an_unregistered_task_names_the_registered_ones() {
        let app = app_with(vec![schedule_without_input("every", "missing")], true);
        let schedule = app.config().schedule.clone().unwrap();
        let refusals = app.schedule_refusals(&schedule);
        assert_eq!(refusals[0].code, Some(HorsiesErrorCode::TaskNotRegistered));
        assert!(refusals[0].notes.iter().any(|note| note.contains("count")));
        let text = refusals[0].to_string();
        assert!(text.contains("schedule 'every'"), "{text}");
        assert!(text.contains("task 'missing'"), "{text}");
    }

    /// A schedule on a queue that the app does not admit is refused before
    /// any connection, with the schedule and the queue named.
    #[test]
    fn a_schedule_on_a_queue_that_the_app_does_not_admit_names_the_schedule_and_the_queue() {
        let mut config = AppConfig::for_database_url("postgresql://localhost/horsies");
        config.queue_mode = crate::QueueMode::Custom;
        config.custom_queues = Some(vec![crate::CustomQueueConfig {
            name: "fast".to_owned(),
            priority: 1,
            max_concurrency: Some(10),
        }]);
        let mut on_unknown = schedule("bad_queue_job", "count", &json!({"count": 5}));
        on_unknown.queue_name = Some("nonexistent_queue".to_owned());
        config.schedule = Some(ScheduleConfig {
            enabled: true,
            schedules: vec![on_unknown],
            ..ScheduleConfig::default()
        });
        let mut app = Horsies::new(config).unwrap();
        app.register_with_queue(
            "count",
            crate::blocking_task_fn!(count_task, CountInput),
            "fast",
        )
        .unwrap();
        let schedule = app.config().schedule.clone().unwrap();
        let refusals = app.schedule_refusals(&schedule);
        assert_eq!(refusals.len(), 1);
        assert_eq!(
            refusals[0].code,
            Some(HorsiesErrorCode::ConfigInvalidSchedule)
        );
        let text = refusals[0].to_string();
        assert!(text.contains("bad_queue_job"), "{text}");
        assert!(text.contains("nonexistent_queue"), "{text}");
    }

    #[test]
    fn a_disabled_schedule_is_not_checked() {
        let mut disabled = schedule_without_input("off", "missing");
        disabled.enabled = false;
        let app = app_with(vec![disabled], true);
        assert!(messages(&app).is_empty());
    }

    #[test]
    fn an_input_over_the_reject_limit_is_refused() {
        let mut config = AppConfig::for_database_url("postgresql://localhost/horsies");
        config.payload = PayloadPolicy {
            reject_bytes: Some(4),
            ..PayloadPolicy::default()
        };
        config.schedule = Some(ScheduleConfig {
            enabled: true,
            schedules: vec![schedule("every", "count", &json!({"count": 5}))],
            ..ScheduleConfig::default()
        });
        let mut app = Horsies::new(config).unwrap();
        app.register("count", crate::blocking_task_fn!(count_task, CountInput))
            .unwrap();
        let texts = messages(&app);
        assert_eq!(texts.len(), 1);
        assert!(texts[0].contains("over payload.reject_bytes=4"));
    }

    #[test]
    fn no_schedule_section_and_a_disabled_one_are_refused() {
        let app = Horsies::new(AppConfig::for_database_url(
            "postgresql://localhost/horsies",
        ))
        .unwrap();
        assert!(matches!(
            app.scheduler(),
            Err(SchedulerRunError::NotConfigured)
        ));
        let disabled = app_with(Vec::new(), false);
        assert!(matches!(
            disabled.scheduler(),
            Err(SchedulerRunError::Disabled)
        ));
    }

    #[test]
    fn a_start_refusal_names_its_schedule_and_a_family_failure_keeps_its_retry_rule() {
        let refused = start_error(&SchedulerStartError::UnknownTimezone {
            schedule: "every".to_owned(),
            timezone: "Mars/Olympus".to_owned(),
        });
        assert!(matches!(
            refused,
            SchedulerRunError::StartRefused { ref schedule, .. } if schedule == "every"
        ));
        let failed = start_error(&SchedulerStartError::ClientFamily(
            horsies_core::BrokerError::ListenerClosed,
        ));
        assert!(matches!(
            failed,
            SchedulerRunError::StartFailed { retryable, .. }
                if retryable == horsies_core::BrokerError::ListenerClosed.is_retryable()
        ));
    }
}

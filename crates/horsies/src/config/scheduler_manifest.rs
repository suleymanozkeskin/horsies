//! The scheduler manifest of a Rust app (Python `to_scheduler_manifest`,
//! `python/horsies/core/scheduler/manifest.py`).
//!
//! Per schedule, in config order: a disabled schedule is its name; an
//! enabled schedule is its pattern text, timezone, catch-up rule and the
//! send facts of each slot (the task, the queue and its priority, the input
//! text, the task's options text, the retention class key and the
//! rerun-input posture).
//!
//! The core's scheduler manifest types are built only by its parser, so the
//! builder writes the JSON document and the parser reads it back: every
//! bound (the check interval, the catch-up runs, the queue priority, the
//! pattern's rules, a repeated name) is checked once, by the core, with the
//! field path and never a value.

use horsies_core::manifest::ManifestRefused;
use horsies_core::scheduler_manifest::{
    parse_scheduler_manifest, SchedulerManifest, SCHEDULER_MANIFEST_VERSION,
};
use horsies_core::{RegisteredTask, TaskRegistry};
use serde_json::{json, Value};

use crate::registry::{queue_priority, resolve_task_queue};
use crate::{AppConfig, HorsiesError, ScheduleConfig, TaskSchedule};

/// Why the scheduler manifest of a config was not built.
#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum ScheduleManifestRefused {
    /// An enabled schedule names a task that this binary did not register.
    #[error("schedule.schedules[{position}].task_name: task '{task_name}' is not registered")]
    TaskNotRegistered { position: usize, task_name: String },
    /// The queue of an enabled schedule is not a queue of the app (Python
    /// `Horsies.validate_queue_name`).
    #[error("schedule.schedules[{position}].queue_name: {cause}")]
    QueueNotAdmitted {
        position: usize,
        cause: HorsiesError,
    },
    /// The core's parser refused the document: a value outside its manifest
    /// bound, a pattern that breaks its rules, or a repeated schedule name.
    /// The path is the manifest's (`schedules[0].activation`).
    #[error("scheduler manifest refused: {0}")]
    Refused(ManifestRefused),
}

/// The scheduler manifest of `schedule` in the app of `config` and `tasks`.
/// Pure: no I/O.
///
/// The input text of each enabled schedule is its `input` value as JSON,
/// `null` for a schedule without input. The caller checks it with the
/// task's input type before the scheduler starts.
///
/// Errors: [`ScheduleManifestRefused`], for the first schedule in config
/// order that has one, then the parse of the whole document.
pub(crate) fn scheduler_manifest(
    config: &AppConfig,
    schedule: &ScheduleConfig,
    tasks: &TaskRegistry,
) -> Result<SchedulerManifest, ScheduleManifestRefused> {
    let schedules = schedule
        .schedules
        .iter()
        .enumerate()
        .map(|(position, task_schedule)| schedule_document(config, tasks, position, task_schedule))
        .collect::<Result<Vec<Value>, ScheduleManifestRefused>>()?;
    let document = json!({
        "manifest_version": SCHEDULER_MANIFEST_VERSION,
        "enabled": schedule.enabled,
        "check_interval_seconds": schedule.check_interval_seconds,
        "schedules": schedules,
    });
    parse_scheduler_manifest(&document.to_string()).map_err(ScheduleManifestRefused::Refused)
}

/// One schedule of the document: disabled, or enabled with its send.
fn schedule_document(
    config: &AppConfig,
    tasks: &TaskRegistry,
    position: usize,
    schedule: &TaskSchedule,
) -> Result<Value, ScheduleManifestRefused> {
    match schedule.enabled {
        false => Ok(json!({
            "name": schedule.name,
            "activation": {"kind": "DISABLED"},
        })),
        true => {
            let task = tasks.get(&schedule.task_name).map_err(|_| {
                ScheduleManifestRefused::TaskNotRegistered {
                    position,
                    task_name: schedule.task_name.clone(),
                }
            })?;
            Ok(json!({
                "name": schedule.name,
                "activation": {
                    "kind": "ENABLED",
                    "pattern_json": pattern_json(schedule),
                    "timezone": schedule.timezone,
                    "catch_up_missed": schedule.catch_up_missed,
                    "max_catch_up_runs": schedule.max_catch_up_runs,
                    "send": send_document(config, task, position, schedule)?,
                },
            }))
        }
    }
}

/// The send facts of each slot: the values that a plain send of the
/// schedule's input on its queue stores (Python `_send_manifest`).
fn send_document(
    config: &AppConfig,
    task: &RegisteredTask,
    position: usize,
    schedule: &TaskSchedule,
) -> Result<Value, ScheduleManifestRefused> {
    let queue = schedule_queue(config, task, schedule)
        .map_err(|cause| ScheduleManifestRefused::QueueNotAdmitted { position, cause })?;
    let task_options = match task
        .task_options()
        .and_then(|options| options.serialize_retry_options())
    {
        Some(json) => json!({"kind": "ENCODED", "json": json}),
        None => json!({"kind": "NO_OPTIONS"}),
    };
    Ok(json!({
        "task_name": schedule.task_name,
        "queue_name": queue,
        "priority": queue_priority(config, &queue),
        "input_json": input_json(schedule),
        "task_options": task_options,
        "retention_class_key": config.retention.resolve_queue_retention(&queue).class_key(),
        "retain_rerun_input": config.broker.retain_rerun_input_default,
    }))
}

/// The queue of each slot (Python `resolve_schedule_queue`): the schedule's
/// queue, checked with the queue rule of a registration, else the queue
/// that the task resolved when it registered.
fn schedule_queue(
    config: &AppConfig,
    task: &RegisteredTask,
    schedule: &TaskSchedule,
) -> Result<String, HorsiesError> {
    match (schedule.queue_name.as_deref(), task.queue_name()) {
        (Some(queue), _) => resolve_task_queue(config, Some(queue)),
        (None, Some(task_queue)) => Ok(task_queue.to_owned()),
        (None, None) => resolve_task_queue(config, None),
    }
}

/// The pattern as the JSON text of its serde form, which the core's
/// `PatternJson` reads with the same type.
fn pattern_json(schedule: &TaskSchedule) -> String {
    serde_json::to_string(&schedule.pattern).unwrap_or_else(|error| {
        panic!(
            "the pattern of schedule '{}' holds plain values and encodes: {error}",
            schedule.name
        )
    })
}

/// The input of each slot as one JSON value; `null` for a schedule without
/// input.
fn input_json(schedule: &TaskSchedule) -> String {
    serde_json::to_string(&schedule.input).unwrap_or_else(|error| {
        panic!(
            "the input of schedule '{}' is a JSON value and encodes: {error}",
            schedule.name
        )
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use horsies_core::core::task::fn_trait::{BlockingTaskFn, RawTaskResult, TaskMeta};
    use horsies_core::scheduler_manifest::{
        ScheduleActivation, ScheduleSendManifest, ScheduleTaskOptions,
    };
    use horsies_core::{Execution, TaskOptions, TaskResult};
    use serde_json::json;

    use super::*;
    use crate::registry::{register_task, QueueSource};
    use crate::{CustomQueueConfig, IntervalSchedule, QueueMode, SchedulePattern};

    struct EchoTask;

    impl BlockingTaskFn for EchoTask {
        fn execute(&self, input: &[u8]) -> RawTaskResult {
            TaskResult::Ok(input.to_vec())
        }
    }

    fn task(timeout_ms: Option<u32>, queue: Option<&str>) -> RegisteredTask {
        RegisteredTask::Blocking {
            task: Arc::new(EchoTask),
            meta: TaskMeta::default(),
        }
        .with_task_options(TaskOptions {
            task_name: "reports.nightly".to_owned(),
            queue_name: queue.map(str::to_owned),
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
            timeout_ms,
            execution: Execution::Isolated,
            cancel_grace_ms: None,
        })
    }

    fn every_minute() -> SchedulePattern {
        SchedulePattern::Interval(IntervalSchedule {
            seconds: Some(60),
            minutes: None,
            hours: None,
            days: None,
        })
    }

    fn config() -> AppConfig {
        AppConfig::for_database_url("postgresql://app@db.example/horsies")
    }

    fn registry(config: &AppConfig, queue: Option<&str>) -> TaskRegistry {
        let mut tasks = TaskRegistry::new();
        register_task(
            &mut tasks,
            config,
            "reports.nightly".to_owned(),
            task(Some(5_000), queue),
            QueueSource::TaskOptions,
        )
        .unwrap();
        register_task(
            &mut tasks,
            config,
            "cleanup".to_owned(),
            task(None, queue),
            QueueSource::TaskOptions,
        )
        .unwrap();
        tasks
    }

    fn nightly() -> TaskSchedule {
        TaskSchedule {
            input: json!({"day": "monday", "limit": 5}),
            ..TaskSchedule::new("nightly", "reports.nightly", every_minute())
        }
    }

    fn send_of(manifest: &SchedulerManifest, position: usize) -> &ScheduleSendManifest {
        match &manifest.schedules[position].activation {
            ScheduleActivation::Enabled { send, .. } => send,
            ScheduleActivation::Disabled => panic!("schedule {position} is disabled"),
        }
    }

    #[test]
    fn an_enabled_and_a_disabled_schedule_read_back() {
        let config = config();
        let paused = TaskSchedule {
            enabled: false,
            ..TaskSchedule::new("paused", "not.registered", every_minute())
        };
        let schedule = ScheduleConfig::new(vec![nightly(), paused]);
        let manifest = scheduler_manifest(&config, &schedule, &registry(&config, None)).unwrap();
        assert!(manifest.enabled);
        assert_eq!(manifest.schedules.len(), 2);
        assert!(matches!(
            manifest.schedules[1].activation,
            ScheduleActivation::Disabled
        ));
        let send = send_of(&manifest, 0);
        assert_eq!(send.task_name, "reports.nightly");
        assert_eq!(send.queue_name, "default");
        assert_eq!(send.priority.0.get(), 100);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&send.input_json).unwrap(),
            json!({"day": "monday", "limit": 5})
        );
        assert!(matches!(
            send.task_options,
            ScheduleTaskOptions::Encoded { .. }
        ));
        assert!(!send.retain_rerun_input);
    }

    #[test]
    fn a_schedule_without_input_sends_null_and_a_task_without_options_stores_none() {
        let config = config();
        let schedule = ScheduleConfig::new(vec![TaskSchedule::new(
            "cleanup",
            "cleanup",
            every_minute(),
        )]);
        let manifest = scheduler_manifest(&config, &schedule, &registry(&config, None)).unwrap();
        let send = send_of(&manifest, 0);
        assert_eq!(send.input_json, "null");
        assert_eq!(send.task_options, ScheduleTaskOptions::NoOptions);
    }

    #[test]
    fn the_queue_and_the_retention_class_follow_the_config() {
        let mut config = config();
        config.queue_mode = QueueMode::Custom;
        config.custom_queues = Some(vec![
            CustomQueueConfig {
                name: "reports".to_owned(),
                priority: 20,
                max_concurrency: None,
            },
            CustomQueueConfig {
                name: "audit".to_owned(),
                priority: 5,
                max_concurrency: None,
            },
        ]);
        let _previous = config
            .retention
            .queue_retention
            .insert("audit".to_owned(), None);
        let tasks = registry(&config, Some("reports"));
        let on_audit = TaskSchedule {
            queue_name: Some("audit".to_owned()),
            ..TaskSchedule::new("audit", "reports.nightly", every_minute())
        };
        let schedule = ScheduleConfig::new(vec![nightly(), on_audit]);
        let manifest = scheduler_manifest(&config, &schedule, &tasks).unwrap();
        let task_queue = send_of(&manifest, 0);
        assert_eq!(task_queue.queue_name, "reports");
        assert_eq!(task_queue.priority.0.get(), 20);
        assert_eq!(
            task_queue.retention_class_key.retention().class_key(),
            "standard_30d"
        );
        let schedule_queue = send_of(&manifest, 1);
        assert_eq!(schedule_queue.queue_name, "audit");
        assert_eq!(schedule_queue.priority.0.get(), 5);
        assert_eq!(
            schedule_queue.retention_class_key.retention().class_key(),
            "forever"
        );
    }

    #[test]
    fn an_unregistered_task_is_refused_with_its_position() {
        let config = config();
        let schedule = ScheduleConfig::new(vec![
            nightly(),
            TaskSchedule::new("ghost", "not.registered", every_minute()),
        ]);
        let refused = scheduler_manifest(&config, &schedule, &registry(&config, None)).unwrap_err();
        assert!(matches!(
            refused,
            ScheduleManifestRefused::TaskNotRegistered { position: 1, .. }
        ));
    }

    #[test]
    fn a_named_queue_in_default_mode_is_refused() {
        let config = config();
        let on_default = TaskSchedule {
            queue_name: Some("default".to_owned()),
            ..nightly()
        };
        let schedule = ScheduleConfig::new(vec![on_default]);
        let refused = scheduler_manifest(&config, &schedule, &registry(&config, None)).unwrap_err();
        assert!(matches!(
            refused,
            ScheduleManifestRefused::QueueNotAdmitted { position: 0, .. }
        ));
    }

    #[test]
    fn a_value_outside_its_manifest_bound_is_refused_by_the_core_parser() {
        let config = config();
        let schedule = ScheduleConfig {
            check_interval_seconds: 61,
            ..ScheduleConfig::new(vec![nightly()])
        };
        let refused = scheduler_manifest(&config, &schedule, &registry(&config, None)).unwrap_err();
        match refused {
            ScheduleManifestRefused::Refused(refused) => {
                assert_eq!(refused.field_path, "check_interval_seconds");
            }
            other => panic!("expected the parser's refusal, got {other}"),
        }
    }
}

//! The metric registry of a worker parent, built from the facts of its
//! worker manifest: the task names and the declared error codes of each
//! task, the queue names of the app, and the facts of the process.
//!
//! A task or queue name that cannot be a label value of its own (empty, the
//! fallback text of its label, or past the bound of names) is not declared:
//! its samples count in the fallback series, and the registry names it
//! ([`WorkerRegistry::folded`]) for the WARNING line of the start.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::core::task::DeclaredErrorCodes;
use crate::telemetry::declared::{
    DeclaredNames, ErrorCodeValues, FoldedNames, HandlerName, QueueName, TaskName,
};
use crate::telemetry::registry::{ProcessFacts, Registry, RegistryLabels, WorkerFacts};
use crate::telemetry::worker::DeclaredCodesByTask;

/// The registry of a worker and the declared codes of each of its tasks,
/// which the telemetry value takes with it (`TelemetryOn::metrics_only`),
/// and the names that the registry does not declare.
pub struct WorkerRegistry {
    pub registry: Arc<Registry>,
    pub codes: DeclaredCodesByTask,
    /// The task names, queue names and error codes that are not declared.
    pub folded: FoldedNames,
}

/// The registry of a worker whose manifest registers `tasks` (each with its
/// declared error codes) and serves `queues`, with the facts `facts`, and
/// no event handler. No schedule is declared: a worker has no schedule
/// label. Pure.
///
/// Never refuses: a task or queue name that is empty, is the fallback text
/// of its label, or comes after `MAX_DECLARED_NAMES` names, and an error
/// code after `MAX_DECLARED_NAMES` codes, is not declared, and `folded`
/// names it.
pub fn worker_registry(
    tasks: &BTreeMap<String, DeclaredErrorCodes>,
    queues: &[String],
    facts: WorkerFacts,
) -> WorkerRegistry {
    worker_registry_with_handlers(tasks, queues, &[], facts)
}

/// The registry of [`worker_registry`] that also declares `handlers`: the
/// names of the event handlers of the app, the values of the `handler`
/// label of the metrics of the handler child. Pure.
///
/// Never refuses: a handler name that cannot be a label value of its own is
/// not declared, as a task or queue name, and `folded` names it.
pub fn worker_registry_with_handlers(
    tasks: &BTreeMap<String, DeclaredErrorCodes>,
    queues: &[String],
    handlers: &[String],
    facts: WorkerFacts,
) -> WorkerRegistry {
    let task_names = DeclaredNames::<TaskName>::new(tasks.keys());
    let queue_names = DeclaredNames::<QueueName>::new(queues);
    let handler_names = DeclaredNames::<HandlerName>::new(handlers);
    let error_codes = ErrorCodeValues::new(tasks.values());
    let folded = task_names
        .folded
        .and(queue_names.folded)
        .and(handler_names.folded)
        .and(error_codes.folded);
    let labels = RegistryLabels {
        tasks: task_names.declared,
        queues: queue_names.declared,
        handlers: handler_names.declared,
        schedules: DeclaredNames::none(),
        error_codes: error_codes.declared,
        process: ProcessFacts::Worker(facts),
    };
    let registry = match Registry::for_worker(labels) {
        Ok(registry) => registry,
        // Only a registry with no worker facts refuses the worker metrics.
        Err(refused) => panic!("a worker registry with worker facts was refused: {refused}"),
    };
    let codes = DeclaredCodesByTask::new(
        tasks
            .iter()
            .map(|(name, codes)| (name.clone(), codes.clone()))
            .collect::<HashMap<String, DeclaredErrorCodes>>(),
    );
    WorkerRegistry {
        registry: Arc::new(registry),
        codes,
        folded,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::catalog::MetricId;
    use crate::telemetry::declared::{FoldReason, FoldedName, MAX_DECLARED_NAMES};
    use crate::telemetry::labels::DeclaredSource;
    use crate::telemetry::registry::{Recorded, SampleValue};
    use crate::test_support::telemetry::series_of;
    use crate::Execution;

    fn facts() -> WorkerFacts {
        WorkerFacts {
            version: "0.8.0".to_owned(),
            profile: "default".to_owned(),
            worker_id: "worker-1".to_owned(),
        }
    }

    fn codes(codes: &[&str]) -> DeclaredErrorCodes {
        serde_json::from_value(serde_json::json!(codes)).expect("valid declared codes")
    }

    #[test]
    fn the_registry_declares_the_tasks_the_queues_and_their_codes() {
        let tasks = BTreeMap::from([
            ("app.fetch".to_owned(), codes(&["NOT_FOUND"])),
            ("app.store".to_owned(), DeclaredErrorCodes::NONE),
        ]);
        let queues = ["default".to_owned(), "billing".to_owned()];
        let WorkerRegistry {
            registry,
            codes,
            folded,
        } = worker_registry(&tasks, &queues, facts());

        assert_eq!(folded, FoldedNames::none());

        assert_eq!(
            registry.labels().tasks.texts(),
            [
                "app.fetch",
                "app.store",
                DeclaredSource::TaskNames.fallback()
            ]
        );
        assert_eq!(
            registry.labels().queues.texts(),
            ["default", "billing", DeclaredSource::QueueNames.fallback()]
        );
        assert_eq!(codes.of("app.fetch"), &tasks["app.fetch"]);
        assert_eq!(codes.of("app.other"), &DeclaredErrorCodes::NONE);
        let info = series_of(&registry, MetricId::WorkerInfo);
        assert_eq!(info.len(), 1);
        assert_eq!(info[0].0, ["0.8.0", "default", "worker-1"]);
    }

    fn folded_name(set: DeclaredSource, name: &str, reason: FoldReason) -> FoldedName {
        FoldedName {
            set,
            name: name.to_owned(),
            reason,
        }
    }

    /// Add 1 to `horsies.task.attempts.end_not_known` for the task `task`
    /// on the queue `queue`, as the worker records it.
    fn record_end_not_known(registry: &Registry, task: &str, queue: &str) {
        let ended = registry
            .counter::<(
                crate::telemetry::declared::DeclaredValue<TaskName>,
                crate::telemetry::declared::DeclaredValue<QueueName>,
                Execution,
            )>(MetricId::TaskAttemptsEndNotKnown)
            .expect("a worker registry holds the metric");
        let labels = registry.labels();
        let recorded = ended.add(
            (
                labels.tasks.value_of(task),
                labels.queues.value_of(queue),
                Execution::Isolated,
            ),
            1,
        );
        assert_eq!(recorded, Recorded::Added);
    }

    /// A task or queue name that is empty or is the fallback text of its
    /// label is not declared: no refusal, each is named once with its
    /// reason, and its samples count in the fallback series. A declared
    /// name keeps its own series.
    #[test]
    fn an_empty_or_fallback_name_counts_in_the_fallback_series() {
        let tasks = BTreeMap::from([
            ("app.fetch".to_owned(), DeclaredErrorCodes::NONE),
            ("(unregistered)".to_owned(), DeclaredErrorCodes::NONE),
            (String::new(), DeclaredErrorCodes::NONE),
        ]);
        let queues = ["default".to_owned(), "(unknown)".to_owned(), String::new()];
        let WorkerRegistry {
            registry, folded, ..
        } = worker_registry(&tasks, &queues, facts());

        assert_eq!(
            folded.names().to_vec(),
            vec![
                folded_name(DeclaredSource::TaskNames, "", FoldReason::EmptyName),
                folded_name(
                    DeclaredSource::TaskNames,
                    "(unregistered)",
                    FoldReason::FallbackText
                ),
                folded_name(
                    DeclaredSource::QueueNames,
                    "(unknown)",
                    FoldReason::FallbackText
                ),
                folded_name(DeclaredSource::QueueNames, "", FoldReason::EmptyName),
            ]
        );
        assert_eq!(
            registry.labels().tasks.texts(),
            ["app.fetch", DeclaredSource::TaskNames.fallback()]
        );
        assert_eq!(
            registry.labels().queues.texts(),
            ["default", DeclaredSource::QueueNames.fallback()]
        );

        record_end_not_known(&registry, "app.fetch", "default");
        record_end_not_known(&registry, "(unregistered)", "(unknown)");
        record_end_not_known(&registry, "", "");
        assert_eq!(
            series_of(&registry, MetricId::TaskAttemptsEndNotKnown),
            vec![
                (
                    vec![
                        "(unregistered)".to_owned(),
                        "(unknown)".to_owned(),
                        "ISOLATED".to_owned()
                    ],
                    SampleValue::Counter(2)
                ),
                (
                    vec![
                        "app.fetch".to_owned(),
                        "default".to_owned(),
                        "ISOLATED".to_owned()
                    ],
                    SampleValue::Counter(1)
                ),
            ]
        );
    }

    /// A task after `MAX_DECLARED_NAMES` tasks is not declared: no refusal,
    /// it is named once, and its samples count in the fallback series.
    #[test]
    fn a_task_past_the_bound_counts_in_the_fallback_series() {
        let tasks: BTreeMap<String, DeclaredErrorCodes> = (0..=MAX_DECLARED_NAMES)
            .map(|index| (format!("task_{index:06}"), DeclaredErrorCodes::NONE))
            .collect();
        let last = format!("task_{MAX_DECLARED_NAMES:06}");
        let WorkerRegistry {
            registry, folded, ..
        } = worker_registry(&tasks, &["default".to_owned()], facts());

        assert_eq!(
            folded.names().to_vec(),
            vec![folded_name(
                DeclaredSource::TaskNames,
                &last,
                FoldReason::PastBound
            )]
        );
        assert_eq!(registry.labels().tasks.names().len(), MAX_DECLARED_NAMES);

        record_end_not_known(&registry, &last, "default");
        record_end_not_known(&registry, "task_000000", "default");
        assert_eq!(
            series_of(&registry, MetricId::TaskAttemptsEndNotKnown),
            vec![
                (
                    vec![
                        "(unregistered)".to_owned(),
                        "default".to_owned(),
                        "ISOLATED".to_owned()
                    ],
                    SampleValue::Counter(1)
                ),
                (
                    vec![
                        "task_000000".to_owned(),
                        "default".to_owned(),
                        "ISOLATED".to_owned()
                    ],
                    SampleValue::Counter(1)
                ),
            ]
        );
    }
}

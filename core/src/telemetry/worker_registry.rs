//! The metric registry of a worker parent, built from the facts of its
//! worker manifest: the task names and the declared error codes of each
//! task, the queue names of the app, and the facts of the process.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::core::task::DeclaredErrorCodes;
use crate::telemetry::declared::{DeclaredNames, DeclaredNamesRefused, ErrorCodeValues};
use crate::telemetry::registry::{ProcessFacts, Registry, RegistryLabels, WorkerFacts};
use crate::telemetry::worker::DeclaredCodesByTask;

/// The registry of a worker and the declared codes of each of its tasks,
/// which the telemetry value takes with it (`TelemetryOn::metrics_only`).
pub struct WorkerRegistry {
    pub registry: Arc<Registry>,
    pub codes: DeclaredCodesByTask,
}

/// The registry of a worker whose manifest registers `tasks` (each with its
/// declared error codes) and serves `queues`, with the facts `facts`. No
/// handler and no schedule is declared: a worker has neither label. Pure.
///
/// Errors: [`DeclaredNamesRefused`] when a task or queue name is empty or
/// is the fallback text of its label, or a set holds more than
/// `MAX_DECLARED_NAMES` names.
pub fn worker_registry(
    tasks: &BTreeMap<String, DeclaredErrorCodes>,
    queues: &[String],
    facts: WorkerFacts,
) -> Result<WorkerRegistry, DeclaredNamesRefused> {
    let labels = RegistryLabels {
        tasks: DeclaredNames::new(tasks.keys())?,
        queues: DeclaredNames::new(queues)?,
        handlers: DeclaredNames::none(),
        schedules: DeclaredNames::none(),
        error_codes: ErrorCodeValues::new(tasks.values())?,
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
    Ok(WorkerRegistry {
        registry: Arc::new(registry),
        codes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::catalog::MetricId;
    use crate::telemetry::labels::DeclaredSource;
    use crate::test_support::telemetry::series_of;

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
        let WorkerRegistry { registry, codes } =
            worker_registry(&tasks, &queues, facts()).expect("valid names");

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

    #[test]
    fn a_queue_named_as_the_fallback_is_refused() {
        let refused = worker_registry(&BTreeMap::new(), &["(unknown)".to_owned()], facts());
        assert!(matches!(
            refused,
            Err(DeclaredNamesRefused::FallbackName {
                set: DeclaredSource::QueueNames,
                ..
            })
        ));
    }
}

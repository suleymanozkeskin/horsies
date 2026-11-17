//! The closed set of Python loggers that core records go to, and the routes
//! from Rust event targets to them.
//!
//! A core event's target is its Rust module path (for example
//! `horsies_core::broker::postgres`) unless the event names one. An event
//! that names the Python name of a logger of the set (for example
//! `horsies.db`) goes to that logger. Otherwise the routes map each target
//! prefix to the logger of the same component in Python
//! (`horsies.<component>`, `python/horsies/core/logging.py`). An event whose target
//! is neither is not forwarded: it comes from another crate.
//!
//! `horsies.sql` holds the statement log of the broker (`echo=True`): one
//! record per statement, with its name and SQL text, never its bound values.
//! It is a logger of its own, so the statement log is turned on and off
//! apart from the connection records of `horsies.db`, as Python's `echo`
//! goes to SQLAlchemy's own `sqlalchemy.engine` logger.

/// A Python logger of the `horsies` namespace.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LoggerName {
    App,
    Bridge,
    Broker,
    Cli,
    Codec,
    Core,
    Db,
    Lifecycle,
    Listener,
    Monitoring,
    Scheduler,
    SchedulerState,
    Sql,
    Web,
    Worker,
    WorkflowEngine,
    WorkflowHandle,
    WorkflowRecovery,
}

impl LoggerName {
    /// The number of loggers.
    pub const COUNT: usize = 18;

    /// Every logger, in declaration order.
    pub const ALL: [LoggerName; Self::COUNT] = [
        Self::App,
        Self::Bridge,
        Self::Broker,
        Self::Cli,
        Self::Codec,
        Self::Core,
        Self::Db,
        Self::Lifecycle,
        Self::Listener,
        Self::Monitoring,
        Self::Scheduler,
        Self::SchedulerState,
        Self::Sql,
        Self::Web,
        Self::Worker,
        Self::WorkflowEngine,
        Self::WorkflowHandle,
        Self::WorkflowRecovery,
    ];

    /// The name that `logging.getLogger` takes.
    pub const fn python_name(self) -> &'static str {
        match self {
            Self::App => "horsies.app",
            Self::Bridge => "horsies.bridge",
            Self::Broker => "horsies.broker",
            Self::Cli => "horsies.cli",
            Self::Codec => "horsies.codec",
            Self::Core => "horsies.core",
            Self::Db => "horsies.db",
            Self::Lifecycle => "horsies.lifecycle",
            Self::Listener => "horsies.listener",
            Self::Monitoring => "horsies.monitoring",
            Self::Scheduler => "horsies.scheduler",
            Self::SchedulerState => "horsies.scheduler.state",
            Self::Sql => "horsies.sql",
            Self::Web => "horsies.web",
            Self::Worker => "horsies.worker",
            Self::WorkflowEngine => "horsies.workflow.engine",
            Self::WorkflowHandle => "horsies.workflow.handle",
            Self::WorkflowRecovery => "horsies.workflow.recovery",
        }
    }

    /// The position of this logger in [`LoggerName::ALL`].
    pub const fn index(self) -> usize {
        self as usize
    }
}

/// Target prefixes and their loggers. A prefix matches a target that equals
/// it or continues with `::`. The first match wins, so a longer prefix comes
/// before a shorter one of the same module.
const TARGET_ROUTES: [(&str, LoggerName); 21] = [
    (
        "horsies_core::broker::process_listener",
        LoggerName::Listener,
    ),
    ("horsies_core::broker::listener", LoggerName::Listener),
    ("horsies_core::broker", LoggerName::Broker),
    ("horsies_core::core::lifecycle", LoggerName::Lifecycle),
    ("horsies_core::core::codec", LoggerName::Codec),
    ("horsies_core::core::config", LoggerName::App),
    ("horsies_core::monitoring", LoggerName::Monitoring),
    ("horsies_core::web", LoggerName::Web),
    (
        "horsies_core::worker::scheduler::state",
        LoggerName::SchedulerState,
    ),
    ("horsies_core::worker::scheduler", LoggerName::Scheduler),
    ("horsies_core::worker::cli", LoggerName::Cli),
    ("horsies_core::worker", LoggerName::Worker),
    (
        "horsies_core::workflow_engine::recovery",
        LoggerName::WorkflowRecovery,
    ),
    (
        "horsies_core::workflow_engine::recovery_limit",
        LoggerName::WorkflowRecovery,
    ),
    (
        "horsies_core::workflow_engine::phase2_recovery",
        LoggerName::WorkflowRecovery,
    ),
    (
        "horsies_core::workflow_engine::query",
        LoggerName::WorkflowHandle,
    ),
    ("horsies_core::workflow_engine", LoggerName::WorkflowEngine),
    // The explicit targets of the core: the claim pass of the worker, and
    // the listener of the statement layer (`horsies_core::db`).
    ("horsies::claim", LoggerName::Worker),
    ("horsies.listener", LoggerName::Listener),
    ("_core", LoggerName::Bridge),
    ("horsies_core", LoggerName::Core),
];

/// The logger of a target, or `None` for a target of another crate. A
/// target that is the Python name of a logger goes to that logger; any other
/// target goes through the prefix routes.
pub fn route(target: &str) -> Option<LoggerName> {
    LoggerName::ALL
        .into_iter()
        .find(|logger| logger.python_name() == target)
        .or_else(|| {
            TARGET_ROUTES
                .iter()
                .find(|(prefix, _)| prefix_matches(prefix, target))
                .map(|(_, logger)| *logger)
        })
}

fn prefix_matches(prefix: &str, target: &str) -> bool {
    match target.strip_prefix(prefix) {
        Some(rest) => rest.is_empty() || rest.starts_with("::"),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_logger_sits_at_its_index() {
        for (position, logger) in LoggerName::ALL.iter().enumerate() {
            assert_eq!(logger.index(), position, "{logger:?}");
        }
    }

    #[test]
    fn every_logger_is_under_the_horsies_namespace() {
        for logger in LoggerName::ALL {
            assert!(logger.python_name().starts_with("horsies."), "{logger:?}");
        }
    }

    #[test]
    fn a_module_path_goes_to_its_component() {
        let cases = [
            ("horsies_core::broker::postgres", LoggerName::Broker),
            (
                "horsies_core::broker::process_listener",
                LoggerName::Listener,
            ),
            ("horsies.listener", LoggerName::Listener),
            (
                "horsies_core::worker::scheduler::service",
                LoggerName::Scheduler,
            ),
            (
                "horsies_core::worker::scheduler::state",
                LoggerName::SchedulerState,
            ),
            ("horsies_core::worker::worker", LoggerName::Worker),
            (
                "horsies_core::workflow_engine::recovery_limit",
                LoggerName::WorkflowRecovery,
            ),
            (
                "horsies_core::workflow_engine::engine",
                LoggerName::WorkflowEngine,
            ),
            (
                "horsies_core::core::history::maintenance::pruning",
                LoggerName::Core,
            ),
            ("horsies::claim", LoggerName::Worker),
            ("_core::bridge::blocking", LoggerName::Bridge),
            // Python `horsies.monitoring.queries` logs on
            // `get_logger('monitoring')`: the cut of a bounded list.
            ("horsies_core::monitoring::queries", LoggerName::Monitoring),
        ];
        for (target, logger) in cases {
            assert_eq!(route(target), Some(logger), "{target}");
        }
    }

    #[test]
    fn a_target_named_after_a_logger_goes_to_that_logger() {
        // The statement layer's pool and pooling check (`db/pool.rs`,
        // `db/pooling_check.rs`) and its listener (`db/listener.rs`).
        assert_eq!(route("horsies.db"), Some(LoggerName::Db));
        assert_eq!(route("horsies.listener"), Some(LoggerName::Listener));
        for logger in LoggerName::ALL {
            assert_eq!(route(logger.python_name()), Some(logger), "{logger:?}");
        }
    }

    #[test]
    fn the_statement_log_goes_to_its_own_logger() {
        // The broker's statement log (`StatementLog::Info`, `echo=True`)
        // names the target `horsies.sql`.
        assert_eq!(route("horsies.sql"), Some(LoggerName::Sql));
        assert_eq!(LoggerName::Sql.python_name(), "horsies.sql");
        assert_ne!(route("horsies.sql"), route("horsies.db"));
    }

    // The records of the switched lifecycle operations reach the logger
    // that the Python caller of the same write used.

    #[test]
    fn the_stale_running_pass_logs_on_the_broker_logger() {
        // Python `PostgresBroker.mark_stale_tasks_as_failed`.
        assert_eq!(
            route(horsies_core::log_targets::STALE_RUNNING_PASS),
            Some(LoggerName::Broker)
        );
    }

    #[test]
    fn the_failed_attempt_writes_log_on_the_lifecycle_logger() {
        // Python `horsies.core.worker.failed_attempt`.
        assert_eq!(
            route(horsies_core::log_targets::FAILED_ATTEMPT),
            Some(LoggerName::Lifecycle)
        );
    }

    #[test]
    fn the_workflow_success_logs_on_the_lifecycle_logger() {
        // Python `horsies.core.worker.workflow_success`.
        assert_eq!(
            route(horsies_core::log_targets::WORKFLOW_SUCCESS),
            Some(LoggerName::Lifecycle)
        );
    }

    #[test]
    fn the_success_batch_outcomes_log_on_the_lifecycle_logger() {
        // Python `horsies.core.lifecycle.persistence`; the batch logs each
        // row through `log_terminalization_outcome`.
        assert_eq!(
            route(horsies_core::log_targets::TERMINALIZATION_OUTCOME),
            Some(LoggerName::Lifecycle)
        );
    }

    #[test]
    fn a_dotted_target_outside_the_set_is_not_forwarded() {
        assert_eq!(route("horsies.db.pool"), None);
        assert_eq!(route("horsies.sql.statement"), None);
        assert_eq!(route("horsies.unknown"), None);
    }

    #[test]
    fn a_prefix_matches_only_at_a_module_boundary() {
        assert_eq!(route("horsies_core::workers"), Some(LoggerName::Core));
        assert_eq!(route("horsies_core_extra::broker"), None);
    }

    #[test]
    fn a_target_of_another_crate_is_not_forwarded() {
        assert_eq!(route("sqlx::query"), None);
        assert_eq!(route("tokio::runtime"), None);
    }
}

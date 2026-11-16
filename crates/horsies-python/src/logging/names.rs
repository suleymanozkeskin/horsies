//! The closed set of Python loggers that core records go to, and the routes
//! from Rust event targets to them.
//!
//! A core event's target is its Rust module path (for example
//! `horsies_core::broker::postgres`) unless the event names one. The routes
//! map each target prefix to the logger of the same component in Python
//! (`horsies.<component>`, `horsies/core/logging.py`). An event whose target
//! has no route is not forwarded: it comes from another crate.

/// A Python logger of the `horsies` namespace.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LoggerName {
    App,
    Bridge,
    Broker,
    Cli,
    Codec,
    Core,
    Lifecycle,
    Listener,
    Scheduler,
    SchedulerState,
    Web,
    Worker,
    WorkflowEngine,
    WorkflowHandle,
    WorkflowRecovery,
}

impl LoggerName {
    /// The number of loggers.
    pub const COUNT: usize = 15;

    /// Every logger, in declaration order.
    pub const ALL: [LoggerName; Self::COUNT] = [
        Self::App,
        Self::Bridge,
        Self::Broker,
        Self::Cli,
        Self::Codec,
        Self::Core,
        Self::Lifecycle,
        Self::Listener,
        Self::Scheduler,
        Self::SchedulerState,
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
            Self::Lifecycle => "horsies.lifecycle",
            Self::Listener => "horsies.listener",
            Self::Scheduler => "horsies.scheduler",
            Self::SchedulerState => "horsies.scheduler.state",
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
const TARGET_ROUTES: [(&str, LoggerName); 22] = [
    (
        "horsies_core::broker::process_listener",
        LoggerName::Listener,
    ),
    ("horsies_core::broker::listener", LoggerName::Listener),
    ("horsies_core::broker", LoggerName::Broker),
    ("horsies_core::core::lifecycle", LoggerName::Lifecycle),
    ("horsies_core::core::codec", LoggerName::Codec),
    ("horsies_core::core::config", LoggerName::App),
    ("horsies_core::core::app", LoggerName::App),
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
        "horsies_core::workflow_engine::bound_handle",
        LoggerName::WorkflowHandle,
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

/// The logger of a target, or `None` for a target of another crate.
pub fn route(target: &str) -> Option<LoggerName> {
    TARGET_ROUTES
        .iter()
        .find(|(prefix, _)| prefix_matches(prefix, target))
        .map(|(_, logger)| *logger)
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
        ];
        for (target, logger) in cases {
            assert_eq!(route(target), Some(logger), "{target}");
        }
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

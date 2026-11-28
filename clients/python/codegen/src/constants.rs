//! Render integer constants of horsies-core as modules of `_catalog`
//! (`monitoring_bounds.py`, `broker_bounds.py`, `broker_pool_defaults.py`,
//! `exit_codes.py`, `child_bounds.py`, `task_bounds.py`, `queue_defaults.py`,
//! `schedule_defaults.py`, `workflow_bounds.py`). Each
//! constant is a `Final` integer, text or flag whose value is the core's constant, so a
//! Python bound cannot differ from the bound that the core enforces: `check`
//! fails when a core value changes and the module is not written again.

use horsies_core::core::config::broker::{BROKER_MAX_OVERFLOW_DEFAULT, BROKER_POOL_SIZE_DEFAULT};
use horsies_core::frame_protocol::{
    BAD_ARGUMENTS_EXIT_CODE, BAD_PARENT_FRAME_EXIT_CODE, BOOT_FAILED_EXIT_CODE,
    CHILD_HOOK_FAILED_EXIT_CODE, CORE_STOPPED_EXIT_CODE, PARENT_GONE_EXIT_CODE,
    PROTOCOL_MISMATCH_EXIT_CODE, REPARENT_FAILED_EXIT_CODE,
    STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE,
};
use horsies_core::monitoring::{
    LIST_ROW_BOUND, MAX_BREAKDOWN_LIMIT, MAX_TASK_PAGE_LIMIT, MAX_TASK_PAGE_REACH,
    MAX_WORKFLOW_RUNS_LIMIT, MONITORING_WINDOW_DEFAULT, MONITORING_WINDOW_MAX,
};
use horsies_core::{
    CANCELLED_WRITE_END, DEFAULT_MAX_CATCH_UP_RUNS, DEFAULT_QUEUE_NAME, DEFAULT_QUEUE_PRIORITY,
    DEFAULT_SCHEDULE_CHECK_INTERVAL_SECONDS, DEFAULT_SCHEDULE_ENABLED, DEFAULT_SCHEDULE_TIMEZONE,
    IDEMPOTENCY_KEY_MAX_BYTES, LIVENESS_LOCK_LOST_EXIT_CODE, MAX_CHECK_SUBWORKFLOW_BUILDS,
    MAX_TASK_TIMEOUT_MS, MIN_TASK_TIMEOUT_MS, RUNNER_HEARTBEAT_LOST_EXIT_CODE, TASK_LIST_BOUND,
};

use crate::python_text::{docstring, string_literal, GENERATED_HEADER};

/// One integer constant of a generated module.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoreConstant {
    /// The Python name.
    pub name: &'static str,
    /// The docstring under the constant.
    pub doc: &'static str,
    /// The core's value.
    pub value: CoreValue,
}

/// The value of one core constant. Each variant has its Python literal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CoreValue {
    /// A whole number, as a Python `int`.
    Integer(i64),
    /// A text, as a Python `str`.
    Text(&'static str),
    /// A yes or no, as a Python `bool`.
    Flag(bool),
}

impl CoreValue {
    /// The Python source of the value.
    pub fn python_literal(self) -> String {
        match self {
            Self::Integer(number) => number.to_string(),
            Self::Text(text) => string_literal(text),
            Self::Flag(true) => "True".to_owned(),
            Self::Flag(false) => "False".to_owned(),
        }
    }
}

/// One generated module of core constants, inside `_catalog`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConstantModule {
    /// The file name inside `_catalog`.
    pub file: &'static str,
    /// The module docstring.
    pub doc: &'static str,
    /// The constants, in module order.
    pub constants: &'static [CoreConstant],
}

const MONITORING_CONSTANTS: [CoreConstant; 7] = [
    CoreConstant {
        name: "MONITORING_WINDOW_DEFAULT_SECONDS",
        doc: "The span of the window of a read that gives no bound.",
        value: CoreValue::Integer(MONITORING_WINDOW_DEFAULT.num_seconds()),
    },
    CoreConstant {
        name: "MONITORING_WINDOW_MAX_SECONDS",
        doc: "The longest window; a longer one is refused.",
        value: CoreValue::Integer(MONITORING_WINDOW_MAX.num_seconds()),
    },
    CoreConstant {
        name: "MAX_TASK_PAGE_REACH",
        doc: "The most `offset + limit` of one task list page.",
        value: CoreValue::Integer(MAX_TASK_PAGE_REACH),
    },
    CoreConstant {
        name: "MAX_TASK_PAGE_LIMIT",
        doc: "The most rows of one task list page.",
        value: CoreValue::Integer(MAX_TASK_PAGE_LIMIT),
    },
    CoreConstant {
        name: "MAX_BREAKDOWN_LIMIT",
        doc: "The most groups of one task breakdown.",
        value: CoreValue::Integer(MAX_BREAKDOWN_LIMIT),
    },
    CoreConstant {
        name: "MAX_WORKFLOW_RUNS_LIMIT",
        doc: "The most runs of one workflow runs list.",
        value: CoreValue::Integer(MAX_WORKFLOW_RUNS_LIMIT),
    },
    CoreConstant {
        name: "LIST_ROW_BOUND",
        doc: "The most rows of the workflow names list and of the schedules list; a\n\
              longer list is cut at the bound.",
        value: CoreValue::Integer(LIST_ROW_BOUND as i64),
    },
];

/// The bounds of the monitoring reads. The window spans are whole seconds.
pub const MONITORING_BOUNDS: ConstantModule = ConstantModule {
    file: "monitoring_bounds.py",
    doc: "The bounds of the monitoring reads of horsies-core, generated from its Rust\n\
          constants. The window spans are in whole seconds.",
    constants: &MONITORING_CONSTANTS,
};

const BROKER_CONSTANTS: [CoreConstant; 1] = [CoreConstant {
    name: "TASK_LIST_ROW_BOUND",
    doc: "The most rows of the stale task list and of the expired task list; a\n\
          longer list is cut at the bound.",
    value: CoreValue::Integer(TASK_LIST_BOUND as i64),
}];

/// The bound of the stale and expired task lists of the broker.
pub const BROKER_BOUNDS: ConstantModule = ConstantModule {
    file: "broker_bounds.py",
    doc: "The bound of the task lists of the broker of horsies-core, generated from its\n\
          Rust constant.",
    constants: &BROKER_CONSTANTS,
};

const BROKER_POOL_DEFAULT_CONSTANTS: [CoreConstant; 2] = [
    CoreConstant {
        name: "BROKER_POOL_SIZE_DEFAULT",
        doc: "The persistent connections of the general broker pool of a config that\n\
              names no `pool_size`: the default of `PostgresConfig.pool_size`.",
        value: CoreValue::Integer(BROKER_POOL_SIZE_DEFAULT as i64),
    },
    CoreConstant {
        name: "BROKER_MAX_OVERFLOW_DEFAULT",
        doc: "The connections past the pool size that the general broker pool of a\n\
              config that names no `max_overflow` opens under load: the default of\n\
              `PostgresConfig.max_overflow`.",
        value: CoreValue::Integer(BROKER_MAX_OVERFLOW_DEFAULT as i64),
    },
];

/// The defaults of the general broker pool, the same in both clients.
pub const BROKER_POOL_DEFAULTS: ConstantModule = ConstantModule {
    file: "broker_pool_defaults.py",
    doc: "The defaults of the general broker pool of horsies-core, generated from its\n\
          Rust constants. Both clients have these defaults.",
    constants: &BROKER_POOL_DEFAULT_CONSTANTS,
};

const EXIT_CODE_CONSTANTS: [CoreConstant; 11] = [
    CoreConstant {
        name: "PARENT_GONE_EXIT_CODE",
        doc: "A child: the parent is gone (the socket reached EOF, or the parent-death\n\
              check found another parent).",
        value: CoreValue::Integer(PARENT_GONE_EXIT_CODE as i64),
    },
    CoreConstant {
        name: "BAD_PARENT_FRAME_EXIT_CODE",
        doc: "A child: the parent sent a frame over the bounds, or a malformed frame.",
        value: CoreValue::Integer(BAD_PARENT_FRAME_EXIT_CODE as i64),
    },
    CoreConstant {
        name: "BAD_ARGUMENTS_EXIT_CODE",
        doc: "A child: the child program got arguments it cannot use.",
        value: CoreValue::Integer(BAD_ARGUMENTS_EXIT_CODE as i64),
    },
    CoreConstant {
        name: "STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE",
        doc: "A child: after a stop frame, threads of the task loop's executor still\n\
              ran; the child left without waiting for them.",
        value: CoreValue::Integer(STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE as i64),
    },
    CoreConstant {
        name: "BOOT_FAILED_EXIT_CODE",
        doc: "A child: the boot failed, after a `boot_failed` frame that names the cause.",
        value: CoreValue::Integer(BOOT_FAILED_EXIT_CODE as i64),
    },
    CoreConstant {
        name: "REPARENT_FAILED_EXIT_CODE",
        doc: "A child: a forked child did not reach the worker as its parent in time.",
        value: CoreValue::Integer(REPARENT_FAILED_EXIT_CODE as i64),
    },
    CoreConstant {
        name: "RUNNER_HEARTBEAT_LOST_EXIT_CODE",
        doc: "The worker process: it failed closed because its runner heartbeats\n\
              stopped.",
        value: CoreValue::Integer(RUNNER_HEARTBEAT_LOST_EXIT_CODE as i64),
    },
    CoreConstant {
        name: "PROTOCOL_MISMATCH_EXIT_CODE",
        doc: "A child: the init frame named another protocol version than the child\n\
              speaks.",
        value: CoreValue::Integer(PROTOCOL_MISMATCH_EXIT_CODE as i64),
    },
    CoreConstant {
        name: "LIVENESS_LOCK_LOST_EXIT_CODE",
        doc: "The worker process: it failed closed because its liveness lock was not\n\
              held for the regain bound while it held a task.",
        value: CoreValue::Integer(LIVENESS_LOCK_LOST_EXIT_CODE as i64),
    },
    CoreConstant {
        name: "CORE_STOPPED_EXIT_CODE",
        doc: "A child: its core stopped, or the process was forked after its core\n\
              started; the child ended itself.",
        value: CoreValue::Integer(CORE_STOPPED_EXIT_CODE as i64),
    },
    CoreConstant {
        name: "CHILD_HOOK_FAILED_EXIT_CODE",
        doc: "A child: a start hook of the child failed or hung.",
        value: CoreValue::Integer(CHILD_HOOK_FAILED_EXIT_CODE as i64),
    },
];

/// The exit codes of the worker process and of its children, in code order.
pub const EXIT_CODES: ConstantModule = ConstantModule {
    file: "exit_codes.py",
    doc: "The exit codes of the worker process and of its children, generated from the\n\
          constants of horsies-core. The code of a child names how the child ended;\n\
          77 and 79 are codes of the worker process.",
    constants: &EXIT_CODE_CONSTANTS,
};

const CHILD_CONSTANTS: [CoreConstant; 1] = [CoreConstant {
    name: "CANCELLED_WRITE_END_SECONDS",
    doc: "The longest end of a write of a child after its bound: the cancel request\n\
          of the statement and the wait for its reply.",
    value: CoreValue::Integer(CANCELLED_WRITE_END.as_secs() as i64),
}];

/// The bounds of the writes of a worker child. The spans are whole seconds.
pub const CHILD_BOUNDS: ConstantModule = ConstantModule {
    file: "child_bounds.py",
    doc: "The bounds of the writes of a worker child in horsies-core, generated from its\n\
          Rust constants. The spans are in whole seconds.",
    constants: &CHILD_CONSTANTS,
};

const TASK_BOUND_CONSTANTS: [CoreConstant; 3] = [
    CoreConstant {
        name: "MIN_TASK_TIMEOUT_MS",
        doc: "The smallest `TaskOptions.timeout_ms`.",
        value: CoreValue::Integer(MIN_TASK_TIMEOUT_MS as i64),
    },
    CoreConstant {
        name: "MAX_TASK_TIMEOUT_MS",
        doc: "The largest `TaskOptions.timeout_ms`, and the largest wait of a result get.

\
              About 49.7 days. It is the limit of the Rust port's `u32` field, so both
\
              clients refuse the same values.",
        value: CoreValue::Integer(MAX_TASK_TIMEOUT_MS as i64),
    },
    CoreConstant {
        name: "IDEMPOTENCY_KEY_MAX_BYTES",
        doc: "The most UTF-8 bytes of an idempotency key; a key has at least one byte.",
        value: CoreValue::Integer(IDEMPOTENCY_KEY_MAX_BYTES as i64),
    },
];

/// The bounds of a task timeout and of an idempotency key.
pub const TASK_BOUNDS: ConstantModule = ConstantModule {
    file: "task_bounds.py",
    doc: "The bounds of a task timeout and of an idempotency key of horsies-core,
\
          generated from its Rust constants.",
    constants: &TASK_BOUND_CONSTANTS,
};

const QUEUE_DEFAULT_CONSTANTS: [CoreConstant; 2] = [
    CoreConstant {
        name: "DEFAULT_QUEUE_NAME",
        doc: "The name of the one queue of `QueueMode.DEFAULT`.",
        value: CoreValue::Text(DEFAULT_QUEUE_NAME),
    },
    CoreConstant {
        name: "DEFAULT_QUEUE_PRIORITY",
        doc: "The priority of the queue of `QueueMode.DEFAULT`, and of a task that
\
              resolves no queue priority: 100, the last priority.",
        value: CoreValue::Integer(DEFAULT_QUEUE_PRIORITY as i64),
    },
];

/// The default route of a task.
pub const QUEUE_DEFAULTS: ConstantModule = ConstantModule {
    file: "queue_defaults.py",
    doc: "The default queue route of horsies-core, generated from its Rust constants.",
    constants: &QUEUE_DEFAULT_CONSTANTS,
};

const SCHEDULE_DEFAULT_CONSTANTS: [CoreConstant; 4] = [
    CoreConstant {
        name: "DEFAULT_SCHEDULE_ENABLED",
        doc: "The default of `TaskSchedule.enabled`.",
        value: CoreValue::Flag(DEFAULT_SCHEDULE_ENABLED),
    },
    CoreConstant {
        name: "DEFAULT_SCHEDULE_TIMEZONE",
        doc: "The default of `TaskSchedule.timezone`.",
        value: CoreValue::Text(DEFAULT_SCHEDULE_TIMEZONE),
    },
    CoreConstant {
        name: "DEFAULT_MAX_CATCH_UP_RUNS",
        doc: "The default of `TaskSchedule.max_catch_up_runs`.",
        value: CoreValue::Integer(DEFAULT_MAX_CATCH_UP_RUNS as i64),
    },
    CoreConstant {
        name: "DEFAULT_SCHEDULE_CHECK_INTERVAL_SECONDS",
        doc: "The default of `ScheduleConfig.check_interval_seconds`.",
        value: CoreValue::Integer(DEFAULT_SCHEDULE_CHECK_INTERVAL_SECONDS as i64),
    },
];

/// The defaults of a task schedule.
pub const SCHEDULE_DEFAULTS: ConstantModule = ConstantModule {
    file: "schedule_defaults.py",
    doc: "The defaults of a task schedule of horsies-core, generated from its Rust
\
          constants.",
    constants: &SCHEDULE_DEFAULT_CONSTANTS,
};

const WORKFLOW_BOUND_CONSTANTS: [CoreConstant; 1] = [CoreConstant {
    name: "MAX_CHECK_SUBWORKFLOW_BUILDS",
    doc: "The most builds of child workflows in the nesting walk of `check` for one\n\
          root workflow. At the bound the walk stops and `check` reports `HRS-039`.",
    value: CoreValue::Integer(MAX_CHECK_SUBWORKFLOW_BUILDS as i64),
}];

/// The bound of the nesting walk of `check`.
pub const WORKFLOW_BOUNDS: ConstantModule = ConstantModule {
    file: "workflow_bounds.py",
    doc: "The bound of the nesting walk of `check` in horsies-core, generated from its\n\
          Rust constant.",
    constants: &WORKFLOW_BOUND_CONSTANTS,
};

/// Every generated module of core constants, in file order.
pub const CONSTANT_MODULES: [ConstantModule; 9] = [
    MONITORING_BOUNDS,
    BROKER_BOUNDS,
    BROKER_POOL_DEFAULTS,
    EXIT_CODES,
    CHILD_BOUNDS,
    TASK_BOUNDS,
    QUEUE_DEFAULTS,
    SCHEDULE_DEFAULTS,
    WORKFLOW_BOUNDS,
];

/// The text of one module of core constants.
pub fn render_constant_module(module: &ConstantModule) -> String {
    let mut text = format!("{GENERATED_HEADER}\n");
    if let Some(doc) = docstring(module.doc, "") {
        text.push_str(&doc);
    }
    text.push_str(
        "\nfrom __future__ import annotations\n\nfrom typing import Final\n\n__all__ = [\n",
    );
    for constant in module.constants {
        text.push_str(&format!("    {},\n", string_literal(constant.name)));
    }
    text.push_str("]\n");
    for constant in module.constants {
        text.push_str(&format!(
            "\n{}: Final = {}\n",
            constant.name,
            constant.value.python_literal()
        ));
        if let Some(doc) = docstring(constant.doc, "") {
            text.push_str(&doc);
        }
    }
    text
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn a_module_lists_its_constants_and_gives_each_its_value_and_doc() {
        let module = ConstantModule {
            file: "bounds.py",
            doc: "Bounds.",
            constants: &[
                CoreConstant {
                    name: "MAX_ROWS",
                    doc: "The most rows.",
                    value: CoreValue::Integer(1_000),
                },
                CoreConstant {
                    name: "SPAN_SECONDS",
                    doc: "",
                    value: CoreValue::Integer(86_400),
                },
            ],
        };
        assert_eq!(
            render_constant_module(&module),
            format!(
                "{GENERATED_HEADER}\n\"\"\"Bounds.\"\"\"\n\nfrom __future__ import annotations\n\n\
                 from typing import Final\n\n__all__ = [\n    'MAX_ROWS',\n    'SPAN_SECONDS',\n]\n\n\
                 MAX_ROWS: Final = 1000\n\"\"\"The most rows.\"\"\"\n\nSPAN_SECONDS: Final = 86400\n"
            )
        );
    }

    #[test]
    fn each_module_holds_the_core_values() {
        let rendered = render_constant_module(&MONITORING_BOUNDS);
        for (name, value) in [
            (
                "MONITORING_WINDOW_DEFAULT_SECONDS",
                MONITORING_WINDOW_DEFAULT.num_seconds(),
            ),
            (
                "MONITORING_WINDOW_MAX_SECONDS",
                MONITORING_WINDOW_MAX.num_seconds(),
            ),
            ("MAX_TASK_PAGE_REACH", MAX_TASK_PAGE_REACH),
            ("MAX_TASK_PAGE_LIMIT", MAX_TASK_PAGE_LIMIT),
            ("MAX_BREAKDOWN_LIMIT", MAX_BREAKDOWN_LIMIT),
            ("MAX_WORKFLOW_RUNS_LIMIT", MAX_WORKFLOW_RUNS_LIMIT),
            ("LIST_ROW_BOUND", LIST_ROW_BOUND as i64),
        ] {
            assert!(
                rendered.contains(&format!("\n{name}: Final = {value}\n")),
                "{name}"
            );
        }
        let rendered = render_constant_module(&BROKER_BOUNDS);
        assert!(rendered.contains(&format!(
            "\nTASK_LIST_ROW_BOUND: Final = {TASK_LIST_BOUND}\n"
        )));
        let rendered = render_constant_module(&BROKER_POOL_DEFAULTS);
        for (name, value) in [
            ("BROKER_POOL_SIZE_DEFAULT", BROKER_POOL_SIZE_DEFAULT),
            ("BROKER_MAX_OVERFLOW_DEFAULT", BROKER_MAX_OVERFLOW_DEFAULT),
        ] {
            assert!(
                rendered.contains(&format!("\n{name}: Final = {value}\n")),
                "{name}"
            );
        }
        let rendered = render_constant_module(&EXIT_CODES);
        for (name, value) in [
            ("PARENT_GONE_EXIT_CODE", PARENT_GONE_EXIT_CODE),
            ("BAD_PARENT_FRAME_EXIT_CODE", BAD_PARENT_FRAME_EXIT_CODE),
            ("BAD_ARGUMENTS_EXIT_CODE", BAD_ARGUMENTS_EXIT_CODE),
            (
                "STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE",
                STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE,
            ),
            ("BOOT_FAILED_EXIT_CODE", BOOT_FAILED_EXIT_CODE),
            ("REPARENT_FAILED_EXIT_CODE", REPARENT_FAILED_EXIT_CODE),
            (
                "RUNNER_HEARTBEAT_LOST_EXIT_CODE",
                RUNNER_HEARTBEAT_LOST_EXIT_CODE,
            ),
            ("PROTOCOL_MISMATCH_EXIT_CODE", PROTOCOL_MISMATCH_EXIT_CODE),
            ("LIVENESS_LOCK_LOST_EXIT_CODE", LIVENESS_LOCK_LOST_EXIT_CODE),
            ("CORE_STOPPED_EXIT_CODE", CORE_STOPPED_EXIT_CODE),
            ("CHILD_HOOK_FAILED_EXIT_CODE", CHILD_HOOK_FAILED_EXIT_CODE),
        ] {
            assert!(
                rendered.contains(&format!("\n{name}: Final = {value}\n")),
                "{name}"
            );
        }
    }

    /// The exit code module names every code of a child of the protocol,
    /// once, and the two codes of the worker process.
    #[test]
    fn the_exit_codes_are_every_code_of_the_core_and_distinct() {
        let values: BTreeSet<CoreValue> = EXIT_CODES
            .constants
            .iter()
            .map(|constant| constant.value)
            .collect();
        assert_eq!(values.len(), EXIT_CODES.constants.len());
        let mut expected: BTreeSet<CoreValue> = horsies_core::frame_protocol::ChildExitCode::ALL
            .into_iter()
            .map(|code| CoreValue::Integer(i64::from(code.code())))
            .collect();
        expected.insert(CoreValue::Integer(i64::from(
            RUNNER_HEARTBEAT_LOST_EXIT_CODE,
        )));
        expected.insert(CoreValue::Integer(i64::from(LIVENESS_LOCK_LOST_EXIT_CODE)));
        assert_eq!(values, expected);
    }

    /// A span in whole seconds loses nothing of the core's span.
    #[test]
    fn the_window_spans_are_whole_seconds() {
        assert_eq!(MONITORING_WINDOW_DEFAULT.subsec_nanos(), 0);
        assert_eq!(MONITORING_WINDOW_MAX.subsec_nanos(), 0);
        assert_eq!(CANCELLED_WRITE_END.subsec_nanos(), 0);
    }

    #[test]
    fn the_child_bounds_hold_the_core_spans() {
        let rendered = render_constant_module(&CHILD_BOUNDS);
        assert!(rendered.contains(&format!(
            "\nCANCELLED_WRITE_END_SECONDS: Final = {}\n",
            CANCELLED_WRITE_END.as_secs()
        )));
    }

    /// Each new export is rendered with the core's value, the one source.
    #[test]
    fn the_task_bounds_queue_and_schedule_defaults_hold_the_core_values() {
        let rendered = render_constant_module(&TASK_BOUNDS);
        for (name, value) in [
            ("MIN_TASK_TIMEOUT_MS", MIN_TASK_TIMEOUT_MS as i64),
            ("MAX_TASK_TIMEOUT_MS", MAX_TASK_TIMEOUT_MS as i64),
            (
                "IDEMPOTENCY_KEY_MAX_BYTES",
                IDEMPOTENCY_KEY_MAX_BYTES as i64,
            ),
        ] {
            assert!(
                rendered.contains(&format!("\n{name}: Final = {value}\n")),
                "{name}"
            );
        }
        assert_eq!(MIN_TASK_TIMEOUT_MS, 1_000);
        assert_eq!(MAX_TASK_TIMEOUT_MS, u64::from(u32::MAX));
        assert_eq!(IDEMPOTENCY_KEY_MAX_BYTES, 255);
        let rendered = render_constant_module(&QUEUE_DEFAULTS);
        assert!(rendered.contains("\nDEFAULT_QUEUE_NAME: Final = 'default'\n"));
        assert!(rendered.contains("\nDEFAULT_QUEUE_PRIORITY: Final = 100\n"));
        let rendered = render_constant_module(&SCHEDULE_DEFAULTS);
        assert!(rendered.contains("\nDEFAULT_SCHEDULE_ENABLED: Final = True\n"));
        assert!(rendered.contains("\nDEFAULT_SCHEDULE_TIMEZONE: Final = 'UTC'\n"));
        assert!(rendered.contains("\nDEFAULT_MAX_CATCH_UP_RUNS: Final = 100\n"));
    }

    #[test]
    fn the_workflow_bounds_hold_the_core_check_build_bound() {
        let rendered = render_constant_module(&WORKFLOW_BOUNDS);
        assert!(rendered.contains(&format!(
            "\nMAX_CHECK_SUBWORKFLOW_BUILDS: Final = {MAX_CHECK_SUBWORKFLOW_BUILDS}\n"
        )));
        assert_eq!(MAX_CHECK_SUBWORKFLOW_BUILDS, 1_000);
    }

    #[test]
    fn names_and_files_are_distinct() {
        let files: BTreeSet<&str> = CONSTANT_MODULES.iter().map(|module| module.file).collect();
        assert_eq!(files.len(), CONSTANT_MODULES.len());
        for module in &CONSTANT_MODULES {
            let names: BTreeSet<&str> = module
                .constants
                .iter()
                .map(|constant| constant.name)
                .collect();
            assert_eq!(names.len(), module.constants.len(), "{}", module.file);
        }
    }
}

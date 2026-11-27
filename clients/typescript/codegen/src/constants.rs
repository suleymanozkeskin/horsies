//! Renders integer constants of horsies-core as modules of the catalog
//! folder: `monitoring_bounds.ts`, `exit_codes.ts`, `event_bounds.ts` and
//! `broker_bounds.ts`.
//!
//! `CONSTANT_MODULES` is the list of the core constants that the
//! TypeScript package publishes. The list is a choice of this renderer. Each
//! entry names the core constant itself, so the value comes from the core:
//! `check` fails when a core value changes and the module is not written
//! again.
//!
//! Each constant is one statement, `export const NAME = <literal>;`. An
//! integer in the safe range of a `number` is a number literal; another
//! integer is a `bigint` literal.

use std::time::Duration;

use horsies_core::events::{
    MAX_ATTEMPT, MAX_EVENT_CUSTOM_CODE_CHARS, MAX_EVENT_QUEUE_NAME_CHARS,
    MAX_EVENT_TASK_NAME_CHARS, MAX_EVENT_TRACE_CARRIER_CHARS, MAX_EVENT_WORKER_ID_CHARS,
    MAX_RETRY_COUNT,
};
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
    LIVENESS_LOCK_LOST_EXIT_CODE, RUNNER_HEARTBEAT_LOST_EXIT_CODE, TASK_LIST_BOUND,
};

use crate::output::{catalog_file, GeneratedFile};
use crate::ts_text::{bigint_literal, integer_literal, is_safe_integer, module_text};

/// The suffix of the name of a span that a module publishes in whole
/// seconds.
const SECONDS_SUFFIX: &str = "_SECONDS";

/// One constant that a module publishes, with the core value that it names.
///
/// The TypeScript type is not stored: it follows from the value. The
/// renderer adds the `_SECONDS` suffix to the name of a span, and to no
/// other name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishedConstant {
    /// An integer of the core (a count, a bound or an exit code), published
    /// under `name`. `value` is the core integer widened to `i128`, which
    /// holds every integer type of the core constants without loss.
    Integer { name: &'static str, value: i128 },
    /// A span of the core, published in whole seconds under
    /// `<name>_SECONDS`.
    WholeSeconds { name: &'static str, span: Duration },
}

/// One generated module of core constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConstantModule {
    /// The stem of the module in the catalog folder.
    stem: &'static str,
    /// The constants, in module order.
    constants: &'static [PublishedConstant],
}

const MONITORING_CONSTANTS: [PublishedConstant; 7] = [
    PublishedConstant::WholeSeconds {
        name: "MONITORING_WINDOW_DEFAULT",
        span: match MONITORING_WINDOW_DEFAULT.to_std() {
            Ok(span) => span,
            Err(_) => panic!("MONITORING_WINDOW_DEFAULT is a negative span"),
        },
    },
    PublishedConstant::WholeSeconds {
        name: "MONITORING_WINDOW_MAX",
        span: match MONITORING_WINDOW_MAX.to_std() {
            Ok(span) => span,
            Err(_) => panic!("MONITORING_WINDOW_MAX is a negative span"),
        },
    },
    PublishedConstant::Integer {
        name: "MAX_TASK_PAGE_REACH",
        value: MAX_TASK_PAGE_REACH as i128,
    },
    PublishedConstant::Integer {
        name: "MAX_TASK_PAGE_LIMIT",
        value: MAX_TASK_PAGE_LIMIT as i128,
    },
    PublishedConstant::Integer {
        name: "MAX_BREAKDOWN_LIMIT",
        value: MAX_BREAKDOWN_LIMIT as i128,
    },
    PublishedConstant::Integer {
        name: "MAX_WORKFLOW_RUNS_LIMIT",
        value: MAX_WORKFLOW_RUNS_LIMIT as i128,
    },
    PublishedConstant::Integer {
        name: "LIST_ROW_BOUND",
        value: LIST_ROW_BOUND as i128,
    },
];

/// The bounds of the monitoring reads. The window spans are whole seconds.
const MONITORING_BOUNDS: ConstantModule = ConstantModule {
    stem: "monitoring_bounds",
    constants: &MONITORING_CONSTANTS,
};

const EXIT_CODE_CONSTANTS: [PublishedConstant; 11] = [
    PublishedConstant::Integer {
        name: "PARENT_GONE_EXIT_CODE",
        value: PARENT_GONE_EXIT_CODE as i128,
    },
    PublishedConstant::Integer {
        name: "BAD_PARENT_FRAME_EXIT_CODE",
        value: BAD_PARENT_FRAME_EXIT_CODE as i128,
    },
    PublishedConstant::Integer {
        name: "BAD_ARGUMENTS_EXIT_CODE",
        value: BAD_ARGUMENTS_EXIT_CODE as i128,
    },
    PublishedConstant::Integer {
        name: "STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE",
        value: STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE as i128,
    },
    PublishedConstant::Integer {
        name: "BOOT_FAILED_EXIT_CODE",
        value: BOOT_FAILED_EXIT_CODE as i128,
    },
    PublishedConstant::Integer {
        name: "REPARENT_FAILED_EXIT_CODE",
        value: REPARENT_FAILED_EXIT_CODE as i128,
    },
    PublishedConstant::Integer {
        name: "RUNNER_HEARTBEAT_LOST_EXIT_CODE",
        value: RUNNER_HEARTBEAT_LOST_EXIT_CODE as i128,
    },
    PublishedConstant::Integer {
        name: "PROTOCOL_MISMATCH_EXIT_CODE",
        value: PROTOCOL_MISMATCH_EXIT_CODE as i128,
    },
    PublishedConstant::Integer {
        name: "LIVENESS_LOCK_LOST_EXIT_CODE",
        value: LIVENESS_LOCK_LOST_EXIT_CODE as i128,
    },
    PublishedConstant::Integer {
        name: "CORE_STOPPED_EXIT_CODE",
        value: CORE_STOPPED_EXIT_CODE as i128,
    },
    PublishedConstant::Integer {
        name: "CHILD_HOOK_FAILED_EXIT_CODE",
        value: CHILD_HOOK_FAILED_EXIT_CODE as i128,
    },
];

/// The exit codes of the worker process and of its children, in code order.
const EXIT_CODES: ConstantModule = ConstantModule {
    stem: "exit_codes",
    constants: &EXIT_CODE_CONSTANTS,
};

const EVENT_CONSTANTS: [PublishedConstant; 7] = [
    PublishedConstant::Integer {
        name: "MAX_ATTEMPT",
        value: MAX_ATTEMPT as i128,
    },
    PublishedConstant::Integer {
        name: "MAX_RETRY_COUNT",
        value: MAX_RETRY_COUNT as i128,
    },
    PublishedConstant::Integer {
        name: "MAX_EVENT_TASK_NAME_CHARS",
        value: MAX_EVENT_TASK_NAME_CHARS as i128,
    },
    PublishedConstant::Integer {
        name: "MAX_EVENT_QUEUE_NAME_CHARS",
        value: MAX_EVENT_QUEUE_NAME_CHARS as i128,
    },
    PublishedConstant::Integer {
        name: "MAX_EVENT_CUSTOM_CODE_CHARS",
        value: MAX_EVENT_CUSTOM_CODE_CHARS as i128,
    },
    PublishedConstant::Integer {
        name: "MAX_EVENT_TRACE_CARRIER_CHARS",
        value: MAX_EVENT_TRACE_CARRIER_CHARS as i128,
    },
    PublishedConstant::Integer {
        name: "MAX_EVENT_WORKER_ID_CHARS",
        value: MAX_EVENT_WORKER_ID_CHARS as i128,
    },
];

/// The bounds of the values of an event: the attempt number, the retry
/// count, and the most characters of each text.
const EVENT_BOUNDS: ConstantModule = ConstantModule {
    stem: "event_bounds",
    constants: &EVENT_CONSTANTS,
};

const BROKER_CONSTANTS: [PublishedConstant; 1] = [PublishedConstant::Integer {
    name: "TASK_LIST_BOUND",
    value: TASK_LIST_BOUND as i128,
}];

/// The bound of the stale and expired task reads of the broker. The name is
/// the name of the core's constant.
const BROKER_BOUNDS: ConstantModule = ConstantModule {
    stem: "broker_bounds",
    constants: &BROKER_CONSTANTS,
};

/// Every generated module of core constants, in file order.
const CONSTANT_MODULES: [ConstantModule; 4] =
    [MONITORING_BOUNDS, EXIT_CODES, EVENT_BOUNDS, BROKER_BOUNDS];

/// The modules of core constants, in file order. Pure.
///
/// Panics: a core span is not whole seconds (see `constant_statement`).
pub fn generated_files() -> Vec<GeneratedFile> {
    CONSTANT_MODULES.iter().map(constant_module_file).collect()
}

/// The file of one module: one statement per constant, in list order. Pure.
fn constant_module_file(module: &ConstantModule) -> GeneratedFile {
    let sections: Vec<String> = module.constants.iter().map(constant_statement).collect();
    catalog_file(module.stem, module_text(&sections))
}

/// The statement of one constant: `export const NAME = <literal>;`. Pure.
///
/// Panics: a span has a fraction of a second. The module cannot publish it
/// in whole seconds without loss; a core change that the list does not
/// follow is a defect.
fn constant_statement(constant: &PublishedConstant) -> String {
    let (name, value) = match *constant {
        PublishedConstant::Integer { name, value } => (name.to_owned(), value),
        PublishedConstant::WholeSeconds { name, span } => {
            assert_eq!(
                span.subsec_nanos(),
                0,
                "the span {name} ({span:?}) is not whole seconds"
            );
            (
                format!("{name}{SECONDS_SUFFIX}"),
                i128::from(span.as_secs()),
            )
        }
    };
    format!("export const {name} = {};", integer_value_literal(value))
}

/// A number literal when a `number` holds `value` exactly, else a `bigint`
/// literal. Pure.
fn integer_value_literal(value: i128) -> String {
    match is_safe_integer(value) {
        true => integer_literal(value),
        false => bigint_literal(value),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::Path;

    use horsies_core::frame_protocol::ChildExitCode;

    use super::*;
    use crate::ts_text::{GENERATED_HEADER, MAX_SAFE_INTEGER};

    /// The statement of an integer constant as the modules hold it.
    fn integer_statement(name: &str, value: i128) -> String {
        format!("\nexport const {name} = {};\n", integer_literal(value))
    }

    /// The text of the module of `stem`.
    fn module_of(stem: &str) -> String {
        generated_files()
            .into_iter()
            .find(|file| file.relative_path() == Path::new(&format!("src/_catalog/{stem}.ts")))
            .map(|file| file.text().to_owned())
            .expect("the module is generated")
    }

    #[test]
    fn a_module_has_the_header_and_one_statement_per_constant() {
        static SAMPLE_CONSTANTS: [PublishedConstant; 2] = [
            PublishedConstant::Integer {
                name: "MAX_ROWS",
                value: 1_000,
            },
            PublishedConstant::WholeSeconds {
                name: "SPAN",
                span: Duration::from_secs(86_400),
            },
        ];
        let module = ConstantModule {
            stem: "bounds",
            constants: &SAMPLE_CONSTANTS,
        };

        let file = constant_module_file(&module);

        assert_eq!(file.relative_path(), Path::new("src/_catalog/bounds.ts"));
        assert_eq!(
            file.text(),
            format!(
                "{GENERATED_HEADER}\n\nexport const MAX_ROWS = 1000;\n\n\
                 export const SPAN_SECONDS = 86_400;\n"
            )
        );
    }

    #[test]
    fn an_integer_in_the_safe_range_is_a_number() {
        assert_eq!(
            constant_statement(&PublishedConstant::Integer {
                name: "LARGEST",
                value: MAX_SAFE_INTEGER,
            }),
            "export const LARGEST = 9_007_199_254_740_991;"
        );
        assert_eq!(
            constant_statement(&PublishedConstant::Integer {
                name: "SMALLEST",
                value: -MAX_SAFE_INTEGER,
            }),
            "export const SMALLEST = -9_007_199_254_740_991;"
        );
    }

    #[test]
    fn an_integer_past_the_safe_range_is_a_bigint() {
        assert_eq!(
            constant_statement(&PublishedConstant::Integer {
                name: "OVER",
                value: MAX_SAFE_INTEGER + 1,
            }),
            "export const OVER = 9_007_199_254_740_992n;"
        );
        assert_eq!(
            constant_statement(&PublishedConstant::Integer {
                name: "UNDER",
                value: -MAX_SAFE_INTEGER - 1,
            }),
            "export const UNDER = -9_007_199_254_740_992n;"
        );
    }

    #[test]
    fn a_span_is_published_in_whole_seconds_under_a_seconds_name() {
        assert_eq!(
            constant_statement(&PublishedConstant::WholeSeconds {
                name: "WINDOW",
                span: Duration::from_secs(30 * 86_400),
            }),
            "export const WINDOW_SECONDS = 2_592_000;"
        );
    }

    #[test]
    #[should_panic(expected = "the span WINDOW (1.5s) is not whole seconds")]
    fn a_span_with_a_fraction_of_a_second_is_a_defect() {
        let _statement = constant_statement(&PublishedConstant::WholeSeconds {
            name: "WINDOW",
            span: Duration::from_millis(1_500),
        });
    }

    #[test]
    fn the_monitoring_bounds_hold_the_core_values() {
        let text = module_of("monitoring_bounds");
        let window_default = MONITORING_WINDOW_DEFAULT
            .to_std()
            .expect("the default window is a positive span");
        let window_max = MONITORING_WINDOW_MAX
            .to_std()
            .expect("the longest window is a positive span");
        for (name, value) in [
            (
                "MONITORING_WINDOW_DEFAULT_SECONDS",
                i128::from(window_default.as_secs()),
            ),
            (
                "MONITORING_WINDOW_MAX_SECONDS",
                i128::from(window_max.as_secs()),
            ),
            ("MAX_TASK_PAGE_REACH", i128::from(MAX_TASK_PAGE_REACH)),
            ("MAX_TASK_PAGE_LIMIT", i128::from(MAX_TASK_PAGE_LIMIT)),
            ("MAX_BREAKDOWN_LIMIT", i128::from(MAX_BREAKDOWN_LIMIT)),
            (
                "MAX_WORKFLOW_RUNS_LIMIT",
                i128::from(MAX_WORKFLOW_RUNS_LIMIT),
            ),
            ("LIST_ROW_BOUND", LIST_ROW_BOUND as i128),
        ] {
            assert!(text.contains(&integer_statement(name, value)), "{name}");
        }
        assert_eq!(text.matches("export const ").count(), 7);
    }

    #[test]
    fn the_exit_codes_hold_the_core_values() {
        let text = module_of("exit_codes");
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
                text.contains(&integer_statement(name, i128::from(value))),
                "{name}"
            );
        }
        assert_eq!(text.matches("export const ").count(), 11);
    }

    /// The exit code module names every code of a child of the protocol,
    /// once, and the two codes of the worker process.
    #[test]
    fn the_exit_codes_are_every_code_of_the_core_and_distinct() {
        let values: BTreeSet<i128> = EXIT_CODES
            .constants
            .iter()
            .map(|constant| match *constant {
                PublishedConstant::Integer { value, .. } => value,
                PublishedConstant::WholeSeconds { name, .. } => {
                    panic!("the exit code {name} is not a span")
                }
            })
            .collect();
        assert_eq!(values.len(), EXIT_CODES.constants.len());
        let mut expected: BTreeSet<i128> = ChildExitCode::ALL
            .into_iter()
            .map(|code| i128::from(code.code()))
            .collect();
        expected.insert(i128::from(RUNNER_HEARTBEAT_LOST_EXIT_CODE));
        expected.insert(i128::from(LIVENESS_LOCK_LOST_EXIT_CODE));
        assert_eq!(values, expected);
    }

    #[test]
    fn the_event_bounds_hold_the_core_values() {
        let text = module_of("event_bounds");
        for (name, value) in [
            ("MAX_ATTEMPT", i128::from(MAX_ATTEMPT)),
            ("MAX_RETRY_COUNT", i128::from(MAX_RETRY_COUNT)),
            (
                "MAX_EVENT_TASK_NAME_CHARS",
                MAX_EVENT_TASK_NAME_CHARS as i128,
            ),
            (
                "MAX_EVENT_QUEUE_NAME_CHARS",
                MAX_EVENT_QUEUE_NAME_CHARS as i128,
            ),
            (
                "MAX_EVENT_CUSTOM_CODE_CHARS",
                MAX_EVENT_CUSTOM_CODE_CHARS as i128,
            ),
            (
                "MAX_EVENT_TRACE_CARRIER_CHARS",
                MAX_EVENT_TRACE_CARRIER_CHARS as i128,
            ),
            (
                "MAX_EVENT_WORKER_ID_CHARS",
                MAX_EVENT_WORKER_ID_CHARS as i128,
            ),
        ] {
            assert!(text.contains(&integer_statement(name, value)), "{name}");
        }
        assert_eq!(text.matches("export const ").count(), 7);
    }

    #[test]
    fn the_broker_bounds_hold_the_task_list_bound() {
        assert_eq!(
            module_of("broker_bounds"),
            format!(
                "{GENERATED_HEADER}\n{}",
                integer_statement("TASK_LIST_BOUND", TASK_LIST_BOUND as i128)
            )
        );
    }

    #[test]
    fn the_files_are_the_four_modules_in_order_with_distinct_names() {
        let paths: Vec<String> = generated_files()
            .iter()
            .map(|file| file.relative_path().display().to_string())
            .collect();
        assert_eq!(
            paths,
            [
                "src/_catalog/monitoring_bounds.ts",
                "src/_catalog/exit_codes.ts",
                "src/_catalog/event_bounds.ts",
                "src/_catalog/broker_bounds.ts",
            ]
        );
        for module in &CONSTANT_MODULES {
            let names: BTreeSet<&str> = module
                .constants
                .iter()
                .map(|constant| match *constant {
                    PublishedConstant::Integer { name, .. }
                    | PublishedConstant::WholeSeconds { name, .. } => name,
                })
                .collect();
            assert_eq!(names.len(), module.constants.len(), "{}", module.stem);
        }
    }
}

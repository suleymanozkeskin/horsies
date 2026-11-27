//! Renders constants of horsies-core as modules of the catalog folder:
//! `monitoring_bounds.ts`, `exit_codes.ts`, `event_bounds.ts`,
//! `broker_bounds.ts`, `codec_bounds.ts`, `send_bounds.ts` and
//! `schedule_defaults.ts`.
//!
//! `constant_modules` is the list of the core constants that the
//! TypeScript package publishes. The list is a choice of this renderer. Each
//! entry names the core constant itself, so the value comes from the core:
//! `check` fails when a core value changes and the module is not written
//! again.
//!
//! Each constant is one statement, `export const NAME = <literal>;`. An
//! integer in the safe range of a `number` is a number literal; another
//! integer is a `bigint` literal. A text is a string literal, a flag is
//! `true` or `false`, and a list of texts is a frozen array of string
//! literals.

use std::time::Duration;

use horsies_core::core::codec::user_value::{BUILTIN_CODE_KEY, ENGINE_KEY_PREFIXES};
use horsies_core::core::codec::{
    UserValueRefused, MAX_INT_DIGITS, MAX_JSON_DEPTH, MAX_VALUE_JSON_DEPTH,
};
use horsies_core::core::task::error::{
    DATA_REFUSED_KEY, ERROR_DATA_DEPTH_LIMIT, SUMMARY_OUTPUT_DEPTH_LIMIT,
};
use horsies_core::core::task::result::TASK_RESULT_ENVELOPE_KEY;
use horsies_core::events::{
    MAX_ATTEMPT, MAX_EVENT_CUSTOM_CODE_CHARS, MAX_EVENT_QUEUE_NAME_CHARS,
    MAX_EVENT_TASK_NAME_CHARS, MAX_EVENT_TRACE_CARRIER_CHARS, MAX_EVENT_WORKER_ID_CHARS,
    MAX_RETRY_COUNT,
};
use horsies_core::frame_protocol::{
    BAD_ARGUMENTS_EXIT_CODE, BAD_PARENT_FRAME_EXIT_CODE, BOOT_FAILED_EXIT_CODE,
    CHILD_HOOK_FAILED_EXIT_CODE, CORE_STOPPED_EXIT_CODE, MAX_EVENT_HANDLERS,
    MAX_HANDLER_NAME_CHARS, PARENT_GONE_EXIT_CODE, PROTOCOL_MISMATCH_EXIT_CODE,
    REPARENT_FAILED_EXIT_CODE, STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE,
};
use horsies_core::monitoring::{
    LIST_ROW_BOUND, MAX_BREAKDOWN_LIMIT, MAX_TASK_PAGE_LIMIT, MAX_TASK_PAGE_REACH,
    MAX_WORKFLOW_RUNS_LIMIT, MONITORING_WINDOW_DEFAULT, MONITORING_WINDOW_MAX,
};
use horsies_core::telemetry::labels::DeclaredSource;
use horsies_core::{
    DEFAULT_MAX_CATCH_UP_RUNS, DEFAULT_QUEUE_NAME, DEFAULT_QUEUE_PRIORITY,
    DEFAULT_SCHEDULE_ENABLED, DEFAULT_SCHEDULE_TIMEZONE, IDEMPOTENCY_KEY_MAX_BYTES,
    LIVENESS_LOCK_LOST_EXIT_CODE, MAX_WAIT_LIMIT_MS, RUNNER_HEARTBEAT_LOST_EXIT_CODE,
    TASK_LIST_BOUND,
};

use crate::output::{catalog_file, GeneratedFile};
use crate::ts_text::{
    bigint_literal, integer_literal, is_safe_integer, module_text, string_literal,
};

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
    /// A text of the core, published under `name` as a string literal: its
    /// TypeScript type is the literal type of the text.
    Text {
        name: &'static str,
        value: &'static str,
    },
    /// A flag of the core, published under `name` as `true` or `false`.
    Flag { name: &'static str, value: bool },
    /// A list of texts of the core, in core order, published under `name` as
    /// a frozen `ReadonlyArray<string>`.
    TextList {
        name: &'static str,
        values: &'static [&'static str],
    },
}

/// One generated module of core constants.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ConstantModule {
    /// The stem of the module in the catalog folder.
    stem: &'static str,
    /// The constants, in module order.
    constants: Vec<PublishedConstant>,
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

/// The stem of the module of the monitoring bounds.
const MONITORING_BOUNDS_MODULE: &str = "monitoring_bounds";

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

/// The stem of the module of the exit codes.
const EXIT_CODES_MODULE: &str = "exit_codes";

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

/// The stem of the module of the event bounds.
const EVENT_BOUNDS_MODULE: &str = "event_bounds";

/// The constants of the event registration of an app, after the bounds of
/// the values of an event: the most handlers, the most characters of a
/// handler name, and the `handler` label value of a name outside the
/// declared handlers (no handler may take it). The label value is the
/// return value of a core function, so the list is built at render time.
/// Pure.
fn event_handler_constants() -> [PublishedConstant; 3] {
    [
        PublishedConstant::Integer {
            name: "MAX_EVENT_HANDLERS",
            value: MAX_EVENT_HANDLERS as i128,
        },
        PublishedConstant::Integer {
            name: "MAX_HANDLER_NAME_CHARS",
            value: MAX_HANDLER_NAME_CHARS as i128,
        },
        PublishedConstant::Text {
            name: "UNKNOWN_HANDLER_NAME",
            value: DeclaredSource::HandlerNames.fallback(),
        },
    ]
}

const BROKER_CONSTANTS: [PublishedConstant; 1] = [PublishedConstant::Integer {
    name: "TASK_LIST_ROW_BOUND",
    value: TASK_LIST_BOUND as i128,
}];

/// The stem of the module of the broker bound. The constant name is the
/// name that the Python client publishes for the core's `TASK_LIST_BOUND`.
const BROKER_BOUNDS_MODULE: &str = "broker_bounds";

/// The stem of the module of the bounds and spellings of the stored forms.
const CODEC_BOUNDS_MODULE: &str = "codec_bounds";

/// The constant of the `data_refused` cause of a refused user value.
///
/// The match has no wildcard: a new refusal of the core fails the build
/// here, and the codec module names its cause. Pure.
fn data_refused_cause_constant(refusal: &UserValueRefused) -> &'static str {
    match refusal {
        UserValueRefused::EngineKey { .. } | UserValueRefused::DeeperThanTextBound => {
            "DATA_REFUSED_STRICT_JSON_CAUSE"
        }
        UserValueRefused::DeeperThanValueBound { .. } => "DATA_REFUSED_VALUE_VALIDATION_CAUSE",
        UserValueRefused::IntegerTooLong => "DATA_REFUSED_SERIALIZATION_CAUSE",
    }
}

/// One refusal of each `data_refused` cause, in module order. Pure.
fn data_refused_cause_refusals() -> [UserValueRefused; 3] {
    [
        UserValueRefused::DeeperThanTextBound,
        UserValueRefused::DeeperThanValueBound {
            limit: ERROR_DATA_DEPTH_LIMIT,
        },
        UserValueRefused::IntegerTooLong,
    ]
}

/// The bounds and spellings of the core's stored forms: the depth and digit
/// bounds of a stored text and of a user value, the depth limits of the
/// `data` of a task error and of a sub-workflow summary output, the keys
/// that only the engine writes, the envelope marker key, the key of the
/// replacement `data` and its causes (`UserValueRefused::stored_cause`).
/// Pure.
fn codec_constants() -> Vec<PublishedConstant> {
    let mut constants = vec![
        PublishedConstant::Integer {
            name: "MAX_JSON_DEPTH",
            value: MAX_JSON_DEPTH as i128,
        },
        PublishedConstant::Integer {
            name: "MAX_INT_DIGITS",
            value: MAX_INT_DIGITS as i128,
        },
        PublishedConstant::Integer {
            name: "MAX_VALUE_JSON_DEPTH",
            value: MAX_VALUE_JSON_DEPTH as i128,
        },
        PublishedConstant::Integer {
            name: "ERROR_DATA_DEPTH_LIMIT",
            value: ERROR_DATA_DEPTH_LIMIT as i128,
        },
        PublishedConstant::Integer {
            name: "SUMMARY_OUTPUT_DEPTH_LIMIT",
            value: SUMMARY_OUTPUT_DEPTH_LIMIT as i128,
        },
        PublishedConstant::TextList {
            name: "ENGINE_KEY_PREFIXES",
            values: &ENGINE_KEY_PREFIXES,
        },
        PublishedConstant::Text {
            name: "BUILTIN_CODE_KEY",
            value: BUILTIN_CODE_KEY,
        },
        PublishedConstant::Text {
            name: "TASK_RESULT_ENVELOPE_KEY",
            value: TASK_RESULT_ENVELOPE_KEY,
        },
        PublishedConstant::Text {
            name: "DATA_REFUSED_KEY",
            value: DATA_REFUSED_KEY,
        },
    ];
    constants.extend(
        data_refused_cause_refusals()
            .iter()
            .map(|refusal| PublishedConstant::Text {
                name: data_refused_cause_constant(refusal),
                value: refusal.stored_cause(),
            }),
    );
    constants
}

/// The stem of the module of the send bounds and of the default route of a
/// task.
const SEND_BOUNDS_MODULE: &str = "send_bounds";

const SEND_CONSTANTS: [PublishedConstant; 4] = [
    PublishedConstant::Integer {
        name: "MAX_WAIT_LIMIT_MS",
        value: MAX_WAIT_LIMIT_MS as i128,
    },
    PublishedConstant::Integer {
        name: "IDEMPOTENCY_KEY_MAX_BYTES",
        value: IDEMPOTENCY_KEY_MAX_BYTES as i128,
    },
    PublishedConstant::Text {
        name: "DEFAULT_QUEUE_NAME",
        value: DEFAULT_QUEUE_NAME,
    },
    PublishedConstant::Integer {
        name: "DEFAULT_QUEUE_PRIORITY",
        value: DEFAULT_QUEUE_PRIORITY as i128,
    },
];

/// The stem of the module of the defaults of a task schedule.
const SCHEDULE_DEFAULTS_MODULE: &str = "schedule_defaults";

const SCHEDULE_DEFAULT_CONSTANTS: [PublishedConstant; 3] = [
    PublishedConstant::Flag {
        name: "DEFAULT_SCHEDULE_ENABLED",
        value: DEFAULT_SCHEDULE_ENABLED,
    },
    PublishedConstant::Text {
        name: "DEFAULT_SCHEDULE_TIMEZONE",
        value: DEFAULT_SCHEDULE_TIMEZONE,
    },
    PublishedConstant::Integer {
        name: "DEFAULT_MAX_CATCH_UP_RUNS",
        value: DEFAULT_MAX_CATCH_UP_RUNS as i128,
    },
];

/// Every generated module of core constants, in file order:
///
/// - the bounds of the monitoring reads (the window spans in whole seconds);
/// - the exit codes of the worker process and of its children, in code
///   order;
/// - the bounds of the values of an event, then of the event registration;
/// - the bound of the stale and expired task reads of the broker;
/// - the bounds and spellings of the stored forms;
/// - the longest result wait, the key bound and the default route of a task;
/// - the defaults of a task schedule.
///
/// Pure.
fn constant_modules() -> [ConstantModule; 7] {
    let mut event_constants = EVENT_CONSTANTS.to_vec();
    event_constants.extend(event_handler_constants());
    [
        ConstantModule {
            stem: MONITORING_BOUNDS_MODULE,
            constants: MONITORING_CONSTANTS.to_vec(),
        },
        ConstantModule {
            stem: EXIT_CODES_MODULE,
            constants: EXIT_CODE_CONSTANTS.to_vec(),
        },
        ConstantModule {
            stem: EVENT_BOUNDS_MODULE,
            constants: event_constants,
        },
        ConstantModule {
            stem: BROKER_BOUNDS_MODULE,
            constants: BROKER_CONSTANTS.to_vec(),
        },
        ConstantModule {
            stem: CODEC_BOUNDS_MODULE,
            constants: codec_constants(),
        },
        ConstantModule {
            stem: SEND_BOUNDS_MODULE,
            constants: SEND_CONSTANTS.to_vec(),
        },
        ConstantModule {
            stem: SCHEDULE_DEFAULTS_MODULE,
            constants: SCHEDULE_DEFAULT_CONSTANTS.to_vec(),
        },
    ]
}

/// The modules of core constants, in file order. Pure.
///
/// Panics: a core span is not whole seconds (see `constant_statement`).
pub fn generated_files() -> Vec<GeneratedFile> {
    constant_modules()
        .iter()
        .map(constant_module_file)
        .collect()
}

/// The file of one module: one statement per constant, in list order. Pure.
fn constant_module_file(module: &ConstantModule) -> GeneratedFile {
    let sections: Vec<String> = module.constants.iter().map(constant_statement).collect();
    catalog_file(module.stem, module_text(&sections))
}

/// The statement of one constant: `export const NAME = <literal>;`, or for
/// a list of texts
/// `export const NAME: ReadonlyArray<string> = Object.freeze([...]);`. Pure.
///
/// Panics: a span has a fraction of a second. The module cannot publish it
/// in whole seconds without loss; a core change that the list does not
/// follow is a defect.
fn constant_statement(constant: &PublishedConstant) -> String {
    match *constant {
        PublishedConstant::Integer { name, value } => {
            format!("export const {name} = {};", integer_value_literal(value))
        }
        PublishedConstant::WholeSeconds { name, span } => {
            assert_eq!(
                span.subsec_nanos(),
                0,
                "the span {name} ({span:?}) is not whole seconds"
            );
            format!(
                "export const {name}{SECONDS_SUFFIX} = {};",
                integer_value_literal(i128::from(span.as_secs()))
            )
        }
        PublishedConstant::Text { name, value } => {
            format!("export const {name} = {};", string_literal(value))
        }
        PublishedConstant::Flag { name, value } => {
            format!("export const {name} = {value};")
        }
        PublishedConstant::TextList { name, values } => {
            let literals: Vec<String> = values.iter().map(|value| string_literal(value)).collect();
            format!(
                "export const {name}: ReadonlyArray<string> = Object.freeze([{}]);",
                literals.join(", ")
            )
        }
    }
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

    /// The statement of a text constant as the modules hold it.
    fn text_statement(name: &str, value: &str) -> String {
        format!("\nexport const {name} = {};\n", string_literal(value))
    }

    #[test]
    fn a_module_has_the_header_and_one_statement_per_constant() {
        let module = ConstantModule {
            stem: "bounds",
            constants: vec![
                PublishedConstant::Integer {
                    name: "MAX_ROWS",
                    value: 1_000,
                },
                PublishedConstant::WholeSeconds {
                    name: "SPAN",
                    span: Duration::from_secs(86_400),
                },
            ],
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
    fn a_text_is_a_string_literal() {
        assert_eq!(
            constant_statement(&PublishedConstant::Text {
                name: "KEY",
                value: "say \"hi\"",
            }),
            "export const KEY = \"say \\\"hi\\\"\";"
        );
    }

    #[test]
    fn a_list_of_texts_is_a_frozen_array_of_string_literals() {
        assert_eq!(
            constant_statement(&PublishedConstant::TextList {
                name: "PREFIXES",
                values: &["__a_", "__b_"],
            }),
            "export const PREFIXES: ReadonlyArray<string> = Object.freeze([\"__a_\", \"__b_\"]);"
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
        let values: BTreeSet<i128> = EXIT_CODE_CONSTANTS
            .iter()
            .map(|constant| match *constant {
                PublishedConstant::Integer { value, .. } => value,
                PublishedConstant::WholeSeconds { name, .. }
                | PublishedConstant::Text { name, .. }
                | PublishedConstant::Flag { name, .. }
                | PublishedConstant::TextList { name, .. } => {
                    panic!("the exit code {name} is not an integer")
                }
            })
            .collect();
        assert_eq!(values.len(), EXIT_CODE_CONSTANTS.len());
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
        assert_eq!(text.matches("export const ").count(), 10);
    }

    #[test]
    fn the_event_bounds_hold_the_handler_rules_of_the_core() {
        let text = module_of("event_bounds");
        assert!(text.contains(&integer_statement(
            "MAX_EVENT_HANDLERS",
            MAX_EVENT_HANDLERS as i128
        )));
        assert!(text.contains(&integer_statement(
            "MAX_HANDLER_NAME_CHARS",
            MAX_HANDLER_NAME_CHARS as i128
        )));
        assert!(text.contains(&text_statement(
            "UNKNOWN_HANDLER_NAME",
            DeclaredSource::HandlerNames.fallback()
        )));
    }

    #[test]
    fn the_codec_bounds_hold_the_core_values() {
        let text = module_of("codec_bounds");
        for (name, value) in [
            ("MAX_JSON_DEPTH", MAX_JSON_DEPTH as i128),
            ("MAX_INT_DIGITS", MAX_INT_DIGITS as i128),
            ("MAX_VALUE_JSON_DEPTH", MAX_VALUE_JSON_DEPTH as i128),
            ("ERROR_DATA_DEPTH_LIMIT", ERROR_DATA_DEPTH_LIMIT as i128),
            (
                "SUMMARY_OUTPUT_DEPTH_LIMIT",
                SUMMARY_OUTPUT_DEPTH_LIMIT as i128,
            ),
        ] {
            assert!(text.contains(&integer_statement(name, value)), "{name}");
        }
        for (name, value) in [
            ("BUILTIN_CODE_KEY", BUILTIN_CODE_KEY),
            ("TASK_RESULT_ENVELOPE_KEY", TASK_RESULT_ENVELOPE_KEY),
            ("DATA_REFUSED_KEY", DATA_REFUSED_KEY),
        ] {
            assert!(text.contains(&text_statement(name, value)), "{name}");
        }
        let prefixes: Vec<String> = ENGINE_KEY_PREFIXES
            .iter()
            .map(|prefix| string_literal(prefix))
            .collect();
        assert!(text.contains(&format!(
            "\nexport const ENGINE_KEY_PREFIXES: ReadonlyArray<string> = Object.freeze([{}]);\n",
            prefixes.join(", ")
        )));
        assert_eq!(text.matches("export const ").count(), 12);
    }

    /// Each refusal of a user value has its stored cause under the constant
    /// of its cause, and the three cause constants are distinct texts.
    #[test]
    fn the_codec_bounds_hold_the_stored_cause_of_each_refusal() {
        let text = module_of("codec_bounds");
        for refusal in [
            UserValueRefused::EngineKey {
                key: "__h_x".to_owned(),
            },
            UserValueRefused::DeeperThanTextBound,
            UserValueRefused::DeeperThanValueBound {
                limit: ERROR_DATA_DEPTH_LIMIT,
            },
            UserValueRefused::IntegerTooLong,
        ] {
            assert!(
                text.contains(&text_statement(
                    data_refused_cause_constant(&refusal),
                    refusal.stored_cause()
                )),
                "{refusal:?}"
            );
        }
        let causes: BTreeSet<&str> = data_refused_cause_refusals()
            .iter()
            .map(UserValueRefused::stored_cause)
            .collect();
        assert_eq!(causes.len(), 3);
    }

    #[test]
    fn the_send_bounds_hold_the_longest_wait_the_key_bound_and_the_default_route() {
        assert_eq!(
            module_of("send_bounds"),
            format!(
                "{GENERATED_HEADER}\n{}{}{}{}",
                integer_statement("MAX_WAIT_LIMIT_MS", i128::from(MAX_WAIT_LIMIT_MS)),
                integer_statement(
                    "IDEMPOTENCY_KEY_MAX_BYTES",
                    IDEMPOTENCY_KEY_MAX_BYTES as i128
                ),
                text_statement("DEFAULT_QUEUE_NAME", DEFAULT_QUEUE_NAME),
                integer_statement("DEFAULT_QUEUE_PRIORITY", DEFAULT_QUEUE_PRIORITY as i128),
            )
        );
    }

    #[test]
    fn the_schedule_defaults_hold_the_enabled_flag_the_zone_and_the_catch_up_bound() {
        assert_eq!(
            module_of("schedule_defaults"),
            format!(
                "{GENERATED_HEADER}\n\nexport const DEFAULT_SCHEDULE_ENABLED = {DEFAULT_SCHEDULE_ENABLED};\n{}{}",
                text_statement("DEFAULT_SCHEDULE_TIMEZONE", DEFAULT_SCHEDULE_TIMEZONE),
                integer_statement("DEFAULT_MAX_CATCH_UP_RUNS", DEFAULT_MAX_CATCH_UP_RUNS as i128),
            )
        );
    }

    #[test]
    fn the_broker_bounds_hold_the_task_list_bound() {
        assert_eq!(
            module_of("broker_bounds"),
            format!(
                "{GENERATED_HEADER}\n{}",
                integer_statement("TASK_LIST_ROW_BOUND", TASK_LIST_BOUND as i128)
            )
        );
    }

    #[test]
    fn the_files_are_the_seven_modules_in_order_with_distinct_names() {
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
                "src/_catalog/codec_bounds.ts",
                "src/_catalog/send_bounds.ts",
                "src/_catalog/schedule_defaults.ts",
            ]
        );
        for module in &constant_modules() {
            let names: BTreeSet<&str> = module
                .constants
                .iter()
                .map(|constant| match *constant {
                    PublishedConstant::Integer { name, .. }
                    | PublishedConstant::WholeSeconds { name, .. }
                    | PublishedConstant::Text { name, .. }
                    | PublishedConstant::Flag { name, .. }
                    | PublishedConstant::TextList { name, .. } => name,
                })
                .collect();
            assert_eq!(names.len(), module.constants.len(), "{}", module.stem);
        }
    }
}

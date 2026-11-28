//! The check of the options of one task: its retry policy, its timeout, its
//! execution class and its cancel grace, by the core's rules
//! (`check_declared_options`), into the core's `TaskOptions`.
//!
//! - Pure: starts no core, opens no connection, reads no clock.
//! - Each number of the facts crosses as a `u32` first. A number that is not
//!   a whole number from 0 to 4294967295 is refused with `HRS-102`, one
//!   refusal per field, and then the core's rules do not run.
//! - The core's rules give every refusal in one list, in the core's order:
//!   the retry policy and its `retryFor` codes (`HRS-102`), a timeout
//!   below its minimum (`HRS-102`), a shared class with no timeout
//!   (`HRS-109`), a grace with `SHARED_THREAD` (`HRS-110`), a grace outside
//!   its range (`HRS-111`). The code of each refusal is the core's
//!   (`DeclaredOptionRefusal::error_code`); this file writes its text with
//!   the names of `defineTask`.
//! - A `SHARED_ASYNC` task that sets no grace gets the core's default grace
//!   (`DEFAULT_CANCEL_GRACE_MS`), stored as resolved.
//! - The accepted options give the stored options text that a send writes
//!   (`TaskOptions::stored_options_text`).

use horsies_core::core::task::declared_options::{
    check_declared_options, DeclaredMaxDelay, DeclaredMilliseconds, DeclaredOptionRefusal,
    DeclaredRetry, DeclaredTaskOptions, RetryCodeScope,
};
use horsies_core::core::task::execution::{MAX_CANCEL_GRACE_MS, MIN_CANCEL_GRACE_MS};
use horsies_core::{
    Execution, HorsiesErrorCode, TaskOptions, MAX_TASK_TIMEOUT_MS, MIN_TASK_TIMEOUT_MS,
};
use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::Envelope;
use crate::sends::refusals::{CoreRefusal, RefusalHelp};
use crate::values::boundary::{u32_of_number, BoundaryRefusal};

/// The field name of the timeout in the refusal texts.
const TIMEOUT_FIELD: &str = "timeoutMs";

/// The field name of the cancel grace in the refusal texts.
const GRACE_FIELD: &str = "cancelGraceMs";

/// The field name of the retry codes in the refusal texts.
const RETRY_FOR_FIELD: &str = "retryPolicy.retryFor";

/// The help line of a refusal with `HRS-102`.
const OPTIONS_HELP: &str = "check the options of defineTask";

/// The execution class of a task.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionName {
    Isolated,
    SharedAsync,
    SharedThread,
}

impl ExecutionName {
    /// The core's class. Pure.
    pub fn execution(self) -> Execution {
        match self {
            Self::Isolated => Execution::Isolated,
            Self::SharedAsync => Execution::SharedAsync,
            Self::SharedThread => Execution::SharedThread,
        }
    }
}

/// A duration in milliseconds that a task sets, or not.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq)]
pub enum MillisecondsFact {
    NotSet,
    Set { milliseconds: f64 },
}

/// The longest wait between two retries of an exponential policy.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq)]
pub enum MaxDelayFact {
    NoCap,
    Seconds { seconds: f64 },
}

/// The retry policy of a task. Each code of `retry_for` is a custom code
/// or a built-in code, as its text.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq)]
pub enum RetryFacts {
    NoRetry,
    Fixed {
        intervals_seconds: Vec<f64>,
        jitter: bool,
        retry_for: Vec<String>,
    },
    Exponential {
        base_seconds: f64,
        max_retries: f64,
        jitter: bool,
        max_delay: MaxDelayFact,
        retry_for: Vec<String>,
    },
}

/// The error codes of a task: any code, or the declared list.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeclaredCodesFact {
    Open,
    Declared { codes: Vec<String> },
}

/// The facts of a task that its stored options text holds.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq)]
pub struct TaskOptionsFacts {
    pub task_name: String,
    pub queue: String,
    pub execution: ExecutionName,
    pub timeout: MillisecondsFact,
    pub cancel_grace: MillisecondsFact,
    pub retry: RetryFacts,
    pub declared_codes: DeclaredCodesFact,
}

/// The checked options of one task. JavaScript gets it from
/// `checkTaskOptions`; it has no constructor.
#[napi]
#[derive(Clone, Debug)]
pub struct TaskOptionsRecord {
    options: TaskOptions,
}

impl TaskOptionsRecord {
    /// The core's options.
    pub fn options(&self) -> &TaskOptions {
        &self.options
    }
}

#[napi]
impl TaskOptionsRecord {
    /// The stored options text of a send with no deadline. Pure.
    #[napi(ts_return_type = "NativeEnvelope<string>")]
    pub fn stored_text(&self) -> Envelope<String> {
        sync_call("task_options_stored_text", || {
            Envelope::Value(self.options.stored_options_text())
        })
    }
}

/// The outcome of the check of the options of a task.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Debug)]
pub enum TaskOptionsCheck {
    Accepted { options: TaskOptionsRecord },
    Refused { refusals: Vec<CoreRefusal> },
}

/// Check the options of a task. Starts nothing.
#[napi(ts_return_type = "NativeEnvelope<TaskOptionsCheck>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn check_task_options(facts: TaskOptionsFacts) -> Envelope<TaskOptionsCheck> {
    sync_call("check_task_options", || {
        Envelope::Value(task_options_check(&facts))
    })
}

/// The check of the options of a task: the facts as the core's integers,
/// then the core's rules. Pure.
fn task_options_check(facts: &TaskOptionsFacts) -> TaskOptionsCheck {
    let name = facts.task_name.as_str();
    match declared_options(facts) {
        Err(refused) => TaskOptionsCheck::Refused {
            refusals: refused
                .iter()
                .map(|refusal| number_refusal(name, refusal))
                .collect(),
        },
        Ok(declared) => match check_declared_options(declared) {
            Ok(options) => TaskOptionsCheck::Accepted {
                options: TaskOptionsRecord { options },
            },
            Err(refused) => TaskOptionsCheck::Refused {
                refusals: refused
                    .iter()
                    .map(|refusal| rule_refusal(name, refusal))
                    .collect(),
            },
        },
    }
}

/// The declaration of the core's check, or the refusal of each number that
/// is not a `u32`, in field order: the retry policy, the timeout, the grace.
/// Pure.
fn declared_options(facts: &TaskOptionsFacts) -> Result<DeclaredTaskOptions, Vec<BoundaryRefusal>> {
    let retry = declared_retry(&facts.retry);
    let timeout = declared_milliseconds(TIMEOUT_FIELD, &facts.timeout);
    let grace = declared_milliseconds(GRACE_FIELD, &facts.cancel_grace);
    match (retry, timeout, grace) {
        (Ok(retry), Ok(timeout_ms), Ok(cancel_grace_ms)) => Ok(DeclaredTaskOptions {
            task_name: facts.task_name.clone(),
            queue: facts.queue.clone(),
            execution: facts.execution.execution(),
            timeout_ms,
            cancel_grace_ms,
            retry,
            retry_codes: match &facts.declared_codes {
                DeclaredCodesFact::Open => RetryCodeScope::AnyCustomCode,
                DeclaredCodesFact::Declared { codes } => RetryCodeScope::Declared(codes.clone()),
            },
        }),
        (retry, timeout, grace) => Err(retry
            .err()
            .into_iter()
            .flatten()
            .chain(timeout.err())
            .chain(grace.err())
            .collect()),
    }
}

/// A duration of the facts as the core's declaration, or the refusal of a
/// number that is not a `u32`. Pure.
fn declared_milliseconds(
    subject: &str,
    fact: &MillisecondsFact,
) -> Result<DeclaredMilliseconds, BoundaryRefusal> {
    match fact {
        MillisecondsFact::NotSet => Ok(DeclaredMilliseconds::NotSet),
        MillisecondsFact::Set { milliseconds } => {
            u32_of_number(subject, *milliseconds).map(DeclaredMilliseconds::Set)
        }
    }
}

/// The retry policy of the facts as the core's declaration, or the refusal
/// of each number that is not a `u32` (the intervals give one refusal).
/// Pure.
fn declared_retry(retry: &RetryFacts) -> Result<DeclaredRetry, Vec<BoundaryRefusal>> {
    match retry {
        RetryFacts::NoRetry => Ok(DeclaredRetry::NoRetry),
        RetryFacts::Fixed {
            intervals_seconds,
            jitter,
            retry_for,
        } => intervals_seconds
            .iter()
            .map(|seconds| u32_of_number("retryPolicy.intervalsSeconds", *seconds))
            .collect::<Result<Vec<u32>, BoundaryRefusal>>()
            .map(|intervals| DeclaredRetry::Fixed {
                intervals_seconds: intervals,
                jitter: *jitter,
                retry_for: retry_for.clone(),
            })
            .map_err(|refusal| vec![refusal]),
        RetryFacts::Exponential {
            base_seconds,
            max_retries,
            jitter,
            max_delay,
            retry_for,
        } => {
            let base = u32_of_number("retryPolicy.baseSeconds", *base_seconds);
            let retries = u32_of_number("retryPolicy.maxRetries", *max_retries);
            let cap = match max_delay {
                MaxDelayFact::NoCap => Ok(DeclaredMaxDelay::NoCap),
                MaxDelayFact::Seconds { seconds } => {
                    u32_of_number("retryPolicy.maxDelaySeconds", *seconds)
                        .map(DeclaredMaxDelay::Seconds)
                }
            };
            match (base, retries, cap) {
                (Ok(base_seconds), Ok(max_retries), Ok(max_delay)) => {
                    Ok(DeclaredRetry::Exponential {
                        base_seconds,
                        max_retries,
                        jitter: *jitter,
                        max_delay,
                        retry_for: retry_for.clone(),
                    })
                }
                (base, retries, cap) => Err(base
                    .err()
                    .into_iter()
                    .chain(retries.err())
                    .chain(cap.err())
                    .collect()),
            }
        }
    }
}

/// The refusal of a number that is not a `u32` (`HRS-102`). Pure.
fn number_refusal(name: &str, refusal: &BoundaryRefusal) -> CoreRefusal {
    CoreRefusal::coded(
        HorsiesErrorCode::TaskInvalidOptions,
        format!("{}: {}", refusal.subject, refusal.rule),
        vec![format!("task '{name}'")],
        RefusalHelp::Help {
            line: OPTIONS_HELP.to_owned(),
        },
    )
}

/// The refusal of a rule of the core, with the core's code and the text of
/// `defineTask`. The first note names the task. Pure.
fn rule_refusal(name: &str, refusal: &DeclaredOptionRefusal) -> CoreRefusal {
    let text = rule_text(refusal);
    CoreRefusal::coded(
        refusal.error_code(),
        text.message,
        std::iter::once(format!("task '{name}'"))
            .chain(text.notes)
            .collect(),
        RefusalHelp::Help { line: text.help },
    )
}

/// The text of one refusal: the message, the notes after the task note, and
/// the help line.
struct RuleText {
    message: String,
    notes: Vec<String>,
    help: String,
}

impl RuleText {
    /// The text of a refusal with `HRS-102`: the message alone, and the help
    /// of the options. Pure.
    fn option(message: String) -> Self {
        Self {
            message,
            notes: Vec::new(),
            help: OPTIONS_HELP.to_owned(),
        }
    }
}

/// The text of each refusal of the core's rules, with the names of
/// `defineTask`. Pure.
fn rule_text(refusal: &DeclaredOptionRefusal) -> RuleText {
    match refusal {
        DeclaredOptionRefusal::RetryPolicy(error) => {
            RuleText::option(format!("invalid retry policy: {error}"))
        }
        DeclaredOptionRefusal::NoRetryCode => {
            RuleText::option(format!("{RETRY_FOR_FIELD} must not be empty"))
        }
        DeclaredOptionRefusal::RetryCodeText { code, refusal } => {
            RuleText::option(refusal.message(RETRY_FOR_FIELD, code))
        }
        DeclaredOptionRefusal::RetryCodeNotCustomCode { refused } => {
            RuleText::option(refused.to_string())
        }
        DeclaredOptionRefusal::RetryCodeNotDeclared { retry_code } => RuleText::option(format!(
            "{RETRY_FOR_FIELD} code '{retry_code}' is not a declared error code of the task; a \
             custom code that retries the task is one of its errors"
        )),
        DeclaredOptionRefusal::TimeoutBelowMinimum => RuleText::option(format!(
            "{TIMEOUT_FIELD} must be a whole number from {MIN_TASK_TIMEOUT_MS} to \
             {MAX_TASK_TIMEOUT_MS} ms"
        )),
        DeclaredOptionRefusal::SharedClassWithoutTimeout { execution } => {
            shared_timeout_text(*execution)
        }
        DeclaredOptionRefusal::GraceNotTaken { execution } => RuleText {
            message: format!("{GRACE_FIELD} is not valid with execution {execution}"),
            notes: vec![
                "a thread cannot be cancelled or stopped, so a SHARED_THREAD task has no cancel \
                 grace"
                    .to_owned(),
            ],
            help: format!(
                "remove {GRACE_FIELD}, or use execution SHARED_ASYNC with an async body, or \
                 ISOLATED"
            ),
        },
        DeclaredOptionRefusal::GraceOutOfRange { execution } => RuleText {
            message: format!("{GRACE_FIELD} is out of range"),
            notes: vec![
                format!(
                    "{GRACE_FIELD} must be a whole number from {MIN_CANCEL_GRACE_MS} to \
                     {MAX_CANCEL_GRACE_MS} ms"
                ),
                format!("execution is {execution}"),
            ],
            help: format!("set {GRACE_FIELD} to a whole number of milliseconds inside the range"),
        },
    }
}

/// The text of a shared class with no timeout (`HRS-109`). Pure.
fn shared_timeout_text(execution: Execution) -> RuleText {
    RuleText {
        message: format!("execution {execution} requires {TIMEOUT_FIELD}"),
        notes: vec!["a shared task runs in one child process with other tasks".to_owned()],
        help: format!(
            "set {TIMEOUT_FIELD}, for example execution: \"{execution}\", {TIMEOUT_FIELD}: 30_000"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The facts of an `ISOLATED` task with no options.
    fn facts() -> TaskOptionsFacts {
        TaskOptionsFacts {
            task_name: "charge_payment".to_owned(),
            queue: "default".to_owned(),
            execution: ExecutionName::Isolated,
            timeout: MillisecondsFact::NotSet,
            cancel_grace: MillisecondsFact::NotSet,
            retry: RetryFacts::NoRetry,
            declared_codes: DeclaredCodesFact::Open,
        }
    }

    fn stored_text(facts: &TaskOptionsFacts) -> String {
        match task_options_check(facts) {
            TaskOptionsCheck::Accepted { options } => options.options().stored_options_text(),
            TaskOptionsCheck::Refused { refusals } => {
                panic!("the options are accepted, got {refusals:?}")
            }
        }
    }

    fn codes_of(facts: &TaskOptionsFacts) -> Vec<String> {
        match task_options_check(facts) {
            TaskOptionsCheck::Refused { refusals } => {
                refusals.into_iter().map(|refusal| refusal.code).collect()
            }
            TaskOptionsCheck::Accepted { .. } => panic!("the options are refused"),
        }
    }

    fn catalog(code: &str) -> String {
        code.to_owned()
    }

    #[test]
    fn an_isolated_task_with_no_options_stores_the_text_with_nulls() {
        assert_eq!(
            stored_text(&facts()),
            r#"{"retry_policy":null,"good_until":null,"timeout_ms":null,"execution":"ISOLATED","cancel_grace_ms":null}"#
        );
    }

    #[test]
    fn a_fixed_policy_stores_its_codes_by_name() {
        let text = stored_text(&TaskOptionsFacts {
            timeout: MillisecondsFact::Set {
                milliseconds: 30_000.0,
            },
            retry: RetryFacts::Fixed {
                intervals_seconds: vec![60.0, 300.0, 900.0],
                jitter: true,
                retry_for: vec!["TASK_TIMEOUT".to_owned(), "CARD_DECLINED".to_owned()],
            },
            ..facts()
        });
        assert_eq!(
            text,
            r#"{"retry_policy":{"max_retries":3,"intervals_seconds":[60,300,900],"backoff_strategy":"fixed","jitter":true,"retry_for":["TASK_TIMEOUT","CARD_DECLINED"]},"good_until":null,"timeout_ms":30000,"execution":"ISOLATED","cancel_grace_ms":null}"#
        );
    }

    #[test]
    fn an_exponential_policy_stores_its_max_delay() {
        let text = stored_text(&TaskOptionsFacts {
            retry: RetryFacts::Exponential {
                base_seconds: 30.0,
                max_retries: 5.0,
                jitter: false,
                max_delay: MaxDelayFact::Seconds { seconds: 600.0 },
                retry_for: vec!["WORKER_CRASHED".to_owned()],
            },
            ..facts()
        });
        assert!(
            text.starts_with(r#"{"retry_policy":{"max_retries":5,"intervals_seconds":[30],"backoff_strategy":"exponential","jitter":false,"max_delay_seconds":600,"#),
            "{text}"
        );
    }

    #[test]
    fn a_shared_async_task_with_no_grace_stores_the_default_grace() {
        let text = stored_text(&TaskOptionsFacts {
            execution: ExecutionName::SharedAsync,
            timeout: MillisecondsFact::Set {
                milliseconds: 10_000.0,
            },
            ..facts()
        });
        assert!(
            text.ends_with(r#""execution":"SHARED_ASYNC","cancel_grace_ms":1000}"#),
            "{text}"
        );
    }

    #[test]
    fn a_shared_class_with_no_timeout_is_refused_with_hrs_109() {
        for execution in [ExecutionName::SharedAsync, ExecutionName::SharedThread] {
            assert_eq!(
                codes_of(&TaskOptionsFacts {
                    execution,
                    ..facts()
                }),
                vec![catalog("HRS-109")],
                "{execution:?}"
            );
        }
    }

    #[test]
    fn a_grace_with_shared_thread_is_refused_with_hrs_110() {
        assert_eq!(
            codes_of(&TaskOptionsFacts {
                execution: ExecutionName::SharedThread,
                timeout: MillisecondsFact::Set {
                    milliseconds: 10_000.0,
                },
                cancel_grace: MillisecondsFact::Set {
                    milliseconds: 1_000.0,
                },
                ..facts()
            }),
            vec![catalog("HRS-110")]
        );
    }

    #[test]
    fn a_grace_outside_its_range_is_refused_with_hrs_111() {
        for milliseconds in [99.0, 10_001.0] {
            assert_eq!(
                codes_of(&TaskOptionsFacts {
                    cancel_grace: MillisecondsFact::Set { milliseconds },
                    ..facts()
                }),
                vec![catalog("HRS-111")],
                "{milliseconds}"
            );
        }
    }

    #[test]
    fn a_timeout_outside_its_range_is_refused_with_hrs_102() {
        for milliseconds in [999.0, 4_294_967_296.0, 1_500.5] {
            assert_eq!(
                codes_of(&TaskOptionsFacts {
                    timeout: MillisecondsFact::Set { milliseconds },
                    ..facts()
                }),
                vec![catalog("HRS-102")],
                "{milliseconds}"
            );
        }
    }

    #[test]
    fn a_retry_policy_that_the_core_refuses_is_refused_with_hrs_102() {
        assert_eq!(
            codes_of(&TaskOptionsFacts {
                retry: RetryFacts::Exponential {
                    base_seconds: 30.0,
                    max_retries: 21.0,
                    jitter: true,
                    max_delay: MaxDelayFact::NoCap,
                    retry_for: vec!["WORKER_CRASHED".to_owned()],
                },
                ..facts()
            }),
            vec![catalog("HRS-102")]
        );
    }

    #[test]
    fn an_empty_retry_code_list_is_refused_with_hrs_102() {
        assert_eq!(
            codes_of(&TaskOptionsFacts {
                retry: RetryFacts::Fixed {
                    intervals_seconds: vec![60.0],
                    jitter: true,
                    retry_for: Vec::new(),
                },
                ..facts()
            }),
            vec![catalog("HRS-102")]
        );
    }

    #[test]
    fn a_retry_code_that_breaks_the_code_text_rule_is_refused_with_hrs_102() {
        assert_eq!(
            codes_of(&TaskOptionsFacts {
                retry: RetryFacts::Fixed {
                    intervals_seconds: vec![60.0],
                    jitter: true,
                    retry_for: vec!["CardDeclinedError".to_owned()],
                },
                ..facts()
            }),
            vec![catalog("HRS-102")]
        );
    }

    #[test]
    fn a_user_retry_code_that_the_declared_list_does_not_hold_is_refused_with_hrs_102() {
        let retry = RetryFacts::Fixed {
            intervals_seconds: vec![60.0],
            jitter: true,
            retry_for: vec!["PROVIDER_DOWN".to_owned(), "TASK_TIMEOUT".to_owned()],
        };
        assert_eq!(
            codes_of(&TaskOptionsFacts {
                retry: retry.clone(),
                declared_codes: DeclaredCodesFact::Declared {
                    codes: vec!["CARD_DECLINED".to_owned()],
                },
                ..facts()
            }),
            vec![catalog("HRS-102")]
        );
        let _accepted = stored_text(&TaskOptionsFacts {
            retry,
            declared_codes: DeclaredCodesFact::Declared {
                codes: vec!["PROVIDER_DOWN".to_owned()],
            },
            ..facts()
        });
    }

    #[test]
    fn the_text_of_an_undeclared_retry_code_names_the_retry_code() {
        let text = rule_text(&DeclaredOptionRefusal::RetryCodeNotDeclared {
            retry_code: "PROVIDER_DOWN".to_owned(),
        });
        assert!(
            text.message
                .contains("code 'PROVIDER_DOWN' is not a declared error code of the task"),
            "{}",
            text.message
        );
    }

    /// A number that is not a `u32` is refused at the boundary, one refusal
    /// per field; the core's rules then do not run, so the grace of a
    /// `SHARED_THREAD` task gives no `HRS-110` here.
    #[test]
    fn a_number_that_is_not_a_u32_is_hrs_102_and_the_rules_do_not_run() {
        let refused = task_options_check(&TaskOptionsFacts {
            execution: ExecutionName::SharedThread,
            timeout: MillisecondsFact::Set {
                milliseconds: 1_500.5,
            },
            cancel_grace: MillisecondsFact::Set { milliseconds: -1.0 },
            ..facts()
        });
        let TaskOptionsCheck::Refused { refusals } = refused else {
            panic!("the numbers are refused");
        };
        let messages: Vec<(&str, &str)> = refusals
            .iter()
            .map(|refusal| (refusal.code.as_str(), refusal.message.as_str()))
            .collect();
        assert_eq!(
            messages,
            vec![
                ("HRS-102", "timeoutMs: a whole number from 0 to 4294967295"),
                (
                    "HRS-102",
                    "cancelGraceMs: a whole number from 0 to 4294967295"
                ),
            ]
        );
    }

    #[test]
    fn a_refusal_of_the_core_has_the_text_of_define_task_and_names_the_task() {
        let TaskOptionsCheck::Refused { refusals } = task_options_check(&TaskOptionsFacts {
            execution: ExecutionName::SharedAsync,
            ..facts()
        }) else {
            panic!("a shared class with no timeout is refused");
        };
        assert_eq!(
            refusals,
            vec![CoreRefusal {
                code: "HRS-109".to_owned(),
                message: "execution SHARED_ASYNC requires timeoutMs".to_owned(),
                notes: vec![
                    "task 'charge_payment'".to_owned(),
                    "a shared task runs in one child process with other tasks".to_owned(),
                ],
                help: RefusalHelp::Help {
                    line: "set timeoutMs, for example execution: \"SHARED_ASYNC\", timeoutMs: \
                           30_000"
                        .to_owned(),
                },
            }]
        );
        let TaskOptionsCheck::Refused { refusals } = task_options_check(&TaskOptionsFacts {
            timeout: MillisecondsFact::Set {
                milliseconds: 999.0,
            },
            ..facts()
        }) else {
            panic!("a timeout below the minimum is refused");
        };
        assert_eq!(
            refusals[0].message,
            "timeoutMs must be a whole number from 1000 to 4294967295 ms"
        );
    }

    #[test]
    fn every_refusal_comes_in_one_list_in_the_stated_order() {
        assert_eq!(
            codes_of(&TaskOptionsFacts {
                execution: ExecutionName::SharedThread,
                cancel_grace: MillisecondsFact::Set {
                    milliseconds: 1_000.0,
                },
                retry: RetryFacts::Fixed {
                    intervals_seconds: vec![0.0],
                    jitter: true,
                    retry_for: vec!["WORKER_CRASHED".to_owned()],
                },
                ..facts()
            }),
            vec![catalog("HRS-102"), catalog("HRS-109"), catalog("HRS-110")]
        );
    }
}

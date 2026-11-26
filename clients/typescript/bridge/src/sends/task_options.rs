//! The check of the options of one task: its retry policy, its timeout, its
//! execution class and its cancel grace, as the core's `TaskOptions`.
//!
//! - Pure: starts no core, opens no connection, reads no clock.
//! - Every refusal comes in one list, in this order: the retry policy and
//!   its `autoRetryFor` codes (`HRS-102`), the timeout range (`HRS-102`), a
//!   shared class with no timeout (`HRS-109`), a grace with `SHARED_THREAD`
//!   (`HRS-110`), a grace outside its range (`HRS-111`). The Python client
//!   checks in the same order: its `TaskOptions` model first, then the
//!   rules of the class.
//! - A `SHARED_ASYNC` task that sets no grace gets the core's default grace
//!   (`CancelGraceMs::DEFAULT`), stored as resolved.
//! - The accepted options give the stored options text that a send writes
//!   (`TaskOptions::stored_options_text`).

use horsies_core::core::task::error::check_error_code_text;
use horsies_core::core::task::execution::{MAX_CANCEL_GRACE_MS, MIN_CANCEL_GRACE_MS};
use horsies_core::{
    BuiltInTaskCode, CancelGraceMs, ErrorCode, Execution, RetryPolicy, TaskErrorCode, TaskOptions,
    UserCode,
};
use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::Envelope;
use crate::sends::refusals::CoreRefusal;
use crate::values::boundary::{u32_of_number, BoundaryRefusal};

/// The smallest `timeout_ms` of a task (Python `MIN_TASK_TIMEOUT_MS`). The
/// core holds the same bound in the dispatch read of stored options
/// (`worker::retry::MIN_TASK_TIMEOUT_MS`), which it does not export. The
/// largest timeout is the bound of a `u32`, as the core stores it.
const MIN_TASK_TIMEOUT_MS: u32 = 1_000;

/// The field name of the retry codes in the refusal texts.
const AUTO_RETRY_FOR_FIELD: &str = "retry.autoRetryFor";

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

/// The retry policy of a task. Each code of `auto_retry_for` is a user code
/// or a library code, as its text.
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
        auto_retry_for: Vec<String>,
    },
    Exponential {
        base_seconds: f64,
        max_retries: f64,
        jitter: bool,
        max_delay: MaxDelayFact,
        auto_retry_for: Vec<String>,
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
    pub queue_name: String,
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
        sync_call("stored_text", || {
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

/// The retry policy of a task and the codes that retry it.
#[derive(Clone, Debug)]
struct RetryParts {
    policy: RetryPolicy,
    codes: Vec<TaskErrorCode>,
}

/// The check of the options of a task. Pure.
fn task_options_check(facts: &TaskOptionsFacts) -> TaskOptionsCheck {
    let name = facts.task_name.as_str();
    let retry = retry_of(name, &facts.retry, &facts.declared_codes);
    let timeout = timeout_of(name, &facts.timeout);
    let class = shared_timeout_refusal(name, facts.execution, &facts.timeout);
    let grace = grace_of(name, facts.execution, &facts.cancel_grace);
    match (retry, timeout, class, grace) {
        (Ok(retry), Ok(timeout_ms), None, Ok(cancel_grace_ms)) => TaskOptionsCheck::Accepted {
            options: TaskOptionsRecord {
                options: TaskOptions {
                    task_name: facts.task_name.clone(),
                    queue_name: Some(facts.queue_name.clone()),
                    good_until: None,
                    auto_retry_for: retry.as_ref().map(|parts| parts.codes.clone()),
                    retry_policy: retry.map(|parts| parts.policy),
                    timeout_ms,
                    execution: facts.execution.execution(),
                    cancel_grace_ms,
                },
            },
        },
        (retry, timeout, class, grace) => TaskOptionsCheck::Refused {
            refusals: retry
                .err()
                .into_iter()
                .flatten()
                .chain(timeout.err())
                .chain(class)
                .chain(grace.err())
                .collect(),
        },
    }
}

/// The retry policy and its codes, or every refusal of them. Pure.
fn retry_of(
    name: &str,
    retry: &RetryFacts,
    declared: &DeclaredCodesFact,
) -> Result<Option<RetryParts>, Vec<CoreRefusal>> {
    let (policy, codes) = match retry {
        RetryFacts::NoRetry => return Ok(None),
        RetryFacts::Fixed {
            intervals_seconds,
            jitter,
            auto_retry_for,
        } => (fixed_policy(intervals_seconds, *jitter), auto_retry_for),
        RetryFacts::Exponential {
            base_seconds,
            max_retries,
            jitter,
            max_delay,
            auto_retry_for,
        } => (
            exponential_policy(*base_seconds, *max_retries, *jitter, max_delay),
            auto_retry_for,
        ),
    };
    let policy =
        policy.map_err(|cause| option_refusal(name, format!("invalid retry policy: {cause}")));
    let codes = retry_codes(name, codes, declared);
    match (policy, codes) {
        (Ok(policy), Ok(codes)) => Ok(Some(RetryParts { policy, codes })),
        (policy, codes) => Err(policy
            .err()
            .into_iter()
            .chain(codes.err().into_iter().flatten())
            .collect()),
    }
}

/// A fixed policy of the intervals, or the cause of its refusal. Pure.
fn fixed_policy(intervals_seconds: &[f64], jitter: bool) -> Result<RetryPolicy, String> {
    let intervals = intervals_seconds
        .iter()
        .map(|seconds| u32_of_number("retry.intervalsSeconds", *seconds))
        .collect::<Result<Vec<u32>, BoundaryRefusal>>()
        .map_err(|refusal| boundary_text(&refusal))?;
    RetryPolicy::fixed(intervals, jitter).map_err(|error| error.to_string())
}

/// An exponential policy, or the cause of its refusal. Pure.
fn exponential_policy(
    base_seconds: f64,
    max_retries: f64,
    jitter: bool,
    max_delay: &MaxDelayFact,
) -> Result<RetryPolicy, String> {
    let base = u32_of_number("retry.baseSeconds", base_seconds)
        .map_err(|refusal| boundary_text(&refusal))?;
    let retries = u32_of_number("retry.maxRetries", max_retries)
        .map_err(|refusal| boundary_text(&refusal))?;
    let policy =
        RetryPolicy::exponential(base, retries, jitter).map_err(|error| error.to_string())?;
    let policy = match max_delay {
        MaxDelayFact::NoCap => policy,
        MaxDelayFact::Seconds { seconds } => policy.with_max_delay_seconds(
            u32_of_number("retry.maxDelaySeconds", *seconds)
                .map_err(|refusal| boundary_text(&refusal))?,
        ),
    };
    policy.validate().map_err(|error| error.to_string())?;
    Ok(policy)
}

/// The codes that retry a task, or the refusal of each code that breaks a
/// rule: at least one code; the code-text rule of retry matching
/// (`check_error_code_text`); with a declared list, each user code is a
/// declared one. Pure.
fn retry_codes(
    name: &str,
    codes: &[String],
    declared: &DeclaredCodesFact,
) -> Result<Vec<TaskErrorCode>, Vec<CoreRefusal>> {
    if codes.is_empty() {
        return Err(vec![option_refusal(
            name,
            format!("{AUTO_RETRY_FOR_FIELD} must not be empty"),
        )]);
    }
    let (accepted, refused): (Vec<_>, Vec<_>) = codes
        .iter()
        .map(|code| retry_code(name, code, declared))
        .partition(Result::is_ok);
    match refused.is_empty() {
        true => Ok(accepted.into_iter().filter_map(Result::ok).collect()),
        false => Err(refused.into_iter().filter_map(Result::err).collect()),
    }
}

/// One code that retries a task, or its refusal. Pure.
fn retry_code(
    name: &str,
    code: &str,
    declared: &DeclaredCodesFact,
) -> Result<TaskErrorCode, CoreRefusal> {
    check_error_code_text(code)
        .map_err(|refusal| option_refusal(name, refusal.message(AUTO_RETRY_FOR_FIELD, code)))?;
    if let Some(built_in) = BuiltInTaskCode::parse(code) {
        return Ok(TaskErrorCode::BuiltIn(built_in));
    }
    let user =
        UserCode::parse(code).map_err(|refused| option_refusal(name, refused.to_string()))?;
    match declared {
        DeclaredCodesFact::Open => Ok(TaskErrorCode::User(user)),
        DeclaredCodesFact::Declared { codes } => match codes.iter().any(|known| known == code) {
            true => Ok(TaskErrorCode::User(user)),
            false => Err(option_refusal(
                name,
                format!(
                    "{AUTO_RETRY_FOR_FIELD} code '{code}' is not a declared error code of the \
                     task; a user code that retries the task is one of its errors"
                ),
            )),
        },
    }
}

/// The timeout of a task, or the refusal of a value outside 1000 to
/// 4294967295 ms. Pure.
fn timeout_of(name: &str, timeout: &MillisecondsFact) -> Result<Option<u32>, CoreRefusal> {
    let refused = || {
        option_refusal(
            name,
            format!(
                "timeoutMs must be a whole number from {MIN_TASK_TIMEOUT_MS} to {} ms",
                u32::MAX
            ),
        )
    };
    match timeout {
        MillisecondsFact::NotSet => Ok(None),
        MillisecondsFact::Set { milliseconds } => match u32_of_number("timeoutMs", *milliseconds) {
            Ok(value) if value >= MIN_TASK_TIMEOUT_MS => Ok(Some(value)),
            Ok(_) | Err(_) => Err(refused()),
        },
    }
}

/// The refusal of a shared class with no timeout (`HRS-109`). Pure.
fn shared_timeout_refusal(
    name: &str,
    execution: ExecutionName,
    timeout: &MillisecondsFact,
) -> Option<CoreRefusal> {
    match (execution, timeout) {
        (ExecutionName::SharedAsync | ExecutionName::SharedThread, MillisecondsFact::NotSet) => {
            let class = execution.execution();
            Some(CoreRefusal::coded(
                ErrorCode::TaskSharedExecutionRequiresTimeout,
                format!("execution {class} requires timeoutMs"),
                vec![
                    format!("task '{name}'"),
                    "a shared task runs in one child process with other tasks".to_owned(),
                ],
                vec![format!(
                    "set timeoutMs, for example execution: \"{class}\", timeoutMs: 30_000"
                )],
            ))
        }
        (
            ExecutionName::SharedAsync | ExecutionName::SharedThread,
            MillisecondsFact::Set { .. },
        )
        | (ExecutionName::Isolated, _) => None,
    }
}

/// The cancel grace of a task, or its refusal: a grace with
/// `SHARED_THREAD` (`HRS-110`), a grace outside 100 to 10,000 ms
/// (`HRS-111`). A `SHARED_ASYNC` task with no grace gets the default. Pure.
fn grace_of(
    name: &str,
    execution: ExecutionName,
    grace: &MillisecondsFact,
) -> Result<Option<CancelGraceMs>, CoreRefusal> {
    match (execution, grace) {
        (ExecutionName::SharedAsync, MillisecondsFact::NotSet) => Ok(Some(CancelGraceMs::DEFAULT)),
        (ExecutionName::Isolated | ExecutionName::SharedThread, MillisecondsFact::NotSet) => {
            Ok(None)
        }
        (ExecutionName::SharedThread, MillisecondsFact::Set { .. }) => Err(CoreRefusal::coded(
            ErrorCode::TaskCancelGraceRequiresSharedAsync,
            "cancelGraceMs is not valid with execution SHARED_THREAD".to_owned(),
            vec![
                format!("task '{name}'"),
                "a thread cannot be cancelled or stopped, so a SHARED_THREAD task has no cancel \
                 grace"
                    .to_owned(),
            ],
            vec![
                "remove cancelGraceMs, or use execution SHARED_ASYNC with an async body, or \
                 ISOLATED"
                    .to_owned(),
            ],
        )),
        (
            ExecutionName::Isolated | ExecutionName::SharedAsync,
            MillisecondsFact::Set { milliseconds },
        ) => u32_of_number("cancelGraceMs", *milliseconds)
            .ok()
            .and_then(|value| CancelGraceMs::new(value).ok())
            .map(Some)
            .ok_or_else(|| grace_range_refusal(name, execution)),
    }
}

/// The refusal of a grace outside its range (`HRS-111`). Pure.
fn grace_range_refusal(name: &str, execution: ExecutionName) -> CoreRefusal {
    CoreRefusal::coded(
        ErrorCode::TaskCancelGraceOutOfRange,
        "cancelGraceMs is out of range".to_owned(),
        vec![
            format!("task '{name}'"),
            format!(
                "cancelGraceMs must be a whole number from {MIN_CANCEL_GRACE_MS} to \
                 {MAX_CANCEL_GRACE_MS} ms"
            ),
            format!("execution is {}", execution.execution()),
        ],
        vec!["set cancelGraceMs to a whole number of milliseconds inside the range".to_owned()],
    )
}

/// The refusal of a task option (`HRS-102`). Pure.
fn option_refusal(name: &str, message: String) -> CoreRefusal {
    CoreRefusal::coded(
        ErrorCode::TaskInvalidOptions,
        message,
        vec![format!("task '{name}'")],
        vec!["check the options of defineTask".to_owned()],
    )
}

/// The text of a boundary refusal: the field and the rule. Pure.
fn boundary_text(refusal: &BoundaryRefusal) -> String {
    format!("{}: {}", refusal.subject, refusal.rule)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sends::refusals::CoreRefusalCode;

    /// The facts of an `ISOLATED` task with no options.
    fn facts() -> TaskOptionsFacts {
        TaskOptionsFacts {
            task_name: "charge_payment".to_owned(),
            queue_name: "default".to_owned(),
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

    fn codes_of(facts: &TaskOptionsFacts) -> Vec<CoreRefusalCode> {
        match task_options_check(facts) {
            TaskOptionsCheck::Refused { refusals } => {
                refusals.into_iter().map(|refusal| refusal.code).collect()
            }
            TaskOptionsCheck::Accepted { .. } => panic!("the options are refused"),
        }
    }

    fn catalog(code: &str) -> CoreRefusalCode {
        CoreRefusalCode::Catalog {
            code: code.to_owned(),
        }
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
                auto_retry_for: vec!["TASK_TIMEOUT".to_owned(), "CARD_DECLINED".to_owned()],
            },
            ..facts()
        });
        assert_eq!(
            text,
            r#"{"retry_policy":{"max_retries":3,"intervals":[60,300,900],"backoff_strategy":"fixed","jitter":true,"auto_retry_for":["TASK_TIMEOUT","CARD_DECLINED"]},"good_until":null,"timeout_ms":30000,"execution":"ISOLATED","cancel_grace_ms":null}"#
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
                auto_retry_for: vec!["WORKER_CRASHED".to_owned()],
            },
            ..facts()
        });
        assert!(
            text.starts_with(r#"{"retry_policy":{"max_retries":5,"intervals":[30],"backoff_strategy":"exponential","jitter":false,"max_delay_seconds":600,"#),
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
        for milliseconds in [99.0, 10_001.0, 150.5] {
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
                    auto_retry_for: vec!["WORKER_CRASHED".to_owned()],
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
                    auto_retry_for: Vec::new(),
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
                    auto_retry_for: vec!["CardDeclinedError".to_owned()],
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
            auto_retry_for: vec!["PROVIDER_DOWN".to_owned(), "TASK_TIMEOUT".to_owned()],
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
                    auto_retry_for: vec!["WORKER_CRASHED".to_owned()],
                },
                ..facts()
            }),
            vec![catalog("HRS-102"), catalog("HRS-109"), catalog("HRS-110")]
        );
    }
}

//! The check of the options that a client declares for one task, before any
//! send: the retry policy and the codes that retry the task, the timeout,
//! the execution class and the cancel grace.
//!
//! - One copy of these rules for each client that checks a declaration at
//!   run time. Each refusal has one catalog code
//!   ([`DeclaredOptionRefusal::error_code`]). The client writes the text of
//!   a refusal with the names and the syntax of its own declaration.
//! - The class rules are the facts [`Execution::timeout_use`] and
//!   [`Execution::grace_use`], which the checks of stored options at a send
//!   (`check_shared_timeout`, `stored_execution`) read too. The bounds are
//!   [`MIN_TASK_TIMEOUT_MS`], [`CancelGraceMs::new`] and the constructors of
//!   [`RetryPolicy`]. A `u32` holds each timeout up to
//!   [`MAX_TASK_TIMEOUT_MS`](crate::MAX_TASK_TIMEOUT_MS), which is
//!   `u32::MAX`.
//! - Python reference: the `TaskOptions` and `RetryPolicy` models and
//!   `check_execution_rules` (`clients/python/horsies/core/task_decorator.py`).
//! - Pure: does no I/O and logs nothing.

use super::error::{
    check_error_code_text, BuiltInTaskCode, CodeTextRefusal, CustomCode, CustomCodeRefused,
    TaskErrorCode,
};
use super::execution::{Execution, GraceUse, TimeoutUse};
use super::options::{CancelGraceMs, RetryPolicy, RetryPolicyError, TaskOptions};
use crate::core::error::ErrorCode;
use crate::worker::retry::MIN_TASK_TIMEOUT_MS;

/// A duration in milliseconds that a task declares, or none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeclaredMilliseconds {
    /// The task declares none.
    NotSet,
    /// The task declares this many milliseconds.
    Set(u32),
}

/// The longest wait between two retries of an exponential policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeclaredMaxDelay {
    /// No cap: the wait doubles with each retry.
    NoCap,
    /// At most this many seconds.
    Seconds(u32),
}

/// The retry policy that a task declares, with the codes that retry the
/// task, each as its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeclaredRetry {
    /// The task does not retry.
    NoRetry,
    /// One wait per retry, in seconds.
    Fixed {
        intervals_seconds: Vec<u32>,
        jitter: bool,
        auto_retry_for: Vec<String>,
    },
    /// A base wait that doubles with each retry.
    Exponential {
        base_seconds: u32,
        max_retries: u32,
        jitter: bool,
        max_delay: DeclaredMaxDelay,
        auto_retry_for: Vec<String>,
    },
}

/// The custom codes that may retry a task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryCodeScope {
    /// Any custom code: the task declares no list of its codes.
    AnyCustomCode,
    /// Only a code of the task's declared list.
    Declared(Vec<String>),
}

/// The options of one task, as a client declares them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredTaskOptions {
    pub task_name: String,
    pub queue_name: String,
    pub execution: Execution,
    pub timeout_ms: DeclaredMilliseconds,
    pub cancel_grace_ms: DeclaredMilliseconds,
    pub retry: DeclaredRetry,
    pub retry_codes: RetryCodeScope,
}

/// Why a declared option is refused: the rule, and the part of the
/// declaration that breaks it. Each refusal has one catalog code.
#[derive(Debug, Clone)]
pub enum DeclaredOptionRefusal {
    /// The retry policy breaks a rule of [`RetryPolicy`] (`HRS-102`).
    RetryPolicy(RetryPolicyError),
    /// A retry policy names no code that retries the task (`HRS-102`).
    NoRetryCode,
    /// A retry code breaks the text rule of retry matching (`HRS-102`).
    RetryCodeText {
        code: String,
        refusal: CodeTextRefusal,
    },
    /// A retry code is neither a built-in code nor a custom code (`HRS-102`).
    RetryCodeNotCustomCode { refused: CustomCodeRefused },
    /// A custom retry code that the task's declared list does not hold
    /// (`HRS-102`).
    RetryCodeNotDeclared { code: String },
    /// A timeout below [`MIN_TASK_TIMEOUT_MS`] (`HRS-102`).
    TimeoutBelowMinimum,
    /// A class that needs a timeout, with none (`HRS-109`).
    SharedClassWithoutTimeout { execution: Execution },
    /// A grace on a class that takes none, `SHARED_THREAD` (`HRS-110`).
    GraceNotTaken { execution: Execution },
    /// A grace outside the bounds of [`CancelGraceMs`] (`HRS-111`).
    GraceOutOfRange { execution: Execution },
}

impl DeclaredOptionRefusal {
    /// The catalog code of the refusal.
    pub fn error_code(&self) -> ErrorCode {
        match self {
            Self::RetryPolicy(_)
            | Self::NoRetryCode
            | Self::RetryCodeText { .. }
            | Self::RetryCodeNotCustomCode { .. }
            | Self::RetryCodeNotDeclared { .. }
            | Self::TimeoutBelowMinimum => ErrorCode::TaskInvalidOptions,
            Self::SharedClassWithoutTimeout { .. } => ErrorCode::TaskSharedExecutionRequiresTimeout,
            Self::GraceNotTaken { .. } => ErrorCode::TaskCancelGraceRequiresSharedAsync,
            Self::GraceOutOfRange { .. } => ErrorCode::TaskCancelGraceOutOfRange,
        }
    }
}

/// The retry part of a declaration that passed its rules.
#[derive(Debug, Clone)]
enum CheckedRetry {
    NoRetry,
    Policy {
        policy: RetryPolicy,
        codes: Vec<TaskErrorCode>,
    },
}

/// The core's options of a declaration, or every refusal of it.
///
/// Order of the refusals: the retry policy, the codes that retry the task
/// (in their order), the timeout (below the minimum, or none for a class
/// that needs one), the cancel grace. A `SHARED_ASYNC` task that declares no
/// grace takes [`CancelGraceMs::DEFAULT`]. The options have no deadline
/// (`good_until`): a send adds its own. Pure.
pub fn check_declared_options(
    declared: DeclaredTaskOptions,
) -> Result<TaskOptions, Vec<DeclaredOptionRefusal>> {
    let retry = checked_retry(&declared.retry, &declared.retry_codes);
    let timeout = checked_timeout(declared.execution, declared.timeout_ms);
    let grace = checked_grace(declared.execution, declared.cancel_grace_ms);
    match (retry, timeout, grace) {
        (Ok(retry), Ok(timeout_ms), Ok(cancel_grace_ms)) => {
            let (retry_policy, auto_retry_for) = match retry {
                CheckedRetry::NoRetry => (None, None),
                CheckedRetry::Policy { policy, codes } => (Some(policy), Some(codes)),
            };
            Ok(TaskOptions {
                task_name: declared.task_name,
                queue_name: Some(declared.queue_name),
                good_until: None,
                auto_retry_for,
                retry_policy,
                timeout_ms,
                execution: declared.execution,
                cancel_grace_ms,
            })
        }
        (retry, timeout, grace) => Err(retry
            .err()
            .into_iter()
            .flatten()
            .chain(timeout.err())
            .chain(grace.err())
            .collect()),
    }
}

/// The retry policy and its codes, or every refusal of them. Pure.
fn checked_retry(
    retry: &DeclaredRetry,
    scope: &RetryCodeScope,
) -> Result<CheckedRetry, Vec<DeclaredOptionRefusal>> {
    let (policy, codes) = match retry {
        DeclaredRetry::NoRetry => return Ok(CheckedRetry::NoRetry),
        DeclaredRetry::Fixed {
            intervals_seconds,
            jitter,
            auto_retry_for,
        } => (
            RetryPolicy::fixed(intervals_seconds.clone(), *jitter),
            auto_retry_for,
        ),
        DeclaredRetry::Exponential {
            base_seconds,
            max_retries,
            jitter,
            max_delay,
            auto_retry_for,
        } => (
            exponential_policy(*base_seconds, *max_retries, *jitter, *max_delay),
            auto_retry_for,
        ),
    };
    let codes = retry_codes(codes, scope);
    match (policy, codes) {
        (Ok(policy), Ok(codes)) => Ok(CheckedRetry::Policy { policy, codes }),
        (policy, codes) => Err(policy
            .err()
            .map(DeclaredOptionRefusal::RetryPolicy)
            .into_iter()
            .chain(codes.err().into_iter().flatten())
            .collect()),
    }
}

/// An exponential policy with its cap, or the rule that it breaks. Pure.
fn exponential_policy(
    base_seconds: u32,
    max_retries: u32,
    jitter: bool,
    max_delay: DeclaredMaxDelay,
) -> Result<RetryPolicy, RetryPolicyError> {
    let policy = RetryPolicy::exponential(base_seconds, max_retries, jitter)?;
    let policy = match max_delay {
        DeclaredMaxDelay::NoCap => policy,
        DeclaredMaxDelay::Seconds(seconds) => policy.with_max_delay_seconds(seconds),
    };
    policy.validate()?;
    Ok(policy)
}

/// The codes that retry a task, or the refusal of each code that breaks a
/// rule: at least one code; the text rule of retry matching; a code that is
/// not built in is a custom code; with a declared list, a custom code is one of
/// the list. Pure.
fn retry_codes(
    codes: &[String],
    scope: &RetryCodeScope,
) -> Result<Vec<TaskErrorCode>, Vec<DeclaredOptionRefusal>> {
    if codes.is_empty() {
        return Err(vec![DeclaredOptionRefusal::NoRetryCode]);
    }
    let (accepted, refused): (Vec<_>, Vec<_>) = codes
        .iter()
        .map(|code| retry_code(code, scope))
        .partition(Result::is_ok);
    match refused.is_empty() {
        true => Ok(accepted.into_iter().filter_map(Result::ok).collect()),
        false => Err(refused.into_iter().filter_map(Result::err).collect()),
    }
}

/// One code that retries a task, or its refusal. Pure.
fn retry_code(code: &str, scope: &RetryCodeScope) -> Result<TaskErrorCode, DeclaredOptionRefusal> {
    check_error_code_text(code).map_err(|refusal| DeclaredOptionRefusal::RetryCodeText {
        code: code.to_owned(),
        refusal,
    })?;
    let user = match BuiltInTaskCode::parse(code) {
        Some(built_in) => return Ok(TaskErrorCode::BuiltIn(built_in)),
        None => CustomCode::parse(code)
            .map_err(|refused| DeclaredOptionRefusal::RetryCodeNotCustomCode { refused })?,
    };
    match scope {
        RetryCodeScope::AnyCustomCode => Ok(TaskErrorCode::Custom(user)),
        RetryCodeScope::Declared(declared) => match declared.iter().any(|known| known == code) {
            true => Ok(TaskErrorCode::Custom(user)),
            false => Err(DeclaredOptionRefusal::RetryCodeNotDeclared {
                code: code.to_owned(),
            }),
        },
    }
}

/// The timeout of a task, or its refusal: a value below the minimum, or
/// none for a class that needs one. Pure.
fn checked_timeout(
    execution: Execution,
    timeout: DeclaredMilliseconds,
) -> Result<Option<u32>, DeclaredOptionRefusal> {
    match (execution.timeout_use(), timeout) {
        (TimeoutUse::Required, DeclaredMilliseconds::NotSet) => {
            Err(DeclaredOptionRefusal::SharedClassWithoutTimeout { execution })
        }
        (TimeoutUse::Optional, DeclaredMilliseconds::NotSet) => Ok(None),
        (TimeoutUse::Required | TimeoutUse::Optional, DeclaredMilliseconds::Set(milliseconds)) => {
            match u64::from(milliseconds) < MIN_TASK_TIMEOUT_MS {
                true => Err(DeclaredOptionRefusal::TimeoutBelowMinimum),
                false => Ok(Some(milliseconds)),
            }
        }
    }
}

/// The cancel grace of a task, or its refusal: a grace on a class that takes
/// none, or a grace outside its bounds. A class that needs a grace and
/// declares none takes the default. Pure.
fn checked_grace(
    execution: Execution,
    grace: DeclaredMilliseconds,
) -> Result<Option<CancelGraceMs>, DeclaredOptionRefusal> {
    match (execution.grace_use(), grace) {
        (GraceUse::Needed, DeclaredMilliseconds::NotSet) => Ok(Some(CancelGraceMs::DEFAULT)),
        (GraceUse::Optional | GraceUse::NotTaken, DeclaredMilliseconds::NotSet) => Ok(None),
        (GraceUse::NotTaken, DeclaredMilliseconds::Set(_)) => {
            Err(DeclaredOptionRefusal::GraceNotTaken { execution })
        }
        (GraceUse::Needed | GraceUse::Optional, DeclaredMilliseconds::Set(milliseconds)) => {
            CancelGraceMs::new(milliseconds)
                .map(Some)
                .map_err(|_out_of_range| DeclaredOptionRefusal::GraceOutOfRange { execution })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::retry::MAX_TASK_TIMEOUT_MS;

    /// The declaration of an `ISOLATED` task with no options.
    fn declared() -> DeclaredTaskOptions {
        DeclaredTaskOptions {
            task_name: "charge_payment".to_owned(),
            queue_name: "default".to_owned(),
            execution: Execution::Isolated,
            timeout_ms: DeclaredMilliseconds::NotSet,
            cancel_grace_ms: DeclaredMilliseconds::NotSet,
            retry: DeclaredRetry::NoRetry,
            retry_codes: RetryCodeScope::AnyCustomCode,
        }
    }

    fn accepted(declared: DeclaredTaskOptions) -> TaskOptions {
        check_declared_options(declared).expect("the declaration is accepted")
    }

    fn refusals_of(declared: DeclaredTaskOptions) -> Vec<DeclaredOptionRefusal> {
        match check_declared_options(declared) {
            Ok(options) => panic!("the declaration is refused, got {options:?}"),
            Err(refusals) => refusals,
        }
    }

    /// The catalog code of each refusal, as its text.
    fn codes_of(declared: DeclaredTaskOptions) -> Vec<&'static str> {
        refusals_of(declared)
            .iter()
            .map(|refusal| refusal.error_code().as_code_str())
            .collect()
    }

    fn fixed_retry(codes: &[&str]) -> DeclaredRetry {
        DeclaredRetry::Fixed {
            intervals_seconds: vec![60],
            jitter: true,
            auto_retry_for: codes.iter().map(|code| (*code).to_owned()).collect(),
        }
    }

    #[test]
    fn an_isolated_task_with_no_options_stores_the_text_with_nulls() {
        let options = accepted(declared());
        assert_eq!(options.queue_name.as_deref(), Some("default"));
        assert_eq!(
            options.stored_options_text(),
            r#"{"retry_policy":null,"good_until":null,"timeout_ms":null,"execution":"ISOLATED","cancel_grace_ms":null}"#
        );
    }

    #[test]
    fn a_fixed_policy_keeps_its_codes_in_their_order() {
        let options = accepted(DeclaredTaskOptions {
            retry: DeclaredRetry::Fixed {
                intervals_seconds: vec![60, 300, 900],
                jitter: true,
                auto_retry_for: vec!["TASK_TIMEOUT".to_owned(), "CARD_DECLINED".to_owned()],
            },
            ..declared()
        });
        assert!(
            options.stored_options_text().starts_with(
                r#"{"retry_policy":{"max_retries":3,"intervals":[60,300,900],"backoff_strategy":"fixed","jitter":true,"auto_retry_for":["TASK_TIMEOUT","CARD_DECLINED"]}"#
            ),
            "{}",
            options.stored_options_text()
        );
    }

    #[test]
    fn an_exponential_policy_keeps_its_max_delay() {
        let options = accepted(DeclaredTaskOptions {
            retry: DeclaredRetry::Exponential {
                base_seconds: 30,
                max_retries: 5,
                jitter: false,
                max_delay: DeclaredMaxDelay::Seconds(600),
                auto_retry_for: vec!["WORKER_CRASHED".to_owned()],
            },
            ..declared()
        });
        assert_eq!(
            options
                .retry_policy
                .and_then(|policy| policy.max_delay_seconds),
            Some(600)
        );
    }

    #[test]
    fn a_shared_async_task_with_no_grace_takes_the_default_grace() {
        let options = accepted(DeclaredTaskOptions {
            execution: Execution::SharedAsync,
            timeout_ms: DeclaredMilliseconds::Set(10_000),
            ..declared()
        });
        assert_eq!(options.cancel_grace_ms, Some(CancelGraceMs::DEFAULT));
        assert_eq!(options.timeout_ms, Some(10_000));
    }

    #[test]
    fn an_isolated_task_keeps_a_grace_inside_the_bounds() {
        let options = accepted(DeclaredTaskOptions {
            cancel_grace_ms: DeclaredMilliseconds::Set(500),
            ..declared()
        });
        assert_eq!(options.cancel_grace_ms.map(CancelGraceMs::get), Some(500));
    }

    #[test]
    fn a_retry_policy_that_breaks_a_rule_is_hrs_102() {
        let refusals = refusals_of(DeclaredTaskOptions {
            retry: DeclaredRetry::Exponential {
                base_seconds: 30,
                max_retries: 21,
                jitter: true,
                max_delay: DeclaredMaxDelay::NoCap,
                auto_retry_for: vec!["WORKER_CRASHED".to_owned()],
            },
            ..declared()
        });
        assert!(matches!(
            refusals.as_slice(),
            [DeclaredOptionRefusal::RetryPolicy(
                RetryPolicyError::MaxRetriesOutOfRange(21)
            )]
        ));
        assert_eq!(refusals[0].error_code(), ErrorCode::TaskInvalidOptions);
    }

    #[test]
    fn a_max_delay_of_zero_is_hrs_102() {
        let refusals = refusals_of(DeclaredTaskOptions {
            retry: DeclaredRetry::Exponential {
                base_seconds: 30,
                max_retries: 3,
                jitter: true,
                max_delay: DeclaredMaxDelay::Seconds(0),
                auto_retry_for: vec!["WORKER_CRASHED".to_owned()],
            },
            ..declared()
        });
        assert!(matches!(
            refusals.as_slice(),
            [DeclaredOptionRefusal::RetryPolicy(
                RetryPolicyError::NonPositiveMaxDelay
            )]
        ));
    }

    #[test]
    fn a_policy_with_no_retry_code_is_hrs_102() {
        let refusals = refusals_of(DeclaredTaskOptions {
            retry: fixed_retry(&[]),
            ..declared()
        });
        assert!(matches!(
            refusals.as_slice(),
            [DeclaredOptionRefusal::NoRetryCode]
        ));
    }

    #[test]
    fn a_retry_code_that_breaks_the_text_rule_is_hrs_102_with_the_code() {
        let refusals = refusals_of(DeclaredTaskOptions {
            retry: fixed_retry(&["CardDeclinedError", "WORKFLOW_STOPPED"]),
            ..declared()
        });
        assert!(matches!(
            refusals.as_slice(),
            [
                DeclaredOptionRefusal::RetryCodeText {
                    refusal: CodeTextRefusal::ExceptionClassName,
                    ..
                },
                DeclaredOptionRefusal::RetryCodeText {
                    refusal: CodeTextRefusal::ReservedName,
                    ..
                },
            ]
        ));
    }

    #[test]
    fn a_user_retry_code_outside_the_declared_list_is_hrs_102() {
        let retry = fixed_retry(&["PROVIDER_DOWN", "TASK_TIMEOUT"]);
        let refusals = refusals_of(DeclaredTaskOptions {
            retry: retry.clone(),
            retry_codes: RetryCodeScope::Declared(vec!["CARD_DECLINED".to_owned()]),
            ..declared()
        });
        assert!(matches!(
            refusals.as_slice(),
            [DeclaredOptionRefusal::RetryCodeNotDeclared { code }] if code == "PROVIDER_DOWN"
        ));
        let options = accepted(DeclaredTaskOptions {
            retry,
            retry_codes: RetryCodeScope::Declared(vec!["PROVIDER_DOWN".to_owned()]),
            ..declared()
        });
        assert_eq!(options.auto_retry_for.map(|codes| codes.len()), Some(2));
    }

    #[test]
    fn a_timeout_below_the_minimum_is_hrs_102() {
        let below = u32::try_from(MIN_TASK_TIMEOUT_MS - 1).expect("the minimum fits a u32");
        assert_eq!(
            codes_of(DeclaredTaskOptions {
                timeout_ms: DeclaredMilliseconds::Set(below),
                ..declared()
            }),
            vec!["HRS-102"]
        );
        let minimum = u32::try_from(MIN_TASK_TIMEOUT_MS).expect("the minimum fits a u32");
        assert_eq!(
            accepted(DeclaredTaskOptions {
                timeout_ms: DeclaredMilliseconds::Set(minimum),
                ..declared()
            })
            .timeout_ms,
            Some(minimum)
        );
    }

    /// The largest `u32` is the largest task timeout, so the type holds the
    /// upper bound.
    #[test]
    fn the_largest_u32_is_the_largest_task_timeout() {
        assert_eq!(u64::from(u32::MAX), MAX_TASK_TIMEOUT_MS);
    }

    #[test]
    fn a_shared_class_with_no_timeout_is_hrs_109() {
        for execution in [Execution::SharedAsync, Execution::SharedThread] {
            assert_eq!(
                codes_of(DeclaredTaskOptions {
                    execution,
                    ..declared()
                }),
                vec!["HRS-109"],
                "{execution}"
            );
        }
    }

    #[test]
    fn a_grace_with_shared_thread_is_hrs_110() {
        assert_eq!(
            codes_of(DeclaredTaskOptions {
                execution: Execution::SharedThread,
                timeout_ms: DeclaredMilliseconds::Set(10_000),
                cancel_grace_ms: DeclaredMilliseconds::Set(1_000),
                ..declared()
            }),
            vec!["HRS-110"]
        );
    }

    #[test]
    fn a_grace_outside_its_bounds_is_hrs_111() {
        for (execution, milliseconds) in [
            (Execution::Isolated, 99),
            (Execution::Isolated, 10_001),
            (Execution::SharedAsync, 99),
        ] {
            assert_eq!(
                codes_of(DeclaredTaskOptions {
                    execution,
                    timeout_ms: DeclaredMilliseconds::Set(10_000),
                    cancel_grace_ms: DeclaredMilliseconds::Set(milliseconds),
                    ..declared()
                }),
                vec!["HRS-111"],
                "{execution} {milliseconds}"
            );
        }
    }

    #[test]
    fn every_refusal_comes_in_one_list_in_the_stated_order() {
        assert_eq!(
            codes_of(DeclaredTaskOptions {
                execution: Execution::SharedThread,
                cancel_grace_ms: DeclaredMilliseconds::Set(1_000),
                retry: DeclaredRetry::Fixed {
                    intervals_seconds: vec![0],
                    jitter: true,
                    auto_retry_for: vec!["lower_case".to_owned()],
                },
                ..declared()
            }),
            vec!["HRS-102", "HRS-102", "HRS-109", "HRS-110"]
        );
    }
}

//! The user's error codes: a closed enum of the task's own codes.
//!
//! `#[derive(horsies::UserErrorCode)]` on an enum of unit variants gives each
//! variant one code, its name in UPPER_SNAKE_CASE (`RateLimited` is
//! `RATE_LIMITED`). A code with the name of a built-in code or of a reserved
//! code is a compile error, the rule that `UserCode::parse` applies at run
//! time (Python `TaskError._validate_error_code`).

use horsies_core::catalog::{CatalogMember, CodeEnum};
use horsies_core::core::task::error::RESERVED_NON_TASK_CODES;
use horsies_core::{ContractCode, OperationalErrorCode, OutcomeCode, RetrievalCode, UserCode};

use crate::{ErrorCode, HorsiesError};

/// The error codes that a task's own failures carry.
///
/// Derive it: `#[derive(horsies::UserErrorCode)]`. The derive checks every
/// rule at compile time. A hand-written impl keeps the same contract: `code`
/// returns one of `CODES`, `from_code` reads exactly the texts of `CODES`,
/// and no text of `CODES` names a built-in or reserved code; the app checks
/// `CODES` when it registers the task.
///
/// `std::convert::Infallible` is the code type of a task that has no codes
/// of its own.
pub trait UserErrorCode:
    Sized + Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static
{
    /// The code text of every variant, in declaration order.
    const CODES: &'static [&'static str];

    /// The stored code text of this code.
    fn code(&self) -> &str;

    /// The code whose stored text is `text`, or `None` when the type has
    /// no such code.
    fn from_code(text: &str) -> Option<Self>;
}

/// No code of its own: a task whose failures carry library codes only.
impl UserErrorCode for std::convert::Infallible {
    const CODES: &'static [&'static str] = &[];

    fn code(&self) -> &'static str {
        match *self {}
    }

    fn from_code(_text: &str) -> Option<Self> {
        None
    }
}

/// Any user code, as checked text: the code type of a read that takes the
/// codes of several tasks, such as a workflow result or a dependency result
/// whose task has another code type (Python's `str` code). `CODES` is empty:
/// the set is open, and `from_code` takes every text that `UserCode::parse`
/// takes.
impl UserErrorCode for UserCode {
    const CODES: &'static [&'static str] = &[];

    fn code(&self) -> &str {
        self.as_str()
    }

    fn from_code(text: &str) -> Option<Self> {
        UserCode::parse(text).ok()
    }
}

/// The members of the four built-in code families (Python
/// `BuiltInTaskCode`), from the core's catalogs.
const BUILT_IN_FAMILIES: [&[CatalogMember]; 4] = [
    <OperationalErrorCode as CodeEnum>::CATALOG.members,
    <ContractCode as CodeEnum>::CATALOG.members,
    <RetrievalCode as CodeEnum>::CATALOG.members,
    <OutcomeCode as CodeEnum>::CATALOG.members,
];

/// Whether `text` may be a user code: it is not the name of a built-in code
/// and not a reserved name. A `const fn`, so that the derive checks each code
/// at compile time with the core's own catalogs.
pub(crate) const fn is_free_user_code(text: &str) -> bool {
    let mut family = 0;
    while family < BUILT_IN_FAMILIES.len() {
        let members = BUILT_IN_FAMILIES[family];
        let mut member = 0;
        while member < members.len() {
            if same_text(members[member].value, text) {
                return false;
            }
            member += 1;
        }
        family += 1;
    }
    let mut reserved = 0;
    while reserved < RESERVED_NON_TASK_CODES.len() {
        if same_text(RESERVED_NON_TASK_CODES[reserved], text) {
            return false;
        }
        reserved += 1;
    }
    true
}

/// Whether `text` is the name of a built-in code (Python `BuiltInTaskCode`):
/// a member of one of the four built-in families. A `const fn` over the
/// core's catalogs.
pub(crate) const fn is_built_in_code(text: &str) -> bool {
    let mut family = 0;
    while family < BUILT_IN_FAMILIES.len() {
        let members = BUILT_IN_FAMILIES[family];
        let mut member = 0;
        while member < members.len() {
            if same_text(members[member].value, text) {
                return true;
            }
            member += 1;
        }
        family += 1;
    }
    false
}

/// Whether `text` may stand in `auto_retry_for` of a task whose code type
/// has `user_codes`: a built-in code, or one of `user_codes`. The task
/// attribute asserts it for each entry at compile time.
pub(crate) const fn is_retry_code(text: &str, user_codes: &[&str]) -> bool {
    if is_built_in_code(text) {
        return true;
    }
    let mut index = 0;
    while index < user_codes.len() {
        if same_text(user_codes[index], text) {
            return true;
        }
        index += 1;
    }
    false
}

/// The stored retry code of an `auto_retry_for` entry: the built-in code
/// of that name, else the user code. The task attribute asserted at
/// compile time that `text` is one of the two, and `E::CODES` holds only
/// user codes (`user_code_refusals`, checked before the registration), so a
/// text that is neither is a defect.
pub(crate) fn stored_retry_code(text: &str) -> horsies_core::TaskErrorCode {
    match horsies_core::BuiltInTaskCode::parse(text) {
        Some(code) => horsies_core::TaskErrorCode::BuiltIn(code),
        None => {
            horsies_core::TaskErrorCode::User(UserCode::parse(text).unwrap_or_else(|refused| {
                panic!("auto_retry_for code {text:?} is not a retry code: {refused}")
            }))
        }
    }
}

const fn same_text(left: &str, right: &str) -> bool {
    let (left, right) = (left.as_bytes(), right.as_bytes());
    if left.len() != right.len() {
        return false;
    }
    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }
    true
}

/// The user code of `code`, as the core stores it.
///
/// `UserCode::parse` refuses only a built-in or reserved name. The derive
/// refuses those at compile time and the app checks `E::CODES` at
/// registration, so a refusal here is a defect of a hand-written impl.
pub(crate) fn stored_user_code<E: UserErrorCode>(code: &E) -> UserCode {
    UserCode::parse(code.code()).unwrap_or_else(|refused| {
        panic!(
            "UserErrorCode impl of {} returned a code that is not a user code: {refused}",
            std::any::type_name::<E>()
        )
    })
}

/// The codes of `E` that a user code may not have, as registration
/// refusals (`HRS-212`). Empty for every derived type. Pure.
pub(crate) fn user_code_refusals<E: UserErrorCode>(task_name: &str) -> Vec<HorsiesError> {
    E::CODES
        .iter()
        .filter_map(|code| UserCode::parse(*code).err())
        .map(|refused| {
            HorsiesError::new(format!(
                "task '{task_name}': error code type {} has a code that is not a user code: \
                 {refused}",
                std::any::type_name::<E>()
            ))
            .with_code(ErrorCode::CheckReservedCodeCollision)
            .with_help("derive the code type with #[derive(horsies::UserErrorCode)]")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use horsies_core::BuiltInTaskCode;

    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    enum HandWritten {
        Fine,
        Collides,
    }

    impl UserErrorCode for HandWritten {
        const CODES: &'static [&'static str] = &["FINE", "BROKER_ERROR"];

        fn code(&self) -> &'static str {
            match self {
                Self::Fine => "FINE",
                Self::Collides => "BROKER_ERROR",
            }
        }

        fn from_code(text: &str) -> Option<Self> {
            match text {
                "FINE" => Some(Self::Fine),
                "BROKER_ERROR" => Some(Self::Collides),
                _ => None,
            }
        }
    }

    #[test]
    fn every_built_in_code_is_not_a_free_user_code() {
        let built_in: Vec<BuiltInTaskCode> = OperationalErrorCode::ALL
            .into_iter()
            .map(BuiltInTaskCode::from)
            .chain(ContractCode::ALL.into_iter().map(BuiltInTaskCode::from))
            .chain(RetrievalCode::ALL.into_iter().map(BuiltInTaskCode::from))
            .chain(OutcomeCode::ALL.into_iter().map(BuiltInTaskCode::from))
            .collect();
        for code in built_in {
            assert!(!is_free_user_code(&code.value()), "{code}");
        }
    }

    #[test]
    fn a_reserved_name_is_not_a_free_user_code() {
        for reserved in RESERVED_NON_TASK_CODES {
            assert!(!is_free_user_code(reserved), "{reserved}");
        }
    }

    #[test]
    fn the_const_check_agrees_with_user_code_parse() {
        for text in [
            "RATE_LIMITED",
            "TASK_TIMEOUT",
            "WORKFLOW_STOPPED",
            "TASK_TIMEOUTS",
            "",
            "broker_error",
        ] {
            assert_eq!(
                is_free_user_code(text),
                UserCode::parse(text).is_ok(),
                "{text:?}"
            );
        }
    }

    #[test]
    fn a_retry_code_is_a_built_in_code_or_a_code_of_the_type() {
        assert!(is_retry_code("TASK_TIMEOUT", &[]));
        assert!(is_retry_code("WORKER_CRASHED", &["FINE"]));
        assert!(is_retry_code("FINE", HandWritten::CODES));
        assert!(!is_retry_code("RATE_LIMITED", HandWritten::CODES));
        assert!(!is_retry_code("WORKFLOW_STOPPED", &[]));
    }

    #[test]
    fn the_stored_retry_code_keeps_built_in_and_user_codes_apart() {
        assert_eq!(
            stored_retry_code("TASK_TIMEOUT"),
            horsies_core::TaskErrorCode::BuiltIn(BuiltInTaskCode::from(OutcomeCode::TaskTimeout))
        );
        assert_eq!(
            stored_retry_code("RATE_LIMITED"),
            horsies_core::TaskErrorCode::User(UserCode::parse("RATE_LIMITED").unwrap())
        );
    }

    #[test]
    fn infallible_has_no_code() {
        assert!(<std::convert::Infallible as UserErrorCode>::CODES.is_empty());
        assert_eq!(
            <std::convert::Infallible as UserErrorCode>::from_code("ANY"),
            None
        );
    }

    #[test]
    fn user_code_reads_every_user_code_and_no_built_in_name() {
        assert!(<UserCode as UserErrorCode>::CODES.is_empty());
        let read = <UserCode as UserErrorCode>::from_code("RATE_LIMITED").unwrap();
        assert_eq!(read.code(), "RATE_LIMITED");
        assert_eq!(<UserCode as UserErrorCode>::from_code("TASK_TIMEOUT"), None);
        assert_eq!(
            <UserCode as UserErrorCode>::from_code("WORKFLOW_STOPPED"),
            None
        );
        assert!(user_code_refusals::<UserCode>("charge").is_empty());
    }

    #[test]
    fn a_hand_written_impl_with_a_built_in_name_is_refused_at_registration() {
        let refusals = user_code_refusals::<HandWritten>("charge");
        assert_eq!(refusals.len(), 1);
        assert_eq!(
            refusals[0].code,
            Some(ErrorCode::CheckReservedCodeCollision)
        );
        assert!(refusals[0].message.contains("charge"));
        assert!(user_code_refusals::<std::convert::Infallible>("charge").is_empty());
    }

    #[test]
    fn stored_user_code_keeps_the_text() {
        assert_eq!(stored_user_code(&HandWritten::Fine).as_str(), "FINE");
    }

    #[test]
    #[should_panic(expected = "not a user code")]
    fn stored_user_code_of_a_built_in_name_is_a_defect() {
        let _code = stored_user_code(&HandWritten::Collides);
    }
}

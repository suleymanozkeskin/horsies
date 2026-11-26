//! The custom error codes: a closed enum of the task's own codes.
//!
//! `#[derive(horsies::CustomErrorCode)]` on an enum of unit variants gives each
//! variant one code, its name in UPPER_SNAKE_CASE (`RateLimited` is
//! `RATE_LIMITED`). A code with the name of a built-in code or of a reserved
//! code is a compile error, the rule that `CustomCode::parse` applies at run
//! time (Python `TaskError._validate_code`).

use horsies_core::catalog::{CatalogMember, CodeEnum};
use horsies_core::core::task::error::RESERVED_NON_TASK_CODES;
use horsies_core::{ContractCode, CustomCode, OperationalErrorCode, OutcomeCode, RetrievalCode};

/// The error codes that a task's own failures carry.
///
/// Derive it: `#[derive(horsies::CustomErrorCode)]`. The derive checks every
/// rule at compile time. A hand-written impl keeps the same contract: `as_str`
/// returns one of `CODES`, `from_code` reads exactly the texts of `CODES`,
/// and no text of `CODES` names a built-in or reserved code; the app checks
/// `CODES` when it registers the task.
///
/// `std::convert::Infallible` is the code type of a task that has no codes
/// of its own.
pub trait CustomErrorCode:
    Sized + Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static
{
    /// The code text of every variant, in declaration order.
    const CODES: &'static [&'static str];

    /// The stored code text of this code.
    fn as_str(&self) -> &str;

    /// The code whose stored text is `text`, or `None` when the type has
    /// no such code.
    fn from_code(text: &str) -> Option<Self>;
}

/// No code of its own: a task whose failures carry built-in codes only.
impl CustomErrorCode for std::convert::Infallible {
    const CODES: &'static [&'static str] = &[];

    fn as_str(&self) -> &'static str {
        match *self {}
    }

    fn from_code(_text: &str) -> Option<Self> {
        None
    }
}

/// Any custom code, as checked text: the code type of a read that takes the
/// codes of several tasks, such as a workflow result or a dependency result
/// whose task has another code type (Python's `str` code). `CODES` is empty:
/// the set is open, and `from_code` takes every text that `CustomCode::parse`
/// takes.
impl CustomErrorCode for CustomCode {
    const CODES: &'static [&'static str] = &[];

    fn as_str(&self) -> &str {
        // The inherent method of `CustomCode`, not this trait method.
        CustomCode::as_str(self)
    }

    fn from_code(text: &str) -> Option<Self> {
        CustomCode::parse(text).ok()
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

/// Whether `text` may be a custom code: it is not the name of a built-in code
/// and not a reserved name. A `const fn`, so that the derive checks each code
/// at compile time with the core's own catalogs.
pub(crate) const fn is_free_custom_code(text: &str) -> bool {
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

/// Whether `text` may stand in `retry_for` of a task whose code type
/// has `custom_codes`: a built-in code, or one of `custom_codes`. The task
/// attribute asserts it for each entry at compile time.
pub(crate) const fn is_retry_code(text: &str, custom_codes: &[&str]) -> bool {
    if is_built_in_code(text) {
        return true;
    }
    let mut index = 0;
    while index < custom_codes.len() {
        if same_text(custom_codes[index], text) {
            return true;
        }
        index += 1;
    }
    false
}

/// The stored retry code of a `retry_for` entry: the built-in code
/// of that name, else the custom code. The task attribute asserted at
/// compile time that `text` is one of the two, and `E::CODES` holds only
/// custom codes (the registration refuses a set with another name), so a
/// text that is neither is a defect.
pub(crate) fn stored_retry_code(text: &str) -> horsies_core::TaskErrorCode {
    match horsies_core::BuiltInTaskCode::parse(text) {
        Some(code) => horsies_core::TaskErrorCode::BuiltIn(code),
        None => {
            horsies_core::TaskErrorCode::Custom(CustomCode::parse(text).unwrap_or_else(|refused| {
                panic!("retry_for code {text:?} is not a retry code: {refused}")
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

/// The custom code of `code`, as the core stores it.
///
/// `CustomCode::parse` refuses only a built-in or reserved name. The derive
/// refuses those at compile time and the app checks `E::CODES` at
/// registration, so a refusal here is a defect of a hand-written impl.
pub(crate) fn stored_custom_code<E: CustomErrorCode>(code: &E) -> CustomCode {
    CustomCode::parse(code.as_str()).unwrap_or_else(|refused| {
        panic!(
            "CustomErrorCode impl of {} returned a code that is not a custom code: {refused}",
            std::any::type_name::<E>()
        )
    })
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

    impl CustomErrorCode for HandWritten {
        const CODES: &'static [&'static str] = &["FINE", "BROKER_ERROR"];

        fn as_str(&self) -> &'static str {
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
    fn every_built_in_code_is_not_a_free_custom_code() {
        let built_in: Vec<BuiltInTaskCode> = OperationalErrorCode::ALL
            .into_iter()
            .map(BuiltInTaskCode::from)
            .chain(ContractCode::ALL.into_iter().map(BuiltInTaskCode::from))
            .chain(RetrievalCode::ALL.into_iter().map(BuiltInTaskCode::from))
            .chain(OutcomeCode::ALL.into_iter().map(BuiltInTaskCode::from))
            .collect();
        for code in built_in {
            assert!(!is_free_custom_code(code.as_str()), "{code}");
        }
    }

    #[test]
    fn a_reserved_name_is_not_a_free_custom_code() {
        for reserved in RESERVED_NON_TASK_CODES {
            assert!(!is_free_custom_code(reserved), "{reserved}");
        }
    }

    #[test]
    fn the_const_check_agrees_with_custom_code_parse() {
        for text in [
            "RATE_LIMITED",
            "TASK_TIMEOUT",
            "WORKFLOW_STOPPED",
            "TASK_TIMEOUTS",
            "",
            "broker_error",
        ] {
            assert_eq!(
                is_free_custom_code(text),
                CustomCode::parse(text).is_ok(),
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
    fn the_stored_retry_code_keeps_built_in_and_custom_codes_apart() {
        assert_eq!(
            stored_retry_code("TASK_TIMEOUT"),
            horsies_core::TaskErrorCode::BuiltIn(BuiltInTaskCode::from(OutcomeCode::TaskTimeout))
        );
        assert_eq!(
            stored_retry_code("RATE_LIMITED"),
            horsies_core::TaskErrorCode::Custom(CustomCode::parse("RATE_LIMITED").unwrap())
        );
    }

    #[test]
    fn infallible_has_no_code() {
        assert_eq!(
            <std::convert::Infallible as CustomErrorCode>::CODES,
            &[] as &[&str]
        );
        assert_eq!(
            <std::convert::Infallible as CustomErrorCode>::from_code("ANY"),
            None
        );
    }

    #[test]
    fn custom_code_reads_every_custom_code_and_no_built_in_name() {
        assert_eq!(<CustomCode as CustomErrorCode>::CODES, &[] as &[&str]);
        let read = <CustomCode as CustomErrorCode>::from_code("RATE_LIMITED").unwrap();
        assert_eq!(CustomErrorCode::as_str(&read), "RATE_LIMITED");
        assert_eq!(
            <CustomCode as CustomErrorCode>::from_code("TASK_TIMEOUT"),
            None
        );
        assert_eq!(
            <CustomCode as CustomErrorCode>::from_code("WORKFLOW_STOPPED"),
            None
        );
    }

    #[test]
    fn stored_custom_code_keeps_the_text() {
        assert_eq!(stored_custom_code(&HandWritten::Fine).as_str(), "FINE");
    }

    #[test]
    #[should_panic(expected = "not a custom code")]
    fn stored_custom_code_of_a_built_in_name_is_a_defect() {
        let _code = stored_custom_code(&HandWritten::Collides);
    }
}

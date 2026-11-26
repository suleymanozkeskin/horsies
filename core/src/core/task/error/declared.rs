//! The user error codes that a task declares, and the value of the
//! `error_code` metric label of an error code.
//!
//! The label set of one task is every built-in code by its value, the codes
//! that the task declares, and [`UNDECLARED_LABEL`]. A user code that the
//! task does not declare takes [`UNDECLARED_LABEL`]. A named value closes the
//! set; no count limits it. Only the label folds a code: events, traces, logs
//! and the history row keep the exact code.
//!
//! A declared code passes the code-text rule ([`check_error_code_text`]) and
//! is not the name of a built-in code. [`UNDECLARED_LABEL`] is not
//! `UPPER_SNAKE_CASE`, so it is never the value of a built-in or a declared
//! code.
//!
//! The Python client takes the codes from the task decorator
//! (`error_codes=`); the Rust client from `UserErrorCode::CODES` of the
//! task's code type. Each client refuses a bad set when the task is defined,
//! with the HRS code of [`DeclaredCodeRefusal::error_code`].

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::{check_error_code_text, BuiltInTaskCode, CodeTextRefusal, TaskErrorCode};
use crate::catalog::CodeEnum;
use crate::core::error::HorsiesErrorCode;

/// The label value of a user code that the task does not declare.
pub const UNDECLARED_LABEL: &str = "undeclared";

/// One user error code that a task declares. Only [`DeclaredErrorCodes::parse`]
/// builds one: the text passes the code-text rule and is not the name of a
/// built-in code.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DeclaredErrorCode(String);

impl DeclaredErrorCode {
    /// The code text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for DeclaredErrorCode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// The user error codes that one task declares, each once, sorted by text.
/// Empty when the task declares none.
///
/// The JSON form is an array of the code texts. A read refuses the array as
/// [`DeclaredErrorCodes::parse`] does.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<String>", into = "Vec<String>")]
pub struct DeclaredErrorCodes(Vec<DeclaredErrorCode>);

/// Why one entry of a declared set is refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeclaredCodeRefusal {
    /// The entry fails the code-text rule: it is empty, not
    /// `UPPER_SNAKE_CASE`, the name of an exception class, or a reserved
    /// name.
    #[error("{}", .refusal.message("declared error code", .code))]
    Text {
        code: String,
        refusal: CodeTextRefusal,
    },
    /// The entry is the name of a built-in code. Each label set holds every
    /// built-in code already.
    #[error("declared error code '{code}' is the name of a built-in code")]
    BuiltInName { code: BuiltInTaskCode },
    /// The entry is the same text as an earlier accepted entry.
    #[error("declared error code '{code}' is given more than once")]
    Repeated { code: String },
}

impl DeclaredCodeRefusal {
    /// The HRS code of the refusal: `HRS-212` for the name of a built-in or
    /// a reserved code (as for the codes of an exception mapper), `HRS-112`
    /// for another text that fails the code-text rule, `HRS-113` for a
    /// repeated code.
    pub fn error_code(&self) -> HorsiesErrorCode {
        match self {
            Self::Text {
                refusal: CodeTextRefusal::ReservedName,
                ..
            }
            | Self::BuiltInName { .. } => HorsiesErrorCode::CheckReservedCodeCollision,
            Self::Text {
                refusal:
                    CodeTextRefusal::Empty
                    | CodeTextRefusal::ExceptionClassName
                    | CodeTextRefusal::NotUpperSnakeCase,
                ..
            } => HorsiesErrorCode::TaskInvalidErrorCode,
            Self::Repeated { .. } => HorsiesErrorCode::TaskDuplicateErrorCode,
        }
    }
}

/// Every refused entry of one declared set, in the order of the input. It
/// holds at least one refusal: only [`DeclaredErrorCodes::parse`] builds it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the declared error codes are refused: {}", joined(.refusals))]
pub struct DeclaredCodesRefused {
    refusals: Vec<DeclaredCodeRefusal>,
}

impl DeclaredCodesRefused {
    /// The refusals, in the order of the input.
    pub fn refusals(&self) -> &[DeclaredCodeRefusal] {
        &self.refusals
    }
}

fn joined(refusals: &[DeclaredCodeRefusal]) -> String {
    refusals
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<String>>()
        .join("; ")
}

impl DeclaredErrorCodes {
    /// The set of a task that declares no code.
    pub const NONE: Self = Self(Vec::new());

    /// The declared set of `codes`. Pure.
    ///
    /// Each entry is checked in this order, and the first rule that refuses
    /// it gives its one refusal: the code-text rule, then the name of a
    /// built-in code, then the text of an earlier accepted entry. An empty
    /// input gives [`DeclaredErrorCodes::NONE`].
    ///
    /// Errors: [`DeclaredCodesRefused`] with one refusal per refused entry,
    /// in the order of `codes`.
    pub fn parse<I, S>(codes: I) -> Result<Self, DeclaredCodesRefused>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut accepted: BTreeSet<String> = BTreeSet::new();
        let mut refusals: Vec<DeclaredCodeRefusal> = Vec::new();
        for code in codes {
            let text = code.as_ref();
            match refusal_of(text, &accepted) {
                Some(refusal) => refusals.push(refusal),
                None => {
                    let _first = accepted.insert(text.to_owned());
                }
            }
        }
        match refusals.is_empty() {
            true => Ok(Self(accepted.into_iter().map(DeclaredErrorCode).collect())),
            false => Err(DeclaredCodesRefused { refusals }),
        }
    }

    /// The declared code whose text is `text`, if the set has one. A binary
    /// search over the sorted codes.
    pub fn get(&self, text: &str) -> Option<&DeclaredErrorCode> {
        self.0
            .binary_search_by(|code| code.0.as_str().cmp(text))
            .ok()
            .and_then(|index| self.0.get(index))
    }

    /// The codes, sorted by text.
    pub fn iter(&self) -> impl Iterator<Item = &DeclaredErrorCode> {
        self.0.iter()
    }

    /// The number of declared codes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the task declares no code.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// The refusal of the entry `text`, given the entries accepted before it.
fn refusal_of(text: &str, accepted: &BTreeSet<String>) -> Option<DeclaredCodeRefusal> {
    match (
        check_error_code_text(text),
        BuiltInTaskCode::parse(text),
        accepted.contains(text),
    ) {
        (Err(refusal), _, _) => Some(DeclaredCodeRefusal::Text {
            code: text.to_owned(),
            refusal,
        }),
        (Ok(()), Some(code), _) => Some(DeclaredCodeRefusal::BuiltInName { code }),
        (Ok(()), None, true) => Some(DeclaredCodeRefusal::Repeated {
            code: text.to_owned(),
        }),
        (Ok(()), None, false) => None,
    }
}

impl TryFrom<Vec<String>> for DeclaredErrorCodes {
    type Error = DeclaredCodesRefused;

    fn try_from(codes: Vec<String>) -> Result<Self, Self::Error> {
        Self::parse(codes)
    }
}

impl From<DeclaredErrorCodes> for Vec<String> {
    fn from(codes: DeclaredErrorCodes) -> Self {
        codes.0.into_iter().map(|code| code.0).collect()
    }
}

/// The `error_code` label of one error code of one task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCodeLabel<'a> {
    /// A built-in code; the label value is its value, for example
    /// `DATABASE_ERROR`.
    BuiltIn(&'a BuiltInTaskCode),
    /// A user code that the task declares; the label value is the code.
    Declared(&'a DeclaredErrorCode),
    /// A user code that the task does not declare; the label value is
    /// [`UNDECLARED_LABEL`].
    Undeclared,
}

impl<'a> ErrorCodeLabel<'a> {
    /// The label value.
    pub fn value(&self) -> &'a str {
        match *self {
            Self::BuiltIn(code) => built_in_value(code),
            Self::Declared(code) => code.as_str(),
            Self::Undeclared => UNDECLARED_LABEL,
        }
    }
}

/// The `error_code` label of `code` for a task that declares `declared`.
/// Pure and total: a built-in code is its own label, a user code in
/// `declared` is its own label, and every other user code is
/// [`ErrorCodeLabel::Undeclared`]. A task that is not registered declares
/// no code: pass [`DeclaredErrorCodes::NONE`].
pub fn error_code_label<'a>(
    declared: &'a DeclaredErrorCodes,
    code: &'a TaskErrorCode,
) -> ErrorCodeLabel<'a> {
    match code {
        TaskErrorCode::BuiltIn(built_in) => ErrorCodeLabel::BuiltIn(built_in),
        TaskErrorCode::User(user) => match declared.get(user.as_str()) {
            Some(found) => ErrorCodeLabel::Declared(found),
            None => ErrorCodeLabel::Undeclared,
        },
    }
}

/// The value of a built-in code, from the catalog of its family.
fn built_in_value(code: &BuiltInTaskCode) -> &'static str {
    match code {
        BuiltInTaskCode::Operational(code) => code.wire_value(),
        BuiltInTaskCode::Contract(code) => code.wire_value(),
        BuiltInTaskCode::Retrieval(code) => code.wire_value(),
        BuiltInTaskCode::Outcome(code) => code.wire_value(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::{ContractCode, OperationalCode, OutcomeCode, RetrievalCode, UserCode};
    use super::*;

    fn declared(codes: &[&str]) -> DeclaredErrorCodes {
        DeclaredErrorCodes::parse(codes).expect("the test set is a valid declared set")
    }

    fn refusals(codes: &[&str]) -> Vec<DeclaredCodeRefusal> {
        DeclaredErrorCodes::parse(codes)
            .expect_err("the test set is refused")
            .refusals()
            .to_vec()
    }

    fn texts(codes: &DeclaredErrorCodes) -> Vec<&str> {
        codes.iter().map(DeclaredErrorCode::as_str).collect()
    }

    fn every_built_in_code() -> Vec<BuiltInTaskCode> {
        OperationalCode::ALL
            .into_iter()
            .map(BuiltInTaskCode::from)
            .chain(ContractCode::ALL.into_iter().map(BuiltInTaskCode::from))
            .chain(RetrievalCode::ALL.into_iter().map(BuiltInTaskCode::from))
            .chain(OutcomeCode::ALL.into_iter().map(BuiltInTaskCode::from))
            .collect()
    }

    fn user(text: &str) -> TaskErrorCode {
        TaskErrorCode::User(UserCode::parse(text).expect("the test text is a user code"))
    }

    #[test]
    fn a_valid_set_is_kept_sorted_by_text() {
        let codes = declared(&["RATE_LIMITED", "CARD_DECLINED", "HTTP2_FAILED"]);
        assert_eq!(
            texts(&codes),
            ["CARD_DECLINED", "HTTP2_FAILED", "RATE_LIMITED"]
        );
        assert_eq!(codes.len(), 3);
        assert!(!codes.is_empty());
    }

    #[test]
    fn no_entry_is_the_empty_set() {
        let codes = declared(&[]);
        assert_eq!(codes, DeclaredErrorCodes::NONE);
        assert!(codes.is_empty());
        assert_eq!(DeclaredErrorCodes::default(), DeclaredErrorCodes::NONE);
    }

    #[test]
    fn a_text_that_fails_the_code_text_rule_is_refused_with_hrs_112() {
        let cases = [
            ("", CodeTextRefusal::Empty),
            ("RateLimitError", CodeTextRefusal::ExceptionClassName),
            ("rate_limited", CodeTextRefusal::NotUpperSnakeCase),
            ("_RATE", CodeTextRefusal::NotUpperSnakeCase),
        ];
        for (code, refusal) in cases {
            let found = refusals(&[code]);
            assert_eq!(
                found,
                [DeclaredCodeRefusal::Text {
                    code: code.to_owned(),
                    refusal
                }],
                "{code:?}"
            );
            assert_eq!(found[0].error_code(), HorsiesErrorCode::TaskInvalidErrorCode);
            assert_eq!(found[0].error_code().as_code_str(), "HRS-112");
        }
    }

    #[test]
    fn a_reserved_name_is_refused_with_hrs_212() {
        let found = refusals(&["WORKFLOW_STOPPED"]);
        assert_eq!(
            found,
            [DeclaredCodeRefusal::Text {
                code: "WORKFLOW_STOPPED".to_owned(),
                refusal: CodeTextRefusal::ReservedName,
            }]
        );
        assert_eq!(found[0].error_code(), HorsiesErrorCode::CheckReservedCodeCollision);
    }

    #[test]
    fn every_built_in_name_is_refused_with_hrs_212() {
        for built_in in every_built_in_code() {
            let value = built_in_value(&built_in);
            let found = refusals(&[value]);
            assert_eq!(
                found,
                [DeclaredCodeRefusal::BuiltInName {
                    code: built_in.clone()
                }],
                "{value}"
            );
            assert_eq!(found[0].error_code(), HorsiesErrorCode::CheckReservedCodeCollision);
            assert_eq!(found[0].error_code().as_code_str(), "HRS-212");
        }
    }

    #[test]
    fn a_repeated_code_is_refused_with_hrs_113_once_per_repeat() {
        let found = refusals(&[
            "RATE_LIMITED",
            "CARD_DECLINED",
            "RATE_LIMITED",
            "RATE_LIMITED",
        ]);
        let repeat = DeclaredCodeRefusal::Repeated {
            code: "RATE_LIMITED".to_owned(),
        };
        assert_eq!(found, [repeat.clone(), repeat]);
        assert_eq!(found[0].error_code(), HorsiesErrorCode::TaskDuplicateErrorCode);
        assert_eq!(found[0].error_code().as_code_str(), "HRS-113");
    }

    /// Each refused entry gets one refusal, from the first rule that refuses
    /// it; a refused entry is not an earlier entry of a repeat.
    #[test]
    fn every_refused_entry_is_named_in_input_order() {
        let found = refusals(&["bad", "DATABASE_ERROR", "bad", "GOOD", "GOOD"]);
        assert_eq!(
            found,
            [
                DeclaredCodeRefusal::Text {
                    code: "bad".to_owned(),
                    refusal: CodeTextRefusal::NotUpperSnakeCase,
                },
                DeclaredCodeRefusal::BuiltInName {
                    code: BuiltInTaskCode::from(OperationalCode::DatabaseError),
                },
                DeclaredCodeRefusal::Text {
                    code: "bad".to_owned(),
                    refusal: CodeTextRefusal::NotUpperSnakeCase,
                },
                DeclaredCodeRefusal::Repeated {
                    code: "GOOD".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn a_refusal_names_the_code_and_the_rule() {
        let refused = DeclaredErrorCodes::parse(["rate", "DATABASE_ERROR", "A", "A"])
            .expect_err("the set is refused");
        assert_eq!(
            refused.to_string(),
            "the declared error codes are refused: declared error code 'rate' is invalid; \
             expected UPPER_SNAKE_CASE (e.g. RATE_LIMITED); declared error code 'DATABASE_ERROR' \
             is the name of a built-in code; declared error code 'A' is given more than once"
        );
    }

    #[test]
    fn the_json_form_is_an_array_of_texts() {
        let codes: DeclaredErrorCodes =
            serde_json::from_str(r#"["RATE_LIMITED","CARD_DECLINED"]"#).expect("a valid set");
        assert_eq!(texts(&codes), ["CARD_DECLINED", "RATE_LIMITED"]);
        assert_eq!(
            serde_json::to_string(&codes).expect("a set encodes"),
            r#"["CARD_DECLINED","RATE_LIMITED"]"#
        );
        assert_eq!(
            serde_json::from_str::<DeclaredErrorCodes>("[]").expect("an empty set"),
            DeclaredErrorCodes::NONE
        );
    }

    #[test]
    fn a_json_read_refuses_what_parse_refuses() {
        for text in [
            r#"["RATE_LIMITED","RATE_LIMITED"]"#,
            r#"["rate_limited"]"#,
            r#"["TASK_TIMEOUT"]"#,
            r#"["WORKFLOW_STOPPED"]"#,
            r#"[""]"#,
            r#"[1]"#,
            r#""RATE_LIMITED""#,
        ] {
            assert!(
                serde_json::from_str::<DeclaredErrorCodes>(text).is_err(),
                "{text}"
            );
        }
    }

    #[test]
    fn a_built_in_code_is_its_own_label() {
        let codes = declared(&["RATE_LIMITED"]);
        for built_in in every_built_in_code() {
            let code = TaskErrorCode::BuiltIn(built_in.clone());
            let label = error_code_label(&codes, &code);
            assert_eq!(label, ErrorCodeLabel::BuiltIn(&built_in));
            assert_eq!(label.value(), built_in.value());
        }
    }

    #[test]
    fn a_declared_user_code_is_its_own_label() {
        let codes = declared(&["RATE_LIMITED", "CARD_DECLINED"]);
        let code = user("CARD_DECLINED");
        let label = error_code_label(&codes, &code);
        assert_eq!(label.value(), "CARD_DECLINED");
        assert!(
            matches!(label, ErrorCodeLabel::Declared(found) if found.as_str() == "CARD_DECLINED")
        );
    }

    #[test]
    fn an_undeclared_user_code_is_undeclared() {
        let codes = declared(&["RATE_LIMITED"]);
        for text in ["CARD_DECLINED", "my_user_code", "RATE_LIMITED_2"] {
            let code = user(text);
            let label = error_code_label(&codes, &code);
            assert_eq!(label, ErrorCodeLabel::Undeclared, "{text}");
            assert_eq!(label.value(), UNDECLARED_LABEL);
        }
    }

    #[test]
    fn a_task_that_declares_nothing_labels_every_user_code_undeclared() {
        let code = user("RATE_LIMITED");
        assert_eq!(
            error_code_label(&DeclaredErrorCodes::NONE, &code),
            ErrorCodeLabel::Undeclared
        );
    }

    /// The fallback value is outside every other value of the set.
    #[test]
    fn the_undeclared_value_is_never_a_code_value() {
        assert!(check_error_code_text(UNDECLARED_LABEL).is_err());
        assert!(DeclaredErrorCodes::parse([UNDECLARED_LABEL]).is_err());
        for built_in in every_built_in_code() {
            assert_ne!(built_in_value(&built_in), UNDECLARED_LABEL);
        }
    }

    #[test]
    fn the_label_value_of_a_built_in_code_is_its_catalog_value() {
        for built_in in every_built_in_code() {
            assert_eq!(built_in_value(&built_in), built_in.value());
        }
    }
}

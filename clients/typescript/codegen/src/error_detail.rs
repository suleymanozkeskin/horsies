//! `error_detail.ts`: the cause union of the `exception` field of a catalog
//! error, and `NoCause`.
//!
//! The union `CoreCause` is told apart by `kind`:
//! - `coreFailure`: a core operation failed (`ExceptionValue::CoreFailure`).
//!   `failure` names its `FailureCause`: `server`, `connection`,
//!   `contractViolation` or `preconditionFailed`. A fact of one cause is
//!   only in the interface of that cause: the SQLSTATE only on `server`. A
//!   contract violation never clears, so it has no `retryable`, as the
//!   Python client's `CoreContractViolation`.
//! - One kind per exception class of the core (`EXCEPTION_CLASSES`): the
//!   class name without `Error`, in camelCase (`outcomeDecode`), with its
//!   `message`.
//! - One kind per exception class that a client defines
//!   (`ClientExceptionClass`): `ruleRefused` and `jsonUnreadable`, with
//!   their `message`.
//!
//! `NoCause` (`kind: "noCause"`) is not a member of `CoreCause`: only a slot
//! of the type `ExceptionOrNoCause` holds it.

use horsies_core::error_catalog::{ClientExceptionClass, ExceptionClass, EXCEPTION_CLASSES};

use crate::model::exception_class_type_name;
use crate::output::{catalog_file, GeneratedFile};
use crate::ts_text::{camel_case_of_pascal, module_text, string_literal, union_alias};

/// The stem of the module, for a renderer that imports from it.
pub const ERROR_DETAIL_MODULE: &str = "error_detail";

/// The union of the causes of a failed core operation.
pub const CORE_CAUSE_TYPE: &str = "CoreCause";

/// The value that says that no exception caused the failure.
pub const NO_CAUSE_TYPE: &str = "NoCause";

/// The union of the core failures, one interface per `FailureCause`.
const CORE_FAILURE_TYPE: &str = "CoreFailure";

/// The kind of a core failure.
const CORE_FAILURE_KIND: &str = "coreFailure";

/// The kind of [`NO_CAUSE_TYPE`].
const NO_CAUSE_KIND: &str = "noCause";

/// The suffix of the name of an exception class of the core. The kind of
/// its cause is the name without it.
const EXCEPTION_CLASS_SUFFIX: &str = "Error";

/// The exception classes that a client defines, in render order.
const CLIENT_EXCEPTION_CLASSES: [ClientExceptionClass; 2] = [
    ClientExceptionClass::ValueError,
    ClientExceptionClass::SerializationError,
];

const CORE_FAILURE_COMMENT: &str =
    "/** A core operation failed. `subject` names the operation and `failure` names the cause. */";

const CORE_CAUSE_COMMENT: &str =
    "/** The cause of a failed core operation: the `exception` field of a catalog error. */";

const NO_CAUSE_COMMENT: &str =
    "/** No exception caused the failure: a check of the core found it. */";

/// One member of an exported interface: `readonly <name>: <type_text>;`.
/// `name` is the property key as the source text has it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterfaceMember {
    pub name: String,
    pub type_text: String,
}

impl InterfaceMember {
    /// The member `name` of the type `type_text`. Pure.
    pub fn new(name: impl Into<String>, type_text: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            type_text: type_text.into(),
        }
    }
}

/// `export interface <name> { ... }` with one readonly member per line.
/// Pure.
///
/// An interface without members is a defect of the caller: the lint rule
/// `no-empty-object-type` refuses it.
pub fn interface_text(name: &str, members: &[InterfaceMember]) -> String {
    assert!(!members.is_empty(), "the interface {name} has no member");
    let mut text = format!("export interface {name} {{\n");
    for member in members {
        text.push_str(&format!(
            "  readonly {}: {};\n",
            member.name, member.type_text
        ));
    }
    text.push('}');
    text
}

/// `error_detail.ts`. Pure.
pub fn generated_files() -> Vec<GeneratedFile> {
    vec![catalog_file(
        ERROR_DETAIL_MODULE,
        module_text(&error_detail_sections(EXCEPTION_CLASSES)),
    )]
}

/// A member of a cause interface with a fixed name and type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CauseField {
    name: &'static str,
    type_text: &'static str,
}

/// The core operation that failed.
const SUBJECT: CauseField = CauseField {
    name: "subject",
    type_text: "string",
};

/// What failed and why; for a server error, the server's primary message.
const MESSAGE: CauseField = CauseField {
    name: "message",
    type_text: "string",
};

/// Whether the cause may clear, so a repeat of the operation can succeed.
const RETRYABLE: CauseField = CauseField {
    name: "retryable",
    type_text: "boolean",
};

/// The five-character SQLSTATE of a server error.
const SQLSTATE: CauseField = CauseField {
    name: "sqlstate",
    type_text: "string",
};

/// The cause of a core failure, one per `FailureCause` variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CoreFailureKind {
    Server,
    Connection,
    ContractViolation,
    PreconditionFailed,
}

impl CoreFailureKind {
    /// Every kind, in render order.
    const ALL: [Self; 4] = [
        Self::Server,
        Self::Connection,
        Self::ContractViolation,
        Self::PreconditionFailed,
    ];

    /// The interface name: the Python client's class name.
    fn type_name(self) -> &'static str {
        match self {
            Self::Server => "CoreServerError",
            Self::Connection => "CoreConnectionError",
            Self::ContractViolation => "CoreContractViolation",
            Self::PreconditionFailed => "CorePreconditionFailed",
        }
    }

    /// The value of the `failure` member.
    fn failure(self) -> &'static str {
        match self {
            Self::Server => "server",
            Self::Connection => "connection",
            Self::ContractViolation => "contractViolation",
            Self::PreconditionFailed => "preconditionFailed",
        }
    }

    /// The members after `kind` and `failure`.
    fn fields(self) -> &'static [CauseField] {
        match self {
            Self::Server => &[SUBJECT, MESSAGE, RETRYABLE, SQLSTATE],
            Self::Connection | Self::PreconditionFailed => &[SUBJECT, MESSAGE, RETRYABLE],
            Self::ContractViolation => &[SUBJECT, MESSAGE],
        }
    }
}

/// The interface name and the kind of a cause that holds a message.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MessageCause {
    type_name: &'static str,
    kind: String,
}

/// The cause of an exception class of the core: its class name, and the
/// name without [`EXCEPTION_CLASS_SUFFIX`] in camelCase as its kind. Pure.
///
/// A class name without the suffix is a defect of the model: its kind
/// would have no rule.
fn generated_cause(class: &ExceptionClass) -> MessageCause {
    let type_name = exception_class_type_name(class);
    let stem = type_name
        .strip_suffix(EXCEPTION_CLASS_SUFFIX)
        .unwrap_or_else(|| {
            panic!(
                "the exception class {type_name} does not end in {EXCEPTION_CLASS_SUFFIX}, so its \
                 cause kind has no name"
            )
        });
    MessageCause {
        type_name,
        kind: camel_case_of_pascal(stem),
    }
}

/// The cause of an exception class that a client defines. Pure.
fn client_cause(class: ClientExceptionClass) -> MessageCause {
    let (type_name, kind) = match class {
        ClientExceptionClass::ValueError => ("RuleRefused", "ruleRefused"),
        ClientExceptionClass::SerializationError => ("JsonUnreadable", "jsonUnreadable"),
    };
    MessageCause {
        type_name,
        kind: kind.to_owned(),
    }
}

/// The sections of the module: the core failures and their union, the
/// causes with a message, `CoreCause`, then `NoCause`. Pure.
fn error_detail_sections(exception_classes: &[ExceptionClass]) -> Vec<String> {
    let mut sections: Vec<String> = CoreFailureKind::ALL
        .iter()
        .map(|kind| core_failure_interface(*kind))
        .collect();
    let failure_names: Vec<String> = CoreFailureKind::ALL
        .iter()
        .map(|kind| kind.type_name().to_owned())
        .collect();
    sections.push(format!(
        "{CORE_FAILURE_COMMENT}\n{}",
        union_alias(CORE_FAILURE_TYPE, &failure_names)
    ));
    let message_causes: Vec<MessageCause> = exception_classes
        .iter()
        .map(generated_cause)
        .chain(
            CLIENT_EXCEPTION_CLASSES
                .iter()
                .map(|class| client_cause(*class)),
        )
        .collect();
    sections.extend(message_causes.iter().map(message_cause_interface));
    let mut cause_names = vec![CORE_FAILURE_TYPE.to_owned()];
    cause_names.extend(
        message_causes
            .iter()
            .map(|cause| cause.type_name.to_owned()),
    );
    sections.push(format!(
        "{CORE_CAUSE_COMMENT}\n{}",
        union_alias(CORE_CAUSE_TYPE, &cause_names)
    ));
    sections.push(format!(
        "{NO_CAUSE_COMMENT}\n{}",
        interface_text(
            NO_CAUSE_TYPE,
            &[InterfaceMember::new("kind", string_literal(NO_CAUSE_KIND))]
        )
    ));
    sections
}

/// The interface of one core failure: `kind`, `failure`, then its fields.
/// Pure.
fn core_failure_interface(kind: CoreFailureKind) -> String {
    let mut members = vec![
        InterfaceMember::new("kind", string_literal(CORE_FAILURE_KIND)),
        InterfaceMember::new("failure", string_literal(kind.failure())),
    ];
    members.extend(
        kind.fields()
            .iter()
            .map(|field| InterfaceMember::new(field.name, field.type_text)),
    );
    interface_text(kind.type_name(), &members)
}

/// The interface of a cause with a message: `kind` and `message`. Pure.
fn message_cause_interface(cause: &MessageCause) -> String {
    interface_text(
        cause.type_name,
        &[
            InterfaceMember::new("kind", string_literal(&cause.kind)),
            InterfaceMember::new(MESSAGE.name, MESSAGE.type_text),
        ],
    )
}

#[cfg(test)]
mod tests {
    use horsies_core::error_catalog::{
        ExceptionValue, HISTORY_CONTRACT_ERROR, OUTCOME_DECODE_ERROR,
    };
    use horsies_core::{FailureCause, ServerErrorFacts};

    use super::*;
    use crate::ts_text::GENERATED_HEADER;

    /// A new client class of the core fails this match: list it in
    /// `CLIENT_EXCEPTION_CLASSES` and add its arm here.
    #[test]
    fn the_client_class_list_holds_every_client_class() {
        let every_class = [
            ClientExceptionClass::ValueError,
            ClientExceptionClass::SerializationError,
        ];
        for class in every_class {
            match class {
                ClientExceptionClass::ValueError | ClientExceptionClass::SerializationError => {
                    assert!(CLIENT_EXCEPTION_CLASSES.contains(&class), "{class:?}");
                }
            }
        }
        assert_eq!(CLIENT_EXCEPTION_CLASSES.len(), every_class.len());
    }

    const PROBE_ERROR: ExceptionClass = ExceptionClass {
        python_module: "probe_errors",
        python_class: "ProbeReadError",
        doc: "",
    };

    /// The kind of a `FailureCause`. The match has one arm per variant, so
    /// a new variant fails the test build here.
    fn kind_of(cause: &FailureCause) -> CoreFailureKind {
        match cause {
            FailureCause::Server(_) => CoreFailureKind::Server,
            FailureCause::Connection => CoreFailureKind::Connection,
            FailureCause::ContractViolation => CoreFailureKind::ContractViolation,
            FailureCause::PreconditionFailed => CoreFailureKind::PreconditionFailed,
        }
    }

    /// The kind of the cause that a client builds from `value`. The match
    /// has one arm per variant, so a new variant fails the test build here.
    fn kind_of_value(value: &ExceptionValue) -> String {
        match value {
            ExceptionValue::CoreFailure { .. } => CORE_FAILURE_KIND.to_owned(),
            ExceptionValue::Generated { class, .. } => generated_cause(class).kind,
            ExceptionValue::ClientClass { class, .. } => client_cause(*class).kind,
            ExceptionValue::NoCause => NO_CAUSE_KIND.to_owned(),
        }
    }

    #[test]
    fn the_module_has_the_core_failures_the_message_causes_and_no_cause() {
        assert_eq!(
            module_text(&error_detail_sections(&[PROBE_ERROR])),
            format!(
                "{GENERATED_HEADER}\n\
                 \n\
                 export interface CoreServerError {{\n  \
                 readonly kind: \"coreFailure\";\n  \
                 readonly failure: \"server\";\n  \
                 readonly subject: string;\n  \
                 readonly message: string;\n  \
                 readonly retryable: boolean;\n  \
                 readonly sqlstate: string;\n\
                 }}\n\
                 \n\
                 export interface CoreConnectionError {{\n  \
                 readonly kind: \"coreFailure\";\n  \
                 readonly failure: \"connection\";\n  \
                 readonly subject: string;\n  \
                 readonly message: string;\n  \
                 readonly retryable: boolean;\n\
                 }}\n\
                 \n\
                 export interface CoreContractViolation {{\n  \
                 readonly kind: \"coreFailure\";\n  \
                 readonly failure: \"contractViolation\";\n  \
                 readonly subject: string;\n  \
                 readonly message: string;\n\
                 }}\n\
                 \n\
                 export interface CorePreconditionFailed {{\n  \
                 readonly kind: \"coreFailure\";\n  \
                 readonly failure: \"preconditionFailed\";\n  \
                 readonly subject: string;\n  \
                 readonly message: string;\n  \
                 readonly retryable: boolean;\n\
                 }}\n\
                 \n\
                 {CORE_FAILURE_COMMENT}\n\
                 export type CoreFailure =\n  \
                 | CoreServerError\n  \
                 | CoreConnectionError\n  \
                 | CoreContractViolation\n  \
                 | CorePreconditionFailed;\n\
                 \n\
                 export interface ProbeReadError {{\n  \
                 readonly kind: \"probeRead\";\n  \
                 readonly message: string;\n\
                 }}\n\
                 \n\
                 export interface RuleRefused {{\n  \
                 readonly kind: \"ruleRefused\";\n  \
                 readonly message: string;\n\
                 }}\n\
                 \n\
                 export interface JsonUnreadable {{\n  \
                 readonly kind: \"jsonUnreadable\";\n  \
                 readonly message: string;\n\
                 }}\n\
                 \n\
                 {CORE_CAUSE_COMMENT}\n\
                 export type CoreCause =\n  \
                 | CoreFailure\n  \
                 | ProbeReadError\n  \
                 | RuleRefused\n  \
                 | JsonUnreadable;\n\
                 \n\
                 {NO_CAUSE_COMMENT}\n\
                 export interface NoCause {{\n  \
                 readonly kind: \"noCause\";\n\
                 }}\n"
            )
        );
    }

    #[test]
    fn an_interface_has_one_readonly_member_per_line() {
        assert_eq!(
            interface_text(
                "Sample",
                &[
                    InterfaceMember::new("code", "\"X\""),
                    InterfaceMember::new("taskId", "string"),
                ]
            ),
            "export interface Sample {\n  readonly code: \"X\";\n  readonly taskId: string;\n}"
        );
    }

    #[test]
    #[should_panic(expected = "the interface Empty has no member")]
    fn an_interface_without_members_is_a_defect() {
        let _text = interface_text("Empty", &[]);
    }

    #[test]
    fn the_kind_of_a_core_exception_class_is_its_name_without_error() {
        assert_eq!(
            generated_cause(&OUTCOME_DECODE_ERROR),
            MessageCause {
                type_name: "OutcomeDecodeError",
                kind: "outcomeDecode".to_owned(),
            }
        );
        assert_eq!(
            generated_cause(&HISTORY_CONTRACT_ERROR).kind,
            "historyContract"
        );
    }

    #[test]
    #[should_panic(expected = "the exception class ProbeFailure does not end in Error")]
    fn a_core_exception_class_without_the_suffix_is_a_defect() {
        let _cause = generated_cause(&ExceptionClass {
            python_module: "probe_errors",
            python_class: "ProbeFailure",
            doc: "",
        });
    }

    /// The four kinds are the four `FailureCause` variants, in the order of
    /// the variants.
    #[test]
    fn each_failure_cause_has_its_kind_in_render_order() {
        let causes = [
            FailureCause::Server(ServerErrorFacts::of_code(
                "57014",
                "canceling statement due to statement timeout".to_owned(),
            )),
            FailureCause::Connection,
            FailureCause::ContractViolation,
            FailureCause::PreconditionFailed,
        ];
        let kinds: Vec<CoreFailureKind> = causes.iter().map(kind_of).collect();
        assert_eq!(kinds, CoreFailureKind::ALL);
    }

    /// Each exception value that a client builds has its kind in the
    /// module, and `NoCause` is not a member of `CoreCause`.
    #[test]
    fn each_exception_value_has_a_kind_of_the_module() {
        let files = generated_files();
        let [file] = files.as_slice() else {
            panic!("one file, got {}", files.len());
        };
        let mut values = vec![
            ExceptionValue::CoreFailure {
                text: "connection refused".to_owned(),
                retryable: true,
                cause: FailureCause::Connection,
            },
            ExceptionValue::NoCause,
        ];
        values.extend(
            EXCEPTION_CLASSES
                .iter()
                .map(|class| ExceptionValue::Generated {
                    class: *class,
                    text: "row".to_owned(),
                }),
        );
        values.extend(
            CLIENT_EXCEPTION_CLASSES
                .iter()
                .map(|class| ExceptionValue::ClientClass {
                    class: *class,
                    text: "rule".to_owned(),
                }),
        );
        for value in &values {
            let kind = format!("  readonly kind: \"{}\";\n", kind_of_value(value));
            assert!(file.text().contains(&kind), "{value:?}");
        }
        assert!(file.text().contains(
            "export type CoreCause =\n  | CoreFailure\n  | OutcomeDecodeError\n  \
             | HistoryContractError\n  | RuleRefused\n  | JsonUnreadable;\n"
        ));
        assert_eq!(
            file.relative_path().display().to_string(),
            "src/_catalog/error_detail.ts"
        );
    }

    /// The SQLSTATE is only on the server failure; the contract violation
    /// has no `retryable`.
    #[test]
    fn a_fact_of_one_cause_is_only_in_its_interface() {
        let sqlstate: Vec<CoreFailureKind> = CoreFailureKind::ALL
            .into_iter()
            .filter(|kind| kind.fields().contains(&SQLSTATE))
            .collect();
        assert_eq!(sqlstate, [CoreFailureKind::Server]);
        let not_retryable: Vec<CoreFailureKind> = CoreFailureKind::ALL
            .into_iter()
            .filter(|kind| !kind.fields().contains(&RETRYABLE))
            .collect();
        assert_eq!(not_retryable, [CoreFailureKind::ContractViolation]);
    }
}

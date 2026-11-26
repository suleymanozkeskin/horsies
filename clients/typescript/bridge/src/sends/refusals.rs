//! The refusal record of a core check: the code, the message, the notes and
//! the help lines.
//!
//! A check of `Horsies.create` gives every refusal as a value. The code is a
//! catalog code (`HRS-nnn`), or none for a field bound that has no catalog
//! code (the Python client refuses such a value through a field constraint).

use horsies_core::{ErrorCode, HorsiesError};
use napi_derive::napi;

/// The code of a refusal of a core check.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CoreRefusalCode {
    /// A code of the core's catalog, as its text (`HRS-102`).
    Catalog { code: String },
    /// A field bound with no catalog code.
    NoCode,
}

/// One refusal of a core check.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoreRefusal {
    pub code: CoreRefusalCode,
    pub message: String,
    pub notes: Vec<String>,
    /// Zero or one line: how to fix the value.
    pub help: Vec<String>,
}

impl CoreRefusal {
    /// The refusal of a core error, with its code, notes and help. Pure.
    pub fn of_error(error: &HorsiesError) -> Self {
        Self {
            code: match &error.code {
                Some(code) => CoreRefusalCode::Catalog {
                    code: code.as_code_str().to_owned(),
                },
                None => CoreRefusalCode::NoCode,
            },
            message: error.message.clone(),
            notes: error.notes.clone(),
            help: error.help_text.iter().cloned().collect(),
        }
    }

    /// A refusal with the catalog code `code`, its notes and its help
    /// lines. Pure.
    pub fn coded(code: ErrorCode, message: String, notes: Vec<String>, help: Vec<String>) -> Self {
        Self {
            code: CoreRefusalCode::Catalog {
                code: code.as_code_str().to_owned(),
            },
            message,
            notes,
            help,
        }
    }

    /// A refusal of a field bound that has no catalog code. Pure.
    pub fn uncoded(message: String) -> Self {
        Self {
            code: CoreRefusalCode::NoCode,
            message,
            notes: Vec::new(),
            help: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_core_error_keeps_its_code_notes_and_help() {
        let error = HorsiesError::new("custom_queues must not be empty in CUSTOM mode")
            .with_code(ErrorCode::ConfigInvalidQueueMode)
            .with_note("the app is configured with QueueMode::Custom")
            .with_help("name one queue");
        assert_eq!(
            CoreRefusal::of_error(&error),
            CoreRefusal {
                code: CoreRefusalCode::Catalog {
                    code: "HRS-200".to_owned()
                },
                message: "custom_queues must not be empty in CUSTOM mode".to_owned(),
                notes: vec!["the app is configured with QueueMode::Custom".to_owned()],
                help: vec!["name one queue".to_owned()],
            }
        );
    }

    #[test]
    fn a_core_error_with_no_code_has_no_code_and_no_help() {
        let error = HorsiesError::new("payload.warn_bytes must be >= 1 (use None to disable)");
        assert_eq!(
            CoreRefusal::of_error(&error),
            CoreRefusal::uncoded(
                "payload.warn_bytes must be >= 1 (use None to disable)".to_owned()
            )
        );
    }
}

//! The refusal record of a core check: the catalog code, the message, the
//! notes and the help lines.
//!
//! A check of `Horsies.create` gives every refusal as a value. Every refusal
//! carries a code of the core's catalog (`HRS-nnn`).

use horsies_core::{AppConfigError, ErrorCode};
use napi_derive::napi;

/// One refusal of a core check.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoreRefusal {
    /// A code of the core's catalog, as its text (`HRS-102`).
    pub code: String,
    pub message: String,
    pub notes: Vec<String>,
    /// Zero or one line: how to fix the value.
    pub help: Vec<String>,
}

impl CoreRefusal {
    /// The refusal of an app config error: its catalog code
    /// (`AppConfigError::error_code`), and the message, notes and help of
    /// the core's error. Pure.
    ///
    /// Panics when the error has no catalog code: every `AppConfigError`
    /// has one, so an error with none is a defect of the core. In a bridge
    /// call, `sync_call` turns the panic into `CORE_PANIC`.
    pub fn of_config_error(error: &AppConfigError) -> Self {
        let code = error
            .error_code()
            .expect("every AppConfigError has a catalog code (AppConfigError::error_code)");
        let converted = error.to_horsies_error();
        Self::coded(
            code,
            converted.message,
            converted.notes,
            converted.help_text.into_iter().collect(),
        )
    }

    /// A refusal with the catalog code `code`, its notes and its help
    /// lines. Pure.
    pub fn coded(code: ErrorCode, message: String, notes: Vec<String>, help: Vec<String>) -> Self {
        Self {
            code: code.as_code_str().to_owned(),
            message,
            notes,
            help,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use horsies_core::HorsiesError;

    #[test]
    fn a_config_error_keeps_its_code_notes_and_help() {
        let error = AppConfigError::WorkerProfile(
            HorsiesError::new("worker_profiles['empty']: the profile starts no children")
                .with_code(ErrorCode::ConfigInvalidWorkerProfile)
                .with_note("a worker of the profile runs no task")
                .with_help("give the profile at least one child kind"),
        );
        assert_eq!(
            CoreRefusal::of_config_error(&error),
            CoreRefusal {
                code: "HRS-218".to_owned(),
                message: "worker_profiles['empty']: the profile starts no children".to_owned(),
                notes: vec!["a worker of the profile runs no task".to_owned()],
                help: vec!["give the profile at least one child kind".to_owned()],
            }
        );
    }

    #[test]
    fn a_payload_error_has_hrs_227_and_no_help() {
        let error = AppConfigError::Payload(
            "payload.warn_bytes must be >= 1 (use None to disable)".to_owned(),
        );
        assert_eq!(
            CoreRefusal::of_config_error(&error),
            CoreRefusal {
                code: "HRS-227".to_owned(),
                message: "payload.warn_bytes must be >= 1 (use None to disable)".to_owned(),
                notes: Vec::new(),
                help: Vec::new(),
            }
        );
    }
}

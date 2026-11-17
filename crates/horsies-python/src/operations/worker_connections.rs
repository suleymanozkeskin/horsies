//! The worker connection step of the live check through the core:
//! `check_handle_worker_connections`, the rule of both clients.
//!
//! The profile set comes as the JSON text of the `profiles` member of a
//! worker manifest. The answer is one `WorkerConnectionError` per refusal:
//! a profile whose worker needs more connections than the server has usable
//! slots, or a limit that was not read. No profile, or PgBouncer transaction
//! mode in the handle's manifest, connects nothing and answers none. Nothing
//! is written; no schema step runs.

use std::collections::BTreeMap;

use horsies_core::core::config::WorkerProfile;
use horsies_core::{check_handle_worker_connections, HorsiesError};
use pyo3::prelude::*;

use crate::handle::BrokerConnection;

/// One refusal of the worker connection check: the core's error as data.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerConnectionError {
    pub message: String,
    /// The core's error code, for example `HRS-219`; `None` for an error
    /// that has none.
    pub code: Option<String>,
    pub notes: Vec<String>,
    pub help_text: Option<String>,
}

/// The record of one error of the core's check. Pure.
pub fn error_record(error: &HorsiesError) -> WorkerConnectionError {
    WorkerConnectionError {
        message: error.message.clone(),
        code: error.code.map(|code| code.as_code_str().to_owned()),
        notes: error.notes.clone(),
        help_text: error.help_text.clone(),
    }
}

/// Run the core's worker connection check of `profiles` on the handle of
/// `connection`.
pub async fn check_worker_connections_operation(
    connection: BrokerConnection,
    profiles: BTreeMap<String, WorkerProfile>,
) -> Vec<WorkerConnectionError> {
    check_handle_worker_connections(&profiles, connection.lazy_handle())
        .await
        .iter()
        .map(error_record)
        .collect()
}

#[cfg(test)]
mod tests {
    use horsies_core::core::ErrorCode;

    use super::*;

    #[test]
    fn the_record_keeps_every_part_of_the_core_error() {
        let error = HorsiesError::new("the server connection limit could not be read")
            .with_code(ErrorCode::CheckWorkerConnections)
            .with_note("reading the server connection limit failed: refused")
            .with_help("check the role");

        let record = error_record(&error);

        assert_eq!(
            record,
            WorkerConnectionError {
                message: "the server connection limit could not be read".to_owned(),
                code: Some("HRS-219".to_owned()),
                notes: vec!["reading the server connection limit failed: refused".to_owned()],
                help_text: Some("check the role".to_owned()),
            }
        );
    }

    #[test]
    fn an_error_without_code_or_help_keeps_them_absent() {
        let record = error_record(&HorsiesError::new("plain"));
        assert_eq!(record.code, None);
        assert_eq!(record.help_text, None);
        assert!(record.notes.is_empty());
    }
}

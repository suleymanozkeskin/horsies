//! Diagnostics of the app's configuration, registration and checks.
//!
//! A refusal names its cause with an [`HorsiesErrorCode`] (`HRS-xxx`) and the
//! code's [`HorsiesErrorCategory`], with notes and help text, as the Python client
//! does. A [`ValidationReport`] collects the refusals of one phase. The types
//! are the core's: both clients print the same codes.

pub use horsies_core::core::error::diagnostic::{format_diagnostic, format_report_diagnostic};
pub use horsies_core::{HorsiesError, HorsiesErrorCategory, HorsiesErrorCode, ValidationReport};

use miette::{Diagnostic, SourceSpan};
use thiserror::Error;

use super::ErrorCode;

/// A miette-compatible diagnostic error for rich terminal output.
///
/// Provides Rust-compiler-style error formatting with:
/// - Error code
/// - Human-readable message
/// - Notes and help text
#[derive(Debug, Error, Diagnostic)]
#[error("{message}")]
pub struct HorsiesDiagnostic {
    pub message: String,

    #[help]
    pub help_text: Option<String>,

    /// Error code (e.g. "HRS-007").
    #[diagnostic(code)]
    pub code: Option<String>,

    /// Source code for display (optional).
    #[source_code]
    pub source_code: Option<String>,

    /// Span within source_code to highlight.
    #[label("here")]
    pub span: Option<SourceSpan>,
}

impl HorsiesDiagnostic {
    /// Create a diagnostic from a HorsiesError.
    pub fn from_horsies_error(error: &super::HorsiesError) -> Self {
        Self {
            message: error.message.clone(),
            help_text: error.help_text.clone(),
            code: error.code.map(|c| c.as_code_str().to_owned()),
            source_code: None,
            span: None,
        }
    }
}

/// Format a HorsiesError using miette's diagnostic rendering.
///
/// Returns the error formatted as a string with colors if the terminal supports it.
pub fn format_diagnostic(error: &super::HorsiesError) -> String {
    let diag = HorsiesDiagnostic::from_horsies_error(error);
    format!("{:?}", miette::Report::new(diag))
}

/// Format a ValidationReport using miette's diagnostic rendering.
pub fn format_report_diagnostic(report: &super::ValidationReport) -> String {
    let mut parts = Vec::new();
    for error in &report.errors {
        parts.push(format_diagnostic(error));
    }
    parts.push(format!(
        "error: aborting due to {} previous errors",
        report.errors.len()
    ));
    parts.join("\n")
}

impl From<ErrorCode> for String {
    fn from(code: ErrorCode) -> Self {
        code.as_code_str().to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::error::HorsiesError;

    #[test]
    fn diagnostic_from_error() {
        let err = HorsiesError::new("test error")
            .with_code(ErrorCode::WorkflowCycleDetected)
            .with_help("fix the cycle");

        let diag = HorsiesDiagnostic::from_horsies_error(&err);
        assert_eq!(diag.message, "test error");
        assert_eq!(diag.code.as_deref(), Some("HRS-007"));
        assert_eq!(diag.help_text.as_deref(), Some("fix the cycle"));
    }

    #[test]
    fn format_diagnostic_produces_output() {
        let err =
            HorsiesError::new("something went wrong").with_code(ErrorCode::ConfigInvalidQueueMode);

        let output = format_diagnostic(&err);
        assert!(!output.is_empty());
        assert!(output.contains("something went wrong"));
    }
}

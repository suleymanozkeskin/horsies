//! The Rust client's side of the shared golden vectors of the stored JSON
//! forms (`crates/horsies-core/tests/fixtures/stored_forms/`, with its
//! `README.md`).
//!
//! Each vector is a value and the exact text that Python stores for it. The
//! client encodes each value with its own types to the text, and reads the
//! text back to the value. A vector names what the client cannot express
//! (`rust_cannot_express`) or writes differently today (`rust_drift`, a
//! finding of the README); the tests check both, so a change in either client
//! fails a test.

mod task_input;

use serde::Deserialize;

/// The text of the vector file `name` of the core's vector folder.
fn vector_file(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../horsies-core/tests/fixtures/stored_forms")
        .join(name);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read the vector file {}: {error}", path.display()))
}

/// The text that the client writes today in place of Python's, and the name
/// of the finding in the README.
#[derive(Debug, Clone, Deserialize)]
struct RustDrift {
    text: String,
    finding: String,
}

/// What the client does with a vector.
#[derive(Debug, Clone)]
enum RustCase {
    /// It writes Python's text.
    Same,
    /// It writes `RustDrift::text`.
    Drift(RustDrift),
    /// It has no type for the value, for the reason given.
    CannotExpress(String),
}

impl RustCase {
    /// The case of a vector row from its two optional notes. A row with both
    /// is a defect of the file.
    fn of(name: &str, cannot_express: Option<String>, drift: Option<RustDrift>) -> Self {
        match (cannot_express, drift) {
            (None, None) => Self::Same,
            (None, Some(drift)) => {
                assert!(
                    !drift.finding.is_empty(),
                    "{name}: a drift names its finding"
                );
                Self::Drift(drift)
            }
            (Some(reason), None) => {
                assert!(
                    !reason.is_empty(),
                    "{name}: a vector that Rust cannot express names why"
                );
                Self::CannotExpress(reason)
            }
            (Some(_), Some(_)) => panic!("{name}: a vector is either a drift or not expressible"),
        }
    }

    /// The text that the client writes for a vector whose Python text is
    /// `python`; `None` when it cannot express the value.
    fn expected_text<'a>(&'a self, python: &'a str) -> Option<&'a str> {
        match self {
            Self::Same => Some(python),
            Self::Drift(drift) => Some(drift.text.as_str()),
            Self::CannotExpress(_) => None,
        }
    }
}

/// The text of a value in the vectors' notation: decimal text for an
/// integer, the text of a float, ISO 8601 text, UUID text.
fn notation_text<'a>(name: &str, notation: &'a serde_json::Value) -> &'a str {
    notation
        .as_str()
        .unwrap_or_else(|| panic!("{name}: expected the value as text, got {notation}"))
}

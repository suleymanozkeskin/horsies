//! The core's side of the shared golden vectors of the stored JSON forms
//! (`tests/fixtures/stored_forms/`, with its `README.md`).
//!
//! Each vector is a value and the exact text that Python stores for it. The
//! core encodes each value that it writes to the text, and reads each text to
//! the value. The Python client and the Rust client test the same files.

mod task_input;

use crate::core::codec::StrictJsonError;

/// The text of the vector file `name` of the vector folder.
fn vector_file(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/stored_forms")
        .join(name);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read the vector file {}: {error}", path.display()))
}

/// The variant name of a refusal of the strict read rule, as a vector file
/// names it.
fn refusal_name(refusal: &StrictJsonError) -> &'static str {
    match refusal {
        StrictJsonError::Empty => "Empty",
        StrictJsonError::NotUtf8 { .. } => "NotUtf8",
        StrictJsonError::Syntax { .. } => "Syntax",
        StrictJsonError::NonStandardConstant { .. } => "NonStandardConstant",
        StrictJsonError::IntegerTooLong { .. } => "IntegerTooLong",
        StrictJsonError::NonFiniteFloat { .. } => "NonFiniteFloat",
        StrictJsonError::LoneSurrogate { .. } => "LoneSurrogate",
        StrictJsonError::TooDeep { .. } => "TooDeep",
        StrictJsonError::EngineKey { .. } => "EngineKey",
        StrictJsonError::DuplicateKey { .. } => "DuplicateKey",
    }
}

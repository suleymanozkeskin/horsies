//! The metrics of horsies, and the trace carrier of a send.
//!
//! - [`catalog`]: every metric once, as data: name, kind, unit, help text,
//!   labels.
//! - [`labels`]: the label names, and the closed value set of each label.
//! - [`trace_carrier`]: the carrier that a client hands the core at a send,
//!   its bounds, and the text that a row stores.

pub mod catalog;
pub mod labels;
pub mod trace_carrier;

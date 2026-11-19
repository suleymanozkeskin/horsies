//! The metrics of horsies.
//!
//! - [`catalog`]: every metric once, as data: name, kind, unit, help text,
//!   labels.
//! - [`labels`]: the label names, and the closed value set of each label.

pub mod catalog;
pub mod labels;

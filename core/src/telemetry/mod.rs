//! The metrics of horsies.
//!
//! - [`catalog`]: every metric once, as data: name, kind, unit, help text,
//!   labels.
//! - [`labels`]: the label names, and the closed value set of each label.
//! - [`declared`]: the label values that are names declared at start.
//! - [`registry`]: the series of each metric of a process, and their values.
//! - [`exposition`]: the Prometheus text of a registry.

pub mod catalog;
pub mod declared;
pub mod exposition;
pub mod labels;
pub mod registry;
#[cfg(test)]
mod registry_tests;

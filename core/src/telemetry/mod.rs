//! The metrics of horsies, and the trace carrier of a send.
//!
//! - [`catalog`]: every metric once, as data: name, kind, unit, help text,
//!   labels.
//! - [`labels`]: the label names, and the closed value set of each label.
//! - [`declared`]: the label values that are names declared at start.
//! - [`registry`]: the series of each metric of a process, and their values.
//! - [`exposition`]: the Prometheus text of a registry.
//! - [`listen`]: the `HOST:PORT` address of a metrics endpoint.
//! - `endpoint` (feature `metrics-endpoint`): the HTTP server of a metrics
//!   endpoint.
//! - [`trace_carrier`]: the carrier that a client hands the core at a send,
//!   its bounds, and the text that a row stores.
//! - [`worker`]: the telemetry value of a worker parent: the metric handles
//!   of its lifecycle points, and the bounded queue of its events.

pub mod catalog;
pub mod declared;
#[cfg(feature = "metrics-endpoint")]
pub mod endpoint;
pub mod exposition;
pub mod labels;
pub mod listen;
pub mod registry;
#[cfg(test)]
mod registry_tests;
pub mod trace_carrier;
pub mod worker;

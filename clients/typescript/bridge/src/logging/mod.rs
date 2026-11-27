//! Core log records to a JavaScript sink: the subscriber, the logger names,
//! the levels, the format of the stderr lines, the bounded channel, the forwarder thread, the drain before an
//! abort, the install form of a child process, and the record of a span that
//! the package did not make. A build with the cargo feature `test-probes`
//! also has the forwarder panic probe.

pub mod bridge;
pub mod format;
pub mod forwarder;
#[cfg(feature = "test-probes")]
pub mod forwarder_probe;
pub mod js_sink;
pub mod levels;
pub mod names;
pub mod span_not_made;
pub mod subscriber;

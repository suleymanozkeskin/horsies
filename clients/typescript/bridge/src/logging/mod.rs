//! Core log records to a JavaScript sink: the subscriber, the logger names,
//! the levels, the bounded channel, the forwarder thread, the drain before an
//! abort, the install form of a child process, and the record of a span that
//! the package did not make.

pub mod bridge;
pub mod forwarder;
pub mod js_sink;
pub mod levels;
pub mod names;
pub mod span_not_made;
pub mod subscriber;

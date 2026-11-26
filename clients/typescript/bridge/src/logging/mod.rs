//! Core log records to a JavaScript sink: the subscriber, the logger names,
//! the levels, the bounded channel, the forwarder thread, the drain before an
//! abort, and the install form of a child process.

pub mod bridge;
pub mod forwarder;
pub mod js_sink;
pub mod levels;
pub mod names;
pub mod subscriber;

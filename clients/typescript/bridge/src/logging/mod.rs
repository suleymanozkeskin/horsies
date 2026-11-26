//! Core log records to a JavaScript sink: the subscriber, the logger names,
//! the levels, the bounded channel, the forwarder thread, the drain before an
//! abort, and the install form of a child process. A build with the cargo
//! feature `test-probes` also has the forwarder panic probe.

pub mod bridge;
pub mod forwarder;
#[cfg(feature = "test-probes")]
pub mod forwarder_probe;
pub mod js_sink;
pub mod levels;
pub mod names;
pub mod subscriber;

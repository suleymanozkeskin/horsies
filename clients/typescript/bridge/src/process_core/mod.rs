//! One core per process: the runtime shape, the process id stamp, the stop
//! signal, the panic rule, the count of Node-API environments, the
//! main-thread rule, the environment cleanup hook, and the pure functions.
//! Loading the addon and calling a pure function start nothing.

pub mod environment;
pub mod panic_rule;
pub mod pure;
pub mod runtime;

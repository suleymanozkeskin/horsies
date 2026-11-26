//! The scheduler: the core's checks of a schedule declaration, the
//! scheduler of one scheduler manifest, and its run until a stop request.
//!
//! - `checks`: `checkSchedulePattern` (the core's rules of one pattern
//!   text) and `schedulerConfigFacts` (what the accepted app config gives
//!   each slot's send, and the payload bound of each schedule's input).
//!   Pure.
//! - `run`: `prepareScheduler` (pure: the core's parser reads the
//!   manifest), `runScheduler` (the start, then the ticks until a stop
//!   request, on the core's process runtime) and
//!   `PreparedScheduler.requestStop`.

pub mod checks;
pub mod run;

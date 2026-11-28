//! The end of a `SHARED_THREAD` child whose resident memory reached its
//! bound (`recycle.above` of the profile) while it held runs.
//!
//! - The main thread of the child reads its resident memory while it holds
//!   runs (`src/seats/memory-watch.ts` of the package) and calls
//!   `endAtMemoryLimit` at the bound.
//! - The call writes one line to stderr that names the bound and the
//!   measured value, then ends the process with the core's code of the end
//!   ([`ChildHostExit::MemoryLimit`], exit code 81) through the immediate
//!   exit of a JavaScript child ([`child_process_exit`]): the drain of Rust's
//!   standard output and standard error, then `_exit`. No exit handler and
//!   no telemetry flush runs, so a child that grows does not grow further
//!   while it waits for them.
//! - The line goes to stderr directly, not through the log bridge: the
//!   forwarder of the bridge and a JavaScript log sink can still hold the
//!   record when the process ends.
//! - The worker reads exit code 81 as a memory recycle
//!   (`ReplaceReason::MemoryLimit`). A run that the child held is lost with
//!   it and takes the path of a lost child, as the retry policy of its task
//!   says.

use std::io::Write;

use horsies_core::ChildHostExit;
use napi_derive::napi;

use crate::child::exit::child_process_exit;

/// The line of the end: the measured resident memory and the bound, both in
/// MiB. Pure.
fn memory_limit_line(bound_mb: u32, measured_mb: f64) -> String {
    format!(
        "horsies child: the resident memory of this SHARED_THREAD child is {measured_mb:.1} MiB, \
         at or above its bound recycle.above = {bound_mb} MiB, while it holds runs; the child \
         ends with exit code {} (MEMORY_LIMIT), and each run that it holds follows the retry \
         policy of its task",
        ChildHostExit::MemoryLimit.exit_code(),
    )
}

/// End this child process at its memory bound: one stderr line with
/// `bound_mb` and `measured_mb` (MiB), then the exit with code 81. Does not
/// return. Only the main thread of a `SHARED_THREAD` child calls it.
#[napi(ts_return_type = "never")]
pub fn end_at_memory_limit(bound_mb: u32, measured_mb: f64) {
    let line = memory_limit_line(bound_mb, measured_mb);
    // A closed stderr loses the line; the process ends with its code either way.
    let _written = writeln!(std::io::stderr(), "{line}");
    child_process_exit().end(ChildHostExit::MemoryLimit.exit_code())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_line_names_the_measured_value_the_bound_and_the_code_81() {
        let line = memory_limit_line(256, 301.25);

        assert!(line.contains("301.2 MiB"), "{line}");
        assert!(line.contains("recycle.above = 256 MiB"), "{line}");
        assert!(line.contains("exit code 81 (MEMORY_LIMIT)"), "{line}");
    }
}

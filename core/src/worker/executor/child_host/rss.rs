//! The resident set size of the child, sent with each end so the parent can
//! recycle a child at its memory limit (Python `core/worker/runtime.py`,
//! `_read_current_rss_mb`). An unreadable size never recycles.

use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};

use crate::worker::executor::protocol::{RssMb, RssReading};

const BYTES_PER_MIB: f64 = 1024.0 * 1024.0;

pub(super) fn read_rss() -> RssReading {
    let Ok(pid) = sysinfo::get_current_pid() else {
        return RssReading::Unreadable;
    };
    let Ok(mut system) = System::new() else {
        return RssReading::Unreadable;
    };
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_memory(),
    );
    let mebibytes = system
        .process(pid)
        .map(|process| process.memory() as f64 / BYTES_PER_MIB);
    match mebibytes.and_then(RssMb::new) {
        Some(rss) => RssReading::Mb(rss),
        None => RssReading::Unreadable,
    }
}

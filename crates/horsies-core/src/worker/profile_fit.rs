//! The fit of the worker's profile at boot (Python `python/horsies/core/cli.py`,
//! `_log_memory_fit` before the schema step and `_log_connection_fit` after
//! it, before any child starts or any row is claimed).
//!
//! Both log and never refuse: the worker starts in every case.
//! - Memory ([`crate::worker::memory_fit`]): the sum of the per-child memory
//!   limits against the container limit or the host RAM.
//! - Connections ([`crate::worker::connection_fit`]): the connections of one
//!   worker of the profile against the server's usable slots.

use crate::broker::PostgresBroker;
use crate::core::config::worker_profile::SelectedProfile;
use crate::core::config::PostgresConfig;
use crate::worker::connection_fit::{
    boot_connection_fit, boot_connection_log, log_boot_connection_fit, BootConnectionLog,
};
use crate::worker::memory_fit::{
    boot_memory_log, log_boot_memory_fit, read_boot_memory_fit, BootMemoryLog,
};

/// Read the memory fit of `selected` and log it. Reads two or three small
/// files; never refuses. Returns the records it logged.
pub fn log_memory_fit_at_boot(selected: &SelectedProfile) -> Vec<BootMemoryLog> {
    let records = boot_memory_log(&selected.name, &read_boot_memory_fit(&selected.profile));
    log_boot_memory_fit(&records);
    records
}

/// Read the server connection limit through `broker` (nothing through a
/// transaction pooler), compare `selected` with it, and log the result.
/// Writes nothing; never refuses: an unreadable limit is a warning. Returns
/// the record it logged.
pub async fn log_connection_fit_at_boot(
    selected: &SelectedProfile,
    broker_config: &PostgresConfig,
    broker: &PostgresBroker,
) -> BootConnectionLog {
    let fit = boot_connection_fit(&selected.profile, broker_config, broker).await;
    let record = boot_connection_log(&selected.name, &fit);
    log_boot_connection_fit(&record);
    record
}

#[cfg(test)]
mod tests;

//! Internal task-history vocabulary and immutable archive contracts.
//!
//! This module is intentionally dark until the broker and worker integration
//! phases wire it into production paths.

#![allow(dead_code)]

pub mod archive;
pub mod commands;
pub mod cutover;
pub mod ddl;
pub mod enqueue;
pub mod errors;
pub mod heartbeats;
pub mod identity;
pub mod maintenance;
pub mod names;
pub mod outcomes;
pub mod partitions;
pub mod phase2;
pub mod projection;
pub mod reads;
pub mod rerun;
pub mod transcode;

#[cfg(test)]
mod tests;

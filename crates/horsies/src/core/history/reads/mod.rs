//! Read paths over split live/history storage.

pub mod aggregates;
pub mod detail;
pub mod identity_lookup;
pub mod lookup_generation;
pub mod pages;
pub mod publisher;
pub mod result;

#[cfg(test)]
mod tests;

//! Partition catalog, lifecycle, health, and publication seams.

pub mod catalog;
pub mod locks;
pub mod manager;
pub mod publication;
pub mod retirement;

#[cfg(test)]
mod tests;

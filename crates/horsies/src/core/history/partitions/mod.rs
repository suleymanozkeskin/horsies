//! Partition catalog, lifecycle, health, and publication seams.

pub mod catalog;
pub mod forever;
pub mod health;
pub mod locks;
pub mod manager;
pub mod publication;

#[cfg(test)]
mod tests;

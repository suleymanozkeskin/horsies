//! Operator-owned, offline transition from the emitted schema to frozen v35.

pub mod drain;
pub mod identity;
pub mod ladder;
pub mod preflight;
pub mod preparation;
pub mod program;
pub mod relocation;
pub mod runner;
pub mod state;
pub mod tighten;
pub mod validation;

#[cfg(test)]
mod tests;

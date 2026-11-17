//! Typed clients for the database-owned workflow phase-2 program.

/// The client of `horsies_phase2_consume`, the phase-2 function that Python
/// calls. The core's phase 2 runs `horsies_workflow_node_finish` instead; the
/// tests of the consume function's dispositions use this client.
#[cfg(test)]
pub mod consumption;
pub mod quarantine;

#[cfg(test)]
mod ports;

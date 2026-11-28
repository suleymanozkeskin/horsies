//! Values that cross the Node-API boundary: the rules for integers, instants,
//! UUIDs and text; records; failure causes; catalog error values; the trace
//! carrier.

pub mod boundary;
pub mod catalog_values;
pub mod core_failures;
pub mod records;
pub mod trace_carrier;

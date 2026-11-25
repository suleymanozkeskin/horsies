//! How a JavaScript call runs a core operation: the call envelope and its
//! thrown codes, `guard`, the operation spawn and its bound of calls in
//! flight, the service runs, and the primitive through which the core calls
//! JavaScript.

// In a build without the feature `test-probes`, no export registers a
// callback yet: the later areas register theirs with this primitive.
#[cfg_attr(
    not(feature = "test-probes"),
    allow(
        dead_code,
        reason = "only the probe of the feature test-probes registers a callback yet"
    )
)]
pub mod callback;
#[cfg(feature = "test-probes")]
pub mod callback_probe;
pub mod guard;
pub mod operation;
pub mod service_run;
pub mod thrown;

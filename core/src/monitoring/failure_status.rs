//! The HTTP status of a monitoring read or action that failed on the
//! database: the one place of the policy, for the web routes of the core and
//! for the mounts of both clients.

use crate::db::Transience;

/// The HTTP status of a read or an action whose database operation failed,
/// by the core's classification of the cause ([`Transience`]):
///
/// - `Transient`, 503 (Service Unavailable): the condition may clear, and the
///   same request may succeed later. SQLSTATE classes 08 (connection), 40
///   (serialization failure, deadlock), 53 (insufficient resources), 55
///   (object not in prerequisite state, for example 55P03 lock not
///   available) and 57 (operator intervention, for example 57014 statement
///   timeout) ([`crate::db::TRANSIENT_SQLSTATE_CLASSES`]); a connection that
///   did not answer; a pool checkout that timed out.
/// - `Permanent`, 500 (Internal Server Error): the same request fails again
///   until the schema, the stored data, the configuration or the input
///   changes. Every other SQLSTATE class, for example 22 (data), 23
///   (integrity, for example 23514 check violation) and 42 (syntax or
///   undefined object), and an SQLSTATE whose class the core does not know;
///   a connection setting that the core refuses.
///
/// The status follows the `retryable` flag that the error catalogs carry,
/// because the flag is the same classification. Pure.
pub const fn database_failure_status(transience: Transience) -> u16 {
    match transience {
        Transience::Transient => 503,
        Transience::Permanent => 500,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_transient_cause_is_503_and_a_permanent_cause_is_500() {
        assert_eq!(database_failure_status(Transience::Transient), 503);
        assert_eq!(database_failure_status(Transience::Permanent), 500);
    }
}

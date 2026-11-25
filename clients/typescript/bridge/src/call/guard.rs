//! The panic guard of a bridge call.
//!
//! A panic that unwinds through the Node-API boundary aborts the process. So
//! every sync `#[napi]` function runs its body inside [`sync_call`]: a panic
//! becomes `CORE_PANIC` in the envelope, with the subject and the cause, and
//! the process runs on.

use std::panic::AssertUnwindSafe;

use crate::call::thrown::{Envelope, NativeThrown};
use crate::process_core::panic_rule::take_panic_cause;

/// Run the body of a sync bridge function. A panic of the body becomes
/// `CORE_PANIC { subject, cause }`.
pub fn sync_call<T>(subject: &'static str, body: impl FnOnce() -> Envelope<T>) -> Envelope<T> {
    match std::panic::catch_unwind(AssertUnwindSafe(body)) {
        Ok(envelope) => envelope,
        Err(payload) => Envelope::Thrown(NativeThrown::CorePanic {
            subject: subject.to_owned(),
            cause: take_panic_cause(payload.as_ref()),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_body_that_returns_gives_its_envelope() {
        assert_eq!(
            sync_call("core_state", || Envelope::Value(7_u32)),
            Envelope::Value(7)
        );
    }

    #[test]
    fn a_body_that_panics_gives_core_panic_with_the_subject() {
        let envelope: Envelope<u32> = sync_call("core_state", || panic!("planned panic"));
        match envelope {
            Envelope::Thrown(NativeThrown::CorePanic { subject, cause }) => {
                assert_eq!(subject, "core_state");
                assert!(cause.starts_with("planned panic"), "{cause}");
            }
            other => panic!("a panic is CORE_PANIC, got {other:?}"),
        }
    }
}

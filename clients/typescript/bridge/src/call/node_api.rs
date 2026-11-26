//! What the status of a threadsafe function call means.
//!
//! Every status of Node-API has its arm: a new status of napi-rs fails the
//! build here, so no status turns into a retry by a wildcard.

use napi::Status;

/// The meaning of the status of one threadsafe function call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallStatus {
    /// The call is in the queue of the JavaScript thread.
    Queued,
    /// The queue is full. It clears when the JavaScript thread takes a call.
    QueueFull,
    /// The threadsafe function or its environment is closing. It does not
    /// clear in this environment.
    Closing,
    /// Node-API refused the call with `status`: a defect, not a transient
    /// state.
    Refused { status: Status },
}

/// The meaning of `status`. Pure.
pub fn call_status(status: Status) -> CallStatus {
    match status {
        Status::Ok => CallStatus::Queued,
        Status::QueueFull => CallStatus::QueueFull,
        Status::Closing => CallStatus::Closing,
        Status::InvalidArg
        | Status::ObjectExpected
        | Status::StringExpected
        | Status::NameExpected
        | Status::FunctionExpected
        | Status::NumberExpected
        | Status::BooleanExpected
        | Status::ArrayExpected
        | Status::GenericFailure
        | Status::PendingException
        | Status::Cancelled
        | Status::EscapeCalledTwice
        | Status::HandleScopeMismatch
        | Status::CallbackScopeMismatch
        | Status::BigintExpected
        | Status::DateExpected
        | Status::ArrayBufferExpected
        | Status::DetachableArraybufferExpected
        | Status::WouldDeadlock
        | Status::NoExternalBuffersAllowed
        | Status::Unknown => CallStatus::Refused { status },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ok_is_queued() {
        assert_eq!(call_status(Status::Ok), CallStatus::Queued);
    }

    #[test]
    fn a_full_queue_is_queue_full() {
        assert_eq!(call_status(Status::QueueFull), CallStatus::QueueFull);
    }

    #[test]
    fn closing_is_closing() {
        assert_eq!(call_status(Status::Closing), CallStatus::Closing);
    }

    #[test]
    fn any_other_status_is_a_refusal_that_keeps_its_status() {
        for status in [
            Status::InvalidArg,
            Status::GenericFailure,
            Status::PendingException,
            Status::Unknown,
        ] {
            assert_eq!(call_status(status), CallStatus::Refused { status });
        }
    }
}

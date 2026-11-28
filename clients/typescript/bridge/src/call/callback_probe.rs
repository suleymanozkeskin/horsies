//! `probeCallback`: a test export that registers a JavaScript function and
//! calls it once from a core thread. The native tests of the callback
//! primitive call it; the package does not.
//!
//! Only a build with the cargo feature `test-probes` has this module
//! (`bun run --cwd bridge build:dev`). A published addon does not export it.

use napi::bindgen_prelude::{BigInt, FnArgs, Function, Object};
use napi::Env;
use napi_derive::napi;
use std::time::Duration;

use crate::call::callback::{CallbackFailure, CallbackWait, JsCallback, NotRunCause};
use crate::call::guard::CallKind;
use crate::call::operation::spawn_operation;
use crate::values::boundary::bigint_of_u64;

/// The queue bound of the registration of `probeCallback`: one call.
const PROBE_QUEUE_BOUND: usize = 1;

/// How long a call of `probeCallback` waits for the settle call.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProbeWait {
    /// The call ends with `noAnswer` after `millis` milliseconds.
    Within { millis: u32 },
    /// The call ends at the answer.
    NoBound,
}

impl ProbeWait {
    /// The wait of the callback primitive. Pure.
    fn callback_wait(self) -> CallbackWait {
        match self {
            Self::Within { millis } => {
                CallbackWait::Within(Duration::from_millis(u64::from(millis)))
            }
            Self::NoBound => CallbackWait::NoBound,
        }
    }
}

/// How one call of `probeCallback` ended.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallbackProbe {
    Answered {
        answer: String,
    },
    NotRun {
        cause: NotRunCause,
    },
    /// No settle call came within the bound of `ProbeWait::Within`.
    NoAnswer,
    Threw {
        call_id: BigInt,
        name: String,
        message: String,
    },
    Panicked {
        cause: String,
    },
    AnswerRefused {
        cause: String,
    },
    NodeApiRefused {
        status: String,
    },
    OnJavaScriptThread,
}

impl CallbackProbe {
    /// The record of one call's end. Pure.
    fn of(ended: Result<String, CallbackFailure>) -> Self {
        match ended {
            Ok(answer) => Self::Answered { answer },
            Err(CallbackFailure::NotRun { cause }) => Self::NotRun { cause },
            Err(CallbackFailure::NoAnswer { bound: _ }) => Self::NoAnswer,
            Err(CallbackFailure::Threw {
                call_id,
                name,
                message,
            }) => Self::Threw {
                call_id: bigint_of_u64(call_id),
                name,
                message,
            },
            Err(CallbackFailure::Panicked { cause }) => Self::Panicked { cause },
            Err(CallbackFailure::AnswerRefused { cause }) => Self::AnswerRefused { cause },
            Err(CallbackFailure::NodeApiRefused { status }) => Self::NodeApiRefused {
                status: status.to_string(),
            },
            Err(CallbackFailure::OnJavaScriptThread) => Self::OnJavaScriptThread,
        }
    }
}

/// Register `callback` and call it once from a core thread with `argument`,
/// waiting as `wait` says; resolve with how the call ended. Starts the core.
#[napi(ts_return_type = "Promise<NativeEnvelope<CallbackProbe>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn probe_callback<'env>(
    env: &'env Env,
    callback: Function<FnArgs<(BigInt, String)>, ()>,
    argument: String,
    wait: ProbeWait,
) -> napi::Result<Object<'env>> {
    let registered = JsCallback::<String, String, PROBE_QUEUE_BOUND>::register(&callback)?;
    spawn_operation(
        env,
        "probe_callback",
        CallKind::Read,
        move |_core| async move {
            CallbackProbe::of(registered.call(argument, wait.callback_wait()).await)
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_probe_wait_within_is_a_bound_in_milliseconds() {
        assert_eq!(
            ProbeWait::Within { millis: 250 }.callback_wait(),
            CallbackWait::Within(Duration::from_millis(250))
        );
    }

    #[test]
    fn a_probe_wait_no_bound_is_no_bound() {
        assert_eq!(ProbeWait::NoBound.callback_wait(), CallbackWait::NoBound);
    }

    #[test]
    fn a_call_with_no_answer_has_its_probe_record() {
        assert_eq!(
            CallbackProbe::of(Err(CallbackFailure::NoAnswer {
                bound: Duration::from_millis(50)
            })),
            CallbackProbe::NoAnswer
        );
    }

    #[test]
    fn each_end_of_a_call_has_its_probe_record() {
        assert_eq!(
            CallbackProbe::of(Ok("pong".to_owned())),
            CallbackProbe::Answered {
                answer: "pong".to_owned()
            }
        );
        assert_eq!(
            CallbackProbe::of(Err(CallbackFailure::NotRun {
                cause: NotRunCause::EnvClosing
            })),
            CallbackProbe::NotRun {
                cause: NotRunCause::EnvClosing
            }
        );
        assert_eq!(
            CallbackProbe::of(Err(CallbackFailure::Threw {
                call_id: u64::MAX,
                name: "Error".to_owned(),
                message: "boom".to_owned(),
            })),
            CallbackProbe::Threw {
                call_id: BigInt::from(u64::MAX),
                name: "Error".to_owned(),
                message: "boom".to_owned(),
            }
        );
        assert_eq!(
            CallbackProbe::of(Err(CallbackFailure::Panicked {
                cause: "boom".to_owned()
            })),
            CallbackProbe::Panicked {
                cause: "boom".to_owned()
            }
        );
        assert_eq!(
            CallbackProbe::of(Err(CallbackFailure::AnswerRefused {
                cause: "not a string".to_owned()
            })),
            CallbackProbe::AnswerRefused {
                cause: "not a string".to_owned()
            }
        );
        assert_eq!(
            CallbackProbe::of(Err(CallbackFailure::NodeApiRefused {
                status: napi::Status::GenericFailure
            })),
            CallbackProbe::NodeApiRefused {
                status: "GenericFailure".to_owned()
            }
        );
        assert_eq!(
            CallbackProbe::of(Err(CallbackFailure::OnJavaScriptThread)),
            CallbackProbe::OnJavaScriptThread
        );
    }
}

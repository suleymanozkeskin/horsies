//! The call envelope and its thrown codes.
//!
//! Every bridge function returns, or resolves with, one envelope:
//! `{ kind: "value", value }` or `{ kind: "thrown", thrown }`.
//!
//! - Rust never builds a JavaScript `Error` and never rejects a promise.
//! - The package's one call wrapper reads `thrown.code` and throws the class
//!   that the code names. The classes live in the package only.
//! - An expected failure of an operation is not thrown: it is the value of
//!   the envelope (`crate::values::records::Outcome`).
//!
//! Two families of codes:
//! - Process-state refusals: the call could not use the core in this
//!   process (`CORE_STOPPED`, `CORE_START_FAILED`, `TOO_MANY_PENDING_CALLS`,
//!   `FORKED_AFTER_CORE_START`).
//! - Client defects: the package sent a value or made a call that the bridge
//!   refuses, or the bridge itself panicked (`MANIFEST_REFUSED`,
//!   `PANIC_RULE_FIXED`, `CORE_PANIC`, `CLIENT_DEFECT`).
//!
//! A `worker_threads` isolate never reaches a call: the package's loader
//! refuses it, and a registration of the addon in a second environment
//! throws (`process_core::environment`).

use napi::bindgen_prelude::{sys, ToNapiValue};
use napi_derive::napi;

use crate::process_core::panic_rule::PanicRule;
use crate::values::boundary::BoundaryRefusal;
use crate::values::records::tagged_object;

/// What remains true after a call that a stop of the core ended early.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StateAfter {
    /// A read: nothing changed.
    NoChange,
    /// A write: it may have committed or not.
    OutcomeUnknown,
}

/// Why a call threw, with the facts of the cause.
#[napi(discriminant = "code", discriminant_case = "UPPER_SNAKE")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeThrown {
    /// A core background task panicked, so the core is stopped in this
    /// process. `subject` and `cause` are those of the first panic.
    CoreStopped {
        subject: String,
        cause: String,
        state_after: StateAfter,
    },
    /// The core runtime or its log forwarder could not start.
    CoreStartFailed { cause: String },
    /// The process already has the most core calls in flight that it may
    /// have.
    TooManyPendingCalls { subject: String, bound: u32 },
    /// This process was forked without exec after the core started in its
    /// parent.
    ForkedAfterCoreStart { core_pid: u32, this_pid: u32 },
    /// The core refused a manifest that the package built.
    ManifestRefused { field_path: String, cause: String },
    /// The panic rule of this process was set to another rule before, or
    /// fixed when the core started. `current` is the rule in force.
    PanicRuleFixed { current: PanicRule },
    /// The bridge panicked in the call; the core runs on.
    CorePanic { subject: String, cause: String },
    /// The package passed a value or made a call that it should have refused
    /// first. `rule` is the rule that does not hold.
    ClientDefect { subject: String, rule: String },
}

impl From<BoundaryRefusal> for NativeThrown {
    fn from(refusal: BoundaryRefusal) -> Self {
        Self::ClientDefect {
            subject: refusal.subject,
            rule: refusal.rule,
        }
    }
}

/// The answer of a bridge function: its value, or why it threw.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Envelope<T> {
    Value(T),
    Thrown(NativeThrown),
}

impl<T> From<NativeThrown> for Envelope<T> {
    fn from(thrown: NativeThrown) -> Self {
        Self::Thrown(thrown)
    }
}

impl<T: ToNapiValue> ToNapiValue for Envelope<T> {
    /// `{ kind: "value", value }` or `{ kind: "thrown", thrown }`.
    unsafe fn to_napi_value(env: sys::napi_env, envelope: Self) -> napi::Result<sys::napi_value> {
        match envelope {
            Self::Value(value) => {
                // SAFETY: `env` is the live environment of the JavaScript thread
                // that calls `to_napi_value`, as `tagged_object` requires.
                unsafe { tagged_object(env, "value", "value", value) }
            }
            Self::Thrown(thrown) => {
                // SAFETY: `env` is the live environment of the JavaScript thread
                // that calls `to_napi_value`, as `tagged_object` requires.
                unsafe { tagged_object(env, "thrown", "thrown", thrown) }
            }
        }
    }
}

//! The handler child app: the JavaScript event handlers, the start hooks of a
//! handler child, and the spans of ended workers.
//!
//! - `fillHandlerChild(plan, deliver, hook, endedSpan)`: on the JavaScript
//!   thread, before `serveChild`. It builds the core's `HandlerChildApp` of
//!   the plan and puts it in the handler slot of the child host, which the
//!   serve takes. A task child reads none of it.
//!   - The handlers: one `add_async::<E>` per handler (`handlers`), each
//!     calling `deliver` with its index and the wire text of the event.
//!   - The start hooks: the `onHandlerChildStart` hooks as the core's named
//!     start hooks, each calling `hook` with its index, as the start hooks
//!     of a task child do; the core runs each for at most
//!     `CHILD_START_HOOK_BOUND` and ends the handler child with 173 at a
//!     failure.
//!   - The spans: with `MAKE_SPANS`, the maker posts the facts of each span
//!     to `endedSpan` (`ended_spans`); with `NO_SPANS` the core makes none.
//! - A plan that the core does not take, a function that Node-API cannot
//!   register, and a second fill are defects of the package: `CLIENT_DEFECT`,
//!   and the slot does not change.

pub mod ended_spans;
pub mod handlers;

use std::sync::Arc;

use horsies_core::{EndedSpanScope, HandlerChildApp};
use napi::bindgen_prelude::{BigInt, FnArgs, Function};
use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::{Envelope, NativeThrown};
use crate::child::handler_slot::{fill_handler_slot, HandlerSlotRefused};
use crate::child::start_hooks::{start_hooks, HookDispatcher, StartHookCall};
use crate::handler_child::ended_spans::{EndedAttemptRecord, JsEndedSpans};
use crate::handler_child::handlers::{
    handler_set, HandlerDispatcher, HandlerEntry, HandlerEventCall,
};

/// The subject of the fill in its refusals.
const FILL_SUBJECT: &str = "fill_handler_child";

/// Whether the handler child makes the span of each attempt that the worker
/// ended.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndedSpansMode {
    NoSpans,
    MakeSpans,
}

/// What the package gives the handler child.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandlerChildPlan {
    /// The handlers in registration order. Handler `index` of the
    /// dispatcher is the entry at `index`.
    pub handlers: Vec<HandlerEntry>,
    /// The name of each `onHandlerChildStart` hook, in run order. Hook
    /// `index` of the hook dispatcher is the name at `index`.
    pub start_hooks: Vec<String>,
    pub ended_spans: EndedSpansMode,
}

/// Fill the handler slot of the child host with the handler child app of
/// `plan`. On the JavaScript thread, before `serveChild`.
#[napi(
    ts_args_type = "plan: HandlerChildPlan, deliver: (callId: bigint, call: HandlerEventCall) => void, hook: (callId: bigint, call: StartHookCall) => void, endedSpan: (attempt: EndedAttemptRecord) => void",
    ts_return_type = "NativeEnvelope<undefined>"
)]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn fill_handler_child(
    plan: HandlerChildPlan,
    deliver: Function<FnArgs<(BigInt, HandlerEventCall)>, ()>,
    hook: Function<FnArgs<(BigInt, StartHookCall)>, ()>,
    ended_span: Function<EndedAttemptRecord, ()>,
) -> Envelope<()> {
    sync_call(FILL_SUBJECT, move || {
        match handler_child_app(plan, &deliver, &hook, &ended_span) {
            Err(thrown) => Envelope::Thrown(thrown),
            Ok(app) => match fill_handler_slot(app) {
                Ok(()) => Envelope::Value(()),
                Err(refused) => Envelope::Thrown(client_defect(&slot_refusal_text(refused))),
            },
        }
    })
}

/// The handler child app of `plan`, on the JavaScript thread.
fn handler_child_app(
    plan: HandlerChildPlan,
    deliver: &Function<FnArgs<(BigInt, HandlerEventCall)>, ()>,
    hook: &Function<FnArgs<(BigInt, StartHookCall)>, ()>,
    ended_span: &Function<EndedAttemptRecord, ()>,
) -> Result<HandlerChildApp, NativeThrown> {
    let HandlerChildPlan {
        handlers,
        start_hooks: hook_names,
        ended_spans,
    } = plan;
    let dispatcher: Arc<HandlerDispatcher> =
        Arc::new(HandlerDispatcher::register(deliver).map_err(|error| {
            client_defect(&format!("Node-API refused the event dispatcher: {error}"))
        })?);
    let hooks: Arc<HookDispatcher> = Arc::new(HookDispatcher::register(hook).map_err(|error| {
        client_defect(&format!("Node-API refused the hook dispatcher: {error}"))
    })?);
    let handlers = handler_set(handlers, &dispatcher)
        .map_err(|refused| client_defect(&format!("the core refused a handler: {refused}")))?;
    Ok(HandlerChildApp {
        handlers,
        start_hook: start_hooks(hook_names, &hooks),
        ended_spans: ended_span_scope(ended_spans, ended_span)?,
    })
}

/// The span scope of `mode`: the JavaScript maker, or none.
fn ended_span_scope(
    mode: EndedSpansMode,
    ended_span: &Function<EndedAttemptRecord, ()>,
) -> Result<EndedSpanScope, NativeThrown> {
    match mode {
        EndedSpansMode::NoSpans => Ok(EndedSpanScope::NoSpans),
        EndedSpansMode::MakeSpans => {
            let maker = JsEndedSpans::register(ended_span).map_err(|error| {
                client_defect(&format!("Node-API refused the span maker: {error}"))
            })?;
            Ok(EndedSpanScope::MakeSpans(Arc::new(maker)))
        }
    }
}

/// The rule of a fill that the slot refused. Pure.
fn slot_refusal_text(refused: HandlerSlotRefused) -> String {
    match refused {
        HandlerSlotRefused::FilledAlready => "a process fills the handler child once".to_owned(),
        HandlerSlotRefused::TakenByTheServe => {
            "the handler child is filled before serveChild".to_owned()
        }
    }
}

fn client_defect(rule: &str) -> NativeThrown {
    NativeThrown::ClientDefect {
        subject: FILL_SUBJECT.to_owned(),
        rule: rule.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_slot_refusal_names_its_rule() {
        assert!(slot_refusal_text(HandlerSlotRefused::FilledAlready).contains("once"));
        assert!(
            slot_refusal_text(HandlerSlotRefused::TakenByTheServe).contains("before serveChild")
        );
    }
}

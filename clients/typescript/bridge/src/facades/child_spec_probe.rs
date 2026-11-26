//! `setProbeChildSpecSource`: a test export that sets a child spec source
//! that builds no child on a handle. The native tests of the monitoring
//! handler build a handle with no app, so no app sets its source; the
//! package does not call it.
//!
//! Only a build with the cargo feature `test-probes` has this module
//! (`bun run --cwd bridge build:dev`). A published addon does not export it.

use std::sync::Arc;

use horsies_core::{ChildSpecFuture, ChildSpecRequest, ChildSpecSource, ChildSpecUnavailable};
use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::{Envelope, NativeThrown};
use crate::handle::core_handle::CoreHandle;

/// The subject of the probe, in its refusal and in the answer of its
/// source.
const PROBE_SUBJECT: &str = "setProbeChildSpecSource";

/// A source that answers every child build with `NotRun`: the step that
/// asked rolls back and runs again through its retry or the workflow
/// recovery.
struct ProbeChildSpecSource;

impl ChildSpecSource for ProbeChildSpecSource {
    fn child_spec(&self, _request: ChildSpecRequest) -> ChildSpecFuture<'_> {
        Box::pin(std::future::ready(Err(ChildSpecUnavailable::NotRun {
            subject: PROBE_SUBJECT.to_owned(),
            cause: "the native tests build no sub-workflow child".to_owned(),
        })))
    }
}

/// Set the probe source on `handle`. A handle whose source is set already
/// throws `CLIENT_DEFECT`.
#[napi(ts_return_type = "NativeEnvelope<undefined>")]
pub fn set_probe_child_spec_source(handle: &CoreHandle) -> Envelope<()> {
    sync_call(PROBE_SUBJECT, || {
        match handle.set_child_spec_source(Arc::new(ProbeChildSpecSource)) {
            Ok(()) => Envelope::Value(()),
            Err(_set_already) => Envelope::Thrown(NativeThrown::ClientDefect {
                subject: PROBE_SUBJECT.to_owned(),
                rule: "the handle has no child spec source yet".to_owned(),
            }),
        }
    })
}

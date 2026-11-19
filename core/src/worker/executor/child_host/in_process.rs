//! Test support: one isolated run in the test process, as a Rust child runs
//! it (`seats::run_isolated`): the start write, the refusal or the body, the
//! result envelope, the finalizing marker, and the result frame. Tests of
//! the worker drive a run end to end with it, then hand the reported end to
//! the outcome writer, as the parent does.

use std::os::unix::net::UnixStream;
use std::sync::Arc;

use super::host::{ChildApp, ChildLogSetup, ChildStartHook};
use super::link::{read_frame, FrameWriter};
use super::run::HostContext;
use super::seats::run_isolated;
use crate::broker::PostgresBroker;
use crate::core::registry::task::TaskRegistry;
use crate::worker::executor::protocol::{
    decode_child_frame, ChildFrame, FrameReader, RunEnd, RunFrame,
};

/// The pid that the start of an in-process run writes.
pub(crate) const IN_PROCESS_PID: i32 = 4242;

/// The host name that the start of an in-process run writes.
pub(crate) const IN_PROCESS_HOSTNAME: &str = "in-process-host";

/// Run `run` with the tasks of `registry` on `broker`, and return the end
/// that the result frame reports.
///
/// Panics when the run sends any frame other than its result first (a body
/// that left work alive sends `body_returned`): a defect of the test.
pub(crate) async fn run_isolated_in_process(
    broker: Arc<PostgresBroker>,
    registry: Arc<TaskRegistry>,
    run: RunFrame,
) -> RunEnd {
    let (ours, theirs) = UnixStream::pair().expect("a socket pair");
    let writer = FrameWriter::start(ours, 16).expect("the frame writer starts");
    let context = HostContext {
        broker,
        app: Arc::new(ChildApp {
            registry,
            hostname: IN_PROCESS_HOSTNAME.to_owned(),
            start_hook: ChildStartHook::NoHook,
            producer_manifest: super::producer::test_producer_manifest(),
            log_setup: ChildLogSetup::NoSetup,
            family: crate::ClientFamily::Rust,
        }),
        pid: IN_PROCESS_PID,
        writer,
    };
    run_isolated(&context, run).await;
    let frame = tokio::task::spawn_blocking(move || {
        let mut reader = FrameReader::new();
        let mut stream = &theirs;
        let raw = match read_frame(&mut stream, &mut reader) {
            Ok(raw) => raw,
            Err(end) => panic!("the run sent no frame: {end:?}"),
        };
        match decode_child_frame(&raw.meta, raw.body) {
            Ok(frame) => frame,
            Err(refusal) => panic!("the run sent a frame that does not decode: {refusal}"),
        }
    })
    .await
    .expect("the frame read ends");
    match frame {
        ChildFrame::Result(result) => result.end,
        other => panic!("expected the result frame of the run, got {other:?}"),
    }
}

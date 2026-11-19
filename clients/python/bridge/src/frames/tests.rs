//! The codec as Python sees it: the classes and functions of `add_frames`
//! run in a module of their own, and each test drives them with Python code.
//! Parent frames come as wire bytes, pinned by the core's protocol tests;
//! child and preload frames are checked by the core's decoders.

use std::ffi::CStr;

use chrono::{TimeZone, Utc};
use horsies_core::events::AttemptStart;
use horsies_core::frame_protocol::{
    decode_preload_frame, BodyEnd, ChildAfterFrame, ChildFrame, ChildFrameRefusal, LeftWorkEnd,
    LoopCulprit, NoLeftWork, PreloadFrame, ReportedBootCause, RssReading, RunEnd, SeatEnd,
    ShortageErrno, StopReason, BOOT_FAILED_EXIT_CODE, CORE_STOPPED_EXIT_CODE, HEADER_BYTES,
    MAX_BODY_BYTES, MAX_BOOT_ERROR_NAME_CHARS, MAX_EVENT_HANDLERS, MAX_HANDLER_NAME_CHARS,
    MAX_LEFTOVER_TASK_NAMES, PROTOCOL_MISMATCH_EXIT_CODE, PROTOCOL_VERSION, SPAWN_REQUEST_BYTE,
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyModule};

use super::add_frames;

const TASK_ID: &str = "0190f2d8-7c1e-7a3b-9f00-000000000001";

const RUN_META: &str = r#"{"kind":"run","task_id":"0190f2d8-7c1e-7a3b-9f00-000000000001","task_name":"app.add","queue_name":"default","attempt":1,"worker_id":"w-1","claimed_at":"2026-11-17T10:00:00.123456Z","orphan_self_heal":true,"work":{"kind":"run"},"trace":{"kind":"no_carrier"}}"#;

const TRACED_RUN_META: &str = r#"{"kind":"run","task_id":"0190f2d8-7c1e-7a3b-9f00-000000000001","task_name":"app.add","queue_name":"default","attempt":1,"worker_id":"w-1","claimed_at":"2026-11-17T10:00:00Z","orphan_self_heal":false,"work":{"kind":"run"},"trace":{"kind":"carrier","text":"{\"traceparent\":\"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01\"}"}}"#;

const REFUSED_RUN_META: &str = r#"{"kind":"run","task_id":"0190f2d8-7c1e-7a3b-9f00-000000000001","task_name":"app.add","queue_name":"default","attempt":1,"worker_id":"w-1","claimed_at":"2026-11-17T10:00:00Z","orphan_self_heal":false,"work":{"kind":"refusal","refusal":{"kind":"value_invalid","reason":"timeout_ms is -1"}},"trace":{"kind":"no_carrier"}}"#;

const SHARED_RUN_META: &str = r#"{"kind":"shared_run","dispatch":7,"task_id":"0190f2d8-7c1e-7a3b-9f00-000000000001","task_name":"app.fetch","queue_name":"emails","attempt":2,"worker_id":"w-1","claimed_at":"2026-11-17T10:00:00Z","orphan_self_heal":false,"work":{"kind":"task","timeout_ms":5000,"cancel_grace_ms":250},"trace":{"kind":"no_carrier"}}"#;

const INIT_META: &str = r#"{"kind":"init","protocol":3,"program":{"kind":"python","app_locator":"app.main:app","imports":["app.tasks"],"sys_path_roots":["/srv/app"],"parent_sys_path":["/srv/app"],"parent_argv":["horsies","worker"]},"log_level":"info","database_url":"postgresql://user:secret@db/app","connection":{"tcp_keepalives":{"kind":"off"},"tcp_user_timeout":{"kind":"system_default"},"server_sessions":{"kind":"kept"}},"process_name":"horsies-child-1","child_kind":{"kind":"shared_async","seats":4,"pool":{"min_size":0,"max_size":2,"check":false},"executor_threads":3,"task_send_pool":1}}"#;

const VERSION_ONE_INIT_META: &str = r#"{"kind":"init","app_locator":"app.main:app"}"#;

const HANDLER_INIT_META: &str = r#"{"kind":"init","protocol":3,"program":{"kind":"python","app_locator":"app.main:app","imports":[],"sys_path_roots":[],"parent_sys_path":[],"parent_argv":[]},"log_level":"info","database_url":"postgresql://user:secret@db/app","connection":{"tcp_keepalives":{"kind":"off"},"tcp_user_timeout":{"kind":"system_default"},"server_sessions":{"kind":"kept"}},"process_name":"horsies-handlers","child_kind":{"kind":"handler","task_send_pool":2}}"#;

/// Two event wire forms of `core/tests/fixtures/event_wire/vectors.json`.
const CLAIM_RELEASED: &str = r#"["ClaimReleased",1700000000123456789,"0192f6a1-7b3c-7d4e-8f00-0123456789ab","send_email","default","QUEUE_FULL"]"#;
const SLOT_RECOVERED: &str = r#"["SlotRecovered",1700000000123456789,"SHARED_ASYNC",1]"#;

/// `meta` and `body` with the `>IQ` header.
fn framed(meta: &str, body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(HEADER_BYTES + meta.len() + body.len());
    let meta_bytes = u32::try_from(meta.len()).expect("a test frame is small");
    let body_bytes = u64::try_from(body.len()).expect("a test frame is small");
    bytes.extend_from_slice(&meta_bytes.to_be_bytes());
    bytes.extend_from_slice(&body_bytes.to_be_bytes());
    bytes.extend_from_slice(meta.as_bytes());
    bytes.extend_from_slice(body);
    bytes
}

/// Run `code` with the codec as `core`, the given byte values, and the
/// module `uuid` and `datetime`. Returns the globals after the run.
fn run_python<'py>(
    py: Python<'py>,
    code: &CStr,
    values: &[(&str, &[u8])],
) -> PyResult<Bound<'py, PyDict>> {
    let module = PyModule::new(py, "frames_under_test")?;
    add_frames(&module)?;
    let globals = PyDict::new(py);
    globals.set_item("core", &module)?;
    globals.set_item("uuid", py.import("uuid")?)?;
    globals.set_item("datetime", py.import("datetime")?)?;
    for (name, value) in values {
        globals.set_item(*name, PyBytes::new(py, value))?;
    }
    py.run(code, Some(&globals), None)?;
    Ok(globals)
}

/// The bytes that the Python code left in `name`.
fn bytes_named(globals: &Bound<'_, PyDict>, name: &str) -> Vec<u8> {
    globals
        .get_item(name)
        .expect("the lookup runs")
        .unwrap_or_else(|| panic!("the code sets {name}"))
        .extract::<Vec<u8>>()
        .expect("the value is bytes")
}

/// The child frame that the core parent decodes from `bytes`.
fn decoded_child(bytes: &[u8]) -> Result<ChildFrame, ChildFrameRefusal> {
    let meta_bytes =
        u32::from_be_bytes(bytes[..4].try_into().expect("a header has 4 size bytes")) as usize;
    let meta = &bytes[HEADER_BYTES..HEADER_BYTES + meta_bytes];
    let body = bytes[HEADER_BYTES + meta_bytes..].to_vec();
    horsies_core::frame_protocol::decode_child_frame(meta, body)
}

fn split_meta(bytes: &[u8]) -> (&[u8], &[u8]) {
    let meta_bytes =
        u32::from_be_bytes(bytes[..4].try_into().expect("a header has 4 size bytes")) as usize;
    (
        &bytes[HEADER_BYTES..HEADER_BYTES + meta_bytes],
        &bytes[HEADER_BYTES + meta_bytes..],
    )
}

#[test]
fn a_run_frame_decodes_with_every_field() {
    Python::initialize();
    let run = framed(RUN_META, br#"{"x":1}"#);
    let (meta, body) = split_meta(&run);
    Python::attach(|py| {
        run_python(
            py,
            c"frame = core.decode_parent_frame(meta, body)
assert isinstance(frame, core.RunFrame), frame
assert frame.task_id == uuid.UUID('0190f2d8-7c1e-7a3b-9f00-000000000001')
assert frame.task_name == 'app.add'
assert frame.queue_name == 'default'
assert frame.attempt == 1
assert frame.worker_id == 'w-1'
assert frame.claimed_at == datetime.datetime(2026, 11, 17, 10, 0, 0, 123456, tzinfo=datetime.UTC)
assert frame.orphan_self_heal is True
assert frame.work == 'RUN'
assert frame.trace == 'NO_CARRIER'
assert frame.input == '{\"x\":1}'
",
            &[("meta", meta), ("body", body)],
        )
        .map(drop)
    })
    .expect("the checks pass");
}

#[test]
fn a_refusal_of_stored_options_names_its_kind_and_reason() {
    Python::initialize();
    let run = framed(REFUSED_RUN_META, b"{}");
    let (meta, body) = split_meta(&run);
    Python::attach(|py| {
        run_python(
            py,
            c"frame = core.decode_parent_frame(meta, body)
assert isinstance(frame.work, core.StoredOptionsRefused), frame.work
assert frame.work.kind == 'TIMEOUT_VALUE_INVALID'
assert frame.work.reason == 'timeout_ms is -1'
",
            &[("meta", meta), ("body", body)],
        )
        .map(drop)
    })
    .expect("the checks pass");
}

#[test]
fn an_init_frame_gives_the_program_and_the_child_kind_but_not_the_url() {
    Python::initialize();
    let init = framed(INIT_META, b"");
    let (meta, body) = split_meta(&init);
    Python::attach(|py| {
        run_python(
            py,
            c"frame = core.decode_parent_frame(meta, body)
assert isinstance(frame, core.InitFrame), frame
assert isinstance(frame.program, core.PythonProgram)
assert frame.program.app_locator == 'app.main:app'
assert frame.program.imports == ['app.tasks']
assert frame.program.parent_argv == ['horsies', 'worker']
assert frame.log_level == 'INFO'
assert frame.process_name == 'horsies-child-1'
kind = frame.child_kind
assert isinstance(kind, core.SharedAsyncChildKind), kind
assert (kind.seats, kind.executor_threads, kind.task_send_pool) == (4, 3, 1)
assert (kind.pool.min_size, kind.pool.max_size, kind.pool.check) == (0, 2, False)
assert not hasattr(frame, 'database_url')
",
            &[("meta", meta), ("body", body)],
        )
        .map(drop)
    })
    .expect("the checks pass");
}

#[test]
fn an_init_frame_of_another_version_is_a_named_mismatch() {
    Python::initialize();
    let init = framed(VERSION_ONE_INIT_META, b"");
    let (meta, body) = split_meta(&init);
    Python::attach(|py| {
        let globals = run_python(
            py,
            c"refused = core.decode_parent_frame(meta, body)
assert isinstance(refused, core.ParentProtocolMismatch), refused
assert refused.received == 'NOT_NAMED'
numbers = core.protocol_numbers()
assert refused.expected == numbers.protocol_version
",
            &[("meta", meta), ("body", body)],
        )?;
        let expected: u32 = globals
            .get_item("numbers")?
            .expect("the code sets numbers")
            .getattr("protocol_version")?
            .extract()?;
        assert_eq!(expected, PROTOCOL_VERSION);
        Ok::<(), PyErr>(())
    })
    .expect("the checks pass");
}

#[test]
fn a_malformed_parent_frame_is_refused_with_its_reason() {
    Python::initialize();
    Python::attach(|py| {
        run_python(
            py,
            c"refused = core.decode_parent_frame(b'{\"kind\":\"dance\"}', b'')
assert isinstance(refused, core.FrameRefused), refused
assert 'dance' in refused.reason
",
            &[],
        )
        .map(drop)
    })
    .expect("the checks pass");
}

#[test]
fn the_reader_gives_each_frame_once_in_order_however_the_bytes_arrive() {
    Python::initialize();
    let mut stream = framed(RUN_META, br#"{"x":1}"#);
    stream.extend(framed(SHARED_RUN_META, br#"{"y":2}"#));
    stream.extend(framed(r#"{"kind":"stop"}"#, b""));
    Python::attach(|py| {
        run_python(
            py,
            c"for size in (1, 7, 64, len(stream)):
    reader = core.FrameReader()
    frames = []
    for start in range(0, len(stream), size):
        fed = reader.feed(stream[start:start + size])
        assert isinstance(fed, list), fed
        frames.extend(fed)
    kinds = [type(frame).__name__ for frame in frames]
    assert kinds == ['RunFrame', 'SharedRunFrame', 'StopFrame'], (size, kinds)
    assert frames[1].dispatch == 7
    assert (frames[1].queue_name, frames[1].attempt) == ('emails', 2)
    assert frames[1].orphan_self_heal is False
    assert isinstance(frames[1].work, core.AsyncTaskWork)
    assert (frames[1].work.timeout_ms, frames[1].work.cancel_grace_ms) == (5000, 250)
    assert frames[1].input == '{\"y\":2}'
",
            &[("stream", &stream)],
        )
        .map(drop)
    })
    .expect("the checks pass");
}

#[test]
fn a_header_over_the_bounds_stops_the_reader() {
    Python::initialize();
    let mut header = Vec::new();
    header.extend_from_slice(&u32::MAX.to_be_bytes());
    header.extend_from_slice(&0_u64.to_be_bytes());
    Python::attach(|py| {
        run_python(
            py,
            c"over = core.FrameReader().feed(header)
assert isinstance(over, core.FrameOverBound), over
assert over.meta_bytes == 2**32 - 1
",
            &[("header", &header)],
        )
        .map(drop)
    })
    .expect("the checks pass");
}

#[test]
fn a_result_frame_reaches_the_parent_with_its_envelope() {
    Python::initialize();
    let globals = Python::attach(|py| {
        run_python(
            py,
            c"task_id = uuid.UUID('0190f2d8-7c1e-7a3b-9f00-000000000001')
started_at = datetime.datetime(2026, 11, 17, 10, 0, 1, 250000, tzinfo=datetime.UTC)
encoded = core.encode_child_frame(core.ResultFrame(
    task_id=task_id,
    end=core.TaskResult(envelope='{\"ok\":true}'),
    started_at=started_at,
    rss_mb=31.5,
    task_loop='NO_TASK_LOOP',
    child='SERVES_ON',
))
assert isinstance(encoded, bytes), encoded
",
            &[],
        )
        .map(Bound::unbind)
    })
    .expect("the frame encodes");
    let bytes = Python::attach(|py| bytes_named(globals.bind(py), "encoded"));
    match decoded_child(&bytes).expect("the parent decodes the frame") {
        ChildFrame::Result(result) => {
            assert_eq!(result.task_id.to_string(), TASK_ID);
            assert!(
                matches!(&result.end, RunEnd::Result(envelope) if envelope.as_str() == r#"{"ok":true}"#)
            );
            assert!(matches!(result.rss, RssReading::Mb(rss) if rss.get() == 31.5));
            assert_eq!(result.task_loop, NoLeftWork::NoTaskLoop);
            assert_eq!(result.child, ChildAfterFrame::ServesOn);
            assert_eq!(
                result.started_at,
                AttemptStart::Reported {
                    started_at: Utc.with_ymd_and_hms(2026, 11, 17, 10, 0, 1).unwrap()
                        + chrono::Duration::milliseconds(250),
                }
            );
        }
        other => panic!("a result frame decodes as a result frame, not {other:?}"),
    }
}

#[test]
fn every_end_and_report_reaches_the_parent() {
    Python::initialize();
    let globals = Python::attach(|py| {
        run_python(
            py,
            c"task_id = uuid.UUID('0190f2d8-7c1e-7a3b-9f00-000000000001')
started_at = datetime.datetime(2026, 11, 17, 10, 0, 1, tzinfo=datetime.UTC)
left = core.LoopHoldsTasks(count=12, names=[f'job_{n}' for n in range(10)])
stuck = core.StuckWork(tasks=0, executor_items=1, names=[])
stopped = core.encode_child_frame(core.ResultFrame(
    task_id=task_id,
    end=core.StoppedBeforeRunning(reason='WORKFLOW_STOPPED'),
    started_at='NOT_REPORTED',
    rss_mb='UNREADABLE',
    task_loop='LOOP_HOLDS_NO_TASK',
    child='ENDS_ITSELF',
))
timed_out = core.encode_child_frame(core.SharedResultFrame(
    dispatch=3,
    task_id=task_id,
    end='TIMED_OUT',
    started_at=started_at,
    rss_mb=12.0,
    child='SERVES_ON',
))
released = core.encode_child_frame(core.SharedResultFrame(
    dispatch=4,
    task_id=task_id,
    end=core.StoppedBeforeRunning(reason='OWNERSHIP_UNCONFIRMED'),
    started_at='NOT_REPORTED',
    rss_mb=12.0,
    child='ENDS_ITSELF',
))
returned = core.encode_child_frame(core.BodyReturnedFrame(
    task_id=task_id, returned=core.WorkerFailure(detail='boom'), started_at=started_at, left=left,
))
ended = core.encode_child_frame(core.LeftWorkEndedFrame(
    task_id=task_id, end='CANCELLED', rss_mb=1.0,
))
leftover = core.encode_child_frame(core.LeftoverStuckFrame(
    dispatch=3,
    task_id=task_id,
    work=stuck,
    returned=core.TaskResult(envelope='{}'),
    started_at=started_at,
))
cancel = core.encode_child_frame(core.LeftoverCancelFrame(
    dispatch=3, task_id=task_id, returned=core.TaskResult(envelope='{}'), started_at=started_at,
))
blocked = core.encode_child_frame(core.LoopBlockedFrame(
    culprit=core.BlockingRun(dispatch=3, task_id=task_id), blocked_ms=5100, location='x' * 900,
))
booted = core.encode_child_frame(core.BootFailedFrame(
    cause=core.ResourceShortage(errno='EMFILE'), error_name='E' * 500, detail='too many files',
))
ready = core.encode_child_frame(core.ReadyFrame(pid=4321, rss_mb=25.0))
",
            &[],
        )
        .map(Bound::unbind)
    })
    .expect("every frame encodes");
    let frame = |name: &str| {
        let bytes = Python::attach(|py| bytes_named(globals.bind(py), name));
        decoded_child(&bytes).unwrap_or_else(|refused| panic!("{name}: {refused}"))
    };
    let started_at = Utc.with_ymd_and_hms(2026, 11, 17, 10, 0, 1).unwrap();
    assert!(matches!(
        frame("stopped"),
        ChildFrame::Result(result)
            if result.end == RunEnd::StoppedBeforeRunning(StopReason::WorkflowStopped)
                && result.started_at == AttemptStart::NotReported
                && result.rss == RssReading::Unreadable
                && result.child == ChildAfterFrame::EndsItself
    ));
    assert!(matches!(
        frame("timed_out"),
        ChildFrame::SharedResult(result)
            if result.end == SeatEnd::TimedOut
                && result.started_at == AttemptStart::Reported { started_at }
                && result.child == ChildAfterFrame::ServesOn
    ));
    assert!(matches!(
        frame("released"),
        ChildFrame::SharedResult(result)
            if result.end
                == SeatEnd::Finished(RunEnd::StoppedBeforeRunning(
                    StopReason::OwnershipUnconfirmed
                ))
                && result.started_at == AttemptStart::NotReported
                && result.child == ChildAfterFrame::EndsItself
    ));
    match frame("returned") {
        ChildFrame::BodyReturned(returned) => {
            assert!(
                matches!(&returned.returned, BodyEnd::WorkerFailure(detail) if detail.as_str() == "boom")
            );
            assert_eq!(returned.started_at, started_at);
            assert_eq!(returned.left.count().get(), 12);
            assert_eq!(returned.left.names().len(), 10);
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        frame("cancel"),
        ChildFrame::LeftoverCancel(cancel) if cancel.started_at == started_at
    ));
    assert!(matches!(
        frame("ended"),
        ChildFrame::LeftWorkEnded(ended) if ended.end == LeftWorkEnd::Cancelled
    ));
    assert!(matches!(
        frame("leftover"),
        ChildFrame::LeftoverStuck(stuck)
            if stuck.work.executor_items() == 1 && stuck.started_at == started_at
    ));
    match frame("blocked") {
        ChildFrame::LoopBlocked(blocked) => {
            assert!(matches!(blocked.culprit, LoopCulprit::Run(run) if run.dispatch.get() == 3));
            assert_eq!(blocked.location.as_str().chars().count(), 500);
        }
        other => panic!("{other:?}"),
    }
    match frame("booted") {
        ChildFrame::BootFailed(failed) => {
            assert_eq!(
                failed.cause,
                ReportedBootCause::ResourceShortage(ShortageErrno::Emfile)
            );
            assert_eq!(
                failed.error_name.as_str().chars().count(),
                MAX_BOOT_ERROR_NAME_CHARS
            );
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(frame("ready"), ChildFrame::Ready(ready) if ready.pid.get() == 4321));
}

#[test]
fn a_value_outside_its_bound_is_a_value_error_of_the_child() {
    Python::initialize();
    Python::attach(|py| {
        run_python(
            py,
            c"task_id = uuid.UUID('0190f2d8-7c1e-7a3b-9f00-000000000001')
started_at = datetime.datetime(2026, 11, 17, 10, 0, 1, tzinfo=datetime.UTC)
cases = [
    lambda: core.ReadyFrame(pid=0, rss_mb=1.0),
    lambda: core.ReadyFrame(pid=1, rss_mb=float('nan')),
    lambda: core.ReadyFrame(pid=1, rss_mb='LOST'),
    lambda: core.DeadlineFiredFrame(dispatch=0, task_id=task_id),
    lambda: core.LoopHoldsTasks(count=3, names=['a']),
    lambda: core.LoopHoldsTasks(count=0, names=[]),
    lambda: core.StuckWork(tasks=0, executor_items=0, names=[]),
    lambda: core.StoppedBeforeRunning(reason='GONE'),
    lambda: core.ResourceShortage(errno='EPERM'),
    lambda: core.LeftWorkEndedFrame(task_id=task_id, end='LATER', rss_mb=1.0),
    lambda: core.SharedResultFrame(
        dispatch=1,
        task_id=task_id,
        end='LATE',
        started_at=started_at,
        rss_mb=1.0,
        child='SERVES_ON',
    ),
    lambda: core.SharedResultFrame(
        dispatch=1,
        task_id=task_id,
        end='TIMED_OUT',
        started_at=started_at,
        rss_mb=1.0,
        child='RETIRES',
    ),
    lambda: core.ResultFrame(
        task_id=task_id,
        end=core.TaskResult(envelope='{}'),
        started_at=started_at,
        rss_mb=1.0,
        task_loop='NO_TASK_LOOP',
        child='RETIRES',
    ),
    lambda: core.ResultFrame(
        task_id=task_id,
        end=core.StoppedBeforeRunning(reason='CLAIM_LOST'),
        started_at=started_at,
        rss_mb=1.0,
        task_loop='NO_TASK_LOOP',
        child='SERVES_ON',
    ),
    lambda: core.ResultFrame(
        task_id=task_id,
        end=core.TaskResult(envelope='{}'),
        started_at='NOT_REPORTED',
        rss_mb=1.0,
        task_loop='NO_TASK_LOOP',
        child='SERVES_ON',
    ),
    lambda: core.SharedResultFrame(
        dispatch=1,
        task_id=task_id,
        end=core.StoppedBeforeRunning(reason='CLAIM_LOST'),
        started_at=started_at,
        rss_mb=1.0,
        child='SERVES_ON',
    ),
    lambda: core.SharedResultFrame(
        dispatch=1,
        task_id=task_id,
        end='TIMED_OUT',
        started_at='LATER',
        rss_mb=1.0,
        child='SERVES_ON',
    ),
    lambda: core.HandlerCounts(handler='', errors=0, dropped=0),
    lambda: core.HandlerCounts(handler='h' * 256, errors=0, dropped=0),
    lambda: core.HandlerStatsFrame(
        handlers=[
            core.HandlerCounts(handler='a', errors=0, dropped=0),
            core.HandlerCounts(handler='a', errors=1, dropped=0),
        ],
        rss_mb=1.0,
    ),
    lambda: core.HandlerStatsFrame(
        handlers=[core.HandlerCounts(handler=f'h{n}', errors=0, dropped=0) for n in range(33)],
        rss_mb=1.0,
    ),
]
for position, case in enumerate(cases):
    try:
        case()
    except ValueError:
        continue
    raise AssertionError(f'case {position} was not refused')
",
            &[],
        )
        .map(drop)
    })
    .expect("every case is refused");
}

#[test]
fn a_preload_frame_reaches_the_parent() {
    Python::initialize();
    let globals = Python::attach(|py| {
        run_python(
            py,
            c"forked = core.encode_preload_frame(core.ChildForkedFrame(pid=99))
refused = core.encode_preload_frame(core.PreloadRefusedFrame(reason='r' * 5000))
",
            &[],
        )
        .map(Bound::unbind)
    })
    .expect("the frames encode");
    let frame = |name: &str| {
        let bytes = Python::attach(|py| bytes_named(globals.bind(py), name));
        let (meta, body) = split_meta(&bytes);
        decode_preload_frame(meta, body).unwrap_or_else(|malformed| panic!("{name}: {malformed}"))
    };
    assert!(matches!(
        frame("forked"),
        PreloadFrame::ChildForked { pid } if pid.get() == 99
    ));
    assert!(matches!(
        frame("refused"),
        PreloadFrame::PreloadRefused { reason } if reason.as_str().chars().count() == 4096
    ));
}

#[test]
fn the_protocol_numbers_are_the_core_constants() {
    Python::initialize();
    Python::attach(|py| {
        let globals = run_python(py, c"numbers = core.protocol_numbers()\n", &[])?;
        let numbers = globals.get_item("numbers")?.expect("the code sets numbers");
        let read = |name: &str| -> PyResult<i64> { numbers.getattr(name)?.extract() };
        assert_eq!(read("protocol_version")?, i64::from(PROTOCOL_VERSION));
        assert_eq!(
            read("boot_failed_exit_code")?,
            i64::from(BOOT_FAILED_EXIT_CODE)
        );
        assert_eq!(
            read("protocol_mismatch_exit_code")?,
            i64::from(PROTOCOL_MISMATCH_EXIT_CODE)
        );
        assert_eq!(
            read("core_stopped_exit_code")?,
            i64::from(CORE_STOPPED_EXIT_CODE)
        );
        assert_eq!(read("spawn_request_byte")?, i64::from(SPAWN_REQUEST_BYTE));
        assert_eq!(
            read("max_leftover_task_names")?,
            i64::try_from(MAX_LEFTOVER_TASK_NAMES).expect("a small bound")
        );
        assert_eq!(
            read("max_body_bytes")?,
            i64::try_from(MAX_BODY_BYTES).expect("the body bound fits an i64")
        );
        Ok::<(), PyErr>(())
    })
    .expect("the checks pass");
}

#[test]
fn a_run_frame_gives_its_stored_trace_carrier() {
    Python::initialize();
    let run = framed(TRACED_RUN_META, b"{}");
    let (meta, body) = split_meta(&run);
    Python::attach(|py| {
        run_python(
            py,
            c"frame = core.decode_parent_frame(meta, body)
assert isinstance(frame, core.RunFrame), frame
assert isinstance(frame.trace, core.StoredTraceCarrier), frame.trace
assert frame.trace.carrier == {
    'traceparent': '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01',
}
",
            &[("meta", meta), ("body", body)],
        )
        .map(drop)
    })
    .expect("the checks pass");
}

#[test]
fn an_init_frame_gives_the_handler_kind() {
    Python::initialize();
    let init = framed(HANDLER_INIT_META, b"");
    let (meta, body) = split_meta(&init);
    Python::attach(|py| {
        run_python(
            py,
            c"frame = core.decode_parent_frame(meta, body)
assert isinstance(frame, core.InitFrame), frame
assert isinstance(frame.child_kind, core.HandlerChildKind), frame.child_kind
assert frame.child_kind.task_send_pool == 2
",
            &[("meta", meta), ("body", body)],
        )
        .map(drop)
    })
    .expect("the checks pass");
}

#[test]
fn an_events_frame_gives_the_wire_text_of_each_event() {
    Python::initialize();
    let body = format!("[{CLAIM_RELEASED},{SLOT_RECOVERED}]");
    let mut stream = framed(r#"{"kind":"events"}"#, body.as_bytes());
    stream.extend(framed(r#"{"kind":"stop"}"#, b""));
    Python::attach(|py| {
        let globals = run_python(
            py,
            c"frames = core.FrameReader().feed(stream)
assert isinstance(frames, list), frames
assert [type(frame).__name__ for frame in frames] == ['EventsFrame', 'StopFrame'], frames
events = frames[0].events
",
            &[("stream", &stream)],
        )?;
        let events: Vec<String> = globals
            .get_item("events")?
            .expect("the code sets events")
            .extract()?;
        assert_eq!(events, vec![CLAIM_RELEASED, SLOT_RECOVERED]);
        Ok::<(), PyErr>(())
    })
    .expect("the checks pass");
}

#[test]
fn a_handler_stats_frame_reaches_the_parent() {
    Python::initialize();
    let globals = Python::attach(|py| {
        run_python(
            py,
            c"encoded = core.encode_child_frame(core.HandlerStatsFrame(
    handlers=[
        core.HandlerCounts(handler='app.handlers.on_claim', errors=0, dropped=3),
        core.HandlerCounts(handler='app.handlers.on_end', errors=2, dropped=0),
    ],
    rss_mb=40.5,
))
empty = core.encode_child_frame(core.HandlerStatsFrame(handlers=[], rss_mb='UNREADABLE'))
",
            &[],
        )
        .map(Bound::unbind)
    })
    .expect("the frames encode");
    let frame = |name: &str| {
        let bytes = Python::attach(|py| bytes_named(globals.bind(py), name));
        decoded_child(&bytes).unwrap_or_else(|refused| panic!("{name}: {refused}"))
    };
    match frame("encoded") {
        ChildFrame::HandlerStats(stats) => {
            let counts: Vec<(&str, u64, u64)> = stats
                .handlers
                .handlers()
                .iter()
                .map(|counts| (counts.handler.as_str(), counts.errors, counts.dropped))
                .collect();
            assert_eq!(
                counts,
                vec![
                    ("app.handlers.on_claim", 0, 3),
                    ("app.handlers.on_end", 2, 0)
                ]
            );
            assert!(matches!(stats.rss, RssReading::Mb(rss) if rss.get() == 40.5));
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        frame("empty"),
        ChildFrame::HandlerStats(stats)
            if stats.handlers.handlers().is_empty() && stats.rss == RssReading::Unreadable
    ));
}

#[test]
fn the_protocol_numbers_give_the_bounds_of_a_handler_child() {
    Python::initialize();
    Python::attach(|py| {
        let globals = run_python(py, c"numbers = core.protocol_numbers()\n", &[])?;
        let numbers = globals.get_item("numbers")?.expect("the code sets numbers");
        let read = |name: &str| -> PyResult<usize> { numbers.getattr(name)?.extract() };
        assert_eq!(read("max_event_handlers")?, MAX_EVENT_HANDLERS);
        assert_eq!(read("max_handler_name_chars")?, MAX_HANDLER_NAME_CHARS);
        Ok::<(), PyErr>(())
    })
    .expect("the checks pass");
}

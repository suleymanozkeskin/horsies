//! Tests of the frame codec: byte examples of the Python codec, pinned
//! version-4 metadata, every refusal, and the read buffer.

mod field_checks;
mod frame_steps;
mod python_vectors;
mod version_four;
mod version_three;

use std::num::NonZeroU64;

use chrono::{DateTime, TimeZone, Utc};
use uuid::Uuid;

use self::python_vectors::*;
use super::*;
use crate::broker::OrphanSelfHeal;
use crate::core::codec::StrictJsonError;
use crate::core::config::socket::{
    KeepaliveProbes, KeepaliveSeconds, TcpKeepalivesConfig, TcpUserTimeoutConfig, TcpUserTimeoutMs,
};
use crate::core::config::{PostgresConfig, PostgresConfigError};
use crate::core::task::error::OutcomeCode;
use crate::events::{AttemptNumber, QueueName};

const T1: &str = "0190f5a2-7b1c-7d3e-8f00-000000000001";
const T2: &str = "0190f5a2-7b1c-7d3e-8f00-000000000002";
const T3: &str = "0190f5a2-7b1c-7d3e-8f00-000000000003";
const T4: &str = "0190f5a2-7b1c-7d3e-8f00-000000000004";
const T5: &str = "0190f5a2-7b1c-7d3e-8f00-000000000005";

fn task_id(text: &str) -> Uuid {
    Uuid::parse_str(text).unwrap()
}

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).unwrap())
        .collect()
}

/// A frame from its metadata text and body; the header names both lengths.
fn framed(meta: &str, body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&u32::try_from(meta.len()).unwrap().to_be_bytes());
    bytes.extend_from_slice(&u64::try_from(body.len()).unwrap().to_be_bytes());
    bytes.extend_from_slice(meta.as_bytes());
    bytes.extend_from_slice(body);
    bytes
}

/// The metadata and the body of one whole frame, cut by its header.
fn split(bytes: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let mut header = [0_u8; HEADER_BYTES];
    header.copy_from_slice(&bytes[..HEADER_BYTES]);
    let sizes = parse_header(header).unwrap();
    let body_at = HEADER_BYTES + sizes.meta_bytes();
    assert_eq!(bytes.len(), body_at + sizes.body_bytes());
    (
        bytes[HEADER_BYTES..body_at].to_vec(),
        bytes[body_at..].to_vec(),
    )
}

fn decode_child_bytes(bytes: &[u8]) -> Result<ChildFrame, ChildFrameRefusal> {
    let (meta, body) = split(bytes);
    decode_child_frame(&meta, body)
}

fn decode_parent_bytes(bytes: &[u8]) -> Result<ParentFrame, ParentFrameRefusal> {
    let (meta, body) = split(bytes);
    decode_parent_frame(&meta, body)
}

/// Encode, check the bytes, decode, check the frame.
fn assert_child_bytes(frame: &ChildFrame, expected: &[u8]) {
    let bytes = encode_child_frame(frame).unwrap().to_bytes();
    assert_eq!(
        String::from_utf8_lossy(&bytes),
        String::from_utf8_lossy(expected)
    );
    assert_eq!(bytes, expected);
    assert_eq!(&decode_child_bytes(expected).unwrap(), frame);
}

fn assert_parent_bytes(frame: &ParentFrame, expected: &[u8]) {
    let bytes = encode_parent_frame(frame).unwrap().to_bytes();
    assert_eq!(
        String::from_utf8_lossy(&bytes),
        String::from_utf8_lossy(expected)
    );
    assert_eq!(bytes, expected);
    assert_eq!(&decode_parent_bytes(expected).unwrap(), frame);
}

fn assert_preload_bytes(frame: &PreloadFrame, expected: &[u8]) {
    let bytes = encode_preload_frame(frame).unwrap().to_bytes();
    assert_eq!(bytes, expected);
    let (meta, body) = split(expected);
    assert_eq!(&decode_preload_frame(&meta, &body).unwrap(), frame);
}

fn malformed_child(bytes: &[u8]) -> MalformedFrame {
    match decode_child_bytes(bytes) {
        Err(ChildFrameRefusal::Malformed(malformed)) => malformed,
        other => panic!("expected a malformed frame, got {other:?}"),
    }
}

fn malformed_parent(bytes: &[u8]) -> MalformedFrame {
    match decode_parent_bytes(bytes) {
        Err(ParentFrameRefusal::Malformed(malformed)) => malformed,
        other => panic!("expected a malformed frame, got {other:?}"),
    }
}

fn run_ref(dispatch: u64, task: &str) -> RunRef {
    RunRef {
        dispatch: DispatchNumber::new(dispatch).unwrap(),
        task_id: task_id(task),
    }
}

fn rss(value: f64) -> RssReading {
    RssReading::Mb(RssMb::new(value).unwrap())
}

fn names(texts: &[&str]) -> Vec<LeftoverName> {
    texts
        .iter()
        .map(|text| LeftoverName::new((*text).to_owned()).unwrap())
        .collect()
}

fn claim_time() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 16, 9, 30, 0).unwrap() + chrono::Duration::microseconds(123_456)
}

fn whole_second_claim_time() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 16, 9, 30, 0).unwrap()
}

/// The queue of the run frames of these tests.
fn default_queue() -> QueueName {
    QueueName::new("default".to_owned()).unwrap()
}

fn pool() -> ChildPool {
    ChildPool {
        min_size: PoolMinSize::new(0).unwrap(),
        max_size: PoolMaxSize::new(1).unwrap(),
        check: true,
    }
}

fn isolated_kind() -> ChildKind {
    ChildKind::Isolated(IsolatedChild {
        pool: pool(),
        task_send_pool: TaskSendPool::new(2).unwrap(),
    })
}

fn envelope(text: &str) -> ResultEnvelopeText {
    ResultEnvelopeText::new(text.to_owned())
}

/// The start time of the attempts of these tests.
fn start_time() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 16, 9, 30, 1).unwrap() + chrono::Duration::microseconds(250_000)
}

/// A start that the child reported, at [`start_time`].
fn reported() -> crate::events::AttemptStart {
    crate::events::AttemptStart::Reported {
        started_at: start_time(),
    }
}

/// The wire text of [`reported`].
const REPORTED: &str = r#"{"kind":"reported","time":"2026-11-16T09:30:01.250Z"}"#;

// ---------------------------------------------------------------------------
// Byte examples of the Python codec: frames whose content did not change
// ---------------------------------------------------------------------------

#[test]
fn the_stop_frame_has_the_python_bytes() {
    assert_parent_bytes(&ParentFrame::Stop, &hex(PY_STOP));
}

#[test]
fn boot_failed_frames_have_the_python_bytes() {
    assert_child_bytes(
        &ChildFrame::BootFailed(BootFailedFrame {
            cause: ReportedBootCause::ResourceShortage(ShortageErrno::Emfile),
            error_name: BootErrorName::new("OSError".to_owned()).unwrap(),
            detail: BootDetail::new("[Errno 24] Too many open files".to_owned()).unwrap(),
        }),
        &hex(PY_BOOT_FAILED_SHORTAGE),
    );
    assert_child_bytes(
        &ChildFrame::BootFailed(BootFailedFrame {
            cause: ReportedBootCause::AppLoadFailed,
            error_name: BootErrorName::new("ImportError".to_owned()).unwrap(),
            detail: BootDetail::new("no module named app".to_owned()).unwrap(),
        }),
        &hex(PY_BOOT_FAILED_APP_LOAD),
    );
}

#[test]
fn left_work_reports_have_the_python_bytes() {
    assert_child_bytes(
        &ChildFrame::LeftWorkEnded(LeftWorkEndedFrame {
            task_id: task_id(T1),
            end: LeftWorkEnd::Cancelled,
            rss: rss(41.5),
        }),
        &hex(PY_LEFT_WORK_ENDED),
    );
    assert_child_bytes(
        &ChildFrame::LeftWorkEnded(LeftWorkEndedFrame {
            task_id: task_id(T1),
            end: LeftWorkEnd::InGrace,
            rss: RssReading::Unreadable,
        }),
        &hex(PY_LEFT_WORK_ENDED_RSS_UNREADABLE),
    );
    let job_names: Vec<String> = (0..10).map(|at| format!("job_{at}")).collect();
    let job_names: Vec<&str> = job_names.iter().map(String::as_str).collect();
    assert_child_bytes(
        &ChildFrame::LeftWorkStuck(LeftWorkStuckFrame {
            task_id: task_id(T1),
            still: LoopHoldsTasks::new(NonZeroU64::new(12).unwrap(), names(&job_names)).unwrap(),
        }),
        &hex(PY_LEFT_WORK_STUCK),
    );
}

#[test]
fn run_reports_have_the_python_bytes() {
    assert_child_bytes(
        &ChildFrame::DeadlineFired(run_ref(7, T2)),
        &hex(PY_DEADLINE_FIRED),
    );
    assert_child_bytes(
        &ChildFrame::SecondCancel(run_ref(7, T2)),
        &hex(PY_SECOND_CANCEL),
    );
    assert_child_bytes(
        &ChildFrame::RunStuck(RunStuckFrame {
            run: run_ref(3, T3),
            work: StuckWork::new(2, 1, names(&["main", "helper"])).unwrap(),
        }),
        &hex(PY_RUN_STUCK),
    );
}

#[test]
fn loop_blocked_frames_have_the_python_bytes() {
    assert_child_bytes(
        &ChildFrame::LoopBlocked(LoopBlockedFrame {
            culprit: LoopCulprit::Run(run_ref(4, T4)),
            blocked_ms: 5012,
            location: BlockedWhere::new("app.py:12 in spin".to_owned()).unwrap(),
        }),
        &hex(PY_LOOP_BLOCKED_BY_RUN),
    );
    assert_child_bytes(
        &ChildFrame::LoopBlocked(LoopBlockedFrame {
            culprit: LoopCulprit::Unknown,
            blocked_ms: 0,
            location: BlockedWhere::new(String::new()).unwrap(),
        }),
        &hex(PY_LOOP_BLOCKED_UNKNOWN),
    );
}

/// Non-ASCII text stays UTF-8, and quotes and line feeds are escaped as
/// Python's `json.dumps(ensure_ascii=False)` escapes them.
#[test]
fn text_is_escaped_as_python_escapes_it() {
    assert_child_bytes(
        &ChildFrame::LoopBlocked(LoopBlockedFrame {
            culprit: LoopCulprit::Unknown,
            blocked_ms: 1,
            location: BlockedWhere::new("\u{e4}\u{f6} \"q\"\n".to_owned()).unwrap(),
        }),
        &hex(PY_LOOP_BLOCKED_ESCAPED_TEXT),
    );
}

#[test]
fn preload_frames_have_the_python_bytes() {
    assert_preload_bytes(
        &PreloadFrame::PreloadReady {
            pid: ProcessId::new(4242).unwrap(),
        },
        &hex(PY_PRELOAD_READY),
    );
    assert_preload_bytes(
        &PreloadFrame::ChildForked {
            pid: ProcessId::new(4243).unwrap(),
        },
        &hex(PY_CHILD_FORKED),
    );
    assert_preload_bytes(
        &PreloadFrame::ForkFailed {
            reason: PreloadReason::new("fork: EAGAIN".to_owned()).unwrap(),
        },
        &hex(PY_FORK_FAILED),
    );
    assert_preload_bytes(
        &PreloadFrame::PreloadRefused {
            reason: PreloadReason::new("threads running: 2".to_owned()).unwrap(),
        },
        &hex(PY_PRELOAD_REFUSED),
    );
}

// ---------------------------------------------------------------------------
// Version check
// ---------------------------------------------------------------------------

#[test]
fn a_version_one_ready_frame_is_a_protocol_mismatch() {
    assert_eq!(
        decode_child_bytes(&hex(PY_V1_READY)),
        Err(ChildFrameRefusal::ProtocolMismatch(ProtocolMismatch {
            received: ReceivedProtocol::NotNamed,
        })),
    );
}

#[test]
fn a_version_one_init_frame_is_a_protocol_mismatch() {
    assert_eq!(
        decode_parent_bytes(&hex(PY_V1_INIT)),
        Err(ParentFrameRefusal::ProtocolMismatch(ProtocolMismatch {
            received: ReceivedProtocol::NotNamed,
        })),
    );
}

/// The version is read before any other field, so a later version with
/// fields this build does not know is still named as a mismatch.
#[test]
fn another_version_is_named_before_the_fields_are_read() {
    let init = framed(
        r#"{"kind":"init","protocol":5,"a_field_of_version_5":true}"#,
        b"",
    );
    assert_eq!(
        decode_parent_bytes(&init),
        Err(ParentFrameRefusal::ProtocolMismatch(ProtocolMismatch {
            received: ReceivedProtocol::Named(5),
        })),
    );
    let ready = framed(r#"{"kind":"ready","protocol":1,"pid":5}"#, b"");
    assert_eq!(
        decode_child_bytes(&ready),
        Err(ChildFrameRefusal::ProtocolMismatch(ProtocolMismatch {
            received: ReceivedProtocol::Named(1),
        })),
    );
}

#[test]
fn a_protocol_that_is_not_a_whole_number_is_malformed() {
    let ready = framed(
        r#"{"kind":"ready","protocol":"2","pid":5,"rss_mb":null}"#,
        b"",
    );
    assert_eq!(
        malformed_child(&ready),
        MalformedFrame::WrongType {
            field: "protocol".to_owned(),
            expected: ExpectedType::WholeNumber,
        },
    );
}

// ---------------------------------------------------------------------------
// Version-3 content, pinned
// ---------------------------------------------------------------------------

#[test]
fn the_ready_frame_names_the_protocol() {
    assert_child_bytes(
        &ChildFrame::Ready(ReadyFrame {
            pid: ProcessId::new(4242).unwrap(),
            rss: rss(27.25),
        }),
        &framed(
            r#"{"kind":"ready","protocol":4,"pid":4242,"rss_mb":27.25}"#,
            b"",
        ),
    );
}

#[test]
fn the_init_frame_of_a_rust_child() {
    assert_parent_bytes(
        &ParentFrame::Init(InitFrame {
            program: ChildProgram::Rust,
            log_level: ChildLogLevel::Info,
            database_url: DatabaseUrl::new("postgresql://u@h/db".to_owned()),
            connection: ChildConnection::default(),
            process_name: "horsies-child".to_owned(),
            child_kind: isolated_kind(),
        }),
        &framed(
            concat!(
                r#"{"kind":"init","protocol":4,"program":{"kind":"rust"},"log_level":"info","#,
                r#""database_url":"postgresql://u@h/db","#,
                r#""connection":{"tcp_keepalives":{"kind":"on","idle_seconds":30,"#,
                r#""interval_seconds":10,"probes":3},"tcp_user_timeout":{"kind":"after","#,
                r#""milliseconds":30000},"server_sessions":{"kind":"kept"}},"#,
                r#""process_name":"horsies-child","#,
                r#""child_kind":{"kind":"isolated","pool":{"min_size":0,"max_size":1,"#,
                r#""check":true},"task_send_pool":2}}"#,
            ),
            b"",
        ),
    );
}

#[test]
fn the_init_frame_of_a_python_child() {
    assert_parent_bytes(
        &ParentFrame::Init(InitFrame {
            program: ChildProgram::Python(PythonProgram {
                app_locator: "app:app".to_owned(),
                imports: vec!["app.tasks".to_owned()],
                sys_path_roots: vec!["/srv".to_owned()],
                parent_sys_path: vec!["/srv".to_owned(), "/usr/lib".to_owned()],
                parent_argv: vec!["horsies".to_owned(), "worker".to_owned()],
            }),
            log_level: ChildLogLevel::Warning,
            database_url: DatabaseUrl::new("postgresql://u@h/db".to_owned()),
            connection: ChildConnection::default(),
            process_name: "horsies-child".to_owned(),
            child_kind: ChildKind::SharedAsync(SharedAsyncChild {
                seats: AsyncSeats::new(100).unwrap(),
                pool: pool(),
                executor_threads: ExecutorThreads::new(32).unwrap(),
                task_send_pool: TaskSendPool::new(2).unwrap(),
            }),
        }),
        &framed(
            concat!(
                r#"{"kind":"init","protocol":4,"program":{"kind":"python","#,
                r#""app_locator":"app:app","imports":["app.tasks"],"sys_path_roots":["/srv"],"#,
                r#""parent_sys_path":["/srv","/usr/lib"],"parent_argv":["horsies","worker"]},"#,
                r#""log_level":"warning","database_url":"postgresql://u@h/db","#,
                r#""connection":{"tcp_keepalives":{"kind":"on","idle_seconds":30,"#,
                r#""interval_seconds":10,"probes":3},"tcp_user_timeout":{"kind":"after","#,
                r#""milliseconds":30000},"server_sessions":{"kind":"kept"}},"#,
                r#""process_name":"horsies-child","child_kind":{"kind":"shared_async","#,
                r#""seats":100,"pool":{"min_size":0,"max_size":1,"check":true},"#,
                r#""executor_threads":32,"task_send_pool":2}}"#,
            ),
            b"",
        ),
    );
}

/// Keepalives off, the system's TCP user timeout and PgBouncer transaction
/// mode, with the session URL of the parent's config.
fn transaction_mode_connection() -> ChildConnection {
    ChildConnection {
        tcp_keepalives: TcpKeepalivesConfig::Off,
        tcp_user_timeout: TcpUserTimeoutConfig::SystemDefault,
        server_sessions: ServerSessions::PgBouncerTransactionMode {
            session_database_url: DatabaseUrl::new("postgresql://u@direct/db".to_owned()),
        },
    }
}

fn init_with(connection: ChildConnection) -> InitFrame {
    InitFrame {
        program: ChildProgram::Rust,
        log_level: ChildLogLevel::Info,
        database_url: DatabaseUrl::new("postgresql://u@pooler/db".to_owned()),
        connection,
        process_name: "horsies-child".to_owned(),
        child_kind: isolated_kind(),
    }
}

#[test]
fn the_init_frame_carries_keepalives_off_and_transaction_mode() {
    assert_parent_bytes(
        &ParentFrame::Init(init_with(transaction_mode_connection())),
        &framed(
            concat!(
                r#"{"kind":"init","protocol":4,"program":{"kind":"rust"},"log_level":"info","#,
                r#""database_url":"postgresql://u@pooler/db","#,
                r#""connection":{"tcp_keepalives":{"kind":"off"},"#,
                r#""tcp_user_timeout":{"kind":"system_default"},"#,
                r#""server_sessions":{"kind":"pgbouncer_transaction_mode","#,
                r#""session_database_url":"postgresql://u@direct/db"}},"#,
                r#""process_name":"horsies-child","#,
                r#""child_kind":{"kind":"isolated","pool":{"min_size":0,"max_size":1,"#,
                r#""check":true},"task_send_pool":2}}"#,
            ),
            b"",
        ),
    );
}

#[test]
fn the_init_frame_carries_keepalive_values_and_a_user_timeout() {
    let connection = ChildConnection {
        tcp_keepalives: TcpKeepalivesConfig::On {
            idle_seconds: KeepaliveSeconds::try_from(45_u32).unwrap(),
            interval_seconds: KeepaliveSeconds::try_from(7_u32).unwrap(),
            probes: KeepaliveProbes::try_from(5_u32).unwrap(),
        },
        tcp_user_timeout: TcpUserTimeoutConfig::After {
            milliseconds: TcpUserTimeoutMs::try_from(1500_u32).unwrap(),
        },
        server_sessions: ServerSessions::Kept,
    };
    let bytes = encode_parent_frame(&ParentFrame::Init(init_with(connection.clone())))
        .unwrap()
        .to_bytes();
    let text = String::from_utf8_lossy(&bytes);
    assert!(
        text.contains(concat!(
            r#""connection":{"tcp_keepalives":{"kind":"on","idle_seconds":45,"#,
            r#""interval_seconds":7,"probes":5},"tcp_user_timeout":{"kind":"after","#,
            r#""milliseconds":1500},"server_sessions":{"kind":"kept"}}"#,
        )),
        "{text}"
    );
    match decode_parent_bytes(&bytes) {
        Ok(ParentFrame::Init(init)) => assert_eq!(init.connection, connection),
        other => panic!("expected the init frame, got {other:?}"),
    }
}

/// An init frame whose `connection` object is `connection`.
fn init_bytes_with_connection(connection: &str) -> Vec<u8> {
    framed(
        &format!(
            concat!(
                r#"{{"kind":"init","protocol":4,"program":{{"kind":"rust"}},"#,
                r#""log_level":"info","database_url":"postgresql://u@h/db","#,
                r#""connection":{},"process_name":"c","#,
                r#""child_kind":{{"kind":"isolated","pool":{{"min_size":0,"max_size":1,"#,
                r#""check":true}},"task_send_pool":2}}}}"#,
            ),
            connection
        ),
        b"",
    )
}

#[test]
fn a_keepalive_value_outside_its_bounds_is_refused() {
    for (key, connection) in [
        (
            "connection.tcp_keepalives.probes",
            r#"{"tcp_keepalives":{"kind":"on","idle_seconds":30,"interval_seconds":10,"probes":0},"tcp_user_timeout":{"kind":"system_default"},"server_sessions":{"kind":"kept"}}"#,
        ),
        (
            "connection.tcp_keepalives.idle_seconds",
            r#"{"tcp_keepalives":{"kind":"on","idle_seconds":32768,"interval_seconds":10,"probes":3},"tcp_user_timeout":{"kind":"system_default"},"server_sessions":{"kind":"kept"}}"#,
        ),
        (
            "connection.tcp_user_timeout.milliseconds",
            r#"{"tcp_keepalives":{"kind":"off"},"tcp_user_timeout":{"kind":"after","milliseconds":0},"server_sessions":{"kind":"kept"}}"#,
        ),
        (
            "connection.tcp_user_timeout.milliseconds",
            r#"{"tcp_keepalives":{"kind":"off"},"tcp_user_timeout":{"kind":"after","milliseconds":4294967296},"server_sessions":{"kind":"kept"}}"#,
        ),
    ] {
        assert_eq!(
            malformed_parent(&init_bytes_with_connection(connection)),
            MalformedFrame::OutOfBounds {
                field: key.to_owned(),
            },
            "{connection}"
        );
    }
}

#[test]
fn an_init_frame_without_its_connection_settings_is_refused() {
    let init = framed(
        concat!(
            r#"{"kind":"init","protocol":4,"program":{"kind":"rust"},"log_level":"info","#,
            r#""database_url":"postgresql://u@h/db","process_name":"c","#,
            r#""child_kind":{"kind":"isolated","pool":{"min_size":0,"max_size":1,"#,
            r#""check":true},"task_send_pool":2}}"#,
        ),
        b"",
    );
    assert_eq!(
        malformed_parent(&init),
        MalformedFrame::MissingField {
            field: "connection".to_owned(),
        },
    );
}

#[test]
fn transaction_mode_without_its_session_url_is_refused() {
    let connection = concat!(
        r#"{"tcp_keepalives":{"kind":"off"},"tcp_user_timeout":{"kind":"system_default"},"#,
        r#""server_sessions":{"kind":"pgbouncer_transaction_mode"}}"#,
    );
    assert_eq!(
        malformed_parent(&init_bytes_with_connection(connection)),
        MalformedFrame::MissingField {
            field: "connection.server_sessions.session_database_url".to_owned(),
        },
    );
}

#[test]
fn the_connection_of_a_config_is_its_tcp_settings_and_server_sessions() {
    assert_eq!(
        ChildConnection::of_config(&PostgresConfig::from_database_url("postgresql://u@h/db")),
        Ok(ChildConnection::default()),
    );
    let mut config =
        PostgresConfig::from_pgbouncer_urls("postgresql://u@pooler/db", "postgresql://u@direct/db");
    config.tcp_keepalives = crate::core::config::TcpKeepalivesConfig::Off;
    config.tcp_user_timeout = crate::core::config::TcpUserTimeoutConfig::SystemDefault;
    assert_eq!(
        ChildConnection::of_config(&config),
        Ok(transaction_mode_connection()),
    );
}

#[test]
fn a_config_that_does_not_validate_has_no_child_connection() {
    let mut config = PostgresConfig::from_database_url("postgresql://u@pooler/db");
    config.pgbouncer_transaction_mode = true;
    assert_eq!(
        ChildConnection::of_config(&config),
        Err(PostgresConfigError::MissingSessionDatabase),
    );
}

#[test]
fn the_isolated_run_frame_carries_the_input_as_its_body() {
    assert_parent_bytes(
        &ParentFrame::Run(RunFrame {
            task_id: task_id(T5),
            task_name: "app.tasks.add".to_owned(),
            queue_name: default_queue(),
            attempt: AttemptNumber::SMALLEST,
            max_retries: crate::events::MaxRetries::SMALLEST,
            worker_id: "worker-1".to_owned(),
            claimed_at: claim_time(),
            orphan_self_heal: OrphanSelfHeal::On,
            work: IsolatedWork::Run,
            trace: crate::events::RunTrace::NoCarrier,
            input: TaskInputText::new(r#"{"a":1,"b":2}"#.to_owned()),
        }),
        &framed(
            concat!(
                r#"{"kind":"run","task_id":"0190f5a2-7b1c-7d3e-8f00-000000000005","#,
                r#""task_name":"app.tasks.add","queue_name":"default","attempt":1,"#,
                r#""worker_id":"worker-1","#,
                r#""claimed_at":"2026-11-16T09:30:00.123456Z","orphan_self_heal":true,"#,
                r#""work":{"kind":"run"},"#,
                r#""trace":{"kind":"no_carrier"}}"#,
            ),
            br#"{"a":1,"b":2}"#,
        ),
    );
}

#[test]
fn the_isolated_run_frame_carries_a_refusal_of_stored_options() {
    assert_parent_bytes(
        &ParentFrame::Run(RunFrame {
            task_id: task_id(T5),
            task_name: "app.tasks.add".to_owned(),
            queue_name: default_queue(),
            attempt: AttemptNumber::SMALLEST,
            max_retries: crate::events::MaxRetries::SMALLEST,
            worker_id: "worker-1".to_owned(),
            claimed_at: whole_second_claim_time(),
            orphan_self_heal: OrphanSelfHeal::Off,
            work: IsolatedWork::Refusal(StoredOptionsRefusal::TimeoutValueInvalid(
                RefusalReason::new("timeout_ms is below 1000".to_owned()).unwrap(),
            )),
            trace: crate::events::RunTrace::NoCarrier,
            input: TaskInputText::new("null".to_owned()),
        }),
        &framed(
            concat!(
                r#"{"kind":"run","task_id":"0190f5a2-7b1c-7d3e-8f00-000000000005","#,
                r#""task_name":"app.tasks.add","queue_name":"default","attempt":1,"#,
                r#""worker_id":"worker-1","#,
                r#""claimed_at":"2026-11-16T09:30:00Z","orphan_self_heal":false,"#,
                r#""work":{"kind":"refusal","#,
                r#""refusal":{"kind":"value_invalid","reason":"timeout_ms is below 1000"}},"#,
                r#""trace":{"kind":"no_carrier"}}"#,
            ),
            b"null",
        ),
    );
}

#[test]
fn the_shared_run_frame_carries_the_seat_work() {
    let frame = |work: SharedWork| {
        ParentFrame::SharedRun(SharedRunFrame {
            dispatch: DispatchNumber::new(9).unwrap(),
            task_id: task_id(T5),
            task_name: "app.tasks.fetch".to_owned(),
            queue_name: default_queue(),
            attempt: AttemptNumber::SMALLEST,
            max_retries: crate::events::MaxRetries::SMALLEST,
            worker_id: "worker-1".to_owned(),
            claimed_at: whole_second_claim_time(),
            orphan_self_heal: OrphanSelfHeal::Off,
            work,
            trace: crate::events::RunTrace::NoCarrier,
            input: TaskInputText::new("{}".to_owned()),
        })
    };
    let meta = |work: &str| {
        format!(
            concat!(
                r#"{{"kind":"shared_run","dispatch":9,"#,
                r#""task_id":"0190f5a2-7b1c-7d3e-8f00-000000000005","#,
                r#""task_name":"app.tasks.fetch","queue_name":"default","attempt":1,"#,
                r#""worker_id":"worker-1","#,
                r#""claimed_at":"2026-11-16T09:30:00Z","orphan_self_heal":false,"work":{},"#,
                r#""trace":{{"kind":"no_carrier"}}}}"#,
            ),
            work
        )
    };
    assert_parent_bytes(
        &frame(SharedWork::AsyncTask {
            timeout_ms: TaskTimeoutMs::new(30_000).unwrap(),
            cancel_grace_ms: CancelGraceMs::new(1_000).unwrap(),
        }),
        &framed(
            &meta(r#"{"kind":"task","timeout_ms":30000,"cancel_grace_ms":1000}"#),
            b"{}",
        ),
    );
    assert_parent_bytes(
        &frame(SharedWork::ThreadTask {
            timeout_ms: TaskTimeoutMs::new(5_000).unwrap(),
        }),
        &framed(&meta(r#"{"kind":"thread_task","timeout_ms":5000}"#), b"{}"),
    );
    assert_parent_bytes(
        &frame(SharedWork::Refusal(
            StoredOptionsRefusal::CancelGraceInvalid(
                RefusalReason::new("no cancel_grace_ms".to_owned()).unwrap(),
            ),
        )),
        &framed(
            &meta(
                r#"{"kind":"refusal","refusal":{"kind":"grace_invalid","reason":"no cancel_grace_ms"}}"#,
            ),
            b"{}",
        ),
    );
}

#[test]
fn the_result_frame_carries_a_closed_run_end() {
    let frame = |end: RunEnd,
                 started_at: crate::events::AttemptStart,
                 rss_reading: RssReading,
                 task_loop: NoLeftWork| {
        ChildFrame::Result(ResultFrame {
            task_id: task_id(T5),
            end,
            started_at,
            rss: rss_reading,
            task_loop,
            child: ChildAfterFrame::ServesOn,
        })
    };
    assert_child_bytes(
        &frame(
            RunEnd::Result(envelope(r#"{"ok":3}"#)),
            reported(),
            rss(30.5),
            NoLeftWork::NoTaskLoop,
        ),
        &framed(
            &format!(
                concat!(
                    r#"{{"kind":"result","task_id":"0190f5a2-7b1c-7d3e-8f00-000000000005","#,
                    r#""end":{{"kind":"result"}},"started_at":{},"rss_mb":30.5,"#,
                    r#""task_loop":{{"kind":"none"}},"child":"serves_on"}}"#,
                ),
                REPORTED
            ),
            br#"{"ok":3}"#,
        ),
    );
    assert_child_bytes(
        &frame(
            RunEnd::StoppedBeforeRunning(StopReason::ClaimLost),
            crate::events::AttemptStart::NotReported,
            RssReading::Unreadable,
            NoLeftWork::LoopHoldsNoTask,
        ),
        &framed(
            concat!(
                r#"{"kind":"result","task_id":"0190f5a2-7b1c-7d3e-8f00-000000000005","#,
                r#""end":{"kind":"stopped_before_running","reason":"CLAIM_LOST"},"#,
                r#""started_at":{"kind":"not_reported"},"#,
                r#""rss_mb":null,"task_loop":{"kind":"empty"},"child":"serves_on"}"#,
            ),
            b"",
        ),
    );
    assert_child_bytes(
        &frame(
            RunEnd::WorkerFailure(FailureDetail::new("encode failed".to_owned()).unwrap()),
            reported(),
            rss(31.0),
            NoLeftWork::NoTaskLoop,
        ),
        &framed(
            &format!(
                concat!(
                    r#"{{"kind":"result","task_id":"0190f5a2-7b1c-7d3e-8f00-000000000005","#,
                    r#""end":{{"kind":"worker_failure","detail":"encode failed"}},"#,
                    r#""started_at":{},"rss_mb":31.0,"#,
                    r#""task_loop":{{"kind":"none"}},"child":"serves_on"}}"#,
                ),
                REPORTED
            ),
            b"",
        ),
    );
}

#[test]
fn the_shared_result_frame_carries_a_seat_end() {
    assert_child_bytes(
        &ChildFrame::SharedResult(SharedResultFrame {
            run: run_ref(9, T5),
            end: SeatEnd::TimedOut,
            started_at: reported(),
            rss: rss(31.0),
            child: ChildAfterFrame::ServesOn,
        }),
        &framed(
            &format!(
                concat!(
                    r#"{{"kind":"shared_result","dispatch":9,"#,
                    r#""task_id":"0190f5a2-7b1c-7d3e-8f00-000000000005","#,
                    r#""end":{{"kind":"timed_out"}},"started_at":{},"rss_mb":31.0,"#,
                    r#""child":"serves_on"}}"#,
                ),
                REPORTED
            ),
            b"",
        ),
    );
    assert_child_bytes(
        &ChildFrame::SharedResult(SharedResultFrame {
            run: run_ref(9, T5),
            end: SeatEnd::Finished(RunEnd::Result(envelope("[1,2]"))),
            started_at: reported(),
            rss: RssReading::Unreadable,
            child: ChildAfterFrame::ServesOn,
        }),
        &framed(
            &format!(
                concat!(
                    r#"{{"kind":"shared_result","dispatch":9,"#,
                    r#""task_id":"0190f5a2-7b1c-7d3e-8f00-000000000005","#,
                    r#""end":{{"kind":"result"}},"started_at":{},"rss_mb":null,"#,
                    r#""child":"serves_on"}}"#,
                ),
                REPORTED
            ),
            b"[1,2]",
        ),
    );
}

/// A child whose core stopped says so in the frame that ends its run: the
/// release of a refused start, or the result of a body that finished.
#[test]
fn a_result_frame_names_a_child_that_ends_itself() {
    assert_child_bytes(
        &ChildFrame::Result(ResultFrame {
            task_id: task_id(T5),
            end: RunEnd::StoppedBeforeRunning(StopReason::OwnershipUnconfirmed),
            started_at: crate::events::AttemptStart::NotReported,
            rss: RssReading::Unreadable,
            task_loop: NoLeftWork::NoTaskLoop,
            child: ChildAfterFrame::EndsItself,
        }),
        &framed(
            concat!(
                r#"{"kind":"result","task_id":"0190f5a2-7b1c-7d3e-8f00-000000000005","#,
                r#""end":{"kind":"stopped_before_running","reason":"OWNERSHIP_UNCONFIRMED"},"#,
                r#""started_at":{"kind":"not_reported"},"#,
                r#""rss_mb":null,"task_loop":{"kind":"none"},"child":"ends_itself"}"#,
            ),
            b"",
        ),
    );
    assert_child_bytes(
        &ChildFrame::SharedResult(SharedResultFrame {
            run: run_ref(9, T5),
            end: SeatEnd::Finished(RunEnd::Result(envelope("[1,2]"))),
            started_at: reported(),
            rss: RssReading::Unreadable,
            child: ChildAfterFrame::EndsItself,
        }),
        &framed(
            &format!(
                concat!(
                    r#"{{"kind":"shared_result","dispatch":9,"#,
                    r#""task_id":"0190f5a2-7b1c-7d3e-8f00-000000000005","#,
                    r#""end":{{"kind":"result"}},"started_at":{},"rss_mb":null,"#,
                    r#""child":"ends_itself"}}"#,
                ),
                REPORTED
            ),
            b"[1,2]",
        ),
    );
}

/// A result frame without `child`, or with a value outside it, is refused:
/// the parent never guesses whether a child serves on.
#[test]
fn a_result_frame_without_a_known_child_value_is_refused() {
    let without = format!(
        r#"{{"kind":"result","task_id":"{T1}","end":{{"kind":"stopped_before_running","reason":"CLAIM_LOST"}},"started_at":{{"kind":"not_reported"}},"rss_mb":null,"task_loop":{{"kind":"none"}}}}"#
    );
    assert_eq!(
        malformed_child(&framed(&without, b"")),
        MalformedFrame::MissingField {
            field: "child".to_owned(),
        },
    );
    let unknown = format!(
        r#"{{"kind":"shared_result","dispatch":1,"task_id":"{T1}","end":{{"kind":"timed_out"}},"started_at":{{"kind":"not_reported"}},"rss_mb":null,"child":"retires"}}"#
    );
    assert_eq!(
        malformed_child(&framed(&unknown, b"")),
        MalformedFrame::BrokenRule {
            field: "child".to_owned(),
            rule: "is not one of serves_on, ends_itself",
        },
    );
}

#[test]
fn reports_after_a_return_carry_what_the_body_returned() {
    let returned = BodyEnd::Result(envelope(r#"{"ok":null}"#));
    let left = LoopHoldsTasks::new(NonZeroU64::new(1).unwrap(), names(&["poll"])).unwrap();
    assert_child_bytes(
        &ChildFrame::BodyReturned(BodyReturnedFrame {
            task_id: task_id(T5),
            returned: returned.clone(),
            started_at: start_time(),
            left,
        }),
        &framed(
            concat!(
                r#"{"kind":"body_returned","task_id":"0190f5a2-7b1c-7d3e-8f00-000000000005","#,
                r#""returned":{"kind":"result"},"started_at":"2026-11-16T09:30:01.250Z","#,
                r#""left":{"count":1,"names":["poll"]}}"#,
            ),
            br#"{"ok":null}"#,
        ),
    );
    assert_child_bytes(
        &ChildFrame::LeftoverCancel(LeftoverCancelFrame {
            run: run_ref(2, T5),
            returned: returned.clone(),
            started_at: start_time(),
        }),
        &framed(
            concat!(
                r#"{"kind":"leftover_cancel","dispatch":2,"#,
                r#""task_id":"0190f5a2-7b1c-7d3e-8f00-000000000005","#,
                r#""returned":{"kind":"result"},"started_at":"2026-11-16T09:30:01.250Z"}"#,
            ),
            br#"{"ok":null}"#,
        ),
    );
    assert_child_bytes(
        &ChildFrame::LeftoverStuck(LeftoverStuckFrame {
            run: run_ref(2, T5),
            work: StuckWork::new(0, 2, Vec::new()).unwrap(),
            returned: BodyEnd::WorkerFailure(FailureDetail::new("x".to_owned()).unwrap()),
            started_at: start_time(),
        }),
        &framed(
            concat!(
                r#"{"kind":"leftover_stuck","dispatch":2,"#,
                r#""task_id":"0190f5a2-7b1c-7d3e-8f00-000000000005","#,
                r#""tasks":0,"executor_items":2,"names":[],"#,
                r#""returned":{"kind":"worker_failure","detail":"x"},"#,
                r#""started_at":"2026-11-16T09:30:01.250Z"}"#,
            ),
            b"",
        ),
    );
}

#[test]
fn every_stop_reason_round_trips() {
    for reason in [
        StopReason::ClaimLost,
        StopReason::OwnershipUnconfirmed,
        StopReason::WorkflowStopped,
        StopReason::TaskExpired,
        StopReason::NodeNotLinked,
        StopReason::OrphanCancelled,
    ] {
        let frame = ChildFrame::SharedResult(SharedResultFrame {
            run: run_ref(1, T1),
            end: SeatEnd::Finished(RunEnd::StoppedBeforeRunning(reason)),
            started_at: crate::events::AttemptStart::NotReported,
            rss: RssReading::Unreadable,
            child: ChildAfterFrame::ServesOn,
        });
        let bytes = encode_child_frame(&frame).unwrap().to_bytes();
        assert_eq!(decode_child_bytes(&bytes).unwrap(), frame);
    }
}

/// The run end of a result frame whose end object is `end`, or the refusal
/// of the frame.
fn run_end_of(end: &str) -> Result<RunEnd, MalformedFrame> {
    let meta = format!(
        r#"{{"kind":"result","task_id":"{T1}","end":{end},"started_at":{{"kind":"not_reported"}},"rss_mb":null,"task_loop":{{"kind":"none"}},"child":"serves_on"}}"#
    );
    match decode_child_bytes(&framed(&meta, b"")) {
        Ok(ChildFrame::Result(frame)) => Ok(frame.end),
        Ok(other) => panic!("expected a result frame, got {other:?}"),
        Err(ChildFrameRefusal::Malformed(malformed)) => Err(malformed),
        Err(other) => panic!("expected a malformed frame, got {other:?}"),
    }
}

/// Each stop reason reads from its wire spelling, the one place that
/// compares the text. `NODE_NOT_LINKED` is the version-2 spelling of
/// Python's `WORKFLOW_CHECK_FAILED`.
#[test]
fn each_stop_reason_reads_from_its_wire_spelling() {
    for (spelling, reason) in [
        ("CLAIM_LOST", StopReason::ClaimLost),
        ("OWNERSHIP_UNCONFIRMED", StopReason::OwnershipUnconfirmed),
        ("WORKFLOW_STOPPED", StopReason::WorkflowStopped),
        ("TASK_EXPIRED", StopReason::TaskExpired),
        ("NODE_NOT_LINKED", StopReason::NodeNotLinked),
        ("ORPHAN_CANCELLED", StopReason::OrphanCancelled),
    ] {
        let end = format!(r#"{{"kind":"stopped_before_running","reason":"{spelling}"}}"#);
        assert_eq!(
            run_end_of(&end),
            Ok(RunEnd::StoppedBeforeRunning(reason)),
            "{spelling}"
        );
    }
}

/// Python `TestSentinelParsing::test_expired_sentinel_matches_the_code_the_child_writes`:
/// the expired stop travels as the spelling of the `TASK_EXPIRED` outcome
/// code, both ways, not as a parallel spelling.
#[test]
fn the_expired_stop_reason_is_spelled_as_the_task_expired_code() {
    let spelling = OutcomeCode::TaskExpired.to_string();
    let end = format!(r#"{{"kind":"stopped_before_running","reason":"{spelling}"}}"#);
    assert_eq!(
        run_end_of(&end),
        Ok(RunEnd::StoppedBeforeRunning(StopReason::TaskExpired))
    );
    let frame = ChildFrame::Result(ResultFrame {
        task_id: task_id(T1),
        end: RunEnd::StoppedBeforeRunning(StopReason::TaskExpired),
        started_at: crate::events::AttemptStart::NotReported,
        rss: RssReading::Unreadable,
        task_loop: NoLeftWork::NoTaskLoop,
        child: ChildAfterFrame::ServesOn,
    });
    let (meta, _body) = split(&encode_child_frame(&frame).unwrap().to_bytes());
    let meta = String::from_utf8(meta).unwrap();
    let reason = format!(r#""reason":"{spelling}""#);
    assert!(meta.contains(&reason), "{meta}");
}

/// Python `TestSentinelParsing::test_prose_is_not_a_sentinel`: the text of a
/// worker failure travels in its own end and stays text, also when it spells
/// a stop reason. A stop end whose reason is no stop reason is refused; it
/// is never read as a worker failure.
#[test]
fn a_worker_failure_keeps_its_text_and_an_unknown_stop_reason_is_refused() {
    for detail in ["Worker process crashed (no heartbeat)", "CLAIM_LOST"] {
        let frame = ChildFrame::Result(ResultFrame {
            task_id: task_id(T1),
            end: RunEnd::WorkerFailure(FailureDetail::new(detail.to_owned()).unwrap()),
            started_at: crate::events::AttemptStart::NotReported,
            rss: RssReading::Unreadable,
            task_loop: NoLeftWork::NoTaskLoop,
            child: ChildAfterFrame::ServesOn,
        });
        let bytes = encode_child_frame(&frame).unwrap().to_bytes();
        assert_eq!(decode_child_bytes(&bytes).unwrap(), frame, "{detail}");
    }
    assert_eq!(
        run_end_of(r#"{"kind":"stopped_before_running","reason":"Worker process crashed"}"#),
        Err(MalformedFrame::BrokenRule {
            field: "end.reason".to_owned(),
            rule: "is not a known stop reason",
        })
    );
}

/// Python `TestSentinelParsing::test_absent_reason_is_not_a_sentinel`: a stop
/// end without its reason is refused, so no stop reaches the worker without
/// one.
#[test]
fn a_stop_without_its_reason_is_refused() {
    assert_eq!(
        run_end_of(r#"{"kind":"stopped_before_running"}"#),
        Err(MalformedFrame::MissingField {
            field: "end.reason".to_owned(),
        })
    );
}

/// The claim generation keeps every digit, so the child fences on the exact
/// value the claim returned.
#[test]
fn the_claim_generation_round_trips_exactly() {
    let claimed_at = claim_time() + chrono::Duration::nanoseconds(789);
    let frame = ParentFrame::Run(RunFrame {
        task_id: task_id(T5),
        task_name: "t".to_owned(),
        queue_name: default_queue(),
        attempt: AttemptNumber::SMALLEST,
        max_retries: crate::events::MaxRetries::SMALLEST,
        worker_id: "w".to_owned(),
        claimed_at,
        orphan_self_heal: OrphanSelfHeal::Off,
        work: IsolatedWork::Run,
        trace: crate::events::RunTrace::NoCarrier,
        input: TaskInputText::new("{}".to_owned()),
    });
    let bytes = encode_parent_frame(&frame).unwrap().to_bytes();
    match decode_parent_bytes(&bytes).unwrap() {
        ParentFrame::Run(run) => assert_eq!(run.claimed_at, claimed_at),
        other => panic!("expected a run frame, got {other:?}"),
    }
}

/// The orphan self-heal of the app reaches each run: both run frames carry
/// it, on or off.
#[test]
fn the_orphan_self_heal_round_trips_in_both_run_frames() {
    for self_heal in [OrphanSelfHeal::On, OrphanSelfHeal::Off] {
        let run = ParentFrame::Run(RunFrame {
            task_id: task_id(T5),
            task_name: "t".to_owned(),
            queue_name: default_queue(),
            attempt: AttemptNumber::SMALLEST,
            max_retries: crate::events::MaxRetries::SMALLEST,
            worker_id: "w".to_owned(),
            claimed_at: claim_time(),
            orphan_self_heal: self_heal,
            work: IsolatedWork::Run,
            trace: crate::events::RunTrace::NoCarrier,
            input: TaskInputText::new("{}".to_owned()),
        });
        let shared = ParentFrame::SharedRun(SharedRunFrame {
            dispatch: DispatchNumber::new(3).unwrap(),
            task_id: task_id(T5),
            task_name: "t".to_owned(),
            queue_name: default_queue(),
            attempt: AttemptNumber::SMALLEST,
            max_retries: crate::events::MaxRetries::SMALLEST,
            worker_id: "w".to_owned(),
            claimed_at: claim_time(),
            orphan_self_heal: self_heal,
            work: SharedWork::ThreadTask {
                timeout_ms: TaskTimeoutMs::new(5_000).unwrap(),
            },
            trace: crate::events::RunTrace::NoCarrier,
            input: TaskInputText::new("{}".to_owned()),
        });
        for frame in [run, shared] {
            let bytes = encode_parent_frame(&frame).unwrap().to_bytes();
            assert_eq!(decode_parent_bytes(&bytes).unwrap(), frame);
        }
    }
}

/// The setting has no default: a run frame without it is refused.
#[test]
fn a_run_frame_without_its_orphan_self_heal_is_refused() {
    let run = format!(
        r#"{{"kind":"run","task_id":"{T1}","task_name":"t","queue_name":"default","attempt":1,"worker_id":"w","claimed_at":"2026-11-16T09:30:00Z","work":{{"kind":"run"}},"trace":{{"kind":"no_carrier"}}}}"#
    );
    assert_eq!(
        malformed_parent(&framed(&run, b"{}")),
        MalformedFrame::MissingField {
            field: "orphan_self_heal".to_owned(),
        },
    );
    let shared = format!(
        r#"{{"kind":"shared_run","dispatch":1,"task_id":"{T1}","task_name":"t","queue_name":"default","attempt":1,"worker_id":"w","claimed_at":"2026-11-16T09:30:00Z","work":{{"kind":"thread_task","timeout_ms":5000}},"trace":{{"kind":"no_carrier"}}}}"#
    );
    assert_eq!(
        malformed_parent(&framed(&shared, b"{}")),
        MalformedFrame::MissingField {
            field: "orphan_self_heal".to_owned(),
        },
    );
}

/// Python writes `isoformat()`, with `+00:00`; the decoder reads it too.
#[test]
fn a_claim_generation_with_an_offset_is_read() {
    let run = framed(
        concat!(
            r#"{"kind":"run","task_id":"0190f5a2-7b1c-7d3e-8f00-000000000005","#,
            r#""task_name":"t","queue_name":"default","attempt":1,"worker_id":"w","#,
            r#""claimed_at":"2026-11-16T09:30:00.123456+00:00","orphan_self_heal":false,"#,
            r#""work":{"kind":"run"},"#,
            r#""trace":{"kind":"no_carrier"}}"#,
        ),
        b"{}",
    );
    match decode_parent_bytes(&run).unwrap() {
        ParentFrame::Run(run) => assert_eq!(run.claimed_at, claim_time()),
        other => panic!("expected a run frame, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

#[test]
fn an_unknown_kind_is_malformed() {
    assert_eq!(
        malformed_child(&framed(r#"{"kind":"dance"}"#, b"")),
        MalformedFrame::UnknownKind {
            field: "kind".to_owned(),
            kind: BoundedText::cut("dance"),
        },
    );
}

#[test]
fn a_missing_field_is_named() {
    assert_eq!(
        malformed_child(&framed(r#"{"kind":"deadline_fired","dispatch":1}"#, b"")),
        MalformedFrame::MissingField {
            field: "task_id".to_owned(),
        },
    );
}

#[test]
fn a_field_outside_the_frame_is_refused() {
    let meta = format!(r#"{{"kind":"deadline_fired","dispatch":1,"task_id":"{T1}","x":1}}"#);
    assert_eq!(
        malformed_child(&framed(&meta, b"")),
        MalformedFrame::UnknownField {
            field: "x".to_owned(),
        },
    );
}

#[test]
fn a_nested_field_is_named_with_its_path() {
    let meta = format!(
        r#"{{"kind":"loop_blocked","culprit":{{"kind":"run","dispatch":0,"task_id":"{T1}"}},"blocked_ms":1,"where":""}}"#
    );
    assert_eq!(
        malformed_child(&framed(&meta, b"")),
        MalformedFrame::OutOfBounds {
            field: "culprit.dispatch".to_owned(),
        },
    );
}

#[test]
fn a_repeated_key_is_refused() {
    assert!(matches!(
        malformed_parent(&framed(r#"{"kind":"stop","kind":"stop"}"#, b"")),
        MalformedFrame::MetadataNotJson(StrictJsonError::DuplicateKey { .. }),
    ));
}

#[test]
fn metadata_that_is_not_an_object_is_refused() {
    assert_eq!(
        malformed_parent(&framed("[1,2]", b"")),
        MalformedFrame::MetadataNotObject,
    );
}

#[test]
fn a_body_on_a_frame_without_one_is_refused() {
    assert_eq!(
        malformed_parent(&framed(r#"{"kind":"stop"}"#, b"x")),
        MalformedFrame::UnexpectedBody {
            kind: "stop",
            body_bytes: 1,
        },
    );
    let stopped = format!(
        r#"{{"kind":"result","task_id":"{T1}","end":{{"kind":"stopped_before_running","reason":"CLAIM_LOST"}},"rss_mb":null,"task_loop":{{"kind":"none"}},"child":"serves_on"}}"#
    );
    assert_eq!(
        malformed_child(&framed(&stopped, b"{}")),
        MalformedFrame::UnexpectedBody {
            kind: "stopped_before_running",
            body_bytes: 2,
        },
    );
}

#[test]
fn input_that_is_not_utf8_is_refused() {
    let meta = format!(
        r#"{{"kind":"run","task_id":"{T1}","task_name":"t","queue_name":"default","attempt":1,"worker_id":"w","claimed_at":"2026-11-16T09:30:00Z","orphan_self_heal":false,"work":{{"kind":"run"}},"trace":{{"kind":"no_carrier"}}}}"#
    );
    assert_eq!(
        malformed_parent(&framed(&meta, &[0x7b, 0xff])),
        MalformedFrame::BodyNotUtf8 { valid_up_to: 1 },
    );
}

#[test]
fn a_child_cannot_report_a_spawn_failure() {
    let meta =
        r#"{"kind":"boot_failed","cause":{"kind":"spawn_failed"},"error_name":"E","detail":"d"}"#;
    assert_eq!(
        malformed_child(&framed(meta, b"")),
        MalformedFrame::BrokenRule {
            field: "cause.kind".to_owned(),
            rule: "a child cannot report a spawn failure",
        },
    );
}

#[test]
fn a_boot_failure_text_over_its_bound_is_refused() {
    let meta = format!(
        r#"{{"kind":"boot_failed","cause":{{"kind":"app_load_failed"}},"error_name":"{}","detail":"d"}}"#,
        "E".repeat(MAX_BOOT_ERROR_NAME_CHARS + 1),
    );
    assert_eq!(
        malformed_child(&framed(&meta, b"")),
        MalformedFrame::OutOfBounds {
            field: "error_name".to_owned(),
        },
    );
}

#[test]
fn a_leftover_report_names_min_count_ten_tasks() {
    let meta = format!(
        r#"{{"kind":"left_work_stuck","task_id":"{T1}","still":{{"count":12,"names":["a","b","c"]}}}}"#
    );
    assert_eq!(
        malformed_child(&framed(&meta, b"")),
        MalformedFrame::BrokenRule {
            field: "still.names".to_owned(),
            rule: "must name min(count, 10) tasks",
        },
    );
    let zero = format!(
        r#"{{"kind":"left_work_stuck","task_id":"{T1}","still":{{"count":0,"names":[]}}}}"#
    );
    assert_eq!(
        malformed_child(&framed(&zero, b"")),
        MalformedFrame::OutOfBounds {
            field: "still.count".to_owned(),
        },
    );
}

#[test]
fn a_stuck_report_names_live_work() {
    let meta = format!(
        r#"{{"kind":"run_stuck","dispatch":1,"task_id":"{T1}","tasks":0,"executor_items":0,"names":[]}}"#
    );
    assert!(matches!(
        malformed_child(&framed(&meta, b"")),
        MalformedFrame::BrokenRule { .. },
    ));
    assert_eq!(
        StuckWork::new(0, 0, Vec::new()),
        Err(StuckWorkInvalid::NothingAlive)
    );
}

#[test]
fn seat_work_outside_its_bounds_is_refused() {
    let meta = format!(
        r#"{{"kind":"shared_run","dispatch":1,"task_id":"{T1}","task_name":"t","queue_name":"default","attempt":1,"worker_id":"w","claimed_at":"2026-11-16T09:30:00Z","orphan_self_heal":false,"work":{{"kind":"task","timeout_ms":999,"cancel_grace_ms":1000}},"trace":{{"kind":"no_carrier"}}}}"#
    );
    assert_eq!(
        malformed_parent(&framed(&meta, b"{}")),
        MalformedFrame::OutOfBounds {
            field: "work.timeout_ms".to_owned(),
        },
    );
}

#[test]
fn a_negative_or_fractional_number_is_refused() {
    let negative = framed(
        r#"{"kind":"ready","protocol":4,"pid":-1,"rss_mb":null}"#,
        b"",
    );
    assert_eq!(
        malformed_child(&negative),
        MalformedFrame::OutOfBounds {
            field: "pid".to_owned(),
        },
    );
    let fraction = framed(
        r#"{"kind":"ready","protocol":4,"pid":1.5,"rss_mb":null}"#,
        b"",
    );
    assert_eq!(
        malformed_child(&fraction),
        MalformedFrame::WrongType {
            field: "pid".to_owned(),
            expected: ExpectedType::WholeNumber,
        },
    );
    let negative_rss = framed(
        r#"{"kind":"ready","protocol":4,"pid":1,"rss_mb":-1.0}"#,
        b"",
    );
    assert_eq!(
        malformed_child(&negative_rss),
        MalformedFrame::OutOfBounds {
            field: "rss_mb".to_owned(),
        },
    );
}

#[test]
fn a_preload_frame_carries_no_body() {
    let (meta, _) = split(&hex(PY_PRELOAD_READY));
    assert_eq!(
        decode_preload_frame(&meta, b"x"),
        Err(MalformedFrame::UnexpectedBody {
            kind: "preload",
            body_bytes: 1,
        }),
    );
}

// ---------------------------------------------------------------------------
// Encoding bounds and value types
// ---------------------------------------------------------------------------

#[test]
fn metadata_over_its_bound_is_not_sent() {
    let long_argv = vec!["a".repeat(1024); 1100];
    let frame = ParentFrame::Init(InitFrame {
        program: ChildProgram::Python(PythonProgram {
            app_locator: "app:app".to_owned(),
            imports: Vec::new(),
            sys_path_roots: Vec::new(),
            parent_sys_path: Vec::new(),
            parent_argv: long_argv,
        }),
        log_level: ChildLogLevel::Info,
        database_url: DatabaseUrl::new("postgresql://u@h/db".to_owned()),
        connection: ChildConnection::default(),
        process_name: "c".to_owned(),
        child_kind: isolated_kind(),
    });
    assert!(matches!(
        encode_parent_frame(&frame),
        Err(FrameEncodeError::MetadataOverBound { .. }),
    ));
}

#[test]
fn the_debug_form_of_the_database_url_hides_the_password() {
    let url = DatabaseUrl::new("postgresql://user:secret@host/db".to_owned());
    let shown = format!("{url:?}");
    assert!(!shown.contains("secret"), "{shown}");
}

#[test]
fn text_bounds_count_characters_not_bytes() {
    let two_byte_chars = "\u{e4}".repeat(MAX_LEFTOVER_NAME_CHARS);
    assert!(LeftoverName::new(two_byte_chars.clone()).is_ok());
    assert_eq!(
        LeftoverName::new(format!("{two_byte_chars}x")),
        Err(TextOverBound {
            chars: MAX_LEFTOVER_NAME_CHARS + 1,
            bound: MAX_LEFTOVER_NAME_CHARS,
        }),
    );
    assert_eq!(
        LeftoverName::cut(&format!("{two_byte_chars}x")).as_str(),
        two_byte_chars
    );
}

#[test]
fn value_types_hold_their_bounds() {
    assert!(ProcessId::new(0).is_none());
    assert!(ProcessId::new(u32::MAX).is_none());
    assert_eq!(ProcessId::new(42).unwrap().as_pid_t(), 42);
    assert!(DispatchNumber::new(0).is_none());
    assert!(RssMb::new(f64::NAN).is_none());
    assert!(RssMb::new(-0.5).is_none());
    assert!(TaskTimeoutMs::new(999).is_none());
    assert!(TaskTimeoutMs::new(4_294_967_295).is_some());
    assert!(TaskTimeoutMs::new(4_294_967_296).is_none());
    assert!(CancelGraceMs::new(99).is_none());
    assert!(CancelGraceMs::new(10_001).is_none());
    assert!(AsyncSeats::new(1_001).is_none());
    assert!(ThreadSeats::new(257).is_none());
    assert!(PoolMinSize::new(0).is_some());
    assert!(PoolMaxSize::new(0).is_none());
}

#[test]
fn exit_codes_are_distinct_and_leave_the_worker_code_free() {
    let mut codes: Vec<i32> = ChildExitCode::ALL.iter().map(|code| code.code()).collect();
    codes.sort_unstable();
    codes.dedup();
    assert_eq!(codes.len(), ChildExitCode::ALL.len());
    assert!(!codes.contains(&crate::worker::runner_beats::RUNNER_HEARTBEAT_LOST_EXIT_CODE));
    assert!(!codes.contains(&crate::worker::liveness::LIVENESS_LOCK_LOST_EXIT_CODE));
    assert_ne!(
        crate::worker::liveness::LIVENESS_LOCK_LOST_EXIT_CODE,
        crate::worker::runner_beats::RUNNER_HEARTBEAT_LOST_EXIT_CODE
    );
    for code in ChildExitCode::ALL {
        assert_eq!(ChildExitCode::from_code(code.code()), Some(code));
    }
    assert_eq!(ChildExitCode::from_code(0), None);
    assert_eq!(ChildExitCode::from_code(1), None);
}

/// Python `test_preload.py::TestChildExitCodes::test_every_named_exit_code_is_distinct`,
/// the part of the worker process: the codes that it exits with when it
/// fails closed are not the codes of the `horsies worker` command (0
/// stopped, 1 failed, 2 a usage error), so a reader tells the cause from the
/// code.
#[test]
fn the_fail_closed_codes_of_the_worker_process_are_not_command_codes() {
    const COMMAND_CODES: [i32; 3] = [0, 1, 2];
    for code in [
        crate::worker::runner_beats::RUNNER_HEARTBEAT_LOST_EXIT_CODE,
        crate::worker::liveness::LIVENESS_LOCK_LOST_EXIT_CODE,
    ] {
        assert!(!COMMAND_CODES.contains(&code), "{code}");
    }
}

/// The code of a child whose core stopped is a code of the protocol, apart
/// from the codes of the worker process.
#[test]
fn the_core_stopped_code_is_a_named_child_code() {
    assert_eq!(super::CORE_STOPPED_EXIT_CODE, 80);
    assert_eq!(
        ChildExitCode::from_code(super::CORE_STOPPED_EXIT_CODE),
        Some(ChildExitCode::CoreStopped)
    );
    assert_eq!(
        ChildExitCode::CoreStopped.code(),
        super::CORE_STOPPED_EXIT_CODE
    );
}

// ---------------------------------------------------------------------------
// The step parser and the read plan
// ---------------------------------------------------------------------------

/// A frame with `meta_bytes` of metadata and `body_bytes` of body. The
/// parser does not decode the metadata, so its bytes need not be JSON.
fn sized_frame(meta_bytes: usize, body_bytes: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&u32::try_from(meta_bytes).unwrap().to_be_bytes());
    bytes.extend_from_slice(&u64::try_from(body_bytes).unwrap().to_be_bytes());
    bytes.extend(std::iter::repeat_n(b'm', meta_bytes));
    bytes.extend(std::iter::repeat_n(b'b', body_bytes));
    bytes
}

#[test]
fn a_split_header_needs_the_whole_header() {
    let frame = sized_frame(10, 5);
    for at_hand in 0..HEADER_BYTES {
        assert_eq!(
            next_step(&frame, 0, at_hand),
            FrameStep::NeedBytes {
                wanted: HEADER_BYTES
            }
        );
    }
    assert_eq!(
        next_step(&frame, 0, HEADER_BYTES),
        FrameStep::NeedBytes {
            wanted: frame.len()
        }
    );
}

#[test]
fn a_whole_frame_names_where_its_parts_are() {
    let start = 100;
    let mut buffer = vec![b'x'; start];
    buffer.extend(sized_frame(10, 5));
    assert_eq!(
        next_step(&buffer, start, buffer.len()),
        FrameStep::WholeFrame {
            meta_at: start + HEADER_BYTES,
            body_at: start + HEADER_BYTES + 10,
            frame_end: buffer.len(),
        }
    );
}

#[test]
fn a_body_over_the_threshold_is_read_directly() {
    let frame = sized_frame(10, DIRECT_BODY_BYTES + 1);
    assert_eq!(
        next_step(&frame, 0, HEADER_BYTES + 9),
        FrameStep::NeedBytes {
            wanted: HEADER_BYTES + 10
        }
    );
    assert_eq!(
        next_step(&frame, 0, frame.len() - 1),
        FrameStep::ReadBodyDirectly {
            meta_at: HEADER_BYTES,
            body_at: HEADER_BYTES + 10,
            body_bytes: DIRECT_BODY_BYTES + 1,
        }
    );
    let at_threshold = sized_frame(10, DIRECT_BODY_BYTES);
    assert_eq!(
        next_step(&at_threshold, 0, at_threshold.len() - 1),
        FrameStep::NeedBytes {
            wanted: at_threshold.len()
        }
    );
}

#[test]
fn a_header_over_the_bounds_is_refused_before_allocation() {
    let over = |meta_bytes: u32, body_bytes: u64| {
        let mut header = [0_u8; HEADER_BYTES];
        header[..4].copy_from_slice(&meta_bytes.to_be_bytes());
        header[4..].copy_from_slice(&body_bytes.to_be_bytes());
        parse_header(header)
    };
    assert!(over(MIN_META_BYTES - 1, 0).is_err());
    assert!(over(MAX_META_BYTES + 1, 0).is_err());
    assert!(over(MIN_META_BYTES, MAX_BODY_BYTES + 1).is_err());
    assert!(over(MAX_META_BYTES, MAX_BODY_BYTES).is_ok());
}

#[test]
fn read_plans_grow_only_to_the_bytes_wanted() {
    assert_eq!(
        plan_read(FRAME_READ_BUFFER_BYTES, 0, 0, HEADER_BYTES),
        ReadPlan::ReadFromFront
    );
    assert_eq!(
        plan_read(FRAME_READ_BUFFER_BYTES, 10, 20, 30),
        ReadPlan::ReadIntoTail
    );
    assert_eq!(
        plan_read(
            FRAME_READ_BUFFER_BYTES,
            FRAME_READ_BUFFER_BYTES - 5,
            FRAME_READ_BUFFER_BYTES - 1,
            20
        ),
        ReadPlan::MoveToFront
    );
    assert_eq!(
        plan_read(FRAME_READ_BUFFER_BYTES, 0, 10, FRAME_READ_BUFFER_BYTES + 1),
        ReadPlan::MoveAndGrow {
            buffer_bytes: FRAME_READ_BUFFER_BYTES + 1
        }
    );
}

// ---------------------------------------------------------------------------
// The read buffer
// ---------------------------------------------------------------------------

/// Feed `stream` to a reader in reads of the sizes in `reads`, in turn, and
/// collect every frame.
fn read_all(stream: &[u8], reads: &[usize]) -> Vec<RawFrame> {
    let mut reader = FrameReader::new();
    let mut frames = Vec::new();
    let mut fed = 0;
    let mut turn = 0;
    while fed < stream.len() {
        while let Some(frame) = reader.next_frame().unwrap() {
            frames.push(frame);
        }
        let space = reader.read_space();
        let count = reads[turn % reads.len()]
            .min(space.len())
            .min(stream.len() - fed);
        space[..count].copy_from_slice(&stream[fed..fed + count]);
        reader.arrived(count);
        fed += count;
        turn += 1;
    }
    while let Some(frame) = reader.next_frame().unwrap() {
        frames.push(frame);
    }
    assert!(!reader.has_partial_frame());
    assert_eq!(
        reader.bytes_received(),
        u64::try_from(stream.len()).unwrap()
    );
    frames
}

#[test]
fn the_reader_gives_every_frame_in_order_for_any_read_size() {
    let large_body: Vec<u8> = (0..DIRECT_BODY_BYTES * 3 + 7)
        .map(|at| u8::try_from(at % 251).unwrap())
        .collect();
    let parts: Vec<(&str, Vec<u8>)> = vec![
        (r#"{"kind":"stop"}"#, Vec::new()),
        (r#"{"kind":"a"}"#, b"0123456789".to_vec()),
        (r#"{"kind":"large"}"#, large_body),
        (r#"{"kind":"b"}"#, b"x".to_vec()),
    ];
    let stream: Vec<u8> = parts
        .iter()
        .flat_map(|(meta, body)| framed(meta, body))
        .collect();
    for reads in [
        vec![1],
        vec![7, 13],
        vec![HEADER_BYTES],
        vec![4096, 3],
        vec![DIRECT_BODY_BYTES, 1],
        vec![usize::MAX],
    ] {
        let frames = read_all(&stream, &reads);
        assert_eq!(frames.len(), parts.len(), "reads {reads:?}");
        for (frame, (meta, body)) in frames.iter().zip(&parts) {
            assert_eq!(frame.meta, meta.as_bytes(), "reads {reads:?}");
            assert_eq!(&frame.body, body, "reads {reads:?}");
        }
    }
}

#[test]
fn the_reader_refuses_a_header_over_the_bounds() {
    let mut reader = FrameReader::new();
    let mut header = [0_u8; HEADER_BYTES];
    header[..4].copy_from_slice(&(MAX_META_BYTES + 1).to_be_bytes());
    let space = reader.read_space();
    space[..HEADER_BYTES].copy_from_slice(&header);
    reader.arrived(HEADER_BYTES);
    assert_eq!(
        reader.next_frame(),
        Err(FrameOverBound {
            meta_bytes: MAX_META_BYTES + 1,
            body_bytes: 0,
        })
    );
}

#[test]
fn the_reader_knows_a_partial_frame() {
    let mut reader = FrameReader::new();
    assert!(!reader.has_partial_frame());
    let frame = framed(r#"{"kind":"stop"}"#, b"");
    let space = reader.read_space();
    space[..5].copy_from_slice(&frame[..5]);
    reader.arrived(5);
    assert_eq!(reader.next_frame(), Ok(None));
    assert!(reader.has_partial_frame());
}

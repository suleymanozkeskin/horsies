//! The record of a panic of a task body (Python: the flattened exception of
//! an unhandled exception, `core/codec/error_payload.py`, `flatten_exception`).
//!
//! The child host installs one panic hook. It keeps, per thread, the text of
//! the last panic and its backtrace when one was captured, and then runs the
//! hook that was there before, so the panic still reaches stderr. A body's
//! panic is caught on the thread that panicked, right after the hook ran:
//! [`catch_body_panic`] for a blocking body, [`CatchPanic`] for each poll of
//! an async body. The catch site takes the kept record and builds the task
//! error: `UNHANDLED_EXCEPTION`, `data.exception_type` = `panic`, and the
//! five-key exception record.

use std::backtrace::{Backtrace, BacktraceStatus};
use std::cell::RefCell;
use std::future::Future;
use std::panic::{AssertUnwindSafe, PanicHookInfo};
use std::pin::Pin;
use std::sync::Once;
use std::task::{Context, Poll};

use crate::core::task::error::{ExceptionRecord, OperationalCode, TaskError};
use crate::core::task::fn_trait::TaskModule;

/// The `type` of the exception record of a panic, and its
/// `data.exception_type`.
pub(super) const PANIC_TYPE_NAME: &str = "panic";
/// The longest panic text kept, in bytes; a longer text is cut at a char
/// boundary.
const MAX_PANIC_TEXT_BYTES: usize = 16 * 1024;
/// The longest backtrace kept, in bytes; a longer one is cut at a char
/// boundary.
const MAX_BACKTRACE_BYTES: usize = 64 * 1024;

static HOOK: Once = Once::new();

thread_local! {
    /// The last panic on this thread, written by the hook.
    static LAST_PANIC: RefCell<Option<PanicRecord>> = const { RefCell::new(None) };
}

/// Whether a backtrace was captured for a panic. The standard library
/// captures one when `RUST_BACKTRACE` or `RUST_LIB_BACKTRACE` asks for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PanicBacktrace {
    Captured(String),
    NotCaptured,
}

/// What a panic of a task body left.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PanicRecord {
    /// The panic message, cut to [`MAX_PANIC_TEXT_BYTES`].
    pub(super) text: String,
    pub(super) backtrace: PanicBacktrace,
}

/// Install the hook of the child host, once per process. The hook that was
/// there before still runs after it.
pub(super) fn install_panic_hook() {
    HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info: &PanicHookInfo<'_>| {
            let record = PanicRecord {
                text: cut(payload_text(info.payload()), MAX_PANIC_TEXT_BYTES),
                backtrace: captured_backtrace(),
            };
            // A thread that is ending has no slot left: the catch site then
            // reads the payload text.
            let _thread_ending = LAST_PANIC.try_with(|slot| {
                if let Ok(mut kept) = slot.try_borrow_mut() {
                    *kept = Some(record);
                }
            });
            previous(info);
        }));
    });
}

fn captured_backtrace() -> PanicBacktrace {
    let backtrace = Backtrace::capture();
    match backtrace.status() {
        BacktraceStatus::Captured => {
            PanicBacktrace::Captured(cut(backtrace.to_string(), MAX_BACKTRACE_BYTES))
        }
        BacktraceStatus::Disabled | BacktraceStatus::Unsupported => PanicBacktrace::NotCaptured,
        // `BacktraceStatus` is non-exhaustive; a status that a later
        // standard library adds has no text to keep.
        _ => PanicBacktrace::NotCaptured,
    }
}

/// The text of a panic payload: a `&str` or a `String`, else a fixed text.
fn payload_text(payload: &(dyn std::any::Any + Send)) -> String {
    match (
        payload.downcast_ref::<&str>(),
        payload.downcast_ref::<String>(),
    ) {
        (Some(text), _) => (*text).to_owned(),
        (None, Some(text)) => text.clone(),
        (None, None) => "the panic payload is not text".to_owned(),
    }
}

/// `text` cut to at most `limit` bytes at a char boundary. Pure.
fn cut(mut text: String, limit: usize) -> String {
    if text.len() > limit {
        let mut end = limit;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    text
}

/// The record that the hook kept on this thread for the panic with
/// `payload`, or the payload text when the hook did not run (a hook set later
/// replaced it).
fn take_record(payload: &(dyn std::any::Any + Send)) -> PanicRecord {
    let kept = LAST_PANIC
        .try_with(|slot| match slot.try_borrow_mut() {
            Ok(mut kept) => kept.take(),
            Err(_already_borrowed) => None,
        })
        .ok()
        .flatten();
    match kept {
        Some(record) => record,
        None => PanicRecord {
            text: cut(payload_text(payload), MAX_PANIC_TEXT_BYTES),
            backtrace: PanicBacktrace::NotCaptured,
        },
    }
}

/// Run a blocking body on this thread, and catch its panic.
pub(super) fn catch_body_panic<T>(body: impl FnOnce() -> T) -> Result<T, PanicRecord> {
    std::panic::catch_unwind(AssertUnwindSafe(body))
        .map_err(|payload| take_record(payload.as_ref()))
}

/// An async body whose panic is caught in the poll that panicked, on the
/// thread that ran it. The body is not polled after its panic.
pub(super) struct CatchPanic<F> {
    inner: Pin<Box<F>>,
}

impl<F> CatchPanic<F> {
    pub(super) fn new(inner: F) -> Self {
        Self {
            inner: Box::pin(inner),
        }
    }
}

impl<F: Future> Future for CatchPanic<F> {
    type Output = Result<F::Output, PanicRecord>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.inner.as_mut();
        match std::panic::catch_unwind(AssertUnwindSafe(|| inner.poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Err(payload) => Poll::Ready(Err(take_record(payload.as_ref()))),
        }
    }
}

/// The task error of a body's panic (Python's unhandled exception, with
/// `panic` as the exception type): the code `UNHANDLED_EXCEPTION`; the
/// message `Unhandled exception in task <task>: panic: <text>`;
/// `data.exception_type` = `panic`; the exception record with the module of
/// the task, the panic text as message and repr, and the captured backtrace
/// or an empty traceback. Pure.
pub(super) fn panic_error(task_name: &str, module: TaskModule, record: PanicRecord) -> TaskError {
    let mut error = TaskError::built_in(
        OperationalCode::UnhandledException,
        format!(
            "Unhandled exception in task {task_name}: {PANIC_TYPE_NAME}: {}",
            record.text
        ),
    );
    error.data = Some(crate::core::codec::JsonText::of_engine_value(
        &serde_json::json!({ "exception_type": PANIC_TYPE_NAME }),
    ));
    error.exception = Some(Box::new(ExceptionRecord {
        type_name: PANIC_TYPE_NAME.to_owned(),
        module: match module {
            TaskModule::Path(path) => path.to_owned(),
            TaskModule::NotGiven => String::new(),
        },
        message: record.text.clone(),
        repr: record.text,
        traceback: match record.backtrace {
            PanicBacktrace::Captured(text) => text,
            PanicBacktrace::NotCaptured => String::new(),
        },
    }));
    error
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_blocking_panic_keeps_its_text() {
        install_panic_hook();
        let caught = catch_body_panic(|| -> u8 { panic!("the input was {}", 7) });
        let record = caught.expect_err("the body panics");
        assert_eq!(record.text, "the input was 7");
    }

    #[test]
    fn a_body_that_returns_is_not_a_panic() {
        assert_eq!(catch_body_panic(|| 3_u8), Ok(3));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_async_panic_is_caught_in_its_poll() {
        install_panic_hook();
        let body = tokio::spawn(CatchPanic::new(async {
            tokio::task::yield_now().await;
            if std::hint::black_box(true) {
                panic!("async body failed");
            }
            1_u8
        }));
        let caught = body.await.expect("the catch keeps the task from panicking");
        assert_eq!(
            caught.expect_err("the body panics").text,
            "async body failed"
        );
    }

    #[test]
    fn a_panic_error_is_python_s_unhandled_exception_with_the_panic_type() {
        let error = panic_error(
            "app.tasks.fetch",
            TaskModule::Path("app::tasks"),
            PanicRecord {
                text: "index out of bounds".to_owned(),
                backtrace: PanicBacktrace::NotCaptured,
            },
        );
        assert_eq!(
            error.error_code,
            crate::core::task::error::TaskErrorCode::BuiltIn(
                OperationalCode::UnhandledException.into()
            )
        );
        assert_eq!(
            error.message.as_deref(),
            Some("Unhandled exception in task app.tasks.fetch: panic: index out of bounds")
        );
        assert_eq!(
            error.data_value(),
            Some(serde_json::json!({ "exception_type": "panic" }))
        );
        assert_eq!(
            error.exception,
            Some(Box::new(ExceptionRecord {
                type_name: "panic".to_owned(),
                module: "app::tasks".to_owned(),
                message: "index out of bounds".to_owned(),
                repr: "index out of bounds".to_owned(),
                traceback: String::new(),
            }))
        );
    }

    #[test]
    fn a_captured_backtrace_is_the_traceback_and_no_module_is_empty() {
        let error = panic_error(
            "app.tasks.fetch",
            TaskModule::NotGiven,
            PanicRecord {
                text: "x".to_owned(),
                backtrace: PanicBacktrace::Captured("0: app::tasks::fetch".to_owned()),
            },
        );
        let record = error.exception.expect("a panic has an exception record");
        assert_eq!(record.module, "");
        assert_eq!(record.traceback, "0: app::tasks::fetch");
    }

    #[test]
    fn a_long_text_is_cut_at_a_char_boundary() {
        let text = "é".repeat(MAX_PANIC_TEXT_BYTES);
        let kept = cut(text, MAX_PANIC_TEXT_BYTES);
        assert!(kept.len() <= MAX_PANIC_TEXT_BYTES);
        assert!(kept.chars().all(|c| c == 'é'));
    }
}

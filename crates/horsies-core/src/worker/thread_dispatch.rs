//! The log dispatcher of the threads that the worker starts itself.
//!
//! A `tracing` default dispatcher belongs to one thread. A client sets its
//! subscriber as the default of each runtime thread (the Python bridge does
//! it in the runtime's thread start hook), and that default does not reach a
//! thread that the core starts with `std::thread`. Such a thread takes the
//! default dispatcher of the thread that starts it, so its records reach the
//! same subscriber as the records of its starter.

/// `body` with the default dispatcher of the calling thread. Call it on the
/// starting thread and run the returned closure on the new thread. Pure: it
/// reads the dispatcher and runs nothing.
pub(crate) fn with_starter_dispatch<T>(body: impl FnOnce() -> T) -> impl FnOnce() -> T {
    let dispatch = tracing::dispatcher::get_default(|current| current.clone());
    move || tracing::dispatcher::with_default(&dispatch, body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::LogCapture;

    const STARTED_LINE: &str = "the started thread logs";

    /// A record of a thread started through [`with_starter_dispatch`] reaches
    /// the default dispatcher of its starter; a thread started without it
    /// logs to no subscriber of the test.
    #[test]
    fn a_started_thread_logs_to_the_dispatcher_of_its_starter() {
        let capture = LogCapture::new();
        let _default = capture.set_default();

        std::thread::spawn(with_starter_dispatch(|| tracing::warn!("{STARTED_LINE}")))
            .join()
            .expect("the thread ends");
        std::thread::spawn(|| tracing::warn!("a thread without the dispatcher logs"))
            .join()
            .expect("the thread ends");

        let messages: Vec<String> = capture
            .events()
            .into_iter()
            .map(|event| event.message)
            .collect();
        assert_eq!(messages, [STARTED_LINE]);
    }
}

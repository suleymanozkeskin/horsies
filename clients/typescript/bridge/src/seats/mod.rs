//! The thread seats of a `SHARED_THREAD` child have no code of their own in
//! the addon. A seat run is a run of the core's seat thread with the blocking
//! body form of `child::body` (`JsBlockingBody`): it posts the run to the
//! JavaScript main thread and waits for its settle call, and it posts the
//! run's cancel. The main thread gives the run to a free seat isolate, and a
//! seat isolate never loads this addon. The start facts give the seat count,
//! and the core's host loop watch covers the main thread.

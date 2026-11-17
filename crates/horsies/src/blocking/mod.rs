//! Blocking forms of the async API. Each call runs on the core's per-process
//! runtime; inside a tokio runtime it answers `ASYNC_CONTEXT`.

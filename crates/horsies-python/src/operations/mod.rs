//! The core operations exposed to Python, one module per area. Each module
//! holds the operation future, its argument and record classes, and the
//! conversion of its outcome; `handle.rs` binds them to `CoreHandle`.

pub mod arguments;
pub mod enqueue;
pub mod schema;
pub mod task_info;

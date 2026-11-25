//! The body form of `SHARED_THREAD` tasks: a run goes to the main thread of
//! the child, then to a `worker_threads` seat that never loads this addon;
//! the cancel cause goes to the seat.

//! The Node-API addon of the TypeScript client: the call bridge from Node and
//! Bun to horsies-core.
//!
//! Rules of the addon:
//! - Loading it starts nothing: no runtime, no thread, no connection.
//! - Only the main thread of a process loads it. A `worker_threads` isolate
//!   never loads it.
//! - Every operation runs on the core's process runtime.
//! - Each area is one folder with one owner. This file holds one `mod` line
//!   per area, the musl allocator, the link line of the `napi` crate, and the
//!   crate-level lint attributes; an area adds its own submodules in its
//!   `mod.rs`.
//! - Every `unsafe` block states why it is sound in a `// SAFETY:` note; the
//!   lint `clippy::undocumented_unsafe_blocks` refuses one without it.

#![deny(clippy::undocumented_unsafe_blocks)]
// napi-derive registers an export only outside `cfg(test)`. In a test build,
// an export that no Rust test calls has no use; the library build checks
// dead code with the registrations in place.
#![cfg_attr(
    test,
    allow(
        dead_code,
        reason = "napi-derive registers the exports only outside cfg(test)"
    )
)]

// The Node-API module registration (`napi_register_module_v1`) is an item of
// the `napi` crate. This line links that crate when no area exports an item.
extern crate napi as _;

mod call;
mod child;
mod facades;
mod handle;
mod handler_child;
mod logging;
mod process_core;
mod scheduler;
mod seats;
mod sends;
mod values;
mod worker;
mod workflows;

/// The allocator of musl targets (see `Cargo.toml`).
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

//! The failure code of the seat relay of a `SHARED_THREAD` child of the
//! TypeScript client, as one closed catalog for the clients.
//!
//! A seat isolate of that client never loads the addon: each core call of
//! task code there goes through a relay to the main thread of its child,
//! with a bound of the calls that wait. The Python and Rust clients run a
//! seat on a thread of the child itself, have no relay, and give no such
//! code.

use serde::{Deserialize, Serialize};

/// The failure code of a call that the seat relay of a seat isolate did not
/// send.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_core_derive::CodeCatalog,
)]
#[code_catalog(module = "task_send", base = "str_enum")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SeatRelayCode {
    /// The seat relay holds its bound of waiting calls; nothing was sent;
    /// retry.
    SeatRelayFull,
}

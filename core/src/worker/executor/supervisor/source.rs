//! Where a slot gets its children: the spawner thread of the worker.
//!
//! Every child comes from a spawn of the spawner thread. A slot asks
//! through [`ChildSource`], so a test can build a source over the spawner
//! that changes what the slot sees of a spawn (a spawn error, an exit
//! that is published late) while the slot code under test is the code a
//! worker runs.

use std::future::Future;
use std::pin::Pin;

use super::spawner::{ChildCommand, SpawnError, SpawnedChild, SpawnerHandle};

/// The future of one spawn.
pub type SpawnFuture<'a> =
    Pin<Box<dyn Future<Output = Result<SpawnedChild, SpawnError>> + Send + 'a>>;

/// Where a slot gets its children.
pub trait ChildSource: Send + Sync {
    /// Start a child of `command` (see [`SpawnerHandle::spawn`]).
    fn spawn(&self, command: ChildCommand) -> SpawnFuture<'_>;
}

impl ChildSource for SpawnerHandle {
    fn spawn(&self, command: ChildCommand) -> SpawnFuture<'_> {
        Box::pin(SpawnerHandle::spawn(self, command))
    }
}

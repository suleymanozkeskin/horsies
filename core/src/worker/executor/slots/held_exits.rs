//! A source of children for slot tests that drive a child whose exit is
//! not seen, or a spawn that fails (Python tests use a process double for
//! the same paths). Every child comes from the real spawner thread, which
//! kills and reaps it as in a worker; this source only decides when the
//! slot sees the published exit, and can refuse a spawn as a parent short
//! of descriptors would.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::watch;

use crate::worker::executor::protocol::ShortageErrno;
use crate::worker::executor::supervisor::{
    ChildCommand, ChildSource, ExitState, SpawnError, SpawnFuture, SpawnedChild, SpawnerHandle,
};

/// Whether the slot sees the exit of a child when the spawner publishes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ExitRelease {
    /// The exit waits until the test releases it.
    Held,
    Released,
}

/// Children from the spawner, with exits that the test can hold back.
pub(super) struct HeldExits {
    spawner: SpawnerHandle,
    release: watch::Sender<ExitRelease>,
    refusals: Mutex<VecDeque<ShortageErrno>>,
}

impl HeldExits {
    /// A source whose exits start as `release` says.
    pub(super) fn new(spawner: SpawnerHandle, release: ExitRelease) -> Arc<Self> {
        let (release, _) = watch::channel(release);
        Arc::new(Self {
            spawner,
            release,
            refusals: Mutex::new(VecDeque::new()),
        })
    }

    /// Publish every exit that waits, and every later one at once.
    pub(super) fn release(&self) {
        let _ = self.release.send_replace(ExitRelease::Released);
    }

    /// Refuse the next spawn as a parent short of a resource.
    pub(super) fn refuse_next_spawn(&self, errno: ShortageErrno) {
        lock(&self.refusals).push_back(errno);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl ChildSource for HeldExits {
    fn spawn(&self, command: ChildCommand) -> SpawnFuture<'_> {
        Box::pin(async move {
            let refusal = lock(&self.refusals).pop_front();
            if let Some(errno) = refusal {
                return Err(SpawnError::ResourceShortage {
                    errno,
                    detail: format!("{}: the test refused the spawn", errno.as_str()),
                });
            }
            let SpawnedChild {
                id,
                pid,
                socket,
                exit,
            } = self.spawner.spawn(command).await?;
            let (seen, seen_exit) = watch::channel(ExitState::Running);
            tokio::spawn(relay_exit(exit, seen, self.release.subscribe()));
            Ok(SpawnedChild {
                id,
                pid,
                socket,
                exit: seen_exit,
            })
        })
    }
}

/// Publish the exit that the spawner published once the test releases it.
/// The sender is dropped after, as the spawner drops its own after a reap.
async fn relay_exit(
    mut published: watch::Receiver<ExitState>,
    seen: watch::Sender<ExitState>,
    mut release: watch::Receiver<ExitRelease>,
) {
    let state = match published
        .wait_for(|state| *state != ExitState::Running)
        .await
    {
        Ok(state) => *state,
        // The spawner thread ended; the slot sees the same.
        Err(_) => return,
    };
    if release
        .wait_for(|release| *release == ExitRelease::Released)
        .await
        .is_err()
    {
        return;
    }
    let _ = seen.send_replace(state);
}

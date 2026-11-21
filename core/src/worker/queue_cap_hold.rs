//! Soft-cap mode: a capped queue's count is checked before each start.
//!
//! In soft-cap mode (`prefetch_buffer > 0`) `horsies_claim` counts only the
//! RUNNING rows of a queue against its `max_concurrency`, so a worker can
//! claim more rows of a capped queue than its cap while none of them runs.
//! The worker does not submit those rows at once: it holds them in the
//! parent ([`QueueCapHold`]), and each claim pass reads one count per capped
//! queue that has held rows, in one statement (`count_queue_starts`):
//!
//! - the RUNNING rows of the queue, on every worker;
//! - the claims of the queue that this worker submitted and that are still
//!   CLAIMED, because their child has not set them RUNNING yet.
//!
//! The pass then submits held rows in claim order while that count stays
//! under the cap and a child of the row's route has a free slot or seat at
//! that moment (the [`FreeCapacity`] of the row's budget, read after the
//! count). An admitted row therefore goes to a child at once and does not
//! wait in the pool's queue. Another worker counts the row once its child
//! sets it RUNNING, so the time in which another worker does not count it is
//! one start. A held row stays CLAIMED, counts in the worker's claim budget,
//! and the claimer heartbeat renews its lease, until a later pass submits it
//! or the worker stops and returns it to the queue.
//!
//! A submitted claim counts until its outcome write ends: the write holds
//! its [`StartedClaim`]. The claim is the task id with its claim
//! generation, so a later claim of the same task is another claim.
//!
//! Rows of an uncapped queue are submitted at once. In hard-cap mode every
//! row is: there the claim counts the CLAIMED rows against the cap too.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::claims::{count_queue_starts, ClaimedRow, QueueStartCheck, QueueStarts};
use crate::broker::{BrokerError, PostgresBroker};
use crate::worker::executor::slots::{ExecutionClass, FreeCapacity};

#[cfg(test)]
mod tests;

/// One claim: the task id and its claim generation.
type ClaimKey = (Uuid, DateTime<Utc>);

/// The cap of each queue whose rows wait for the start check.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StartCheckCaps {
    caps: HashMap<String, u32>,
}

impl StartCheckCaps {
    /// Every capped queue in soft-cap mode (`prefetch_buffer > 0`), none in
    /// hard-cap mode (`prefetch_buffer == 0`). Pure.
    pub fn of_mode(
        prefetch_buffer: u32,
        queue_max_concurrency: &HashMap<String, u32>,
    ) -> StartCheckCaps {
        match prefetch_buffer {
            0 => StartCheckCaps::default(),
            _ => StartCheckCaps {
                caps: queue_max_concurrency.clone(),
            },
        }
    }

    fn cap_of(&self, queue_name: &str) -> Option<u32> {
        self.caps.get(queue_name).copied()
    }
}

/// The claims that the start check admitted and whose outcome write has not
/// ended. Shared with the outcome writes, which end their claim.
#[derive(Debug, Clone, Default)]
struct StartedClaims(Arc<Mutex<HashSet<ClaimKey>>>);

impl StartedClaims {
    fn lock(&self) -> MutexGuard<'_, HashSet<ClaimKey>> {
        // A poisoned lock means a panic while it was held; the set is still a
        // valid value.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Record `key` and give its one guard. A claim recorded twice is a
    /// defect: the claim pass returns each claim once, and the hold admits
    /// each held row once.
    fn start(&self, key: ClaimKey) -> StartedClaim {
        let inserted = self.lock().insert(key);
        assert!(
            inserted,
            "task {} at claim generation {} was admitted twice by the start check",
            key.0, key.1
        );
        StartedClaim {
            claims: self.clone(),
            key,
        }
    }

    fn keys(&self) -> Vec<ClaimKey> {
        self.lock().iter().copied().collect()
    }
}

/// A claim that the start check admitted. It counts in its queue's start
/// check until it drops: the outcome write of the claim holds it to its end,
/// and an aborted write drops it too.
#[derive(Debug)]
pub struct StartedClaim {
    claims: StartedClaims,
    key: ClaimKey,
}

impl Drop for StartedClaim {
    fn drop(&mut self) {
        // `StartedClaims::start` recorded the key and made this one guard
        // for it, so the key is in the set.
        let _removed = self.claims.lock().remove(&self.key);
    }
}

/// Whether a submitted claim counts in its queue's start check.
#[derive(Debug)]
pub enum StartCount {
    /// A row that the check does not hold: a row of an uncapped queue, or
    /// any row in hard-cap mode.
    Unchecked,
    /// A row that the check admitted; it counts until this drops.
    Admitted(StartedClaim),
}

/// A held row that may start now, and its count in the start check.
#[derive(Debug)]
pub struct AdmittedRow {
    pub row: ClaimedRow,
    pub started: StartedClaim,
}

/// A held row and the budget whose children run it: the budget that the
/// claim plan routes the row's class to.
#[derive(Debug)]
struct HeldRow {
    row: ClaimedRow,
    budget: ExecutionClass,
}

/// The held rows of the capped queues of one worker, and the claims that it
/// admitted and whose outcome write has not ended.
///
/// The claim loop owns it; the outcome writes share only the admitted
/// claims. Does no I/O. The held rows are CLAIMED rows of this worker, so
/// the claim budget of the worker bounds their number.
#[derive(Debug)]
pub struct QueueCapHold {
    caps: StartCheckCaps,
    held: VecDeque<HeldRow>,
    started: StartedClaims,
}

impl QueueCapHold {
    pub fn new(caps: StartCheckCaps) -> QueueCapHold {
        QueueCapHold {
            caps,
            held: VecDeque::new(),
            started: StartedClaims::default(),
        }
    }

    /// Whether a row of `queue_name` waits for the start check.
    pub fn holds(&self, queue_name: &str) -> bool {
        self.caps.cap_of(queue_name).is_some()
    }

    /// Hold `row` until a pass admits it. `budget` is the budget that the
    /// claim plan routes the row's class to: a child of that budget runs the
    /// row. A row of a queue that the check does not hold is a defect of the
    /// caller.
    pub fn hold(&mut self, row: ClaimedRow, budget: ExecutionClass) {
        assert!(
            self.holds(&row.queue_name),
            "task {} of queue {:?} is not held by the start check",
            row.id,
            row.queue_name
        );
        self.held.push_back(HeldRow { row, budget });
    }

    /// The queues that have held rows, in the order of their first row.
    pub fn held_queues(&self) -> Vec<String> {
        let mut queues: Vec<String> = Vec::new();
        for held in &self.held {
            if !queues.contains(&held.row.queue_name) {
                queues.push(held.row.queue_name.clone());
            }
        }
        queues
    }

    /// The claims that this worker admitted and whose write has not ended,
    /// in no set order.
    pub fn started_claims(&self) -> Vec<(Uuid, DateTime<Utc>)> {
        self.started.keys()
    }

    /// How many rows wait for the start check.
    pub fn held_count(&self) -> usize {
        self.held.len()
    }

    /// The start check of the held rows, or `None` when no row is held:
    /// then the pass sends no statement.
    pub fn check(&self, worker_id: &str) -> Option<QueueStartCheck> {
        match self.held.is_empty() {
            true => None,
            false => Some(QueueStartCheck {
                worker_id: worker_id.to_owned(),
                queues: self.held_queues(),
                started: self.started_claims(),
            }),
        }
    }

    /// Take the held rows that may start, in claim order, and record each as
    /// started.
    ///
    /// A row starts while its queue's count in `starts` plus the rows that
    /// this call admitted of the queue stays under the cap, and the free
    /// capacity of its budget in `free` less the rows that this call
    /// admitted of the budget stays above 0. The other rows stay held, in
    /// their order.
    pub fn admit(&mut self, starts: &QueueStarts, free: &FreeCapacity) -> Vec<AdmittedRow> {
        let held = std::mem::take(&mut self.held);
        let mut admitted = Vec::new();
        let mut taken: HashMap<String, u64> = HashMap::new();
        let mut seated: HashMap<ExecutionClass, u32> = HashMap::new();
        for held_row in held {
            let HeldRow { row, budget } = &held_row;
            let Some(cap) = self.caps.cap_of(&row.queue_name) else {
                panic!(
                    "task {} of queue {:?} was held with no cap",
                    row.id, row.queue_name
                );
            };
            let taken_of_queue = taken.entry(row.queue_name.clone()).or_insert(0);
            let count = starts.of(&row.queue_name).saturating_add(*taken_of_queue);
            let seated_of_budget = seated.entry(*budget).or_insert(0);
            let room = free.of(*budget).saturating_sub(*seated_of_budget);
            match (count < u64::from(cap), room > 0) {
                (true, true) => {
                    *taken_of_queue += 1;
                    *seated_of_budget += 1;
                    let HeldRow { row, budget: _ } = held_row;
                    let started = self.started.start((row.id, row.claimed_at));
                    admitted.push(AdmittedRow { row, started });
                }
                (false, _) | (true, false) => self.held.push_back(held_row),
            }
        }
        admitted
    }

    /// Take every held row, in claim order, at the stop of the worker.
    pub fn drain(&mut self) -> Vec<ClaimedRow> {
        self.held.drain(..).map(|held| held.row).collect()
    }
}

/// The start check of one claim pass: when rows are held, one statement
/// (`count_queue_starts`) on the main core pool of `broker` (the pool of
/// the claim pass), then the held rows that their queue's count leaves room
/// for and that a free child of their budget takes now. `free` reads the
/// free capacity of each budget; it runs after the count, so the admitted
/// rows meet the children that are free at their submit. No statement when
/// no row is held.
///
/// Errors: the [`BrokerError`] of the core pools or of the count. The held
/// rows stay held, and nothing is admitted.
pub async fn admit_held_rows(
    broker: &PostgresBroker,
    worker_id: &str,
    hold: &mut QueueCapHold,
    free: impl FnOnce() -> FreeCapacity,
) -> Result<Vec<AdmittedRow>, BrokerError> {
    let Some(check) = hold.check(worker_id) else {
        return Ok(Vec::new());
    };
    let (open, _write) = broker.core_pools_for_write()?;
    let starts = count_queue_starts(open.main(), broker.statement_log(), &check).await?;
    Ok(hold.admit(&starts, &free()))
}

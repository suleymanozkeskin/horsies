//! Ownership guards a terminal transition applies to the rows it touches.
//!
//! A fence answers "may this caller end this task's life right now". It is not
//! the source-status check, which every transition also carries, and it is not
//! target selection. It is the claim-ownership predicate layered on top.
//!
//! Which fence a transition needs follows from where it runs, not from
//! preference:
//!
//! - a caller acting on a deadline needs the worker but not the generation,
//!   since the outcome is correct for whichever generation holds an expired
//!   row;
//! - a caller acting on one claim it was handed needs the full owner pair;
//! - a caller acting on a batch it was handed needs that pair per task,
//!   because a batch can span claim transactions.
//!
//! The claim generation is `claimed_at`: set by the claim, cleared by every
//! requeue. Worker id alone cannot separate generations, because a worker
//! whose lease lapsed can re-claim its own task and match again.
//!
//! Transitions that act on behalf of a workflow carry no fence at all. They
//! exist to reach claims other workers hold, so an ownership predicate would
//! skip exactly the rows they are for. Their guard is the workflow's own
//! state, which is implied by the command and verified in-statement rather
//! than carried as data — see `commands.rs`.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::LifecycleValidationError;

/// Worker ownership, deliberately without a claim generation.
///
/// Used where a non-ownership guard already makes the outcome correct for any
/// generation — an expired deadline does not become unexpired because the row
/// was re-claimed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerOwned {
    pub worker_id: String,
}

/// One task, held by this worker at this claim generation.
///
/// The claim generation is required: every dispatch carries the `claimed_at`
/// that its claim returned, so no caller can turn the generation half of the
/// fence off.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedClaim {
    pub worker_id: String,
    pub claimed_at: DateTime<Utc>,
}

/// Many tasks, each at its own claim generation.
///
/// One batch can span several claim transactions, so a single generation
/// cannot describe it: that would either spare every task or terminalize
/// every task. Generations travel with their task id, which also makes a
/// length mismatch between ids and generations unrepresentable. Construction
/// rejects duplicate ids: a repeat means two generations claim the same row
/// and the fence is ambiguous.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedClaimBatch {
    worker_id: String,
    claim_generations: Vec<(Uuid, DateTime<Utc>)>,
}

impl OwnedClaimBatch {
    pub fn new(
        worker_id: String,
        claim_generations: Vec<(Uuid, DateTime<Utc>)>,
    ) -> Result<Self, LifecycleValidationError> {
        let mut seen = std::collections::HashSet::new();
        for (task_id, _) in &claim_generations {
            if !seen.insert(*task_id) {
                return Err(LifecycleValidationError::DuplicateTaskIdInBatch { task_id: *task_id });
            }
        }
        Ok(Self {
            worker_id,
            claim_generations,
        })
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    pub fn task_ids(&self) -> Vec<Uuid> {
        self.claim_generations
            .iter()
            .map(|(task_id, _)| *task_id)
            .collect()
    }

    pub fn generations(&self) -> Vec<DateTime<Utc>> {
        self.claim_generations
            .iter()
            .map(|(_, generation)| *generation)
            .collect()
    }

    pub fn len(&self) -> usize {
        self.claim_generations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.claim_generations.is_empty()
    }
}

/// A command's claim-ownership guard, borrowed for outcome reporting.
#[derive(Debug, Clone, Copy)]
pub enum TerminalFence<'a> {
    WorkerOwned(&'a WorkerOwned),
    OwnedClaim(&'a OwnedClaim),
    OwnedClaimBatch(&'a OwnedClaimBatch),
}

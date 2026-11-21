//! Hourly heartbeat registration, create-ahead, and expiry sweep.

use chrono::{DateTime, Duration, Utc};

use crate::broker::core_rows::StatementLog;
use crate::core::history::commands::{is_safe_identifier, LeafBounds, LeafRef};
use crate::core::history::errors::HistoryError;
use crate::core::history::maintenance::pruning::{
    pass_candidates, sweep_counts_toward_bound, sweep_retention_leaf, LeafSweep, PruneReach,
};
use crate::core::history::names::{HEARTBEATS_TABLE, HEARTBEAT_CLASS_KEY};
use crate::core::history::outcomes::LeafDrop;
use crate::core::history::partitions::manager::DetachExpiredLeafOutcome;
use crate::core::history::partitions::retirement::RetiredClassKind;
use crate::db::CorePool;

const HOURLY: Duration = Duration::hours(1);
const HORIZON_FLOOR: Duration = Duration::hours(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeartbeatHorizonError {
    NonPositiveThreshold,
    InvalidSafetyFactor,
    Overflow,
}

pub fn heartbeat_horizon(
    stale_after: Duration,
    finalizing_stale_after: Duration,
    safety_factor: u32,
) -> Result<Duration, HeartbeatHorizonError> {
    if stale_after <= Duration::zero() || finalizing_stale_after <= Duration::zero() {
        return Err(HeartbeatHorizonError::NonPositiveThreshold);
    }
    if safety_factor < 1 {
        return Err(HeartbeatHorizonError::InvalidSafetyFactor);
    }
    let base = stale_after.max(finalizing_stale_after);
    let derived = base
        .checked_mul(i32::try_from(safety_factor).map_err(|_| HeartbeatHorizonError::Overflow)?)
        .ok_or(HeartbeatHorizonError::Overflow)?;
    Ok(derived.max(HORIZON_FLOOR))
}

/// `horsies_retention_classes.class_kind` of the heartbeat class.
pub const HEARTBEAT_CLASS_KIND: &str = "HEARTBEAT";

pub fn hourly_leaf_name(lower: DateTime<Utc>) -> Result<String, HistoryError> {
    let name = format!("{HEARTBEATS_TABLE}_{}", lower.format("%Y_%m_%d_%H"));
    if is_safe_identifier(&name) {
        Ok(name)
    } else {
        Err(HistoryError::contract("unsafe hourly heartbeat leaf name"))
    }
}

pub fn probe_index_name(leaf_name: &str) -> Result<String, HistoryError> {
    let name = format!("{leaf_name}_probe_idx");
    if is_safe_identifier(&name) {
        Ok(name)
    } else {
        Err(HistoryError::contract("unsafe heartbeat probe index name"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnsureHeartbeatCoverage {
    horizon_hours: u32,
}

impl EnsureHeartbeatCoverage {
    pub fn new(horizon_hours: u32) -> Result<Self, HistoryError> {
        if horizon_hours < 2 {
            return Err(HistoryError::contract(
                "heartbeat coverage must include at least two future leaves",
            ));
        }
        Ok(Self { horizon_hours })
    }

    pub fn horizon_hours(&self) -> u32 {
        self.horizon_hours
    }
}

pub fn hourly_leaf_ref(lower: DateTime<Utc>) -> Result<LeafRef, HistoryError> {
    LeafRef::new(
        hourly_leaf_name(lower)?,
        HEARTBEAT_CLASS_KEY,
        LeafBounds::new(lower, lower + HOURLY)
            .map_err(|error| HistoryError::contract(error.to_string()))?,
    )
    .map_err(|error| HistoryError::contract(error.to_string()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatLeafSwept {
    pub leaf_name: String,
    pub detach: DetachExpiredLeafOutcome,
    pub drop: Option<LeafDrop>,
}

/// Detach and drop the given expired heartbeat leaves, in the given order,
/// until `max_acted_leaves` leaves count toward the bound
/// (`sweep_counts_toward_bound`). Each leaf is swept as a period leaf is
/// (`pruning::sweep_retention_leaf`). A leaf whose sweep fails counts, since
/// its effects are unknown; its error goes to the error list and the sweep
/// goes on with the next leaf. `Truncated` when the bound left leaves for the
/// next pass.
pub(crate) async fn sweep_heartbeat_leaves(
    pool: &CorePool,
    log: StatementLog,
    leaves: &[LeafRef],
    max_acted_leaves: usize,
) -> LeafSweep<HeartbeatLeafSwept> {
    let mut swept = Vec::new();
    let mut errors = Vec::new();
    let mut acted = 0;
    let mut reach = PruneReach::WithinBounds;
    for leaf in leaves {
        if acted >= max_acted_leaves {
            reach = PruneReach::Truncated;
            break;
        }
        match sweep_retention_leaf(pool, log, leaf).await {
            Ok((detach, drop)) => {
                acted += usize::from(sweep_counts_toward_bound(&detach, drop.as_ref()));
                swept.push(HeartbeatLeafSwept {
                    leaf_name: leaf.leaf_name().to_owned(),
                    detach,
                    drop,
                });
            }
            Err(error) => {
                acted += 1;
                errors.push(format!("{}: {error}", leaf.leaf_name()));
            }
        }
    }
    LeafSweep {
        swept,
        errors,
        acted,
        reach,
    }
}

/// Sweep expired heartbeat leaves, oldest first, on a direct or
/// session-capable core pool: one candidate read (`horsies_prune_candidates`,
/// at most `MAX_PRUNE_CANDIDATES_PER_PASS` leaves), then
/// `sweep_heartbeat_leaves` over its heartbeat leaves. `Truncated` when the
/// candidate read or the leaf bound left expired leaves for the next pass.
///
/// Errors: the failed candidate read, which sweeps nothing.
pub(crate) async fn sweep_expired_heartbeat_leaves(
    pool: &CorePool,
    log: StatementLog,
    max_acted_leaves: usize,
) -> Result<LeafSweep<HeartbeatLeafSwept>, HistoryError> {
    let candidates = pass_candidates(pool, log).await?;
    let sweep = sweep_heartbeat_leaves(
        pool,
        log,
        &candidates.of_kind(RetiredClassKind::Heartbeat),
        max_acted_leaves,
    )
    .await;
    let reach = match (candidates.reach, sweep.reach) {
        (PruneReach::WithinBounds, PruneReach::WithinBounds) => PruneReach::WithinBounds,
        _ => PruneReach::Truncated,
    };
    Ok(LeafSweep { reach, ..sweep })
}

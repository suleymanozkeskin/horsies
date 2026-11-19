//! The capacity in service of each budget, sent with each claim pass. Pure.
//!
//! - `ISOLATED`: the isolated slots that are not down.
//! - A shared class: every seat of a slot that is booting, open or retiring;
//!   the runs of a closing slot; nothing for a down slot.

use super::dispatch::ExecutionClass;
use super::route::{ClaimPlan, ClaimRoute};
use crate::broker::claims::ClaimClasses;

/// The service state of one slot, as the claim sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotService {
    /// A child boots; its slot counts as capacity, so the first claim does
    /// not wait for every child.
    Booting,
    /// A child serves.
    InService,
    /// A shared child takes no new task; `runs` tasks are still on it.
    Closing { runs: u32 },
    /// The child retires; a new one boots next.
    Retiring,
    /// The slot is down after a boot failure that can clear.
    Down,
}

/// One shared slot: its state and its seats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedSlotService {
    pub service: SlotService,
    pub seats: u32,
}

/// The capacity of one budget in a claim pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetCapacity {
    pub budget: ExecutionClass,
    pub in_service: u32,
}

/// The children or seats of each budget that take a task at once: an
/// isolated child that waits for a task, a free seat of an open shared
/// child, less the tasks that already wait for one (Python
/// `SlotPool.free_slots`, `free_shared_async_seats`,
/// `free_shared_thread_seats`). A kind with no pool has 0.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FreeCapacity {
    pub isolated: u32,
    pub shared_async: u32,
    pub shared_thread: u32,
}

impl FreeCapacity {
    /// The free capacity of `budget`.
    pub fn of(&self, budget: ExecutionClass) -> u32 {
        match budget {
            ExecutionClass::Isolated => self.isolated,
            ExecutionClass::SharedAsync => self.shared_async,
            ExecutionClass::SharedThread => self.shared_thread,
        }
    }
}

/// The capacity of the `ISOLATED` budget.
pub fn isolated_capacity(slots: &[SlotService]) -> u32 {
    let in_service = slots
        .iter()
        .filter(|slot| !matches!(slot, SlotService::Down))
        .count();
    u32::try_from(in_service).unwrap_or(u32::MAX)
}

/// The capacity of a shared budget.
pub fn shared_capacity(slots: &[SharedSlotService]) -> u32 {
    slots
        .iter()
        .map(|slot| match slot.service {
            SlotService::Booting | SlotService::InService | SlotService::Retiring => slot.seats,
            SlotService::Closing { runs } => runs,
            SlotService::Down => 0,
        })
        .fold(0_u32, u32::saturating_add)
}

/// The slots of a worker, per kind, as the claim sees them.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PoolService {
    pub isolated: Vec<SlotService>,
    pub shared_async: Vec<SharedSlotService>,
    pub shared_thread: Vec<SharedSlotService>,
}

impl PoolService {
    /// The plan of this worker: a budget for each kind it runs children of.
    pub fn plan(&self) -> ClaimPlan {
        ClaimPlan::for_children(
            self.isolated.len(),
            self.shared_async.len(),
            self.shared_thread.len(),
        )
    }

    /// The capacity of every budget of the plan.
    pub fn capacities(&self) -> Vec<BudgetCapacity> {
        self.plan()
            .budgets()
            .into_iter()
            .map(|budget| BudgetCapacity {
                budget,
                in_service: match budget {
                    ExecutionClass::Isolated => isolated_capacity(&self.isolated),
                    ExecutionClass::SharedAsync => shared_capacity(&self.shared_async),
                    ExecutionClass::SharedThread => shared_capacity(&self.shared_thread),
                },
            })
            .collect()
    }

    /// The budgets and routes of one claim pass (Python `_claim_params`:
    /// `budgets_json(self._claim_capacity())`, `routes_json(claim_plan)`):
    /// the capacity in service of each budget of the plan, and the budget of
    /// each class that the plan serves. A class the plan does not serve has
    /// no route, so the claim takes no row of it. The routes come from
    /// [`ClaimPlan::route`], the one routing rule of the worker.
    pub fn claim_classes(&self) -> ClaimClasses {
        let plan = self.plan();
        let budgets = self
            .capacities()
            .into_iter()
            .map(|capacity| (capacity.budget, capacity.in_service))
            .collect();
        let routes = ExecutionClass::ALL
            .into_iter()
            .filter_map(|class| match plan.route(class) {
                ClaimRoute::ServedBy(budget) => Some((class, budget)),
                ClaimRoute::NotServed => None,
            })
            .collect();
        match ClaimClasses::new(budgets, routes) {
            Ok(classes) => classes,
            // The plan gives a budget to each class it routes to, so a route
            // to a missing budget is a defect of the plan.
            Err(unheld) => panic!("the claim plan of the worker broke its own rule: {unheld}"),
        }
    }

    /// The seats of the worker: one per isolated slot plus the seats of each
    /// shared slot (Python `core/worker/config.py`, the seat count). The
    /// claim of today takes this one number until it takes a budget per
    /// class.
    pub fn total_capacity(&self) -> u32 {
        self.capacities()
            .iter()
            .map(|capacity| capacity.in_service)
            .fold(0_u32, u32::saturating_add)
    }
}

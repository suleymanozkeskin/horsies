//! Tests of the claim plan and of the capacity of each budget (Python
//! `clients/python/tests/unit/test_claim_plan.py` and the down-slot budget of
//! `clients/python/tests/unit/test_worker_helpers.py`).
//!
//! - The route of each class follows from the children a worker runs: its
//!   own budget, the `ISOLATED` budget, or not served.
//! - The plan of a worker follows its slots, so no stored plan can disagree
//!   with them.
//! - Before the children boot, every slot is booting and counts: one per
//!   isolated slot, every seat of a shared slot, each budget only its kind.
//! - A slot that is down leaves the budget.

use super::*;

const ISOLATED: ExecutionClass = ExecutionClass::Isolated;
const SHARED_ASYNC: ExecutionClass = ExecutionClass::SharedAsync;
const SHARED_THREAD: ExecutionClass = ExecutionClass::SharedThread;

const NOT_SERVED: ClaimRoute = ClaimRoute::NotServed;
const BY_ISOLATED: ClaimRoute = ClaimRoute::ServedBy(ExecutionClass::Isolated);
const BY_SHARED_ASYNC: ClaimRoute = ClaimRoute::ServedBy(ExecutionClass::SharedAsync);
const BY_SHARED_THREAD: ClaimRoute = ClaimRoute::ServedBy(ExecutionClass::SharedThread);

fn layout(isolated: usize, shared_async: usize, shared_thread: usize) -> ChildLayout {
    ChildLayout {
        isolated,
        shared_async,
        shared_thread,
    }
}

/// The classes of `plan` that no budget claims, in the order of the stored
/// values.
fn unserved(plan: ClaimPlan) -> Vec<ExecutionClass> {
    ExecutionClass::ALL
        .into_iter()
        .filter(|class| plan.route(*class) == ClaimRoute::NotServed)
        .collect()
}

/// What the claim reads from the slots of `board`, with the seats of each
/// shared child.
fn service_of(board: &PoolBoard, async_seats: u32, thread_seats: u32) -> PoolService {
    let shared = |class: ExecutionClass, seats: u32| -> Vec<SharedSlotService> {
        board
            .phases_of(class)
            .iter()
            .map(|phase| SharedSlotService {
                service: phase.service(),
                seats,
            })
            .collect()
    };
    let isolated: Vec<SlotService> = board
        .phases_of(ISOLATED)
        .iter()
        .map(SlotPhase::service)
        .collect();
    let shared_async = shared(SHARED_ASYNC, async_seats);
    let shared_thread = shared(SHARED_THREAD, thread_seats);
    PoolService {
        isolated,
        shared_async,
        shared_thread,
    }
}

/// The plan of a worker whose children of `layout` all boot.
fn plan_of(layout: ChildLayout) -> ClaimPlan {
    service_of(&PoolBoard::new(layout), 1, 1).plan()
}

fn capacity(budget: ExecutionClass, in_service: u32) -> BudgetCapacity {
    BudgetCapacity { budget, in_service }
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

/// Python `TestRoutes::test_each_class_routes_by_the_rules`: every mix of
/// children, each class routed by the rules, written out per case. Each
/// budget claims exactly the classes routed to it.
#[test]
fn each_class_routes_by_the_rules_for_every_mix_of_children() {
    // (isolated, shared async, shared thread children), then the route of
    // ISOLATED, SHARED_ASYNC and SHARED_THREAD rows.
    let cases: [([usize; 3], [ClaimRoute; 3]); 8] = [
        ([0, 0, 0], [NOT_SERVED, NOT_SERVED, NOT_SERVED]),
        ([1, 0, 0], [BY_ISOLATED, BY_ISOLATED, BY_ISOLATED]),
        ([0, 1, 0], [NOT_SERVED, BY_SHARED_ASYNC, NOT_SERVED]),
        ([0, 0, 1], [NOT_SERVED, NOT_SERVED, BY_SHARED_THREAD]),
        ([1, 1, 0], [BY_ISOLATED, BY_SHARED_ASYNC, BY_ISOLATED]),
        ([1, 0, 1], [BY_ISOLATED, BY_ISOLATED, BY_SHARED_THREAD]),
        ([0, 1, 1], [NOT_SERVED, BY_SHARED_ASYNC, BY_SHARED_THREAD]),
        ([1, 1, 1], [BY_ISOLATED, BY_SHARED_ASYNC, BY_SHARED_THREAD]),
    ];
    for (children, routes) in cases {
        let [isolated, shared_async, shared_thread] = children;
        let plan = ClaimPlan::for_children(isolated, shared_async, shared_thread);
        for (class, route) in ExecutionClass::ALL.into_iter().zip(routes) {
            assert_eq!(
                plan.route(class),
                route,
                "children {children:?}, a {class} row"
            );
        }
        let with_children: Vec<ExecutionClass> = ExecutionClass::ALL
            .into_iter()
            .zip(children)
            .filter(|(_, count)| *count > 0)
            .map(|(class, _)| class)
            .collect();
        assert_eq!(plan.budgets(), with_children, "children {children:?}");
        for budget in ExecutionClass::ALL {
            let routed_here: Vec<ExecutionClass> = ExecutionClass::ALL
                .into_iter()
                .zip(routes)
                .filter(|(_, route)| *route == ClaimRoute::ServedBy(budget))
                .map(|(class, _)| class)
                .collect();
            assert_eq!(
                plan.served_by(budget),
                routed_here,
                "children {children:?}, the {budget} budget"
            );
        }
    }
}

/// Python `TestRoutes::test_isolated_children_never_take_a_class_with_its_own_children`.
#[test]
fn the_isolated_budget_never_claims_a_class_with_children_of_its_own() {
    let plan = ClaimPlan::for_children(1, 1, 0);
    assert_eq!(plan.route(SHARED_ASYNC), BY_SHARED_ASYNC);
    assert_eq!(plan.served_by(ISOLATED), vec![ISOLATED, SHARED_THREAD]);
    assert_eq!(plan.served_by(SHARED_ASYNC), vec![SHARED_ASYNC]);
}

/// Python `TestRoutes::test_a_worker_without_isolated_children_claims_no_isolated_rows`.
#[test]
fn a_worker_without_isolated_children_claims_no_isolated_rows() {
    let plan = ClaimPlan::for_children(0, 1, 0);
    assert_eq!(plan.route(ISOLATED), NOT_SERVED);
    assert_eq!(plan.route(SHARED_THREAD), NOT_SERVED);
    assert_eq!(unserved(plan), vec![ISOLATED, SHARED_THREAD]);
    assert_eq!(plan.budgets(), vec![SHARED_ASYNC]);
}

/// Python `TestRoutes::test_a_worker_without_children_serves_nothing`, the
/// routes and budgets.
#[test]
fn a_worker_without_children_serves_nothing() {
    let plan = ClaimPlan::for_children(0, 0, 0);
    assert_eq!(unserved(plan), ExecutionClass::ALL.to_vec());
    assert!(plan.budgets().is_empty());
    for budget in ExecutionClass::ALL {
        assert!(plan.served_by(budget).is_empty(), "the {budget} budget");
    }
}

// ---------------------------------------------------------------------------
// The plan follows the slots
// ---------------------------------------------------------------------------

/// Python `TestPlanFromChildren::test_isolated_children_give_the_isolated_plan`.
#[test]
fn isolated_children_give_the_isolated_plan() {
    let plan = plan_of(layout(4, 0, 0));
    assert_eq!(plan, ClaimPlan::for_children(1, 0, 0));
    assert_eq!(plan.budgets(), vec![ISOLATED]);
    assert_eq!(plan.served_by(ISOLATED), ExecutionClass::ALL.to_vec());
}

/// Python `TestPlanFromChildren::test_no_children_give_no_budget`.
#[test]
fn no_children_give_no_budget() {
    let plan = plan_of(layout(0, 0, 0));
    assert_eq!(plan, ClaimPlan::for_children(0, 0, 0));
    assert!(plan.budgets().is_empty());
}

/// Python `TestPlanFromChildren::test_shared_async_children_give_a_shared_async_budget`.
#[test]
fn shared_async_children_give_a_shared_async_budget() {
    let plan = plan_of(layout(1, 2, 0));
    assert_eq!(plan, ClaimPlan::for_children(1, 1, 0));
    assert_eq!(plan.budgets(), vec![ISOLATED, SHARED_ASYNC]);
}

/// Python `TestPlanFromChildren::test_only_shared_async_children_serve_no_isolated_row`.
#[test]
fn only_shared_async_children_serve_no_isolated_row() {
    let plan = plan_of(layout(0, 1, 0));
    assert_eq!(plan.budgets(), vec![SHARED_ASYNC]);
    assert_eq!(unserved(plan), vec![ISOLATED, SHARED_THREAD]);
}

/// Python `TestPlanFromChildren::test_shared_thread_children_give_a_shared_thread_budget`.
#[test]
fn shared_thread_children_give_a_shared_thread_budget() {
    let plan = plan_of(layout(1, 0, 2));
    assert_eq!(plan, ClaimPlan::for_children(1, 0, 1));
    assert_eq!(plan.route(SHARED_THREAD), BY_SHARED_THREAD);
    assert_eq!(plan.route(SHARED_ASYNC), BY_ISOLATED);
}

/// Python `TestPlanFromChildren::test_only_shared_thread_children_serve_only_thread_rows`.
#[test]
fn only_shared_thread_children_serve_only_thread_rows() {
    let plan = plan_of(layout(0, 0, 1));
    assert_eq!(plan.budgets(), vec![SHARED_THREAD]);
    assert_eq!(unserved(plan), vec![ISOLATED, SHARED_ASYNC]);
}

/// Python `TestPlanFromChildren::test_every_mode_gives_its_own_budget`.
#[test]
fn every_kind_of_child_gives_its_own_budget() {
    assert_eq!(
        plan_of(layout(1, 1, 1)).budgets(),
        vec![ISOLATED, SHARED_ASYNC, SHARED_THREAD]
    );
}

/// Python `TestPlanFromChildren::test_the_plan_follows_a_change_of_the_children`:
/// the plan is read from the slots each time, so no stored plan can
/// disagree with them.
#[test]
fn the_plan_follows_a_change_of_the_slots() {
    let mut service = service_of(&PoolBoard::new(layout(2, 0, 0)), 3, 3);
    assert_eq!(service.plan().budgets(), vec![ISOLATED]);
    service.shared_async.push(SharedSlotService {
        service: SlotService::Booting,
        seats: 3,
    });
    assert_eq!(service.plan().budgets(), vec![ISOLATED, SHARED_ASYNC]);
    assert_eq!(
        service.plan().served_by(ISOLATED),
        vec![ISOLATED, SHARED_THREAD]
    );
}

// ---------------------------------------------------------------------------
// The capacity of each budget
// ---------------------------------------------------------------------------

/// Python `TestCapacityHook::test_the_shared_thread_budget_counts_the_seats_in_service`:
/// before boot every slot is booting, so every seat counts, and each shared
/// budget counts only the seats of its own kind.
#[test]
fn before_boot_each_shared_budget_counts_every_seat_of_its_kind() {
    let service = service_of(&PoolBoard::new(layout(1, 1, 3)), 5, 4);
    assert_eq!(
        service.capacities(),
        vec![
            capacity(ISOLATED, 1),
            capacity(SHARED_ASYNC, 5),
            capacity(SHARED_THREAD, 12),
        ]
    );
}

/// Python `TestCapacityHook::test_the_shared_async_budget_counts_the_seats_in_service`.
#[test]
fn before_boot_the_shared_async_budget_counts_every_seat() {
    let service = service_of(&PoolBoard::new(layout(1, 2, 0)), 6, 1);
    assert_eq!(
        service.capacities(),
        vec![capacity(ISOLATED, 1), capacity(SHARED_ASYNC, 12)]
    );
}

/// Python `TestCapacityHook::test_the_isolated_budget_counts_the_slots_in_service`.
#[test]
fn before_boot_the_isolated_budget_counts_every_slot() {
    let service = service_of(&PoolBoard::new(layout(3, 0, 0)), 1, 1);
    assert_eq!(service.capacities(), vec![capacity(ISOLATED, 3)]);
}

/// A slot that is down after a boot failure.
fn down() -> SlotPhase {
    SlotPhase::Down(DownState {
        cause: crate::worker::executor::supervisor::BootFailureCause::ThreadStartRefused,
        detail: "the OS refused a thread of the child".to_owned(),
        down_since: chrono::Utc::now(),
        failed_boots: std::num::NonZeroU32::MIN,
    })
}

/// Python `test_worker_helpers.py::TestClaimPass::test_the_claim_budget_leaves_out_the_slots_that_are_down`:
/// the `ISOLATED` budget counts the slots that are not down, from three to
/// none; the plan keeps the budget while every slot is down.
#[test]
fn the_isolated_budget_leaves_out_the_slots_that_are_down() {
    let cases: [(Vec<usize>, u32); 4] = [
        (vec![], 3),
        (vec![1], 2),
        (vec![0, 2], 1),
        (vec![0, 1, 2], 0),
    ];
    for (down_slots, expected) in cases {
        let board = PoolBoard::new(layout(3, 0, 0));
        for index in &down_slots {
            board.set(*index, down());
        }
        let service = service_of(&board, 1, 1);
        assert_eq!(
            service.capacities(),
            vec![capacity(ISOLATED, expected)],
            "down slots {down_slots:?}"
        );
    }
}

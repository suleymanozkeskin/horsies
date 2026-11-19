//! The soft-cap start check: which held rows of a capped queue may start.
//! A held row starts while its
//! queue's count is under the cap and a child of its budget is free at that
//! moment. The count statement runs in
//! `broker::lifecycle_tests::soft_cap_start_check`; its row contract in
//! `broker::claims::tests`; the free capacity of the pools in
//! `worker::executor::slots::tests` and `shared_pool_tests`.

use chrono::TimeZone;

use super::*;
use crate::core::task::Execution;

fn task(name: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes())
}

fn generation() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 16, 9, 0, 0)
        .single()
        .expect("a valid test instant")
}

fn row(name: &str, queue_name: &str) -> ClaimedRow {
    ClaimedRow {
        id: task(name),
        task_name: "app.tasks.capped".to_owned(),
        queue_name: queue_name.to_owned(),
        input: "{}".to_owned(),
        task_options: None,
        execution: Execution::Isolated,
        is_workflow_task: false,
        claimed_at: generation(),
        retry_count: 0,
        max_retries: 0,
        trace: crate::events::RunTrace::NoCarrier,
        trace_id: crate::telemetry::trace_carrier::StoredTraceId::NoTraceId,
        workflow: crate::worker::executor::protocol::RunWorkflow::NotInWorkflow,
        priority: crate::events::Priority::try_from(100).unwrap(),
        enqueued_at: generation(),
        read: crate::broker::claims::ClaimRead::now(),
    }
}

fn capped(name: &str) -> ClaimedRow {
    row(name, "capped")
}

fn caps(queues: &[(&str, u32)]) -> StartCheckCaps {
    StartCheckCaps::of_mode(
        2,
        &queues
            .iter()
            .map(|(queue, cap)| ((*queue).to_owned(), *cap))
            .collect(),
    )
}

fn hold(queues: &[(&str, u32)], rows: Vec<ClaimedRow>) -> QueueCapHold {
    held_on(queues, rows, ExecutionClass::Isolated)
}

/// A hold of `rows`, each routed to `budget`.
fn held_on(queues: &[(&str, u32)], rows: Vec<ClaimedRow>, budget: ExecutionClass) -> QueueCapHold {
    let mut hold = QueueCapHold::new(caps(queues));
    for row in rows {
        hold.hold(row, budget);
    }
    hold
}

/// Free capacity that never limits the rows of these tests.
fn many_free() -> FreeCapacity {
    FreeCapacity {
        isolated: 100,
        ..FreeCapacity::default()
    }
}

fn free_isolated(slots: u32) -> FreeCapacity {
    FreeCapacity {
        isolated: slots,
        ..FreeCapacity::default()
    }
}

fn starts(counts: &[(&str, u64)]) -> QueueStarts {
    QueueStarts::new(
        counts
            .iter()
            .map(|(queue, count)| ((*queue).to_owned(), *count))
            .collect(),
    )
}

fn ids(admitted: &[AdmittedRow]) -> Vec<Uuid> {
    admitted.iter().map(|admitted| admitted.row.id).collect()
}

mod start_check_caps {
    use super::*;

    /// Python `test_soft_cap_mode_checks_every_capped_queue`.
    #[test]
    fn soft_cap_mode_checks_every_capped_queue() {
        let hold = QueueCapHold::new(caps(&[("capped", 3)]));
        assert!(hold.holds("capped"));
    }

    /// Python `test_hard_cap_mode_checks_no_queue`.
    #[test]
    fn hard_cap_mode_checks_no_queue() {
        let queues = HashMap::from([("capped".to_owned(), 3)]);
        assert_eq!(
            StartCheckCaps::of_mode(0, &queues),
            StartCheckCaps::default()
        );
        assert!(!QueueCapHold::new(StartCheckCaps::of_mode(0, &queues)).holds("capped"));
    }
}

mod holds {
    use super::*;

    /// Python `test_a_capped_queue_is_held`.
    #[test]
    fn a_capped_queue_is_held() {
        assert!(QueueCapHold::new(caps(&[("capped", 1)])).holds("capped"));
    }

    /// Python `test_an_uncapped_queue_is_not_held`.
    #[test]
    fn an_uncapped_queue_is_not_held() {
        assert!(!QueueCapHold::new(caps(&[("capped", 1)])).holds("free"));
    }

    /// Python `test_holding_a_row_of_an_uncapped_queue_is_a_defect`.
    #[test]
    #[should_panic(expected = "is not held by the start check")]
    fn holding_a_row_of_an_uncapped_queue_is_a_defect() {
        QueueCapHold::new(caps(&[("capped", 1)])).hold(row("t1", "free"), ExecutionClass::Isolated);
    }

    /// A pass with no held row sends no statement.
    #[test]
    fn no_held_row_needs_no_check() {
        assert_eq!(
            QueueCapHold::new(caps(&[("capped", 1)])).check("worker-1"),
            None
        );
    }

    /// The check names each queue with held rows once, in the order of its
    /// first row, and the claims this worker admitted.
    #[test]
    fn the_check_names_the_held_queues_and_the_started_claims() {
        let mut hold = hold(
            &[("a", 1), ("b", 2)],
            vec![row("a1", "a"), row("b1", "b"), row("a2", "a")],
        );
        let admitted = hold.admit(&starts(&[("b", 2)]), &many_free());

        let check = hold.check("worker-1").expect("rows are held");

        assert_eq!(ids(&admitted), vec![task("a1")]);
        assert_eq!(check.worker_id, "worker-1");
        assert_eq!(check.queues, vec!["b".to_owned(), "a".to_owned()]);
        assert_eq!(check.started, vec![(task("a1"), generation())]);
    }
}

mod admit {
    use super::*;

    /// Python `test_a_queue_under_its_cap_starts_its_row`.
    #[test]
    fn a_queue_under_its_cap_starts_its_row() {
        let mut hold = hold(&[("capped", 2)], vec![capped("t1")]);

        let admitted = hold.admit(&starts(&[("capped", 1)]), &many_free());

        assert_eq!(ids(&admitted), vec![task("t1")]);
        assert_eq!(hold.held_count(), 0);
        assert_eq!(hold.started_claims(), vec![(task("t1"), generation())]);
    }

    /// Python `test_a_queue_at_its_cap_keeps_its_row`.
    #[test]
    fn a_queue_at_its_cap_keeps_its_row() {
        let mut hold = hold(&[("capped", 2)], vec![capped("t1")]);

        let admitted = hold.admit(&starts(&[("capped", 2)]), &many_free());

        assert!(admitted.is_empty());
        assert_eq!(hold.held_count(), 1);
        assert!(hold.started_claims().is_empty());
    }

    /// Python `test_a_queue_with_no_count_has_0`.
    #[test]
    fn a_queue_with_no_count_has_0() {
        let mut hold = hold(&[("capped", 1)], vec![capped("t1")]);

        assert_eq!(
            ids(&hold.admit(&starts(&[]), &many_free())),
            vec![task("t1")]
        );
    }

    /// Python `test_rows_start_in_claim_order_up_to_the_room`.
    #[test]
    fn rows_start_in_claim_order_up_to_the_room() {
        let mut hold = hold(
            &[("capped", 3)],
            vec![capped("t1"), capped("t2"), capped("t3")],
        );

        let admitted = hold.admit(&starts(&[("capped", 1)]), &many_free());

        assert_eq!(ids(&admitted), vec![task("t1"), task("t2")]);
        assert_eq!(hold.drain(), vec![capped("t3")]);
    }

    /// Python `test_queues_have_their_own_room`.
    #[test]
    fn queues_have_their_own_room() {
        let mut hold = hold(
            &[("a", 1), ("b", 1)],
            vec![row("a1", "a"), row("a2", "a"), row("b1", "b")],
        );

        let admitted = hold.admit(&starts(&[]), &many_free());

        assert_eq!(ids(&admitted), vec![task("a1"), task("b1")]);
        assert_eq!(hold.held_queues(), vec!["a".to_owned()]);
    }

    /// Python `test_a_cap_of_0_starts_nothing`.
    #[test]
    fn a_cap_of_0_starts_nothing() {
        let mut hold = hold(&[("capped", 0)], vec![capped("t1")]);

        assert!(hold.admit(&starts(&[]), &many_free()).is_empty());
    }

    /// A row that a pass kept is admitted by a later pass whose count has
    /// room.
    #[test]
    fn a_kept_row_starts_on_a_later_pass_with_room() {
        let mut hold = hold(&[("capped", 1)], vec![capped("t1")]);
        assert!(hold
            .admit(&starts(&[("capped", 1)]), &many_free())
            .is_empty());

        assert_eq!(
            ids(&hold.admit(&starts(&[("capped", 0)]), &many_free())),
            vec![task("t1")]
        );
    }
}

/// A held row under its cap starts only while a child of its route is free,
/// so it goes to a child at once and never waits in a pool's queue (#1138).
mod admit_only_with_a_free_child {
    use super::*;

    /// Python `test_a_row_under_its_cap_with_no_free_child_stays_held`.
    #[test]
    fn a_row_under_its_cap_with_no_free_child_stays_held() {
        let mut hold = hold(&[("capped", 1)], vec![capped("t1")]);

        assert!(hold.admit(&starts(&[]), &free_isolated(0)).is_empty());
        assert_eq!(hold.held_count(), 1);
        assert!(hold.started_claims().is_empty());
    }

    /// Python `test_a_budget_with_no_free_capacity_entry_has_0`: a row of
    /// the shared async budget does not take a free isolated slot.
    #[test]
    fn a_free_child_of_another_budget_does_not_take_the_row() {
        let mut hold = held_on(
            &[("capped", 1)],
            vec![capped("t1")],
            ExecutionClass::SharedAsync,
        );

        assert!(hold.admit(&starts(&[]), &free_isolated(3)).is_empty());
        assert_eq!(hold.held_count(), 1);
    }

    /// Python `test_the_free_capacity_bounds_the_rows_started_in_one_pass`.
    #[test]
    fn the_free_capacity_bounds_the_rows_started_in_one_pass() {
        let mut hold = hold(
            &[("capped", 5)],
            vec![capped("t1"), capped("t2"), capped("t3")],
        );

        let admitted = hold.admit(&starts(&[]), &free_isolated(2));

        assert_eq!(ids(&admitted), vec![task("t1"), task("t2")]);
        assert_eq!(hold.drain(), vec![capped("t3")]);
    }

    /// Python `test_queues_that_share_a_budget_share_its_free_capacity`.
    #[test]
    fn queues_that_share_a_budget_share_its_free_capacity() {
        let mut hold = hold(&[("a", 1), ("b", 1)], vec![row("a1", "a"), row("b1", "b")]);

        let admitted = hold.admit(&starts(&[]), &free_isolated(1));

        assert_eq!(ids(&admitted), vec![task("a1")]);
        assert_eq!(hold.held_queues(), vec!["b".to_owned()]);
    }

    /// Python `test_a_row_of_a_budget_with_room_starts_while_another_budget_is_full`.
    #[test]
    fn a_row_of_a_budget_with_room_starts_while_another_budget_is_full() {
        let mut hold = hold(&[("capped", 2)], vec![capped("t1")]);
        hold.hold(capped("t2"), ExecutionClass::SharedThread);
        let free = FreeCapacity {
            isolated: 0,
            shared_async: 0,
            shared_thread: 1,
        };

        let admitted = hold.admit(&starts(&[]), &free);

        assert_eq!(ids(&admitted), vec![task("t2")]);
        assert_eq!(hold.drain(), vec![capped("t1")]);
    }

    /// Python `test_a_queue_at_its_cap_keeps_its_row_with_a_free_child`.
    #[test]
    fn a_queue_at_its_cap_keeps_its_row_with_a_free_child() {
        let mut hold = hold(&[("capped", 1)], vec![capped("t1")]);

        assert!(hold
            .admit(&starts(&[("capped", 1)]), &free_isolated(1))
            .is_empty());
    }

    /// Python `test_a_kept_row_starts_on_a_later_pass_with_a_free_child`.
    #[test]
    fn a_kept_row_starts_on_a_later_pass_with_a_free_child() {
        let mut hold = hold(&[("capped", 1)], vec![capped("t1")]);
        assert!(hold.admit(&starts(&[]), &free_isolated(0)).is_empty());

        let admitted = hold.admit(&starts(&[]), &free_isolated(1));

        assert_eq!(ids(&admitted), vec![task("t1")]);
        assert_eq!(hold.started_claims(), vec![(task("t1"), generation())]);
    }
}

mod end_and_drain {
    use super::*;

    /// Python `test_end_forgets_a_started_row`: the claim leaves the check
    /// when its guard drops, at the end of its outcome write.
    #[test]
    fn a_dropped_guard_forgets_its_claim() {
        let mut hold = hold(&[("capped", 1)], vec![capped("t1")]);
        let admitted = hold.admit(&starts(&[]), &many_free());
        assert_eq!(hold.started_claims().len(), 1);

        drop(admitted);

        assert!(hold.started_claims().is_empty());
    }

    /// A later claim of the same task is another claim: each counts until
    /// its own guard drops.
    #[test]
    fn two_claims_of_one_task_count_apart() {
        let later = ClaimedRow {
            claimed_at: generation() + chrono::Duration::seconds(5),
            ..capped("t1")
        };
        let mut hold = hold(&[("capped", 2)], vec![capped("t1"), later]);
        let mut admitted = hold.admit(&starts(&[]), &many_free());
        assert_eq!(hold.started_claims().len(), 2);

        let first = admitted.remove(0);
        drop(first);

        assert_eq!(
            hold.started_claims(),
            vec![(task("t1"), generation() + chrono::Duration::seconds(5))]
        );
    }

    /// Python `test_drain_takes_every_held_row_in_claim_order`.
    #[test]
    fn drain_takes_every_held_row_in_claim_order() {
        let mut hold = hold(&[("a", 1), ("b", 1)], vec![row("a1", "a"), row("b1", "b")]);

        assert_eq!(hold.drain(), vec![row("a1", "a"), row("b1", "b")]);
        assert_eq!(hold.held_count(), 0);
        assert!(hold.held_queues().is_empty());
    }
}

//! Every terminalization kind names one test where its writer applies and
//! one where its writer refuses. The `match` is exhaustive, so a new kind
//! does not compile without its scenarios; the test checks that each named
//! test exists in its source file.

use crate::core::lifecycle::TerminalizationKind;

/// A source file of the core suite that holds terminal-writer tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Suite {
    Matrix,
    Functions,
    AttemptWrites,
}

impl Suite {
    fn source(self) -> &'static str {
        match self {
            Self::Matrix => include_str!("../terminalization_matrix.rs"),
            Self::Functions => include_str!("terminalization_functions.rs"),
            Self::AttemptWrites => include_str!("../attempt_writes/tests.rs"),
        }
    }
}

/// A named test in a suite.
#[derive(Debug, Clone, Copy)]
struct Scenario {
    suite: Suite,
    test: &'static str,
}

const fn matrix(test: &'static str) -> Scenario {
    Scenario {
        suite: Suite::Matrix,
        test,
    }
}

const fn functions(test: &'static str) -> Scenario {
    Scenario {
        suite: Suite::Functions,
        test,
    }
}

/// The test where the writer of `kind` applies, and the test where it
/// refuses. A matrix test that checks both arms is named twice.
fn scenarios_of(kind: TerminalizationKind) -> (Scenario, Scenario) {
    match kind {
        TerminalizationKind::CompleteLocked => (
            matrix("complete_locked_moves_to_history_and_preserves_last_claim_evidence"),
            matrix("complete_locked_wrong_worker_is_lost_claim_and_absent_is_absent"),
        ),
        TerminalizationKind::CompleteFused => (
            matrix("fused_applies_with_attempt_notify_and_generation_fence"),
            matrix("fused_stale_generation_is_lost_claim"),
        ),
        TerminalizationKind::FailRunning => (
            Scenario {
                suite: Suite::AttemptWrites,
                test: "a_plain_failure_moves_the_row_with_its_attempt",
            },
            functions("a_stale_failure_is_a_conflict_not_a_replay"),
        ),
        TerminalizationKind::FailStale => (
            matrix("fail_stale_applies_on_silence_and_refuses_on_heartbeat"),
            functions("a_row_that_never_started_refuses"),
        ),
        TerminalizationKind::ExpireClaimed => (
            matrix("expire_owned_claim_applies_for_any_generation_and_reports_deadline"),
            functions("a_row_with_no_deadline_refuses"),
        ),
        TerminalizationKind::ExpirePending => (
            matrix("expire_pending_batch_is_bounded_and_deadline_ordered"),
            functions("a_row_a_concurrent_claim_holds_is_stepped_around"),
        ),
        TerminalizationKind::CancelAdmin => (
            matrix("cancel_task_applies_and_refuses_a_running_task_without_the_opt_in"),
            matrix("cancel_task_refuses_workflow_tasks_and_never_overwrites_terminal"),
        ),
        TerminalizationKind::CancelOrphan => (
            matrix("cancel_owned_orphan_applies_only_without_runnable_linkage"),
            functions("a_stale_generation_is_a_lost_claim_even_with_a_live_link"),
        ),
        TerminalizationKind::CancelOrphanSweep => (
            matrix("orphan_sweep_cancels_unlinked_and_retains_linked"),
            functions("a_locked_orphan_is_stepped_around"),
        ),
        TerminalizationKind::PauseAbandonClaim => (
            matrix("abandon_owned_node_applies_and_fences_generation"),
            functions("pause_kinds_are_equivalent_but_workflow_cancel_is_not"),
        ),
        TerminalizationKind::PauseAbandonClaimBatch => (
            matrix("abandon_owned_nodes_reports_per_ordinal_in_caller_order"),
            functions("one_ordered_outcome_per_input_across_every_outcome_abandon"),
        ),
        TerminalizationKind::PauseAbandonWorkflow => (
            functions("pause_applies_only_under_both_live_guards"),
            matrix("abandon_nodes_of_paused_workflows_reaches_only_paused_enqueued"),
        ),
        TerminalizationKind::WorkflowCancelClaim => (
            matrix("cancel_owned_node_honors_requeued_pending_carveout"),
            functions("a_stale_claim_is_lost"),
        ),
        TerminalizationKind::WorkflowCancelClaimBatch => (
            matrix("cancel_owned_nodes_batch_applies_owned_claims"),
            functions("one_ordered_outcome_per_input_across_every_outcome_cancel"),
        ),
        TerminalizationKind::WorkflowCancelWorkflow => (
            functions("cancel_accepts_all_three_legacy_task_source_states"),
            functions("cancel_refuses_a_live_workflow_or_non_enqueued_link"),
        ),
    }
}

/// Python `test_every_writer_has_a_scenario`: coverage is driven by the
/// vocabulary, not by the test files.
#[test]
fn every_writer_has_a_scenario() {
    for kind in TerminalizationKind::ALL {
        let (applied, refused) = scenarios_of(kind);
        for scenario in [applied, refused] {
            let declaration = format!("async fn {}()", scenario.test);
            assert!(
                scenario.suite.source().contains(&declaration),
                "{}: no test {:?} in {:?}",
                kind.as_str(),
                scenario.test,
                scenario.suite
            );
        }
    }
}

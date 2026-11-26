// The layer files of the end-to-end suite, in run order, with the wall bound of each process, and the mirrored
// scenarios that wait for a package of the TypeScript client. The runner (`run.ts`) runs the files and prints the
// pending list; no pending scenario has a test body.

/** One layer file: its path under `tests/e2e/layers/` and the wall bound of its process, in seconds. */
export type LayerFile = { readonly name: string; readonly boundSeconds: number };

/** The layer files, in run order. */
export const LAYER_FILES: readonly LayerFile[] = [
  { name: "layer1-tasks", boundSeconds: 1_800 },
  { name: "layer2-cluster", boundSeconds: 1_500 },
  { name: "layer4-workflow-structure", boundSeconds: 600 },
  { name: "layer5-workflow-dataflow", boundSeconds: 600 },
  { name: "layer6-workflow-advanced", boundSeconds: 900 },
  { name: "layer7-subworkflows", boundSeconds: 1_200 },
  { name: "child-model", boundSeconds: 1_200 },
  { name: "typescript-child", boundSeconds: 1_200 },
  { name: "stored-timeout-refusal", boundSeconds: 600 },
  { name: "retention", boundSeconds: 600 },
  { name: "check-live-command", boundSeconds: 600 },
];

/** Mirrored scenarios that need a package that has not merged, by the issue that adds the package. */
export type PendingScenarios = { readonly issue: number; readonly needs: string; readonly mirrors: readonly string[] };

/** The scenarios of the suite's design that no file runs yet. */
export const PENDING_SCENARIOS: readonly PendingScenarios[] = [
  {
    issue: 2336,
    needs: "schedules and the scheduler command",
    mirrors: ["layer 3: Rust layer3_scheduler.rs (10), Python test_layer3_scheduler.py (12)"],
  },
  {
    issue: 2459,
    needs: "workflows with no output node",
    mirrors: [
      "the result of the terminal nodes of a workflow with no output: Python test_layer4_workflow_structure.py::test_default_terminal_results, the outputless reads of test_layer7_subworkflows.py (test_outputless_success_policy_keeps_failed_subworkflow_result, test_subworkflow_recovery_outputless_recovery, test_subworkflow_deep_outputless_nested_failed_optional, test_subworkflow_deep_outputless_all_failed_required, test_subworkflow_deep_mixed_paths); layers 4 and 7 run each of these cases with an output node",
    ],
  },
  {
    issue: 2337,
    needs: "monitoring, cancel and health reads",
    mirrors: [
      "layer 8: Rust layer8_health.rs (6), Python test_worker_health.py (5)",
      "Rust cancel_running.rs (6), Python test_cancel_running.py (3): runtime.signal with OPERATOR, the cancel grace of an isolated run",
      "Rust layer1_tasks.rs::test_task_cancelled, ::test_task_cancelled_get_result",
      "Rust lifecycle_record.rs (2), read through the monitoring task detail",
      "Rust event_handlers.rs (1), Python test_event_handlers.py (1): the event registration",
    ],
  },
  {
    issue: 2335,
    needs: "SHARED_THREAD seats on worker threads (Node only; SHARED_THREAD stays off on Bun)",
    mirrors: [
      "Rust child_model.rs::thread_tasks_run_at_once_and_a_timeout_leaves_neighbours_finished",
      "Rust child_model.rs::a_timed_out_thread_does_not_overlap_its_retry",
      "Rust child_model.rs::shared_thread_runs_read_their_own_fencing_tokens",
      "Python test_shared_thread_tasks.py (3)",
      "sync and async thread bodies; a provider runs once in each seat isolate; a seat isolate loads no addon; two CPU-heavy bodies at once; a blocked main thread gives loop_blocked",
    ],
  },
  {
    issue: 2338,
    needs: "observability client parts",
    mirrors: ["the attempt spans and the worker-ended spans (Python test_worker_ended_spans.py)"],
  },
  {
    issue: 2492,
    needs: "a way to stop the core of a task child for a test",
    mirrors: ["Python test_child_core_stopped.py (3)"],
  },
];

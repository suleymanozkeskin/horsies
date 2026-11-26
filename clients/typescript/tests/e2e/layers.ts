// The layer files of the end-to-end suite, in run order, with the wall bound of each process, and the mirrored
// scenarios that wait for a package of the TypeScript client. The runner (`run.ts`) runs the files and prints the
// pending list; no pending scenario has a test body.

/** One layer file: its path under `tests/e2e/layers/` and the wall bound of its process, in seconds. */
export type LayerFile = { readonly name: string; readonly boundSeconds: number };

/** The layer files, in run order. */
export const LAYER_FILES: readonly LayerFile[] = [
  { name: "layer1-tasks", boundSeconds: 1_800 },
  { name: "layer2-cluster", boundSeconds: 1_500 },
  { name: "layer3-scheduler", boundSeconds: 1_200 },
  { name: "child-model", boundSeconds: 1_200 },
  { name: "typescript-child", boundSeconds: 1_200 },
  { name: "stored-timeout-refusal", boundSeconds: 600 },
  { name: "retention", boundSeconds: 600 },
  { name: "check-live-command", boundSeconds: 600 },
  { name: "layer8-health", boundSeconds: 900 },
  { name: "cancel-running", boundSeconds: 900 },
  { name: "event-handlers", boundSeconds: 300 },
  { name: "lifecycle-record", boundSeconds: 300 },
  { name: "worker-ended-spans", boundSeconds: 600 },
];

/** Mirrored scenarios that need a package that has not merged, by the issue that adds the package. */
export type PendingScenarios = { readonly issue: number; readonly needs: string; readonly mirrors: readonly string[] };

/** The scenarios of the suite's design that no file runs yet. */
export const PENDING_SCENARIOS: readonly PendingScenarios[] = [
  {
    issue: 2331,
    needs: "workflows",
    mirrors: [
      "layer 4: Rust layer4_workflow_structure.rs (11), Python test_layer4_workflow_structure.py (20)",
      "layer 5: Rust layer5_workflow_dataflow.rs (14), Python test_layer5_workflow_dataflow.py (12)",
      "layer 6: Rust layer6_workflow_advanced.rs (22), Python test_layer6_workflow_advanced.py (19)",
      "layer 7: Rust layer7_subworkflows.rs (12), Python test_layer7_subworkflows.py (47), test_layer7_subworkflow_priority.py (1)",
      "Rust stored_timeout_refusal.rs::a_refused_timeout_fails_its_workflow_node",
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
    issue: 2500,
    needs: "the start span of a workflow start, whose carrier the workflow rows store",
    mirrors: ["Python test_worker_ended_spans.py::test_a_workflow_task_killed_at_its_timeout_gets_a_span_with_its_node: the span is in the trace of the start"],
  },
  {
    issue: 2492,
    needs: "a way to stop the core of a task child for a test",
    mirrors: ["Python test_child_core_stopped.py (3)"],
  },
];

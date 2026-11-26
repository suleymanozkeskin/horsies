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
  { name: "shared-thread", boundSeconds: 1_200 },
  { name: "child-core-panic", boundSeconds: 600 },
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
    issue: 2500,
    needs: "the start span of a workflow start, whose carrier the workflow rows store",
    mirrors: ["Python test_worker_ended_spans.py::test_a_workflow_task_killed_at_its_timeout_gets_a_span_with_its_node: the span is in the trace of the start"],
  },
];

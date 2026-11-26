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
  { name: "layer4-workflow-structure", boundSeconds: 600 },
  { name: "layer5-workflow-dataflow", boundSeconds: 600 },
  { name: "layer6-workflow-advanced", boundSeconds: 900 },
  { name: "layer7-subworkflows", boundSeconds: 1_200 },
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
    issue: 2459,
    needs: "workflows with no output node",
    mirrors: [
      "the result of the terminal nodes of a workflow with no output: Python test_layer4_workflow_structure.py::test_default_terminal_results, the outputless reads of test_layer7_subworkflows.py (test_outputless_success_policy_keeps_failed_subworkflow_result, test_subworkflow_recovery_outputless_recovery, test_subworkflow_deep_outputless_nested_failed_optional, test_subworkflow_deep_outputless_all_failed_required, test_subworkflow_deep_mixed_paths); layers 4 and 7 run each of these cases with an output node",
    ],
  },
];

// The layer files of the end-to-end suite, in run order, with the wall bound of each process, and the mirrored
// scenarios that wait for a package of the TypeScript client. The runner (`run.ts`) runs the files and prints the
// pending list; no pending scenario has a test body.

/** One layer file: its path under `tests/e2e/layers/` and the wall bound of its process, in seconds. */
export type LayerFile = { readonly name: string; readonly boundSeconds: number };

/** The layer files, in run order. */
export const LAYER_FILES: readonly LayerFile[] = [
  { name: "layer1-tasks", boundSeconds: 1_800 },
  { name: "layer2-cluster", boundSeconds: 1_500 },
  { name: "child-model", boundSeconds: 1_200 },
  { name: "typescript-child", boundSeconds: 1_200 },
  { name: "shared-thread", boundSeconds: 1_200 },
  { name: "child-core-panic", boundSeconds: 600 },
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
    issue: 2338,
    needs: "observability client parts",
    mirrors: ["the attempt spans and the worker-ended spans (Python test_worker_ended_spans.py)"],
  },
];

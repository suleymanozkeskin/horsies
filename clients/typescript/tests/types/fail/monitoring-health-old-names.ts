// expect: TS2339 at 9, TS2339 at 10, TS2339 at 11, TS2339 at 12, TS2551 at 13, TS2339 at 17
// The worker snapshot, the task rows and the execution class report take the names of the Python and Rust clients:
// `childCount`, `claimLimits`, `executionClassBudgets`, `taskId`, `queue`.

import type { ExecutionClassReport, StaleTaskRow, WorkerStateSnapshot } from "../../../horsies/src/index.ts";

export function oldNames(snapshot: WorkerStateSnapshot, row: StaleTaskRow, report: ExecutionClassReport): unknown[] {
  return [
    snapshot.processes,
    snapshot.maxClaimBatch,
    snapshot.maxClaimPerWorker,
    snapshot.classCapacity,
    row.lastHeartbeat,
    row.taskId,
    report.liveWindow,
    report.streams.map((stream) => stream.queue),
    report.streams.map((stream) => stream.queueName),
  ];
}

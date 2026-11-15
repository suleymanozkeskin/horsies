// Pure helpers for the execution class report: which pending streams no live
// worker can run, and the text the panels show for streams and budgets.

import { formatElapsed, formatRelative } from '@/lib/format-duration';
import type {
  BudgetLoad,
  ClassReport,
  PendingStream,
  StreamWait,
} from '@/types/workers';

/** The streams that no live worker can run: no worker serves the class on
 * the queue, or every serving worker has no child in service for it. */
export function uncoveredStreams(report: ClassReport): PendingStream[] {
  return report.streams.filter(stream => stream.coverage.kind !== 'served');
}

/** The budgets of one worker, or `undefined` when the report has no live
 * snapshot of it (a stale worker, or a report not loaded yet). */
export function budgetsOf(
  report: ClassReport | undefined,
  workerId: string
): BudgetLoad[] | undefined {
  return report?.workers.find(worker => worker.worker_id === workerId)?.budgets;
}

/** One line for the fleet warning: what waits, and what to start or check. */
export function fleetWarningText(stream: PendingStream): string {
  const subject = `PENDING ${stream.execution} tasks on queue '${stream.queue_name}'`;
  switch (stream.coverage.kind) {
    case 'unserved':
      return stream.execution === 'ISOLATED'
        ? `${subject} wait: no live worker serves this class. Start a worker with isolated children on this queue.`
        : `${subject} wait: no live worker serves this class. Start a worker with ${stream.execution} children or with isolated children on this queue.`;
    case 'no_capacity':
      return `${subject} wait: ${stream.coverage.workers} live worker(s) serve this class, and none has a child in service for it. Check their down slots.`;
    case 'served':
      return `${subject} are served by ${stream.coverage.workers} live worker(s).`;
  }
}

/** The coverage cell of the stream table. */
export function coverageText(stream: PendingStream): string {
  switch (stream.coverage.kind) {
    case 'served':
      return `${stream.coverage.workers} worker(s) · capacity ${stream.coverage.capacity}`;
    case 'no_capacity':
      return `${stream.coverage.workers} worker(s) · no capacity`;
    case 'unserved':
      return 'no live worker';
  }
}

/** The wait cell of the stream table. */
export function waitText(wait: StreamWait): string {
  switch (wait.kind) {
    case 'waiting':
      return formatElapsed(wait.wait_s);
    case 'none_due':
      return `next due ${formatRelative(wait.next_due_at)}`;
  }
}

/** `3/8 free`: the free capacity of a budget out of its capacity in service. */
export function budgetFreeText(budget: BudgetLoad): string {
  return `${budget.free}/${budget.capacity} free`;
}

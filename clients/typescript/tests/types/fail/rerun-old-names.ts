// expect: TS2724 at 7, TS2339 at 11, TS2339 at 13, TS2353 at 16, TS2353 at 19
// The rerun takes the names of the Python and Rust clients: `app.rerunTask`, `app.rerunTaskIn`, the policy type
// `RerunEnqueuePolicy`, the command fields `goodUntil` and `idempotencyKey`, and the policy field `retainRerunInput`.

import type { CallerTransaction, Horsies, RerunCommand, RerunEnqueuePolicy } from "../../../horsies/src/index.ts";
import type { TaskId } from "../../../horsies/src/index.ts";
import type { RerunPolicy } from "../../../horsies/src/index.ts";

export async function rerunBoth(app: Horsies, transaction: CallerTransaction, command: RerunCommand, policy: RerunEnqueuePolicy): Promise<void> {
  // The app's own transaction.
  await app.rerun(command, policy);
  // The caller's transaction.
  await app.rerunIn(transaction, command, policy);
}

export const keyed = (sourceTaskId: TaskId): RerunCommand => ({ sourceTaskId, deadline: { kind: "none" } });

export const policy: RerunEnqueuePolicy = {
  rerunInput: "keep",
  retention: { kind: "forever" },
  reservationWindow: Temporal.Duration.from({ hours: 1 }),
};

export type Old = RerunPolicy;

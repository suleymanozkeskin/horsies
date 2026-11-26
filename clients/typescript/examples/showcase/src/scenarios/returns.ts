// The start of a returns review: a return id from the return sequence, then the workflow. The workflow's first node
// (`receive_return`) books the return in.

import type { Result } from "horsies";

import { err, ok } from "../result.ts";
import { storeFailureText } from "../store/connection.ts";
import { nextReturnNumber } from "../store/returns.ts";
import { returnsReview } from "../workflows/returns-review.ts";
import { type ScenarioContext, sendErrorText } from "./context.ts";

/** The digits of the number of a return id: `RET-00001`. */
const RETURN_DIGITS = 5;

/** The return id and the workflow id of a started return. */
export type StartedReturn = { readonly returnId: string; readonly workflowId: string };

/** Start the returns review of `quantity` units of `sku` of `orderId`. */
export async function startReturn(
  context: ScenarioContext,
  orderId: string,
  sku: string,
  quantity: number,
): Promise<Result<StartedReturn, string>> {
  const returnNumber = await nextReturnNumber(context.store);

  if (!returnNumber.ok) {
    return err(storeFailureText(returnNumber.error));
  }

  const returnId = `RET-${String(returnNumber.value).padStart(RETURN_DIGITS, "0")}`;
  const started = await context.showcase.app.start(returnsReview, { returnId, orderId, sku, quantity });

  return started.ok
    ? ok({ returnId, workflowId: started.value.workflowId })
    : err(`start returns_review ${returnId}: ${sendErrorText(started.error)}`);
}

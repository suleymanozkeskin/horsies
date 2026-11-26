// The implementations of the return tasks. A damaged item is an error of the inspection, which pauses the returns
// review workflow; the settle node takes failed dependencies, so a resume of the workflow writes the item off.

import type { BodyResult, Horsies, TaskOutcome } from "horsies";

import { inspectItem, receiveReturn, restockOrWriteoff } from "../contracts/returns.ts";
import type { Inspection, RestockDecision, ReturnCase, ReturnReceipt } from "../domain.ts";
import { draw } from "../simulate.ts";
import { restockUnits } from "../store/catalog.ts";
import type { Store } from "../store/connection.ts";
import { closeReturn, getReturn, openReturn, recordInspection } from "../store/returns.ts";
import { RETURN_DAMAGE_RATE } from "../tuning.ts";
import { type ResultsOf, storeFailure, type StoreKey } from "./support.ts";

/** The notes of an inspection that finds no damage. */
const RESELLABLE_NOTES = "no visible wear, tags intact";

type ReceiveResult = BodyResult<typeof ReturnReceipt, "STORE_UNAVAILABLE">;

type InspectResult = BodyResult<typeof Inspection, "RETURN_NOT_FOUND" | "DAMAGED_ITEM" | "STORE_UNAVAILABLE">;

type SettleResult = BodyResult<typeof RestockDecision, "RETURN_NOT_FOUND" | "UNKNOWN_SKU" | "STORE_UNAVAILABLE">;

type SettleResults = ResultsOf<typeof restockOrWriteoff>;

/** One returned item: `quantity` units of `sku` of the order `orderId`, under the case `returnId`. */
export type ReturnRequest = {
  readonly returnId: string;
  readonly orderId: string;
  readonly sku: string;
  readonly quantity: number;
};

/** Book the returned item in. A case of the id that exists stays as it is. `STORE_UNAVAILABLE`. */
export async function receive(
  store: Store,
  request: ReturnRequest,
  { ok, err }: ResultsOf<typeof receiveReturn>,
): Promise<ReceiveResult> {
  const opened = await openReturn(store, {
    ...request,
    status: "received",
    condition: null,
    createdAt: Temporal.Now.instant(),
  });

  if (!opened.ok) {
    return err({ code: "STORE_UNAVAILABLE", ...storeFailure(opened.error) });
  }

  return ok(request);
}

/**
 * Inspect the item of `returnId` and record its condition. A drawn damage is `DAMAGED_ITEM`, which no policy
 * retries. `RETURN_NOT_FOUND`; `STORE_UNAVAILABLE`.
 */
export async function inspect(store: Store, returnId: string, { ok, err }: ResultsOf<typeof inspectItem>): Promise<InspectResult> {
  const read = await getReturn(store, returnId);

  if (!read.ok) {
    return err({ code: "STORE_UNAVAILABLE", ...storeFailure(read.error) });
  }

  if (read.value.kind === "notFound") {
    return err({ code: "RETURN_NOT_FOUND", message: `no return ${returnId}` });
  }

  const returnCase = read.value.value;
  const condition = draw(RETURN_DAMAGE_RATE, [returnId, "damage"]) ? "damaged" : "resellable";
  const recorded = await recordInspection(store, returnId, condition);

  if (!recorded.ok) {
    return err({ code: "STORE_UNAVAILABLE", ...storeFailure(recorded.error) });
  }

  switch (condition) {
    case "damaged":
      return err({
        code: "DAMAGED_ITEM",
        message: `${returnCase.sku} came back damaged and cannot be resold`,
        data: { returnId, sku: returnCase.sku, quantity: returnCase.quantity },
      });
    case "resellable":
      return ok({ returnId, sku: returnCase.sku, condition, notes: RESELLABLE_NOTES });
    default: {
      const unexpected: never = condition;

      throw new TypeError(`inspect: a condition outside its type: ${String(unexpected)}`);
    }
  }
}

/**
 * Settle the case `returnId` by the outcome of its inspection: a resellable item goes back on the shelf, a failed
 * inspection writes the item off. `RETURN_NOT_FOUND`; `UNKNOWN_SKU`; `STORE_UNAVAILABLE`.
 */
export async function settle(
  store: Store,
  returnId: string,
  inspection: TaskOutcome<Inspection>,
  { ok, err }: SettleResults,
): Promise<SettleResult> {
  const read = await getReturn(store, returnId);

  if (!read.ok) {
    return err({ code: "STORE_UNAVAILABLE", ...storeFailure(read.error) });
  }

  if (read.value.kind === "notFound") {
    return err({ code: "RETURN_NOT_FOUND", message: `no return ${returnId}` });
  }

  const returnCase = read.value.value;

  switch (inspection.kind) {
    case "succeeded":
      return restock(store, returnCase, { ok, err });
    case "failed":
      return writeOff(store, returnCase, { ok, err });
    default: {
      const unexpected: never = inspection;

      throw new TypeError(`settle: an outcome outside its type: ${String(unexpected)}`);
    }
  }
}

async function restock(store: Store, returnCase: ReturnCase, { ok, err }: SettleResults): Promise<SettleResult> {
  const restocked = await restockUnits(store, returnCase.sku, returnCase.quantity);

  if (!restocked.ok) {
    return err({ code: "STORE_UNAVAILABLE", ...storeFailure(restocked.error) });
  }

  if (restocked.value.kind === "notFound") {
    return err({ code: "UNKNOWN_SKU", message: `${returnCase.sku} is not in the catalog` });
  }

  const closed = await closeReturn(store, returnCase.returnId, "restocked");

  if (!closed.ok) {
    return err({ code: "STORE_UNAVAILABLE", ...storeFailure(closed.error) });
  }

  return ok({
    returnId: returnCase.returnId,
    sku: returnCase.sku,
    quantity: returnCase.quantity,
    outcome: { kind: "restocked", availableAfter: restocked.value.value },
  });
}

async function writeOff(store: Store, returnCase: ReturnCase, { ok, err }: SettleResults): Promise<SettleResult> {
  const closed = await closeReturn(store, returnCase.returnId, "written_off");

  if (!closed.ok) {
    return err({ code: "STORE_UNAVAILABLE", ...storeFailure(closed.error) });
  }

  return ok({
    returnId: returnCase.returnId,
    sku: returnCase.sku,
    quantity: returnCase.quantity,
    outcome: { kind: "writtenOff" },
  });
}

/** Implement the return tasks on `app`; each one uses the store of `store`. */
export function implementReturnTasks(app: Horsies, store: StoreKey): void {
  app.implement(receiveReturn, async (input, runtime) => receive(runtime.state(store), input, runtime));
  app.implement(inspectItem, async ({ returnId }, runtime) => inspect(runtime.state(store), returnId, runtime));
  app.implement(restockOrWriteoff, async ({ returnId, inspection }, runtime) =>
    settle(runtime.state(store), returnId, inspection, runtime),
  );
}

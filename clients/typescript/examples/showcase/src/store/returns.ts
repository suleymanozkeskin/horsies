// Return cases: a returned item from its receipt to its restock or write-off.

import type { Result } from "horsies";

import type { ItemCondition, ReturnCase, ReturnStatus } from "../domain.ts";
import { bigintColumn, instantColumn, intColumn, nullableTextColumn, textColumn } from "./columns.ts";
import { changedRows, firstRow, found, inTransaction, type Lookup, NOT_FOUND, type Store, type StoreFailure } from "./connection.ts";

const RETURN_STATUSES: readonly ReturnStatus[] = ["received", "inspected", "restocked", "written_off"];
const ITEM_CONDITIONS: readonly ItemCondition[] = ["resellable", "damaged"];

function returnStatusOf(text: string): ReturnStatus {
  const status = RETURN_STATUSES.find((known) => known === text);

  if (status === undefined) {
    throw new TypeError(`store: ${JSON.stringify(text)} is not a return status`);
  }

  return status;
}

function conditionOf(text: string | null): ItemCondition | null {
  if (text === null) {
    return null;
  }

  const condition = ITEM_CONDITIONS.find((known) => known === text);

  if (condition === undefined) {
    throw new TypeError(`store: ${JSON.stringify(text)} is not an item condition`);
  }

  return condition;
}

/** The next return number of the return sequence. */
export async function nextReturnNumber(store: Store): Promise<Result<number, StoreFailure>> {
  return inTransaction(store, "next_return_number", async (client) => {
    const row = await firstRow(client, "SELECT nextval('acme_return_seq') AS number");

    return row.kind === "found" ? bigintColumn(row.value, "number") : 0;
  });
}

/** Write `returnCase`, unless a case of its id exists: then nothing changes. */
export async function openReturn(store: Store, returnCase: ReturnCase): Promise<Result<void, StoreFailure>> {
  return inTransaction(store, "open_return", async (client) => {
    await client.query(
      `INSERT INTO acme_returns (return_id, order_id, sku, quantity, status, condition, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (return_id) DO NOTHING`,
      [
        returnCase.returnId,
        returnCase.orderId,
        returnCase.sku,
        returnCase.quantity,
        returnCase.status,
        returnCase.condition,
        new Date(returnCase.createdAt.epochMilliseconds),
      ],
    );
  });
}

/** The return case `returnId`, or `notFound`. */
export async function getReturn(store: Store, returnId: string): Promise<Result<Lookup<ReturnCase>, StoreFailure>> {
  return inTransaction(store, "get_return", async (client) => {
    const row = await firstRow(
      client,
      "SELECT return_id, order_id, sku, quantity, status, condition, created_at FROM acme_returns WHERE return_id = $1",
      [returnId],
    );

    if (row.kind === "notFound") {
      return NOT_FOUND;
    }

    return found({
      returnId: textColumn(row.value, "return_id"),
      orderId: textColumn(row.value, "order_id"),
      sku: textColumn(row.value, "sku"),
      quantity: intColumn(row.value, "quantity"),
      status: returnStatusOf(textColumn(row.value, "status")),
      condition: conditionOf(nullableTextColumn(row.value, "condition")),
      createdAt: instantColumn(row.value, "created_at"),
    });
  });
}

/** Record the condition of the item of `returnId` and mark it inspected. `false` for no such case. */
export async function recordInspection(
  store: Store,
  returnId: string,
  condition: ItemCondition,
): Promise<Result<boolean, StoreFailure>> {
  return inTransaction(store, "record_inspection", async (client) => {
    const changed = await changedRows(client, "UPDATE acme_returns SET condition = $1, status = 'inspected' WHERE return_id = $2", [
      condition,
      returnId,
    ]);

    return changed > 0;
  });
}

/** Set the final status of `returnId`. `false` for no such case. */
export async function closeReturn(store: Store, returnId: string, status: ReturnStatus): Promise<Result<boolean, StoreFailure>> {
  return inTransaction(store, "close_return", async (client) => {
    const changed = await changedRows(client, "UPDATE acme_returns SET status = $1 WHERE return_id = $2", [status, returnId]);

    return changed > 0;
  });
}

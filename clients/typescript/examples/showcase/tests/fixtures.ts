// What the tests of the task logic share: the store of a test, seeded products and orders, a bounded search for an
// id with a draw, the result constructors and the reads of a body result, and a store that does not connect.

import assert from "node:assert/strict";

import type {
  ContextUse,
  CustomCode,
  CustomFailure,
  Err,
  Execution,
  Ok,
  OutputSchema,
  ParameterSchemas,
  ResultConstructors,
  SchemaSent,
  TaskContract,
  TaskError,
} from "horsies";

import type { Order, OrderStatus } from "../src/domain.ts";
import { loadCatalog } from "../src/store/catalog.ts";
import { intColumn } from "../src/store/columns.ts";
import { closeStore, firstRow, inTransaction, openStore, type Store } from "../src/store/connection.ts";
import { insertOrder } from "../src/store/orders.ts";
import { testDatabase, uniqueId } from "./support.ts";

/** The price of each seeded product, in cents. */
export const SEEDED_PRICE_CENTS = 1_000;

/** The most ids that a search for a drawn id tries. */
const ID_SEARCH_BOUND = 100_000;

/** A store whose connect is refused at once: nothing listens on port 1. */
export const UNREACHABLE_URL = "postgresql://postgres@127.0.0.1:1/acme_unreachable";

/** Run `test` with the store of the test database; with no database, run nothing. The store closes after. */
export async function withStore(test: (store: Store) => Promise<void>): Promise<void> {
  const database = await testDatabase();

  switch (database.kind) {
    case "skipped":
      return;
    case "ready":
      try {
        await test(database.store);
      } finally {
        await closeStore(database.store);
      }

      return;
    default: {
      const unexpected: never = database;

      throw new TypeError(`withStore: a database outside its type: ${String(unexpected)}`);
    }
  }
}

/** Run `test` with a store that does not connect. The store closes after. */
export async function withUnreachableStore(test: (store: Store) => Promise<void>): Promise<void> {
  const store = openStore(UNREACHABLE_URL);

  try {
    await test(store);
  } finally {
    await closeStore(store);
  }
}

/** A new product with `onHand` units on the shelf and none reserved: its SKU. */
export async function seedProduct(store: Store, label: string, onHand: number): Promise<string> {
  const sku = uniqueId(`t1-sku-${label}`);

  const loaded = await loadCatalog(
    store,
    [{ sku, name: "Task test item", category: "tests", priceCents: SEEDED_PRICE_CENTS }],
    [{ sku, onHand, reserved: 0 }],
  );

  assert.ok(loaded.ok, `seed product ${sku}`);

  return sku;
}

/** One line of a seeded order. */
export type SeedLine = { readonly sku: string; readonly quantity: number };

/** A new order `orderId` of `lines`, in `status`. */
export async function seedOrder(
  store: Store,
  orderId: string,
  lines: readonly SeedLine[],
  status: OrderStatus = "placed",
): Promise<Order> {
  const order: Order = {
    orderId,
    customerId: `customer-${orderId}`,
    status,
    totalCents: lines.reduce((total, line) => total + line.quantity * SEEDED_PRICE_CENTS, 0),
    lines: lines.map((line, index) => ({
      lineNo: index + 1,
      sku: line.sku,
      sizeCode: "M",
      quantity: line.quantity,
      unitPriceCents: SEEDED_PRICE_CENTS,
    })),
    createdAt: Temporal.Now.instant(),
  };

  const inserted = await insertOrder(store, order);

  assert.ok(inserted.ok, `seed order ${orderId}`);

  return order;
}

/** An order `orderId` of one line of `quantity` units of a new product of `onHand` units. */
export async function seedOneLineOrder(store: Store, orderId: string, onHand: number, quantity: number): Promise<Order> {
  const sku = await seedProduct(store, "line", onHand);

  return seedOrder(store, orderId, [{ sku, quantity }]);
}

/** The first id `<prefix>-<n>` (n from 0) for which `wanted` holds. No such id in the bound throws. */
export function idWhere(prefix: string, wanted: (id: string) => boolean): string {
  for (let index = 0; index < ID_SEARCH_BOUND; index += 1) {
    const id = `${prefix}-${String(index)}`;

    if (wanted(id)) {
      return id;
    }
  }

  throw new Error(`no id of ${prefix} in ${String(ID_SEARCH_BOUND)} tries`);
}

/** The shelf and the reserved units of `sku`. A SKU with no stock row throws. */
export async function stockOf(store: Store, sku: string): Promise<{ readonly onHand: number; readonly reserved: number }> {
  const read = await inTransaction(store, "stock_of", async (client) =>
    firstRow(client, "SELECT on_hand, reserved FROM acme_stock WHERE sku = $1", [sku]),
  );

  assert.ok(read.ok, `the stock read of ${sku}`);
  assert.ok(read.value.kind === "found", `the stock row of ${sku}`);

  return { onHand: intColumn(read.value.value, "on_hand"), reserved: intColumn(read.value.value, "reserved") };
}

/**
 * The result constructors of a body of `contract`, for a test that calls a function of a body with no runtime: `ok`
 * and `err` give the frozen result, as the `ok` and `err` of a body's second argument do.
 */
export function resultsOf<
  P extends ParameterSchemas,
  O extends OutputSchema,
  E extends string,
  X extends Execution,
  C extends ContextUse,
>(_contract: TaskContract<P, O, E, X, C>): ResultConstructors<O, E> {
  return Object.freeze({
    ok: (value: SchemaSent<O>): Ok<SchemaSent<O>> => Object.freeze({ ok: true, value }),
    // SAFETY: `CustomCode<K>` is `K` for every code that is not built in, and `K extends E`; the tests give declared codes.
    err: <const K extends E>(failure: CustomFailure<CustomCode<K>>): Err<CustomFailure<E>> =>
      Object.freeze({ ok: false, error: Object.freeze(failure as CustomFailure<E>) }),
  });
}

/** The value of a body result that must succeed. */
export function succeeded<T, E extends string>(result: Ok<T> | Err<CustomFailure<E>>): T {
  if (!result.ok) {
    assert.fail(`expected a success, got ${result.error.code}: ${result.error.message ?? "no message"}`);
  }

  return result.value;
}

/** The error of a body result that must fail. */
export function failed<T, E extends string>(result: Ok<T> | Err<CustomFailure<E>>): CustomFailure<E> {
  if (result.ok) {
    assert.fail(`expected an error, got ${JSON.stringify(result.value)}`);
  }

  return result.error;
}

/** A failed upstream outcome of a node whose task returned `code`. */
export function upstreamError(code: string): TaskError {
  return { kind: "custom", code, message: "the upstream node failed", data: null, exception: null, subWorkflow: null };
}

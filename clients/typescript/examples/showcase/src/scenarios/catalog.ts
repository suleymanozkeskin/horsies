// The deterministic catalog and orders of the scenarios: the same draws as the Rust showcase, so the same order id
// gives the same lines and the same faults.

import type { Result } from "horsies";

import type { Order, OrderLine } from "../domain.ts";
import { err, ok } from "../result.ts";
import { choice, draw, integer, sample } from "../simulate.ts";
import { availableUnits, type CatalogEntry, listCatalog, loadCatalog, type Product, type StockLevel } from "../store/catalog.ts";
import { type Store, storeFailureText } from "../store/connection.ts";
import { insertOrder, nextOrderNumber } from "../store/orders.ts";
import {
  BUNDLE_MIN_QUANTITY,
  CARD_DECLINE_RATE,
  CATALOG_SIZE,
  CATALOG_STOCK_PER_SKU,
  CLEARANCE_PRICE_CENTS,
  CORRUPT_SIZE_CODE,
  DISCONTINUED_SKU_COUNT,
  MAX_LINES_PER_ORDER,
  MAX_PRICE_CENTS,
  MAX_QTY_PER_LINE,
  MIN_LINES_PER_ORDER,
  MIN_PRICE_CENTS,
  MIN_QTY_PER_LINE,
  PRODUCT_CATEGORIES,
  PRODUCT_COLOURS,
  PRODUCT_LINES,
  PROMOTION_BUNDLE_BUG_RATE,
  PROMOTION_SIZE_CODE_BUG_RATE,
  SIZE_CODES,
  STOCK_SHORTFALL_RATE,
} from "../tuning.ts";

/** The most order numbers that one search for an order id with a wanted draw takes from the sequence. */
export const ORDER_ID_SEARCH_BOUND = 500;

/** The customer numbers of the drawn customer ids: `CUS-0001` to `CUS-0400`. */
const CUSTOMER_COUNT = 400;

/** The digits of the number of a SKU, an order id and a customer id. */
const SKU_DIGITS = 4;
const ORDER_DIGITS = 5;

/** `ACME-SKU-0001` for 1. Pure. */
export function skuOf(index: number): string {
  return `ACME-SKU-${String(index).padStart(SKU_DIGITS, "0")}`;
}

/** `ACME-00001` for 1. Pure. */
export function orderIdOf(orderNumber: number): string {
  return `ACME-${String(orderNumber).padStart(ORDER_DIGITS, "0")}`;
}

/** The product of catalog position `index` (from 1). Pure. */
export function product(index: number): Product {
  const sku = skuOf(index);

  return {
    sku,
    name: `${choice(PRODUCT_COLOURS, [sku, "colour"])} ${choice(PRODUCT_LINES, [sku, "line"])}`,
    category: choice(PRODUCT_CATEGORIES, [sku, "category"]),
    priceCents: integer(MIN_PRICE_CENTS, MAX_PRICE_CENTS, [sku, "price"]),
  };
}

/** The seed catalog: `CATALOG_SIZE` products; the last `DISCONTINUED_SKU_COUNT` have no stock. Pure. */
export function catalogSeed(): { readonly products: readonly Product[]; readonly stock: readonly StockLevel[] } {
  const products = Array.from({ length: CATALOG_SIZE }, (_unused, position) => product(position + 1));
  const firstDiscontinued = CATALOG_SIZE - DISCONTINUED_SKU_COUNT;

  const stock = products.map((entry, position) => ({
    sku: entry.sku,
    onHand: position >= firstDiscontinued ? 0 : CATALOG_STOCK_PER_SKU,
    reserved: 0,
  }));

  return { products, stock };
}

/** Write the seed catalog: the count of the products. */
export async function loadSeedCatalog(store: Store): Promise<Result<number, string>> {
  const seed = catalogSeed();
  const loaded = await loadCatalog(store, seed.products, seed.stock);

  return loaded.ok ? loaded : err(storeFailureText(loaded.error));
}

/** The catalog of the store; an empty catalog is a failure that names `seed`. */
export async function readCatalog(store: Store): Promise<Result<readonly CatalogEntry[], string>> {
  const catalog = await listCatalog(store);

  if (!catalog.ok) {
    return err(storeFailureText(catalog.error));
  }

  return catalog.value.length === 0 ? err("the catalog is empty; run the seed scenario first") : catalog;
}

/** The entries that the order lines of `orderId` take: the drawn in-stock entries, the first one a discontinued entry when drawn. */
function pickedEntries(orderId: string, catalog: readonly CatalogEntry[]): readonly CatalogEntry[] {
  const inStock = catalog.filter((entry) => availableUnits(entry.stock) > 0);
  const [firstDiscontinued, ...otherDiscontinued] = catalog.filter((entry) => entry.stock.onHand === 0);
  const lineCount = Math.min(integer(MIN_LINES_PER_ORDER, MAX_LINES_PER_ORDER, [orderId, "lines"]), inStock.length);
  const picked = [...sample(inStock, lineCount, [orderId, "skus"])];

  if (firstDiscontinued !== undefined && draw(STOCK_SHORTFALL_RATE, [orderId, "shortfall"])) {
    picked[0] = choice([firstDiscontinued, ...otherDiscontinued], [orderId, "discontinued"]);
  }

  return picked;
}

/** The lines of `orderId` over `entries`, with the drawn clearance and size-code faults. Pure. */
function orderLines(orderId: string, entries: readonly CatalogEntry[]): readonly OrderLine[] {
  const clearance = draw(PROMOTION_BUNDLE_BUG_RATE, [orderId, "bundle-bug"]);
  const corruptSize = draw(PROMOTION_SIZE_CODE_BUG_RATE, [orderId, "size-bug"]);

  return entries.map((entry, index) => {
    const lineNo = index + 1;
    const lineKey = String(lineNo);

    return {
      lineNo,
      sku: entry.product.sku,
      sizeCode: corruptSize && lineNo === 1 ? CORRUPT_SIZE_CODE : choice(SIZE_CODES, [orderId, "size", lineKey]),
      quantity: clearance ? BUNDLE_MIN_QUANTITY : integer(MIN_QTY_PER_LINE, MAX_QTY_PER_LINE, [orderId, "qty", lineKey]),
      unitPriceCents: clearance ? CLEARANCE_PRICE_CENTS : entry.product.priceCents,
    };
  });
}

/** The order of `orderId` over `catalog`: its lines, customer and total are draws over the id. */
export function buildOrder(orderId: string, catalog: readonly CatalogEntry[]): Result<Order, string> {
  if (!catalog.some((entry) => availableUnits(entry.stock) > 0)) {
    return err("the catalog has no sellable stock");
  }

  const lines = orderLines(orderId, pickedEntries(orderId, catalog));
  const customerNumber = integer(1, CUSTOMER_COUNT, [orderId, "customer"]);

  return ok({
    orderId,
    customerId: `CUS-${String(customerNumber).padStart(SKU_DIGITS, "0")}`,
    status: "placed",
    totalCents: lines.reduce((total, line) => total + line.quantity * line.unitPriceCents, 0),
    lines: [...lines],
    createdAt: Temporal.Now.instant(),
  });
}

/** Build the order of `orderId` and write it. */
export async function storeOrder(store: Store, orderId: string, catalog: readonly CatalogEntry[]): Promise<Result<Order, string>> {
  const order = buildOrder(orderId, catalog);

  if (!order.ok) {
    return order;
  }

  const inserted = await insertOrder(store, order.value);

  return inserted.ok ? order : err(storeFailureText(inserted.error));
}

/** The next order of the order sequence, written. */
export async function nextOrder(store: Store, catalog: readonly CatalogEntry[]): Promise<Result<Order, string>> {
  const orderNumber = await nextOrderNumber(store);

  return orderNumber.ok ? storeOrder(store, orderIdOf(orderNumber.value), catalog) : err(storeFailureText(orderNumber.error));
}

/** An order id of the sequence whose draws `wanted` accepts, or none within `ORDER_ID_SEARCH_BOUND` numbers. */
export type OrderIdSearch = { readonly kind: "found"; readonly orderId: string } | { readonly kind: "notFound" };

/** Take order numbers from the sequence until `wanted` accepts the id, at most `ORDER_ID_SEARCH_BOUND`. */
export async function reserveOrderId(store: Store, wanted: (orderId: string) => boolean): Promise<Result<OrderIdSearch, string>> {
  for (let tried = 0; tried < ORDER_ID_SEARCH_BOUND; tried += 1) {
    const orderNumber = await nextOrderNumber(store);

    if (!orderNumber.ok) {
      return err(storeFailureText(orderNumber.error));
    }

    const orderId = orderIdOf(orderNumber.value);

    if (wanted(orderId)) {
      return ok({ kind: "found", orderId });
    }
  }

  return ok({ kind: "notFound" });
}

/** Whether the card of `orderId` is a drawn decline. Pure. */
export function cardDeclines(orderId: string): boolean {
  return draw(CARD_DECLINE_RATE, [orderId, "card"]);
}

/** Whether the first line of `orderId` takes a discontinued SKU. Pure. */
export function stockFallsShort(orderId: string): boolean {
  return draw(STOCK_SHORTFALL_RATE, [orderId, "shortfall"]);
}

// The catalog: products and their stock.

import type { Result } from "horsies";

import { bigintColumn, intColumn, textColumn } from "./columns.ts";
import {
  changedRows,
  firstRow,
  found,
  inTransaction,
  type Lookup,
  NOT_FOUND,
  rowsOf,
  type Store,
  type StoreFailure,
} from "./connection.ts";

export type Product = {
  readonly sku: string;
  readonly name: string;
  readonly category: string;
  readonly priceCents: number;
};

export type StockLevel = { readonly sku: string; readonly onHand: number; readonly reserved: number };

export type CatalogEntry = { readonly product: Product; readonly stock: StockLevel };

/** The units of a stock level that no order holds. Pure. */
export function availableUnits(stock: StockLevel): number {
  return stock.onHand - stock.reserved;
}

/** What the nightly stocktake changed. */
export type StocktakeChange = { readonly skusToppedUp: number; readonly reservationsCleared: number };

/** Write `products` and `stock` (a new row, or the values of a row of the same SKU), in one transaction. */
export async function loadCatalog(
  store: Store,
  products: readonly Product[],
  stock: readonly StockLevel[],
): Promise<Result<number, StoreFailure>> {
  return inTransaction(store, "load_catalog", async (client) => {
    for (const product of products) {
      await client.query(
        `INSERT INTO acme_products (sku, name, category, price_cents) VALUES ($1, $2, $3, $4)
         ON CONFLICT (sku) DO UPDATE SET name = EXCLUDED.name, category = EXCLUDED.category, price_cents = EXCLUDED.price_cents`,
        [product.sku, product.name, product.category, product.priceCents],
      );
    }

    for (const level of stock) {
      await client.query(
        `INSERT INTO acme_stock (sku, on_hand, reserved, updated_at) VALUES ($1, $2, $3, now())
         ON CONFLICT (sku) DO UPDATE SET on_hand = EXCLUDED.on_hand, reserved = EXCLUDED.reserved, updated_at = now()`,
        [level.sku, level.onHand, level.reserved],
      );
    }

    return products.length;
  });
}

/** Every product with its stock, by SKU. */
export async function listCatalog(store: Store): Promise<Result<readonly CatalogEntry[], StoreFailure>> {
  return inTransaction(store, "list_catalog", async (client) => {
    const rows = await rowsOf(
      client,
      `SELECT p.sku, p.name, p.category, p.price_cents, s.on_hand, s.reserved
       FROM acme_products p JOIN acme_stock s USING (sku) ORDER BY p.sku`,
    );

    return rows.map((row) => ({
      product: {
        sku: textColumn(row, "sku"),
        name: textColumn(row, "name"),
        category: textColumn(row, "category"),
        priceCents: intColumn(row, "price_cents"),
      },
      stock: { sku: textColumn(row, "sku"), onHand: intColumn(row, "on_hand"), reserved: intColumn(row, "reserved") },
    }));
  });
}

/** The count of the products. */
export async function countProducts(store: Store): Promise<Result<number, StoreFailure>> {
  return inTransaction(store, "count_products", async (client) => {
    const row = await firstRow(client, "SELECT count(*) AS products FROM acme_products");

    return row.kind === "found" ? bigintColumn(row.value, "products") : 0;
  });
}

/** The reserved units of `sku`, or `notFound` for a SKU with no stock row. */
export async function reservedUnits(store: Store, sku: string): Promise<Result<Lookup<number>, StoreFailure>> {
  return inTransaction(store, "reserved_units", async (client) => {
    const row = await firstRow(client, "SELECT reserved FROM acme_stock WHERE sku = $1", [sku]);

    return row.kind === "found" ? found(intColumn(row.value, "reserved")) : NOT_FOUND;
  });
}

/** Put `quantity` units of `sku` back on the shelf: the units available after, or `notFound` for no stock row. */
export async function restockUnits(store: Store, sku: string, quantity: number): Promise<Result<Lookup<number>, StoreFailure>> {
  return inTransaction(store, "restock_units", async (client) => {
    const row = await firstRow(
      client,
      `UPDATE acme_stock SET on_hand = on_hand + $1, updated_at = now() WHERE sku = $2
       RETURNING on_hand - reserved AS available`,
      [quantity, sku],
    );

    return row.kind === "found" ? found(intColumn(row.value, "available")) : NOT_FOUND;
  });
}

/** Take `quantity` reserved units of `sku` off the reservation: its available units after, or `notFound`. */
export async function releaseUnits(
  store: Store,
  sku: string,
  quantity: number,
): Promise<Result<Lookup<number>, StoreFailure>> {
  return inTransaction(store, "release_units", async (client) => {
    const row = await firstRow(
      client,
      `UPDATE acme_stock SET reserved = greatest(0, reserved - $1), updated_at = now() WHERE sku = $2
       RETURNING on_hand - reserved AS available`,
      [quantity, sku],
    );

    return row.kind === "found" ? found(intColumn(row.value, "available")) : NOT_FOUND;
  });
}

/**
 * Clear every reservation, and bring the shelf of each SKU into `targetUnits` to `ceilingUnits`: the nightly
 * stocktake, which also clears what a crashed or cancelled order left reserved.
 */
export async function nightlyStocktake(
  store: Store,
  targetUnits: number,
  ceilingUnits: number,
): Promise<Result<StocktakeChange, StoreFailure>> {
  return inTransaction(store, "nightly_stocktake", async (client) => {
    const reservationsCleared = await changedRows(
      client,
      "UPDATE acme_stock SET reserved = 0, updated_at = now() WHERE reserved > 0",
    );

    const skusToppedUp = await changedRows(
      client,
      `UPDATE acme_stock SET on_hand = least(greatest(on_hand, $1), $2), updated_at = now()
       WHERE on_hand < $1 OR on_hand > $2`,
      [targetUnits, ceilingUnits],
    );

    return { skusToppedUp, reservationsCleared };
  });
}

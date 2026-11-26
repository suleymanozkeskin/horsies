// The Acme tables, and the creation of the Acme database. The horsies tables are not here: the app installs them at
// its first operation, in the same database.

import pg from "pg";

import type { Result } from "horsies";

import { err, ok } from "../result.ts";
import type { DatabaseSettings } from "../settings.ts";
import { firstRow, inTransaction, type Store, STORE_CONNECT_TIMEOUT_MS, type StoreFailure } from "./connection.ts";

/** The statements of the Acme tables, in order. Each one is idempotent. */
export const SCHEMA_STATEMENTS: readonly string[] = [
  `CREATE TABLE IF NOT EXISTS acme_products (
    sku          text PRIMARY KEY,
    name         text NOT NULL,
    category     text NOT NULL,
    price_cents  integer NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS acme_stock (
    sku        text PRIMARY KEY REFERENCES acme_products (sku) ON DELETE CASCADE,
    on_hand    integer NOT NULL,
    reserved   integer NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
  )`,
  `CREATE TABLE IF NOT EXISTS acme_orders (
    order_id               text PRIMARY KEY,
    customer_id            text NOT NULL,
    status                 text NOT NULL,
    total_cents            integer NOT NULL,
    authorization_attempts integer NOT NULL DEFAULT 0,
    created_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now()
  )`,
  `CREATE TABLE IF NOT EXISTS acme_order_lines (
    order_id         text NOT NULL REFERENCES acme_orders (order_id) ON DELETE CASCADE,
    line_no          integer NOT NULL,
    sku              text NOT NULL,
    size_code        text NOT NULL,
    quantity         integer NOT NULL,
    unit_price_cents integer NOT NULL,
    reserved         boolean NOT NULL DEFAULT false,
    consumed         boolean NOT NULL DEFAULT false,
    PRIMARY KEY (order_id, line_no)
  )`,
  `CREATE TABLE IF NOT EXISTS acme_payments (
    payment_id    text PRIMARY KEY,
    order_id      text NOT NULL REFERENCES acme_orders (order_id) ON DELETE CASCADE,
    kind          text NOT NULL,
    amount_cents  integer NOT NULL,
    psp_reference text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (order_id, kind)
  )`,
  `CREATE TABLE IF NOT EXISTS acme_shipments (
    shipment_id       text PRIMARY KEY,
    order_id          text NOT NULL UNIQUE REFERENCES acme_orders (order_id) ON DELETE CASCADE,
    courier           text NOT NULL,
    express           boolean NOT NULL,
    attempts          integer NOT NULL DEFAULT 0,
    booking_reference text,
    label_url         text,
    tracking_code     text,
    created_at        timestamptz NOT NULL DEFAULT now()
  )`,
  `CREATE TABLE IF NOT EXISTS acme_returns (
    return_id  text PRIMARY KEY,
    order_id   text NOT NULL REFERENCES acme_orders (order_id) ON DELETE CASCADE,
    sku        text NOT NULL,
    quantity   integer NOT NULL,
    status     text NOT NULL,
    condition  text,
    created_at timestamptz NOT NULL DEFAULT now()
  )`,
  `CREATE TABLE IF NOT EXISTS acme_reruns (
    rerun_id       text PRIMARY KEY,
    source_task_id text NOT NULL,
    new_task_id    text NOT NULL,
    reason         text NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
  )`,
  "CREATE SEQUENCE IF NOT EXISTS acme_order_seq",
  "CREATE SEQUENCE IF NOT EXISTS acme_return_seq",
];

/** Whether `ensureDatabase` created the database. */
export type DatabaseCreation = "created" | "existed";

/**
 * Create the database of `settings` when the server has none of that name, through the maintenance database of the
 * same server. Effects: one connection; `CREATE DATABASE`. The settings parser allows only letters, digits and `_` in
 * the name, so the name needs no quoting rule.
 */
export async function ensureDatabase(settings: DatabaseSettings): Promise<Result<DatabaseCreation, StoreFailure>> {
  const client = new pg.Client({ connectionString: settings.maintenanceUrl, connectionTimeoutMillis: STORE_CONNECT_TIMEOUT_MS });

  try {
    await client.connect();
    const existing = await client.query("SELECT 1 FROM pg_database WHERE datname = $1", [settings.databaseName]);

    if (existing.rows.length > 0) {
      return ok("existed");
    }

    await client.query(`CREATE DATABASE "${settings.databaseName}"`);

    return ok("created");
  } catch (cause) {
    return err({ operation: "ensure_database", cause: cause instanceof Error ? cause.message : String(cause) });
  } finally {
    await client.end().catch(() => undefined);
  }
}

/** Create the Acme tables and sequences that do not exist, in one transaction. */
export async function ensureSchema(store: Store): Promise<Result<void, StoreFailure>> {
  return inTransaction(store, "ensure_schema", async (client) => {
    for (const statement of SCHEMA_STATEMENTS) {
      await client.query(statement);
    }
  });
}

/** Whether a table of `name` exists in the database of `store`. */
export async function tableExists(store: Store, name: string): Promise<Result<boolean, StoreFailure>> {
  return inTransaction(store, "table_exists", async (client) => {
    const row = await firstRow(client, "SELECT to_regclass($1) IS NOT NULL AS present", [name]);

    return row.kind === "found" && row.value["present"] === true;
  });
}

// The workflows of the showcase, with no database: their names and keys, the node ids that the tests and the
// monitoring page name, the check order, and the build of each workflow that takes no params, which `Horsies.create`
// runs with every spec check. The builds of the workflows with params run in `check` (tests/offline.test.ts).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { Horsies } from "horsies";

import { appConfig } from "../src/app.ts";
import { CATALOG_IMPORT_CHUNKS, MAX_LINES_PER_ORDER, SUPPLIERS } from "../src/tuning.ts";
import { catalogImport, chunkNodeId } from "../src/workflows/catalog-import.ts";
import { customerWinback } from "../src/workflows/customer-winback.ts";
import { dailyReport } from "../src/workflows/daily-report.ts";
import { flashSale } from "../src/workflows/flash-sale.ts";
import { fraudReview } from "../src/workflows/fraud-review.ts";
import { ALL_WORKFLOWS, ALL_WORKFLOW_NAMES } from "../src/workflows/index.ts";
import { checkOrder, reservationNodeId } from "../src/workflows/order-fulfillment.ts";
import { priceSync } from "../src/workflows/price-sync.ts";
import { feedNodeId, restock } from "../src/workflows/restock.ts";
import { seasonalMarkdown } from "../src/workflows/seasonal-markdown.ts";
import { warehouseTransfer } from "../src/workflows/warehouse-transfer.ts";
import { OFFLINE_URL } from "./support.ts";

/** The workflow names of the Rust showcase (`ALL_WORKFLOW_NAMES` of `clients/rust/examples/showcase`). */
const RUST_WORKFLOW_NAMES: readonly string[] = [
  "order_fulfillment",
  "shipping",
  "returns_review",
  "catalog_import",
  "customer_winback",
  "daily_report",
  "flash_sale",
  "fraud_review",
  "price_sync",
  "restock",
  "seasonal_markdown",
  "warehouse_transfer",
];

/** The workflows that take no params: `Horsies.create` builds each one. */
const WORKFLOWS_WITHOUT_PARAMS = [
  catalogImport,
  customerWinback,
  dailyReport,
  flashSale,
  fraudReview,
  priceSync,
  restock,
  seasonalMarkdown,
  warehouseTransfer,
] as const;

describe("the workflow list", () => {
  test("it holds the twelve workflows of the Rust showcase, each name once", () => {
    assert.deepEqual([...ALL_WORKFLOW_NAMES].toSorted(), [...RUST_WORKFLOW_NAMES].toSorted());
    assert.equal(new Set(ALL_WORKFLOW_NAMES).size, ALL_WORKFLOW_NAMES.length);
  });

  test("each definition key is acme.<name>.v1", () => {
    for (const workflow of ALL_WORKFLOWS) {
      assert.equal(workflow.definitionKey, `acme.${workflow.name}.v1`);
    }
  });
});

describe("the builds of the workflows that take no params", () => {
  for (const workflow of WORKFLOWS_WITHOUT_PARAMS) {
    test(`${workflow.name}: Horsies.create builds it with every spec check, as the only workflow of the app`, () => {
      const created = Horsies.create({ ...appConfig(OFFLINE_URL), workflows: [workflow] });

      assert.ok(created.ok, created.ok ? "" : created.error.refusals.map((refusal) => refusal.message).join("\n"));
    });
  }
});

describe("node ids", () => {
  test("the feed of each supplier has its own node id, as in the Rust showcase", () => {
    assert.deepEqual(
      SUPPLIERS.map((supplier) => feedNodeId(supplier)),
      ["feed_atlas_textiles", "feed_brera_knitwear", "feed_coastline_denim"],
    );
  });

  test("the chunks of the import are chunk_00 to chunk_39, each id once", () => {
    const ids = Array.from({ length: CATALOG_IMPORT_CHUNKS }, (_unused, index) => chunkNodeId(index));

    assert.equal(ids[0], "chunk_00");
    assert.equal(ids.at(-1), `chunk_${String(CATALOG_IMPORT_CHUNKS - 1)}`);
    assert.equal(new Set(ids).size, CATALOG_IMPORT_CHUNKS);
  });
});

describe("the check order of the order workflow", () => {
  test("it has the most lines, numbered from 1, and its total is the sum of its lines", () => {
    const order = checkOrder();

    assert.equal(order.lines.length, MAX_LINES_PER_ORDER);
    assert.deepEqual(
      order.lines.map((line) => line.lineNo),
      Array.from({ length: MAX_LINES_PER_ORDER }, (_unused, index) => index + 1),
    );
    assert.equal(
      order.totalCents,
      order.lines.reduce((total, line) => total + line.quantity * line.unitPriceCents, 0),
    );
    assert.equal(new Set(order.lines.map((line) => reservationNodeId(line.lineNo))).size, order.lines.length);
  });
});

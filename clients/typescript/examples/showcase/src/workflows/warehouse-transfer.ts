// The warehouse transfer: choose the warehouse that gives the units, then put the units back on the shelf of the
// SKU. Its result is the release.

import { defineWorkflow } from "horsies";

import { releaseStock } from "../contracts/inventory.ts";
import { allocateWarehouse } from "../contracts/orders.ts";
import { StockRelease } from "../domain.ts";

/** The SKU that the workflow transfers. */
export const TRANSFER_SKU = "ACME-SKU-0001";

/** The units that the workflow transfers. */
export const TRANSFER_QUANTITY = 5;

export const warehouseTransfer = defineWorkflow(
  { name: "warehouse_transfer", definitionKey: "acme.warehouse_transfer.v1", parameters: {}, outputType: StockRelease },
  (w) => {
    const allocate = w.task(allocateWarehouse, { sku: TRANSFER_SKU, quantity: TRANSFER_QUANTITY }, { nodeId: "allocate_warehouse" });

    const release = w.task(
      releaseStock,
      { sku: TRANSFER_SKU, quantity: TRANSFER_QUANTITY },
      { nodeId: "release_stock", waitsFor: [allocate] },
    );

    return { output: release };
  },
);

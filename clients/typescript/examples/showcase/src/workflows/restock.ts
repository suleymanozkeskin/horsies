// The supplier restock: read the feed of each supplier, then apply the stock levels when a quorum of the feeds
// completed. Its result is the stock update. The workflow succeeds when the update completed; a feed may fail.
//
// Each feed reaches the update through one parameter of the update (`atlasFeed`, `breraFeed`, `coastlineFeed`). Under
// the quorum join a feed may not have ended when the update runs, so each feed parameter is optional. The Rust
// showcase gives the feeds through the workflow context; a TypeScript body does not get the context yet (#2460).

import { defineWorkflow } from "horsies";

import { syncSupplierFeed, updateStockLevels } from "../contracts/inventory.ts";
import { StockUpdate } from "../domain.ts";
import { RESTOCK_MIN_SUCCESSFUL_FEEDS, SUPPLIERS } from "../tuning.ts";

/** The node id of the feed of `supplier`. Pure. */
export function feedNodeId(supplier: string): `feed_${string}` {
  return `feed_${supplier.replaceAll("-", "_")}`;
}

const [ATLAS, BRERA, COASTLINE] = SUPPLIERS;

export const restock = defineWorkflow(
  { name: "restock", definitionKey: "acme.restock.v1", params: {}, output: StockUpdate },
  (w) => {
    const atlasFeed = w.task(syncSupplierFeed, { supplier: ATLAS }, { id: feedNodeId(ATLAS) });
    const breraFeed = w.task(syncSupplierFeed, { supplier: BRERA }, { id: feedNodeId(BRERA) });
    const coastlineFeed = w.task(syncSupplierFeed, { supplier: COASTLINE }, { id: feedNodeId(COASTLINE) });

    const update = w.task(
      updateStockLevels,
      { atlasFeed, breraFeed, coastlineFeed },
      { id: "update_stock_levels", join: { kind: "quorum", minSuccess: RESTOCK_MIN_SUCCESSFUL_FEEDS } },
    );

    return {
      output: update,
      successPolicy: { cases: [{ required: [update] }], optional: [atlasFeed, breraFeed, coastlineFeed] },
    };
  },
);

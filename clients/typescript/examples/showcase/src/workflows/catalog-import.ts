// The catalog import: one node per chunk of a supplier catalog, all side by side.
//
// As the Rust workflow, it has no output: `get` gives `NoWorkflowOutput`, and `results()` gives the outcome of each
// chunk node, by node id. Every chunk must complete for the workflow to complete.

import { defineWorkflow } from "horsies";

import { catalogImportChunk } from "../contracts/analytics.ts";
import { CATALOG_IMPORT_CHUNKS } from "../tuning.ts";

/** The import that the workflow imports. */
export const IMPORT_ID = "CHECK-IMPORT";

/** The node id of chunk `chunkIndex`: `chunk_00` to `chunk_39`. Pure. */
export function chunkNodeId(chunkIndex: number): `chunk_${string}` {
  return `chunk_${String(chunkIndex).padStart(2, "0")}`;
}

export const catalogImport = defineWorkflow(
  { name: "catalog_import", definitionKey: "acme.catalog_import.v1", parameters: {} },
  (w) => {
    for (let chunkIndex = 0; chunkIndex < CATALOG_IMPORT_CHUNKS; chunkIndex += 1) {
      w.task(catalogImportChunk, { importId: IMPORT_ID, chunkIndex }, { nodeId: chunkNodeId(chunkIndex) });
    }

    return {};
  },
);

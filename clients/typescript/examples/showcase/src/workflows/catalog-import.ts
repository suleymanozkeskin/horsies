// The catalog import: one node per chunk of a supplier catalog, all side by side.
//
// The Rust workflow has no output. A TypeScript workflow declares one (the owner's rule), so the output is the last
// chunk node: the workflow reads as the chunk that closes the import. Every chunk must complete for the workflow to
// complete.

import { defineWorkflow } from "horsies";

import { catalogImportChunk } from "../contracts/analytics.ts";
import { CatalogChunk } from "../domain.ts";
import { CATALOG_IMPORT_CHUNKS } from "../tuning.ts";

/** The import that the workflow imports. */
export const IMPORT_ID = "CHECK-IMPORT";

/** The node id of chunk `chunkIndex`: `chunk_00` to `chunk_39`. Pure. */
export function chunkNodeId(chunkIndex: number): `chunk_${string}` {
  return `chunk_${String(chunkIndex).padStart(2, "0")}`;
}

export const catalogImport = defineWorkflow(
  { name: "catalog_import", definitionKey: "acme.catalog_import.v1", parameters: {}, outputType: CatalogChunk },
  (w) => {
    const chunks = Array.from({ length: CATALOG_IMPORT_CHUNKS }, (_unused, chunkIndex) =>
      w.task(catalogImportChunk, { importId: IMPORT_ID, chunkIndex }, { nodeId: chunkNodeId(chunkIndex) }),
    );

    const last = chunks.at(-1);

    if (last === undefined) {
      throw new TypeError("catalog_import: CATALOG_IMPORT_CHUNKS is 0; the import has no chunk");
    }

    return { output: last };
  },
);

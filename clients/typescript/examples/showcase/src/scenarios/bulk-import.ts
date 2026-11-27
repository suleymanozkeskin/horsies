// `bulk-import`: start the catalog import workflow of 40 chunks.

import type { Result } from "horsies";

import { err, ok } from "../result.ts";
import { catalogImport } from "../workflows/catalog-import.ts";
import { readCatalog } from "./catalog.ts";
import { sendErrorText, withScenario } from "./context.ts";
import { bullet, heading, say } from "./output.ts";

export async function runBulkImport(): Promise<Result<void, string>> {
  heading("Acme Clothing — bulk import");

  return withScenario(async (context) => {
    const started = await context.showcase.app.start(catalogImport, {});

    if (!started.ok) {
      return err(`start catalog_import: ${sendErrorText(started.error)}`);
    }

    bullet(`catalog import workflow: ${started.value.workflowId}`);

    const catalog = await readCatalog(context.store);

    if (!catalog.ok) {
      return catalog;
    }

    say(`catalog rows available: ${String(catalog.value.length)}`);

    return ok(undefined);
  });
}

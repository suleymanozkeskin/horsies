// `seed`: create the Acme database and tables, and load the deterministic catalog.

import { basename, relative } from "node:path";

import type { Result } from "horsies";

import { ok } from "../result.ts";
import { CATALOG_STOCK_PER_SKU, DISCONTINUED_SKU_COUNT } from "../tuning.ts";
import { loadSeedCatalog } from "./catalog.ts";
import { withScenario } from "./context.ts";
import { heading, say, webBaseUrl } from "./output.ts";

/** The command line of this process for `command`: the runtime, its flags and the entry, relative to the folder. */
function commandLine(command: string): string {
  const entry = relative(process.cwd(), process.argv[1] ?? "");

  return [basename(process.execPath), ...process.execArgv, entry, command].join(" ");
}

export async function runSeed(): Promise<Result<void, string>> {
  heading("Acme Clothing — seed");

  return withScenario(async (context) => {
    say(`database: ${context.settings.databaseName} (resolved from ${context.settings.source})`);

    const loaded = await loadSeedCatalog(context.store);

    if (!loaded.ok) {
      return loaded;
    }

    say(`acme_* tables ready; loaded ${String(loaded.value)} products`);
    say(`${String(DISCONTINUED_SKU_COUNT)} products have zero stock; ${String(CATALOG_STOCK_PER_SKU)} units seed each stocked SKU`);
    say("next");
    say(`  ${commandLine("worker")}`);
    say(`  ${commandLine("web --authorization none")}`);
    say(`  ${commandLine("steady")}`);
    say(`  open ${webBaseUrl()}`);

    return ok(undefined);
  });
}

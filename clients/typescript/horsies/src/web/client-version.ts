// The version of the `horsies` package, which `/api/meta` of the monitoring handler reports, read from its own
// `package.json`. The module sits two folders below the package folder in `src/` and in `dist/`, so the path is the
// same in both.

import { createRequire } from "node:module";

import { raiseDefect } from "../native/call/defect.ts";
import { ClientDefect } from "../thrown-errors.ts";

/** The path of the package's `package.json`, from this module. */
const PACKAGE_JSON_PATH = "../../package.json";

/** The `version` of the package's `package.json`. A manifest with no text version is a defect of the package. */
export function clientVersion(): string {
  const manifest: { readonly version?: string | number | boolean | object | null } = createRequire(import.meta.url)(
    PACKAGE_JSON_PATH,
  );

  const version = manifest.version;
  const text = String(version);

  // Only a string is equal to its own text.
  if (text === version) {
    return text;
  }

  return raiseDefect(new ClientDefect({ subject: "clientVersion", rule: "the package.json of horsies has a text version" }));
}

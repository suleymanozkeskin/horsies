// The source folder of the package, for the test of the paths to the codec reader.

import { fileURLToPath } from "node:url";

/** The source folder of the package. */
export const SOURCE_DIR: string = fileURLToPath(new URL("../../../../horsies/src/", import.meta.url));

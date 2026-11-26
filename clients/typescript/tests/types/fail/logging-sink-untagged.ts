// expect: TS2322 at 7, TS2322 at 9
// The log sink names its kind: `{ kind: "stderr" }` or `{ kind: "function", sink }`; a plain string or function is no
// sink setting.

import { configureLogging } from "../../../horsies/src/index.ts";

configureLogging({ sink: "stderr" });

configureLogging({ sink: () => undefined });

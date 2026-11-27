// expect: TS2353 at 9, TS2561 at 13, TS2353 at 16
// The names of the settings follow the Python and Rust clients: the pool test is `prePing` (not `check`), the
// recovery switches have no `auto` word (`requeueStaleClaimed`), and the quarantine bound is
// `finalizationQuarantineAfterAttempts` (not `phase2QuarantineAfterAttempts`).

import type { IsolatedPoolInput, RecoveryConfigInput } from "../../../horsies/src/index.ts";

export const pool: IsolatedPoolInput = {
  check: true,
};

export const recovery: RecoveryConfigInput = {
  autoRequeueStaleClaimed: true,
};

export const quarantine: RecoveryConfigInput = { phase2QuarantineAfterAttempts: 3 };

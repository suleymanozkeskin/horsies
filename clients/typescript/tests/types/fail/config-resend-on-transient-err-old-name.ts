// expect: TS2561 at 8
// The resend after a transient database error is `resendOnTransientError`; `resendOnTransientErr` is no config field.

import type { AppConfigInput } from "../../../horsies/src/index.ts";

export const config: AppConfigInput = {
  broker: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  resendOnTransientErr: true,
};

// expect: TS2305 at 6, TS2551 at 9, TS2322 at 12
// A send again is `app.resend`, the options type is `TaskSendOptions` and the retention of one send is a
// `SendRetention` with the kind `period`; `retrySend`, `SendOptions` and `{ kind: "days" }` are gone.

import type { Horsies, RetryableSendError, TaskSendOptions } from "../../../horsies/src/index.ts";
import type { SendOptions } from "../../../horsies/src/index.ts";

export async function again(app: Horsies, error: RetryableSendError<null, string>): Promise<unknown> {
  return app.retrySend(error);
}

export const options: TaskSendOptions = { retention: { kind: "days", days: 30 } };

export type Old = SendOptions;

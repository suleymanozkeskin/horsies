// Compile-pass cases of the tagged settings: each worker profile setting kind, the pool recycle by age, each log sink
// kind, and a switch over each kind of a codec refusal.

import {
  type BrokerConfigInput,
  type CodecRefused,
  configureLogging,
  type LogSinkSetting,
  type WorkerProfileInput,
} from "../../../horsies/src/index.ts";

export const withChildren: WorkerProfileInput = {
  isolated: { kind: "isolatedChildren", count: 2, recycle: { after: { kind: "tasks", tasks: 100 } } },
  sharedAsync: { kind: "sharedAsyncChildren", count: 1, seats: 50 },
  sharedThread: { kind: "sharedThreadChildren", count: 1, seats: 4 },
};

export const withoutChildren: WorkerProfileInput = {
  isolated: { kind: "noIsolatedChildren" },
  sharedAsync: { kind: "noSharedAsyncChildren" },
  sharedThread: { kind: "noSharedThreadChildren" },
};

export const broker: BrokerConfigInput = {
  databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" },
  poolRecycle: { kind: "after", seconds: 3_600 },
};

const toStderr: LogSinkSetting = { kind: "stderr" };

configureLogging({ sink: toStderr });

configureLogging({ sink: { kind: "function", sink: (records) => void records.length } });

export function refusalPlace(refused: CodecRefused): string {
  switch (refused.kind) {
    case "encodeRefused":
    case "decodeRefused":
      return `${refused.path}: ${refused.refusal}`;
    case "envelopeRefused":
      return `${refused.path}: ${refused.rule}`;
    case "readBackRefused":
      return `${refused.path}: ${refused.cause.kind}`;
    case "readBackChanged":
      return refused.path;
    default: {
      const unexpected: never = refused;

      return String(unexpected);
    }
  }
}

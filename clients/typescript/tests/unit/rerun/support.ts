// Values and fakes of the rerun unit tests: a command and a policy, a fake bridge that records its
// calls, a fake caller transaction, and a core handle whose addon is never called.

import type {
  AppRerunAnswer,
  CallerRowArg,
  CallerStatementRecord,
  WriteDecodeRecord,
} from "../../../horsies/src/native/facades/rerun.ts";
import type { NativeCoreHandle } from "../../../horsies/src/native/handle/addon.ts";
import { CoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import type { TraceArgument } from "../../../horsies/src/native/values/records.ts";
import type { BridgePreparation, BridgeWrite, RerunBridge } from "../../../horsies/src/rerun/bridge.ts";
import type {
  CallerQueryFailure,
  CallerRow,
  CallerStatement,
  CallerTransaction,
} from "../../../horsies/src/rerun/caller.ts";
import type { RerunRequest } from "../../../horsies/src/rerun/parse.ts";
import type { RerunCommand, RerunPolicy } from "../../../horsies/src/rerun/types.ts";
import type { Result } from "../../../horsies/src/wire/result.ts";

export const SOURCE_TASK_ID = "01890a5d-ac96-774b-bcce-b302099a8057";
export const NEW_TASK_ID = "01890a5d-ac96-774b-bcce-b302099a8058";

/** An unkeyed command with no deadline. */
export const COMMAND: RerunCommand = { sourceTaskId: SOURCE_TASK_ID, deadline: { kind: "none" } };

/** A policy that keeps the new task for 7 days and keeps its input, with a key window of one day. */
export const POLICY: RerunPolicy = {
  retention: { kind: "days", days: 7 },
  rerunInput: "keep",
  reservationWindow: Temporal.Duration.from({ days: 1 }),
};

/** The carrier of an active span. */
export const CARRIER: TraceArgument = {
  kind: "carrier",
  carrier: { traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" },
  traceIdHex: "0af7651916cd43dd8448eb211c80319c",
};

/** The source read as the addon gives it. */
export const READ_STATEMENT: CallerStatementRecord = {
  name: "rerun_source_read",
  sql: "SELECT class_registered FROM horsies_rerun_source_read($1::uuid, $2::text)",
  parameters: [
    { kind: "text", text: SOURCE_TASK_ID },
    { kind: "text", text: "standard_7d" },
  ],
  columns: [{ name: "class_registered", valueType: "bool" }],
};

/** The write as the addon gives it. */
export const WRITE_STATEMENT: CallerStatementRecord = {
  name: "rerun_enqueue",
  sql: "SELECT outcome, task_id FROM horsies_enqueue_task($1::uuid, $2::bytea)",
  parameters: [
    { kind: "text", text: NEW_TASK_ID },
    { kind: "null" },
  ],
  columns: [
    { name: "outcome", valueType: "text" },
    { name: "task_id", valueType: "uuid" },
  ],
};

/** The calls that a fake bridge got, in order. */
export type BridgeCall =
  | { readonly call: "rerunTask"; readonly handle: CoreHandle; readonly request: RerunRequest; readonly trace: TraceArgument }
  | { readonly call: "readStatement"; readonly request: RerunRequest }
  | { readonly call: "prepareWrite"; readonly trace: TraceArgument; readonly readRow: CallerRowArg }
  | { readonly call: "decodeRow"; readonly row: CallerRowArg };

/** The answers of a fake bridge. */
export interface BridgeAnswers {
  readonly rerunTask: AppRerunAnswer;
  readonly preparation: "write" | BridgePreparation;
  readonly decoded: WriteDecodeRecord;
}

/** A bridge that answers with `answers` and records each call in `calls`. */
export function fakeBridge(answers: BridgeAnswers): { readonly bridge: RerunBridge; readonly calls: BridgeCall[] } {
  const calls: BridgeCall[] = [];

  const write: BridgeWrite = {
    statement: () => WRITE_STATEMENT,
    decodeRow: (row) => {
      calls.push({ call: "decodeRow", row });

      return answers.decoded;
    },
  };

  return {
    calls,
    bridge: {
      rerunTask: async (handle, request, trace) => {
        calls.push({ call: "rerunTask", handle, request, trace });

        return answers.rerunTask;
      },
      readStatement: (request) => {
        calls.push({ call: "readStatement", request });

        return READ_STATEMENT;
      },
      prepareWrite: (_request, trace, readRow) => {
        calls.push({ call: "prepareWrite", trace, readRow });

        return answers.preparation === "write" ? { kind: "write", write } : answers.preparation;
      },
    },
  };
}

/** The answers of a bridge that writes a new task. */
export const ENQUEUED_ANSWERS: BridgeAnswers = {
  rerunTask: {
    kind: "done",
    outcome: {
      kind: "enqueued",
      newTaskId: NEW_TASK_ID,
      sourceTaskId: SOURCE_TASK_ID,
      rerunRootTaskId: SOURCE_TASK_ID,
    },
  },
  preparation: "write",
  decoded: {
    kind: "decoded",
    outcome: {
      kind: "enqueued",
      newTaskId: NEW_TASK_ID,
      sourceTaskId: SOURCE_TASK_ID,
      rerunRootTaskId: SOURCE_TASK_ID,
    },
  },
};

/** One answer of a fake transaction: a result, or a value that its `queryOne` throws. */
export type TransactionAnswer =
  | { readonly kind: "result"; readonly result: Result<CallerRow, CallerQueryFailure> }
  | { readonly kind: "throw"; readonly thrown: Error };

/** A transaction that gives `answers` in order and records each statement. It has no commit. */
export function fakeTransaction(answers: readonly TransactionAnswer[]): {
  readonly transaction: CallerTransaction;
  readonly statements: CallerStatement[];
} {
  const statements: CallerStatement[] = [];

  return {
    statements,
    transaction: {
      queryOne: async (statement) => {
        const answer = answers[statements.length];

        statements.push(statement);

        switch (answer?.kind) {
          case "result":
            return answer.result;
          case "throw":
            throw answer.thrown;
          case undefined:
            throw new Error(`the fake transaction has no answer for statement ${statement.name}`);
        }
      },
    },
  };
}

/** A row of cells. */
export function rowOf(...cells: (string | null)[]): TransactionAnswer {
  return { kind: "result", result: { ok: true, value: { cells } } };
}

/** A core handle whose addon methods are never called. */
export function unusedCoreHandle(): CoreHandle {
  const unused = (): never => {
    throw new Error("the core handle of a fake bridge is not called");
  };

  const native: NativeCoreHandle = {
    manifestVersion: unused,
    withInstallCoverage: unused,
    close: unused,
    pingDatabase: unused,
    installSchema: unused,
    schemaStatus: unused,
  };

  return new CoreHandle(native);
}

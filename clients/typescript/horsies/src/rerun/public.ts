// The public names of the rerun. `src/index.ts` exports this module. The driver adapters have their
// own entries: `horsies/rerun/pg` (`src/rerun/pg.ts`) and `horsies/rerun/postgres`
// (`src/rerun/postgres.ts`).

export type {
  RerunCommand,
  RerunDeadline,
  RerunEnqueued,
  RerunInputChoice,
  RerunInputCorrupt,
  RerunInputUnavailable,
  RerunKeyConflict,
  RerunKeyReplay,
  RerunNotEligible,
  RerunNotEligibleReason,
  RerunOutcome,
  RerunEnqueuePolicy,
  RerunRetention,
  RerunSourceAbsent,
  RerunSourceCorrupt,
  RerunSourceLive,
} from "./types.ts";

export type {
  CallerRerunError,
  RerunCallerKeyRefused,
  RerunClassNotRegistered,
  RerunClientFamily,
  RerunClientFamilyMismatch,
  RerunConnectFailed,
  RerunContractViolation,
  RerunDriverFailed,
  RerunError,
  RerunFamilyCheckFailed,
  RerunIdMintRefused,
  RerunInvalidCommand,
  RerunInvalidPolicy,
  RerunNoOpenTransaction,
  RerunRequestRefused,
  RerunResultNotText,
  RerunRetryCodeNotDeclared,
  RerunStatementFailed,
  RerunStatementOutcome,
  RerunStepRefused,
  RerunStoredFamily,
  RerunTaskNotRegistered,
  RerunUnknownStoredFamily,
} from "./errors.ts";

export type {
  CallerColumn,
  CallerConnectionState,
  CallerQueryFailure,
  CallerRow,
  CallerStatement,
  CallerTransaction,
  CallerType,
  SqlText,
} from "./caller.ts";

export type { CoreFailure } from "../_catalog/error_detail.ts";

export type { TaskAbsenceReason } from "../_catalog/history.ts";

export { RerunErrorCode } from "../_catalog/history.ts";

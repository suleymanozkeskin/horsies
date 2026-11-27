---
title: Rerun
summary: Run a terminal task again as a new task with app.rerunTask in a transaction of the app, or with app.rerunTaskIn in a transaction that the caller owns, through the pg or postgres.js adapter.
related: [../tasks/sending-tasks, ../monitoring/monitoring-queries, ../../monitoring/action-semantics, ../../configuration/retention-config]
tags: [typescript, rerun, rerunTaskIn, RerunCommand, RerunEnqueuePolicy, RerunOutcome, pg, postgres]
---

# Rerun

A rerun reads a terminal source task and, for an eligible source with a retained input, writes one new task with a new id and the lineage `RERUN`. Every outcome but `enqueued` writes nothing. Which tasks can be rerun, and what each outcome means, is on [Task rerun](/horsies/monitoring/action-semantics/#task-rerun).

| Call | Transaction | Returns |
|------|-------------|---------|
| `app.rerunTask(command, policy)` | A transaction of the app's handle, which the call commits | `Promise<Result<RerunOutcome, RerunError>>` |
| `app.rerunTaskIn(transaction, command, policy)` | A transaction that the caller owns. The rerun commits or rolls back with the caller's own writes | `Promise<Result<RerunOutcome, CallerRerunError>>` |

Both calls check the command and the policy before any call to the database. A refusal is `INVALID_COMMAND` or `INVALID_POLICY`.

## Rerun in a Transaction of the App

```ts
import type { Horsies, RerunCommand, RerunEnqueuePolicy, TaskId } from "horsies";

function assertNever(value: never): never {
  throw new TypeError(`no case for ${String(value)}`);
}

const policy: RerunEnqueuePolicy = {
  retention: { kind: "period", days: 30 },
  retainRerunInput: "keep",
  reservationWindow: Temporal.Duration.from({ hours: 24 }),
};

export async function rerunFailed(app: Horsies, sourceTaskId: TaskId): Promise<TaskId | undefined> {
  const command: RerunCommand = { sourceTaskId, goodUntil: { kind: "none" }, idempotencyKey: `rerun-${sourceTaskId}` };
  const rerun = await app.rerunTask(command, policy);

  if (!rerun.ok) {
    console.error(rerun.error.code, rerun.error.message);
    return undefined;
  }

  const outcome = rerun.value;

  switch (outcome.kind) {
    case "enqueued":
      return outcome.newTaskId;
    case "keyReplay":
      return outcome.existingTaskId;
    case "sourceLive":
    case "sourceAbsent":
    case "sourceCorrupt":
    case "notEligible":
    case "inputUnavailable":
    case "inputCorrupt":
    case "keyConflict":
      console.warn(`no rerun of ${outcome.taskId}: ${outcome.kind}`);
      return undefined;
    default:
      return assertNever(outcome);
  }
}
```

The app's handle connects, checks the client family of the database and installs the schema once, as a send does. In a worker child the call goes through the child's own core handle.

`RerunError` is told apart by `kind` and `code`:

| `kind` | `code` | Members | When |
|--------|--------|---------|------|
| `connectFailed` | `CONNECT_FAILED` | `retryable`, `message`, `cause` | The handle could not connect, install the schema, or open its closed pools again. Nothing ran |
| `statementFailed` | `STATEMENT_FAILED` | `retryable`, `message`, `stateAfter`, `cause` | A statement failed. `stateAfter` is `"notCommitted"`, or `"unknown"` when the reply to `COMMIT` was lost: then the same keyed rerun sent again answers `keyReplay` if the first one committed |
| `familyCheckFailed` | `FAMILY_CHECK_FAILED` | `retryable`, `message`, `cause` | The check of the client family could not read the family row. Nothing was read or written |
| `invalidCommand`, `invalidPolicy` | `INVALID_COMMAND`, `INVALID_POLICY` | `retryable: false`, `message` | A field of the command or the policy breaks its rule. Nothing ran |
| `classNotRegistered` | `CLASS_NOT_REGISTERED` | `retryable: false`, `message`, `classKey` | The retention class of the new task has no class row. Nothing is written |
| `idempotencyKeyRefused` | `IDEMPOTENCY_KEY_REFUSED` | `retryable: false`, `message` | The idempotency key is empty or holds more than 255 UTF-8 bytes. Nothing is written |
| `idMintRefused` | `ID_MINT_REFUSED` | `retryable: false`, `message` | The id of the new task could not be minted. Nothing is written |
| `clientFamilyMismatch` | `CLIENT_FAMILY_MISMATCH` | `retryable: false`, `message`, `stored` | Another client installed the database, or it records none. Nothing is written |
| `unknownStoredFamily` | `UNKNOWN_STORED_FAMILY` | `retryable: false`, `message`, `stored` | The family row holds a value that is not a client family: corrupt stored data. Nothing is written |
| `retryCodeNotDeclared` | `RETRY_CODE_NOT_DECLARED` | `retryable: false`, `message`, `taskName`, `retryCode` | The stored retry list of the source names a custom code that the contract of `taskName` does not declare in `errorCodes`. Nothing is written. The same call meets it again until the contract declares the code |
| `taskNotRegistered` | `TASK_NOT_REGISTERED` | `retryable: false`, `message`, `taskName` | The stored retry list names a custom code, and the app does not list the contract of `taskName`. Nothing is written |
| `contractViolation` | `CONTRACT_VIOLATION` | `retryable: false`, `message` | A row or stored data broke the rerun's contract. Nothing committed; the same call meets it again |
| `seatRelayFull` | `SEAT_RELAY_FULL` | `retryable: true`, `message`, `cause` | A `SHARED_THREAD` body only: the seat relay holds its bound of waiting calls. Nothing ran |

## Rerun in the Caller's Transaction

`app.rerunTaskIn` runs its statements on a `CallerTransaction`, on the database of the app. Two entry points give an adapter for a driver:

| Entry | Adapter | Takes | Driver range |
|-------|---------|-------|--------------|
| `horsies/rerun/pg` | `pgTransaction(client)` | A `pg.Client` or a `pg.PoolClient` on which the caller ran `BEGIN` | `pg` `>=8.23.1 <8.24.0` |
| `horsies/rerun/postgres` | `postgresTransaction(sql)` | The transaction `sql` that `sql.begin` of `postgres.js` gives its callback | `postgres` `>=3.4.9 <3.5.0` |

The drivers are optional peer dependencies of `horsies`: install the one that the app uses. Neither entry point imports its driver.

### With pg

<!-- ts-check: alone -->
```ts
import type { PoolClient } from "pg";
import type { CallerRerunError, Horsies, RerunCommand, RerunOutcome, RerunEnqueuePolicy, Result, TaskId } from "horsies";
import { pgTransaction } from "horsies/rerun/pg";

const policy: RerunEnqueuePolicy = {
  retention: { kind: "period", days: 30 },
  retainRerunInput: "keep",
  reservationWindow: Temporal.Duration.from({ hours: 24 }),
};

export async function rerunWithAudit(
  app: Horsies,
  client: PoolClient,
  sourceTaskId: TaskId,
): Promise<Result<RerunOutcome, CallerRerunError>> {
  const command: RerunCommand = { sourceTaskId, goodUntil: { kind: "none" } };

  await client.query("BEGIN");

  try {
    await client.query("INSERT INTO audit_log (action, task_id) VALUES ('rerun', $1)", [sourceTaskId]);

    const rerun = await app.rerunTaskIn(pgTransaction(client), command, policy);

    await client.query(rerun.ok ? "COMMIT" : "ROLLBACK");

    return rerun;
  } catch (cause) {
    await client.query("ROLLBACK");
    throw cause;
  }
}
```

- A statement runs only when the client's transaction status is an open transaction (`"T"`). Else the call returns `NO_OPEN_TRANSACTION`, and `state` tells why: `idle` (run `BEGIN` first), `failedTransaction` (roll it back), `notConnected`, or `unrecognized`.
- A `pg.Pool` fails the typecheck: each query of a pool can run on another connection, outside the caller's transaction.
- The adapter sends each parameter as text and reads each cell as the text that the server sent. A client with `binary: true` gives `RESULT_NOT_TEXT`: the statement ran, so roll the transaction back.

### With postgres.js

<!-- ts-check: alone -->
```ts
import type { Sql } from "postgres";
import type { CallerRerunError, Horsies, RerunCommand, RerunOutcome, RerunEnqueuePolicy, Result, TaskId } from "horsies";
import { postgresTransaction } from "horsies/rerun/postgres";

/** A rerun error, thrown inside `sql.begin` so that postgres.js rolls the transaction back. */
class RerunRolledBack extends Error {
  readonly failure: CallerRerunError;

  constructor(failure: CallerRerunError) {
    super(failure.message);
    this.failure = failure;
  }
}

const policy: RerunEnqueuePolicy = {
  retention: { kind: "period", days: 30 },
  retainRerunInput: "keep",
  reservationWindow: Temporal.Duration.from({ hours: 24 }),
};

export async function rerunWithAudit(
  app: Horsies,
  sql: Sql,
  sourceTaskId: TaskId,
): Promise<Result<RerunOutcome, CallerRerunError>> {
  const command: RerunCommand = { sourceTaskId, goodUntil: { kind: "none" } };

  try {
    const outcome = await sql.begin(async (transaction) => {
      await transaction`INSERT INTO audit_log (action, task_id) VALUES ('rerun', ${sourceTaskId})`;

      const rerun = await app.rerunTaskIn(postgresTransaction(transaction), command, policy);

      if (!rerun.ok) {
        throw new RerunRolledBack(rerun.error);
      }

      return rerun.value;
    });

    return { ok: true, value: outcome };
  } catch (cause) {
    if (cause instanceof RerunRolledBack) {
      return { ok: false, error: cause.failure };
    }

    throw cause;
  }
}
```

- `sql.begin` commits when its callback returns. After an error, throw from the callback so that `sql.begin` rolls back.
- The root `sql` and a reserved `sql` fail the typecheck: each of their queries can run outside the caller's transaction.
- The adapter sends each parameter typed as `text`; the cast in the SQL text converts it on the server. It reads each row raw. A cell that is not UTF-8 text gives `CONTRACT_VIOLATION`.

### What the Call Does in the Caller's Transaction

- Statements, on the caller's transaction: the read of the source; then, for an eligible source with a retained input, one write of the new task.
- `app.rerunTaskIn` commits nothing and rolls back nothing. The caller's commit keeps the new task and the key claim; its rollback drops them.
- After an error, the transaction may hold the key claim or the new row: do not commit it.

`CallerRerunError` is told apart by `kind` and `code`:

| `kind` | `code` | Members | When |
|--------|--------|---------|------|
| `driverFailed` | `DRIVER_FAILED` | `message`, `statement`, `driverError` | A statement failed in the caller's driver, or the driver threw. The transaction is aborted: roll it back. Whether to run the transaction again is the rule of `driverError`, for example its SQLSTATE |
| `noOpenTransaction` | `NO_OPEN_TRANSACTION` | `retryable: false`, `message`, `statement`, `state` | The connection had no open transaction, so the statement did not run |
| `resultNotText` | `RESULT_NOT_TEXT` | `retryable: false`, `message`, `statement`, `column`, `format` | The driver gave a result column in a format other than text |
| `invalidCommand`, `invalidPolicy` | `INVALID_COMMAND`, `INVALID_POLICY` | `retryable: false`, `message` | As for `app.rerunTask` |
| `classNotRegistered`, `idempotencyKeyRefused`, `idMintRefused`, `clientFamilyMismatch`, `unknownStoredFamily`, `retryCodeNotDeclared`, `taskNotRegistered`, `contractViolation` | The codes of the same name | As for `app.rerunTask` | As for `app.rerunTask` |

`RerunErrorCode` holds every code of both unions.

### Another Driver

Give `app.rerunTaskIn` an object that implements `CallerTransaction`:

<!-- ts-check: skip elided -- the library's interface, shown for reference -->
```ts
interface CallerTransaction {
  queryOne(statement: CallerStatement): Promise<Result<CallerRow, CallerQueryFailure>>;
}
```

- `queryOne` runs `statement.sql` with `statement.parameters` in the transaction and gives its one row. It never commits and never rolls back.
- `statement.sql` is positional (`$1`, `$2`, ...) with a cast on each parameter. Each parameter is PostgreSQL text input, or `null` for SQL `NULL`: send each one as text.
- `statement.columns` names each result column and its built-in type (`bool`, `int2`, `int4`, `int8`, `text`, `bytea`, `uuid`, `timestamptz`).
- The row is `{ cells }`: one cell per column, in column order, each the text that the server sends in text format, or `null`.
- `CallerQueryFailure` is `driverFailed` (with `driverError`), `noOpenTransaction` (with `state`), `resultNotText` (with `column` and `format`), or `rowRefused` (with `detail`). A throw of `queryOne` counts as `driverFailed`.

## RerunCommand

| Member | Type | Meaning |
|--------|------|---------|
| `sourceTaskId` | `TaskId` | The terminal task to run again |
| `goodUntil` | `RerunDeadline` | `{ kind: "until", at }`: the `goodUntil` of the new task, an instant from 0001-01-01T00:00:00Z to 9999-12-31T23:59:59.999999Z, cut toward the past to the microsecond. `{ kind: "none" }`: no deadline |
| `idempotencyKey` | `string`, optional | The idempotency key of the rerun: a second rerun with the same key and the same command answers `keyReplay`. Left out: an unkeyed rerun. A text with a lone surrogate is `INVALID_COMMAND`. The key rules are on [Atomic Enqueue and Topology](/horsies/operations/atomic-enqueue-and-topology/#make-the-send-repeatable-with-an-idempotency-key) |

## RerunEnqueuePolicy

The new task does not take the policy of its source.

| Member | Type | Meaning |
|--------|------|---------|
| `retention` | `RerunRetention` | `{ kind: "period", days }` with whole days from 1 to 1,825, or `{ kind: "forever" }`. A period whose class has no class row gives `CLASS_NOT_REGISTERED` when the rerun runs. See [Retention Config](/horsies/configuration/retention-config/) |
| `retainRerunInput` | `"keep" \| "drop"` | `"keep"` keeps the input of the new task, so that it can be rerun in turn |
| `reservationWindow` | `Temporal.Duration` | How long the idempotency key holds the rerun: more than 0, at most 30 days, whole microseconds. Days and smaller units only: a year, a month or a week has no fixed length |

## RerunOutcome

`RerunOutcome` is a closed union, told apart by `kind`.

| `kind` | Members | Meaning |
|--------|---------|---------|
| `"enqueued"` | `newTaskId`, `sourceTaskId`, `rerunRootTaskId` | The new task is written. `rerunRootTaskId` is the first task of the rerun chain |
| `"keyReplay"` | `existingTaskId` | The idempotency key holds this rerun command already; the existing task answers |
| `"keyConflict"` | `taskId`, `ownerTaskId` | The idempotency key holds another command, the source's own included; `ownerTaskId` holds it |
| `"sourceLive"` | `taskId` | The source has not ended: its own lifecycle owns its retry |
| `"sourceAbsent"` | `taskId`, `reason` | No record has the id. `reason` is a `TaskAbsenceReason`: `PREDATES_RETAINED_HISTORY`, `WITHIN_RETAINED_HISTORY` or `NOT_CLASSIFIED` |
| `"sourceCorrupt"` | `taskId`, `detail` | The source history row does not decode; a repeat gives the same answer |
| `"notEligible"` | `taskId`, `reason` | `reason` is `"COMPLETED_SOURCE"` or `"WORKFLOW_TASK"` |
| `"inputUnavailable"` | `taskId`, `disposition` | The source stores no input; `disposition` is the stored reason, for example `DECLINED_BY_POLICY` |
| `"inputCorrupt"` | `taskId`, `detail` | The stored input or its options fail their checks |

## Things to Avoid

**Do not commit a transaction after an error of `app.rerunTaskIn`.** It may hold the key claim or the new row of a rerun that did not complete.

**Do not run `app.rerunTaskIn` on a transaction of another database.** The rerun reads and writes the tables of the app's database.

**Do not pass a `pg.Pool`, the root `sql` or a reserved `sql`.** Their queries can run outside the caller's transaction; the typecheck refuses them.

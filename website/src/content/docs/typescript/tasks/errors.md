---
title: Task Errors
summary: The declared codes of a contract, err, the kind and code of a stored error, the exceptionMapper option for thrown errors, UNHANDLED_EXCEPTION, and what the library records.
related: [defining-tasks, retrieving-results, ../../tasks/errors, ../../internals/serialization]
tags: [typescript, errors, TaskError, BuiltInTaskCode, exceptionMapper, UNHANDLED_EXCEPTION]
---

# Task Errors

A body returns an error with `err({ code, message?, data? })`. The code is one of the task's own codes. The library records its own codes, for example `TASK_TIMEOUT` or `WORKER_CRASHED`, in the same error shape. A read tells the two apart by the `kind` of the error. The codes and their meaning are on the shared [Errors Reference](/horsies/tasks/errors/).

## Declared Codes

`errorCodes` of `defineTask` lists the codes that the task may return, as literals:

```ts
import { defineTask, h, Horsies } from "horsies";

const chargeCard = defineTask({
  name: "charge_card",
  input: { orderId: h.uuid(), amountCents: h.int() },
  output: h.string(),
  errorCodes: ["CARD_DECLINED", "PROVIDER_DOWN"],
});
```

- A code that `errorCodes` does not list fails typecheck in `err`, in `retryPolicy.retryFor` and in the `exceptionMapper` option.
- A built-in or reserved name (`TASK_TIMEOUT`, `WORKFLOW_STOPPED`) in `errorCodes` fails typecheck. A list of the type `string[]` fails typecheck too: its codes are not known to the types.
- Without `errorCodes`, the task may return any code that is not a built-in name.

The listed codes are the task's [declared error codes](/horsies/tasks/errors/#declared-error-codes). `Horsies.create` checks them with the core's rules:

| Refusal | Code |
|---------|------|
| A built-in or reserved name | `HRS-212` |
| An empty code, or a code that is not `UPPER_SNAKE_CASE` | `HRS-112` |
| A code given twice | `HRS-113` |

## Return an Error

```ts
const created = Horsies.create({
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  tasks: [chargeCard],
});

if (!created.ok) {
  throw new Error(created.error.refusals.map((refusal) => refusal.message).join("\n"));
}

const app = created.value;

class ProviderUnavailable extends Error {
  override readonly name = "ProviderUnavailable";
}

async function chargeWithProvider(orderId: string, amountCents: number): Promise<"approved" | "declined"> {
  if (amountCents <= 0) {
    throw new ProviderUnavailable(`no charge for order ${orderId}`);
  }

  return "approved";
}

app.implement(
  chargeCard,
  async ({ orderId, amountCents }, { ok, err }) => {
    const answer = await chargeWithProvider(orderId, amountCents);

    if (answer === "declined") {
      return err({ code: "CARD_DECLINED", message: "the issuer declined the card", data: { amountCents } });
    }

    return ok(orderId);
  },
  { exceptionMapper: [[ProviderUnavailable, "PROVIDER_DOWN"]] },
);
```

| Field of `err` | Type | Rule |
|----------------|------|------|
| `code` | `E` | A code of the contract |
| `message` | `string` | Optional |
| `data` | `JsonValue` | Optional. `data: undefined` is no data |

`ok` and `err` are members of the second argument of the body. The contract has no `ok` and no `err`. At run time it throws `TypeError` for a built-in name that a cast takes past the types; in a body that throw is stored as `UNHANDLED_EXCEPTION`.

A code that the core's code rules refuse at the end of a run, for example a code that is not `UPPER_SNAKE_CASE` in a task with no `errorCodes` list, is stored as `RETURN_TYPE_MISMATCH`. Its message names the task and the rule.

### Data

`data` is never refused. Data that cannot be stored is replaced with `{"data_refused": "<cause>"}`, and the code and the message stay, so retry matching and outcome routing still work. Data cannot be stored when:

- it is not JSON-native: a function, a class instance, `NaN` or an infinity;
- it holds a lone surrogate, or a key that only the engine writes (`__h_...`, `__horsies_...`, `__builtin_task_code__`);
- it is nested deeper than the bound of error data, or holds an integer of more than 4,300 digits.

The rules of the error path are on [Serialization](/horsies/internals/serialization/#the-error-path).

## The Kind and the Code

A read gives a `TaskError<E>`:

| Field | Type |
|-------|------|
| `kind` | `"custom"`: a code of the contract. `"builtIn"`: a code that the library recorded |
| `code` | `E` for `custom`. `BuiltInTaskCode` for `builtIn` |
| `message` | `string \| null` |
| `data` | `JsonValue`: `null` when the error has none |
| `exception` | `ExceptionRecord \| null`: set by the library for a thrown error |
| `subWorkflow` | `SubWorkflowFailure \| null`: the failed child workflow of a sub-workflow node |

`BuiltInTaskCode` is the union of the library codes, and `BUILT_IN_TASK_CODES` lists them. A switch over `kind` and then over `code` is exhaustive:

```ts
import type { TaskError } from "horsies";

function chargeFailureText(error: TaskError<"CARD_DECLINED" | "PROVIDER_DOWN">): string {
  switch (error.kind) {
    case "custom":
      switch (error.code) {
        case "CARD_DECLINED":
          return "ask the customer for another card";
        case "PROVIDER_DOWN":
          return "the provider is down; the task can run again later";
      }

    case "builtIn":
      return `the library recorded ${error.code}`;
  }
}
```

The read of a stored error is on [Retrieving Results](/horsies/typescript/tasks/retrieving-results/). A stored custom code that the contract's `errorCodes` does not list is the read error `unknownCustomCode`: the task that stored it had another contract.

## Thrown Errors

A value that a body throws, or a Promise of the body that rejects, is stored as an error:

- The `exceptionMapper` option of `app.implement` maps classes of thrown errors to codes of the contract. The first entry whose class the value is an instance of gives the code; a subclass matches its class.
- With `errorCodes`, a mapped code that the list does not hold fails typecheck. Without `errorCodes`, `check` and the worker start refuse a mapped code that the core's code rules refuse (`HRS-112`, `HRS-212`).
- A value that no entry maps is stored as `UNHANDLED_EXCEPTION`.

Each such error has:

| Part | Value |
|------|-------|
| `message` | `Unhandled exception in task <name>: <type>: <text>` |
| `data` | `{ "exception_type": "<type>" }` |
| `exception.type` | `error.name` of a thrown `Error`; for another value, `null`, `undefined`, the constructor name of an object, or the type text (`string`, `number`, ...) |
| `exception.message`, `exception.repr` | `error.message`; for another value, `String(value)`. Cut at 16 KiB |
| `exception.traceback` | `error.stack`, then the stack of each `cause`, at most 64 links. Cut at 64 KiB. Empty for a value that is not an `Error` |
| `exception.module` | Empty: JavaScript has no module of a class |

A thrown `ZodError` stores a summary of its issues, the path and the code of each, never a value.

Throws outside the body's own call chain do not change the outcome:

- A throw in a callback that the body scheduled (a `setTimeout` callback) is logged with the task id, the task name and the attempt. The child lives, and the outcome of the run does not change.
- A Promise that the body does not await and that rejects is logged. The library does not track such a Promise.

## What the Library Records

| Event | Stored error |
|-------|--------------|
| The input does not decode with the contract's parameters | `WORKER_SERIALIZATION_ERROR`. The message names the task, the field path and the kind of the failure, never a value. `data` has `expected_type` (`<task>.input`) and `validation_error` |
| An `h.taskOutcome(contract)` parameter holds a custom code that the `errorCodes` of `contract` do not declare | `WORKER_SERIALIZATION_ERROR`. The body does not run. The field path is `$.<parameter>.err.error_code`. A built-in code always decodes. A contract with no `errorCodes` accepts every custom code |
| The value of `ok` does not encode with the output type, or does not read back to the same text | `WORKER_SERIALIZATION_ERROR`, with `expected_type` `<task>.output` |
| The body returns a value that is neither `ok(value)` nor `err(failure)` | `RETURN_TYPE_MISMATCH` |
| The body throws, with no `exceptionMapper` entry for the value | `UNHANDLED_EXCEPTION` |

The other library codes (timeouts, crashes, cancels, expiry) are on the [Errors Reference](/horsies/tasks/errors/).

## Things to Avoid

**Don't throw for an expected failure.** A throw needs an `exceptionMapper` entry to keep its code, and a reader sees it as an exception. Return the code with `err`.

<!-- ts-check: skip elided -- a Wrong / Correct pair of statements -->
```ts
// Wrong: stored as UNHANDLED_EXCEPTION
throw new Error("card declined");

// Correct
return err({ code: "CARD_DECLINED", message: "the issuer declined the card" });
```

**Don't put a value in `data` that the reader cannot read.** A class instance or a `Map` is replaced with `{"data_refused": ...}`. Put plain JSON in `data`.

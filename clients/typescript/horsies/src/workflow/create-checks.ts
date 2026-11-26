// The checks of the app's workflow list at `Horsies.create`. They are TypeScript code, so they run on every thread,
// and they need no addon: each list entry, its params, its check cases rule, and one build with all spec checks for a
// workflow with no params.

import { ErrorCode } from "../_catalog/error_codes.ts";
import type { QueueSetup } from "../config/queues.ts";
import { refusal, type HorsiesError } from "../app/refusal.ts";
import { thrownFacts } from "../native/call/thrown-facts.ts";
import type { ContractLookup } from "../send/send.ts";
import { runBuild } from "./builder.ts";
import { checkBuild } from "./checks.ts";
import { workflowRecord } from "./define.ts";
import { unreachable } from "../wire/unreachable.ts";
import type { SpecFinding } from "./findings.ts";
import { type ListedWorkflow, listedWorkflowOf, type WorkflowRegistry, workflowRegistry } from "./registry.ts";

/** What the create checks read: the workflow list, and the app's checked tasks and queues. */
export type WorkflowsAtCreate = {
  readonly workflows: readonly object[];
  readonly tasks: ContractLookup;
  readonly queues: QueueSetup;
};

/** The registry of every workflow that passed its own checks, and every refusal in list order. */
export type WorkflowsChecked = { readonly registry: WorkflowRegistry; readonly refusals: readonly HorsiesError[] };

/**
 * The place of a listed workflow in a refusal: `workflows[1] nightly`; only `workflows[1]` when the name breaks a name
 * rule, so a refusal never carries a name that is too long or not well-formed. Pure.
 */
export function workflowPath(index: number, name: string): string {
  const check = nameCheck(name);

  switch (check.kind) {
    case "valid":
      return `workflows[${index}] ${name}`;
    case "empty":
    case "loneSurrogate":
    case "tooLong":
      return `workflows[${index}]`;
    default:
      return unreachable(check, "workflowPath");
  }
}

/** A spec finding as a refusal under the place `path`. Pure. */
export function findingRefusal(path: string, found: SpecFinding): HorsiesError {
  return refusal(found.code, `${path} ${found.place}`, found.detail, found.help);
}

/**
 * The create checks of the workflow list, in list order: the name (`HRS-001`), the definition key (`HRS-016`,
 * `HRS-017`), the error rule (`HRS-033`), the params (`HRS-105`), the check cases rule (`HRS-027`), and for a workflow
 * with no params one build with every spec check. Effects: the builds of the workflows with no params.
 */
export function checkWorkflowsAtCreate(input: WorkflowsAtCreate): WorkflowsChecked {
  const listed: { readonly index: number; readonly entry: ListedWorkflow }[] = [];
  const refusals: HorsiesError[] = [];
  const keys = new Map<string, object>();

  input.workflows.forEach((workflow, index) => {
    const record = workflowRecord(workflow);
    const path = workflowPath(index, record.name);
    const own = recordRefusals(path, workflow, keys);
    const entry = listedWorkflowOf(workflow);

    if (!entry.ok) {
      own.push(
        ...entry.error.map((refused) =>
          refusal(
            ErrorCode.TASK_STRICT_SIGNATURE_VIOLATION,
            path,
            `${paramsContractPlace(refused.path)}: ${refused.detail} [${refused.rule}]`,
            [],
          ),
        ),
      );
    }

    refusals.push(...own);

    if (entry.ok && own.length === 0) {
      listed.push({ index, entry: entry.value });
    }
  });

  const registry = workflowRegistry(
    listed.map((item) => item.entry),
    input.tasks,
    input.queues,
  );

  for (const { index, entry } of listed) {
    if (entry.record.paramNames.length === 0) {
      refusals.push(...buildRefusals(workflowPath(index, entry.record.name), entry, registry));
    }
  }

  return { registry, refusals };
}

/**
 * The place of a refusal of the params contract in the workflow's terms: the contract's `input` is the workflow's
 * `params`; the declared output keeps its path (`output`, `output.total`). Pure.
 */
function paramsContractPlace(path: string): string {
  const inInput = path === "input" || path.startsWith("input.") || path.startsWith("input[");

  return inInput ? `params${path.slice("input".length)}` : path;
}

/** The most characters (code points) of a workflow name: the column `horsies_workflows.name` is `varchar(255)`. */
export const MAX_WORKFLOW_NAME_CHARS = 255;

/** The most characters (code points) of a definition key: `horsies_workflows.definition_key` is `varchar(255)`. */
export const MAX_DEFINITION_KEY_CHARS = 255;

/** How a text that the core stores (a workflow name, a definition key) meets the text rules. */
type StoredTextCheck =
  | { readonly kind: "valid" }
  | { readonly kind: "empty" }
  | { readonly kind: "loneSurrogate" }
  | { readonly kind: "tooLong"; readonly chars: number; readonly maxChars: number };

/** A text that is not empty but that the core cannot store. */
type UnstorableText = Extract<StoredTextCheck, { readonly kind: "loneSurrogate" | "tooLong" }>;

/** The first text rule that `text` breaks, or `valid`: not blank, well-formed, at most `maxChars` code points. Pure. */
function storedTextCheck(text: string, maxChars: number): StoredTextCheck {
  if (text.trim() === "") {
    return { kind: "empty" };
  }

  if (!text.isWellFormed()) {
    return { kind: "loneSurrogate" };
  }

  // A text of at most maxChars UTF-16 units has at most that many code points.
  const chars = text.length <= maxChars ? text.length : [...text].length;

  return chars <= maxChars ? { kind: "valid" } : { kind: "tooLong", chars, maxChars };
}

/** `HRS-033` for the `subject` text that the core cannot store. The refusal does not carry the text. Pure. */
function unstorableTextRefusal(path: string, subject: string, check: UnstorableText): HorsiesError {
  switch (check.kind) {
    case "loneSurrogate":
      return refusal(ErrorCode.WORKFLOW_INVALID_DEFINITION_CONFIG, path, `the ${subject} holds a lone surrogate`, [
        `give the ${subject} as well-formed UTF-16: each surrogate in a pair`,
      ]);
    case "tooLong":
      return refusal(
        ErrorCode.WORKFLOW_INVALID_DEFINITION_CONFIG,
        path,
        `the ${subject} has ${String(check.chars)} characters; the most is ${String(check.maxChars)}`,
        [],
      );
    default:
      return unreachable(check, "unstorableTextRefusal");
  }
}

/** The text check of a workflow name. Pure. */
function nameCheck(name: string): StoredTextCheck {
  return storedTextCheck(name, MAX_WORKFLOW_NAME_CHARS);
}

/**
 * The refusal of a workflow name that breaks a text rule: an empty name is `HRS-001`; a name with a lone surrogate
 * (the spec data text cannot carry it) or with more than `MAX_WORKFLOW_NAME_CHARS` characters (the stored column
 * cannot hold it) is `HRS-033`. Pure.
 */
function nameRefusals(path: string, name: string): readonly HorsiesError[] {
  const check = nameCheck(name);

  switch (check.kind) {
    case "valid":
      return [];
    case "empty":
      return [refusal(ErrorCode.WORKFLOW_NO_NAME, path, "a workflow name must not be empty", [])];
    case "loneSurrogate":
    case "tooLong":
      return [unstorableTextRefusal(path, "workflow name", check)];
    default:
      return unreachable(check, "nameRefusals");
  }
}

/**
 * The refusals of a definition key: an empty key is `HRS-016`; a key with a lone surrogate or with more than
 * `MAX_DEFINITION_KEY_CHARS` characters is `HRS-033`; a valid key that an earlier entry has is `HRS-017`. Records a
 * valid new key in `keys`.
 */
function keyRefusals(path: string, workflow: object, key: string, keys: Map<string, object>): readonly HorsiesError[] {
  const check = storedTextCheck(key, MAX_DEFINITION_KEY_CHARS);

  switch (check.kind) {
    case "valid":
      return uniqueKeyRefusals(path, workflow, key, keys);
    case "empty":
      return [refusal(ErrorCode.WORKFLOW_NO_DEFINITION_KEY, path, "a definition key must not be empty", [])];
    case "loneSurrogate":
    case "tooLong":
      return [unstorableTextRefusal(path, "definition key", check)];
    default:
      return unreachable(check, "keyRefusals");
  }
}

/** `HRS-017` for a key that an earlier entry has (or the same workflow listed twice); else records the key. */
function uniqueKeyRefusals(path: string, workflow: object, key: string, keys: Map<string, object>): readonly HorsiesError[] {
  const first = keys.get(key);

  if (first === undefined) {
    keys.set(key, workflow);

    return [];
  }

  const detail =
    first === workflow ? "the workflow is listed twice" : `another workflow has the definition key "${key}"`;

  return [refusal(ErrorCode.WORKFLOW_DUPLICATE_DEFINITION_KEY, path, detail, ["give each workflow its own key"])];
}

/** The refusals of a record: the name, the key, its uniqueness, the error rule, the check cases rule. */
function recordRefusals(path: string, workflow: object, keys: Map<string, object>): HorsiesError[] {
  const record = workflowRecord(workflow);

  const refusals: HorsiesError[] = [
    ...nameRefusals(path, record.name),
    ...keyRefusals(path, workflow, record.definitionKey, keys),
  ];

  if (record.onError.kind === "invalid") {
    refusals.push(
      refusal(
        ErrorCode.WORKFLOW_INVALID_DEFINITION_CONFIG,
        path,
        `onError "${record.onError.given}" is not "fail" or "pause"`,
        [],
      ),
    );
  }

  if (record.paramNames.length > 0 && record.checkCases.length === 0) {
    refusals.push(
      refusal(ErrorCode.WORKFLOW_CHECK_CASES_REQUIRED, path, "a workflow with params declares check cases", [
        "add checkCases: one params value per case that check builds",
      ]),
    );
  }

  return refusals;
}

/** The refusals of one build with no params: a throw or a thenable (`HRS-029`), then every spec finding. */
export function buildRefusals(
  path: string,
  entry: ListedWorkflow,
  registry: WorkflowRegistry,
  params: object = {},
): readonly HorsiesError[] {
  const run = runBuild(entry.record.build, params);

  switch (run.kind) {
    case "threw": {
      const facts = thrownFacts(run.cause);

      return [
        refusal(ErrorCode.WORKFLOW_CHECK_BUILDER_EXCEPTION, path, `the build threw ${facts.name}: ${facts.message}`, [
          "a build returns { output } and never throws",
        ]),
      ];
    }

    case "thenable":
      return [
        refusal(ErrorCode.WORKFLOW_CHECK_BUILDER_EXCEPTION, path, "the build returned a thenable; a build is sync", []),
      ];
    case "returned": {
      const checked = checkBuild(entry.record, run, registry.view);

      return checked.ok ? [] : checked.error.map((found) => findingRefusal(path, found));
    }

    default: {
      const unexpected: never = run;

      throw new TypeError(`buildRefusals: a run outside its type: ${String(unexpected)}`);
    }
  }
}

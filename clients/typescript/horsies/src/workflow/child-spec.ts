// The JavaScript side of the child spec source: the core asks for the spec of the child of a sub-workflow node inside
// a workflow step. The addon merges the node's input and runs the strict JSON check on it; this side finds the child by
// its definition key, decodes the params, runs the build, runs the checks and converts the spec.
//
// A build that throws, or whose spec fails a check, fails its node (`WORKFLOW_ENQUEUE_FAILED`) and the step goes on,
// as the Rust client does. This function never throws for user code: a throw here is a defect of the package.

import type { ChildSpecAnswer, ChildSpecCall } from "../native/workflows/addon.ts";
import { jsonTextCheckedByCore } from "../codec/json-text.ts";
import { isTreeObject, readTree } from "../codec/number-text.ts";
import { decodeNode, decodeParameters } from "../codec/reader.ts";
import { memberPath, refusalText, ROOT_PATH } from "../codec/refusal.ts";
import { thrownFacts } from "../native/call/thrown-facts.ts";
import type { ObjectKind } from "../wire/kinds.ts";
import { unreachable } from "../wire/unreachable.ts";
import { runBuild } from "./builder.ts";
import { checkBuild } from "./checks.ts";
import type { SpecFinding } from "./findings.ts";
import type { WorkflowRegistry } from "./registry.ts";
import { type ConversionFacts, specDataTextOf } from "./spec-data.ts";

/** The answer to one child spec call. Effects: the user's build. */
export function childSpecAnswer(
  registry: WorkflowRegistry,
  conversion: ConversionFacts,
  call: ChildSpecCall,
): ChildSpecAnswer {
  const listed = registry.byKey(call.definitionKey);

  if (listed.kind === "notListed") {
    return { kind: "notListed" };
  }

  const params = childParams(listed.params, call.inputText);

  if (params.kind !== "decoded") {
    return params;
  }

  const run = runBuild(listed.record.build, params.value);

  switch (run.kind) {
    case "threw": {
      const facts = thrownFacts(run.cause);

      return {
        kind: "buildFailed",
        code: { kind: "noCode" },
        message: `the build threw ${facts.name}: ${facts.message}`,
      };
    }

    case "thenable":
      return {
        kind: "buildFailed",
        code: { kind: "noCode" },
        message: "the build returned a thenable; a workflow build is sync",
      };
    case "returned": {
      const checked = checkBuild(listed.record, run, registry.view);

      if (!checked.ok) {
        return checksFailed(checked.error);
      }

      const text = specDataTextOf(checked.value.spec, conversion);

      return text.ok
        ? { kind: "built", specData: text.value }
        : {
            kind: "buildFailed",
            code: { kind: "noCode" },
            message: `a string has no stored text at ${text.error.path}`,
          };
    }

    default:
      return unreachable(run, "childSpecAnswer");
  }
}

/** The answer of a build whose spec fails its checks: the code of the first finding, and every finding. */
function checksFailed(findings: readonly [SpecFinding, ...SpecFinding[]]): ChildSpecAnswer {
  const [first] = findings;
  const message = findings.map((found) => `${found.code} ${found.place}: ${found.detail}`).join("; ");

  return {
    kind: "buildFailed",
    code: { kind: "catalog", code: first.code },
    message,
  };
}

/** The params of a child, or the answer of params that do not decode. */
type ChildParams =
  | { readonly kind: "decoded"; readonly value: object }
  | Extract<ChildSpecAnswer, { readonly kind: "memberRefused" | "paramsUndecodable" }>;

/**
 * The params of a child from the merged input (a text that passed the strict check), in the worker form. A member
 * that no param takes or whose value does not decode is `memberRefused`; params that do not decode as a whole (a
 * missing param) are `paramsUndecodable`. Pure.
 */
function childParams(params: ObjectKind, inputText: string): ChildParams {
  const text = jsonTextCheckedByCore(inputText);
  const decoded = decodeParameters(params, text, "worker");

  if (decoded.ok) {
    return { kind: "decoded", value: Object(decoded.value) };
  }

  const tree = readTree(text);

  if (isTreeObject(tree)) {
    for (const [key, node] of Object.entries(tree)) {
      const member = params.byName.get(key);

      if (member === undefined) {
        return { kind: "memberRefused", key, cause: "no param takes the member" };
      }

      const value = decodeNode(member.kind, node, memberPath(ROOT_PATH, key), 0, "worker");

      if (!value.ok) {
        return { kind: "memberRefused", key, cause: refusalText(value.error) };
      }
    }
  }

  return { kind: "paramsUndecodable", cause: refusalText(decoded.error) };
}

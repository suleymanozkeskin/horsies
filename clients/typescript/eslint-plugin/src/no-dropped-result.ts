// The rule `no-dropped-result`: an expression statement whose value is a horsies `Result`, an array or a tuple of Results, or a
// `Promise` of one of these, is an error. The statement drops the value, so the caller never reads which error the call gave.
//
// Passes:
// - `void expression`: the explicit discard. Its value is `undefined`.
// - An assignment (`r = await app.send(...)`): the value is stored.
// - A value that is used (`const r = ...`, `return ...`, an argument): that is not an expression statement.
//
// `@typescript-eslint/no-floating-promises` refuses a `Promise` that is not awaited, of any type. This rule refuses
// a `Result` that is dropped, awaited or not.
//
// The rule needs type information: the parser of typescript-eslint with `parserOptions.projectService` (or
// `parserOptions.project`). Without it, `getParserServices` throws, and ESLint stops the lint with "Error while loading
// rule 'horsies/no-dropped-result': You have used a rule which requires type information, but don't have
// parserOptions set to generate type information for this file".

import { AST_NODE_TYPES, ESLintUtils, type TSESLint, type TSESTree } from "@typescript-eslint/utils";

import { heldResultOf } from "./result-brand.ts";

/** The page that documents the rules of this package. */
const RULE_DOCS_URL = "https://github.com/suleymanozkeskin/horsies/tree/main/clients/typescript/eslint-plugin#no-dropped-result";

/** The ids of the messages that the rule reports. */
export type DroppedResultMessage = "droppedResult" | "droppedPromisedResult";

const createRule = ESLintUtils.RuleCreator(() => RULE_DOCS_URL);

/** The rule `no-dropped-result`. */
export const noDroppedResult: TSESLint.RuleModule<DroppedResultMessage, []> = createRule({
  name: "no-dropped-result",
  meta: {
    type: "problem",
    docs: {
      description: "Refuse an expression statement that drops a horsies Result or a Promise of one",
    },
    messages: {
      droppedResult:
        "This statement drops a horsies Result. Read the Result (check `ok`), or write `void` before the expression to discard it.",
      droppedPromisedResult:
        "This statement drops a Promise of a horsies Result. Await it and read the Result (check `ok`), or write `void` before the expression to discard it.",
    },
    schema: [],
  },
  defaultOptions: [],
  create(context) {
    const services = ESLintUtils.getParserServices(context);
    const checker = services.program.getTypeChecker();

    return {
      ExpressionStatement(statement: TSESTree.ExpressionStatement): void {
        if (statement.expression.type === AST_NODE_TYPES.AssignmentExpression) {
          return;
        }

        const held = heldResultOf(checker, services.getTypeAtLocation(statement.expression));

        switch (held) {
          case "noResult":
            return;
          case "result":
            context.report({ node: statement, messageId: "droppedResult" });

            return;
          case "promisedResult":
            context.report({ node: statement, messageId: "droppedPromisedResult" });

            return;
          default: {
            const unreachable: never = held;

            return unreachable;
          }
        }
      },
    };
  },
});

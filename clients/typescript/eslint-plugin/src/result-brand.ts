// The brand of the horsies `Result` type, read from the type checker.
//
// horsies marks each member of its `Result` union (`Ok`, `Err`) with an optional property whose key is a
// `unique symbol` and whose type is the text literal `"horsies.Result"` (`horsies/src/wire/result.ts`). This module
// reads that structure. It does not read the name of a type, of a method or of an import, so it finds the brand in
// every copy of horsies that a project installs.

import ts from "typescript";

/**
 * The type of the brand property: equal to the brand of `horsies/src/wire/result.ts`. The tests lint the client's
 * own `Result` type, so a change of the brand in the client fails them.
 */
const RESULT_BRAND = "horsies.Result";

/**
 * The horsies `Result` that a value holds:
 * - `result`: the value is a `Result`, a union with a `Result` member, or an array or tuple whose element type is
 *   one of these (`Result[]`, `[Result, Result]`), to the depth `MAX_COLLECTION_DEPTH`.
 * - `promisedResult`: the value is a `Promise` (or another thenable) whose awaited value is a `Result`.
 * - `noResult`: neither.
 */
export type HeldResult = "result" | "promisedResult" | "noResult";

/** The key of `property` is a `unique symbol` and its type, without `undefined`, is the brand literal. Pure. */
function isBrandProperty(checker: ts.TypeChecker, property: ts.Symbol): boolean {
  const declaration = property.valueDeclaration;

  if (declaration === undefined) {
    return false;
  }

  const name = ts.getNameOfDeclaration(declaration);

  if (name === undefined || !ts.isComputedPropertyName(name)) {
    return false;
  }

  const keyType = checker.getTypeAtLocation(name.expression);

  if ((keyType.flags & ts.TypeFlags.UniqueESSymbol) === 0) {
    return false;
  }

  const valueType = checker.getNonNullableType(checker.getTypeOfSymbol(property));

  return valueType.isStringLiteral() && valueType.value === RESULT_BRAND;
}

/** The deepest nesting of arrays and tuples that the rule reads for a `Result` (`Result[][]` is depth 2). */
const MAX_COLLECTION_DEPTH = 3;

/** The type has the brand property. Pure. */
function hasBrandProperty(checker: ts.TypeChecker, type: ts.Type): boolean {
  return checker.getPropertiesOfType(type).some((property) => isBrandProperty(checker, property));
}

/** A member of `type` (the type itself, or a member of its union) has the brand property, or is a collection of such. Pure. */
function carriesBrand(checker: ts.TypeChecker, type: ts.Type, depth: number = 0): boolean {
  const members = type.isUnion() ? type.types : [type];

  return members.some((member) => {
    if (hasBrandProperty(checker, member)) {
      return true;
    }

    const isCollection = checker.isArrayType(member) || checker.isTupleType(member);

    return isCollection && depth < MAX_COLLECTION_DEPTH && checker.getTypeArguments(member as ts.TypeReference).some((element) => carriesBrand(checker, element, depth + 1));
  });
}

/** The horsies `Result` that a value of `type` holds. Pure. */
export function heldResultOf(checker: ts.TypeChecker, type: ts.Type): HeldResult {
  if (carriesBrand(checker, type)) {
    return "result";
  }

  const awaited = checker.getAwaitedType(type);

  if (awaited !== undefined && awaited !== type && carriesBrand(checker, awaited)) {
    return "promisedResult";
  }

  return "noResult";
}

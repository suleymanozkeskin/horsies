// The internals of a zod 4 schema that the adapter reads (`schema._zod`), declared by their structure.
//
// The adapter imports no zod module, also as a type: the main entry point imports it statically, and a user without
// zod loads, bundles and type checks the main entry point. The adapter reads and runs a schema only through the
// schema's own internals, so a schema of each zod copy (the ES module or the CommonJS build, a bundled copy, another
// install) reads the same. `tests/unit/zod/internals.test.ts` checks at type check that the declarations of the
// installed zod fit these types, and that both list the same definition types.
//
// zod calls the functions of a schema with its own payload type, which holds more than `ZodPayload` names. So each
// function type here is the type of a method (`DefinitionFunctions`, `ZodInternals.run`): its parameters compare in
// both directions with zod's declaration.

/** A primitive value of a zod literal or enum (zod's `util.Primitive`). */
export type ZodLiteralValue = string | number | symbol | bigint | boolean | null | undefined;

/** An issue that a zod check or parse adds to a payload: the parts that the adapter reads. */
export type ZodIssue = {
  readonly code: string;
  readonly path?: readonly PropertyKey[] | undefined;
  readonly continue?: boolean | undefined;
};

/** The value of one zod run and the issues of its checks and parses (zod's `ParsePayload`). zod writes all three. */
export type ZodPayload = { value: unknown; issues: ZodIssue[]; aborted?: boolean | undefined };

/** The context of one zod run that the adapter starts: synchronous, in one direction. */
export type ZodRunContext = { readonly async: false; readonly direction: "forward" | "backward" };

/** The functions that zod's definitions hold, each as a method (see the module comment). */
type DefinitionFunctions = {
  /** The run rule of a check: whether the check runs after the issues so far. */
  when(payload: ZodPayload): boolean;
  /** The decode (`transform`) or the encode (`reverseTransform`) of `z.codec()`: it takes the payload's value. */
  convert(value: ZodPayload["value"], payload: ZodPayload): unknown;
  /** The test function of `z.custom()`. */
  test(value: never): unknown;
};

/** The run rule of a check. */
export type ZodWhenRule = DefinitionFunctions["when"];

/** A zod codec function: the decode (`transform`) or the encode (`reverseTransform`) of `z.codec()`. */
export type ZodCodecFunction = DefinitionFunctions["convert"];

/** The test function of `z.custom()`. */
export type ZodCustomTest = DefinitionFunctions["test"];

/** The definition of a check: its name, its stop rule and its run rule. */
export type ZodCheckDefinition = {
  readonly check: string;
  readonly abort?: boolean | undefined;
  readonly when?: ZodWhenRule | undefined;
};

/** A check that zod runs on a value: a refinement, a length or range check, a string or number format. */
export type ZodCheck = {
  readonly _zod: {
    readonly def: ZodCheckDefinition;
    check(payload: ZodPayload): void | Promise<void>;
  };
};

/** The fields of every definition: its type, and its own checks. */
export type ZodDefinitionBase = { readonly type: string; readonly checks?: readonly ZodCheck[] | undefined };

/** A zod 4 schema: its internals. */
export type ZodSchema = { readonly _zod: ZodInternals };

/** The internals of a schema that the adapter reads and runs. */
export type ZodInternals = {
  readonly def: ZodDefinitionBase;
  readonly traits: ReadonlySet<string>;
  /** The values that pass the schema, when they are a closed set (an enum, a literal). */
  readonly values?: ReadonlySet<ZodLiteralValue> | undefined;
  /** The schema's own constructor: a new schema of the same zod copy. */
  readonly constr: new (definition: ZodDefinitionBase) => ZodSchema;
  /** The parse of a value with every check: the function that zod's `safeParse` and `safeEncode` call. */
  run(payload: ZodPayload, context: ZodRunContext): ZodPayload | Promise<ZodPayload>;
};

/** The entries of a `z.enum()`: each key and its value (a TypeScript enum, or the values as their own keys). */
export type ZodEnumEntries = { readonly [key: string]: string | number };

/** The declared members of a `z.object()`, by name. */
export type ZodMembers = { readonly [name: string]: ZodSchema };

/** The definition of a pipe: a zod codec when it holds both functions. */
export type ZodPipeDefinition = Typed<"pipe"> & {
  readonly in: ZodSchema;
  readonly out: ZodSchema;
  readonly transform?: ZodCodecFunction | undefined;
  readonly reverseTransform?: ZodCodecFunction | undefined;
};

/** The definition of `z.custom()`: its test function. */
export type ZodCustomDefinition = Typed<"custom"> & { readonly fn: ZodCustomTest };

type Typed<T extends string> = { readonly type: T; readonly checks?: readonly ZodCheck[] | undefined };

type Coercible<T extends string> = Typed<T> & { readonly coerce?: boolean | undefined };

type Wrapping<T extends string> = Typed<T> & { readonly innerType: ZodSchema };

type Defaulting<T extends string> = Wrapping<T> & { readonly defaultValue: unknown };

/**
 * The definition data of a zod 4 schema: one member per definition type of the supported releases, with the fields
 * that the reader reads. The fields that only some releases write (`partial`, `mode`, `inclusive`, `discriminator`,
 * `unionFallback`, `format`) are read with `in`, not declared.
 */
export type ZodDefinitionData =
  | Coercible<"string">
  | Coercible<"number">
  | Coercible<"bigint">
  | Coercible<"boolean">
  | Typed<"null">
  | (Typed<"literal"> & { readonly values: readonly ZodLiteralValue[] })
  | (Typed<"enum"> & { readonly entries: ZodEnumEntries })
  | Typed<"template_literal">
  | (Typed<"object"> & { readonly "shape": ZodMembers; readonly catchall?: ZodSchema | undefined })
  | (Typed<"array"> & { readonly element: ZodSchema })
  | (Typed<"tuple"> & { readonly items: readonly ZodSchema[]; readonly rest: ZodSchema | null })
  | (Typed<"record"> & { readonly keyType: ZodSchema; readonly valueType: ZodSchema })
  | (Typed<"union"> & { readonly options: readonly ZodSchema[] })
  | Wrapping<"nullable">
  | Wrapping<"optional">
  | Wrapping<"nonoptional">
  | Wrapping<"readonly">
  | Defaulting<"default">
  | Defaulting<"prefault">
  | (Typed<"lazy"> & { readonly getter: () => ZodSchema })
  | ZodPipeDefinition
  | ZodCustomDefinition
  | Typed<"any">
  | Typed<"unknown">
  | Typed<"transform">
  | Typed<"success">
  | Typed<"catch">
  | Typed<"date">
  | Typed<"intersection">
  | Typed<"symbol">
  | Typed<"undefined">
  | Typed<"void">
  | Typed<"never">
  | Typed<"nan">
  | Typed<"map">
  | Typed<"set">
  | Typed<"file">
  | Typed<"promise">
  | Typed<"function">;

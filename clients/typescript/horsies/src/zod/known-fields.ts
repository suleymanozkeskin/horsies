// The closed lists of the zod definitions that the adapter reads: the fields of each definition type that it maps,
// the check names, and the string, number and bigint formats. Each list is the union over the supported zod releases
// (`SUPPORTED_ZOD`, `version.ts`), from the definition interfaces of `zod/v4/core` and from the definitions that each
// release builds. A field, a check or a format outside a list is refused: the adapter reads no definition part that
// it does not know, so a part that a newer zod adds cannot make a mapping looser than zod's own parse.

/** Fields that every definition may hold: its type, its custom error map, its checks. */
const COMMON_FIELDS = ["type", "error", "checks"] as const;

/** Fields of a definition that is a check itself (a string, number or bigint format, `z.custom()`). */
const CHECK_FIELDS = ["check", "abort", "when"] as const;

/** Fields of the parameters of the string formats: patterns, versions, date-time options, a custom check. */
const STRING_FORMAT_FIELDS = [
  "format",
  "pattern",
  "fn",
  "version",
  "hostname",
  "protocol",
  "normalize",
  "local",
  "offset",
  "precision",
  "alg",
  "delimiter",
  "length",
] as const;

function fields(...names: readonly string[]): ReadonlySet<string> {
  return new Set([...COMMON_FIELDS, ...names]);
}

/** The known fields of each definition type that the adapter maps to a kind. */
export const KNOWN_FIELDS: ReadonlyMap<string, ReadonlySet<string>> = new Map([
  ["string", fields("coerce", ...CHECK_FIELDS, ...STRING_FORMAT_FIELDS)],
  ["number", fields("coerce", "format", ...CHECK_FIELDS)],
  ["bigint", fields("coerce", "format", ...CHECK_FIELDS)],
  ["boolean", fields("coerce")],
  ["null", fields()],
  ["literal", fields("values")],
  ["enum", fields("entries")],
  ["template_literal", fields("parts", "format")],
  ["object", fields("shape", "catchall")],
  ["array", fields("element")],
  ["tuple", fields("items", "rest")],
  ["record", fields("keyType", "valueType", "mode", "partial")],
  ["union", fields("options", "discriminator", "unionFallback", "inclusive")],
  ["nullable", fields("innerType")],
  ["optional", fields("innerType")],
  ["nonoptional", fields("innerType")],
  ["readonly", fields("innerType")],
  ["default", fields("innerType", "defaultValue")],
  ["prefault", fields("innerType", "defaultValue")],
  ["lazy", fields("getter")],
  ["pipe", fields("in", "out", "transform", "reverseTransform")],
  ["custom", fields("fn", "params", "path", ...CHECK_FIELDS)],
]);

/** The check names of zod's checks. Each one accepts or refuses a value, except `overwrite`, which replaces it. */
export const KNOWN_CHECKS: ReadonlySet<string> = new Set([
  "custom",
  "less_than",
  "greater_than",
  "multiple_of",
  "number_format",
  "bigint_format",
  "max_size",
  "min_size",
  "size_equals",
  "max_length",
  "min_length",
  "length_equals",
  "string_format",
  "property",
  "properties",
  "mime_type",
  "overwrite",
]);

/** The string formats of zod's own string schemas and string checks. A custom string format has its own name. */
export const KNOWN_STRING_FORMATS: ReadonlySet<string> = new Set([
  "email",
  "url",
  "emoji",
  "uuid",
  "guid",
  "nanoid",
  "cuid",
  "cuid2",
  "ulid",
  "xid",
  "ksuid",
  "datetime",
  "date",
  "time",
  "duration",
  "ipv4",
  "ipv6",
  "mac",
  "cidrv4",
  "cidrv6",
  "base64",
  "base64url",
  "e164",
  "jwt",
  "credit_card",
  "iban",
  "regex",
  "lowercase",
  "uppercase",
  "starts_with",
  "ends_with",
  "includes",
]);

/** The number formats that hold an integer: `z.int()`, `z.int32()`, `z.uint32()`, `.int()`. */
export const INTEGER_FORMATS: ReadonlySet<string> = new Set(["safeint", "int32", "uint32"]);

/** The number formats that hold a float: `z.float32()`, `z.float64()`. */
export const FLOAT_FORMATS: ReadonlySet<string> = new Set(["float32", "float64"]);

/** The bigint formats: `z.int64()`, `z.uint64()`. */
export const BIGINT_FORMATS: ReadonlySet<string> = new Set(["int64", "uint64"]);

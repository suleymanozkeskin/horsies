// The public names of the zod adapter. `src/zod.ts` (the entry point `horsies/zod`) exports this module. The main entry
// loads this entry with a synchronous `require` when a contract holds a zod schema, and calls `zodMemberWire` or
// `zodValueWire`. Every type that a public declaration of this entry names is exported here (the public API gate
// refuses a type that it cannot import); the types of the wire types are the ones of `horsies`.

export type { DecimalRefused } from "../codec/decimal-text.ts";

export type { KindRefused, KindRule } from "../wire/check.ts";

export type { Decimal } from "../wire/decimal.ts";

export type { AnyWire, RequiredWire, SentOf, ValueOf, ValueWire } from "../wire/infer.ts";

export type { Err, Ok, Result } from "../wire/result.ts";

export type { Presence, Role, Wire } from "../wire/wire-type.ts";

export { hz, type TextCodec, type WireSchema, type ZodBuilders } from "./codecs.ts";

export { type ZodAdapterRefused, zodMemberWire, zodValueWire } from "./kind.ts";

export type { NamedZodRule, ZodRule, ZodSchemaRefused } from "./rules.ts";

export type { SelfCheckFailure } from "./self-check.ts";

export type { StandardSchemaMarked, ZodReleaseRefused, ZodVersion } from "./version.ts";

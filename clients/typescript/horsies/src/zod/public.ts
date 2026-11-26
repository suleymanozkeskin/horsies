// The public names of `horsies/zod`: the `hz.*` builders, which build zod schemas. `src/zod.ts` (the entry point
// `horsies/zod`) exports this module. The adapter that reads a zod schema imports no zod and is part of the main entry
// point (`src/zod/kind.ts`). Every type that a public declaration of this entry names is exported here (the public API
// gate refuses a type that it cannot import); the types of the wire types are the ones of `horsies`.

export type { DecimalRefused } from "../codec/decimal-text.ts";

export type { Decimal } from "../wire/decimal.ts";

export type { AnyWire, RequiredWire, SentOf, ValueOf } from "../wire/infer.ts";

export type { Err, Ok, Result } from "../wire/result.ts";

export type { Presence, Role, Wire } from "../wire/wire-type.ts";

export { hz, type TextCodec, type WireSchema, type ZodBuilders } from "./codecs.ts";

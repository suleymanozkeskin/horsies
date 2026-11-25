// The public names of the wire types and the codec. `src/index.ts` exports this module.

export type {
  CodecRefused,
  DecodeRefusalKind,
  DecodeRefused,
  EncodeRefusalKind,
  EncodeRefused,
} from "../codec/refusal.ts";

export { h, type WireBuilders } from "./builders.ts";

export type { EmptyObject } from "./infer.ts";

export type { Err, Ok, Result } from "./result.ts";

export type { Presence, Wire } from "./wire-type.ts";

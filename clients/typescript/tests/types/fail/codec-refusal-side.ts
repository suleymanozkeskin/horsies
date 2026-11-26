// expect: TS2339 at 8, TS2367 at 12
// A codec refusal has one `kind` per member (`encodeRefused`, `decodeRefused`, ...), and the refusal of the writer
// is in `refusal`: there is no `side`, and `kind` is not the refusal.

import type { CodecRefused, EncodeRefused } from "../../../horsies/src/index.ts";

export function sideOf(refused: CodecRefused): string {
  return refused.side;
}

export function isWrongType(refused: EncodeRefused): boolean {
  return refused.kind === "WrongType";
}

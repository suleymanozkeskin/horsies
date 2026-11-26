// expect: TS2345 at 9
// decodeWire gives the worker-form type; the stored form is read with decodeValue.

import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import { decodeWire } from "../../../horsies/src/codec/typed.ts";
import { h } from "../../../horsies/src/index.ts";

const retries = h.object({ count: h.int().default(3) });
export const read = decodeWire(retries, jsonTextCheckedByCore("{}"), "stored");

import { describe, expect, test } from "bun:test";

import { DEFAULT_RETENTION_CLASS_KEY } from "../../../horsies/src/_catalog/retention_class.ts";
import type { RetentionClassKey } from "../../../horsies/src/_catalog/retention_class.ts";

describe("retention class key", () => {
  test("the default key is a key without a type assertion", () => {
    const key: RetentionClassKey = DEFAULT_RETENTION_CLASS_KEY;

    expect(key).toBe("standard_30d");
  });
});

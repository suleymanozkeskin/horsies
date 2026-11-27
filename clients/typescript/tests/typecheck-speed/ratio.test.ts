import { describe, expect, test } from "bun:test";

import { durationFromMilliseconds } from "./measure.ts";
import { medianOfRatios, type Ratio, ratioOf, ratioText, spreadOfRatios, sumOfRatios } from "./ratio.ts";

const ratios: Ratio[] = [12, 10, 11].map((appPerControl) => ({ appPerControl }));

describe("ratioOf", () => {
  test("ratio: the app median divided by the control median", () => {
    expect(ratioOf(durationFromMilliseconds(300), durationFromMilliseconds(30))).toEqual({ kind: "ratio", ratio: { appPerControl: 10 } });
  });

  test("controlTimeZero: a control median of zero", () => {
    expect(ratioOf(durationFromMilliseconds(300), durationFromMilliseconds(0))).toEqual({ kind: "controlTimeZero" });
  });

  test("the ratio does not change when both times scale with the machine speed", () => {
    const fast = ratioOf(durationFromMilliseconds(150), durationFromMilliseconds(15));
    const slow = ratioOf(durationFromMilliseconds(600), durationFromMilliseconds(60));

    expect(fast).toEqual(slow);
  });
});

describe("statistics of ratios", () => {
  test("medianOfRatios: the middle value", () => {
    expect(medianOfRatios(ratios)).toEqual({ appPerControl: 11 });
  });

  test("medianOfRatios: an even count is a defect that throws", () => {
    expect(() => medianOfRatios(ratios.slice(0, 2))).toThrow("odd count");
  });

  test("spreadOfRatios: the largest value minus the smallest value", () => {
    expect(spreadOfRatios(ratios)).toEqual({ appPerControl: 2 });
  });

  test("spreadOfRatios: no values is a defect that throws", () => {
    expect(() => spreadOfRatios([])).toThrow("no ratios");
  });

  test("sumOfRatios: the sum", () => {
    expect(sumOfRatios({ appPerControl: 10 }, { appPerControl: 2 })).toEqual({ appPerControl: 12 });
  });

  test("ratioText: two decimals", () => {
    expect(ratioText({ appPerControl: 9.3149 })).toBe("9.31");
  });
});

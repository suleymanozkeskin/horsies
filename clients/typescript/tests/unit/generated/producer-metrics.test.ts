import { describe, expect, test } from "bun:test";

import { TASK_SEND_DURATION_BUCKET_BOUNDS_SECONDS } from "../../../horsies/src/_catalog/producer_metrics.ts";

const bucketBoundCount = 21;

describe("generated producer metrics", () => {
  test("the send duration has 21 bucket bounds", () => {
    expect(TASK_SEND_DURATION_BUCKET_BOUNDS_SECONDS).toHaveLength(bucketBoundCount);
  });

  test("the bucket bounds ascend and are distinct", () => {
    const ascending = TASK_SEND_DURATION_BUCKET_BOUNDS_SECONDS.toSorted((left, right) => left - right);

    expect(TASK_SEND_DURATION_BUCKET_BOUNDS_SECONDS).toEqual(ascending);
    expect(new Set(TASK_SEND_DURATION_BUCKET_BOUNDS_SECONDS).size).toBe(bucketBoundCount);
  });

  test("the bucket bounds are frozen", () => {
    expect(Object.isFrozen(TASK_SEND_DURATION_BUCKET_BOUNDS_SECONDS)).toBe(true);
  });
});

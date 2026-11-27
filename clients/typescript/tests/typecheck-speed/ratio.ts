// The statistic of the typecheck-speed check: the typecheck time of the app project divided by the typecheck time
// of the control project, both measured in one session on one machine. The machine speed cancels in the ratio.

/** The app time divided by the control time of one session. A ratio is above 0. */
export interface Ratio {
  readonly appPerControl: number;
}

export type RatioOutcome = { readonly kind: "ratio"; readonly ratio: Ratio } | { readonly kind: "controlTimeZero" };

/** The ratio of the two medians of a session. `controlTimeZero` when the control median is zero. */
export function ratioOf(appMedian: Temporal.Duration, controlMedian: Temporal.Duration): RatioOutcome {
  const controlMicroseconds = controlMedian.total("microsecond");

  if (controlMicroseconds === 0) {
    return { kind: "controlTimeZero" };
  }

  return { kind: "ratio", ratio: { appPerControl: appMedian.total("microsecond") / controlMicroseconds } };
}

function sortedRatios(values: readonly Ratio[]): Ratio[] {
  return [...values].sort((one, two) => one.appPerControl - two.appPerControl);
}

/** The middle value of an odd count of ratios. */
export function medianOfRatios(values: readonly Ratio[]): Ratio {
  if (values.length % 2 !== 1) {
    throw new Error(`typecheck speed: a median needs an odd count of ratios, got ${values.length}`);
  }

  const middle = sortedRatios(values)[(values.length - 1) / 2];

  if (middle === undefined) {
    throw new Error("typecheck speed: no middle ratio");
  }

  return middle;
}

/** The largest ratio minus the smallest ratio. */
export function spreadOfRatios(values: readonly Ratio[]): Ratio {
  const sorted = sortedRatios(values);
  const smallest = sorted[0];
  const largest = sorted.at(-1);

  if (smallest === undefined || largest === undefined) {
    throw new Error("typecheck speed: the spread of no ratios");
  }

  return { appPerControl: largest.appPerControl - smallest.appPerControl };
}

export function sumOfRatios(one: Ratio, two: Ratio): Ratio {
  return { appPerControl: one.appPerControl + two.appPerControl };
}

/** A ratio with two decimals, for example `9.31`. */
export function ratioText(ratio: Ratio): string {
  return ratio.appPerControl.toFixed(2);
}

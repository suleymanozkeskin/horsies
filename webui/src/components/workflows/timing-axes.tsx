// The axes of the timing views. A linear axis labels round elapsed times
// from 0; a log axis labels each decade and draws minor ticks that crowd
// toward the next decade, so it never looks linear.

import { assertNever } from '@/lib/exhaustive';
import { formatAxisTick } from '@/lib/format-duration';

import { linearTicks, logMajorTicks, logMinorTicks, logPct, type LogDomain } from './time-scale';

/** Where a linear axis sits: under a strip, with its tick marks on top, or
 * over the rows of a table, with its tick marks at the bottom. */
export type LinearAxisPlacement = 'under_strip' | 'over_rows';

function tickMarkClass(placement: LinearAxisPlacement): string {
  switch (placement) {
    case 'under_strip':
      return 'absolute -top-1.5 h-1 w-px bg-tick';
    case 'over_rows':
      return 'absolute -bottom-2 h-[5px] w-px bg-tick';
    default:
      return assertNever(placement, 'linear axis placement');
  }
}

function labelClass(placement: LinearAxisPlacement): string {
  switch (placement) {
    case 'under_strip':
      return 'absolute top-1.5 whitespace-nowrap';
    case 'over_rows':
      return 'absolute bottom-2 whitespace-nowrap';
    default:
      return assertNever(placement, 'linear axis placement');
  }
}

/** The labels of a linear axis of `spanMs`. Each label slides inside the
 * axis box, so the first and the last stay in it; its tick mark stays at its
 * time. */
export function LinearAxisLabels({ spanMs, placement }: { spanMs: number; placement: LinearAxisPlacement }) {
  return (
    <>
      {linearTicks(spanMs).map(offsetMs => {
        const pct = (offsetMs / spanMs) * 100;
        return (
          <span
            key={offsetMs}
            data-axis-tick={offsetMs}
            className={labelClass(placement)}
            style={{ left: `${pct}%`, transform: `translateX(-${pct}%)` }}
          >
            <span className={tickMarkClass(placement)} style={{ left: `${pct}%` }} aria-hidden />
            {formatAxisTick(offsetMs)}
          </span>
        );
      })}
    </>
  );
}

/** Vertical lines at the ticks of a linear axis, over the rows of a view. */
export function LinearGridLines({ spanMs }: { spanMs: number }) {
  return (
    <>
      {linearTicks(spanMs).map(offsetMs => (
        <span
          key={offsetMs}
          className="absolute inset-y-0 w-px bg-hairline"
          style={{ left: `${(offsetMs / spanMs) * 100}%` }}
        />
      ))}
    </>
  );
}

/** A log axis: a label on each labeled line, and the minor ticks. */
export function LogAxisLabels({ domain }: { domain: LogDomain }) {
  return (
    <span className="absolute inset-0 font-mono text-10 text-muted-foreground" aria-hidden>
      {logMinorTicks(domain).map(ms => (
        <span key={ms} className="absolute bottom-0 h-[3px] w-px bg-hairline-strong" style={{ left: `${logPct(ms, domain)}%` }} />
      ))}
      {logMajorTicks(domain).map(ms => (
        <span key={ms} data-log-tick={ms} className="absolute bottom-0 h-[5px] w-px bg-tick" style={{ left: `${logPct(ms, domain)}%` }}>
          <span className="absolute bottom-2 left-0 -translate-x-1/2 whitespace-nowrap">{formatAxisTick(ms)}</span>
        </span>
      ))}
    </span>
  );
}

/** How the lines of a log axis cross the bars: `grid` behind them, `notch`
 * over them in the color of the window, so a long bar shows its decades. */
export type LogLinesTone = 'grid' | 'notch';

/** The look of a line of a log axis in its tone. */
function logLineLook(tone: LogLinesTone): { className: string; opacity: number } {
  switch (tone) {
    case 'grid':
      return { className: 'absolute inset-y-0 w-px bg-hairline', opacity: 1 };
    case 'notch':
      return { className: 'absolute inset-y-0 w-px bg-background', opacity: 0.55 };
    default:
      return assertNever(tone, 'log lines tone');
  }
}

/** Vertical lines at the labeled lines of a log axis. */
export function LogLines({ domain, tone }: { domain: LogDomain; tone: LogLinesTone }) {
  const look = logLineLook(tone);
  return (
    <>
      {logMajorTicks(domain).map(ms => (
        <span
          key={ms}
          className={look.className}
          style={{ left: `${logPct(ms, domain)}%`, opacity: look.opacity }}
        />
      ))}
    </>
  );
}

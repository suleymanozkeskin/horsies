// The details of the attempt panel, closed until the reader opens them: the
// exact times of each interval, the worker hosts, the task id, the trace
// link, the expiry and the date of the times.

import { useId, useState, type ReactNode } from 'react';

import { ChevronRight } from 'lucide-react';

import { OutcomeCap } from '@/components/workflows/outcome-sign';
import { phaseLabel } from '@/components/workflows/phase';
import { dateText, zoneText } from '@/lib/clock-text';
import { assertNever } from '@/lib/exhaustive';
import { formatInstant, formatMilliseconds } from '@/lib/format-duration';
import { statusLabel } from '@/lib/status-utils';
import { instantMs } from '@/lib/time-text';
import { traceLinkOf } from '@/lib/trace-link';
import { cn } from '@/lib/utils';
import type { TaskRow } from '@/types/workflows';

import { clockFrom, intervalMs, ownerAttempt, type PanelBase, type PanelModel } from './attempt-panel-model';
import type { RecordedAttempt } from './attempt-story';

/** The exact times of each attempt: a head row with its outcome, then a row
 * for each of its intervals (the retry wait after it included). */
function TimesTable({ model, base }: { model: PanelModel; base: PanelBase }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse text-11">
        <tbody>
          {model.attempts.map((attempt, index) => (
            <AttemptRows key={attempt.number} model={model} attempt={attempt} first={index === 0} base={base} />
          ))}
        </tbody>
      </table>
    </div>
  );
}

function AttemptRows({
  model,
  attempt,
  first,
  base,
}: {
  model: PanelModel;
  attempt: RecordedAttempt;
  first: boolean;
  base: PanelBase;
}) {
  const number = attempt.number;
  return (
    <>
      <tr data-detail-attempt={number}>
        <td colSpan={2} className={cn('py-0.5 font-semibold whitespace-nowrap', !first && 'pt-1.75')}>
          <span className="inline-flex items-center gap-1.25">
            <OutcomeCap status={attempt.outcome} sizePx={11} />
            attempt {number}
          </span>
        </td>
        <td className={cn('py-0.5 text-right align-middle font-mono text-11 whitespace-nowrap text-muted-foreground', !first && 'pt-1.75')}>
          {attempt.errorCode ?? statusLabel(attempt.outcome)}
        </td>
      </tr>
      {model.intervals
        .filter(interval => ownerAttempt(interval.owner) === number)
        .map(interval => (
          <tr key={interval.id} data-detail-interval={interval.kind}>
            <td className="py-0.5 pl-4.25 whitespace-nowrap">{phaseLabel(interval.kind)}</td>
            <td className="py-0.5 pl-2.5 font-mono text-10 whitespace-nowrap text-muted-foreground">
              {clockFrom(base, interval.startMs)} → {clockFrom(base, interval.endMs)}
            </td>
            <td className="py-0.5 pl-2.5 text-right font-mono text-11 whitespace-nowrap tabular-nums">
              {formatMilliseconds(intervalMs(interval))}
            </td>
          </tr>
        ))}
    </>
  );
}

/** The trace id of a task: a link under the server's template, or the id as
 * text with no template. */
export function TraceValue({ traceId, template }: { traceId: string; template: string | null }) {
  return template === null ? (
    <>{traceId}</>
  ) : (
    <a
      href={traceLinkOf(template, traceId)}
      target="_blank"
      rel="noopener noreferrer"
      className="underline underline-offset-2 hover:text-foreground"
    >
      {traceId}
    </a>
  );
}

function Fact({ label, children }: { label: string; children: ReactNode }) {
  return (
    <>
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="m-0 font-mono text-11 leading-normal wrap-anywhere">{children}</dd>
    </>
  );
}

/** The date of the times of the task, with its time zone. */
function DateFact({ base }: { base: PanelBase }) {
  switch (base.kind) {
    case 'at':
      return (
        <Fact label="date">
          {dateText(base.ms)} · {zoneText(base.ms)}
        </Fact>
      );
    case 'no_times':
      return null;
    default:
      return assertNever(base, 'panel base');
  }
}

/**
 * The details section, closed at first. `base`: the first time of the task,
 * whose date the section names; the times of another day carry their day
 * offset.
 */
export function AttemptDetails({
  model,
  leaf,
  base,
  traceTemplate,
}: {
  model: PanelModel;
  leaf: TaskRow;
  base: PanelBase;
  traceTemplate: string | null;
}) {
  const [open, setOpen] = useState(false);
  const bodyId = useId();
  const hosts = [...new Set(model.attempts.flatMap(attempt => (attempt.host === null ? [] : [attempt.host])))];
  return (
    <section aria-label="Details" className="border-t border-hairline pt-3">
      <button
        type="button"
        aria-expanded={open}
        aria-controls={bodyId}
        onClick={() => setOpen(current => !current)}
        className="inline-flex items-center gap-1.5 py-0.75 text-xs font-medium text-muted-foreground hover:text-foreground"
      >
        <ChevronRight className={cn('size-3.5 transition-transform motion-reduce:transition-none', open && 'rotate-90')} aria-hidden />
        details
      </button>
      <div id={bodyId} hidden={!open} className="mt-2 flex flex-col gap-2.5">
        {model.attempts.length > 0 && <TimesTable model={model} base={base} />}
        <dl className="m-0 grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-0.5 text-11">
          <Fact label="worker">{hosts.length === 0 ? '—' : hosts.join(', ')}</Fact>
          <Fact label="task">{leaf.task_id}</Fact>
          {leaf.trace_id !== null && (
            <Fact label="trace">
              <TraceValue traceId={leaf.trace_id} template={traceTemplate} />
            </Fact>
          )}
          {leaf.good_until !== null && (
            <Fact label="good until">
              {formatInstant(instantMs(leaf.good_until))} {zoneText(instantMs(leaf.good_until))}
            </Fact>
          )}
          <DateFact base={base} />
        </dl>
      </div>
    </section>
  );
}

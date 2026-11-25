// The errors of the attempt panel: identical errors once, with the attempts
// that had them. A long message shows 3 lines until the reader opens it.

import { useLayoutEffect, useRef, useState } from 'react';

import { assertNever } from '@/lib/exhaustive';
import { cn } from '@/lib/utils';

import { rangeText, type ErrorCodeOf, type ErrorGroup, type ErrorMessage } from './attempt-panel-model';

/** The start of the message of an exception that the task code did not
 * handle. The panel shows it muted: the cause follows it. */
export const unhandledPrefix = (taskName: string): string => `Unhandled exception in task ${taskName}: `;

/** The error code of a row, as a pill. */
export function CodePill({ code }: { code: string }) {
  return (
    <span className="inline-flex h-[22px] items-center rounded-full bg-glass-inset px-2 font-mono text-11 font-medium whitespace-nowrap">
      {code}
    </span>
  );
}

/** A message in its box, clamped to 3 lines with a `more` button when it is
 * longer. */
function MessageBox({ text, taskName }: { text: string; taskName: string }) {
  const messageRef = useRef<HTMLPreElement>(null);
  const [open, setOpen] = useState(false);
  const [overflows, setOverflows] = useState(false);
  useLayoutEffect(() => {
    const element = messageRef.current;
    if (element === null || open) {
      return undefined;
    }
    const measure = (): void => setOverflows(element.scrollHeight > element.clientHeight + 1);
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => observer.disconnect();
  }, [text, open]);
  const prefix = unhandledPrefix(taskName);
  return (
    <div className="rounded-lg bg-glass-inset px-2.5 py-2">
      <pre
        ref={messageRef}
        data-error-message
        className={cn('m-0 font-mono text-11 leading-normal whitespace-pre-wrap wrap-anywhere', !open && 'line-clamp-3')}
      >
        {text.startsWith(prefix) ? (
          <>
            <span className="text-muted-foreground">{prefix}</span>
            {text.slice(prefix.length)}
          </>
        ) : (
          text
        )}
      </pre>
      {(overflows || open) && (
        <button
          type="button"
          aria-expanded={open}
          onClick={() => setOpen(current => !current)}
          className="mt-1.25 text-11 font-medium text-muted-foreground underline decoration-dotted underline-offset-2 hover:text-foreground"
        >
          {open ? 'less' : 'more'}
        </button>
      )}
    </div>
  );
}

function MessageOf({ message, taskName }: { message: ErrorMessage; taskName: string }) {
  switch (message.kind) {
    case 'text':
      return <MessageBox text={message.text} taskName={taskName} />;
    case 'no_message':
      return null;
    default:
      return assertNever(message, 'error message');
  }
}

/** Whether the code of the one group is the code in the title of the panel:
 * then the rows do not repeat it. */
function codeInTitle(groups: readonly ErrorGroup[], titleCode: string | null): boolean {
  const [only] = groups;
  return groups.length === 1 && only !== undefined && only.code.kind === 'code' && only.code.code === titleCode;
}

/** The key of a group in its list. */
function codeKey(code: ErrorCodeOf): string {
  switch (code.kind) {
    case 'code':
      return `code:${code.code}`;
    case 'no_code':
      return 'no_code';
    default:
      return assertNever(code, 'error code');
  }
}

function RowCode({ code }: { code: ErrorCodeOf }) {
  switch (code.kind) {
    case 'code':
      return <CodePill code={code.code} />;
    case 'no_code':
      return null;
    default:
      return assertNever(code, 'error code');
  }
}

/**
 * The errors section: each group of one code, each message once with its
 * attempt range (`attempt 1–4`), and the failure of the task that no attempt
 * shows. Nothing when the task has no error.
 */
export function AttemptErrors({
  groups,
  taskName,
  titleCode,
  taskFailure,
}: {
  groups: readonly ErrorGroup[];
  taskName: string;
  titleCode: string | null;
  taskFailure: string | null;
}) {
  if (groups.length === 0 && taskFailure === null) {
    return null;
  }
  const hideCode = codeInTitle(groups, titleCode);
  return (
    <section aria-label="Errors" className="flex flex-col gap-2.5 border-t border-hairline pt-3">
      {groups.map(group => (
        <div key={codeKey(group.code)} data-error-group className="flex flex-col gap-1.5">
          {group.rows.map((row, index) => (
            <div key={index} className="flex flex-col gap-1">
              <div className="flex flex-wrap items-center gap-x-2 gap-y-1.5 text-11 text-muted-foreground">
                {index === 0 && !hideCode && <RowCode code={group.code} />}
                <span data-error-attempts>attempt {rangeText(row.attempts)}</span>
              </div>
              <MessageOf message={row.message} taskName={taskName} />
            </div>
          ))}
        </div>
      ))}
      {taskFailure !== null && (
        <div className="flex flex-col gap-1">
          <span className="text-11 text-muted-foreground">task</span>
          <MessageBox text={taskFailure} taskName={taskName} />
        </div>
      )}
    </section>
  );
}

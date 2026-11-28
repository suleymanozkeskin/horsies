// The reports of spans that were not made: the first report of each kind is written, with the type and the message of
// the cause; the addon writer writes the record and gives one stderr line when the core log does not start; a seat
// isolate sends its report to the main thread of its child.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { NativeEnvelope } from "../../../../horsies/src/native/call/envelope.ts";
import {
  addonNotMadeWriter,
  channelNotMadeWriter,
  lineText,
  type NotMadeSpan,
  SpansNotMade,
} from "../../../../horsies/src/telemetry/not-made.ts";
import { fakeChannel } from "../../seats/support.ts";

const WORKFLOW_ID = "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b";
const TASK_ID = "0192f6a1-7b3c-7d4e-8f00-0123456789ab";
const START: NotMadeSpan = { kind: "workflowStart", workflowId: WORKFLOW_ID };
const ATTEMPT: NotMadeSpan = { kind: "seatAttempt", taskId: TASK_ID };

describe("the reports of spans that were not made", () => {
  test("the first report of each kind is written, with the type and the message of the cause; later ones are not", () => {
    const written: [NotMadeSpan, string][] = [];
    const reports = new SpansNotMade((span, cause) => written.push([span, cause]));

    reports.report(START, new TypeError("the SDK is broken"));
    reports.report(START, new Error("a second one"));
    reports.reportText(ATTEMPT, "RangeError: from a seat isolate");
    reports.report(ATTEMPT, new Error("a second one"));
    assert.deepEqual(written, [
      [START, "TypeError: the SDK is broken"],
      [ATTEMPT, "RangeError: from a seat isolate"],
    ]);
  });
});

describe("the writer of a main thread", () => {
  test("the addon writes the record; nothing goes to stderr", () => {
    const calls: [NotMadeSpan, string][] = [];
    const lines: string[] = [];

    const write = addonNotMadeWriter((span, cause): NativeEnvelope<undefined> => {
      calls.push([span, cause]);

      return { kind: "value", value: undefined };
    }, (line) => lines.push(line));

    write(START, "Error: broken");
    assert.deepEqual(calls, [[START, "Error: broken"]]);
    assert.deepEqual(lines, []);
  });

  test("a core log that does not start gives one stderr line with the span, the cause and why", () => {
    const lines: string[] = [];

    const write = addonNotMadeWriter(
      (): NativeEnvelope<undefined> => ({ kind: "thrown", thrown: { code: "RUNTIME_START_FAILED", cause: "the OS refused a thread" } }),
      (line) => lines.push(line),
    );

    write(ATTEMPT, "Error: broken");
    assert.deepEqual(lines, [`${lineText(ATTEMPT, "Error: broken")} The core log did not start: the OS refused a thread.`]);
  });

  test("the line of each kind names its span and what goes on", () => {
    const later = "Later spans of this kind that are not made are not logged.";

    const lines: readonly [NotMadeSpan, string][] = [
      [{ kind: "send", taskId: TASK_ID }, `the span of the send of task ${TASK_ID} was not made or is not complete: Error: broken. The send goes on.`],
      [START, `the span of the start of workflow ${WORKFLOW_ID} was not made or is not complete: Error: broken. The start goes on.`],
      [{ kind: "attempt", taskId: TASK_ID }, `the span of the attempt of task ${TASK_ID} was not made or is not complete: Error: broken. The run goes on.`],
      [ATTEMPT, `the span of the attempt of task ${TASK_ID} in a seat isolate was not made or is not complete: Error: broken. The run goes on.`],
      [
        { kind: "workerEnded", taskId: TASK_ID },
        `the span of the attempt of task ${TASK_ID} that the worker ended was not made or is not complete: Error: broken. The event still goes to the handlers.`,
      ],
    ];

    for (const [span, text] of lines) {
      assert.equal(lineText(span, "Error: broken"), `horsies.telemetry: ${text} ${later}`);
    }
  });
});

describe("the writer of a seat isolate", () => {
  test("the report goes to the main thread of the child as spanNotMade", () => {
    const { channel, port } = fakeChannel();

    channelNotMadeWriter(channel)(ATTEMPT, "TypeError: no exporter");
    assert.deepEqual(port.postsOf("spanNotMade"), [{ kind: "spanNotMade", protocol: 1, span: ATTEMPT, cause: "TypeError: no exporter" }]);
  });
});

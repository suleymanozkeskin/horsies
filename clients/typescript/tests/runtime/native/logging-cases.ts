// The facts that `logging.test.ts` and its child script `logging-child.ts` share. The test does not import the child
// script: its top level runs a case.

/** The message of the record that the child exit case writes just before the exit rule. */
export const EXIT_RECORD_MESSAGE = "the record just before the exit of the child";

/** The exit code of the child exit case: the code of a child whose start hook failed. */
export const EXIT_RECORD_CODE = 173;

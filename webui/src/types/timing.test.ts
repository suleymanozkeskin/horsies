import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

import { describe, expect, it } from 'vitest';

import { TIMING_EXAMPLES } from '@/test/timing-examples';

/**
 * The core pins core/src/monitoring/timing_examples.json to the JSON of its
 * own types (timing_tests.rs), and the Python client validates the same file
 * into its models. Here the file must equal the typed examples, which the
 * type checker holds against types/timing.ts: a field, a variant or a tag that
 * the core renames fails this test or the type check.
 */
// The test environment gives import.meta.url an http URL, so the path is
// built from the working folder of the test run (webui/).
const SHARED_EXAMPLES_PATH = resolve(
  process.cwd(),
  '../core/src/monitoring/timing_examples.json'
);

describe('the timing types of the web UI', () => {
  it('match the shared JSON examples of the core, value for value', () => {
    const shared: unknown = JSON.parse(readFileSync(SHARED_EXAMPLES_PATH, 'utf8'));

    expect(shared).toEqual(TIMING_EXAMPLES);
  });

  it('cover every variant of each union that the examples hold', () => {
    const nodeKinds = new Set(TIMING_EXAMPLES.node_timings.map(timing => timing.kind));
    const runKinds = new Set(TIMING_EXAMPLES.run_timings.map(timing => timing.kind));
    const attemptKinds = new Set(TIMING_EXAMPLES.attempt_phases.map(phases => phases.kind));

    expect([...nodeKinds].sort()).toEqual(['exact', 'inconsistent', 'partial', 'unavailable']);
    expect([...runKinds].sort()).toEqual(['composed', 'unavailable']);
    expect([...attemptKinds].sort()).toEqual(['captured', 'not_captured']);
  });
});

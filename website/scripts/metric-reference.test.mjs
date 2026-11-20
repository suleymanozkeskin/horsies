import { describe, expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import {
	PAGE_PATH,
	SNAPSHOT_PATH,
	labelValues,
	parseSnapshot,
	readSnapshot,
	renderPage,
	seriesBound,
} from './metric-reference.mjs';

const TWO_METRICS = `horsies.task.attempts
  prometheus: horsies_task_attempts_total
  kind: counter
  unit: 1
  recorder: worker
  help: Attempts.
  label task (horsies.task.name): declared task names, fallback "(unregistered)"
  label outcome (horsies.task.outcome): "COMPLETED" "FAILED" "WORKER_FAILURE"
  label error_code (error.type): "" "TASK_TIMEOUT" then declared user error codes, fallback "undeclared"
horsies.task.run.duration
  prometheus: horsies_task_run_duration_seconds
  kind: histogram
  unit: s
  recorder: worker
  help: Run time.
  buckets: 0.005 1
  label version (horsies.version): the version of the process
`;

/**
 * The metrics of `text`; fails the test when it does not parse.
 *
 * @param {string} text
 */
function parsed(text) {
	const outcome = parseSnapshot(text);
	if (outcome.kind !== 'parsed') throw new Error(`refused at line ${outcome.failure.line}: ${outcome.failure.cause}`);
	return outcome.metrics;
}

/**
 * The failure of `text`; fails the test when it parses.
 *
 * @param {string} text
 */
function refused(text) {
	const outcome = parseSnapshot(text);
	if (outcome.kind !== 'refused') throw new Error('the text parsed');
	return outcome.failure;
}

describe('parseSnapshot', () => {
	test('a block gives its names, kind, unit, recorder, buckets and labels in order', () => {
		const [attempts, duration] = parsed(TWO_METRICS);
		expect(attempts.name).toBe('horsies.task.attempts');
		expect(attempts.prometheus).toBe('horsies_task_attempts_total');
		expect(attempts.kind).toBe('counter');
		expect(attempts.unit).toBe('1');
		expect(attempts.recorder).toBe('worker');
		expect(attempts.labels.map((label) => label.name)).toEqual(['task', 'outcome', 'error_code']);
		expect(attempts.labels[1].attribute).toBe('horsies.task.outcome');
		expect(duration.buckets).toEqual(['0.005', '1']);
	});

	test('each form of label values is read', () => {
		expect(labelValues('"COMPLETED" "FAILED"')).toEqual({ kind: 'closed', values: ['COMPLETED', 'FAILED'] });
		expect(labelValues('declared task names, fallback "(unregistered)"')).toEqual({
			kind: 'declared',
			fixed: [],
			names: 'task names',
			fallback: '(unregistered)',
		});
		expect(labelValues('"" "TASK_TIMEOUT" then declared user error codes, fallback "undeclared"')).toEqual({
			kind: 'declared',
			fixed: ['', 'TASK_TIMEOUT'],
			names: 'user error codes',
			fallback: 'undeclared',
		});
		expect(labelValues('the version of the process')).toEqual({ kind: 'process-fact', fact: 'version' });
	});

	test('a text of no known form is refused', () => {
		expect(labelValues('COMPLETED FAILED')).toBeUndefined();
		expect(labelValues('"COMPLETED"  "FAILED"')).toBeUndefined();
		expect(refused(TWO_METRICS.replace('"COMPLETED" "FAILED" "WORKER_FAILURE"', 'COMPLETED')).line).toBe(8);
	});

	test('a line that is not part of the format is refused with its number', () => {
		expect(refused(`${TWO_METRICS}\n`).line).toBe(18);
		expect(refused(`  kind: counter\n${TWO_METRICS}`).line).toBe(1);
		expect(refused(TWO_METRICS.replace('  unit: 1\n', '  units: 1\n')).cause).toContain('not a field or a label line');
		expect(refused(TWO_METRICS.replace('  unit: 1\n', '  unit: 1\n  unit: 1\n')).cause).toContain('unit given twice');
	});

	test('a block without a required field, or with a field of another kind, is refused', () => {
		expect(refused(TWO_METRICS.replace('  help: Attempts.\n', '')).cause).toContain('no help');
		expect(refused(TWO_METRICS.replace('kind: counter', 'kind: summary')).cause).toContain('kind "summary"');
		expect(refused(TWO_METRICS.replace('unit: s', 'unit: ms')).cause).toContain('unit "ms"');
		expect(refused(TWO_METRICS.replace('recorder: worker', 'recorder: scheduler')).cause).toContain('recorder "scheduler"');
		expect(refused(TWO_METRICS.replace('  buckets: 0.005 1\n', '')).cause).toContain('a histogram with no buckets');
		expect(refused(TWO_METRICS.replace('  help: Attempts.\n', '  help: Attempts.\n  buckets: 1\n')).cause).toContain('a counter with buckets');
	});

	test('a name given twice is refused', () => {
		const twice = TWO_METRICS.replace('horsies_task_run_duration_seconds', 'horsies_task_attempts_total');
		expect(refused(twice).cause).toContain('prometheus horsies_task_attempts_total given twice');
	});
});

describe('seriesBound', () => {
	test('closed sets multiply, a process fact is one value, a declared label has no bound', () => {
		const [attempts, duration] = parsed(TWO_METRICS);
		expect(seriesBound(attempts)).toBeUndefined();
		expect(seriesBound(duration)).toBe(1);
		const closed = parsed(TWO_METRICS.replace('  label task (horsies.task.name): declared task names, fallback "(unregistered)"\n', '').replace(
			'  label error_code (error.type): "" "TASK_TIMEOUT" then declared user error codes, fallback "undeclared"\n',
			'  label kind (horsies.child.kind): "ISOLATED" "SHARED_ASYNC"\n',
		));
		expect(seriesBound(closed[0])).toBe(6);
	});
});

describe('renderPage', () => {
	test('the page names each metric, its Prometheus name, its labels and their values', () => {
		const page = renderPage(parsed(TWO_METRICS), 'core/src/telemetry/catalog_snapshot.txt');
		expect(page).toContain('### horsies.task.attempts');
		expect(page).toContain('| Prometheus name | `horsies_task_attempts_total` |');
		expect(page).toContain('| `outcome` / `horsies.task.outcome` | `COMPLETED`, `FAILED`, `WORKER_FAILURE` |');
		expect(page).toContain('`""`, `TASK_TIMEOUT`, then the declared user error codes; any other value is `undeclared`');
		expect(page).toContain('| Lines per series | 5: 2 buckets, `+Inf`, `_sum`, `_count` |');
		expect(page.endsWith('\n')).toBe(true);
	});

	test('the same metrics give the same text', () => {
		const metrics = parsed(TWO_METRICS);
		expect(renderPage(metrics, 'snapshot')).toBe(renderPage(metrics, 'snapshot'));
	});
});

describe('the committed page', () => {
	test('is the page of the committed catalog snapshot', () => {
		const snapshot = readSnapshot(SNAPSHOT_PATH);
		if (snapshot.kind !== 'read') throw new Error(snapshot.cause);
		const page = renderPage(parsed(snapshot.text), 'core/src/telemetry/catalog_snapshot.txt');
		expect(readFileSync(PAGE_PATH, 'utf-8')).toBe(page);
	});
});

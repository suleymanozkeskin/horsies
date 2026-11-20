#!/usr/bin/env bun
/**
 * The metric reference page of the docs, generated from the metric catalog
 * of the core.
 *
 * Source: `core/src/telemetry/catalog_snapshot.txt`, the text of the catalog.
 * The core test `the_catalog_is_its_snapshot` fails when the catalog and the
 * snapshot differ, so the page follows the catalog through the snapshot.
 * One block per metric:
 *
 *     horsies.task.attempts
 *       prometheus: horsies_task_attempts_total
 *       kind: counter
 *       unit: 1
 *       recorder: worker
 *       help: ...
 *       buckets: 0.005 0.01 ...                      (histograms only)
 *       label task (horsies.task.name): <values>     (zero or more)
 *
 * A label's values are a quoted list (a closed set), `declared <names>,
 * fallback "<text>"`, a quoted list then ` then declared <names>, fallback
 * "<text>"`, or `the <fact> of the process`.
 *
 * Output: `src/content/docs/observability/metric-reference.md`.
 *
 * Usage:
 *   bun scripts/metric-reference.mjs          # write the page
 *   bun scripts/metric-reference.mjs --check  # compare the page; write nothing
 *
 * Exit codes:
 *   0  the page was written, or `--check` found it equal to the generated text
 *   1  `--check` found the page different from the generated text, or missing
 *   2  the snapshot cannot be read, is larger than MAX_SNAPSHOT_BYTES, or does
 *      not parse; the page cannot be written; or an argument is not known
 */

import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

/** The largest snapshot that the generator reads. The snapshot of 41 metrics has about 20 kB. */
export const MAX_SNAPSHOT_BYTES = 1_000_000;

const WEBSITE_DIRECTORY = join(dirname(fileURLToPath(import.meta.url)), '..');

/** The committed catalog text of the core. */
export const SNAPSHOT_PATH = join(WEBSITE_DIRECTORY, '..', 'core', 'src', 'telemetry', 'catalog_snapshot.txt');

/** The page that the generator writes. */
export const PAGE_PATH = join(WEBSITE_DIRECTORY, 'src', 'content', 'docs', 'observability', 'metric-reference.md');

/** The kinds of a metric, as the snapshot names them. */
const KINDS = Object.freeze(['counter', 'gauge', 'histogram']);

/** The units of a metric, as the snapshot names them (UCUM). */
const UNITS = Object.freeze(['1', 's', 'By']);

/** The recorders of a metric, in page order. */
const RECORDERS = Object.freeze(['worker', 'producer', 'fleet collector']);

const FIELD_LINE = /^ {2}(prometheus|kind|unit|recorder|help|buckets): (.*)$/;
const LABEL_LINE = /^ {2}label (\S+) \((\S+)\): (.*)$/;
const DECLARED_VALUES = /^declared (.+), fallback "((?:[^"\\]|\\.)*)"$/;
const PROCESS_FACT = /^the (.+) of the process$/;
const QUOTED_VALUE = /"((?:[^"\\]|\\.)*)"/g;
const DECLARED_TAIL = ' then declared ';

/**
 * @typedef {'counter' | 'gauge' | 'histogram'} MetricKind
 * @typedef {'1' | 's' | 'By'} MetricUnit
 * @typedef {'worker' | 'producer' | 'fleet collector'} Recorder
 * @typedef {(
 *   | { kind: 'closed', values: readonly string[] }
 *   | { kind: 'declared', fixed: readonly string[], names: string, fallback: string }
 *   | { kind: 'process-fact', fact: string }
 * )} LabelValues
 * @typedef {{ name: string, attribute: string, values: LabelValues }} Label
 * @typedef {{
 *   name: string,
 *   prometheus: string,
 *   kind: MetricKind,
 *   unit: MetricUnit,
 *   recorder: Recorder,
 *   help: string,
 *   buckets: readonly string[],
 *   labels: readonly Label[],
 * }} Metric
 * @typedef {{ line: number, cause: string }} ParseFailure
 * @typedef {{ kind: 'parsed', metrics: readonly Metric[] } | { kind: 'refused', failure: ParseFailure }} ParseOutcome
 */

/**
 * The quoted values of `text`, a list of quoted texts separated by one space.
 * `undefined` when `text` is not such a list. Pure.
 *
 * @param {string} text
 * @returns {string[] | undefined}
 */
function quotedValues(text) {
	const values = [...text.matchAll(QUOTED_VALUE)].map((match) => match[1].replace(/\\(.)/g, '$1'));
	const rebuilt = [...text.matchAll(QUOTED_VALUE)].map((match) => match[0]).join(' ');
	return rebuilt === text ? values : undefined;
}

/**
 * The values of a label line, after its `:`. Pure.
 *
 * @param {string} text
 * @returns {LabelValues | undefined} `undefined` for a text of no known form
 */
export function labelValues(text) {
	const fact = PROCESS_FACT.exec(text);
	if (fact !== null) return { kind: 'process-fact', fact: fact[1] };
	const tail = text.indexOf(DECLARED_TAIL);
	const fixedText = tail === -1 ? undefined : text.slice(0, tail);
	const declaredText = tail === -1 ? text : `declared ${text.slice(tail + DECLARED_TAIL.length)}`;
	const declared = DECLARED_VALUES.exec(declaredText);
	if (declared !== null) {
		const fixed = fixedText === undefined ? [] : quotedValues(fixedText);
		if (fixed === undefined) return undefined;
		return { kind: 'declared', fixed, names: declared[1], fallback: declared[2] };
	}
	const closed = quotedValues(text);
	if (closed === undefined || closed.length === 0) return undefined;
	return { kind: 'closed', values: closed };
}

/**
 * The metric of one block whose lines are all read. Pure.
 *
 * @param {{ name: string, line: number, fields: Map<string, string>, labels: Label[] }} block
 * @returns {{ kind: 'metric', metric: Metric } | { kind: 'refused', failure: ParseFailure }}
 */
function blockMetric(block) {
	const refuse = (/** @type {string} */ cause) => ({ kind: /** @type {const} */ ('refused'), failure: { line: block.line, cause: `${block.name}: ${cause}` } });
	for (const field of ['prometheus', 'kind', 'unit', 'recorder', 'help']) {
		if (!block.fields.has(field)) return refuse(`no ${field}`);
	}
	const kind = /** @type {string} */ (block.fields.get('kind'));
	const unit = /** @type {string} */ (block.fields.get('unit'));
	const recorder = /** @type {string} */ (block.fields.get('recorder'));
	if (!KINDS.includes(kind)) return refuse(`kind ${JSON.stringify(kind)} is not one of ${KINDS.join(', ')}`);
	if (!UNITS.includes(unit)) return refuse(`unit ${JSON.stringify(unit)} is not one of ${UNITS.join(', ')}`);
	if (!RECORDERS.includes(recorder)) return refuse(`recorder ${JSON.stringify(recorder)} is not one of ${RECORDERS.join(', ')}`);
	const bucketText = block.fields.get('buckets');
	const isHistogram = kind === 'histogram';
	if (isHistogram !== (bucketText !== undefined)) {
		return refuse(isHistogram ? 'a histogram with no buckets' : `a ${kind} with buckets`);
	}
	const buckets = bucketText === undefined ? [] : bucketText.split(' ');
	if (buckets.some((bound) => !/^[0-9]+(\.[0-9]+)?$/.test(bound))) return refuse(`a bucket bound that is not a number: ${bucketText}`);
	return {
		kind: 'metric',
		metric: {
			name: block.name,
			prometheus: /** @type {string} */ (block.fields.get('prometheus')),
			kind: /** @type {MetricKind} */ (kind),
			unit: /** @type {MetricUnit} */ (unit),
			recorder: /** @type {Recorder} */ (recorder),
			help: /** @type {string} */ (block.fields.get('help')),
			buckets,
			labels: block.labels,
		},
	};
}

/**
 * The metrics of a snapshot text, in snapshot order. Pure.
 *
 * Refuses an empty line, a field before the first metric name, a line that is
 * not a field or a label, a field given twice, a block with no required
 * field, a value of no known form, and a name or a Prometheus name given
 * twice. The failure names the line (1-based) and the cause.
 *
 * @param {string} text
 * @returns {ParseOutcome}
 */
export function parseSnapshot(text) {
	/** @type {{ name: string, line: number, fields: Map<string, string>, labels: Label[] }[]} */
	const blocks = [];
	const lines = text.endsWith('\n') ? text.slice(0, -1).split('\n') : text.split('\n');
	for (const [index, line] of lines.entries()) {
		const number = index + 1;
		const refuse = (/** @type {string} */ cause) => ({ kind: /** @type {const} */ ('refused'), failure: { line: number, cause } });
		if (line === '') return refuse('an empty line');
		if (!line.startsWith(' ')) {
			if (/\s/.test(line)) return refuse(`a metric name with a space: ${JSON.stringify(line)}`);
			blocks.push({ name: line, line: number, fields: new Map(), labels: [] });
			continue;
		}
		const block = blocks.at(-1);
		if (block === undefined) return refuse('a field before the first metric name');
		const label = LABEL_LINE.exec(line);
		if (label !== null) {
			const values = labelValues(label[3]);
			if (values === undefined) return refuse(`label values of no known form: ${JSON.stringify(label[3])}`);
			block.labels.push({ name: label[1], attribute: label[2], values });
			continue;
		}
		const field = FIELD_LINE.exec(line);
		if (field === null) return refuse(`not a field or a label line: ${JSON.stringify(line)}`);
		if (block.fields.has(field[1])) return refuse(`${field[1]} given twice for ${block.name}`);
		block.fields.set(field[1], field[2]);
	}
	/** @type {Metric[]} */
	const metrics = [];
	for (const block of blocks) {
		const built = blockMetric(block);
		if (built.kind === 'refused') return built;
		metrics.push(built.metric);
	}
	for (const key of /** @type {const} */ (['name', 'prometheus'])) {
		const seen = new Set();
		for (const metric of metrics) {
			if (seen.has(metric[key])) return { kind: 'refused', failure: { line: 0, cause: `${key} ${metric[key]} given twice` } };
			seen.add(metric[key]);
		}
	}
	if (metrics.length === 0) return { kind: 'refused', failure: { line: 0, cause: 'no metric' } };
	return { kind: 'parsed', metrics };
}

/**
 * A text as a code span. A text with a backtick is not written by the
 * catalog; it is written with a longer fence. Pure.
 *
 * @param {string} text
 * @returns {string}
 */
function code(text) {
	if (text === '') return '`""`';
	return text.includes('`') ? `\`\` ${text} \`\`` : `\`${text}\``;
}

/**
 * A text for a table cell: the `|` of the text escaped. Pure.
 *
 * @param {string} text
 * @returns {string}
 */
function cell(text) {
	return text.replaceAll('|', '\\|');
}

/**
 * The values of a label, as the text of a table cell. Pure.
 *
 * @param {LabelValues} values
 * @returns {string}
 */
function valuesText(values) {
	switch (values.kind) {
		case 'closed':
			return values.values.map(code).join(', ');
		case 'declared': {
			const fixed = values.fixed.length === 0 ? '' : `${values.fixed.map(code).join(', ')}, then `;
			return `${fixed}the declared ${values.names}; any other value is ${code(values.fallback)}`;
		}
		case 'process-fact':
			return `The ${values.fact} of the process: one value`;
	}
}

/**
 * The most series of a metric whose labels are all closed sets or process
 * facts: the product of the sizes of its label sets. `undefined` when a label
 * takes declared names, whose count the app decides. Pure.
 *
 * @param {Metric} metric
 * @returns {number | undefined}
 */
export function seriesBound(metric) {
	let bound = 1;
	for (const label of metric.labels) {
		switch (label.values.kind) {
			case 'closed':
				bound *= label.values.values.length;
				break;
			case 'process-fact':
				break;
			case 'declared':
				return undefined;
		}
	}
	return bound;
}

/**
 * The count of lines of the Prometheus text of one series of a histogram
 * with `buckets`: one per bound, `+Inf`, `_sum` and `_count`. Pure.
 *
 * @param {readonly string[]} buckets
 * @returns {number}
 */
function histogramLines(buckets) {
	return buckets.length + 3;
}

/**
 * The unit of a metric, for a table cell. Pure.
 *
 * @param {MetricUnit} unit
 * @returns {string}
 */
function unitText(unit) {
	switch (unit) {
		case '1':
			return '`1` (a count)';
		case 's':
			return '`s` (seconds)';
		case 'By':
			return '`By` (bytes)';
	}
}

/**
 * The process that records the metrics of `recorder`, for the page. Pure.
 *
 * @param {Recorder} recorder
 * @returns {string}
 */
function recorderText(recorder) {
	switch (recorder) {
		case 'worker':
			return 'The core worker of each worker process records them. Its metrics endpoint (`--metrics-listen HOST:PORT`) serves them.';
		case 'producer':
			return 'The client, in each process that sends tasks, with the OpenTelemetry meter of the process and the task name and queue name of each send. No endpoint of horsies serves them.';
		case 'fleet collector':
			return 'The fleet collector: `horsies metrics`, and `GET /metrics` of the web UI. It reads them from PostgreSQL.';
	}
}

/**
 * The heading of the section of `recorder`. Pure.
 *
 * @param {Recorder} recorder
 * @returns {string}
 */
function recorderHeading(recorder) {
	switch (recorder) {
		case 'worker':
			return 'Worker Metrics';
		case 'producer':
			return 'Producer Metrics';
		case 'fleet collector':
			return 'Fleet Metrics';
	}
}

/**
 * The anchor of the section of `metric` on the page: the id that Starlight
 * gives the heading `metric.name`. Pure.
 *
 * @param {Metric} metric
 * @returns {string}
 */
function metricAnchor(metric) {
	return metric.name.toLowerCase().replaceAll('.', '');
}

/**
 * The section of one metric. Pure.
 *
 * @param {Metric} metric
 * @returns {string[]}
 */
function metricSection(metric) {
	const lines = [`### ${metric.name}`, '', cell(metric.help), '', '| Item | Value |', '| ---- | ----- |'];
	lines.push(`| Prometheus name | ${code(metric.prometheus)} |`);
	lines.push(`| Kind | ${metric.kind} |`);
	lines.push(`| Unit | ${unitText(metric.unit)} |`);
	const bound = seriesBound(metric);
	if (bound !== undefined) lines.push(`| Series | At most ${bound.toLocaleString('en-US')} |`);
	if (metric.kind === 'histogram') {
		lines.push(`| Lines per series | ${histogramLines(metric.buckets)}: ${metric.buckets.length} buckets, \`+Inf\`, \`_sum\`, \`_count\` |`);
	}
	lines.push('');
	if (metric.labels.length === 0) {
		lines.push('No labels: one series.', '');
		return lines;
	}
	lines.push('| Label and attribute | Values |', '| ------------------- | ------ |');
	for (const label of metric.labels) {
		lines.push(`| ${code(label.name)} / ${code(label.attribute)} | ${cell(valuesText(label.values))} |`);
	}
	lines.push('');
	return lines;
}

/**
 * The bucket bounds that every histogram shares, or `undefined` when two
 * histograms differ. Pure.
 *
 * @param {readonly Metric[]} metrics
 * @returns {readonly string[] | undefined}
 */
function sharedBuckets(metrics) {
	const histograms = metrics.filter((metric) => metric.kind === 'histogram');
	const first = histograms[0];
	if (first === undefined) return undefined;
	const text = first.buckets.join(' ');
	return histograms.every((metric) => metric.buckets.join(' ') === text) ? first.buckets : undefined;
}

/**
 * The page text of `metrics`. Pure: the same metrics give the same text.
 *
 * @param {readonly Metric[]} metrics
 * @param {string} sourcePath the snapshot path, relative to the repository root, for the page
 * @returns {string}
 */
export function renderPage(metrics, sourcePath) {
	const lines = [
		'---',
		'title: Metric Reference',
		'section: shared',
		'summary: Every metric of horsies, generated from the metric catalog of the core - its OpenTelemetry and Prometheus names, kind, unit, help text, labels and label values.',
		'related: [overview, metrics, fleet-collector, ../tasks/errors]',
		'tags: [observability, metrics, prometheus, opentelemetry, reference]',
		'---',
		'',
		`<!-- Generated by website/scripts/metric-reference.mjs from ${sourcePath}. Do not edit this file: change the catalog of the core, then run \`bun run metrics:reference\` in website/. -->`,
		'',
		`This page lists the ${metrics.length} metrics of the metric catalog of the core, in catalog order. The build of the docs generates it from the catalog and fails when the page and the catalog differ.`,
		'',
		'## Names',
		'',
		'- Each metric has an OpenTelemetry name, for example `horsies.task.attempts`, and a Prometheus name, for example `horsies_task_attempts_total`.',
		'- The Prometheus name is the OpenTelemetry name with each `.` replaced by `_`, then the unit suffix (`_seconds` for `s`, `_bytes` for `By`), then `_total` for a counter.',
		'- Each label has a Prometheus label name, for example `task`, and an OpenTelemetry attribute key, for example `horsies.task.name`.',
		'- A label value that is not in the set of its label is written as the fallback value of the label. A label with no fallback value has a closed set.',
		'',
		'## Recorders',
		'',
		'| Recorder | Process | Metrics |',
		'| -------- | ------- | ------- |',
	];
	for (const recorder of RECORDERS) {
		const count = metrics.filter((metric) => metric.recorder === recorder).length;
		lines.push(`| ${recorder} | ${recorderText(recorder)} | ${count} |`);
	}
	lines.push('');
	const buckets = sharedBuckets(metrics);
	if (buckets !== undefined) {
		lines.push(
			'## Duration Buckets',
			'',
			`Every histogram has the same ${buckets.length} bucket upper bounds, in seconds: ${buckets.map(code).join(', ')}. A duration above the last bound counts only in the \`+Inf\` bucket. One series of a histogram is ${histogramLines(buckets)} lines of the Prometheus text.`,
			'',
		);
	}
	const workerBounds = metrics.filter((metric) => metric.recorder === 'worker').map((metric) => ({ metric, bound: seriesBound(metric) }));
	const closedWorker = workerBounds.filter((entry) => entry.bound !== undefined);
	const closedTotal = closedWorker.reduce((sum, entry) => sum + /** @type {number} */ (entry.bound), 0);
	lines.push(
		'## Series Bounds',
		'',
		`A series exists from its first sample. The ${closedWorker.length} worker metrics whose labels are all closed sets hold at most ${closedTotal.toLocaleString('en-US')} series together. The other worker metrics have labels whose values the app declares, so the app decides their count:`,
		'',
	);
	for (const { metric } of workerBounds.filter((entry) => entry.bound === undefined)) {
		const declared = metric.labels.filter((label) => label.values.kind === 'declared').map((label) => code(label.name));
		lines.push(`- [${code(metric.name)}](#${metricAnchor(metric)}): ${declared.join(', ')}`);
	}
	lines.push('');
	for (const recorder of RECORDERS) {
		lines.push(
			`## ${recorderHeading(recorder)}`,
			'',
			recorderText(recorder),
			'',
			'| Metric | Prometheus name | Kind | Labels |',
			'| ------ | --------------- | ---- | ------ |',
		);
		const recorded = metrics.filter((metric) => metric.recorder === recorder);
		for (const metric of recorded) {
			const labels = metric.labels.length === 0 ? 'none' : metric.labels.map((label) => code(label.name)).join(', ');
			lines.push(`| [${code(metric.name)}](#${metricAnchor(metric)}) | ${code(metric.prometheus)} | ${metric.kind} | ${labels} |`);
		}
		lines.push('');
		for (const metric of recorded) lines.push(...metricSection(metric));
	}
	return `${lines.join('\n').replace(/\n+$/, '')}\n`;
}

/**
 * Read the snapshot at `path`, at most MAX_SNAPSHOT_BYTES. Reads the file system.
 *
 * @param {string} path
 * @returns {{ kind: 'read', text: string } | { kind: 'unreadable', cause: string }}
 */
export function readSnapshot(path) {
	/** @type {Buffer} */
	let data;
	try {
		data = readFileSync(path);
	} catch (error) {
		return { kind: 'unreadable', cause: String(error) };
	}
	if (data.length > MAX_SNAPSHOT_BYTES) return { kind: 'unreadable', cause: `${data.length} bytes; the bound is ${MAX_SNAPSHOT_BYTES}` };
	return { kind: 'read', text: data.toString('utf-8') };
}

/**
 * Read the page at `path`. Reads the file system.
 *
 * @param {string} path
 * @returns {{ kind: 'read', text: string } | { kind: 'missing', cause: string }}
 */
function readPage(path) {
	try {
		return { kind: 'read', text: readFileSync(path, 'utf-8') };
	} catch (error) {
		return { kind: 'missing', cause: String(error) };
	}
}

/**
 * Generate the page, then write it or compare it. Sets the process exit code.
 *
 * @returns {void}
 */
function main() {
	const argumentsGiven = process.argv.slice(2);
	const check = argumentsGiven.includes('--check');
	const unknown = argumentsGiven.filter((argument) => argument !== '--check');
	if (unknown.length > 0) {
		console.error(`metric-reference: unknown arguments ${unknown.join(' ')}; the only argument is --check`);
		process.exitCode = 2;
		return;
	}
	const repositoryRoot = join(WEBSITE_DIRECTORY, '..');
	const snapshot = readSnapshot(SNAPSHOT_PATH);
	if (snapshot.kind === 'unreadable') {
		console.error(`metric-reference: ${SNAPSHOT_PATH} cannot be read: ${snapshot.cause}`);
		process.exitCode = 2;
		return;
	}
	const parsed = parseSnapshot(snapshot.text);
	if (parsed.kind === 'refused') {
		console.error(`metric-reference: ${SNAPSHOT_PATH}:${parsed.failure.line}: ${parsed.failure.cause}`);
		process.exitCode = 2;
		return;
	}
	const page = renderPage(parsed.metrics, relative(repositoryRoot, SNAPSHOT_PATH));
	if (!check) {
		try {
			mkdirSync(dirname(PAGE_PATH), { recursive: true });
			writeFileSync(PAGE_PATH, page);
		} catch (error) {
			console.error(`metric-reference: ${PAGE_PATH} cannot be written: ${String(error)}`);
			process.exitCode = 2;
			return;
		}
		console.log(`metric-reference: wrote ${relative(repositoryRoot, PAGE_PATH)} (${parsed.metrics.length} metrics)`);
		return;
	}
	const committed = readPage(PAGE_PATH);
	if (committed.kind === 'missing' || committed.text !== page) {
		console.error(
			`metric-reference: ${relative(repositoryRoot, PAGE_PATH)} is not the page of the catalog snapshot. ` +
				'Run `bun run metrics:reference` in website/ and commit the page.',
		);
		process.exitCode = 1;
		return;
	}
	console.log(`metric-reference: the page matches the catalog snapshot (${parsed.metrics.length} metrics)`);
}

if (import.meta.main) main();

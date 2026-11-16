import { afterEach, describe, expect, test } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { chmodSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { checkSite, decodeAttribute, describeBrokenLink, describeRunFailure } from './check-links.mjs';

const ROOT = new URL('https://example.test/docs/');
const LIMITS = { maxDirectoryDepth: 16, maxFiles: 100 };
const SCRIPT = join(dirname(fileURLToPath(import.meta.url)), 'check-links.mjs');

/** @type {string[]} */
const directories = [];

afterEach(() => {
	for (const directory of directories.splice(0)) rmSync(directory, { recursive: true, force: true });
});

/**
 * Write a build output to a new temporary directory.
 *
 * @param {Record<string, string>} files path relative to the output -> content
 * @returns {string}
 */
function writeSite(files) {
	const directory = mkdtempSync(join(tmpdir(), 'check-links-'));
	directories.push(directory);
	for (const [path, content] of Object.entries(files)) {
		mkdirSync(dirname(join(directory, path)), { recursive: true });
		writeFileSync(join(directory, path), content);
	}
	return directory;
}

/**
 * Check a site and return its broken links; fail the test if it cannot run.
 *
 * @param {Record<string, string>} files
 */
function brokenLinks(files) {
	const outcome = checkSite(writeSite(files), ROOT, LIMITS);
	if (outcome.kind !== 'checked') throw new Error(`check did not run: ${JSON.stringify(outcome)}`);
	return outcome.broken;
}

const GUIDE = '<h1 id="_top">Guide</h1><h2 id="setup">Setup</h2>';

describe('decodeAttribute', () => {
	test('decodes numeric and the five named references', () => {
		expect(decodeAttribute('a&amp;b&#39;c&#x26;d&quot;')).toEqual({ kind: 'decoded', value: "a&b'c&d\"" });
	});

	test('keeps an & that starts no reference', () => {
		expect(decodeAttribute('?a=1&b=2')).toEqual({ kind: 'decoded', value: '?a=1&b=2' });
	});

	test('reports a named reference outside the five', () => {
		expect(decodeAttribute('a&nbsp;b')).toEqual({ kind: 'undecodable', reference: '&nbsp;' });
	});

	test('reports a numeric reference outside Unicode', () => {
		expect(decodeAttribute('&#x110000;')).toEqual({ kind: 'undecodable', reference: '&#x110000;' });
	});
});

describe('checkSite resolves', () => {
	test('every form of an internal link that GitHub Pages serves', () => {
		const index = [
			'<h1 id="_top">Home</h1>',
			'<a href="/docs/guide/">absolute</a>',
			'<a href="guide/#setup">relative with anchor</a>',
			'<a href="/docs/guide#setup">no trailing slash</a>',
			'<a href="https://example.test/docs/guide/">site URL</a>',
			'<a href="//example.test/docs/guide/">protocol-relative</a>',
			'<a href="/docs/page">page.html</a>',
			'<a href="/docs">base without slash</a>',
			'<a href="#_top">id</a><a href="#top">top</a><a href="#">empty</a><a href="">self</a>',
			'<a href="/docs/guide/?q=1">query</a>',
			'<img src="/docs/logo.svg" srcset="/docs/logo.svg 1x, logo.svg 2x">',
			'<link rel="canonical" href="https://example.test/docs/404/">',
		].join('');
		const broken = brokenLinks({
			'index.html': index,
			'guide/index.html': `${GUIDE}<a href="../">up</a><a href="../#_top">up anchor</a>`,
			'page.html': '<p>page</p>',
			'logo.svg': '<svg/>',
		});
		expect(broken).toEqual([]);
	});

	test('an anchor written as an a name, or percent-encoded', () => {
		const broken = brokenLinks({
			'index.html': '<a href="/docs/guide/#legacy">name</a><a href="/docs/guide/#caf%C3%A9">encoded</a>',
			'guide/index.html': '<a name="legacy"></a><h2 id="café">Café</h2>',
		});
		expect(broken).toEqual([]);
	});

	test('a link to a redirect page without a fragment', () => {
		const broken = brokenLinks({
			'index.html': '<a href="/docs/old/">old</a>',
			'old/index.html': '<meta http-equiv="refresh" content="0;url=/docs/guide/"><a href="/docs/guide/">guide</a>',
			'guide/index.html': GUIDE,
		});
		expect(broken).toEqual([]);
	});

	test('no external URL', () => {
		const broken = brokenLinks({
			'index.html': [
				'<a href="https://other.test/docs/missing/">other origin</a>',
				'<a href="http://example.test/docs/missing/">other scheme</a>',
				'<a href="mailto:team@example.test">mail</a>',
				'<a href="javascript:void(0)">script</a>',
				'<img src="data:image/png;base64,AAAA">',
			].join(''),
		});
		expect(broken).toEqual([]);
	});
});

describe('checkSite reports', () => {
	test('missing-target for a path no file serves', () => {
		const broken = brokenLinks({ 'index.html': '<a href="/docs/absent/">x</a>' });
		expect(broken).toMatchObject([{ kind: 'missing-target', page: 'index.html', path: '/docs/absent/' }]);
		expect(describeBrokenLink(broken[0])).toContain('/docs/absent/');
	});

	test('missing-target for a missing srcset candidate and asset', () => {
		const broken = brokenLinks({
			'index.html': '<img src="/docs/a.png" srcset="/docs/a.png 1x, /docs/b.png 2x">',
			'a.png': 'png',
		});
		expect(broken).toMatchObject([{ kind: 'missing-target', path: '/docs/b.png' }]);
	});

	test('missing-target for a refresh target', () => {
		const broken = brokenLinks({
			'index.html': '<p>home</p>',
			'old/index.html': '<meta http-equiv="refresh" content="0; URL=\'/docs/gone/\'">',
		});
		expect(broken).toMatchObject([
			{ kind: 'missing-target', page: 'old/index.html', path: '/docs/gone/', reference: { attribute: 'content' } },
		]);
	});

	test('missing-anchor for a fragment with no element', () => {
		const broken = brokenLinks({
			'index.html': '<a href="/docs/guide/#absent">x</a><a href="#also-absent">y</a>',
			'guide/index.html': GUIDE,
		});
		expect(broken).toMatchObject([
			{ kind: 'missing-anchor', target: 'guide/index.html', fragment: 'absent' },
			{ kind: 'missing-anchor', target: 'index.html', fragment: 'also-absent' },
		]);
		expect(describeBrokenLink(broken[0])).toContain('"absent"');
	});

	test('anchor-on-redirect for a fragment on a link to a redirect page', () => {
		const broken = brokenLinks({
			'index.html': '<a href="/docs/old/#setup">x</a>',
			'old/index.html': '<meta http-equiv="refresh" content="0;url=/docs/guide/">',
			'guide/index.html': GUIDE,
		});
		expect(broken).toMatchObject([
			{ kind: 'anchor-on-redirect', target: 'old/index.html', redirectTarget: '/docs/guide/', fragment: 'setup' },
		]);
		expect(describeBrokenLink(broken[0])).toContain('link to /docs/guide/ directly');
	});

	test('outside-base for a root path outside the base path', () => {
		const broken = brokenLinks({ 'index.html': '<a href="/guide/">x</a><a href="../../guide/">y</a>' });
		expect(broken).toMatchObject([
			{ kind: 'outside-base', path: '/guide/' },
			{ kind: 'outside-base', path: '/guide/' },
		]);
	});

	test('malformed-url for a path that does not percent-decode', () => {
		const broken = brokenLinks({ 'index.html': '<a href="/docs/%E0%A4%A/">x</a>' });
		expect(broken).toMatchObject([{ kind: 'malformed-url' }]);
	});

	test('undecodable-attribute for a reference the check does not decode', () => {
		const broken = brokenLinks({ 'index.html': '<a href="/docs/a&nbsp;b/">x</a>' });
		expect(broken).toMatchObject([{ kind: 'undecodable-attribute', characterReference: '&nbsp;' }]);
		expect(describeBrokenLink(broken[0])).toContain('&nbsp;');
	});
});

describe('checkSite cannot run', () => {
	test('output-missing for a directory that does not exist', () => {
		const outcome = checkSite(join(tmpdir(), 'check-links-absent-directory'), ROOT, LIMITS);
		expect(outcome).toMatchObject({ kind: 'cannot-run', failure: { kind: 'output-missing' } });
	});

	test('unreadable for a page that cannot be read', () => {
		const directory = writeSite({ 'index.html': '<p>home</p>' });
		chmodSync(join(directory, 'index.html'), 0o000);
		const outcome = checkSite(directory, ROOT, LIMITS);
		chmodSync(join(directory, 'index.html'), 0o644);
		expect(outcome).toMatchObject({ kind: 'cannot-run', failure: { kind: 'unreadable' } });
	});

	test('no-pages for a directory without HTML', () => {
		const outcome = checkSite(writeSite({ 'notes.txt': 'text' }), ROOT, LIMITS);
		expect(outcome).toMatchObject({ kind: 'cannot-run', failure: { kind: 'no-pages' } });
	});

	test('depth-limit past the directory depth bound', () => {
		const directory = writeSite({ 'a/b/c/index.html': '<p>deep</p>' });
		const outcome = checkSite(directory, ROOT, { maxDirectoryDepth: 2, maxFiles: 100 });
		expect(outcome).toMatchObject({ kind: 'cannot-run', failure: { kind: 'depth-limit', path: 'a/b/c' } });
	});

	test('file-limit past the file bound', () => {
		const directory = writeSite({ 'a.html': '', 'b.html': '', 'c.html': '' });
		const outcome = checkSite(directory, ROOT, { maxDirectoryDepth: 16, maxFiles: 2 });
		expect(outcome).toMatchObject({ kind: 'cannot-run', failure: { kind: 'file-limit', limit: 2 } });
		if (outcome.kind === 'cannot-run') expect(describeRunFailure(outcome.failure)).toContain('2 files');
	});
});

describe('command line', () => {
	/** @param {string} directory */
	const run = (directory) => spawnSync(process.execPath, [SCRIPT, directory], { encoding: 'utf8' });

	test('exits 0 when every link resolves under the configured base path', () => {
		const result = run(writeSite({ 'index.html': '<a href="/horsies/guide/">x</a>', 'guide/index.html': '' }));
		expect(result.status).toBe(0);
	});

	test('exits 1 and names the page and link when a link is broken', () => {
		const result = run(writeSite({ 'index.html': '<a href="/horsies/absent/">x</a>' }));
		expect(result.status).toBe(1);
		expect(result.stderr).toContain('index.html');
		expect(result.stderr).toContain('/horsies/absent/');
	});

	test('exits 2 when the build output is missing', () => {
		const result = run(join(tmpdir(), 'check-links-absent-directory'));
		expect(result.status).toBe(2);
	});
});

#!/usr/bin/env bun
/**
 * Internal link and anchor check for the built docs site.
 *
 * Reads every HTML page of the build output and resolves each URL that a page
 * references against the URL that GitHub Pages serves the page at
 * (`site` + `base` from astro.config.mjs). For each internal URL it asserts:
 *
 *   1. The path is under the base path.
 *   2. A file of the build output serves the path. `/x/` is served by
 *      `x/index.html`. `/x` is served by the file `x`, by `x/index.html`
 *      (GitHub Pages redirects `/x` to `/x/` and keeps the fragment), or by
 *      `x.html`.
 *   3. A `#fragment` names an `id`, or the `name` of an `<a>`, on the target
 *      page. An empty fragment and `#top` need no element.
 *
 * A redirect page is a page with `<meta http-equiv="refresh">`, as Astro builds
 * for each entry of `redirects`. A link to a redirect page resolves; the
 * refresh target is checked as a link of the redirect page. A fragment on a
 * link to a redirect page is reported, because a refresh navigation drops the
 * fragment. Astro writes a `redirects` destination as given, without the base
 * path, so a destination must start with the base path.
 *
 * References read: `href` of `<a>`, `<area>`, and of `<link>` other than
 * `rel="canonical"`; `src` of `<img>`, `<script>`, `<source>`, `<video>`,
 * `<audio>`, `<track>`, `<iframe>`, `<embed>`; `srcset` of `<img>` and
 * `<source>`; `poster` of `<video>`; the URL of `<meta http-equiv="refresh">`.
 * A URL with another origin (`mailto:`, `data:`, and `javascript:` URLs
 * included) is external and is not checked. The query string is ignored.
 *
 * Usage:
 *   bun scripts/check-links.mjs        # check dist/
 *   bun scripts/check-links.mjs DIR    # check another build output
 *
 * Exit codes:
 *   0  every internal link and anchor resolves
 *   1  one or more links are broken; each one is written to stderr
 *   2  the check cannot run: the directory is missing, a file in it cannot
 *      be read, it holds no HTML page, or it exceeds MAX_DIRECTORY_DEPTH or
 *      MAX_FILES
 */

import { readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, relative, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

/** Deepest directory level below the build output that the walk enters. */
export const MAX_DIRECTORY_DEPTH = 16;

/** Most files the walk accepts. The build output holds a few hundred. */
export const MAX_FILES = 20000;

/**
 * Elements and the URL attribute that each one is checked for. A canonical
 * `<link>` is not checked: Starlight derives it from the page's own route, and
 * gives 404.html the canonical `/404/`, which GitHub Pages answers with
 * 404.html itself.
 */
const URL_ATTRIBUTES = Object.freeze([
	['a[href]', 'href'],
	['area[href]', 'href'],
	['link[href]:not([rel~="canonical"])', 'href'],
	['img[src]', 'src'],
	['script[src]', 'src'],
	['source[src]', 'src'],
	['video[src]', 'src'],
	['audio[src]', 'src'],
	['track[src]', 'src'],
	['iframe[src]', 'src'],
	['embed[src]', 'src'],
	['video[poster]', 'poster'],
]);

/** Elements whose `srcset` holds a comma-separated list of URL candidates. */
const SRCSET_ELEMENTS = Object.freeze(['img[srcset]', 'source[srcset]']);

/** The five named character references that HTML serializers emit. */
const NAMED_REFERENCES = new Map([
	['amp', '&'],
	['lt', '<'],
	['gt', '>'],
	['quot', '"'],
	['apos', "'"],
]);

const CHARACTER_REFERENCE = /&(?:#([0-9]+)|#[xX]([0-9a-fA-F]+)|([A-Za-z][A-Za-z0-9]*));/g;

/** `content` of a refresh `<meta>`: a delay, then an optional `url=`. */
const REFRESH_CONTENT = /^\s*[0-9.]+\s*[;,]?\s*(?:url\s*=\s*)?(['"]?)(.*?)\1\s*$/i;

/**
 * @typedef {{ selector: string, attribute: string, value: string }} Reference
 *   One URL that a page references. `value` is the attribute value after
 *   character references are decoded.
 * @typedef {(
 *   | { kind: 'not-redirect' }
 *   | { kind: 'redirect', target: string }
 * )} RedirectState
 * @typedef {{
 *   ids: ReadonlySet<string>,
 *   references: readonly Reference[],
 *   undecodable: readonly { selector: string, attribute: string, raw: string, characterReference: string }[],
 *   redirect: RedirectState,
 * }} Page
 * @typedef {(
 *   | { kind: 'missing-target', page: string, reference: Reference, path: string }
 *   | { kind: 'missing-anchor', page: string, reference: Reference, target: string, fragment: string }
 *   | { kind: 'anchor-on-redirect', page: string, reference: Reference, target: string, redirectTarget: string, fragment: string }
 *   | { kind: 'outside-base', page: string, reference: Reference, path: string }
 *   | { kind: 'malformed-url', page: string, reference: Reference, cause: string }
 *   | { kind: 'undecodable-attribute', page: string, selector: string, attribute: string, raw: string, characterReference: string }
 * )} BrokenLink
 * @typedef {(
 *   | { kind: 'output-missing', directory: string, cause: string }
 *   | { kind: 'unreadable', path: string, cause: string }
 *   | { kind: 'no-pages', directory: string }
 *   | { kind: 'depth-limit', directory: string, path: string, limit: number }
 *   | { kind: 'file-limit', directory: string, limit: number }
 * )} RunFailure
 * @typedef {(
 *   | { kind: 'checked', pages: number, references: number, broken: readonly BrokenLink[] }
 *   | { kind: 'cannot-run', failure: RunFailure }
 * )} CheckOutcome
 * @typedef {{ maxDirectoryDepth: number, maxFiles: number }} WalkLimits
 */

/**
 * Decode the character references of a raw attribute value.
 *
 * Decodes numeric references and the five named references in
 * NAMED_REFERENCES. An `&` that does not start a reference stays as it is.
 * Pure; does no I/O.
 *
 * @param {string} raw
 * @returns {{ kind: 'decoded', value: string } | { kind: 'undecodable', reference: string }}
 */
export function decodeAttribute(raw) {
	/** @type {string | undefined} */
	let undecodable;
	const value = raw.replace(CHARACTER_REFERENCE, (match, decimal, hexadecimal, name) => {
		if (name !== undefined) {
			const character = NAMED_REFERENCES.get(name);
			if (character === undefined) undecodable ??= match;
			return character ?? match;
		}
		const codePoint = decimal !== undefined ? Number(decimal) : Number.parseInt(hexadecimal, 16);
		if (codePoint < 1 || codePoint > 0x10ffff) {
			undecodable ??= match;
			return match;
		}
		return String.fromCodePoint(codePoint);
	});
	if (undecodable !== undefined) return { kind: 'undecodable', reference: undecodable };
	return { kind: 'decoded', value };
}

/**
 * List every file below `directory`, as `/`-separated paths relative to it.
 *
 * Reads the file system; changes nothing. Does not follow symbolic links.
 *
 * @param {string} directory
 * @param {WalkLimits} limits
 * @returns {{ kind: 'listed', files: ReadonlySet<string> } | { kind: 'cannot-run', failure: RunFailure }}
 */
export function listFiles(directory, limits) {
	try {
		if (!statSync(directory).isDirectory()) {
			return { kind: 'cannot-run', failure: { kind: 'output-missing', directory, cause: 'not a directory' } };
		}
	} catch (error) {
		return { kind: 'cannot-run', failure: { kind: 'output-missing', directory, cause: String(error) } };
	}
	/** @type {Set<string>} */
	const files = new Set();
	/** @type {[string, number][]} */
	const pending = [[directory, 0]];
	while (pending.length > 0) {
		const [current, depth] = /** @type {[string, number]} */ (pending.pop());
		if (depth > limits.maxDirectoryDepth) {
			const failure = { kind: 'depth-limit', directory, path: relative(directory, current), limit: limits.maxDirectoryDepth };
			return { kind: 'cannot-run', failure: /** @type {RunFailure} */ (failure) };
		}
		/** @type {import('node:fs').Dirent[]} */
		let entries;
		try {
			entries = readdirSync(current, { withFileTypes: true });
		} catch (error) {
			return { kind: 'cannot-run', failure: { kind: 'unreadable', path: current, cause: String(error) } };
		}
		for (const entry of entries) {
			const path = join(current, entry.name);
			if (entry.isDirectory()) {
				pending.push([path, depth + 1]);
			} else if (entry.isFile()) {
				if (files.size >= limits.maxFiles) {
					return { kind: 'cannot-run', failure: { kind: 'file-limit', directory, limit: limits.maxFiles } };
				}
				files.add(relative(directory, path).split(sep).join('/'));
			}
		}
	}
	return { kind: 'listed', files };
}

/**
 * Read the ids, the URL references, and the refresh target of one HTML page.
 *
 * Pure apart from the parser; `html` is the page source.
 *
 * @param {string} html
 * @returns {Page}
 */
export function readPage(html) {
	/** @type {Set<string>} */
	const ids = new Set();
	/** @type {Reference[]} */
	const references = [];
	/** @type {Page['undecodable'][number][]} */
	const undecodable = [];
	/** @type {RedirectState} */
	let redirect = { kind: 'not-redirect' };

	/** @param {string} selector @param {string} attribute @param {string | null} raw @returns {string | undefined} */
	const decoded = (selector, attribute, raw) => {
		if (raw === null) return undefined;
		const result = decodeAttribute(raw);
		switch (result.kind) {
			case 'decoded':
				return result.value;
			case 'undecodable':
				undecodable.push({ selector, attribute, raw, characterReference: result.reference });
				return undefined;
		}
	};
	/** @param {string} selector @param {string} attribute @param {string | undefined} value */
	const add = (selector, attribute, value) => {
		if (value !== undefined) references.push({ selector, attribute, value });
	};

	let rewriter = new HTMLRewriter()
		.on('[id]', { element: (element) => void addId(ids, decoded('[id]', 'id', element.getAttribute('id'))) })
		.on('a[name]', { element: (element) => void addId(ids, decoded('a[name]', 'name', element.getAttribute('name'))) })
		.on('meta[http-equiv]', {
			element: (element) => {
				if (element.getAttribute('http-equiv')?.toLowerCase() !== 'refresh') return;
				const content = decoded('meta[http-equiv=refresh]', 'content', element.getAttribute('content'));
				const target = content === undefined ? undefined : REFRESH_CONTENT.exec(content)?.[2];
				if (target === undefined || target === '') return;
				redirect = { kind: 'redirect', target };
				add('meta[http-equiv=refresh]', 'content', target);
			},
		});
	for (const [selector, attribute] of URL_ATTRIBUTES) {
		rewriter = rewriter.on(selector, {
			element: (element) => add(selector, attribute, decoded(selector, attribute, element.getAttribute(attribute))),
		});
	}
	for (const selector of SRCSET_ELEMENTS) {
		rewriter = rewriter.on(selector, {
			element: (element) => {
				const srcset = decoded(selector, 'srcset', element.getAttribute('srcset'));
				for (const candidate of srcset === undefined ? [] : srcset.split(/,\s+/)) {
					add(selector, 'srcset', candidate.trim().split(/\s+/)[0]);
				}
			},
		});
	}
	void rewriter.transform(html);
	return { ids, references, undecodable, redirect };
}

/** @param {Set<string>} ids @param {string | undefined} id */
function addId(ids, id) {
	if (id !== undefined && id !== '') ids.add(id);
}

/**
 * The URL that GitHub Pages serves a build-output HTML file at.
 *
 * @param {string} file `/`-separated path relative to the build output
 * @param {URL} root the site URL with the base path and a trailing `/`
 * @returns {URL}
 */
export function pageUrl(file, root) {
	const path = file === 'index.html' ? '' : file.endsWith('/index.html') ? file.slice(0, -'index.html'.length) : file;
	return new URL(path.split('/').map(encodeURIComponent).join('/'), root);
}

/**
 * Find the build-output file that serves a path below the base path.
 *
 * @param {string} path decoded path below the base path, without a leading `/`
 * @param {ReadonlySet<string>} files
 * @returns {{ kind: 'found', file: string } | { kind: 'missing' }}
 */
export function locateFile(path, files) {
	const candidates = path === '' || path.endsWith('/') ? [`${path}index.html`] : [path, `${path}/index.html`, `${path}.html`];
	const file = candidates.find((candidate) => files.has(candidate));
	return file === undefined ? { kind: 'missing' } : { kind: 'found', file };
}

/**
 * Check one reference of one page.
 *
 * Pure; reads only `files` and `pages`.
 *
 * @param {string} page the referencing file
 * @param {Reference} reference
 * @param {URL} root the site URL with the base path and a trailing `/`
 * @param {ReadonlySet<string>} files
 * @param {ReadonlyMap<string, Page>} pages
 * @returns {{ kind: 'resolves' } | { kind: 'external' } | { kind: 'broken', link: BrokenLink }}
 */
export function checkReference(page, reference, root, files, pages) {
	/** @param {BrokenLink} link */
	const broken = (link) => ({ kind: /** @type {const} */ ('broken'), link });
	/** @type {URL} */
	let url;
	try {
		url = new URL(reference.value.trim(), pageUrl(page, root));
	} catch (error) {
		return broken({ kind: 'malformed-url', page, reference, cause: String(error) });
	}
	if (url.origin !== root.origin) return { kind: 'external' };
	if (!url.pathname.startsWith(root.pathname) && url.pathname !== root.pathname.slice(0, -1)) {
		return broken({ kind: 'outside-base', page, reference, path: url.pathname });
	}
	/** @type {string} */
	let path;
	try {
		path = decodeURIComponent(url.pathname.slice(root.pathname.length));
	} catch (error) {
		return broken({ kind: 'malformed-url', page, reference, cause: String(error) });
	}
	const located = locateFile(path, files);
	switch (located.kind) {
		case 'missing':
			return broken({ kind: 'missing-target', page, reference, path: url.pathname });
		case 'found':
			return checkFragment(page, reference, url.hash.slice(1), located.file, pages);
	}
}

/**
 * Check the fragment of a reference against the page that serves it.
 *
 * @param {string} page
 * @param {Reference} reference
 * @param {string} fragment without the leading `#`
 * @param {string} target the file that serves the reference
 * @param {ReadonlyMap<string, Page>} pages
 * @returns {{ kind: 'resolves' } | { kind: 'broken', link: BrokenLink }}
 */
export function checkFragment(page, reference, fragment, target, pages) {
	const targetPage = pages.get(target);
	if (fragment === '' || targetPage === undefined) return { kind: 'resolves' };
	switch (targetPage.redirect.kind) {
		case 'redirect': {
			const redirectTarget = targetPage.redirect.target;
			return { kind: 'broken', link: { kind: 'anchor-on-redirect', page, reference, target, redirectTarget, fragment } };
		}
		case 'not-redirect':
			break;
	}
	/** @type {string} */
	let decodedFragment;
	try {
		decodedFragment = decodeURIComponent(fragment);
	} catch {
		decodedFragment = fragment;
	}
	const matches =
		targetPage.ids.has(fragment) || targetPage.ids.has(decodedFragment) || decodedFragment.toLowerCase() === 'top';
	if (matches) return { kind: 'resolves' };
	return { kind: 'broken', link: { kind: 'missing-anchor', page, reference, target, fragment: decodedFragment } };
}

/**
 * Check every internal reference of every HTML page below `directory`.
 *
 * Reads the file system; changes nothing.
 *
 * @param {string} directory the build output
 * @param {URL} root the site URL with the base path and a trailing `/`
 * @param {WalkLimits} limits
 * @returns {CheckOutcome}
 */
export function checkSite(directory, root, limits) {
	const listed = listFiles(directory, limits);
	switch (listed.kind) {
		case 'cannot-run':
			return listed;
		case 'listed':
			break;
	}
	const htmlFiles = [...listed.files].filter((file) => file.endsWith('.html')).sort();
	if (htmlFiles.length === 0) return { kind: 'cannot-run', failure: { kind: 'no-pages', directory } };
	/** @type {Map<string, Page>} */
	const pages = new Map();
	for (const file of htmlFiles) {
		const path = join(directory, file);
		/** @type {string} */
		let html;
		try {
			html = readFileSync(path, 'utf8');
		} catch (error) {
			return { kind: 'cannot-run', failure: { kind: 'unreadable', path, cause: String(error) } };
		}
		pages.set(file, readPage(html));
	}
	/** @type {BrokenLink[]} */
	const broken = [];
	let references = 0;
	for (const [file, page] of pages) {
		for (const entry of page.undecodable) broken.push({ kind: 'undecodable-attribute', page: file, ...entry });
		for (const reference of page.references) {
			references += 1;
			const result = checkReference(file, reference, root, listed.files, pages);
			switch (result.kind) {
				case 'resolves':
				case 'external':
					break;
				case 'broken':
					broken.push(result.link);
					break;
			}
		}
	}
	return { kind: 'checked', pages: pages.size, references, broken };
}

/**
 * One line that names a broken link and its cause.
 *
 * @param {BrokenLink} link
 * @returns {string}
 */
export function describeBrokenLink(link) {
	switch (link.kind) {
		case 'undecodable-attribute':
			return `${link.selector} ${link.attribute}="${link.raw}": character reference ${link.characterReference} is not decoded by this check`;
		case 'missing-target':
			return `${describeReference(link.reference)}: no file serves ${link.path}`;
		case 'missing-anchor':
			return `${describeReference(link.reference)}: ${link.target} has no element with id "${link.fragment}"`;
		case 'anchor-on-redirect':
			return `${describeReference(link.reference)}: ${link.target} redirects to ${link.redirectTarget}, and the redirect drops "#${link.fragment}"; link to ${link.redirectTarget} directly`;
		case 'outside-base':
			return `${describeReference(link.reference)}: ${link.path} is outside the base path`;
		case 'malformed-url':
			return `${describeReference(link.reference)}: ${link.cause}`;
	}
}

/** @param {Reference} reference @returns {string} */
function describeReference(reference) {
	return `${reference.selector} ${reference.attribute}="${reference.value}"`;
}

/**
 * One line that names why the check cannot run.
 *
 * @param {RunFailure} failure
 * @returns {string}
 */
export function describeRunFailure(failure) {
	switch (failure.kind) {
		case 'output-missing':
			return `${failure.directory} cannot be read (${failure.cause}); run \`bun run build\` first`;
		case 'unreadable':
			return `${failure.path} cannot be read (${failure.cause})`;
		case 'no-pages':
			return `${failure.directory} holds no HTML page`;
		case 'depth-limit':
			return `${failure.directory}/${failure.path} is more than ${failure.limit} directory levels deep`;
		case 'file-limit':
			return `${failure.directory} holds more than ${failure.limit} files`;
	}
}

/**
 * Run the check on the build output and set the process exit code.
 *
 * Writes the result to stdout and stderr.
 *
 * @returns {Promise<void>}
 */
async function main() {
	const { default: config } = await import('../astro.config.mjs');
	const root = new URL(`${config.base.replace(/\/+$/, '')}/`, config.site);
	const websiteDirectory = join(dirname(fileURLToPath(import.meta.url)), '..');
	const directory = process.argv[2] ?? join(websiteDirectory, 'dist');
	const outcome = checkSite(directory, root, { maxDirectoryDepth: MAX_DIRECTORY_DEPTH, maxFiles: MAX_FILES });
	switch (outcome.kind) {
		case 'cannot-run':
			console.error(`Link check cannot run: ${describeRunFailure(outcome.failure)}`);
			process.exitCode = 2;
			return;
		case 'checked':
			break;
	}
	if (outcome.broken.length === 0) {
		console.log(`Link check: ${outcome.references} references on ${outcome.pages} pages resolve under ${root.href}`);
		return;
	}
	console.error(`\nLink check: ${outcome.broken.length} broken links\n`);
	/** @type {Map<string, BrokenLink[]>} */
	const byPage = Map.groupBy(outcome.broken, (link) => link.page);
	for (const [page, links] of byPage) {
		console.error(`  ${page}`);
		for (const link of links) console.error(`    ${describeBrokenLink(link)}`);
	}
	console.error('\nThe page src/content/docs/<path>.md or .mdx builds <path>/index.html.\n');
	process.exitCode = 1;
}

if (import.meta.main) await main();

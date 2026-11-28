import { getCollection } from 'astro:content';
import { defineRouteMiddleware } from '@astrojs/starlight/route-data';
import type { StarlightRouteData } from '@astrojs/starlight/route-data';

/**
 * The section of a page. Shared pages state the behaviour and the guarantees
 * of the core, which every client runs on. Python, Rust and TypeScript pages
 * state the API of one client. Site pages are the landing page, the changelog
 * and the migration guides.
 */
type PageSection = 'shared' | 'python' | 'rust' | 'typescript' | 'site';

/** A section that a page has by its directory, and declares no `section` field. */
type DirectorySection = 'rust' | 'typescript';

/**
 * Pages under src/content/docs/rust/ are Rust API pages; pages under
 * src/content/docs/typescript/ are TypeScript API pages.
 */
const SECTION_DIRECTORIES: readonly DirectorySection[] = ['rust', 'typescript'];
/**
 * The id of Starlight's own 404 page. Starlight builds it when the docs hold no
 * `404.md`, so it has no frontmatter; it is a site page.
 */
const STARLIGHT_404_ENTRY_ID = '404';
const BASE_PATH = import.meta.env.BASE_URL.replace(/\/$/, '');
const SHARED_BANNER = 'Shared page: this behaviour holds for the Python, Rust and TypeScript clients.';

/** The title suffix of each section; `undefined` for no suffix. */
function titleSuffix(section: PageSection): string | undefined {
	switch (section) {
		case 'python':
			return ' (Python)';
		case 'rust':
			return ' (Rust)';
		case 'typescript':
			return ' (TypeScript)';
		case 'shared':
		case 'site':
			return undefined;
		default: {
			const unreachable: never = section;
			throw new Error(`unknown docs section ${String(unreachable)}`);
		}
	}
}

/** The name of the client of a directory section, as the error messages write it. */
function clientName(section: DirectorySection): string {
	switch (section) {
		case 'rust':
			return 'Rust';
		case 'typescript':
			return 'TypeScript';
		default: {
			const unreachable: never = section;
			throw new Error(`unknown docs directory section ${String(unreachable)}`);
		}
	}
}

/** The section that an entry has by its directory, or `undefined` for an entry outside those directories. */
function directorySection(entryId: string): DirectorySection | undefined {
	return SECTION_DIRECTORIES.find((directory) => entryId === directory || entryId.startsWith(`${directory}/`));
}

/**
 * The section of one entry: `rust` or `typescript` by its path, else its
 * `section` field. Throws, naming the file, for a page under one of those
 * directories that declares a section and for any other page that declares
 * none.
 */
function entrySection(entryId: string, filePath: string, declared: 'shared' | 'python' | 'site' | undefined): PageSection {
	if (entryId === STARLIGHT_404_ENTRY_ID && declared === undefined) return 'site';
	const byDirectory = directorySection(entryId);
	if (byDirectory !== undefined) {
		if (declared !== undefined) {
			throw new Error(
				`${filePath}: a page under ${byDirectory}/ is a ${clientName(byDirectory)} API page; remove "section: ${declared}"`,
			);
		}
		return byDirectory;
	}
	if (declared === undefined) {
		throw new Error(`${filePath}: the frontmatter needs "section": shared, python or site`);
	}
	return declared;
}

/** The URL path that Starlight serves an entry at. */
function entryHref(entryId: string): string {
	return entryId === 'index' ? `${BASE_PATH}/` : `${BASE_PATH}/${entryId}/`;
}

let sectionsByHref: Promise<ReadonlyMap<string, PageSection>> | undefined;

/** The section of every page, by URL path. Read once per build. */
function pageSections(): Promise<ReadonlyMap<string, PageSection>> {
	sectionsByHref ??= getCollection('docs').then(
		(entries) =>
			new Map(
				entries.map((entry) => [
					entryHref(entry.id),
					entrySection(entry.id, entry.filePath ?? entry.id, entry.data.section),
				]),
			),
	);
	return sectionsByHref;
}

/**
 * Adds the section's suffix to the page heading, the search result title,
 * `<title>` and `og:title`. The API pages of the clients often share a
 * title, and one search index serves them all.
 */
function labelTitle(route: StarlightRouteData, suffix: string): void {
	const pageTitle = route.entry.data.title;
	const labelledTitle = `${pageTitle}${suffix}`;
	const titleFromFrontmatter = route.entry.data.head.some((entry) => entry.tag === 'title');
	route.entry.data.title = labelledTitle;
	for (const entry of route.head) {
		if (entry.tag === 'title' && !titleFromFrontmatter) {
			// Starlight writes `<page title> <delimiter> <site title>`.
			if (typeof entry.content !== 'string' || !entry.content.startsWith(`${pageTitle} `)) {
				throw new Error(
					`${route.entry.filePath}: <title> "${String(entry.content)}" does not start with the page title "${pageTitle}"`,
				);
			}
			entry.content = `${labelledTitle}${entry.content.slice(pageTitle.length)}`;
		}
		if (entry.tag === 'meta' && entry.attrs?.property === 'og:title') {
			entry.attrs.content = labelledTitle;
		}
	}
}

/** Keeps the previous and next links of a page within its section. */
function keepPaginationInSection(
	route: StarlightRouteData,
	section: PageSection,
	sections: ReadonlyMap<string, PageSection>,
): void {
	const { prev, next } = route.pagination;
	if (prev !== undefined && sections.get(prev.href) !== section) route.pagination.prev = undefined;
	if (next !== undefined && sections.get(next.href) !== section) route.pagination.next = undefined;
}

export const onRequest = defineRouteMiddleware(async (context) => {
	const route = context.locals.starlightRoute;
	const sections = await pageSections();
	const section = entrySection(route.entry.id, route.entry.filePath, route.entry.data.section);
	const suffix = titleSuffix(section);
	if (suffix !== undefined) labelTitle(route, suffix);
	if (section === 'shared') route.entry.data.banner ??= { content: SHARED_BANNER };
	keepPaginationInSection(route, section, sections);
});

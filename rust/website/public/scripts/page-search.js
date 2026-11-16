/**
 * Vim-style "/" in-page search for Starlight docs.
 * Highlights matches inside .sl-markdown-content and navigates between them.
 * Supports cross-node matching (e.g. syntax-highlighted code spans).
 */
(function pageSearch() {
	'use strict';

	/* ── Constants ───────────────────────────────────── */
	const DEBOUNCE_MS = 150;
	const CONTENT_SELECTOR = '.sl-markdown-content';
	const MARK_CLASS = 'page-search-highlight';
	const ACTIVE_CLASS = 'page-search-active';

	/* ── State ───────────────────────────────────────── */
	var barEl = null;
	var inputEl = null;
	var counterEl = null;
	var marks = [];
	var activeIndex = -1;
	var debounceTimer = null;

	/* ── DOM helpers ─────────────────────────────────── */

	function buildBar() {
		var bar = document.createElement('div');
		bar.id = 'page-search-bar';
		bar.setAttribute('role', 'search');
		bar.setAttribute('aria-label', 'In-page text search');

		var label = document.createElement('span');
		label.className = 'page-search-label';
		label.textContent = '/';

		var input = document.createElement('input');
		input.id = 'page-search-input';
		input.type = 'text';
		input.placeholder = 'search this page…';
		input.setAttribute('aria-label', 'Search text');
		input.autocomplete = 'off';
		input.spellcheck = false;

		var counter = document.createElement('span');
		counter.id = 'page-search-counter';
		counter.textContent = '';

		var closeBtn = document.createElement('button');
		closeBtn.className = 'page-search-close';
		closeBtn.textContent = 'esc';
		closeBtn.setAttribute('aria-label', 'Close search');
		closeBtn.addEventListener('click', closeBar);

		bar.appendChild(label);
		bar.appendChild(input);
		bar.appendChild(counter);
		bar.appendChild(closeBtn);
		document.body.appendChild(bar);

		barEl = bar;
		inputEl = input;
		counterEl = counter;

		inputEl.addEventListener('input', onInput);
	}

	/* ── Open / Close ────────────────────────────────── */

	function openBar() {
		if (!barEl) buildBar();
		barEl.classList.add('visible');
		inputEl.value = '';
		counterEl.textContent = '';
		inputEl.focus();
	}

	function closeBar() {
		if (!barEl) return;
		barEl.classList.remove('visible');
		inputEl.blur();
		clearHighlights();
	}

	function isBarOpen() {
		return barEl !== null && barEl.classList.contains('visible');
	}

	/* ── Highlight engine (cross-node) ───────────────── */

	function clearHighlights() {
		var container = document.querySelector(CONTENT_SELECTOR);
		if (!container) return;

		var existing = container.querySelectorAll('mark.' + MARK_CLASS);
		for (var i = 0; i < existing.length; i++) {
			var mark = existing[i];
			var parent = mark.parentNode;
			if (!parent) continue;
			parent.replaceChild(document.createTextNode(mark.textContent), mark);
			parent.normalize();
		}

		marks = [];
		activeIndex = -1;
	}

	/**
	 * Build a flat list of text nodes and a concatenated string with
	 * an offset map so we can find which text nodes a match spans.
	 */
	function collectTextNodes(container) {
		var walker = document.createTreeWalker(container, NodeFilter.SHOW_TEXT, null);
		var nodes = [];
		var concat = '';
		var offsets = []; // { node, start, end } in concat coordinates

		while (walker.nextNode()) {
			var node = walker.currentNode;
			var text = node.nodeValue;
			if (!text) continue;
			var start = concat.length;
			concat += text;
			offsets.push({ node: node, start: start, end: concat.length });
			nodes.push(node);
		}

		return { concat: concat, offsets: offsets };
	}

	/**
	 * Given a match range [matchStart, matchEnd) in the concatenated string,
	 * wrap the corresponding text across one or more DOM text nodes in <mark> tags.
	 * Returns an array of the created <mark> elements.
	 */
	function wrapRange(offsets, matchStart, matchEnd) {
		var created = [];

		for (var i = 0; i < offsets.length; i++) {
			var entry = offsets[i];

			// Skip nodes entirely before or after the match
			if (entry.end <= matchStart) continue;
			if (entry.start >= matchEnd) break;

			var node = entry.node;
			var nodeStart = entry.start;

			// Offset within this text node where the match portion starts/ends
			var sliceStart = Math.max(0, matchStart - nodeStart);
			var sliceEnd = Math.min(node.nodeValue.length, matchEnd - nodeStart);

			// Split: isolate the matched portion
			// 1) Split off the "after" part if needed
			if (sliceEnd < node.nodeValue.length) {
				var afterNode = node.splitText(sliceEnd);
				// Update offsets for the new "after" node
				offsets.splice(i + 1, 0, {
					node: afterNode,
					start: nodeStart + sliceEnd,
					end: entry.end,
				});
				entry.end = nodeStart + sliceEnd;
			}

			// 2) Split off the "before" part if needed
			if (sliceStart > 0) {
				var matchNode = node.splitText(sliceStart);
				// Insert a new offset entry for the match portion
				offsets.splice(i + 1, 0, {
					node: matchNode,
					start: nodeStart + sliceStart,
					end: entry.end,
				});
				entry.end = nodeStart + sliceStart;
				// Advance to the match portion entry
				i++;
				node = matchNode;
			}

			// Wrap the node in <mark>
			var mark = document.createElement('mark');
			mark.className = MARK_CLASS;
			node.parentNode.replaceChild(mark, node);
			mark.appendChild(node);

			// Update offset entry to reference the text node inside the mark
			offsets[i].node = node;

			created.push(mark);
		}

		return created;
	}

	function highlightMatches(query) {
		clearHighlights();
		if (!query) {
			counterEl.textContent = '';
			return;
		}

		var container = document.querySelector(CONTENT_SELECTOR);
		if (!container) return;

		var data = collectTextNodes(container);
		var lowerConcat = data.concat.toLowerCase();
		var lowerQuery = query.toLowerCase();
		var queryLen = query.length;

		// Find all match positions in the concatenated text
		var matchPositions = [];
		var searchFrom = 0;
		while (true) {
			var idx = lowerConcat.indexOf(lowerQuery, searchFrom);
			if (idx === -1) break;
			matchPositions.push(idx);
			searchFrom = idx + 1;
		}

		// Wrap matches from last to first to preserve offsets
		var allMarks = [];
		for (var i = matchPositions.length - 1; i >= 0; i--) {
			var created = wrapRange(data.offsets, matchPositions[i], matchPositions[i] + queryLen);
			// Prepend so allMarks ends up in document order
			for (var j = created.length - 1; j >= 0; j--) {
				allMarks.unshift(created[j]);
			}
		}

		// Group contiguous marks into logical matches for navigation.
		// Each "match" may span multiple <mark> elements (cross-node).
		// We navigate by match group, but highlight individual marks.
		var matchGroups = [];
		if (matchPositions.length > 0) {
			// Each matchPosition produced a contiguous set of marks.
			// We wrapped from last to first, and unshifted, so allMarks
			// is in document order. We need to split them back into groups.
			// Each group has (matchEnd - matchStart) worth of marks, but
			// the count varies. Instead, re-collect from DOM in order.
			marks = Array.from(container.querySelectorAll('mark.' + MARK_CLASS));

			// Group: consecutive marks that are siblings or within the same
			// parent chain are one group. Use a simpler approach: since we
			// know the count of match positions, walk marks and group by
			// adjacency (a gap means new group).
			var group = [marks[0]];
			for (var k = 1; k < marks.length; k++) {
				// Check if this mark is "right after" the previous one
				// by testing if there's any non-empty text between them
				var prev = marks[k - 1];
				var curr = marks[k];
				var between = textBetween(prev, curr, container);
				if (between === '') {
					group.push(curr);
				} else {
					matchGroups.push(group);
					group = [curr];
				}
			}
			matchGroups.push(group);
		}

		marks = matchGroups; // now marks[i] = array of <mark> elements for match i

		if (marks.length > 0) {
			activeIndex = 0;
			activateMark(0);
		} else {
			counterEl.textContent = '0 / 0';
		}
	}

	/**
	 * Returns the text content between two sibling-ish nodes.
	 * A quick check: if prev.nextSibling === curr, they're adjacent → "".
	 */
	function textBetween(prev, curr, container) {
		// Fast path: direct siblings
		if (prev.nextSibling === curr) return '';
		// Check if prev's parent's next text connects to curr
		// Walk forward from prev until we hit curr or find text
		var node = prev;
		while (node) {
			if (node.nextSibling) {
				node = node.nextSibling;
				if (node === curr) return '';
				if (node.nodeType === Node.TEXT_NODE) {
					return node.nodeValue.replace(/^[\s]*$/, '') === '' ? '' : node.nodeValue;
				}
				if (node.nodeType === Node.ELEMENT_NODE) {
					if (node === curr || node.contains(curr)) return '';
					// There's an element between them — different group
					return ' ';
				}
			} else {
				// Go up
				node = node.parentNode;
				if (!node || node === container) return ' ';
				// Continue to check nextSibling of parent
			}
		}
		return ' ';
	}

	function activateMark(index) {
		// Remove active class from all marks in all groups
		for (var i = 0; i < marks.length; i++) {
			for (var j = 0; j < marks[i].length; j++) {
				marks[i][j].classList.remove(ACTIVE_CLASS);
			}
		}

		if (index < 0 || index >= marks.length) return;
		activeIndex = index;

		var group = marks[index];
		for (var k = 0; k < group.length; k++) {
			group[k].classList.add(ACTIVE_CLASS);
		}

		group[0].scrollIntoView({ behavior: 'smooth', block: 'center' });
		counterEl.textContent = (index + 1) + ' / ' + marks.length;
	}

	function goNext() {
		if (marks.length === 0) return;
		activateMark((activeIndex + 1) % marks.length);
	}

	function goPrev() {
		if (marks.length === 0) return;
		activateMark((activeIndex - 1 + marks.length) % marks.length);
	}

	/* ── Keyboard handling ───────────────────────────── */

	function shouldIgnoreSlash(event) {
		if (event.metaKey || event.ctrlKey || event.altKey) return true;
		var tag = event.target.tagName;
		if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return true;
		if (event.target.isContentEditable) return true;
		var closest = event.target.closest && event.target.closest('pre, code');
		if (closest) return true;
		if (document.body.hasAttribute('data-search-modal-open')) return true;
		return false;
	}

	function onKeyDown(event) {
		if (event.key === '/' && !isBarOpen()) {
			if (shouldIgnoreSlash(event)) return;
			event.preventDefault();
			openBar();
			return;
		}

		if (!isBarOpen()) return;

		if (event.key === 'Escape') {
			event.preventDefault();
			closeBar();
			return;
		}

		if (event.key === 'Enter' && !event.shiftKey) {
			event.preventDefault();
			goNext();
			return;
		}
		if (event.key === 'Enter' && event.shiftKey) {
			event.preventDefault();
			goPrev();
			return;
		}
		if (event.key === 'ArrowDown') {
			event.preventDefault();
			goNext();
			return;
		}
		if (event.key === 'ArrowUp') {
			event.preventDefault();
			goPrev();
			return;
		}
	}

	/* ── Input handler with debounce ─────────────────── */

	function onInput() {
		clearTimeout(debounceTimer);
		debounceTimer = setTimeout(function () {
			highlightMatches(inputEl.value.trim());
		}, DEBOUNCE_MS);
	}

	/* ── "/" hint in Pagefind search button ──────────── */

	function injectSlashHint() {
		var btn = document.querySelector('site-search button[data-open-modal]');
		if (!btn || btn.dataset.slashHint) return;
		btn.dataset.slashHint = '1';

		// Replace the existing kbd group with plain-text shortcut hints
		var existingKbd = btn.querySelector('kbd');
		if (!existingKbd) return;

		var hints = document.createElement('span');
		hints.className = 'search-shortcuts';
		hints.innerHTML = 'cmd + k<span class="search-shortcuts-sep">|</span><span class="search-shortcuts-slash">/</span>';
		hints.title = 'cmd+k: global search | /: search this page';

		existingKbd.replaceWith(hints);
	}

	/* ── Init ────────────────────────────────────────── */

	function init() {
		document.addEventListener('keydown', onKeyDown);
		injectSlashHint();

		document.addEventListener('astro:after-swap', function () {
			if (isBarOpen()) closeBar();
			// Re-inject after Starlight view transitions
			requestAnimationFrame(injectSlashHint);
		});
	}

	if (document.readyState === 'loading') {
		document.addEventListener('DOMContentLoaded', init);
	} else {
		init();
	}
})();

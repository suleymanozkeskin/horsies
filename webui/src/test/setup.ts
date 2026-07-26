import { afterEach } from 'vitest';

import { cleanup } from '@testing-library/react';

/**
 * jsdom ships no EventSource. The live provider opens one on mount, so tests
 * that render it need a constructible stand-in; it never emits, which leaves
 * the client in fallback-polling mode — the state most component tests want.
 */
class InertEventSource {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSED = 2;

  readonly url: string;
  readyState = InertEventSource.CONNECTING;
  onopen: ((event: Event) => void) | null = null;
  onmessage: ((event: MessageEvent<string>) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;

  constructor(url: string) {
    this.url = url;
  }

  close(): void {
    this.readyState = InertEventSource.CLOSED;
  }

  addEventListener(): void {}
  removeEventListener(): void {}
  dispatchEvent(): boolean {
    return false;
  }
}

if (!('EventSource' in globalThis)) {
  Object.defineProperty(globalThis, 'EventSource', {
    writable: true,
    configurable: true,
    value: InertEventSource,
  });
}

/**
 * jsdom implements neither of these, and cmdk calls both on mount: it observes
 * the list to size the menu, and scrolls the active item into view on every
 * selection change. Inert stand-ins — layout is not asserted in jsdom — keep
 * command-menu components (the comboboxes) renderable under test.
 */
class InertResizeObserver {
  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
}

if (!('ResizeObserver' in globalThis)) {
  Object.defineProperty(globalThis, 'ResizeObserver', {
    writable: true,
    configurable: true,
    value: InertResizeObserver,
  });
}

if (!('scrollIntoView' in Element.prototype)) {
  Object.defineProperty(Element.prototype, 'scrollIntoView', {
    writable: true,
    configurable: true,
    value: function scrollIntoView(): void {},
  });
}

afterEach(() => {
  cleanup();
});

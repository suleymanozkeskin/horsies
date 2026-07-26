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

afterEach(() => {
  cleanup();
});

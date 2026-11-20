import { describe, expect, it } from 'vitest';

import { TRACE_ID_PLACEHOLDER, traceLinkOf } from '@/lib/trace-link';

const TRACE_ID = '0af7651916cd43dd8448eb211c80319c';

describe('traceLinkOf', () => {
  it('puts the trace id in place of the placeholder', () => {
    expect(
      traceLinkOf(
        `https://grafana.example.com/explore?traceId=${TRACE_ID_PLACEHOLDER}`,
        TRACE_ID
      )
    ).toBe(`https://grafana.example.com/explore?traceId=${TRACE_ID}`);
  });

  it('keeps the rest of the template as written', () => {
    expect(
      traceLinkOf(`http://jaeger:16686/trace/${TRACE_ID_PLACEHOLDER}?uiEmbed=v0`, TRACE_ID)
    ).toBe(`http://jaeger:16686/trace/${TRACE_ID}?uiEmbed=v0`);
  });
});

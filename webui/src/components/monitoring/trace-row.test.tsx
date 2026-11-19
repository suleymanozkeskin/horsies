import { describe, expect, it } from 'vitest';

import { render, screen } from '@testing-library/react';

import { TraceRow } from '@/components/monitoring/detail';

const TRACE_ID = '0af7651916cd43dd8448eb211c80319c';
const TEMPLATE = 'https://grafana.example.com/explore?traceId={trace_id}';

describe('TraceRow', () => {
  it('links the trace id with the template of the server', () => {
    render(<TraceRow traceId={TRACE_ID} template={TEMPLATE} />);

    const link = screen.getByRole('link');
    expect(link.getAttribute('href')).toBe(
      `https://grafana.example.com/explore?traceId=${TRACE_ID}`
    );
    expect(link.getAttribute('target')).toBe('_blank');
    expect(link.getAttribute('rel')).toBe('noopener noreferrer');
    expect(link.textContent).toBe(TRACE_ID);
  });

  it('shows the trace id as text with no template', () => {
    render(<TraceRow traceId={TRACE_ID} template={null} />);

    expect(screen.queryByRole('link')).toBeNull();
    expect(screen.getByText(TRACE_ID)).toBeTruthy();
  });

  it('shows nothing for a task sent with no trace', () => {
    const { container } = render(<TraceRow traceId={null} template={TEMPLATE} />);

    expect(container.textContent).toBe('');
  });
});

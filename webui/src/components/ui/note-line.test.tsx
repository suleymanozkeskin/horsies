import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { NoteLine } from '@/components/ui/note-line';

describe('NoteLine', () => {
  it('shows a warning as a note with the marked ink on its icon', () => {
    render(<NoteLine view={{ tone: 'warning', text: 'Queue wait 7.2s.' }} />);

    const note = screen.getByRole('note');
    expect(note.textContent).toBe('Queue wait 7.2s.');
    expect(note.getAttribute('data-tone')).toBe('warning');
    expect(note.querySelector('svg')?.getAttribute('style')).toContain('var(--marked-ink)');
  });

  it('shows an info note in the quiet text color', () => {
    render(<NoteLine view={{ tone: 'info', text: 'Claims not counted.' }} />);

    const note = screen.getByRole('note');
    expect(note.getAttribute('data-tone')).toBe('info');
    expect(note.className).toContain('text-muted-foreground');
    expect(note.querySelector('svg')?.getAttribute('aria-hidden')).toBe('true');
  });
});

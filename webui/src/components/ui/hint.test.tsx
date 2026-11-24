import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { Hint } from '@/components/ui/hint';

describe('Hint', () => {
  it('gives the element its text as its accessible description', () => {
    render(<Hint text="The time of the node.">4.0s</Hint>);

    expect(screen.getByText('4.0s').getAttribute('aria-description')).toBe('The time of the node.');
  });

  it('has no native title: the hint opens with no wait of the browser', () => {
    render(<Hint text="The time of the node.">4.0s</Hint>);

    expect(screen.getByText('4.0s').getAttribute('title')).toBeNull();
  });

  it('shows its text when the element gets the focus', async () => {
    render(
      <Hint text="The time of the node.">
        <button type="button">4.0s</button>
      </Hint>
    );

    fireEvent.focus(screen.getByText('4.0s').closest('[aria-description]') as HTMLElement);

    expect((await screen.findByRole('tooltip')).textContent).toBe('The time of the node.');
  });
});

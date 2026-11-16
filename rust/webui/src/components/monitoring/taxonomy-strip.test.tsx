import { describe, expect, it, vi } from 'vitest';

import { fireEvent, render, screen } from '@testing-library/react';

import {
  TaxonomyStrip,
  visibleCategories,
} from '@/components/monitoring/taxonomy-strip';

describe('visibleCategories', () => {
  it('lists the categories that carry tasks, in display order', () => {
    expect(visibleCategories({ DOMAIN: 3, OPERATIONAL: 1 }, [])).toEqual([
      'OPERATIONAL',
      'DOMAIN',
    ]);
  });

  it('omits a category whose total is zero or absent', () => {
    expect(visibleCategories({ OPERATIONAL: 0 }, [])).toEqual([]);
  });

  it('keeps an engaged category listed even with no total', () => {
    // A filter narrowed the surface to nothing; the chip that did it has to
    // remain, or the filter cannot be switched off.
    expect(visibleCategories({}, ['CONTRACT'])).toEqual(['CONTRACT']);
  });

  it('does not duplicate a category that is both present and engaged', () => {
    expect(visibleCategories({ CONTRACT: 2 }, ['CONTRACT'])).toEqual([
      'CONTRACT',
    ]);
  });

  it('is empty only when nothing has tasks and nothing is engaged', () => {
    expect(visibleCategories({}, [])).toEqual([]);
  });
});

describe('TaxonomyStrip', () => {
  it('renders a chip per category with its count', () => {
    render(
      <TaxonomyStrip
        totals={{ OPERATIONAL: 4, DOMAIN: 2 }}
        active={[]}
        onToggle={vi.fn()}
      />
    );

    const chips = screen.getAllByRole('button');
    expect(chips).toHaveLength(2);
    expect(chips[0]?.textContent).toContain('Infrastructure');
    expect(chips[0]?.textContent).toContain('4');
  });

  it('reports the category when a chip is clicked', () => {
    const onToggle = vi.fn();
    render(
      <TaxonomyStrip
        totals={{ OPERATIONAL: 4, DOMAIN: 2 }}
        active={[]}
        onToggle={onToggle}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: /Infrastructure/ }));

    expect(onToggle).toHaveBeenCalledWith('OPERATIONAL');
  });

  it('marks engaged chips pressed and leaves the others unpressed', () => {
    render(
      <TaxonomyStrip
        totals={{ OPERATIONAL: 4, DOMAIN: 2 }}
        active={['DOMAIN']}
        onToggle={vi.fn()}
      />
    );

    expect(
      screen.getByRole('button', { name: /Domain/ }).getAttribute('aria-pressed')
    ).toBe('true');
    expect(
      screen
        .getByRole('button', { name: /Infrastructure/ })
        .getAttribute('aria-pressed')
    ).toBe('false');
  });

  it('reports an engaged category again so the caller can toggle it off', () => {
    const onToggle = vi.fn();
    render(
      <TaxonomyStrip
        totals={{ DOMAIN: 2 }}
        active={['DOMAIN']}
        onToggle={onToggle}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: /Domain/ }));

    expect(onToggle).toHaveBeenCalledWith('DOMAIN');
  });

  it('stays rendered while engaged even when its totals go empty', () => {
    render(<TaxonomyStrip totals={{}} active={['RETRIEVAL']} onToggle={vi.fn()} />);

    const chip = screen.getByRole('button', { name: /Retrieval/ });
    expect(chip.getAttribute('aria-pressed')).toBe('true');
    expect(chip.textContent).toContain('0');
  });

  it('renders nothing when there are no totals and no filter engaged', () => {
    const { container } = render(
      <TaxonomyStrip totals={{}} active={[]} onToggle={vi.fn()} />
    );

    expect(container.firstChild).toBeNull();
  });
});

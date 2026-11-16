import { describe, expect, it, vi } from 'vitest';

import { fireEvent, render, screen, within } from '@testing-library/react';

import {
  SelectCombobox,
  type SelectOption,
} from '@/components/ui/select-combobox';

const workflows = (count: number): SelectOption[] =>
  Array.from({ length: count }, (_, index) => ({ value: `wf-${index}` }));

/** Click the trigger. Only valid while closed: a modal popover hides it. */
const openMenu = (): void => {
  fireEvent.click(screen.getByRole('button'));
};

const typeQuery = (text: string): void => {
  fireEvent.change(screen.getByPlaceholderText('Search…'), {
    target: { value: text },
  });
};

/** The popover is closed once its search box is gone. */
const isClosed = (): boolean =>
  screen.queryByPlaceholderText('Search…') === null;

describe('SelectCombobox', () => {
  it('shows the clear label on the trigger while nothing is selected', () => {
    render(
      <SelectCombobox
        label="Workflow"
        emptyLabel="All workflows"
        options={workflows(3)}
        value={null}
        onChange={vi.fn()}
      />
    );
    expect(screen.getByRole('button').textContent).toContain('All workflows');
  });

  it('shows the selected option label on the trigger', () => {
    render(
      <SelectCombobox
        label="Status"
        emptyLabel="Any status"
        options={[
          { value: 'RUNNING', label: 'running' },
          { value: 'FAILED', label: 'failed' },
        ]}
        value="RUNNING"
        onChange={vi.fn()}
      />
    );
    expect(screen.getByRole('button').textContent).toContain('running');
  });

  it('renders at most `cap` options plus the clear row, and counts the rest', () => {
    render(
      <SelectCombobox
        label="Workflow"
        emptyLabel="All workflows"
        options={workflows(120)}
        value={null}
        onChange={vi.fn()}
      />
    );
    openMenu();

    // 50 capped options + the clear row.
    expect(screen.getAllByRole('option')).toHaveLength(51);
    expect(screen.getByText('wf-49')).toBeTruthy();
    expect(screen.queryByText('wf-50')).toBeNull();
    expect(screen.getByText('70 more — keep typing to narrow')).toBeTruthy();
  });

  it('narrows on the query and drops the overflow hint once it fits', () => {
    render(
      <SelectCombobox
        label="Workflow"
        emptyLabel="All workflows"
        options={workflows(120)}
        value={null}
        onChange={vi.fn()}
      />
    );
    openMenu();
    typeQuery('wf-11');

    // wf-11 and wf-110…wf-119, plus the clear row.
    expect(screen.getAllByRole('option')).toHaveLength(12);
    expect(screen.getByText('wf-113')).toBeTruthy();
    expect(screen.queryByText('wf-2')).toBeNull();
    expect(screen.queryByText(/keep typing to narrow/)).toBeNull();
  });

  it('reports the picked value and closes', () => {
    const onChange = vi.fn();
    render(
      <SelectCombobox
        label="Workflow"
        emptyLabel="All workflows"
        options={workflows(120)}
        value={null}
        onChange={onChange}
      />
    );
    openMenu();
    fireEvent.click(screen.getByText('wf-3'));

    expect(onChange).toHaveBeenCalledWith('wf-3');
    expect(isClosed()).toBe(true);
  });

  it('reports null from the clear row and closes', () => {
    const onChange = vi.fn();
    render(
      <SelectCombobox
        label="Status"
        emptyLabel="Any status"
        options={[{ value: 'RUNNING', label: 'running' }]}
        value="RUNNING"
        onChange={onChange}
      />
    );
    openMenu();
    fireEvent.click(screen.getByText('Any status'));

    expect(onChange).toHaveBeenCalledWith(null);
    expect(isClosed()).toBe(true);
  });

  it('keeps the clear row reachable when the query matches nothing', () => {
    const onChange = vi.fn();
    render(
      <SelectCombobox
        label="Workflow"
        emptyLabel="All workflows"
        options={workflows(120)}
        value={null}
        onChange={onChange}
      />
    );
    openMenu();
    typeQuery('nothing-matches-this');

    expect(screen.getByText('No matches.')).toBeTruthy();
    const list = screen.getByRole('listbox');
    expect(within(list).getAllByRole('option')).toHaveLength(1);

    fireEvent.click(within(list).getByText('All workflows'));
    expect(onChange).toHaveBeenCalledWith(null);
  });

  it('discards the query when the menu closes', () => {
    render(
      <SelectCombobox
        label="Workflow"
        emptyLabel="All workflows"
        options={workflows(120)}
        value={null}
        onChange={vi.fn()}
      />
    );
    openMenu();
    typeQuery('wf-113');
    fireEvent.click(screen.getByText('wf-113'));
    openMenu();

    expect(screen.getByPlaceholderText('Search…')).toHaveProperty('value', '');
    expect(screen.getAllByRole('option')).toHaveLength(51);
  });

  it('renders counts and adornments only for options that carry them', () => {
    render(
      <SelectCombobox
        label="Workflow"
        emptyLabel="All workflows"
        options={[
          { value: 'plain' },
          {
            value: 'counted',
            count: 42,
            adornment: <span data-testid="dot" />,
          },
        ]}
        value={null}
        onChange={vi.fn()}
      />
    );
    openMenu();

    expect(screen.getByText('42')).toBeTruthy();
    expect(screen.getAllByTestId('dot')).toHaveLength(1);
  });
});

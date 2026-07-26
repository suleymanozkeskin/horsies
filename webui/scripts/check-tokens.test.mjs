import { describe, expect, it } from 'vitest';

import { addedLines, RULES } from './check-tokens.mjs';

/** Look up a rule by id so tests read clearly. */
function rule(id) {
  const found = RULES.find(candidate => candidate.id === id);
  if (!found) {
    throw new Error(`rule not found: ${id}`);
  }
  return found;
}

describe('opacity-modifier rule', () => {
  const { re } = rule('opacity-modifier');

  // Tailwind fraction utilities share the `<util>-…/<n>` shape but are NOT
  // opacity modifiers: the slash follows a digit, not a color-token letter.
  it.each([
    'slide-in-from-left-1/2',
    'slide-out-to-right-1/2',
    'data-[state=open]:slide-in-from-left-1/2',
    'from-bottom-1/3',
  ])('does not flag fraction utility: %s', className => {
    expect(re.test(className)).toBe(false);
  });

  it.each([
    'bg-muted/50',
    'text-primary/80',
    'border-accent/20',
    'bg-destructive/90',
    'text-muted-foreground/20',
  ])('flags opacity modifier: %s', className => {
    expect(re.test(className)).toBe(true);
  });
});

describe('palette-shade rule', () => {
  const { re } = rule('palette-shade');

  it('flags a palette-and-shade class', () => {
    expect(re.test('text-gray-600')).toBe(true);
    expect(re.test('bg-red-50')).toBe(true);
  });

  it('does not flag a semantic token', () => {
    expect(re.test('text-muted-foreground')).toBe(false);
    expect(re.test('bg-glass-surface-strong')).toBe(false);
  });
});

describe('black-white rule', () => {
  const { re } = rule('black-white');

  it('flags raw black/white utilities', () => {
    expect(re.test('bg-white')).toBe(true);
    expect(re.test('text-black')).toBe(true);
  });
});

describe('arbitrary-color rule', () => {
  const { re } = rule('arbitrary-color');

  it('flags hex and function color literals', () => {
    expect(re.test('bg-[#daa520]')).toBe(true);
    expect(re.test('text-[oklch(0.5 0 0)]')).toBe(true);
  });

  it('does not flag a non-color arbitrary value', () => {
    expect(re.test('max-h-[calc(100vh-26rem)]')).toBe(false);
    expect(re.test('lg:w-[420px]')).toBe(false);
  });
});

describe('inline-style-color rule', () => {
  const { re } = rule('inline-style-color');

  it('flags a hardcoded inline color', () => {
    expect(re.test("style={{ background: '#ff0000' }}")).toBe(true);
  });

  it('does not flag a token reference', () => {
    expect(re.test("style={{ background: 'var(--success)' }}")).toBe(false);
    expect(
      re.test(
        "style={{ background: `color-mix(in oklab, ${color} 6%, var(--card))` }}"
      )
    ).toBe(false);
  });
});

describe('arbitrary-font-size rule', () => {
  const { re } = rule('arbitrary-font-size');

  it('flags a pixel font size', () => {
    expect(re.test('text-[13px]')).toBe(true);
  });

  it('does not flag a token size', () => {
    expect(re.test('text-13')).toBe(false);
  });
});

describe('diff scoping', () => {
  const diff = [
    'diff --git a/src/a.tsx b/src/a.tsx',
    '--- a/src/a.tsx',
    '+++ b/src/a.tsx',
    '@@ -1,0 +7,1 @@',
    '+  <div className="bg-red-500" />',
    'diff --git a/src/styles/tokens.css b/src/styles/tokens.css',
    '--- a/src/styles/tokens.css',
    '+++ b/src/styles/tokens.css',
    '@@ -1,0 +3,1 @@',
    '+  --primary: #daa520;',
    'diff --git a/README.md b/README.md',
    '--- a/README.md',
    '+++ b/README.md',
    '@@ -1,0 +1,1 @@',
    '+bg-red-500',
  ].join('\n');

  it('scans added lines in scanned extensions and records their line numbers', () => {
    expect(addedLines(diff)).toEqual([
      { file: 'src/a.tsx', line: 7, content: '  <div className="bg-red-500" />' },
    ]);
  });

  it('skips the token source and non-code files', () => {
    const files = addedLines(diff).map(row => row.file);
    expect(files).not.toContain('src/styles/tokens.css');
    expect(files).not.toContain('README.md');
  });
});

import { describe, expect, test } from "bun:test";

import { type Block, type BlockMode, type ExtractError, extractErrorText, pageKindOf, readBlocks } from "./extract.ts";

function modes(text: string): readonly (readonly [number, BlockMode])[] {
  const blocks = readBlocks("md", text);

  if (!blocks.ok) {
    throw new Error(`the page does not read: ${extractErrorText("page.md", blocks.error)}`);
  }

  return blocks.value.map((block) => [block.fenceLine, block.mode] as const);
}

function onlyBlock(text: string): Block {
  const blocks = readBlocks("md", text);
  const [block] = blocks.ok ? blocks.value : [];

  if (block === undefined) {
    throw new Error("the page has no block");
  }

  return block;
}

function refusal(text: string, pageKind: "md" | "mdx" = "md"): ExtractError {
  const blocks = readBlocks(pageKind, text);

  if (blocks.ok) {
    throw new Error(`the page reads with ${blocks.value.length} blocks`);
  }

  return blocks.error;
}

describe("readBlocks: the marker rules", () => {
  test("a block without a marker is chained", () => {
    expect(modes("Text.\n\n```ts\nconst a = 1;\n```\n")).toEqual([[3, { kind: "chained" }]]);
  });

  test("alone: the block compiles in a file of its own", () => {
    expect(modes("<!-- ts-check: alone -->\n```ts\nconst a = 1;\n```\n")).toEqual([[2, { kind: "alone" }]]);
  });

  test("skip: each kind is read with its reason", () => {
    for (const skip of ["elided", "partial", "defect"] as const) {
      const text = `<!-- ts-check: skip ${skip} -- the reason -->\n\`\`\`ts\nawait app.send();\n\`\`\`\n`;

      expect(modes(text)).toEqual([[2, { kind: "skipped", skip, reason: "the reason" }]]);
    }
  });

  test("expect: a must-fail block with one list for both lanes", () => {
    expect(modes("<!-- ts-check: expect TS2322 at 1, TS2353 at 2 -->\n```ts\nconst a: number = '';\n```\n")).toEqual([
      [
        2,
        {
          kind: "mustFail",
          expectation: {
            current: [
              { code: "TS2322", line: 1 },
              { code: "TS2353", line: 2 },
            ],
            floor: [
              { code: "TS2322", line: 1 },
              { code: "TS2353", line: 2 },
            ],
          },
        },
      ],
    ]);
  });

  test("expect: a floor list of its own", () => {
    expect(modes("<!-- ts-check: expect TS2322 at 1; floor: TS2345 at 1 -->\n```ts\n```\n")).toEqual([
      [2, { kind: "mustFail", expectation: { current: [{ code: "TS2322", line: 1 }], floor: [{ code: "TS2345", line: 1 }] } }],
    ]);
  });

  test("the .mdx comment form is read on an .mdx page", () => {
    const blocks = readBlocks("mdx", "{/* ts-check: skip elided -- a signature */}\n```ts\nfunction a(): void;\n```\n");

    expect(blocks.ok && blocks.value.map((block) => block.mode)).toEqual([{ kind: "skipped", skip: "elided", reason: "a signature" }]);
  });

  test("blank lines can stand between a marker and its fence", () => {
    expect(modes("<!-- ts-check: alone -->\n\n\n```ts\nconst a = 1;\n```\n")).toEqual([[4, { kind: "alone" }]]);
  });

  test("a marker holds for its own block only", () => {
    const text = "<!-- ts-check: alone -->\n```ts\nconst a = 1;\n```\n\n```ts\nconst b = 2;\n```\n";

    expect(modes(text)).toEqual([
      [2, { kind: "alone" }],
      [6, { kind: "chained" }],
    ]);
  });

  test("ts and typescript fences are TypeScript blocks; an info string can follow the language", () => {
    const text = "```typescript\nconst a = 1;\n```\n\n```ts title=\"app.ts\"\nconst b = 2;\n```\n\n```ts{2}\nconst c = 3;\n```\n";

    expect(modes(text).map(([line]) => line)).toEqual([1, 5, 9]);
  });

  test("other fences are not read, and a ts fence inside a longer fence is the longer fence's text", () => {
    const text = "```python\nx = 1\n```\n\n````md\n```ts\nconst a = 1;\n```\n````\n\n~~~bash\nls\n~~~\n";

    expect(modes(text)).toEqual([]);
  });

  test("the code of a fence in a list item loses the fence's indent", () => {
    expect(onlyBlock("1. A step:\n\n   ```ts\n   const a = 1;\n     const b = 2;\n   ```\n").code).toEqual([
      "const a = 1;",
      "  const b = 2;",
    ]);
  });

  test("a closing fence is a run of the opening fence's character, at least as long, with no info string", () => {
    expect(onlyBlock("```ts\nconst a = `x`;\n```ts\n``\n````\n").code).toEqual(["const a = `x`;", "```ts", "``"]);
  });

  test("a page with CRLF line ends reads as with LF", () => {
    expect(onlyBlock("<!-- ts-check: alone -->\r\n```ts\r\nconst a = 1;\r\n```\r\n")).toEqual({
      fenceLine: 2,
      mode: { kind: "alone" },
      code: ["const a = 1;"],
    });
  });

  test("a prose line that names the marker word is no marker", () => {
    expect(modes("Mark a block with `ts-check: alone`.\n\n```ts\nconst a = 1;\n```\n")).toEqual([[3, { kind: "chained" }]]);
  });
});

describe("readBlocks: the conditions that stop the check", () => {
  test("UnknownMarker: a marker whose text is not a known marker", () => {
    for (const marker of [
      "<!-- ts-check: chained -->",
      "<!-- ts-check: alone now -->",
      "<!-- ts-check: skip later -- why -->",
      "<!-- ts-check: skip partial elided -- why -->",
      "<!-- ts-check alone -->",
      "<!-- ts-check: alone",
    ]) {
      expect(refusal(`${marker}\n\`\`\`ts\n\`\`\`\n`)).toEqual({ kind: "UnknownMarker", line: 1, text: marker });
    }
  });

  test("UnknownMarker: a marker in the comment form of the other page kind", () => {
    expect(refusal("{/* ts-check: alone */}\n```ts\n```\n", "md")).toEqual({
      kind: "UnknownMarker",
      line: 1,
      text: "{/* ts-check: alone */}",
    });
    expect(refusal("<!-- ts-check: alone -->\n```ts\n```\n", "mdx")).toEqual({
      kind: "UnknownMarker",
      line: 1,
      text: "<!-- ts-check: alone -->",
    });
  });

  test("SkipWithoutKind: a skip with no kind", () => {
    for (const marker of ["<!-- ts-check: skip -->", "<!-- ts-check: skip -- why -->"]) {
      expect(refusal(`${marker}\n\`\`\`ts\n\`\`\`\n`)).toEqual({ kind: "SkipWithoutKind", line: 1 });
    }
  });

  test("SkipWithoutReason: a skip with no reason", () => {
    for (const marker of ["<!-- ts-check: skip partial -->", "<!-- ts-check: skip defect --  -->"]) {
      expect(refusal(`${marker}\n\`\`\`ts\n\`\`\`\n`)).toEqual({ kind: "SkipWithoutReason", line: 1 });
    }
  });

  test("ExpectListRefused: an expect list that does not read", () => {
    for (const [marker, item] of [
      ["<!-- ts-check: expect -->", ""],
      ["<!-- ts-check: expect TS2322 -->", "TS2322"],
      ["<!-- ts-check: expect TS2322 at 0 -->", "TS2322 at 0"],
      ["<!-- ts-check: expect TS2322 at 1 -- why -->", "TS2322 at 1 -- why"],
    ] as const) {
      expect(refusal(`${marker}\n\`\`\`ts\n\`\`\`\n`)).toEqual({ kind: "ExpectListRefused", line: 1, item });
    }
  });

  test("MarkerWithoutFence: a marker with no TypeScript fence directly under it", () => {
    for (const [text, line] of [
      ["<!-- ts-check: alone -->\n```python\nx = 1\n```\n", 1],
      ["<!-- ts-check: alone -->\nText.\n```ts\n```\n", 1],
      ["<!-- ts-check: alone -->\n<!-- ts-check: alone -->\n```ts\n```\n", 1],
      ["Text.\n\n<!-- ts-check: alone -->\n", 3],
    ] as const) {
      expect(refusal(text)).toEqual({ kind: "MarkerWithoutFence", line });
    }
  });

  test("UnclosedFence: a fence that no closing fence ends, of any language", () => {
    expect(refusal("Text.\n```ts\nconst a = 1;\n")).toEqual({ kind: "UnclosedFence", line: 2 });
    expect(refusal("```bash\nls\n")).toEqual({ kind: "UnclosedFence", line: 1 });
    expect(refusal("````ts\nconst a = 1;\n```\n")).toEqual({ kind: "UnclosedFence", line: 1 });
  });

  test("each error names its page and line", () => {
    expect(extractErrorText("docs/page.md", { kind: "UnclosedFence", line: 7 })).toBe(
      "docs/page.md:7: the fence has no closing fence",
    );
    expect(extractErrorText("docs/page.md", { kind: "SkipWithoutReason", line: 3 })).toContain("docs/page.md:3: ");
  });
});

describe("pageKindOf", () => {
  test("the extension gives the kind; another file is no page", () => {
    expect([pageKindOf("a.md"), pageKindOf("a.mdx"), pageKindOf("a.ts"), pageKindOf("md")]).toEqual(["md", "mdx", "notAPage", "notAPage"]);
  });
});

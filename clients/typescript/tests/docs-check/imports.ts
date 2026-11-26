// The imports of a chain of blocks. A page repeats an import in each block, so that a reader can copy one block; in one
// file the repeat is a duplicate name (TS2300). A binding of an import is dropped when an import of a block above it
// in the same file makes the same binding: the same local name, of the same imported name, from the same module, with
// the same `type` form. An import that makes a binding of its own stays, so a name that two blocks import from two
// modules is still a duplicate. The syntax trees come from the TypeScript 6.0 API (TypeScript 7 has no compiler API).

import ts from "@typescript/typescript6";

/** One name that an import declaration binds, and the one-name import statement that makes it alone. */
type ImportBinding = { readonly key: string; readonly statement: string };

/** The code of a block with the repeated bindings dropped, and the bindings that its imports make. */
export type MergedImports = { readonly code: readonly string[]; readonly bindings: ReadonlySet<string> };

/** The bindings of a declaration, or `kept` for one that stays as it is: a side-effect, deferred or attributed import. */
function bindingsOf(declaration: ts.ImportDeclaration, sourceFile: ts.SourceFile): readonly ImportBinding[] | "kept" {
  const clause = declaration.importClause;
  const specifier = declaration.moduleSpecifier;

  if (
    clause === undefined ||
    !ts.isStringLiteral(specifier) ||
    declaration.attributes !== undefined ||
    clause.phaseModifier === ts.SyntaxKind.DeferKeyword
  ) {
    return "kept";
  }

  const from = JSON.stringify(specifier.text);
  const typeOnly = clause.phaseModifier === ts.SyntaxKind.TypeKeyword;
  const typeWord = typeOnly ? "type " : "";
  const bindings: ImportBinding[] = [];

  const add = (imported: string, local: string, type: boolean, statement: string): void => {
    bindings.push({ key: JSON.stringify([specifier.text, imported, local, type]), statement });
  };

  if (clause.name !== undefined) {
    add("default", clause.name.text, typeOnly, `import ${typeWord}${clause.name.text} from ${from};`);
  }

  const named = clause.namedBindings;

  if (named !== undefined && ts.isNamespaceImport(named)) {
    add("*", named.name.text, typeOnly, `import ${typeWord}* as ${named.name.text} from ${from};`);
  }

  if (named !== undefined && ts.isNamedImports(named)) {
    for (const element of named.elements) {
      const imported = element.propertyName?.getText(sourceFile) ?? element.name.text;
      const type = typeOnly || element.isTypeOnly;
      const alias = element.propertyName === undefined ? element.name.text : `${imported} as ${element.name.text}`;

      add(imported, element.name.text, type, `import { ${type ? "type " : ""}${alias} } from ${from};`);
    }
  }

  return bindings;
}

/**
 * The code of a block whose import bindings that `bound` holds (the bindings of the blocks above it in the file) are
 * dropped. A dropped declaration keeps its lines as blank lines, and a declaration that keeps some of its bindings is
 * written as one-name imports on its first line, so each other line of the block keeps its line number. Pure.
 */
export function mergeImports(code: readonly string[], bound: ReadonlySet<string>): MergedImports {
  if (code.length === 0) {
    return { code, bindings: new Set() };
  }

  const text = code.join("\n");
  const sourceFile = ts.createSourceFile("block.ts", text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  const bindings = new Set<string>();
  let merged = "";
  let position = 0;

  for (const statement of sourceFile.statements) {
    if (!ts.isImportDeclaration(statement)) {
      continue;
    }

    const declared = bindingsOf(statement, sourceFile);

    if (declared === "kept") {
      continue;
    }

    const fresh = declared.filter((binding) => !bound.has(binding.key));

    for (const binding of declared) {
      bindings.add(binding.key);
    }

    if (fresh.length === declared.length) {
      continue;
    }

    const start = statement.getStart(sourceFile);
    const lineBreaks = text.slice(start, statement.getEnd()).split("\n").length - 1;

    merged += `${text.slice(position, start)}${fresh.map((binding) => binding.statement).join(" ")}${"\n".repeat(lineBreaks)}`;
    position = statement.getEnd();
  }

  return { code: `${merged}${text.slice(position)}`.split("\n"), bindings };
}

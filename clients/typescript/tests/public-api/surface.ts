// The top types (`any`, `unknown`) of the published declarations, found by a walk of their syntax trees with the
// TypeScript 6.0 API (TypeScript 7 has no compiler API).
//
// - `any`: every place in every declaration file of the package.
// - `unknown`: every place in the public surface: the declarations that an entry point exports, and every declaration
//   of the package that they name, to any depth. A declaration that no entry point reaches is not public: the
//   `exports` map allows no deep import.
//
// A place has a path (the names of the declarations around it, outermost first: `Wire.default.value`) and a form (the
// text of the type that the innermost named declaration holds).

import ts from "@typescript/typescript6";

import type { Result } from "../../horsies/src/wire/result.ts";
import type { BuiltPackage } from "./package.ts";

export type TopType = "any" | "unknown";

/** One place of a top type in the published declarations. */
export type TopTypePlace = {
  readonly keyword: TopType;
  readonly path: string;
  readonly form: string;
  readonly file: string;
  readonly line: number;
};

/**
 * The bounds of the walk: the declarations of the public surface that it visits, and the depth and the nodes of one
 * syntax tree.
 */
export type SurfaceBounds = { readonly declarations: number; readonly depth: number; readonly nodes: number };

/** The bounds of the walk of the published declarations. */
export const SURFACE_BOUNDS: SurfaceBounds = { declarations: 20_000, depth: 400, nodes: 1_000_000 };

export type SurfaceWalkFailed =
  | { readonly kind: "EntryNotFound"; readonly file: string }
  | { readonly kind: "TooManyDeclarations"; readonly bound: number }
  | { readonly kind: "TooDeep"; readonly file: string; readonly bound: number }
  | { readonly kind: "TooManyNodes"; readonly file: string; readonly bound: number };

/** The declaration program of a built package: its entry files and every file that they import. Reads files. */
export function declarationProgram(built: BuiltPackage): ts.Program {
  return ts.createProgram({
    rootNames: built.entryPoints.map((entry) => `${built.folder}/${entry.typesFile}`),
    options: {
      strict: true,
      module: ts.ModuleKind.NodeNext,
      moduleResolution: ts.ModuleResolutionKind.NodeNext,
      lib: ["lib.es2025.d.ts", "lib.esnext.temporal.d.ts"],
      types: [],
      noEmit: true,
      skipLibCheck: true,
    },
  });
}

/** Whether a source file is a declaration file of the package (not of a dependency or of the compiler's lib). */
function isPackageFile(file: ts.SourceFile, built: BuiltPackage): boolean {
  return file.fileName.startsWith(`${built.folder}/dist/`);
}

/** The nodes of a tree, depth first, within the depth and node bounds. Pure. */
function nodesOf(root: ts.Node, file: ts.SourceFile, bounds: SurfaceBounds): Result<readonly ts.Node[], SurfaceWalkFailed> {
  const nodes: ts.Node[] = [];
  const stack: (readonly [ts.Node, number])[] = [[root, 0]];

  for (let entry = stack.pop(); entry !== undefined; entry = stack.pop()) {
    const [node, depth] = entry;

    if (depth > bounds.depth) {
      return { ok: false, error: { kind: "TooDeep", file: file.fileName, bound: bounds.depth } };
    }

    if (nodes.length >= bounds.nodes) {
      return { ok: false, error: { kind: "TooManyNodes", file: file.fileName, bound: bounds.nodes } };
    }

    nodes.push(node);
    ts.forEachChild(node, (child) => {
      stack.push([child, depth + 1]);
    });
  }

  return { ok: true, value: nodes };
}

/** The name of a node that declares a name, or `undefined` for any other node. */
function declaredName(node: ts.Node, file: ts.SourceFile): string | undefined {
  switch (node.kind) {
    case ts.SyntaxKind.CallSignature:
      return "(call)";
    case ts.SyntaxKind.ConstructSignature:
    case ts.SyntaxKind.Constructor:
      return "(new)";
    case ts.SyntaxKind.IndexSignature:
      return "[index]";
    default:
      break;
  }

  if (!ts.isTypeAliasDeclaration(node) && !ts.isInterfaceDeclaration(node) && !ts.isClassDeclaration(node) &&
    !ts.isFunctionDeclaration(node) && !ts.isVariableDeclaration(node) && !ts.isModuleDeclaration(node) &&
    !ts.isEnumDeclaration(node) && !ts.isEnumMember(node) && !ts.isMethodSignature(node) &&
    !ts.isMethodDeclaration(node) && !ts.isPropertySignature(node) && !ts.isPropertyDeclaration(node) &&
    !ts.isParameter(node) && !ts.isTypeParameterDeclaration(node) && !ts.isGetAccessorDeclaration(node) &&
    !ts.isSetAccessorDeclaration(node)) {
    return undefined;
  }

  return node.name?.getText(file) ?? "(unnamed)";
}

/** The path and the form of a place: the declared names around `node`, and the type that the innermost one holds. */
function placeOf(keyword: TopType, node: ts.Node, file: ts.SourceFile, folder: string): TopTypePlace {
  const names: string[] = [];
  let form: ts.Node = node;
  let formFound = false;

  for (let child: ts.Node = node, parent = node.parent; !ts.isSourceFile(parent); child = parent, parent = parent.parent) {
    const name = declaredName(parent, file);

    if (name === undefined) {
      continue;
    }

    names.unshift(name);

    if (!formFound) {
      form = child;
      formFound = true;
    }
  }

  return {
    keyword,
    path: names.join(".") || "(module)",
    form: form.getText(file).replaceAll(/\s+/g, " "),
    file: file.fileName.slice(folder.length + 1),
    line: file.getLineAndCharacterOfPosition(node.getStart(file)).line + 1,
  };
}

const KEYWORDS: Readonly<Record<TopType, ts.SyntaxKind>> = {
  any: ts.SyntaxKind.AnyKeyword,
  unknown: ts.SyntaxKind.UnknownKeyword,
};

/** The places of `keyword` in a tree. */
function placesIn(
  keyword: TopType,
  root: ts.Node,
  built: BuiltPackage,
  bounds: SurfaceBounds,
): Result<readonly TopTypePlace[], SurfaceWalkFailed> {
  const file = root.getSourceFile();
  const nodes = nodesOf(root, file, bounds);

  if (!nodes.ok) {
    return nodes;
  }

  const places = nodes.value
    .filter((node) => node.kind === KEYWORDS[keyword])
    .map((node) => placeOf(keyword, node, file, built.folder));

  return { ok: true, value: places };
}

/** Every `any` in every declaration file of the package. */
export function anyPlaces(
  program: ts.Program,
  built: BuiltPackage,
  bounds: SurfaceBounds = SURFACE_BOUNDS,
): Result<readonly TopTypePlace[], SurfaceWalkFailed> {
  const places: TopTypePlace[] = [];

  // The checker binds each file; the binder sets the parent of each node, which placeOf reads.
  void program.getTypeChecker();

  for (const file of program.getSourceFiles().filter((source) => isPackageFile(source, built))) {
    const found = placesIn("any", file, built, bounds);

    if (!found.ok) {
      return found;
    }

    places.push(...found.value);
  }

  return { ok: true, value: places };
}

/** The statement of the source file that holds `node`. */
function topLevelOf(node: ts.Node): ts.Node {
  let statement = node;

  while (!ts.isSourceFile(statement.parent)) {
    statement = statement.parent;
  }

  return statement;
}

/** The node of each name that a declaration's tree refers to: a type, a `typeof` value, a base, a symbol key. */
function referenceNames(nodes: readonly ts.Node[]): readonly ts.Node[] {
  return nodes.flatMap((node): readonly ts.Node[] => {
    if (ts.isTypeReferenceNode(node)) {
      return [node.typeName];
    }

    if (ts.isExpressionWithTypeArguments(node)) {
      return [node.expression];
    }

    if (ts.isTypeQueryNode(node)) {
      return [node.exprName];
    }

    if (ts.isComputedPropertyName(node)) {
      return [node.expression];
    }

    if (ts.isImportTypeNode(node) && node.qualifier !== undefined) {
      return [node.qualifier];
    }

    return [];
  });
}

/** The package declarations (as top-level statements) of a symbol, through an import or export alias. */
function packageStatementsOf(symbol: ts.Symbol, checker: ts.TypeChecker, built: BuiltPackage): readonly ts.Node[] {
  const target = (symbol.flags & ts.SymbolFlags.Alias) === 0 ? symbol : checker.getAliasedSymbol(symbol);

  return (target.declarations ?? [])
    .filter((declaration) => isPackageFile(declaration.getSourceFile(), built))
    .map((declaration) => topLevelOf(declaration));
}

/** The statements that the entry points export. */
function exportedStatements(program: ts.Program, built: BuiltPackage): Result<readonly ts.Node[], SurfaceWalkFailed> {
  const checker = program.getTypeChecker();
  const statements: ts.Node[] = [];

  for (const entry of built.entryPoints) {
    const fileName = `${built.folder}/${entry.typesFile}`;
    const file = program.getSourceFile(fileName);
    const moduleSymbol = file === undefined ? undefined : checker.getSymbolAtLocation(file);

    if (moduleSymbol === undefined) {
      return { ok: false, error: { kind: "EntryNotFound", file: fileName } };
    }

    for (const exported of checker.getExportsOfModule(moduleSymbol)) {
      statements.push(...packageStatementsOf(exported, checker, built));
    }
  }

  return { ok: true, value: statements };
}

/** The statements of the public surface: the exported ones and every package statement that they name. */
export function surfaceStatements(
  program: ts.Program,
  built: BuiltPackage,
  bounds: SurfaceBounds = SURFACE_BOUNDS,
): Result<readonly ts.Node[], SurfaceWalkFailed> {
  const checker = program.getTypeChecker();
  const exported = exportedStatements(program, built);

  if (!exported.ok) {
    return exported;
  }

  const visited = new Set<ts.Node>(exported.value);
  const queue = [...visited];

  for (let index = 0; index < queue.length; index += 1) {
    if (index >= bounds.declarations) {
      return { ok: false, error: { kind: "TooManyDeclarations", bound: bounds.declarations } };
    }

    const statement = queue[index];
    const nodes = statement === undefined ? { ok: true as const, value: [] } : nodesOf(statement, statement.getSourceFile(), bounds);

    if (!nodes.ok) {
      return nodes;
    }

    for (const name of referenceNames(nodes.value)) {
      const symbol = checker.getSymbolAtLocation(name);
      const named = symbol === undefined ? [] : packageStatementsOf(symbol, checker, built);

      for (const next of named.filter((candidate) => !visited.has(candidate))) {
        visited.add(next);
        queue.push(next);
      }
    }
  }

  return { ok: true, value: queue };
}

/** Every `unknown` in the public surface. */
export function unknownPlaces(
  program: ts.Program,
  built: BuiltPackage,
  bounds: SurfaceBounds = SURFACE_BOUNDS,
): Result<readonly TopTypePlace[], SurfaceWalkFailed> {
  const statements = surfaceStatements(program, built, bounds);

  if (!statements.ok) {
    return statements;
  }

  const places: TopTypePlace[] = [];

  for (const statement of statements.value) {
    const found = placesIn("unknown", statement, built, bounds);

    if (!found.ok) {
      return found;
    }

    places.push(...found.value);
  }

  return { ok: true, value: places };
}

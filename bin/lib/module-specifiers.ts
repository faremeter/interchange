import { parseSync, Visitor, type Node } from "oxc-parser";

export type ModuleDetection = "auto" | "legacy" | "force";

type ModuleSpecifier = {
  start: number;
  end: number;
  text: string;
  kind: "import" | "require" | "augmentation";
};

/** Collect literal module references, including declaration-file imports.
 *  Ambient module definitions are not dependencies, but augmentations of
 *  existing modules are. Offsets include the quotes for text rewriting. */
export function collectModuleSpecifiers(
  sourceText: string,
  fileName: string,
  moduleDetection: ModuleDetection,
): ModuleSpecifier[] {
  const parsed = parseSync(fileName, sourceText);
  if (parsed.errors.length > 0) {
    throw new Error(
      `Cannot parse ${fileName}: ${parsed.errors.map((error) => error.message).join("; ")}`,
    );
  }

  // Forced module detection applies to source files, not .d.ts declarations.
  // Import-equals is a TypeScript module indicator that Oxc's ESM flag omits.
  const externalModule =
    parsed.module.hasModuleSyntax ||
    parsed.program.body.some(
      (node) =>
        node.type === "TSImportEqualsDeclaration" &&
        node.moduleReference.type === "TSExternalModuleReference",
    ) ||
    (!/\.d\.[cm]?ts$/.test(fileName) &&
      (moduleDetection === "force" ||
        (moduleDetection === "auto" && /\.[cm][jt]s$/.test(fileName))));
  const augmentations = new Set<number>();
  for (const node of parsed.program.body) {
    if (node.type !== "TSModuleDeclaration") continue;
    if (externalModule) {
      augmentations.add(node.start);
    } else if (staticText(node.id) !== undefined && node.body != null) {
      // A module nested inside a top-level ambient definition augments an
      // existing module, even though the containing file is a script.
      for (const child of node.body.body) {
        if (child.type === "TSModuleDeclaration")
          augmentations.add(child.start);
      }
    }
  }

  const refs: ModuleSpecifier[] = [];
  const record = (
    node: Node | null | undefined,
    kind: ModuleSpecifier["kind"] = "import",
  ): void => {
    const text = staticText(node);
    if (node && text !== undefined) {
      refs.push({ start: node.start, end: node.end, text, kind });
    }
  };
  new Visitor({
    ImportDeclaration: (node) => record(node.source),
    ExportNamedDeclaration: (node) => record(node.source),
    ExportAllDeclaration: (node) => record(node.source),
    ImportExpression: (node) => record(node.source),
    TSImportType: (node) => record(node.source),
    TSExternalModuleReference: (node) => record(node.expression),
    TSModuleDeclaration: (node) => {
      if (augmentations.has(node.start)) record(node.id, "augmentation");
    },
    CallExpression: (node) => {
      if (node.callee.type === "Identifier" && node.callee.name === "require") {
        record(node.arguments[0], "require");
      }
    },
  }).visit(parsed.program);
  return refs;
}

/** The value of a string literal or of a template literal without
 *  substitutions, which `import()` and `require()` also accept. */
function staticText(node: Node | null | undefined): string | undefined {
  if (node?.type === "Literal" && typeof node.value === "string") {
    return node.value;
  }
  if (node?.type === "TemplateLiteral" && node.expressions.length === 0) {
    return node.quasis[0]?.value.cooked ?? undefined;
  }
  return undefined;
}

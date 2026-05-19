#!/usr/bin/env bun

import { existsSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { createApp } from "@intx/hub-api";
import { setup, getLogger } from "@intx/log";
import * as allTypes from "@intx/types";

await setup({ dev: true });
const log = getLogger(["gen-api-docs"]);

const check = process.argv.includes("--check");
const repoRoot = resolve(import.meta.dirname ?? ".", "..");

// ---------------------------------------------------------------------------
// 1. Build type metadata by scanning packages/types/src/*.ts for export names,
//    then pulling expression and JSON Schema from the runtime ArkType objects.
// ---------------------------------------------------------------------------

type ArkTypeValue = {
  expression: string;
  toJsonSchema: () => Record<string, unknown>;
};

function isArkType(v: unknown): v is ArkTypeValue {
  if (v == null) return false;
  if (typeof v !== "object" && typeof v !== "function") return false;
  return (
    "expression" in v &&
    typeof (v as Record<string, unknown>)["expression"] === "string" &&
    "toJsonSchema" in v &&
    typeof (v as Record<string, unknown>)["toJsonSchema"] === "function"
  );
}

type TypeInfo = {
  name: string;
  sourceFile: string;
  expression: string;
  description: string | null;
  fieldDescriptions: Record<string, string>;
};

const typesByName = new Map<string, TypeInfo>();
const schemaToNames = new Map<string, string[]>();

// Build a lookup of arktype values by name from the module namespace.
const allTypesMap = new Map<string, ArkTypeValue>(
  Object.entries(allTypes).flatMap(([n, v]) => (isArkType(v) ? [[n, v]] : [])),
);

const typesDir = resolve(repoRoot, "packages/types/src");
const typeFiles = readdirSync(typesDir).filter(
  (f) => f.endsWith(".ts") && f !== "index.ts",
);

for (const file of typeFiles) {
  const content = readFileSync(resolve(typesDir, file), "utf-8");
  const exportNames = [...content.matchAll(/export const (\w+)/g)]
    .map((m) => m[1])
    .filter((n): n is string => n != null);
  for (const name of exportNames) {
    const t = allTypesMap.get(name);
    if (t === undefined) continue;

    const js = t.toJsonSchema();
    delete js["$schema"];

    const description =
      typeof js["description"] === "string" ? js["description"] : null;

    const fieldDescriptions: Record<string, string> = {};
    const rawProperties = js["properties"];
    if (typeof rawProperties === "object" && rawProperties !== null) {
      for (const [field, prop] of Object.entries(rawProperties)) {
        if (
          typeof prop === "object" &&
          prop !== null &&
          "description" in prop
        ) {
          if (typeof prop.description === "string") {
            fieldDescriptions[field] = prop.description;
          }
        }
      }
    }

    typesByName.set(name, {
      name,
      sourceFile: `packages/types/src/${file}`,
      expression: t.expression,
      description,
      fieldDescriptions,
    });

    const key = JSON.stringify(js);
    const existing = schemaToNames.get(key);
    if (existing) {
      existing.push(name);
    } else {
      schemaToNames.set(key, [name]);
    }
  }
}

// ---------------------------------------------------------------------------
// 2. Schema matching: map an OpenAPI JSON Schema back to a type name
// ---------------------------------------------------------------------------

type JsonSchema = Record<string, unknown> & {
  type?: string;
  items?: JsonSchema;
};

function matchSchema(schema: JsonSchema, tagHint?: string): string | null {
  const target = schema.type === "array" ? schema.items : schema;
  if (!target) return null;
  const key = JSON.stringify(target);
  const candidates = schemaToNames.get(key);
  if (!candidates) return null;
  if (candidates.length === 1) return candidates[0] ?? null;

  // Resolve collisions: prefer the candidate whose source file matches the tag
  if (tagHint) {
    const tagLower = tagHint.toLowerCase().replace(/s$/, "");
    const preferred = candidates.find((c) => {
      const info = typesByName.get(c);
      return info?.sourceFile.includes(tagLower);
    });
    if (preferred) return preferred;
  }
  return candidates[0] ?? null;
}

function formatTypeName(schema: JsonSchema, tagHint?: string): string {
  const name = matchSchema(schema, tagHint);
  if (!name) return "unknown";
  if (schema.type === "array") return `${name}[]`;
  return name;
}

// ---------------------------------------------------------------------------
// 3. Load the Hono app and fetch the OpenAPI spec
// ---------------------------------------------------------------------------

const app = createApp({
  // Stub dependencies — this script only calls /openapi.json which uses
  // route metadata, not runtime services. These stubs are never called.
  getSession: async () => null,
  authHandler: () => new Response("", { status: 404 }),
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- stub; only /openapi.json is called
  db: {} as never,
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- stub; only /openapi.json is called
  sidecarRouter: {} as never,
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- stub; only /openapi.json is called
  sessionService: {} as never,
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- stub; only /openapi.json is called
  eventCollectors: {} as never,
});
const res = await app.request("/openapi.json");

if (!res.ok) {
  log.error("Failed to fetch OpenAPI spec: {status}", { status: res.status });
  process.exit(1);
}

type OpenAPIParam = {
  name: string;
  in: string;
  required?: boolean;
  schema?: { type?: string; enum?: string[] };
};

type OpenAPIResponse = {
  description?: string;
  content?: Record<string, { schema?: JsonSchema }>;
};

type OpenAPIOperation = {
  summary?: string;
  description?: string;
  tags?: string[];
  parameters?: OpenAPIParam[];
  requestBody?: { content?: Record<string, { schema?: JsonSchema }> };
  responses?: Record<string, OpenAPIResponse>;
};

// eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- spec shape is controlled by our own app; full OpenAPI schema validation is overkill for a dev script
const spec = (await res.json()) as {
  paths: Record<string, Record<string, OpenAPIOperation>>;
};

// ---------------------------------------------------------------------------
// 4. Extract and organize endpoints
// ---------------------------------------------------------------------------

type Endpoint = {
  method: string;
  path: string;
  summary: string;
  description: string | null;
  tag: string;
  queryParams: string[];
  bodyType: string | null;
  responses: { code: string; description: string; typeName: string | null }[];
};

const endpoints: Endpoint[] = [];
const usedTypes = new Set<string>();

function trackType(schema: JsonSchema, tagHint?: string): void {
  const name = matchSchema(schema, tagHint);
  if (name) usedTypes.add(name);
}

for (const [openApiPath, methods] of Object.entries(spec.paths)) {
  const path = openApiPath.replace(/\{(\w+)\}/g, ":$1");

  for (const [method, op] of Object.entries(methods)) {
    const tag = op.tags?.[0] ?? "Other";

    // Query params
    const queryParams: string[] = [];
    for (const param of op.parameters ?? []) {
      if (param.in !== "query") continue;
      let paramStr = param.name;
      if (!param.required) paramStr += "?";
      if (param.schema?.enum) {
        paramStr += `: ${param.schema.enum.join("|")}`;
      }
      queryParams.push(paramStr);
    }

    // Request body
    let bodyType: string | null = null;
    const bodySchema = op.requestBody?.content?.["application/json"]?.schema;
    if (bodySchema) {
      trackType(bodySchema, tag);
      bodyType = formatTypeName(bodySchema, tag);
    }

    // Responses
    const responses: Endpoint["responses"] = [];
    for (const [code, resp] of Object.entries(op.responses ?? {})) {
      const jsonSchema = resp.content?.["application/json"]?.schema;
      const sseContent = resp.content?.["text/event-stream"];
      let typeName: string | null = null;

      if (jsonSchema) {
        trackType(jsonSchema, tag);
        typeName = formatTypeName(jsonSchema, tag);
      } else if (sseContent) {
        typeName = "SSE stream";
      }

      responses.push({
        code,
        description: resp.description ?? "",
        typeName,
      });
    }

    endpoints.push({
      method: method.toUpperCase(),
      path,
      summary: op.summary ?? "",
      description:
        op.description && op.description !== op.summary ? op.description : null,
      tag,
      queryParams,
      bodyType,
      responses,
    });
  }
}

// Group by tag, preserving insertion order
const byTag = new Map<string, Endpoint[]>();
for (const ep of endpoints) {
  const group = byTag.get(ep.tag);
  if (group) {
    group.push(ep);
  } else {
    byTag.set(ep.tag, [ep]);
  }
}

// ---------------------------------------------------------------------------
// 5. Emit the document
// ---------------------------------------------------------------------------

const lines: string[] = [];

function emit(line = "") {
  lines.push(line);
}

emit(
  "<!-- This file is autogenerated by bin/gen-api-docs.ts. Do not edit by hand. -->",
);
emit();
emit("# Interchange Hub API");
emit();

// Endpoint index
emit("## Endpoint Index");
emit();
emit("| Method | Path | Summary |");
emit("| ------ | ---- | ------- |");
for (const ep of endpoints) {
  emit(`| ${ep.method} | ${ep.path} | ${ep.summary} |`);
}
emit();

// Grouped sections
for (const [tag, eps] of byTag) {
  emit(`## ${tag}`);
  emit();

  for (const ep of eps) {
    emit(`### ${ep.method} ${ep.path}`);
    emit(ep.summary);
    emit();

    if (ep.description) {
      emit(ep.description);
      emit();
    }

    if (ep.queryParams.length > 0) {
      emit(`Query: ${ep.queryParams.join(", ")}`);
      emit();
    }

    if (ep.bodyType) {
      emit(`Body: ${ep.bodyType}`);
      emit();
    }

    for (const r of ep.responses) {
      if (r.typeName) {
        emit(`${r.code}: ${r.typeName} -- ${r.description}`);
      } else {
        emit(`${r.code}: (no content) -- ${r.description}`);
      }
    }
    emit();
  }
}

// Type reference
emit("## Type Reference");
emit();

const sortedTypes = [...usedTypes].sort();
for (const name of sortedTypes) {
  const info = typesByName.get(name);
  if (!info) continue;
  emit(`### ${name}`);
  emit(`\`${info.expression}\``);
  emit(`Source: ${info.sourceFile}`);

  if (info.description) {
    emit();
    emit(info.description);
  }

  const fieldEntries = Object.entries(info.fieldDescriptions);
  if (fieldEntries.length > 0) {
    emit();
    for (const [field, desc] of fieldEntries) {
      emit(`**${field}**: ${desc}`);
    }
  }

  emit();
}

// ---------------------------------------------------------------------------
// 6. Write or check
// ---------------------------------------------------------------------------

const outPath = resolve(repoRoot, "docs/API.md");
const generated = lines.join("\n") + "\n";

if (check) {
  if (!existsSync(outPath)) {
    log.error("docs/API.md does not exist. Run: bun run docs");
    process.exit(1);
  }
  const current = readFileSync(outPath, "utf-8");
  if (generated !== current) {
    log.error("docs/API.md is stale. Run: bun run docs");
    process.exit(1);
  }
  log.info("docs/API.md is up to date");
} else {
  writeFileSync(outPath, generated);
  log.info("Wrote {path}", { path: outPath });
}

import {
  resolver as baseResolver,
  describeRoute,
  validator,
} from "hono-openapi";
import type { OpenAPIV3_1 } from "openapi-types";
import * as allTypes from "@intx/types";

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

const typeNames = new Map<unknown, string>();
for (const [name, value] of Object.entries(allTypes)) {
  if (isArkType(value)) {
    typeNames.set(value, name);
  }
}

type ResolverResult = ReturnType<typeof baseResolver>;

/**
 * Wraps hono-openapi's resolver to register known @intx/types
 * exports as named components in the OpenAPI spec. Types not found in
 * the registry fall through to the default inline behavior.
 */
export function resolver(
  schema: Parameters<typeof baseResolver>[0],
): ResolverResult {
  const base = baseResolver(schema);
  const name = typeNames.get(schema);
  if (!name) return base;

  return {
    ...base,
    async toOpenAPISchema(options?: Record<string, unknown>): Promise<{
      schema: OpenAPIV3_1.SchemaObject;
      components: OpenAPIV3_1.ComponentsObject | undefined;
    }> {
      const result = await base.toOpenAPISchema(options);
      return {
        // $ref objects are valid SchemaObjects per OpenAPI 3.1 but the
        // openapi-types definition doesn't model the $ref-only form.
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- openapi-types SchemaObject doesn't model $ref-only form; valid per OpenAPI 3.1
        schema: {
          $ref: `#/components/schemas/${name}`,
        } as unknown as OpenAPIV3_1.SchemaObject,
        components: { schemas: { [name]: result.schema } },
      };
    },
  } as ResolverResult;
}

/**
 * Builds a single OpenAPI response object whose body is a JSON document
 * described by the given arktype schema. Collapses the repeated
 * `content: { "application/json": { schema: resolver(...) } }` wrapper
 * that every route's `responses` map carries.
 *
 * Uses the raw hono-openapi resolver (not the `$ref`-rewriting wrapper
 * above): the docs generator matches response schemas against full JSON
 * schemas, so a response schema must stay expanded.
 */
export function jsonResponse(
  description: string,
  schema: Parameters<typeof baseResolver>[0],
): {
  description: string;
  content: { "application/json": { schema: ResolverResult } };
} {
  return {
    description,
    content: { "application/json": { schema: baseResolver(schema) } },
  };
}

export { describeRoute, validator };

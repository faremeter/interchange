import {
  resolver as baseResolver,
  describeRoute,
  validator,
} from "hono-openapi";
import type { OpenAPIV3_1 } from "openapi-types";
import * as allTypes from "@interchange/types";

type ArkTypeValue = {
  expression: string;
  toJsonSchema: () => Record<string, unknown>;
};

function isArkType(v: unknown): v is ArkTypeValue {
  return (
    v != null &&
    typeof (v as ArkTypeValue).expression === "string" &&
    typeof (v as ArkTypeValue).toJsonSchema === "function"
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
 * Wraps hono-openapi's resolver to register known @interchange/types
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
        schema: {
          $ref: `#/components/schemas/${name}`,
        } as unknown as OpenAPIV3_1.SchemaObject,
        components: { schemas: { [name]: result.schema } },
      };
    },
  } as ResolverResult;
}

export { describeRoute, validator };

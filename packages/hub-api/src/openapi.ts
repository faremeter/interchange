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

export { describeRoute, validator };

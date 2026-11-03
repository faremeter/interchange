import { type, type Type } from "arktype";

import type { ConditionContext, ConditionEvaluator } from "./types";

/**
 * Build a condition evaluator whose shape check and evaluation share one
 * schema. The returned `validate` reports what the schema rejects. The
 * returned `evaluate` parses with the same schema, throws on the same input,
 * and passes the parsed body to the `evaluate` callback. A stored body that
 * bypassed `validate` therefore still fails loud at evaluation time.
 *
 * Every message starts with `name`, which is the condition's registry key.
 */
export function createConditionEvaluator<S extends Type>(
  name: string,
  schema: S,
  evaluate: (
    value: S["infer"],
    ctx: ConditionContext,
  ) => boolean | Promise<boolean>,
): ConditionEvaluator {
  // One line per body, so a caller can join several problems into one
  // message.
  const describe = (errors: type.errors) =>
    `${name}: ${errors.map((e) => e.message).join("; ")}`;

  return {
    validate(value) {
      const parsed = schema(value);
      return parsed instanceof type.errors ? describe(parsed) : null;
    },
    evaluate(value, ctx) {
      const parsed = schema(value);
      if (parsed instanceof type.errors) {
        throw new Error(describe(parsed));
      }
      return evaluate(parsed, ctx);
    },
  };
}

// EffectContext factory.
//
// The capability- and ledger-checked handle an action handler uses to
// perform external effects. Both the runLocal host and the production
// host build their action EffectContext through this factory so the
// exactly-once contract -- authorize, then dedup per effect against the
// ledger -- lives in one place rather than being re-implemented per
// host.
//
// The effect key includes an intra-handler `effectId`, so a handler that
// performs several distinct effects (a clone AND a build AND a push)
// keys each one separately; a node-scoped key would collapse them and
// the ledger would dedup distinct effects against each other.

import { canonicalizeForHash } from "@intx/agent";

import type {
  AuthorizeContext,
  WorkflowAuthorizeFn,
} from "../authorize-context";
import type { EffectContext, EffectLedger } from "./env";

export interface EffectContextConfig {
  authorize: WorkflowAuthorizeFn;
  effects: EffectLedger;
  requires: readonly string[];
  authzContext: AuthorizeContext;
  input: unknown;
}

export function createEffectContext(
  config: EffectContextConfig,
): EffectContext {
  const { authorize, effects, requires, authzContext, input } = config;
  const allowed = new Set(requires);
  const { runId, stepId } = authzContext;
  if (runId === undefined || stepId === undefined) {
    throw new Error(
      "createEffectContext requires runId and stepId in the authz context",
    );
  }
  return {
    async perform({ effectId, capability, run }) {
      if (!allowed.has(capability)) {
        throw new Error(
          `action effect uses capability ${capability} which is not in its declared requires set`,
        );
      }
      // Fail closed: only an explicit allow proceeds. deny, ask, and a
      // null (no matching grant) all block, so the operator-approved
      // effect floor is enforced rather than advisory. The agent harness
      // makes the same call for tool use; an action has no harness, so
      // the EffectContext enforces the decision itself.
      const decision = await authorize(
        `effect:${capability}`,
        "invoke",
        authzContext,
      );
      if (decision.effect !== "allow") {
        throw new Error(
          `action effect ${capability} was not authorized (${String(decision.effect)})`,
        );
      }
      const effectKey = bytesToHex(
        canonicalizeForHash([runId, stepId, effectId, input]),
      );
      const hit = await effects.lookup(effectKey);
      if (hit !== undefined) {
        return hit.output;
      }
      const output = await run();
      await effects.record(effectKey, output);
      return output;
    },
  };
}

function bytesToHex(bytes: Uint8Array): string {
  let out = "";
  for (const b of bytes) {
    out += b.toString(16).padStart(2, "0");
  }
  return out;
}

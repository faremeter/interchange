import { type } from "arktype";

import type { GateType } from "./runtime";

/**
 * The kinds of external control signal an agent can suspend on and later
 * resume from. Exposed as both an arktype validator (so members are
 * iterable and can be composed into wire validators) and a derived
 * TypeScript union.
 */
export const signalKinds = ["approval"] as const;
export const SignalKind = type.enumerated(...signalKinds);
export type SignalKind = typeof SignalKind.infer;

/**
 * A resolved external control signal delivered to a suspended agent. The
 * `kind` discriminant selects the arm; today `approval` is the only arm.
 * `correlationId` ties the signal back to the suspension it resolves;
 * `payload` carries kind-specific data the resolver hands through opaquely.
 */
export const ControlSignal = type({
  correlationId: "string",
  kind: "'approval'",
  outcome: "'approved' | 'rejected' | 'timeout'",
  payload: "unknown",
});
export type ControlSignal = typeof ControlSignal.infer;

/**
 * Map a signal kind to the reactor gate type it clears. The default arm
 * calls `assertNever` so a newly added SignalKind that is not classified
 * here fails to type-check — a bare switch without a default does not.
 */
export function signalKindToGateType(kind: SignalKind): GateType {
  switch (kind) {
    case "approval":
      return "approval";
    default:
      return assertNever(kind);
  }
}

function assertNever(x: never): never {
  throw new Error(`Unclassified signal kind: ${JSON.stringify(x)}`);
}

/**
 * The reserved prefix that marks a signal name as an internal
 * control-plane channel rather than a free-form `awaitSignal` gate name.
 * The writer (`signalName`) and the reader (`correlationIdFromSignalName`)
 * share this one constant so the two cannot drift.
 */
const SIGNAL_NAME_PREFIX = "__signal__:";

/**
 * Construct the reserved, `__signal__:`-prefixed name under which a control
 * signal for `correlationId` is delivered. This reserves a name namespace
 * distinct from the user-authored workflow-signal names that flow through
 * `SignalDeliverFrame.signalName` in `./sidecar`: those are free-form
 * `awaitSignal` gate names chosen by workflow authors, whereas this helper
 * mints an internal name the control plane owns, so the two cannot collide.
 */
export function signalName(correlationId: string): string {
  return `${SIGNAL_NAME_PREFIX}${correlationId}`;
}

/**
 * Recover the `correlationId` from a reserved control-plane signal name
 * minted by `signalName`. Returns `undefined` for a name that does not
 * carry the reserved prefix (a free-form `awaitSignal` gate name), so a
 * caller can tell a control-plane channel apart from an author-chosen one.
 * Symmetric with `signalName`: `correlationIdFromSignalName(signalName(id))
 * === id`.
 */
export function correlationIdFromSignalName(name: string): string | undefined {
  if (!name.startsWith(SIGNAL_NAME_PREFIX)) return undefined;
  return name.slice(SIGNAL_NAME_PREFIX.length);
}

// A credentials capability that works only while a window is open.
//
// An action handler gets its credentials inside `perform`'s `run`, so every
// authenticated call is gated by the effect grant and recorded in the ledger.
// The window is guarded in two layers with this one wrapper: the invoker's
// view closes when the invoker disposes the invocation's capability, and each
// `perform` scope closes when its `run` settles. Every handle the guarded
// capability returns refuses use once its window closes, so a handle kept past
// its `run`, or used by a `perform` the handler left pending, fails closed.
//
// This is a guard against author mistakes, not an isolation boundary: a
// handler can still call `globalThis.fetch` itself.

import type { CredentialCapability, MediatedCredential } from "@intx/types";

/** The message a guarded capability refuses with once its window closes. */
export type CredentialGuardRefusal = (
  handle: string,
  verb: "resolve" | "use",
) => string;

/**
 * Wrap `capability` so `resolve`, and every handle it returns, refuses once
 * `isOpen` returns false.
 */
export function guardCredentialCapability(
  capability: CredentialCapability,
  isOpen: () => boolean,
  refusal: CredentialGuardRefusal,
): CredentialCapability {
  return {
    async resolve(handle) {
      if (!isOpen()) throw new Error(refusal(handle, "resolve"));
      const mediated = await capability.resolve(handle);
      if (!isOpen()) throw new Error(refusal(handle, "resolve"));
      return guardMediatedCredential(mediated, isOpen, () =>
        refusal(handle, "use"),
      );
    },
  };
}

/**
 * Wrap one shaped handle so its use refuses once `isOpen` returns false.
 *
 * `dispose` releases nothing. The capability shares one shaped handle across
 * every scope that resolves the same handle, so a forwarded `dispose` would
 * break a scope that still uses it. The invoker owns disposal.
 */
function guardMediatedCredential(
  mediated: MediatedCredential,
  isOpen: () => boolean,
  refusal: () => string,
): MediatedCredential {
  switch (mediated.kind) {
    case "http":
      return {
        kind: "http",
        fetch: (input, init) =>
          isOpen()
            ? mediated.fetch(input, init)
            : Promise.reject(new Error(refusal())),
        dispose() {
          /* the invoker owns disposal */
        },
      };
    default: {
      const _exhaustive: never = mediated.kind;
      throw new Error(
        `cannot guard a mediated credential of kind ${String(_exhaustive)}`,
      );
    }
  }
}

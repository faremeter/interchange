// Per-consumer assembly of the runtime `credentials` capability for a
// step's tool bundles.
//
// Each tool package is a distinct credential consumer, and Gate 2 -- the
// `{ tool }` condition on a `credential:{id}` / `use` grant -- is baked
// into each capability at construction: a capability shared across
// packages would be a confused-deputy hole (package A's grant would
// authorize package B's resolve). So one capability is built per
// package, and the bundle-invocation site layers each package's
// capability onto only that package's bundles.
//
// Two fail-closed invariants:
//   1. Material is read from the LIVE delivery cell on every use, never
//      snapshotted at shape time. A re-push that revokes access drops
//      the material; the shaped handle must starve immediately. This
//      live read is the revocation enforcement surface.
//   2. The declared-vs-bound reconcile runs per consumer against that
//      consumer's OWN bound set, so package A's declaration can never be
//      satisfied by a handle bound for package B.

import { toolConsumer } from "@intx/authz";
import type {
  CredentialMaterial,
  CredentialMaterialResolver,
  CredentialMaterialSource,
} from "@intx/types";
import type { GrantRule } from "@intx/types/authz";
import type { ToolCredentialDeclaration } from "@intx/types/package-json";
import type { CredentialDelivery } from "@intx/types/sidecar";

import {
  createCredentialCapability,
  reconcileDeclaredCredentials,
  type HostCredentialCapability,
  type ResolvedCredentialBinding,
} from "./credential-capability";
import type { CredentialProviderRegistry } from "./credential-providers";

/**
 * The mutable cell the control channel writes each delivery into. Held
 * structurally (no import from `@intx/workflow-host`) so this module
 * stays free of the transport package and a test can use a plain
 * object.
 */
export interface CredentialMaterialCell {
  readonly current: CredentialDelivery | null;
}

/**
 * What the per-consumer assembly needs beyond the step's factories: the
 * live material cell, the step's grants for Gate 2, and the provider
 * registry. The sidecar assembles this at the invoke-step boundary.
 */
export interface StepCredentialWiring {
  readonly materialCell: CredentialMaterialCell;
  /**
   * Resolve the step's grants for Gate 2. A thunk so grants are read
   * only when a capability is actually built: on the resume path a run
   * resumed by self-discovery may not have a grants snapshot yet, and a
   * toolless resume must not fault on that.
   */
  readonly resolveGrants: () => readonly GrantRule[];
  readonly providers: CredentialProviderRegistry;
}

/**
 * Build the credential capabilities for a step's tool factories, one
 * per distinct package, keyed by package name. A package with neither a
 * declared nor a bound credential gets no entry, so `resolve`
 * ("credentials") fails closed as "not provided by host". Every
 * returned capability owns a `dispose` the caller must run on teardown.
 *
 * The factory list is structural; the body reads only `packageName` and
 * `declaredCredentials` (importing `StepToolFactory` would violate
 * `bin/check-deps.ts`).
 */
export function buildCredentialCapabilities(
  factories: readonly {
    readonly packageName: string;
    readonly declaredCredentials: readonly ToolCredentialDeclaration[];
  }[],
  wiring: StepCredentialWiring,
): Map<string, HostCredentialCapability> {
  const byPackage = new Map<string, HostCredentialCapability>();
  const seenPackages = new Set<string>();
  // Resolve the step's grants at most once, and only if some package
  // actually needs a capability (see StepCredentialWiring.resolveGrants).
  let grants: readonly GrantRule[] | undefined;

  for (const stf of factories) {
    if (seenPackages.has(stf.packageName)) continue;
    seenPackages.add(stf.packageName);

    const consumer = toolConsumer(stf.packageName);
    const bindings = buildConsumerBindings(consumer, wiring.materialCell);

    // Fail the launch closed if a declared handle has no binding for
    // THIS consumer (invariant 2).
    reconcileDeclaredCredentials(
      consumer,
      stf.declaredCredentials,
      new Set(bindings.keys()),
    );

    if (stf.declaredCredentials.length === 0 && bindings.size === 0) {
      continue;
    }

    grants ??= wiring.resolveGrants();
    byPackage.set(
      stf.packageName,
      createCredentialCapability({
        consumer,
        bindings,
        providers: wiring.providers,
        grants: [...grants],
      }),
    );
  }

  return byPackage;
}

/**
 * The binding map one consumer's capability is built from: every
 * descriptor addressed to this consumer, resolved to its material. A
 * descriptor whose material is absent is a malformed delivery and fails
 * the build closed.
 */
function buildConsumerBindings(
  consumer: string,
  cell: CredentialMaterialCell,
): Map<string, ResolvedCredentialBinding> {
  const bindings = new Map<string, ResolvedCredentialBinding>();
  const delivery = cell.current;
  if (delivery === null) return bindings;

  for (const descriptor of delivery.bindings) {
    if (descriptor.consumer !== consumer) continue;

    const material = delivery.materials.find(
      (entry) => entry.credentialId === descriptor.credentialId,
    );
    if (material === undefined) {
      // A descriptor with no material is a delivery bug; refuse at build
      // rather than at first resolve.
      throw new Error(
        `credential delivery is malformed: descriptor for handle "${descriptor.handle}" (consumer ${consumer}) references credential ${descriptor.credentialId} but the delivery carries no material for it`,
      );
    }

    // Capture provider key and origin at build; the secret is read live
    // per use.
    bindings.set(descriptor.handle, {
      credentialId: descriptor.credentialId,
      providerKey: material.providerKey,
      origin: material.origin,
      readCurrentMaterial: makeReadCurrentMaterial({
        cell,
        credentialId: descriptor.credentialId,
        providerKey: material.providerKey,
        origin: material.origin,
        consumer,
      }),
    });
  }

  return bindings;
}

/**
 * The rotation/revocation indirection a shaped handle reads through:
 * looks the material up in the live cell on every call (invariant 1),
 * failing closed when the material is gone and asserting the
 * provider/origin have not drifted under the shaped handle.
 */
function makeReadCurrentMaterial(args: {
  cell: CredentialMaterialCell;
  credentialId: string;
  providerKey: string;
  origin: string;
  consumer: string;
}): CredentialMaterialSource {
  const { cell, credentialId, providerKey, origin, consumer } = args;
  return () => {
    const delivery = cell.current;
    if (delivery === null) {
      throw new Error(
        `credential material for ${credentialId} (consumer ${consumer}) is not available: the delivery cell is empty`,
      );
    }
    const material = delivery.materials.find(
      (entry) => entry.credentialId === credentialId,
    );
    if (material === undefined) {
      throw new Error(
        `credential material for ${credentialId} (consumer ${consumer}) is no longer delivered: a re-push dropped it (rotated away or revoked)`,
      );
    }
    // A rotation changes the secret, not the provider or origin. A
    // divergent live entry means the handle would authenticate somewhere
    // it was not pinned to; surface it rather than follow the change.
    if (material.providerKey !== providerKey || material.origin !== origin) {
      throw new Error(
        `credential ${credentialId} changed provider/origin under an already-shaped handle (${providerKey}@${origin} -> ${material.providerKey}@${material.origin}); a shaped handle cannot follow that change`,
      );
    }
    return { secret: material.secret };
  };
}

/**
 * Generic inference credential resolver over the live cell: resolves a
 * secret by `credentialId`, failing closed when the cell is empty or
 * the credential is absent. Unlike `makeReadCurrentMaterial` it is
 * keyed by `credentialId` at call time and pins no provider/origin -- an
 * inference request authenticates to the source's own `baseURL`, so
 * there is no shaped handle to protect.
 */
export function createInferenceCredentialResolver(
  cell: CredentialMaterialCell,
): CredentialMaterialResolver {
  return (credentialId: string): CredentialMaterial => {
    const delivery = cell.current;
    if (delivery === null) {
      throw new Error(
        `inference credential material for ${credentialId} is not available: the delivery cell is empty`,
      );
    }
    const material = delivery.materials.find(
      (entry) => entry.credentialId === credentialId,
    );
    if (material === undefined) {
      throw new Error(
        `inference credential material for ${credentialId} is no longer delivered: a re-push dropped it (rotated away or revoked)`,
      );
    }
    return { secret: material.secret };
  };
}

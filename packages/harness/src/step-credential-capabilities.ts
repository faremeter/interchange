// Per-consumer assembly of the runtime `credentials` capability for a step's
// tool bundles.
//
// Each tool package is a distinct credential consumer (`toolConsumer(
// packageName)`), and Gate 2 -- the `{ tool }` condition on a
// `credential:{id}` / `use` grant -- is baked into each capability at
// construction, so a single capability shared across a step's packages
// would be a confused-deputy hole (package A's grant would authorize
// package B's resolve). This module therefore builds ONE capability per
// package and the bundle-invocation site layers each package's capability
// onto only that package's bundles.
//
// Two invariants, both fail-closed:
//   1. Material is read from the LIVE delivery cell on every use, never
//      snapshotted at shape time. A re-push that revokes a consumer's
//      access DROPS the material; the already-shaped handle must starve
//      the instant that happens. This live read is the revocation
//      enforcement surface.
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
 * The mutable cell the control channel writes each credential delivery
 * into. Held structurally (rather than importing
 * `CredentialMaterialRef` from `@intx/workflow-host`) so this module
 * stays free of the transport package and a test can drive it with a
 * plain object.
 */
export interface CredentialMaterialCell {
  readonly current: CredentialDelivery | null;
}

/**
 * Everything the per-consumer assembly needs beyond the step's factories:
 * the live material cell, the step's grants for Gate 2, and the provider
 * registry that shapes a credential into a mediated handle. The sidecar
 * assembles this at the invoke-step boundary.
 */
export interface StepCredentialWiring {
  readonly materialCell: CredentialMaterialCell;
  /**
   * Resolves the step's grants for Gate 2. A thunk rather than a resolved
   * array, so the grants are read only when a capability is actually
   * built: a step with no credential-consuming package never touches
   * them. This matters on the resume path -- a run resumed by
   * self-discovery does not pass through the pre-trigger grants barrier,
   * so the grants snapshot may not be present yet; a toolless resume
   * must not fault on that.
   */
  readonly resolveGrants: () => readonly GrantRule[];
  readonly providers: CredentialProviderRegistry;
}

/**
 * Build the credential capabilities for a step's tool factories, one per
 * distinct package, keyed by package name.
 *
 * A package with neither a declared nor a bound credential gets no entry:
 * its bundles keep the base capabilities bag untouched, so
 * `resolve("credentials")` fails closed as "not provided by host".
 * Every returned capability owns a `dispose` the caller must run on
 * teardown.
 *
 * The factory list is structural; the body reads only `packageName` and
 * `declaredCredentials`. Importing `StepToolFactory` or
 * `@intx/tool-packaging` is rejected because `bin/check-deps.ts` counts
 * `import type`.
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

    // Fail the launch closed if the package declares a handle no binding
    // resolves for it, checked against THIS consumer's bound set only
    // (invariant 2).
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
 * Join the delivery's descriptors and materials into the binding map one
 * consumer's capability is built from: every descriptor addressed to
 * this consumer, resolved to the material that backs its credential. A
 * descriptor whose material is absent is a malformed delivery and fails
 * closed at build.
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
      // A descriptor with no material is a delivery bug. Refuse it at
      // build rather than let the handle resolve to nothing at first use.
      throw new Error(
        `credential delivery is malformed: descriptor for handle "${descriptor.handle}" (consumer ${consumer}) references credential ${descriptor.credentialId} but the delivery carries no material for it`,
      );
    }

    // Capture the provider key and origin at build time (they identify
    // the credential and its pinned origin); the secret is read live per
    // use.
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
 * The rotation/revocation indirection a shaped handle reads through: it
 * looks the credential's material up in the LIVE cell on every call
 * (invariant 1), failing closed when the material is gone (a re-push
 * dropped it) and asserting the provider/origin have not drifted under
 * the already-shaped handle.
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
    // A rotation changes the SECRET, not the provider or origin. A live
    // entry whose provider/origin diverged would mean the shaped handle
    // now authenticates somewhere it was not pinned to; surface it
    // rather than silently follow the change.
    if (material.providerKey !== providerKey || material.origin !== origin) {
      throw new Error(
        `credential ${credentialId} changed provider/origin under an already-shaped handle (${providerKey}@${origin} -> ${material.providerKey}@${material.origin}); a shaped handle cannot follow that change`,
      );
    }
    return { secret: material.secret };
  };
}

/**
 * A generic inference credential resolver over the live cell: resolves a
 * credential's current secret by `credentialId`, failing closed when the
 * cell is empty or the credential is absent. Unlike
 * `makeReadCurrentMaterial` it is keyed by `credentialId` at call time
 * (an inference source's failover chain carries a distinct credential
 * per entry) and pins no provider/origin: an inference request
 * authenticates to the source's own `baseURL`, so there is no shaped
 * handle to protect against origin drift.
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

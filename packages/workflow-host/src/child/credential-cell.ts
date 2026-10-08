// Merge semantics for the child's in-memory credential-material cell. The cell
// has several independently-scoped producers, so a wholesale swap would let one
// evict another's credentials. A `credentials-updated` frame therefore merges:
// materials upsert by `credentialId`, bindings by `(consumer, handle)`.
// Revocation is explicit: the `revoke` list drops the named credentialIds and
// every binding that references them, applied before the upsert so a frame that
// both revokes and re-adds resolves to the re-add.

import type {
  CredentialBindingDescriptor,
  CredentialDelivery,
  CredentialMaterialEntry,
} from "@intx/types/sidecar";

// A binding's identity is the (consumer, handle) pair: a handle is only unique
// within a consumer. The NUL joiner keeps the pair injective -- a handle may
// contain a space or a colon, but by convention never a NUL.
function bindingKey(binding: CredentialBindingDescriptor): string {
  return `${binding.consumer}\u0000${binding.handle}`;
}

/**
 * Apply a `credentials-updated` frame to the current cell and return the next
 * cell. Pure, so the caller can assign the result to the live ref in one
 * atomic whole-object swap without a concurrent reader observing a torn cell.
 */
export function mergeCredentialDelivery(
  current: CredentialDelivery | null,
  delivery: CredentialDelivery,
  revoke: readonly string[] | undefined,
): CredentialDelivery {
  const materials = new Map<string, CredentialMaterialEntry>();
  const bindings = new Map<string, CredentialBindingDescriptor>();
  if (current !== null) {
    for (const material of current.materials) {
      materials.set(material.credentialId, material);
    }
    for (const binding of current.bindings) {
      bindings.set(bindingKey(binding), binding);
    }
  }
  if (revoke !== undefined) {
    for (const credentialId of revoke) {
      materials.delete(credentialId);
      for (const [key, binding] of bindings) {
        if (binding.credentialId === credentialId) {
          bindings.delete(key);
        }
      }
    }
  }
  for (const material of delivery.materials) {
    materials.set(material.credentialId, material);
  }
  for (const binding of delivery.bindings) {
    bindings.set(bindingKey(binding), binding);
  }
  return {
    bindings: [...bindings.values()],
    materials: [...materials.values()],
  };
}

// Merge semantics for the child's in-memory credential-material cell.
//
// The cell is fed by several INDEPENDENTLY-SCOPED producers: the deploy frame
// (tool bindings plus the run's inference materials, frozen at deploy time and
// re-asserted on every spawn and pre-trigger barrier), an inference rotation
// (materials only, no binding), and a tool-grant push (tool bindings plus their
// materials). Each producer carries only its own slice of the cell. A wholesale
// swap would therefore let one producer evict another's credentials -- an
// inference rotation would drop every tool binding, a barrier re-push would drop
// a live-pushed tool credential. So a `credentials-updated` frame MERGES:
// materials upsert by `credentialId`, bindings upsert by `(consumer, handle)`.
//
// Because omission no longer means "gone", revocation is EXPLICIT: a frame's
// `revoke` list names the credentialIds to drop, and dropping one also drops
// every binding that references it. Revocation is applied before the upsert, so
// a frame that both revokes and re-adds the same credentialId resolves to the
// re-add. A material an upsert leaves unreferenced (a rebind of a handle to a
// different credential) is not garbage-collected -- inference materials never
// carry a binding, so "unreferenced" is not a drop signal; only an explicit
// `revoke` removes a material.

import type {
  CredentialBindingDescriptor,
  CredentialDelivery,
  CredentialMaterialEntry,
} from "@intx/types/sidecar";

// A binding's identity is the (consumer, handle) pair: a handle string is only
// unique within a consumer, so two consumers can each bind their own handle of
// the same name. The U+0000 joiner keeps the pair injective where a printable
// joiner would not -- a handle is an arbitrary string that may contain a space
// or a colon, but by convention never a NUL.
function bindingKey(binding: CredentialBindingDescriptor): string {
  return `${binding.consumer}\u0000${binding.handle}`;
}

/**
 * Apply a `credentials-updated` frame to the current cell and return the next
 * cell. Pure: it reads neither argument's identity back out, so the caller can
 * assign the result to the live ref in one atomic whole-object swap and a
 * concurrent reader never observes a torn cell.
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

// Typed registry of host-provided capabilities that tool packages request at
// handler-init. The host (sidecar harness, or an alternate runtime) builds a
// RuntimeCapabilities instance and hands it to each tool package's factory;
// the package calls `resolve` to obtain typed handles to host services.
//
// The map is the extension point: new capabilities are added by extending
// RuntimeCapabilityMap inside this file. TypeScript permits module
// augmentation of the interface from any consumer, but augmentation from
// outside @intx/types is not the supported extension path; contribute
// keys here so every host sees the same canonical map.

import type { MessageTransport } from "./runtime";
import type { CredentialCapability } from "./mediated-credential";

/**
 * Registry of capability keys to the value types they resolve to. Keys are
 * dotted strings scoped by subsystem (e.g. `mail.transport`).
 *
 * Adding a capability: extend this interface with the new key and its value
 * type, then have a host populate it when constructing a
 * `RuntimeCapabilities`.
 */
export interface RuntimeCapabilityMap {
  /**
   * The bound agent's message transport — the SMTP/IMAP-equivalent handle
   * for sending and receiving mail.
   */
  "mail.transport": MessageTransport;

  /**
   * Provider-backed credentials the agent's tools resolve by their declared
   * handle. Unlike the other keys, its value is itself a sub-registry: the set
   * of bound handles is per-deploy runtime data, not known at compile time, so
   * the dynamic axis lives inside `CredentialCapability` while this outer map
   * stays fixed and typed. Resolution is consumer-scoped and fail-closed.
   */
  credentials: CredentialCapability;
}

export type RuntimeCapabilityKey = keyof RuntimeCapabilityMap;

/**
 * Host-provided capability registry. Tool packages receive an instance at
 * construction; `resolve` is intended to be called once per key at
 * handler-init, with the returned handle held for the deploy lifetime.
 * `resolve` throws naming the key when the host did not provide a value
 * for it.
 */
export interface RuntimeCapabilities {
  resolve<K extends RuntimeCapabilityKey>(key: K): RuntimeCapabilityMap[K];
}

/**
 * Build a resolver from a partial map of capability values. The map is
 * snapshotted at construction — later mutation of the input is not visible
 * to `resolve`. Keys absent from the snapshot throw at resolve-time with a
 * message naming the key.
 *
 * Use this from any host (harness, test harness, alternate runtime) that
 * wants the standard resolver semantics without re-implementing the
 * throw-on-missing plumbing.
 */
export function createRuntimeCapabilities(
  values: Partial<RuntimeCapabilityMap>,
): RuntimeCapabilities {
  // Snapshot the input. The resolver's lifecycle contract is "resolved
  // once at handler-init, held for the deploy lifetime" — later mutation
  // of the input map by the host must not be observable here.
  const snapshot: Partial<RuntimeCapabilityMap> = { ...values };

  return {
    resolve<K extends RuntimeCapabilityKey>(key: K): RuntimeCapabilityMap[K] {
      // Object.hasOwn distinguishes "host did not provide" from "host
      // provided undefined". Both are distinct failures the host
      // should hear about separately. No capability in
      // RuntimeCapabilityMap currently resolves to undefined, so the
      // second check is a defensive guard against a host accidentally
      // wiring an undefined value to a non-nullable capability slot;
      // adding a nullable capability in the future means revisiting
      // this branch.
      if (!Object.hasOwn(snapshot, key)) {
        throw new Error(
          `Runtime capability "${String(key)}" was requested but not provided by the host`,
        );
      }
      const value = snapshot[key];
      if (value === undefined) {
        throw new Error(
          `Runtime capability "${String(key)}" was provided as undefined; no current capability resolves to undefined`,
        );
      }
      return value;
    },
  };
}

/**
 * Compose a resolver that answers the keys in `overrides` from the override
 * map and delegates every other key to `base`.
 *
 * The host uses this to add a per-bundle capability -- the consumer-scoped
 * `credentials` handle, one instance per tool package -- onto a shared
 * per-step base bag without re-plumbing the base's keys (`mail.transport`
 * and any future shared key stay owned by the step bag). Each tool package's
 * bundle receives the same base layered with ITS OWN credentials capability,
 * so a package cannot resolve a handle scoped to a different package.
 *
 * `overrides` is snapshotted at construction, mirroring
 * `createRuntimeCapabilities`, so later mutation of the input is not
 * observable through `resolve`. An overridden key wired to `undefined`
 * throws with the same guard as the base resolver rather than silently
 * shadowing `base` with a hole -- a host that layers an undefined value
 * has a wiring bug and must hear about it.
 */
export function layerRuntimeCapabilities(
  base: RuntimeCapabilities,
  overrides: Partial<RuntimeCapabilityMap>,
): RuntimeCapabilities {
  const snapshot: Partial<RuntimeCapabilityMap> = { ...overrides };

  return {
    resolve<K extends RuntimeCapabilityKey>(key: K): RuntimeCapabilityMap[K] {
      if (Object.hasOwn(snapshot, key)) {
        const value = snapshot[key];
        if (value === undefined) {
          throw new Error(
            `Runtime capability "${String(key)}" was layered as undefined; no current capability resolves to undefined`,
          );
        }
        return value;
      }
      return base.resolve(key);
    },
  };
}

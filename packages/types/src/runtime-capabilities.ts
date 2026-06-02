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

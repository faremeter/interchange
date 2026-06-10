// Default blob substrate for `runLocal`.
//
// Values smaller than the inline threshold (1 MiB by JSON-stringified
// size) embed their ref directly; larger values are stored against an
// internal Map and the ref carries a `blob://` URI the step executor
// can resolve. Production wires this to the kind-keyed RepoStore;
// runLocal keeps everything in memory.

import type { BlobSubstrate } from "../runtime/env";

const ONE_MIB = 1024 * 1024;

export interface InMemoryBlobOptions {
  inlineMaxBytes?: number;
}

export function createInMemoryBlobSubstrate(
  opts: InMemoryBlobOptions = {},
): BlobSubstrate {
  const inlineMax = opts.inlineMaxBytes ?? ONE_MIB;
  const blobs = new Map<string, unknown>();
  return {
    ephemeral: true,
    async recordOutput(stepId, attempt, value) {
      const encoded = JSON.stringify(value);
      if (encoded === undefined) {
        // JSON.stringify returns undefined for undefined, functions,
        // and symbols (the values JSON cannot represent). Surfacing
        // this as an error keeps the substrate contract honest --
        // the alternative is silent coercion to null, which destroys
        // information about the actual step output.
        throw new Error(
          `step ${stepId} attempt ${String(attempt)} produced an output the blob substrate cannot serialize (typeof ${typeof value})`,
        );
      }
      // Values up to the inline threshold ride inside the ref as a
      // verbatim JSON payload; anything above spills to the blob
      // map. The ref's whole body is the encoded JSON -- no slice
      // truncation, because the round-trip must survive resolveRef
      // for any value the runtime classified as inline. Step outputs
      // in the 4 KB-1 MiB band would otherwise corrupt silently.
      if (encoded.length <= inlineMax) {
        return { ref: `inline:${encoded}` };
      }
      const ref = `blob:${stepId}/${String(attempt)}/${Math.random().toString(36).slice(2, 10)}`;
      blobs.set(ref, value);
      return { ref };
    },
    async resolveRef(ref) {
      if (ref.startsWith("blob:")) {
        if (!blobs.has(ref)) {
          throw new Error(`unknown blob ref ${ref}`);
        }
        return blobs.get(ref);
      }
      if (ref.startsWith("inline:")) {
        return JSON.parse(ref.slice("inline:".length));
      }
      throw new Error(`unrecognized ref ${ref}`);
    },
  };
}

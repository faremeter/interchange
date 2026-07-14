import { describe, test, expect } from "bun:test";
import { sha256 } from "@intx/crypto";
import { hexEncode } from "@intx/types";
import type { DB } from "@intx/db";

import { createSidecarTokenAuthenticator } from "./sidecar-token-authenticator";

type SidecarRow = {
  id: string;
  tokenHashSha256: Uint8Array;
};

type MockDBOpts = {
  sidecar?: SidecarRow | null;
  onFindFirst?: (args: { where: unknown }) => void;
};

function createMockDB(opts: MockDBOpts): DB["db"] {
  const mock = {
    query: {
      sidecar: {
        findFirst: async (args: { where: unknown }) => {
          opts.onFindFirst?.(args);
          return opts.sidecar !== null && opts.sidecar !== undefined
            ? opts.sidecar
            : undefined;
        },
      },
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return mock as unknown as DB["db"];
}

describe("createSidecarTokenAuthenticator", () => {
  test("resolves a known token to the stored sidecar's identity", async () => {
    const token = "sidecar-secret";
    const authenticate = createSidecarTokenAuthenticator({
      db: createMockDB({
        sidecar: { id: "sc-1", tokenHashSha256: await sha256(token) },
      }),
    });

    const identity = await authenticate({ sidecarId: "sc-1", token });

    expect(identity).toEqual({ kind: "sidecar", sidecarId: "sc-1" });
  });

  test("rejects an unknown token with null", async () => {
    const authenticate = createSidecarTokenAuthenticator({
      db: createMockDB({ sidecar: null }),
    });

    const identity = await authenticate({
      sidecarId: "sc-1",
      token: "wrong-secret",
    });

    expect(identity).toBeNull();
  });

  test("derives identity from the token, not the claimed sidecarId", async () => {
    const token = "sidecar-secret";
    const authenticate = createSidecarTokenAuthenticator({
      db: createMockDB({
        sidecar: { id: "sc-real", tokenHashSha256: await sha256(token) },
      }),
    });

    const identity = await authenticate({ sidecarId: "sc-claimed", token });

    expect(identity).toEqual({ kind: "sidecar", sidecarId: "sc-real" });
  });

  test("looks up by the token's hash, never the raw token", async () => {
    const token = "sidecar-secret";
    let capturedWhere: unknown;
    const authenticate = createSidecarTokenAuthenticator({
      db: createMockDB({
        sidecar: null,
        onFindFirst: ({ where }) => {
          capturedWhere = where;
        },
      }),
    });

    await authenticate({ sidecarId: "sc-1", token });

    // The drizzle `eq(...)` condition object embeds the compared value as a
    // parameter. Collect every byte value reachable within it (the graph is
    // cyclic, so walk with a visited set) and assert the compared value is
    // the SHA-256 digest of the token, not the raw token itself.
    const bytesFound = collectByteArrays(capturedWhere);
    const foundHex = bytesFound.map(hexEncode);
    expect(foundHex).toContain(hexEncode(await sha256(token)));
    expect(foundHex).not.toContain(hexEncode(new TextEncoder().encode(token)));
  });
});

// Recursively collect every Uint8Array reachable from `root`, tolerating the
// cyclic object graph drizzle builds for a condition.
function collectByteArrays(root: unknown): Uint8Array[] {
  const found: Uint8Array[] = [];
  const seen = new Set<unknown>();
  const stack: unknown[] = [root];
  while (stack.length > 0) {
    const node = stack.pop();
    if (node instanceof Uint8Array) {
      found.push(node);
      continue;
    }
    if (node === null || typeof node !== "object" || seen.has(node)) {
      continue;
    }
    seen.add(node);
    for (const value of Object.values(node)) {
      stack.push(value);
    }
  }
  return found;
}

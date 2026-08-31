import { describe, test, expect } from "bun:test";
import { sha256 } from "@intx/crypto";
import { hexEncode } from "@intx/types";
import type { DB } from "@intx/db";

import {
  createSidecarCredentialResolver,
  createSidecarTokenAuthenticator,
} from "./sidecar-token-authenticator";

type SidecarRow = {
  id: string;
  tokenHashSha256: Uint8Array;
};

type MockDBOpts = {
  sidecar?: SidecarRow | null;
  allocation?: {
    id: string;
    sidecarId: string;
    tenantId: string;
    anchorRunId: string;
    status: string;
    generation: number;
    ensureAcceptedGeneration: number | null;
  } | null;
  probe?: {
    id: string;
    sidecarId: string;
    tenantId: string;
    status: string;
    generation: number;
  } | null;
  anchorAddress?: string | null;
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
      sidecarAllocation: {
        findFirst: async () => opts.allocation ?? undefined,
      },
      workflowProbe: {
        findFirst: async () => opts.probe ?? undefined,
      },
      workflowRun: {
        findFirst: async () =>
          opts.anchorAddress === undefined
            ? undefined
            : { address: opts.anchorAddress },
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
        sidecar: {
          id: "sc-1",
          tokenHashSha256: await sha256(token),
        },
      }),
    });

    const identity = await authenticate({ sidecarId: "sc-1", token });

    expect(identity).toBeNull();
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
        sidecar: {
          id: "sc-real",
          tokenHashSha256: await sha256(token),
        },
      }),
    });

    const identity = await authenticate({ sidecarId: "sc-claimed", token });

    expect(identity).toBeNull();
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

  test("resolves and revalidates an allocated credential generation", async () => {
    const token = "allocated-secret";
    const resolver = createSidecarCredentialResolver({
      db: createMockDB({
        sidecar: {
          id: "sc-allocated",
          tokenHashSha256: await sha256(token),
        },
        allocation: {
          id: "alloc-1",
          sidecarId: "sc-allocated",
          tenantId: "tenant-1",
          anchorRunId: "run-anchor",
          status: "allocated",
          generation: 2,
          ensureAcceptedGeneration: 2,
        },
        anchorAddress: "workflow@exclusive",
      }),
    });

    const identity = await resolver.resolve(token);

    expect(identity).toEqual({
      kind: "allocated",
      sidecarId: "sc-allocated",
      allocationId: "alloc-1",
      tenantId: "tenant-1",
      anchorRunId: "run-anchor",
      workflowRunAddress: "workflow@exclusive",
      generation: 2,
    });
    if (identity === null) throw new Error("expected allocated identity");
    expect(await resolver.isCurrent(identity, "routing")).toBe(true);
  });

  test("rejects an allocated credential without a current allocation", async () => {
    const token = "stale-allocated-secret";
    const resolver = createSidecarCredentialResolver({
      db: createMockDB({
        sidecar: {
          id: "sc-replaced",
          tokenHashSha256: await sha256(token),
        },
        allocation: null,
      }),
    });

    expect(await resolver.resolve(token)).toBeNull();
  });

  test("resolves and revalidates probe-scoped capacity", async () => {
    const token = "probe-secret";
    const resolver = createSidecarCredentialResolver({
      db: createMockDB({
        sidecar: {
          id: "sc-probe",
          tokenHashSha256: await sha256(token),
        },
        probe: {
          id: "probe-1",
          sidecarId: "sc-probe",
          tenantId: "tenant-1",
          status: "probing",
          generation: 0,
        },
      }),
    });

    const identity = await resolver.resolve(token);

    expect(identity).toEqual({
      kind: "probe",
      sidecarId: "sc-probe",
      allocationId: "probe-1",
      tenantId: "tenant-1",
      generation: 0,
    });
    if (identity === null) throw new Error("expected probe identity");
    expect(await resolver.isCurrent(identity, "routing")).toBe(true);
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

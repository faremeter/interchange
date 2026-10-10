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
    initializationLeaseId?: string | null;
  } | null;
  probe?: {
    id: string;
    sidecarId: string;
    tenantId: string;
    status: string;
    generation: number;
  } | null;
  anchorAddress?: string | null;
  anchorPublicKey?: string | null;
  anchorStatus?: string;
  anchorCancellationRequestedAt?: Date;
  anchorFailureCode?: string;
  anchorCapacityReleaseAt?: Date;
  onFindFirst?: (args: { where: unknown }) => void;
};

// The relational queries below stand in for SQL, so they apply the status
// filters the resolver's WHERE clauses express.
const HOSTING_ALLOCATION_STATUSES = ["provisioning", "allocated"];
const HOSTING_PROBE_STATUSES = ["provisioning", "probing"];

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
        findFirst: async () =>
          opts.allocation === null || opts.allocation === undefined
            ? undefined
            : {
                initializationLeaseId: null,
                ...opts.allocation,
              },
        findMany: async () =>
          opts.allocation !== null &&
          opts.allocation !== undefined &&
          HOSTING_ALLOCATION_STATUSES.includes(opts.allocation.status)
            ? [opts.allocation]
            : [],
      },
      workflowProbe: {
        findFirst: async () => opts.probe ?? undefined,
        findMany: async () =>
          opts.probe !== null &&
          opts.probe !== undefined &&
          HOSTING_PROBE_STATUSES.includes(opts.probe.status)
            ? [opts.probe]
            : [],
      },
      workflowRun: {
        findFirst: async () =>
          opts.anchorAddress === undefined
            ? undefined
            : {
                address: opts.anchorAddress,
                publicKey: opts.anchorPublicKey ?? null,
                status: opts.anchorStatus ?? "running",
                expiresAt: null,
                endedAt: null,
                capacityReleaseAt: opts.anchorCapacityReleaseAt ?? null,
                lifecyclePolicy: null,
                cancellationRequestedAt:
                  opts.anchorCancellationRequestedAt ?? null,
                failureCode: opts.anchorFailureCode ?? null,
              },
        findMany: async () =>
          opts.allocation === null ||
          opts.allocation === undefined ||
          opts.anchorAddress === undefined
            ? []
            : [
                {
                  id: opts.allocation.anchorRunId,
                  address: opts.anchorAddress,
                },
              ],
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

    expect(identity).toEqual({ sidecarId: "sc-1" });
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

    expect(identity).toEqual({ sidecarId: "sc-real" });
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

    const binding = {
      kind: "allocated",
      sidecarId: "sc-allocated",
      allocationId: "alloc-1",
      tenantId: "tenant-1",
      anchorRunId: "run-anchor",
      workflowRunAddress: "workflow@exclusive",
      generation: 2,
    } as const;
    expect(await resolver.resolve(token)).toEqual({
      sidecarId: "sc-allocated",
    });
    expect(await resolver.resolveBindings("sc-allocated")).toEqual([binding]);
    expect(await resolver.isCurrent(binding, "routing")).toBe(true);
  });

  test("reclaims only a deployment whose first deploy completed", async () => {
    const binding = {
      kind: "allocated",
      sidecarId: "sc-allocated",
      allocationId: "alloc-1",
      tenantId: "tenant-1",
      anchorRunId: "run-anchor",
      workflowRunAddress: "workflow@exclusive",
      generation: 2,
    } as const;
    const resolverWith = (
      initializationLeaseId: string | null,
      anchorPublicKey: string | null,
      anchorStatus = "running",
    ) =>
      createSidecarCredentialResolver({
        db: createMockDB({
          allocation: {
            id: "alloc-1",
            sidecarId: "sc-allocated",
            tenantId: "tenant-1",
            anchorRunId: "run-anchor",
            status: "allocated",
            generation: 2,
            ensureAcceptedGeneration: 2,
            initializationLeaseId,
          },
          anchorAddress: "workflow@exclusive",
          anchorPublicKey,
          anchorStatus,
        }),
      });

    expect(
      await resolverWith(null, "public-key").isCurrent(binding, "reclaim"),
    ).toBe(true);
    expect(
      await resolverWith("lease-1", "public-key").isCurrent(binding, "reclaim"),
    ).toBe(false);
    expect(await resolverWith(null, null).isCurrent(binding, "reclaim")).toBe(
      false,
    );
    expect(await resolverWith(null, null).isCurrent(binding, "routing")).toBe(
      true,
    );
    expect(
      await resolverWith(null, "public-key", "failed").isCurrent(
        binding,
        "reclaim",
      ),
    ).toBe(false);
  });

  test("retains initialized terminal copies until their release is due", async () => {
    const binding = {
      kind: "allocated",
      sidecarId: "sc-allocated",
      allocationId: "alloc-1",
      tenantId: "tenant-1",
      anchorRunId: "run-anchor",
      workflowRunAddress: "workflow@exclusive",
      generation: 2,
    } as const;
    const resolverWith = (
      initializationLeaseId: string | null,
      anchorPublicKey: string | null,
      anchorStatus: string,
      outcome: {
        cancelled?: boolean;
        failureCode?: string;
        releaseAt?: Date;
      } = {},
    ) =>
      createSidecarCredentialResolver({
        db: createMockDB({
          allocation: {
            id: "alloc-1",
            sidecarId: "sc-allocated",
            tenantId: "tenant-1",
            anchorRunId: "run-anchor",
            status: "allocated",
            generation: 2,
            ensureAcceptedGeneration: 2,
            initializationLeaseId,
          },
          anchorAddress: "workflow@exclusive",
          anchorPublicKey,
          anchorStatus,
          ...(outcome.releaseAt !== undefined
            ? { anchorCapacityReleaseAt: outcome.releaseAt }
            : {}),
          ...(outcome.cancelled === true
            ? { anchorCancellationRequestedAt: new Date() }
            : {}),
          ...(outcome.failureCode !== undefined
            ? { anchorFailureCode: outcome.failureCode }
            : {}),
        }),
      });

    expect(
      await resolverWith(null, "public-key", "failed").isCurrent(
        binding,
        "retention",
      ),
    ).toBe(true);
    // Hub-ended copies are stopped and retained rather than reclaimed.
    expect(
      await resolverWith(null, "public-key", "cancelled", {
        cancelled: true,
      }).isCurrent(binding, "retention"),
    ).toBe(true);
    expect(
      await resolverWith(null, "public-key", "failed", {
        failureCode: "sidecar_deployment_stopped",
      }).isCurrent(binding, "retention"),
    ).toBe(true);
    expect(
      await resolverWith(null, "public-key", "failed", {
        releaseAt: new Date(0),
      }).isCurrent(binding, "retention"),
    ).toBe(false);
    expect(
      await resolverWith(null, "public-key", "running").isCurrent(
        binding,
        "retention",
      ),
    ).toBe(false);
    expect(
      await resolverWith("lease-1", "public-key", "failed").isCurrent(
        binding,
        "retention",
      ),
    ).toBe(false);
    expect(
      await resolverWith(null, null, "failed").isCurrent(binding, "retention"),
    ).toBe(false);
  });

  test("resolves an allocated credential without a current allocation to no bindings", async () => {
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

    expect(await resolver.resolve(token)).toEqual({ sidecarId: "sc-replaced" });
    expect(await resolver.resolveBindings("sc-replaced")).toEqual([]);
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

    const binding = {
      kind: "probe",
      sidecarId: "sc-probe",
      allocationId: "probe-1",
      tenantId: "tenant-1",
      generation: 0,
    } as const;
    expect(await resolver.resolve(token)).toEqual({ sidecarId: "sc-probe" });
    expect(await resolver.resolveBindings("sc-probe")).toEqual([binding]);
    expect(await resolver.isCurrent(binding, "routing")).toBe(true);
    expect(await resolver.isCurrent(binding, "reclaim")).toBe(false);
    expect(await resolver.isCurrent(binding, "retention")).toBe(false);
  });

  test("resolves every probe and allocation a sidecar hosts", async () => {
    const token = "shared-secret";
    const resolver = createSidecarCredentialResolver({
      db: createMockDB({
        sidecar: {
          id: "sc-shared",
          tokenHashSha256: await sha256(token),
        },
        allocation: {
          id: "alloc-1",
          sidecarId: "sc-shared",
          tenantId: "tenant-1",
          anchorRunId: "run-anchor",
          status: "allocated",
          generation: 3,
          ensureAcceptedGeneration: 3,
        },
        probe: {
          id: "probe-1",
          sidecarId: "sc-shared",
          tenantId: "tenant-1",
          status: "probing",
          generation: 0,
        },
        anchorAddress: "workflow@exclusive",
      }),
    });

    expect(await resolver.resolve(token)).toEqual({ sidecarId: "sc-shared" });
    expect(await resolver.resolveBindings("sc-shared")).toEqual([
      {
        kind: "allocated",
        sidecarId: "sc-shared",
        allocationId: "alloc-1",
        tenantId: "tenant-1",
        anchorRunId: "run-anchor",
        workflowRunAddress: "workflow@exclusive",
        generation: 3,
      },
      {
        kind: "probe",
        sidecarId: "sc-shared",
        allocationId: "probe-1",
        tenantId: "tenant-1",
        generation: 0,
      },
    ]);
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

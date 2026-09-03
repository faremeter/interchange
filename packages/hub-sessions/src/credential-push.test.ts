import { describe, test, expect } from "bun:test";

import { createNoopCredentialCipher } from "@intx/crypto";
import type { DB } from "@intx/db";
import type { CredentialDelivery } from "@intx/types/sidecar";

import {
  pushCredentialRevoke,
  pushSourceUpdates,
  pushSourceUpdatesSubtree,
} from "./credential-push";
import type { SidecarRouter } from "./ws/sidecar-handler";

// The push runs fire-and-forget from request handlers (callers discard the
// promise), so a database failure inside it must surface as a logged warning,
// never as a rejection that becomes an unhandled promise rejection.

function rejectingDB(): DB["db"] {
  const reject = () => Promise.reject(new Error("db boom"));
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- minimal failing stand-in for the drizzle client
  return {
    query: {
      tenant: { findMany: reject },
      workflowRun: { findMany: reject },
    },
  } as unknown as DB["db"];
}

// eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- unused by the failing paths under test
const dummyRouter = {} as unknown as SidecarRouter;

// The DB scan rejects before any credential is resolved, so the cipher is never
// reached; a noop stands in for the required parameter.
const dummyCipher = createNoopCredentialCipher();

describe("source push error containment", () => {
  test("pushSourceUpdatesSubtree resolves when descendant lookup fails", async () => {
    const result = await pushSourceUpdatesSubtree(
      rejectingDB(),
      dummyRouter,
      "tnt_1",
      dummyCipher,
    );
    expect(result).toBeUndefined();
  });

  test("pushSourceUpdates resolves when the instance scan fails", async () => {
    const result = await pushSourceUpdates(
      rejectingDB(),
      dummyRouter,
      "tnt_1",
      dummyCipher,
    );
    expect(result).toBeUndefined();
  });

  test("pushCredentialRevoke resolves when the descendant lookup fails", async () => {
    const result = await pushCredentialRevoke(
      rejectingDB(),
      dummyRouter,
      "tnt_1",
      "cred_x",
    );
    expect(result).toBeUndefined();
  });
});

// A running run's address is the routing key; `null` violates the
// isNotNull(address) filter and must surface, not silently skip.
function dbWithRuns(runs: { id: string; address: string | null }[]): DB["db"] {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- minimal stand-in for the drizzle relational client
  return {
    query: {
      // No children, so getDescendantTenants returns just the root tenant.
      tenant: { findMany: async () => [] },
      workflowRun: { findMany: async () => runs },
    },
  } as unknown as DB["db"];
}

function capturingRouter(): {
  router: Pick<SidecarRouter, "sendCredentialsUpdate">;
  calls: {
    address: string;
    delivery: CredentialDelivery;
    revoke: string[] | undefined;
  }[];
} {
  const calls: {
    address: string;
    delivery: CredentialDelivery;
    revoke: string[] | undefined;
  }[] = [];
  return {
    calls,
    router: {
      sendCredentialsUpdate: async (address, delivery, revoke) => {
        calls.push({ address, delivery, revoke });
      },
    },
  };
}

describe("pushCredentialRevoke", () => {
  test("broadcasts a flat named revoke to each distinct running address", async () => {
    const { router, calls } = capturingRouter();
    await pushCredentialRevoke(
      dbWithRuns([
        { id: "r1", address: "addr-1" },
        { id: "r2", address: "addr-1" },
        { id: "r3", address: "addr-2" },
      ]),
      router,
      "tnt_1",
      "cred_x",
    );
    // Two DISTINCT addresses; the duplicate is deduped.
    expect(calls).toHaveLength(2);
    expect(new Set(calls.map((c) => c.address))).toEqual(
      new Set(["addr-1", "addr-2"]),
    );
    for (const call of calls) {
      expect(call.delivery).toEqual({ bindings: [], materials: [] });
      expect(call.revoke).toEqual(["cred_x"]);
    }
  });

  test("sends nothing when no running run holds an address", async () => {
    const { router, calls } = capturingRouter();
    await pushCredentialRevoke(dbWithRuns([]), router, "tnt_1", "cred_x");
    expect(calls).toHaveLength(0);
  });

  test("surfaces (and contains) a null address that passed the filter", async () => {
    const { router, calls } = capturingRouter();
    // A null address here is a broken invariant; the throw is caught and logged,
    // not propagated, and no partial revoke is sent.
    const result = await pushCredentialRevoke(
      dbWithRuns([{ id: "r1", address: null }]),
      router,
      "tnt_1",
      "cred_x",
    );
    expect(result).toBeUndefined();
    expect(calls).toHaveLength(0);
  });
});

import { describe, test, expect } from "bun:test";

import { createNoopCredentialCipher } from "@intx/crypto";
import type { DB } from "@intx/db";
import type { CredentialDelivery } from "@intx/types/credential-delivery";

import {
  pushCredentialRevoke,
  pushCredentialRotation,
  recheckDeliveredCredentials,
  revokeRemovedCredentials,
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

describe("push error containment", () => {
  test("pushCredentialRotation resolves when the descendant lookup fails", async () => {
    const result = await pushCredentialRotation(
      rejectingDB(),
      dummyRouter,
      "tnt_1",
      "cred_x",
      dummyCipher,
    );
    expect(result).toBeUndefined();
  });

  test("pushCredentialRotation resolves when the deployment scan fails", async () => {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- minimal stand-in for the drizzle client
    const db = {
      query: {
        tenant: { findMany: async () => [] },
        workflowRun: {
          findMany: () => Promise.reject(new Error("db boom")),
        },
      },
    } as unknown as DB["db"];
    const result = await pushCredentialRotation(
      db,
      dummyRouter,
      "tnt_1",
      "cred_x",
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

// A live run's address is the routing key; `null` violates the
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
  test("broadcasts a flat named revoke to each distinct live address", async () => {
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

  test("sends nothing when no live run holds an address", async () => {
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

describe("pushCredentialRotation", () => {
  function rotationDB(deployments: () => Promise<unknown[]>): DB["db"] {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- minimal stand-in for the drizzle client
    return {
      query: {
        tenant: { findMany: async () => [] },
        workflowRun: { findMany: deployments },
        credential: {
          findFirst: async () => ({
            id: "cred_x",
            providerId: "prv_x",
            status: "active",
            secret: "sk-rotated",
            secretVersion: 2,
          }),
        },
        provider: {
          findFirst: async () => ({
            name: "prv-x",
            plugin: "http",
            apiBaseUrl: "https://api.example.test",
          }),
        },
      },
      // The post-send re-check finds the credential still live.
      select: () => ({
        from: () => ({
          where: async () => [{ id: "cred_x", status: "active" }],
        }),
      }),
    } as unknown as DB["db"];
  }

  const holder = {
    id: "dep_1",
    address: "dep-1",
    credentialRefs: { credentialIds: ["cred_x"], bindings: [] },
  };

  test("pushes the current material to a deployment holding the credential", async () => {
    const { router, calls } = capturingRouter();
    await pushCredentialRotation(
      rotationDB(async () => [holder]),
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- only sendCredentialsUpdate is reached
      router as unknown as SidecarRouter,
      "tnt_1",
      "cred_x",
      dummyCipher,
    );
    expect(calls).toEqual([
      {
        address: "dep-1",
        delivery: {
          bindings: [],
          materials: [
            {
              credentialId: "cred_x",
              providerKey: "http",
              origin: "https://api.example.test",
              secret: "sk-rotated",
              secretVersion: 2,
            },
          ],
        },
        revoke: undefined,
      },
    ]);
  });

  test("skips a deployment whose refs do not name the credential", async () => {
    const { router, calls } = capturingRouter();
    await pushCredentialRotation(
      rotationDB(async () => [
        {
          ...holder,
          credentialRefs: { credentialIds: ["cred_y"], bindings: [] },
        },
      ]),
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- no send is reached
      router as unknown as SidecarRouter,
      "tnt_1",
      "cred_x",
      dummyCipher,
    );
    expect(calls).toHaveLength(0);
  });
});

// The deploy path awaits this after its completion commits, so a rejection
// would send a successful deploy into its failure handling.
describe("revokeRemovedCredentials", () => {
  function statusDB(rows: () => Promise<unknown[]>): DB["db"] {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- minimal select-chain stand-in for the status read
    return {
      select: () => ({ from: () => ({ where: rows }) }),
    } as unknown as DB["db"];
  }

  test("resolves when the router has no route for the address", async () => {
    const router = {
      sendCredentialsUpdate: async () => {
        throw new Error('No sidecar connected for agent "a"');
      },
    };
    await expect(
      revokeRemovedCredentials(
        statusDB(async () => []),
        router,
        "a",
        ["gone"],
      ),
    ).resolves.toBeUndefined();
  });

  test("recheckDeliveredCredentials resolves and sends nothing when every read fails", async () => {
    const { router, calls } = capturingRouter();
    await expect(
      recheckDeliveredCredentials(
        statusDB(async () => {
          throw new Error("db down");
        }),
        router,
        "a",
        [{ credentialId: "x", secretVersion: 1 }],
        dummyCipher,
      ),
    ).resolves.toBeUndefined();
    expect(calls).toHaveLength(0);
  });

  test("resolves and sends nothing when the status read fails", async () => {
    const { router, calls } = capturingRouter();
    await expect(
      revokeRemovedCredentials(
        statusDB(async () => {
          throw new Error("db down");
        }),
        router,
        "a",
        ["x"],
      ),
    ).resolves.toBeUndefined();
    expect(calls).toHaveLength(0);
  });

  test("skips the status read for an empty id set", async () => {
    let reads = 0;
    const { router, calls } = capturingRouter();
    await revokeRemovedCredentials(
      statusDB(async () => {
        reads += 1;
        return [];
      }),
      router,
      "a",
      [],
    );
    expect(reads).toBe(0);
    expect(calls).toHaveLength(0);
  });

  test("revokes a duplicated id once", async () => {
    const { router, calls } = capturingRouter();
    await revokeRemovedCredentials(
      statusDB(async () => []),
      router,
      "a",
      ["x", "x"],
    );
    expect(calls.map((call) => call.revoke)).toEqual([["x"]]);
  });
});

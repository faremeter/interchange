import { describe, test, expect } from "bun:test";

import type { DB } from "@intx/db";

import { pushSourceUpdates, pushSourceUpdatesSubtree } from "./credential-push";
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
      agentInstance: { findMany: reject },
    },
  } as unknown as DB["db"];
}

// eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- unused by the failing paths under test
const dummyRouter = {} as unknown as SidecarRouter;

describe("source push error containment", () => {
  test("pushSourceUpdatesSubtree resolves when descendant lookup fails", async () => {
    const result = await pushSourceUpdatesSubtree(
      rejectingDB(),
      dummyRouter,
      "tnt_1",
    );
    expect(result).toBeUndefined();
  });

  test("pushSourceUpdates resolves when the instance scan fails", async () => {
    const result = await pushSourceUpdates(rejectingDB(), dummyRouter, "tnt_1");
    expect(result).toBeUndefined();
  });
});

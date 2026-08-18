import { describe, expect, test } from "bun:test";

import type { DB } from "@intx/db";

import { resolveWorkflowSidecarPlacement } from "./workflow-allocation-service";

const TENANT_ID = "tnt-workflow-allocation";

// A minimal DB stand-in shaped for the two reads `resolveWorkflowSidecarPlacement`
// performs: `getAncestorChain` walks `tenant.findFirst` (a single root tenant here,
// so the chain is one hop) and the resolver then loads each tenant's config with
// `tenant.findMany`. Placement now derives from tenant config alone; the workflow
// definition never enters this decision.
function createFakeDb(config: unknown): DB["db"] {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return {
    query: {
      tenant: {
        findFirst: async () => ({ parentId: null }),
        findMany: async () => [{ id: TENANT_ID, config }],
      },
    },
  } as unknown as DB["db"];
}

describe("resolveWorkflowSidecarPlacement", () => {
  test("permits shared placement when tenant config requires nothing", async () => {
    const placement = await resolveWorkflowSidecarPlacement(
      createFakeDb(null),
      TENANT_ID,
    );
    expect(placement).toBeNull();
  });

  test("inherits exclusive placement from tenant config", async () => {
    const placement = await resolveWorkflowSidecarPlacement(
      createFakeDb({ sidecarPlacement: { sharing: "exclusive" } }),
      TENANT_ID,
    );
    expect(placement).toEqual({ sharing: "exclusive", reuse: "never" });
  });

  test("preserves same-deployment reuse from tenant config", async () => {
    const placement = await resolveWorkflowSidecarPlacement(
      createFakeDb({
        sidecarPlacement: { sharing: "exclusive", reuse: "same-deployment" },
      }),
      TENANT_ID,
    );
    expect(placement).toEqual({
      sharing: "exclusive",
      reuse: "same-deployment",
    });
  });
});

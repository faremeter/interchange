import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { createRunTokenStore } from "@intx/db";
import { deriveRunPrincipalId } from "@intx/hub-common";
import { createRunCredentialsLoader } from "@intx/hub-sessions";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
import {
  seedPrincipal,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

// The loader turns a deployment run's tokens into the credential delivery its
// consumers resolve the reserved hub handle through.

const TENANT = "tnt_runcreds";
const RUN_ID = "run_runcreds";
const ORIGIN = "https://hub.example.test";
const cipher = createTestCredentialCipher();

describe.skipIf(!harnessDbEnvAvailable())(
  "createRunCredentialsLoader (real DB)",
  () => {
    let h: TestDb;

    beforeAll(async () => {
      h = await createTestDb();
    });

    afterAll(async () => {
      await h.close();
    });

    beforeEach(async () => {
      await h.reset();
      await seedTenants(h.db, [{ id: TENANT }]);
      await seedWorkflowRun(h.db, {
        id: RUN_ID,
        tenantId: TENANT,
        anchorRunId: RUN_ID,
      });
    });

    function loader() {
      return createRunCredentialsLoader({
        db: h.db,
        runTokenStore: createRunTokenStore({ db: h.db, cipher }),
        hubApiOrigin: ORIGIN,
      });
    }

    test("delivers each token bound to the hub handle, pinned to the origin", async () => {
      const principalId = await deriveRunPrincipalId(TENANT, RUN_ID);
      await seedPrincipal(h.db, {
        id: principalId,
        tenantId: TENANT,
        kind: "workflow",
        refId: RUN_ID,
      });
      const store = createRunTokenStore({ db: h.db, cipher });
      await store.mintIfAbsent(
        { principalId, consumers: ["action:a", "tool:@acme/t"] },
        h.db,
      );
      const [token] = await store.loadDelivery(principalId);
      if (token === undefined) throw new Error("expected a token");

      const delivery = await loader()(RUN_ID);
      expect(delivery?.bindings).toEqual([
        {
          handle: "interchange.hub",
          credentialId: "hub:action:a",
          consumer: "action:a",
        },
        {
          handle: "interchange.hub",
          credentialId: "hub:tool:@acme/t",
          consumer: "tool:@acme/t",
        },
      ]);
      expect(delivery?.materials[0]).toEqual({
        credentialId: "hub:action:a",
        providerKey: "http",
        origin: ORIGIN,
        secret: token.token,
      });
    });

    test("is null for a run with no tokens", async () => {
      expect(await loader()(RUN_ID)).toBeNull();
    });

    test("is null for an unknown run", async () => {
      expect(await loader()("run_unknown")).toBeNull();
    });
  },
);

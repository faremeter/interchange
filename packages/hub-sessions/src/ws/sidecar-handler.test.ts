import {
  describe,
  expect,
  test,
  beforeAll,
  afterAll,
  beforeEach,
} from "bun:test";

import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";
import { configureSync, getConfig, resetSync } from "@intx/log";
import { MAX_CACHED_SENDER_ADDRESSES_FRAME } from "@intx/types/sidecar";

import {
  createSidecarRouter,
  MAX_RESYNC_SENDER_ADDRESSES,
  type SidecarAuthIdentity,
  type WsHandle,
} from "./sidecar-handler";

const identity: Extract<SidecarAuthIdentity, { kind: "allocated" }> = {
  kind: "allocated",
  sidecarId: "sc-allocated",
  allocationId: "alloc-1",
  tenantId: "tenant-1",
  anchorRunId: "run-anchor",
  workflowRunAddress: "workflow@exclusive",
  generation: 1,
};

const target = { allocationId: identity.allocationId, generation: 1 };
const config = {
  sessionId: "ses-exclusive",
  agentId: "workflow",
  tenantId: identity.tenantId,
  principalId: "principal-1",
  agentAddress: identity.workflowRunAddress,
  systemPrompt: "test",
  tools: [],
  grants: [],
  sources: [
    {
      id: "anthropic:test",
      provider: "anthropic",
      baseURL: "https://api.example.test",
      credentialId: "test-credential",
      model: "test",
    },
  ],
  defaultSource: "anthropic:test",
};

function createMockWs(): WsHandle & { sent: string[]; closed: boolean } {
  return {
    sent: [],
    closed: false,
    send(data: string) {
      this.sent.push(data);
    },
    close() {
      this.closed = true;
    },
  };
}

function lastFrame(
  ws: ReturnType<typeof createMockWs>,
): Record<string, unknown> {
  const raw = ws.sent.at(-1);
  if (raw === undefined) throw new Error("No frame was sent");
  return JSON.parse(raw);
}

async function tick(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

function createAllocatedRouter(
  overrides: Partial<Extract<SidecarAuthIdentity, { kind: "allocated" }>> = {},
) {
  const resolved = { ...identity, ...overrides };
  const router = createSidecarRouter({
    authenticateSidecar: async () => resolved,
    validateSidecarIdentity: async () => true,
    hubPublicKey: "a".repeat(64),
    requestTimeoutMs: 500,
    mailAckRetryIntervalMs: 10_000,
  });
  router.fenceAllocation(resolved.allocationId, resolved.generation);
  return router;
}

function createSenderKeyRouter(
  resolveSenderKeyStrict: (address: string) => Promise<string | null>,
) {
  const router = createSidecarRouter({
    authenticateSidecar: async () => identity,
    validateSidecarIdentity: async () => true,
    hubPublicKey: "a".repeat(64),
    requestTimeoutMs: 500,
    lookups: { resolveSenderKeyStrict },
  });
  router.fenceAllocation(identity.allocationId, identity.generation);
  return router;
}

async function connect(
  router: ReturnType<typeof createSidecarRouter>,
  agentAddresses: string[] = [],
  handshake: {
    frameType?: "register" | "reconnect";
    cachedSenderAddresses?: string[];
  } = {},
) {
  const ws = createMockWs();
  router.handleOpen(ws);
  router.handleMessage(
    ws,
    JSON.stringify({
      type: handshake.frameType ?? "register",
      sidecarId: identity.sidecarId,
      token: "token",
      agentAddresses,
      ...(handshake.cachedSenderAddresses !== undefined
        ? { cachedSenderAddresses: handshake.cachedSenderAddresses }
        : {}),
    }),
  );
  await tick();
  return ws;
}

async function waitForFrame(
  ws: ReturnType<typeof createMockWs>,
  predicate: (frame: Record<string, unknown>) => boolean,
  tries = 25,
): Promise<Record<string, unknown>> {
  for (let attempt = 0; attempt < tries; attempt += 1) {
    for (const raw of ws.sent) {
      const frame: Record<string, unknown> = JSON.parse(raw);
      if (predicate(frame)) return frame;
    }
    await tick();
  }
  throw new Error("expected frame was never sent");
}

describe("SidecarRouter allocation routing", () => {
  test("rejects a worker whose allocation generation is not fenced", async () => {
    const router = createSidecarRouter({
      authenticateSidecar: async () => identity,
      validateSidecarIdentity: async () => true,
    });
    const ws = await connect(router);

    expect(ws.closed).toBe(true);
    expect(router.getConnectedSidecars()).toEqual([]);
  });

  test("registers only the exact allocation address", async () => {
    const router = createAllocatedRouter();
    const ws = await connect(router, [identity.workflowRunAddress]);

    expect(ws.closed).toBe(false);
    expect(router.getConnectedSidecars()).toEqual([identity.sidecarId]);
    expect(router.getRoutableAddresses()).toEqual([
      identity.workflowRunAddress,
    ]);

    const rogue = createAllocatedRouter();
    const rogueWs = await connect(rogue, ["other@tenant"]);
    expect(rogueWs.closed).toBe(true);
    expect(rogue.getRoutableAddresses()).toEqual([]);
  });

  test("reconciles credentials for newly routed run addresses", async () => {
    const resynced: string[] = [];
    const runAddress = "run_alloc1@exclusive";
    const router = createSidecarRouter({
      authenticateSidecar: async () => ({
        ...identity,
        workflowRunAddress: runAddress,
      }),
      validateSidecarIdentity: async () => true,
      hubPublicKey: "a".repeat(64),
      requestTimeoutMs: 500,
      lookups: {
        resyncCredentials: (addr) => resynced.push(addr),
      },
    });
    router.fenceAllocation(identity.allocationId, identity.generation);
    const ws = await connect(router, [runAddress]);

    expect(ws.closed).toBe(false);
    expect(resynced).toEqual([runAddress]);
  });

  test("pushes a sender-key refresh for each reported cached sender", async () => {
    const key = "ab".repeat(32);
    const resolved: string[] = [];
    const router = createSenderKeyRouter((address) => {
      resolved.push(address);
      return Promise.resolve(address === "usr_alice@exclusive" ? key : null);
    });
    const ws = await connect(router, [], {
      cachedSenderAddresses: ["usr_alice@exclusive"],
    });

    const frame = await waitForFrame(
      ws,
      (f) => f.type === "sender.key.refresh",
    );
    expect(frame).toEqual({
      type: "sender.key.refresh",
      address: "usr_alice@exclusive",
      publicKey: key,
    });
    expect(resolved).toEqual(["usr_alice@exclusive"]);
  });

  test("pushes the refresh on the reconnect path too", async () => {
    const key = "cd".repeat(32);
    const router = createSenderKeyRouter(() => Promise.resolve(key));
    const ws = await connect(router, [], {
      frameType: "reconnect",
      cachedSenderAddresses: ["usr_carol@exclusive"],
    });

    const frame = await waitForFrame(
      ws,
      (f) => f.type === "sender.key.refresh",
    );
    expect(frame.address).toBe("usr_carol@exclusive");
    expect(frame.publicKey).toBe(key);
  });

  test("evicts a reported sender that resolves to no key", async () => {
    // A CONFIRMED null from the strict resolver is a deleted sender: the hub
    // pushes an evict so the sidecar drops the stale key, rather than the old
    // no-op-skip that left the revoked key cached forever.
    const resolved: string[] = [];
    const router = createSenderKeyRouter((address) => {
      resolved.push(address);
      return Promise.resolve(null);
    });
    const ws = await connect(router, [], {
      cachedSenderAddresses: ["usr_ghost@exclusive"],
    });

    const frame = await waitForFrame(ws, (f) => f.type === "sender.key.evict");
    expect(frame).toEqual({
      type: "sender.key.evict",
      address: "usr_ghost@exclusive",
    });
    expect(resolved).toEqual(["usr_ghost@exclusive"]);
    const refreshes = ws.sent
      .map((raw): Record<string, unknown> => JSON.parse(raw))
      .filter((f) => f.type === "sender.key.refresh");
    expect(refreshes).toEqual([]);
  });

  test("keeps the stale key (no evict) when resolution faults", async () => {
    // THE load-bearing distinction: a THROW is a transient/data fault, not a
    // deleted sender. Evicting a live key on a DB blip would be worse than
    // doing nothing, so a fault pushes NEITHER a refresh nor an evict -- the
    // sidecar keeps verifying against its cached key until the next reconnect.
    const resolved: string[] = [];
    const router = createSenderKeyRouter((address) => {
      resolved.push(address);
      return Promise.reject(new Error("simulated resolver fault"));
    });
    const ws = await connect(router, [], {
      cachedSenderAddresses: ["usr_faulty@exclusive"],
    });
    // Let the detached resync task run to completion.
    await tick();
    await tick();

    // The resolve was attempted, and neither a refresh nor an evict was pushed.
    expect(resolved).toEqual(["usr_faulty@exclusive"]);
    const reconciliations = ws.sent
      .map((raw): Record<string, unknown> => JSON.parse(raw))
      .filter(
        (f) => f.type === "sender.key.refresh" || f.type === "sender.key.evict",
      );
    expect(reconciliations).toEqual([]);
  });

  test("skips a run address in the report without resolving it", async () => {
    const key = "ef".repeat(32);
    const resolved: string[] = [];
    const router = createSenderKeyRouter((address) => {
      resolved.push(address);
      return Promise.resolve(key);
    });
    const ws = await connect(router, [], {
      cachedSenderAddresses: ["run_job1@exclusive", "usr_bob@exclusive"],
    });

    await waitForFrame(
      ws,
      (f) =>
        f.type === "sender.key.refresh" && f.address === "usr_bob@exclusive",
    );
    // The run address is filtered before resolution; only the user sender is
    // resolved and refreshed. The run address is never resolved and thus never
    // evicted -- its key is the immutable workflow_run.public_key.
    expect(resolved).toEqual(["usr_bob@exclusive"]);
    const reconciled = ws.sent
      .map((raw): Record<string, unknown> => JSON.parse(raw))
      .filter(
        (f) => f.type === "sender.key.refresh" || f.type === "sender.key.evict",
      )
      .map((f) => ({ type: f.type, address: f.address }));
    expect(reconciled).toEqual([
      { type: "sender.key.refresh", address: "usr_bob@exclusive" },
    ]);
  });

  test("deploys only through the exact allocation target", async () => {
    const router = createAllocatedRouter();
    const ws = await connect(router);
    const deployed = router.sendAgentDeployToAllocation(
      target,
      identity.workflowRunAddress,
      config,
    );
    await tick();
    expect(lastFrame(ws).type).toBe("agent.deploy");

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "agent.deploy.ack",
        agentAddress: identity.workflowRunAddress,
        publicKey: "b".repeat(64),
      }),
    );

    await expect(deployed).resolves.toEqual({ publicKey: "b".repeat(64) });
    await expect(
      router.sendAgentDeployToAllocation(
        { ...target, generation: 2 },
        identity.workflowRunAddress,
        config,
      ),
    ).rejects.toThrow("is not current");
  });

  test("restores workflow history without making the address routable", async () => {
    const router = createAllocatedRouter();
    const ws = await connect(router);
    const restored = router.sendWorkflowRunPackToAllocation(
      target,
      identity.workflowRunAddress,
      new Uint8Array([1, 2, 3]),
      "refs/heads/events",
      "d".repeat(40),
    );
    await tick();

    const frame = lastFrame(ws);
    expect(frame).toMatchObject({
      type: "repo.pack.done",
      agentAddress: identity.workflowRunAddress,
      repoId: {
        kind: "workflow-run",
        id: deriveWorkflowRunRepoId(identity.workflowRunAddress),
      },
    });
    expect(router.getRoutableAddresses()).toEqual([]);

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "repo.pack.ack",
        agentAddress: identity.workflowRunAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
      }),
    );
    await expect(restored).resolves.toBeUndefined();
  });

  test("redelivers retained mail once when the generation reconnects", async () => {
    const router = createAllocatedRouter();
    const first = await connect(router, [identity.workflowRunAddress]);
    expect(
      router.routeMail(
        identity.workflowRunAddress,
        "aGVsbG8=",
        "sender@example.test",
        "message-1",
      ),
    ).toBe(true);
    router.handleClose(first);

    const second = createMockWs();
    router.handleOpen(second);
    const reconnect = JSON.stringify({
      type: "reconnect",
      sidecarId: identity.sidecarId,
      token: "token",
      agentAddresses: [identity.workflowRunAddress],
    });
    router.handleMessage(second, reconnect);
    await tick();

    const count = () =>
      second.sent
        .map((raw) => JSON.parse(raw))
        .filter(
          (frame) =>
            frame.type === "mail.inbound" && frame.messageId === "message-1",
        ).length;
    expect(count()).toBe(1);
    router.handleMessage(second, reconnect);
    await tick();
    expect(count()).toBe(1);
  });

  test("delivers durable grants, mail, and signals to the exact generation", async () => {
    const router = createAllocatedRouter();
    const ws = await connect(router);
    const deployed = router.sendAgentDeployToAllocation(
      target,
      identity.workflowRunAddress,
      config,
    );
    await tick();
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "agent.deploy.ack",
        agentAddress: identity.workflowRunAddress,
        publicKey: "b".repeat(64),
      }),
    );
    await deployed;

    await router.sendWorkflowRunDispatchToAllocation(
      target,
      identity.workflowRunAddress,
      identity.workflowRunAddress,
      [],
      "cmF3LW1haWw=",
      "sender@example.test",
      "message-1",
    );
    expect(ws.sent.slice(-2).map((raw) => JSON.parse(raw).type)).toEqual([
      "run.grants",
      "mail.inbound",
    ]);

    await router.sendSignalDeliverToAllocation(target, {
      agentAddress: identity.workflowRunAddress,
      runId: identity.workflowRunAddress,
      signalName: "continue",
      signalId: "signal-1",
      payload: { approved: true },
    });
    expect(lastFrame(ws)).toMatchObject({
      type: "signal.deliver",
      signalId: "signal-1",
    });
  });

  test("advancing the fence disconnects and rejects the old generation", async () => {
    const router = createAllocatedRouter();
    const ws = await connect(router);
    router.fenceAllocation(identity.allocationId, 2);

    expect(ws.closed).toBe(true);
    expect(await router.isAllocatedSidecarReady(target)).toBe(false);
  });
});

type CapturedLog = {
  category: readonly string[];
  level: string;
  message: readonly unknown[];
};

async function waitUntil(predicate: () => boolean, tries = 200): Promise<void> {
  for (let attempt = 0; attempt < tries; attempt += 1) {
    if (predicate()) return;
    await tick();
  }
  throw new Error("condition was not met in time");
}

describe("SidecarRouter sender-key resync cap", () => {
  const capturedLogs: CapturedLog[] = [];
  let savedLogConfig: ReturnType<typeof getConfig>;

  beforeAll(() => {
    savedLogConfig = getConfig();
    configureSync({
      reset: true,
      sinks: {
        capture: (record) => {
          capturedLogs.push({
            category: record.category,
            level: record.level,
            message: record.message,
          });
        },
      },
      loggers: [
        { category: [], lowestLevel: "debug", sinks: ["capture"] },
        {
          category: ["logtape", "meta"],
          lowestLevel: "warning",
          sinks: ["capture"],
        },
      ],
    });
  });

  afterAll(() => {
    if (savedLogConfig) {
      configureSync({ reset: true, ...savedLogConfig });
    } else {
      resetSync();
    }
  });

  beforeEach(() => {
    capturedLogs.length = 0;
  });

  function capWarnings(): string[] {
    return capturedLogs
      .filter(
        (r) =>
          r.level === "warning" && r.message.join("").includes("resync cap"),
      )
      .map((r) => r.message.join(""));
  }

  function refreshAddresses(ws: ReturnType<typeof createMockWs>): unknown[] {
    return ws.sent
      .map((raw): Record<string, unknown> => JSON.parse(raw))
      .filter((f) => f.type === "sender.key.refresh")
      .map((f) => f.address);
  }

  test("resolves and refreshes every reported sender under the cap", async () => {
    const key = "ab".repeat(32);
    const router = createSenderKeyRouter(() => Promise.resolve(key));
    const senders = ["usr_a@exclusive", "usr_b@exclusive", "usr_c@exclusive"];
    const ws = await connect(router, [], { cachedSenderAddresses: senders });

    await waitUntil(() => refreshAddresses(ws).length === senders.length);
    expect([...refreshAddresses(ws)].sort()).toEqual([...senders].sort());
    expect(capWarnings()).toEqual([]);
  });

  test("caps an over-large reported set and warns", async () => {
    const key = "cd".repeat(32);
    const router = createSenderKeyRouter(() => Promise.resolve(key));
    const reported = Array.from(
      { length: MAX_RESYNC_SENDER_ADDRESSES + 1 },
      (_, i) => `usr_s${String(i)}@exclusive`,
    );
    // The cap runs on the de-duped set, so a generator collision would silently
    // hollow the test; assert distinctness before the behavioral checks.
    expect(new Set(reported).size).toBe(MAX_RESYNC_SENDER_ADDRESSES + 1);

    const ws = await connect(router, [], { cachedSenderAddresses: reported });
    await waitUntil(
      () => refreshAddresses(ws).length >= MAX_RESYNC_SENDER_ADDRESSES,
    );
    // Let any erroneous extra send settle, then confirm the cap held exactly.
    await tick();
    expect(refreshAddresses(ws)).toHaveLength(MAX_RESYNC_SENDER_ADDRESSES);

    const warnings = capWarnings();
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toContain(String(MAX_RESYNC_SENDER_ADDRESSES + 1));
  });

  test("the cachedSenderAddresses frame ceiling stays above the resync cap", () => {
    // These caps live in separate packages (`@intx/types` cannot import from
    // `@intx/hub-sessions`), so the invariant that lets this handler degrade
    // gracefully is only enforceable here, where both are visible. If the frame
    // ceiling ever slipped to or below the resync cap, an over-cap report would
    // fail the frame parse and drop the whole register frame -- turning the
    // graceful slice-and-log degrade above into a hard reconnect outage.
    expect(MAX_CACHED_SENDER_ADDRESSES_FRAME).toBeGreaterThan(
      MAX_RESYNC_SENDER_ADDRESSES,
    );
  });
});

describe("SidecarRouter pre-ack sender-key interlock", () => {
  const SENDER = "run_sender@exclusive";
  const RECIPIENT = "run_recipient@exclusive";
  const RAW_MESSAGE = "aGVsbG8=";
  const KEY = "ab".repeat(32);

  const raceIdentities: Record<
    string,
    Extract<SidecarAuthIdentity, { kind: "allocated" }>
  > = {
    "sc-sender": {
      kind: "allocated",
      sidecarId: "sc-sender",
      allocationId: "alloc-sender",
      tenantId: "tenant-1",
      anchorRunId: "run_sender",
      workflowRunAddress: SENDER,
      generation: 1,
    },
    "sc-recipient": {
      kind: "allocated",
      sidecarId: "sc-recipient",
      allocationId: "alloc-recipient",
      tenantId: "tenant-1",
      anchorRunId: "run_recipient",
      workflowRunAddress: RECIPIENT,
      generation: 1,
    },
  };

  function createInterlockRouter(
    resolveSenderKey: (address: string) => Promise<string | null>,
  ) {
    const router = createSidecarRouter({
      authenticateSidecar: async ({ sidecarId }) =>
        raceIdentities[sidecarId] ?? null,
      validateSidecarIdentity: async () => true,
      hubPublicKey: "a".repeat(64),
      requestTimeoutMs: 500,
      mailAckRetryIntervalMs: 10_000,
      // A wide TTL: any delivery the test observes came from a settle, not from
      // the deferred-mail TTL firing.
      disconnectQueueTTLMs: 60_000,
      lookups: {
        resolveSenderKey,
        materializeMailTriggeredRunGrants: async () => ({
          outcome: "materialized",
          stepGrants: [],
        }),
      },
    });
    router.fenceAllocation("alloc-sender", 1);
    router.fenceAllocation("alloc-recipient", 1);
    return router;
  }

  async function connectAs(
    router: ReturnType<typeof createSidecarRouter>,
    sidecarId: string,
    agentAddress: string,
  ) {
    const ws = createMockWs();
    router.handleOpen(ws);
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "register",
        sidecarId,
        token: sidecarId,
        agentAddresses: [agentAddress],
      }),
    );
    await tick();
    return ws;
  }

  function sendMail(
    router: ReturnType<typeof createSidecarRouter>,
    senderWs: ReturnType<typeof createMockWs>,
  ): void {
    router.handleMessage(
      senderWs,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: SENDER,
        recipients: [RECIPIENT],
        rawMessage: RAW_MESSAGE,
        delivered: false,
      }),
    );
  }

  function framesOfType(
    ws: ReturnType<typeof createMockWs>,
    type: string,
  ): Record<string, unknown>[] {
    return ws.sent
      .map((raw): Record<string, unknown> => JSON.parse(raw))
      .filter((f) => f.type === type);
  }

  test("delivers once, promptly, with the sender key when the key is recorded mid-resolve", async () => {
    // The highest-risk failure is a LOST WAKEUP: the key is recorded DURING the
    // resolveSenderKey await, so if the entry were parked AFTER the resolve, only
    // the TTL would free it. Force the settle to land WHILE handleMailOutbound is
    // mid-resolve: the stub, on its FIRST call, fires the settle before returning
    // null (as if the read observed the pre-ack state). The register-before-read
    // parking must already hold an entry for the settle to find.
    let resolveCalls = 0;
    const router = createInterlockRouter(async (address) => {
      expect(address).toBe(SENDER);
      resolveCalls += 1;
      if (resolveCalls === 1) {
        router.noteSenderDeploySettled(SENDER, { recorded: KEY });
        return null;
      }
      return KEY;
    });

    const senderWs = await connectAs(router, "sc-sender", SENDER);
    const recipientWs = await connectAs(router, "sc-recipient", RECIPIENT);

    // Establish the in-flight-deploy precondition the narrowed park gate requires:
    // mark the allocated key-record mid-flight so a null resolve parks rather than
    // delivering keyless.
    router.noteSenderDeployStarted(SENDER);
    sendMail(router, senderWs);
    const inbound = await waitForFrame(
      recipientWs,
      (f) => f.type === "mail.inbound" && f.authenticatedSender === SENDER,
    );

    // 1. The recipient never sees the sender as `unknown`: the sender key IS
    //    co-delivered on the run.grants barrier that precedes the mail.
    const grants = framesOfType(recipientWs, "run.grants");
    expect(grants).toHaveLength(1);
    expect(grants[0]?.senderIdentities).toEqual([
      { address: SENDER, publicKey: KEY },
    ]);
    expect(inbound.authenticatedSender).toBe(SENDER);

    // 2. Delivered EXACTLY ONCE at the wire -- the idempotent claim-by-key keeps
    //    the settle's re-drive and the inline branch from both delivering.
    expect(
      framesOfType(recipientWs, "mail.inbound").filter(
        (f) => f.authenticatedSender === SENDER,
      ),
    ).toHaveLength(1);

    // 3. Delivered PROMPTLY: the wide TTL never fired, so the delivery came from
    //    the settle-driven re-drive, not the backstop.
  });

  test("surfaces a pre-ack sender's parked mail as undelivered when its deploy fails", async () => {
    const undelivered: { rawMessage: string; recipients: string[] }[] = [];
    // The key is never recorded: the sender's deploy fails after the mail parks.
    const router = createInterlockRouter(async () => null);
    router.events.on("mail.outbound.undelivered", (payload) => {
      undelivered.push(payload);
    });

    const senderWs = await connectAs(router, "sc-sender", SENDER);
    const recipientWs = await connectAs(router, "sc-recipient", RECIPIENT);

    // Establish the in-flight-deploy precondition the narrowed park gate requires:
    // mark the allocated key-record mid-flight so a null resolve parks rather than
    // delivering keyless.
    router.noteSenderDeployStarted(SENDER);
    sendMail(router, senderWs);
    await tick();
    // The mail is parked, not delivered.
    expect(framesOfType(recipientWs, "mail.inbound")).toHaveLength(0);

    router.noteSenderDeploySettled(SENDER, { failed: "deploy timed out" });
    await tick();

    // The parked mail is surfaced for external relay rather than hung forever,
    // and it never reaches the recipient keyless.
    expect(undelivered).toEqual([
      { rawMessage: RAW_MESSAGE, recipients: [RECIPIENT] },
    ]);
    expect(framesOfType(recipientWs, "mail.inbound")).toHaveLength(0);
  });

  test("a settle only wakes mail parked under a byte-identical sender address", async () => {
    // The allocated path passes its deploy address as the settle key, and that
    // address is byte-identical to the sender address the run's mail was sent
    // under. Prove the interlock is key-sensitive: a settle for any other
    // address does NOT wake this sender's parked mail; only the exact address
    // does. A diverging key would silently drop the mail to the TTL.
    //
    // `recordedKey` models the durable public-key write the settle reports: null
    // while the run is pre-ack (mail parks), then the key once recorded, so the
    // settle-driven re-drive resolves it exactly as production does after the
    // write happens-before the settle.
    let recordedKey: string | null = null;
    const router = createInterlockRouter(async () => recordedKey);
    const senderWs = await connectAs(router, "sc-sender", SENDER);
    const recipientWs = await connectAs(router, "sc-recipient", RECIPIENT);

    // Establish the in-flight-deploy precondition the narrowed park gate requires:
    // mark the allocated key-record mid-flight so a null resolve parks rather than
    // delivering keyless.
    router.noteSenderDeployStarted(SENDER);
    sendMail(router, senderWs);
    await tick();

    // The key is now recorded. A settle keyed on a DIFFERENT address matches no
    // parked entry, so the sender's mail stays parked even though its key would
    // now resolve -- proving the wake is keyed on the exact sender address.
    recordedKey = KEY;
    router.noteSenderDeploySettled(RECIPIENT, { recorded: KEY });
    await tick();
    expect(framesOfType(recipientWs, "mail.inbound")).toHaveLength(0);

    // The byte-identical settle wakes it and it delivers with the key.
    router.noteSenderDeploySettled(SENDER, { recorded: KEY });
    const inbound = await waitForFrame(
      recipientWs,
      (f) => f.type === "mail.inbound" && f.authenticatedSender === SENDER,
    );
    expect(inbound.authenticatedSender).toBe(SENDER);
    const grants = framesOfType(recipientWs, "run.grants");
    expect(grants).toHaveLength(1);
    expect(grants[0]?.senderIdentities).toEqual([
      { address: SENDER, publicKey: KEY },
    ]);
  });

  test("delivers a run sender's mail keyless when no deploy is in flight and the key does not resolve", async () => {
    // A run sender whose deploy already acked (or never had one) has NO settle
    // coming: parking a null resolve would strand the mail to the TTL. With no
    // in-flight deploy the narrowed gate resolves inline and delivers keyless,
    // co-delivering no sender identity, rather than parking. This is the
    // already-settled-run / transient-fault case that must NOT be held.
    const undelivered: { rawMessage: string; recipients: string[] }[] = [];
    const router = createInterlockRouter(async () => null);
    router.events.on("mail.outbound.undelivered", (payload) => {
      undelivered.push(payload);
    });

    const senderWs = await connectAs(router, "sc-sender", SENDER);
    const recipientWs = await connectAs(router, "sc-recipient", RECIPIENT);

    // No noteSenderDeployStarted and no pendingDeploys entry: no settle is
    // guaranteed, so the gate does not park.
    sendMail(router, senderWs);
    const inbound = await waitForFrame(
      recipientWs,
      (f) => f.type === "mail.inbound" && f.authenticatedSender === SENDER,
    );
    expect(inbound.authenticatedSender).toBe(SENDER);

    // The mail is delivered exactly once, keyless: the run.grants barrier carries
    // no sender identity because the key did not resolve.
    expect(
      framesOfType(recipientWs, "mail.inbound").filter(
        (f) => f.authenticatedSender === SENDER,
      ),
    ).toHaveLength(1);
    const grants = framesOfType(recipientWs, "run.grants");
    expect(grants).toHaveLength(1);
    expect(grants[0]?.senderIdentities).toBeUndefined();

    // It was neither parked nor TTL-dropped: nothing surfaced as undelivered.
    expect(undelivered).toEqual([]);
  });

  test("does not park a run sender's mail to an external recipient even with a deploy in flight", async () => {
    // The co-delivered key is only ever consumed by a run recipient. Mail to a
    // purely external recipient never uses it, so the gate takes the normal path
    // immediately -- even while the run's deploy is in flight -- rather than
    // holding the mail. The external recipient surfaces as undelivered for the
    // host to relay, which proves the mail was not parked.
    const EXTERNAL_RECIPIENT = "alice@external.com";
    const undelivered: { rawMessage: string; recipients: string[] }[] = [];
    const router = createInterlockRouter(async () => null);
    router.events.on("mail.outbound.undelivered", (payload) => {
      undelivered.push(payload);
    });

    const senderWs = await connectAs(router, "sc-sender", SENDER);

    // A deploy IS in flight for the sender, so the run-sender park branch would
    // trigger if it were reached; the external-only recipient short-circuits it.
    router.noteSenderDeployStarted(SENDER);
    router.handleMessage(
      senderWs,
      JSON.stringify({
        type: "mail.outbound",
        senderAddress: SENDER,
        recipients: [EXTERNAL_RECIPIENT],
        rawMessage: RAW_MESSAGE,
        delivered: false,
      }),
    );
    await tick();

    // The mail took the normal path immediately: the external recipient is
    // reported undelivered for host relay rather than being held for a settle.
    expect(undelivered).toEqual([
      { rawMessage: RAW_MESSAGE, recipients: [EXTERNAL_RECIPIENT] },
    ]);
  });
});

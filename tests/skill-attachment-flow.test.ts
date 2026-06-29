// End-to-end skill-materialization slice.
//
// Topology: fully in-process. No subprocess sidecar, no real WS, no
// real hub HTTP. The harness from @intx/inference-testing mocks the
// inference layer via in-process fetch DI; hub-side services and the
// sidecar-side applyAssetPack are called directly as libraries.
//
// The test creates a synthetic skill asset (SKILL.md frontmatter + a
// sample sibling file), attaches it to a fixture agent, manually
// replays the session-launch materialization steps (createPack,
// applyAssetPack, session_asset insert), and drives the harness's
// mocked inference loop with a system prompt carrying the
// `<available_skills>` stanza. Three paths are asserted, in this
// order:
//
//   (b) The agent's read_file tool dispatches returned the SKILL.md
//       body and the sibling file content the materialization wrote
//       to disk. Behavioural assertion first: if materialization is
//       broken, this fails before any prompt-related assertion runs.
//   (c) The session_asset row recorded the materialization with the
//       expected instance_id, mount_path, source_commit_sha (from
//       RepoStore.resolveRef), and a non-empty 64-char hex
//       asset_pack_sha.
//   (a) The outbound inference request body carried the
//       `<available_skills>` stanza qualifying the skill as
//       `my-skills/greet`.
//
// The pieces the test does NOT cover are unit-tested in their owning
// tasks: WS pack-send orchestration, the transactional manifest
// insert wrapped around production sessionService.launchSession, and
// the stanza builder's escaping rules. This test exercises the
// integration the unit tests cannot: that materialization produces a
// workspace tree the harness's tool dispatcher can read against.

import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { applyAssetPack } from "@intx/hub-agent";
import { generateKeyPair } from "@intx/crypto";
import { generateId } from "@intx/hub-common";
import {
  buildAvailableSkillsStanza,
  createAgentRepoStore,
  createAssetService,
  getSkillIndex,
  type AgentRepoStore,
  type AssetService,
  type AvailableSkillEntry,
  type SkillIndexEntry,
} from "@intx/hub-sessions";
import { setupHarness, wire } from "@intx/inference-testing";
import type { Harness } from "@intx/inference-testing";
import type { ConversationTurn, InferenceSource } from "@intx/types/runtime";

// ---------------------------------------------------------------------------
// Synthetic skill fixture
// ---------------------------------------------------------------------------

const SKILL_FRONTMATTER_DESCRIPTION =
  "Greet the user in their language of choice";
const SKILL_BODY =
  `---\nname: greet\ndescription: ${SKILL_FRONTMATTER_DESCRIPTION}\n---\n` +
  `\nBody for the greet skill.\n`;
const SIBLING_BODY = "## Examples\n\nhello world\n";

const ASSET_NAME = "my-skills";
const SKILL_NAME = "greet";
const ASSET_REF = "refs/heads/main";
// Default mount path resolution lives in session-service. Production
// uses `skills/<asset.name>/` when the agent_asset row's mount_path
// is null. The test mirrors that derivation here so the materialized
// tree and the recorded manifest agree.
const MOUNT_PATH = `skills/${ASSET_NAME}/`;

// ---------------------------------------------------------------------------
// Tempdir bookkeeping
// ---------------------------------------------------------------------------

const tempDirs: string[] = [];

async function mkTemp(prefix: string): Promise<string> {
  const d = await fsp.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fsp.rm(d, { recursive: true, force: true })),
  );
});

// ---------------------------------------------------------------------------
// Fixture FK chain
//
// The session_asset row's instance_id must reference an agent_instance
// id. The test builds the minimal chain that satisfies the production
// schema's column shape — tenant -> principal -> agent -> instance.
// The DB stub is closed: it does not enforce FKs, but the test still
// constructs the chain so the manifest row's instance_id has a real
// upstream row it could resolve against in a non-stub setting.
// ---------------------------------------------------------------------------

type TenantRow = { id: string };
type PrincipalRow = { id: string; tenantId: string };
type AgentRow = { id: string; tenantId: string; principalId: string };
type AgentInstanceRow = {
  id: string;
  agentId: string;
  tenantId: string;
  principalId: string;
  address: string;
};
type AssetRow = {
  id: string;
  tenantId: string;
  kind: string;
  name: string;
  displayName: string | null;
  creatorPrincipalId: string | null;
  createdAt: Date;
  updatedAt: Date;
};
type AgentAssetRow = {
  id: string;
  agentId: string;
  assetId: string;
  ref: string;
  mountPath: string | null;
  accessMode: string;
  createdAt: Date;
};
type SessionAssetRow = {
  instanceId: string;
  agentAssetId: string;
  mountPath: string;
  assetPackSha: string;
  sourceCommitSha: string;
  materializedAt: Date;
};

type DbStub = {
  // Fixtures.
  tenants: TenantRow[];
  principals: PrincipalRow[];
  agents: AgentRow[];
  instances: AgentInstanceRow[];
  // Tables the AssetService writes through.
  assets: AssetRow[];
  agentAssets: AgentAssetRow[];
  // Table the manifest-insert step writes through.
  sessionAssets: SessionAssetRow[];
  // The opaque DB handle passed to createAssetService and to the
  // manifest insert.
  db: {
    insert(table: unknown): {
      values(
        row: AssetRow | AgentAssetRow | SessionAssetRow,
      ): Promise<unknown[]> | { returning: () => Promise<unknown[]> };
    };
    query: {
      asset: {
        findFirst: (args: { where: unknown }) => Promise<AssetRow | undefined>;
      };
    };
    select: (cols: unknown) => {
      from: (t: unknown) => {
        innerJoin: (
          t2: unknown,
          on: unknown,
        ) => {
          where: (w: unknown) => Promise<unknown[]>;
        };
      };
    };
  };
  // The test signals which row the next findFirst({where}) should
  // return. The asset-service.test.ts pattern: the drizzle `where`
  // value is opaque, so the stub takes a setter from the test.
  nextFindFirstAssetId: (id: string) => void;
  nextSelectAgentId: (id: string) => void;
};

function createDbStub(): DbStub {
  const tenants: TenantRow[] = [];
  const principals: PrincipalRow[] = [];
  const agents: AgentRow[] = [];
  const instances: AgentInstanceRow[] = [];
  const assets: AssetRow[] = [];
  const agentAssets: AgentAssetRow[] = [];
  const sessionAssets: SessionAssetRow[] = [];

  let lastFindFirstAssetId: string | null = null;
  let lastSelectAgentId: string | null = null;

  function isAssetRow(
    row: AssetRow | AgentAssetRow | SessionAssetRow,
  ): row is AssetRow {
    return "tenantId" in row && "kind" in row && "displayName" in row;
  }

  function isAgentAssetRow(
    row: AssetRow | AgentAssetRow | SessionAssetRow,
  ): row is AgentAssetRow {
    return "agentId" in row && "assetId" in row && "ref" in row;
  }

  function isSessionAssetRow(
    row: AssetRow | AgentAssetRow | SessionAssetRow,
  ): row is SessionAssetRow {
    return (
      "instanceId" in row && "agentAssetId" in row && "assetPackSha" in row
    );
  }

  const builder = {
    values(row: AssetRow | AgentAssetRow | SessionAssetRow) {
      if (isAssetRow(row)) {
        assets.push(row);
        return {
          returning: () => Promise.resolve([row]),
        };
      }
      if (isAgentAssetRow(row)) {
        agentAssets.push(row);
        return {
          returning: () => Promise.resolve([row]),
        };
      }
      if (isSessionAssetRow(row)) {
        sessionAssets.push(row);
        // The manifest-insert path in production does not chain
        // `.returning()`; it awaits the values() promise directly.
        return Promise.resolve([row]);
      }
      throw new Error("DbStub.insert.values: unrecognized row shape");
    },
  };

  function findFirstAsset(): Promise<AssetRow | undefined> {
    if (lastFindFirstAssetId === null) return Promise.resolve(undefined);
    const match = assets.find((a) => a.id === lastFindFirstAssetId);
    lastFindFirstAssetId = null;
    return Promise.resolve(match);
  }

  function joinAgentAssets(): Promise<unknown[]> {
    if (lastSelectAgentId === null) return Promise.resolve([]);
    const id = lastSelectAgentId;
    lastSelectAgentId = null;
    const joined = agentAssets
      .filter((aa) => aa.agentId === id)
      .map((aa) => {
        const a = assets.find((x) => x.id === aa.assetId);
        if (a === undefined) {
          throw new Error(
            `inconsistent stub: agent_asset ${aa.id} references missing asset ${aa.assetId}`,
          );
        }
        return { agentAsset: aa, asset: a };
      });
    return Promise.resolve(joined);
  }

  const db = {
    insert(_t: unknown) {
      return builder;
    },
    query: {
      asset: {
        findFirst: (_args: { where: unknown }) => findFirstAsset(),
      },
    },
    select(_cols: unknown) {
      return {
        from: (_t: unknown) => ({
          innerJoin: (_t2: unknown, _on: unknown) => ({
            where: (_w: unknown) => joinAgentAssets(),
          }),
        }),
      };
    },
  };

  return {
    tenants,
    principals,
    agents,
    instances,
    assets,
    agentAssets,
    sessionAssets,
    db,
    nextFindFirstAssetId: (id) => {
      lastFindFirstAssetId = id;
    },
    nextSelectAgentId: (id) => {
      lastSelectAgentId = id;
    },
  };
}

// ---------------------------------------------------------------------------
// AssetService factory wrapping the stub DB plus a real RepoStore
// rooted in os.tmpdir(). Real isomorphic-git operations run through
// the RepoStore — the skill kind handler's validatePush parses the
// SKILL.md frontmatter and the substrate produces a real packfile
// that applyAssetPack materializes against the workspace.
// ---------------------------------------------------------------------------

async function setupAssetService(): Promise<{
  service: AssetService;
  agentRepoStore: AgentRepoStore;
  db: DbStub;
}> {
  const db = createDbStub();
  const signingKey = await generateKeyPair();
  const dataDir = await mkTemp("intr95-data-");
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase cannot be structurally satisfied in tests
  const dbHandle = db.db as unknown as Parameters<
    typeof createAssetService
  >[0]["db"];
  // AgentRepoStore wires the kind-keyed substrate (agent-state + skill
  // handlers, the appropriate authorize gate) against `dataDir` and
  // exposes the underlying RepoStore via `.repoStore`. The asset
  // service writes against that substrate. Using the agent-side
  // factory keeps the test off the package's internal repo-store
  // module while still exercising the production wiring.
  const agentRepoStore = createAgentRepoStore({ dataDir, signingKey });
  const service = createAssetService({
    db: dbHandle,
    repoStore: agentRepoStore.repoStore,
  });
  return { service, agentRepoStore, db };
}

// ---------------------------------------------------------------------------
// Stanza rendering uses the production `buildAvailableSkillsStanza`
// exported by @intx/hub-sessions. Sharing the renderer with production
// means a drift in either side surfaces as a test failure here, not as
// a divergent reimplementation that hides it.
// ---------------------------------------------------------------------------

function collectSkillEntries(
  assetName: string,
  mountPath: string,
  index: SkillIndexEntry[],
): AvailableSkillEntry[] {
  return index.map((e) => ({
    qualifiedName: `${assetName}/${e.name}`,
    description: e.description,
    workspacePath: `workspace/${mountPath}${e.workspaceSubpath}`,
  }));
}

// ---------------------------------------------------------------------------
// Inference scaffolding
// ---------------------------------------------------------------------------

const ANTHROPIC_SOURCE: InferenceSource = {
  id: "anthropic:claude-test",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "test",
  model: "claude-test",
};

const USAGE_HEAD = {
  input: 10,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};
const USAGE_TAIL = {
  input: 0,
  output: 5,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

function userTurn(text: string): ConversationTurn {
  return {
    role: "user",
    content: [{ type: "text", text }],
    timestamp: 0,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

async function parseRequestBody(
  request: Request,
): Promise<Record<string, unknown>> {
  const text = await request.text();
  const parsed: unknown = JSON.parse(text);
  if (!isRecord(parsed)) {
    throw new Error("captured request body was not a JSON object");
  }
  return parsed;
}

function extractSystemText(body: Record<string, unknown>): string {
  // Anthropic places `system` as an array of typed blocks; the stanza
  // appears in the text field of the first block.
  const system = body["system"];
  if (!Array.isArray(system)) {
    throw new Error("expected anthropic body.system to be an array");
  }
  const blocks: string[] = [];
  for (const entry of system) {
    if (!isRecord(entry)) continue;
    const text = entry["text"];
    if (typeof text === "string") blocks.push(text);
  }
  return blocks.join("\n");
}

let activeHarness: Harness | null = null;

afterEach(() => {
  if (activeHarness !== null) {
    activeHarness.dispose();
    activeHarness = null;
  }
});

// ---------------------------------------------------------------------------
// Hub principal for the AssetService write path. The skill kind
// handler's authorize gate only permits hub-principal writes; the
// asset-service test uses the same shape. The Principal type is an
// internal hub-sessions type; an inline `{ kind: "hub" }` literal
// satisfies the structural constraint where it is consumed.
// ---------------------------------------------------------------------------

const HUB_PRINCIPAL = { kind: "hub" as const };

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("skill attachment flow (end-to-end)", () => {
  test("materializes the asset pack, records the manifest, and surfaces the stanza", async () => {
    // -----------------------------------------------------------------------
    // Step 1: DB stub + RepoStore + AssetService, plus the FK fixture
    // chain. The session_asset row's instance_id comes from the
    // instance row constructed below.
    // -----------------------------------------------------------------------
    const { service, agentRepoStore, db } = await setupAssetService();
    const repoStore = agentRepoStore.repoStore;

    const tenantId = generateId("tenant");
    const principalId = generateId("principal");
    const agentId = generateId("agent");
    const instanceId = generateId("instance");
    const address = `${agentId}@test.local`;

    db.tenants.push({ id: tenantId });
    db.principals.push({ id: principalId, tenantId });
    db.agents.push({ id: agentId, tenantId, principalId });
    db.instances.push({
      id: instanceId,
      agentId,
      tenantId,
      principalId,
      address,
    });

    // -----------------------------------------------------------------------
    // Step 2: Create + populate + attach the skill asset.
    // -----------------------------------------------------------------------
    const asset = await service.createAsset({
      tenantId,
      kind: "skill",
      name: ASSET_NAME,
      displayName: "My skills",
      creatorPrincipalId: principalId,
    });

    db.nextFindFirstAssetId(asset.id);
    const populate = await service.populateAsset({
      assetId: asset.id,
      ref: ASSET_REF,
      principal: HUB_PRINCIPAL,
      tree: {
        files: {
          [`${SKILL_NAME}/SKILL.md`]: SKILL_BODY,
          [`${SKILL_NAME}/examples.md`]: SIBLING_BODY,
        },
        message: "Seed greet skill",
      },
    });
    expect(populate.commitSha).toMatch(/^[0-9a-f]{40}$/);

    // mountPath is not a user-supplied column on agent_asset in v1;
    // session start resolves it as `skills/${asset.name}/` via the
    // production resolveMountPath rule.
    const attachment = await service.attachAsset({
      agentId,
      assetId: asset.id,
      ref: ASSET_REF,
    });
    expect(attachment.assetId).toBe(asset.id);

    // -----------------------------------------------------------------------
    // Step 3: Manually replay materialization. Production
    // launchSession orchestrates the same calls over WS; the test
    // calls them directly to keep everything in-process.
    // -----------------------------------------------------------------------
    const repoId = { kind: "skill" as const, id: asset.id };

    const sourceCommitSha = await repoStore.resolveRef(
      HUB_PRINCIPAL,
      repoId,
      ASSET_REF,
    );
    if (sourceCommitSha === null) {
      throw new Error("resolveRef returned null after populateAsset");
    }
    expect(sourceCommitSha).toMatch(/^[0-9a-f]{40}$/);

    const packResult = await repoStore.createPack(
      HUB_PRINCIPAL,
      repoId,
      ASSET_REF,
    );
    const assetPackSha = Buffer.from(
      await crypto.subtle.digest("SHA-256", new Uint8Array(packResult.pack)),
    ).toString("hex");
    expect(assetPackSha).toMatch(/^[0-9a-f]{64}$/);

    const workspaceRoot = await mkTemp("intr95-ws-");
    await applyAssetPack({
      workspaceRoot,
      mountPath: MOUNT_PATH,
      pack: packResult.pack,
      ref: packResult.ref,
      commitSha: packResult.commitSha,
    });

    // Manifest insert: the production manifest row is shaped from
    // (instanceId, agentAssetId, mountPath, assetPackSha,
    // sourceCommitSha, materializedAt). The test mirrors that.
    await db.db.insert(null).values({
      instanceId,
      agentAssetId: attachment.id,
      mountPath: MOUNT_PATH,
      assetPackSha,
      sourceCommitSha,
      materializedAt: new Date(),
    });

    // -----------------------------------------------------------------------
    // Step 4: Compose the synthetic deploy systemPrompt with the
    // <available_skills> stanza, then drive the harness inference
    // loop.
    // -----------------------------------------------------------------------
    const skillIndex = getSkillIndex(asset.id, ASSET_REF);
    expect(skillIndex).toHaveLength(1);
    const skillEntry = skillIndex[0];
    if (skillEntry === undefined) throw new Error("unreachable");
    expect(skillEntry.name).toBe(SKILL_NAME);
    expect(skillEntry.description).toBe(SKILL_FRONTMATTER_DESCRIPTION);

    const stanza = buildAvailableSkillsStanza(
      collectSkillEntries(ASSET_NAME, MOUNT_PATH, skillIndex),
    );

    const baseSystemPrompt = "You are a test agent.";
    const systemPromptWithStanza = `${baseSystemPrompt}\n\n${stanza}\n`;

    const harness = setupHarness();
    activeHarness = harness;

    // read_file handler closure: strips the `workspace/` prefix and
    // resolves under the materialized workspaceRoot. Returning the
    // file's UTF-8 contents from disk is the behavioural pin — if
    // applyAssetPack did not write the files, this handler fails
    // before any prompt-related assertion runs.
    type ReadFileDispatch = {
      requestedPath: string;
      content: string;
    };
    const readDispatches: ReadFileDispatch[] = [];
    harness.scenario.onTool("read_file", (args: unknown) => {
      if (!isRecord(args)) {
        throw new Error("read_file handler: arguments was not a JSON object");
      }
      const requested = args["path"];
      if (typeof requested !== "string") {
        throw new Error("read_file handler: arguments.path must be a string");
      }
      const stripped = requested.replace(/^workspace\//, "");
      const onDisk = path.join(workspaceRoot, stripped);
      const content = fs.readFileSync(onDisk, "utf-8");
      readDispatches.push({ requestedPath: requested, content });
      return { content };
    });

    // Capture the outbound inference request body for path (a).
    let capturedRequest: Request | null = null;
    const responseStream = harness.scenario.createStream();
    const skillMdRequestedPath = `workspace/${MOUNT_PATH}${SKILL_NAME}/SKILL.md`;
    const examplesMdRequestedPath = `workspace/${MOUNT_PATH}${SKILL_NAME}/examples.md`;
    const responseChunks = wire.completeResponse("anthropic", {
      text: "All done.",
      toolCalls: [
        {
          callId: "call_read_skill_md",
          name: "read_file",
          argsJSON: JSON.stringify({ path: skillMdRequestedPath }),
        },
        {
          callId: "call_read_examples_md",
          name: "read_file",
          argsJSON: JSON.stringify({ path: examplesMdRequestedPath }),
        },
      ],
      headUsage: USAGE_HEAD,
      tailUsage: USAGE_TAIL,
    });
    responseStream.enqueueAll(responseChunks, { startAt: 10 });
    const responseClose = 10 + responseChunks.length;

    harness.scenario.whenRequestMatches((req) => {
      capturedRequest = req.clone();
      return true;
    }, responseStream);

    let seq = 0;
    const collect = (async () => {
      for await (const _ev of harness.runInference({
        turns: [userTurn("Use the greet skill.")],
        source: ANTHROPIC_SOURCE,
        inferenceOptions: {
          systemPrompt: systemPromptWithStanza,
          tools: [
            {
              name: "read_file",
              description: "Read a workspace-relative file.",
              inputSchema: {
                type: "object",
                properties: { path: { type: "string" } },
                required: ["path"],
              },
            },
          ],
        },
        nextSeq: () => ++seq,
      })) {
        // The test reads tool dispatches via the handler closure and
        // request bodies via the captured Request; the event stream
        // itself is not asserted on. We drain it so the inference
        // generator runs to completion.
      }
    })();
    await harness.advanceTo(responseClose + 10);
    await collect;

    // -----------------------------------------------------------------------
    // Assertions, in plan-mandated order.
    // -----------------------------------------------------------------------

    // Path (b): the read_file handler ran twice against the
    // materialized workspace and returned the file contents.
    expect(readDispatches).toHaveLength(2);
    const skillDispatch = readDispatches.find((d) =>
      d.requestedPath.endsWith("SKILL.md"),
    );
    const examplesDispatch = readDispatches.find((d) =>
      d.requestedPath.endsWith("examples.md"),
    );
    if (skillDispatch === undefined) {
      throw new Error("read_file SKILL.md dispatch not observed");
    }
    if (examplesDispatch === undefined) {
      throw new Error("read_file examples.md dispatch not observed");
    }
    expect(skillDispatch.requestedPath).toBe(skillMdRequestedPath);
    expect(skillDispatch.content).toBe(SKILL_BODY);
    expect(examplesDispatch.requestedPath).toBe(examplesMdRequestedPath);
    expect(examplesDispatch.content).toBe(SIBLING_BODY);

    // Path (c): the manifest row recorded the materialization.
    expect(db.sessionAssets).toHaveLength(1);
    const manifestRow = db.sessionAssets[0];
    if (manifestRow === undefined) throw new Error("unreachable");
    expect(manifestRow.instanceId).toBe(instanceId);
    expect(manifestRow.agentAssetId).toBe(attachment.id);
    expect(manifestRow.mountPath).toBe(MOUNT_PATH);
    expect(manifestRow.sourceCommitSha).toBe(sourceCommitSha);
    expect(manifestRow.assetPackSha).toMatch(/^[0-9a-f]{64}$/);
    expect(manifestRow.assetPackSha).toBe(assetPackSha);

    // Path (a): the outbound inference body carried the stanza,
    // qualifying the skill as `my-skills/greet` with its description.
    if (capturedRequest === null) {
      throw new Error("outbound inference matcher never fired");
    }
    const body = await parseRequestBody(capturedRequest);
    const systemText = extractSystemText(body);
    expect(systemText).toContain("<available_skills>");
    expect(systemText).toContain(`<name>${ASSET_NAME}/${SKILL_NAME}</name>`);
    expect(systemText).toContain(
      `<description>${SKILL_FRONTMATTER_DESCRIPTION}</description>`,
    );
    expect(systemText).toContain(
      `<path>workspace/${MOUNT_PATH}${SKILL_NAME}/</path>`,
    );
  });
});

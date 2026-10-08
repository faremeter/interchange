// Seedable human-in-the-loop workflow fixture:
// `draft -> awaitSignal{name:"approve"} -> publish`, with both
// step-agents authored inline on the definition. `bin/seed.ts` pushes it
// as a code-sourced CODEBASE (a `package.json` declaring
// `interchange.workflow` plus a bundled entry module) to a `workflow`-kind
// asset named by `WORKFLOW_FIXTURE_ASSET_NAME` on the Acme tenant.
// `buildWorkflowFixture` is the live definition and single source of
// truth: `buildWorkflowCodebaseTree` bundles it into the pushed entry.
//
// The seed plants the signal-delivery grant at the `workflow-run:*`
// wildcard scope (see `WORKFLOW_RUN_GRANT_RESOURCE`) because the
// deployment id the signal route gates on is minted at deploy time.

import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  awaitSignal,
  defineWorkflow,
  step,
  type WorkflowDefinition,
} from "@intx/workflow";
import { defineAgent } from "@intx/agent";

/**
 * Asset name for the seeded workflow definition. Lowercase-kebab so the
 * smart-HTTP repo path (`assets/workflow/<name>.git`) needs no escaping.
 */
export const WORKFLOW_FIXTURE_ASSET_NAME = "approval-flow";

/**
 * Package identity the codebase tree's `package.json` declares. The
 * loader keys the closure's top-level entry on it, so a stable value
 * keeps the seeded asset legible.
 */
export const WORKFLOW_FIXTURE_PACKAGE_NAME = "@intx-seed/approval-flow";
export const WORKFLOW_FIXTURE_PACKAGE_VERSION = "1.0.0";

/**
 * File the bundled entry module is written to in the codebase tree, and
 * the package-relative `interchange.workflow` entry pointing at it. The
 * `.mjs` extension marks it as an ES module the loader imports directly.
 */
export const WORKFLOW_FIXTURE_ENTRY_FILE = "workflow.mjs";
export const WORKFLOW_FIXTURE_ENTRY = `./${WORKFLOW_FIXTURE_ENTRY_FILE}`;

/** Top-level manifest file name of the codebase tree. */
export const WORKFLOW_FIXTURE_PACKAGE_JSON_FILE = "package.json";

/** Signal the middle node waits on. */
export const WORKFLOW_FIXTURE_SIGNAL_NAME = "approve";

/**
 * Inference provider/model the inline step-agents prefer. Matches the
 * `anthropic` provider the seed wires (with a tenant credential) so a
 * deployed step resolves against a real seeded source.
 */
export const WORKFLOW_FIXTURE_INFERENCE_PROVIDER = "anthropic";
export const WORKFLOW_FIXTURE_INFERENCE_MODEL = "claude-sonnet-5";

/**
 * Resource pattern (with `manage`) the seed plants so the seeded
 * operator can deliver the `approve` signal. The signal route resolves
 * the gate to `workflow-run:<deploymentId>`; since the deployment id is
 * minted at deploy time, the grant uses the `*` wildcard the authz glob
 * matcher resolves against any concrete deployment.
 */
export const WORKFLOW_RUN_GRANT_RESOURCE = "workflow-run:*";
export const WORKFLOW_RUN_GRANT_ACTION = "manage";

/**
 * Authored trigger address. The capability walk derives `mail.address`
 * / `mail.send` approvals from it; the runtime inbound address is
 * derived independently from the deployment id at deploy time, so this
 * only needs a well-formed address whose domain matches the Acme tenant
 * (`<slug>.localhost`) for a coherent approval set.
 */
const WORKFLOW_FIXTURE_TRIGGER_ADDRESS = "workflow-launch@acme.localhost";

function inlineStepAgent(args: {
  id: string;
  systemPrompt: string;
}): ReturnType<typeof defineAgent> {
  return defineAgent({
    id: args.id,
    systemPrompt: args.systemPrompt,
    tools: [],
    capabilities: [],
    inference: {
      sources: [
        {
          provider: WORKFLOW_FIXTURE_INFERENCE_PROVIDER,
          model: WORKFLOW_FIXTURE_INFERENCE_MODEL,
        },
      ],
    },
  });
}

/** Build the inline human-in-the-loop workflow definition, fresh per call. */
export function buildWorkflowFixture(): WorkflowDefinition {
  const draftAgent = inlineStepAgent({
    id: "draft-agent",
    systemPrompt:
      "You are the drafting step of an approval workflow. Produce a concise draft of the requested deliverable so a human reviewer can approve or reject it.",
  });
  const publishAgent = inlineStepAgent({
    id: "publish-agent",
    systemPrompt:
      "You are the publishing step of an approval workflow. The draft has been approved by a human; finalize and publish it, then report what was published.",
  });

  return defineWorkflow({
    id: "wf_approval_flow",
    trigger: { type: "mail", to: WORKFLOW_FIXTURE_TRIGGER_ADDRESS },
    steps: {
      draft: step({ agent: draftAgent }),
      approval: awaitSignal({
        name: WORKFLOW_FIXTURE_SIGNAL_NAME,
        after: ["draft"],
      }),
      // `publish` runs after `approval` but publishes the draft, not
      // the signal payload: the default-input convention wires a single
      // `after` step to its predecessor's output, and the `awaitSignal`
      // output is `null` for a bare approval. Reading the draft
      // explicitly keeps the approved content flowing to publish.
      publish: step({
        agent: publishAgent,
        after: ["approval"],
        input: { from: "steps.draft.output" },
      }),
    },
  });
}

/**
 * A code-sourced workflow codebase tree: the top-level `package.json`
 * (declaring `interchange.workflow`) plus the bundled entry module,
 * keyed by their package-relative file names. The seed writes these
 * into the workflow asset's git tree and pushes them.
 */
export interface WorkflowCodebaseTree {
  readonly files: Record<string, string>;
}

const binDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(binDir, "..");

/**
 * Bundle a workflow entry module into one self-contained ESM string,
 * inlining every `@intx/*` import to source so the sidecar-materialized
 * closure evaluates it with no bare import left to resolve. Mirrors the
 * bundling the source-workflow e2e fixture performs.
 *
 * @throws if `Bun.build` produces no output, or the bundle still
 *   carries a bare `@intx/` import
 */
async function bundleWorkflowEntry(entrySource: string): Promise<string> {
  const scratchDir = await mkdtemp(
    path.join(tmpdir(), "approval-flow-bundle-"),
  );
  try {
    const entrySrcPath = path.join(scratchDir, "workflow-entry.ts");
    await writeFile(entrySrcPath, entrySource);

    const built = await Bun.build({
      entrypoints: [entrySrcPath],
      target: "bun",
      format: "esm",
      throw: true,
      plugins: [
        {
          name: "resolve-intx-to-source",
          setup(build) {
            build.onResolve({ filter: /^@intx\// }, (args) => {
              const fromDir = args.importer.startsWith(`${repoRoot}${path.sep}`)
                ? path.dirname(args.importer)
                : repoRoot;
              return { path: Bun.resolveSync(args.path, fromDir) };
            });
          },
        },
      ],
    });

    const artifact = built.outputs[0];
    if (artifact === undefined) {
      throw new Error("bundleWorkflowEntry: Bun.build produced no output");
    }
    const code = await artifact.text();
    if (code.includes("@intx/")) {
      throw new Error(
        "bundleWorkflowEntry: bundle still carries a bare @intx import",
      );
    }
    return code;
  } finally {
    await rm(scratchDir, { recursive: true, force: true });
  }
}

/**
 * Build the code-sourced codebase tree: a `package.json` declaring the
 * `interchange.workflow` entry, plus that entry bundled from
 * `buildWorkflowFixture` -- the live definition and single source of
 * truth. The bundled entry re-imports `buildWorkflowFixture` from this
 * module and re-exports its result, so the closure the sidecar evaluates
 * resolves to the same definition the seed's row projection hashes.
 */
export async function buildWorkflowCodebaseTree(): Promise<WorkflowCodebaseTree> {
  const fixtureModulePath = fileURLToPath(import.meta.url);
  const entrySource =
    `import { buildWorkflowFixture } from ${JSON.stringify(fixtureModulePath)};\n` +
    `export const workflow = buildWorkflowFixture();\n`;
  const workflowModule = await bundleWorkflowEntry(entrySource);

  const packageManifest = {
    name: WORKFLOW_FIXTURE_PACKAGE_NAME,
    version: WORKFLOW_FIXTURE_PACKAGE_VERSION,
    interchange: { workflow: WORKFLOW_FIXTURE_ENTRY },
  };

  return {
    files: {
      [WORKFLOW_FIXTURE_PACKAGE_JSON_FILE]: `${JSON.stringify(packageManifest, null, 2)}\n`,
      [WORKFLOW_FIXTURE_ENTRY_FILE]: workflowModule,
    },
  };
}

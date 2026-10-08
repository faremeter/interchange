// Pinned tool-package materialization for one workflow step. The child
// factory takes `materializeStepTools` as a required argument; this module
// is the sidecar process's implementation, reading the step's deploy tree
// and loading the pinned closure. The factory never imports it.

import path from "node:path";

import { agentDir, readDeployTree } from "@intx/hub-agent/paths";
import { materializeToolPackages } from "@intx/tool-packaging";
import { parseRunAddress } from "@intx/types";
import { baseStepId } from "@intx/workflow";
import { resolveStepAddress } from "@intx/workflow-deploy";
import type {
  StepToolCacheConfig,
  StepToolMaterialization,
} from "@intx/workflow-host/child";

import {
  readRegistries,
  resolveHostPlatform,
} from "./sidecar-materialization-config";

/**
 * Resolve the on-disk directory holding a step's deploy tree.
 *
 * The deploy tree (`deploy/prompt.md`, `deploy/tool-packages-manifest.json`,
 * `deploy/asset-mounts.json`) is shipped to the sidecar per step by the
 * hub's `launchSession` deploy-pack push, which lands it in the LEGACY
 * per-agent directory keyed by the step's sanitized mail address (see
 * `@intx/hub-agent` `agentDir`). It is NOT in the substrate's
 * `agent-state/<id>` layout -- the multi-step deploy path never pushes
 * step `agent-state` packs to the child's substrate.
 *
 * The step's mail address is `resolveStepAddress(...)`, the single owner
 * of the head/step collapse: for a single-step deployment the lone step
 * IS the head (the deployment mailbox itself), so the tree is read at the
 * head; for multi-step it is `deriveStepAddress(runId, stepId, domain)`.
 * The `runId`/`domain` come from the deployment mailbox address the
 * supervisor threaded into the child as `MAILBOX_ADDRESS`
 * (`<runId>@<domain>`); `stepCount` comes from the host (via
 * `substrateEnv`) so producer and consumer never derive divergent
 * addresses.
 */
export function stepDeployTreeDir(args: {
  dataDir: string;
  mailboxAddress: string;
  stepId: string;
  stepCount: number;
}): string {
  const parsed = parseRunAddress(args.mailboxAddress);
  if (parsed === null) {
    throw new Error(
      `sidecar workflow-child step tools: deployment mailbox address ${JSON.stringify(args.mailboxAddress)} is not a parseable run address; cannot locate the step's deploy tree`,
    );
  }
  // A `map` iteration runs under a scoped step id `<base>[<index>]`, but
  // deploy stages one deploy tree per base step, so the scoped id resolves
  // to its base address -- every iteration reads the base step's tree.
  // `baseStepId` is the identity on an unscoped id, so a plain step is
  // unaffected.
  const stepAddress = resolveStepAddress({
    runId: parsed.runId,
    stepId: baseStepId(args.stepId),
    domain: parsed.domain,
    stepCount: args.stepCount,
  });
  return agentDir(args.dataDir, stepAddress);
}

/**
 * Read a step's deploy tree and materialize its pinned tool-package
 * closure. The tarball cache and the tool instance dir are rooted under
 * the supplied per-step `storeDir` (the Phase-1 per-step state root) so
 * concurrent steps/agents in one child never collide on cache or
 * apply-state paths.
 *
 * A deploy with no tool-package manifest yields empty factories -- the
 * legitimate `rawManifestBytes === undefined` case. A manifest that is
 * present but fails to load surfaces loudly through
 * `materializeToolPackages` (the throw path), never a silent
 * empty-tools fallback that would mask a broken deploy.
 */
export async function materializeStepTools(args: {
  dataDir: string;
  mailboxAddress: string;
  stepId: string;
  stepCount: number;
  /** Per-step state root; cache + instance dir + workspace live under it. */
  storeDir: string;
  cache: StepToolCacheConfig;
}): Promise<StepToolMaterialization> {
  const deployTreeDir = stepDeployTreeDir({
    dataDir: args.dataDir,
    mailboxAddress: args.mailboxAddress,
    stepId: args.stepId,
    stepCount: args.stepCount,
  });
  const deployTree = await readDeployTree(deployTreeDir);

  // Root the tarball cache per step so concurrent steps in one child do
  // not race on the content-addressable cache root. The cache is
  // content-addressed and safe to share globally, but the design calls
  // for a per-step cacheRoot so a wedged or partially-written apply in
  // one step cannot corrupt another's view.
  const cacheRoot = path.join(args.storeDir, "tarball-cache");

  // Asset-mounted tool tarballs are staged by the hub's asset-pack push
  // into the step's LEGACY agent dir workspace (the same dir the deploy
  // tree lives in), not under the per-step store dir. Point the loader's
  // asset resolution there while keeping the apply-state + cache rooted
  // per step under `storeDir`.
  //
  // `<deployTreeDir>/workspace` (read-only staged assets, keyed by the
  // BASE step) and the agent's read-write workdir `<storeDir>/workspace`
  // (keyed by the SCOPED step) share a leaf name but are deliberately
  // different roots -- a map iteration reads one shared deploy tree while
  // each iteration writes its own scratch. Do not unify them.
  const assetRoot = path.join(deployTreeDir, "workspace");

  const materialized = await materializeToolPackages({
    rawManifestBytes: deployTree.toolPackageManifestRaw,
    assetMounts: deployTree.assetMounts,
    storeDir: args.storeDir,
    assetRoot,
    agentAddress: args.mailboxAddress,
    cacheRoot,
    cacheMaxBytes: args.cache.cacheMaxBytes,
    registryMaxTarballBytes: args.cache.registryMaxTarballBytes,
    registries: readRegistries(),
    host: resolveHostPlatform(),
  });
  return {
    factories: materialized.factories,
    pluginFactories: materialized.pluginFactories,
  };
}

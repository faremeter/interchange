import type { GrantRequirement, GrantWalkSnapshot } from "@intx/types";
import { hexEncode } from "@intx/types/hex";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import {
  collectDeclaredPluginNames,
  projectLiveToInert,
} from "@intx/workflow/projection";

import { encodeEnvelope, type FrameEnvelope } from "../ipc/envelope";
import { signHmac } from "../ipc/crypto";
import {
  loadWorkflowDefinitionFromClosure,
  loadWorkflowDirectorRegistryFromClosure,
  loadWorkflowPluginToolDefinitionsFromClosure,
} from "../workflow-definition-loader";
import { parseProbeChildEnv } from "./env";
import { enrichProbeError } from "./errors";
import type { ProbeResultPayload, WalkCapabilities } from "./protocol";

/**
 * One line the child writes to a sink. Production wraps `process.stdout`;
 * tests inject a capture. The bytes are handed to the OS before the child
 * exits so the result is not truncated.
 */
export type ProbeChildLineWriter = (line: string) => Promise<void>;

export interface RunProbeChildOpts {
  /** Raw env the child reads its anchors from (defaults to `process.env`). */
  rawEnv?: Readonly<Record<string, string | undefined>>;
  /** Result-line sink (defaults to a drained `process.stdout` write). */
  writeLine?: ProbeChildLineWriter;
}

/**
 * The airlocked child's whole job: read the materialized package dir and
 * IPC anchors from its fresh env, load+evaluate the workflow entry, run
 * the capability walk plus the live->inert projector, and ship the inert
 * projection + advisory grant set + wire hash back inside one
 * HMAC-signed result frame.
 *
 * An evaluation failure (malformed code, an entry that throws, a package
 * with no `interchange.workflow`) is caught and shipped as an `ok: false`
 * frame so the host reaps cleanly and answers `workflow.probe.error`
 * with the reason -- rather than the child crashing and the host seeing a
 * bare "exited without result".
 */
export async function runWorkflowProbeChildFromProcessEnv(
  walkCapabilities: WalkCapabilities,
  opts: RunProbeChildOpts = {},
): Promise<void> {
  const rawEnv = opts.rawEnv ?? process.env;
  const writeLine = opts.writeLine ?? defaultStdoutWriteLine;
  const { channelId, hmacKey, packageDir } = parseProbeChildEnv(rawEnv);

  let payload: ProbeResultPayload;
  try {
    payload = await computeProbePayload(packageDir, walkCapabilities);
  } catch (err) {
    payload = { ok: false, error: enrichProbeError(err) };
  }

  const envelope: FrameEnvelope = { seq: 0, channelId, payload };
  const envelopeBytes = encodeEnvelope(envelope);
  const mac = hexEncode(await signHmac(envelopeBytes, hmacKey));
  await writeLine(`${JSON.stringify({ envelope, mac })}\n`);
}

async function computeProbePayload(
  packageDir: string,
  walkCapabilities: WalkCapabilities,
): Promise<ProbeResultPayload> {
  const definition = await loadWorkflowDefinitionFromClosure({ packageDir });
  const projection = projectLiveToInert(definition);
  const wireHash = await computeWireDefinitionHash(projection);
  // Compose the director registry from the SAME closure the run-child will,
  // so the `director:<id>` grants advertised here match what the runtime
  // resolves. Built-ins-only when the closure ships no `interchange.directors`.
  const directors = await loadWorkflowDirectorRegistryFromClosure({
    packageDir,
  });
  // Load the static tool `definitions` each declared plugin package
  // contributes from the SAME closure, so the walk emits `tool:<name>`
  // grants for plugin-contributed tools (Tier-2 governance). A plugin
  // package reaches an agent only through `env.plugins`, so its tool grant
  // surface is invisible to the walk otherwise -- the run-child would then
  // load the plugin from the closure and the reactor would fail closed on
  // an un-approved `tool:<name>`. Loading here (over the frozen closure the
  // run-child also materializes from) keeps the approved snapshot and the
  // runtime plugin in lockstep.
  const pluginToolDefinitions =
    await loadWorkflowPluginToolDefinitionsFromClosure({
      packageDir,
      plugins: collectDeclaredPluginNames(definition),
    });
  const walk = walkCapabilities(definition, directors, pluginToolDefinitions);
  // Fail closed on an unresolved director: the runtime does not re-gate
  // `director:<id>` against the approved grant set, so this advertisement is
  // the only approval checkpoint for a director. Shipping an ok probe whose
  // grant set silently omits a director the runtime would still try to
  // resolve would let the operator approve an incomplete manifest. Mirrors
  // the live-authored approval gate (`createApprovalSetGate`).
  const [unresolved] = walk.unresolvedDirectors;
  if (unresolved !== undefined) {
    return { ok: false, error: `unresolvable director: ${unresolved}` };
  }
  return {
    ok: true,
    projection,
    grants: collectDeploymentGrants(walk),
    grantWalkSnapshot: buildGrantWalkSnapshot(
      walk,
      definition.grantRequirements,
    ),
    wireHash,
  };
}

/**
 * Flatten the per-step walk output into the deployment-wide advisory
 * grant set: the deduplicated, sorted union of every step's grant
 * strings. Sorting makes the shipped set order-independent.
 */
function collectDeploymentGrants(walk: ReturnType<WalkCapabilities>): string[] {
  const grants = new Set<string>();
  for (const declarations of walk.perStep.values()) {
    for (const grant of declarations.grants) {
      grants.add(grant);
    }
  }
  return [...grants].sort();
}

/**
 * Serialize the un-flattened capability walk into a plain-data
 * `GrantWalkSnapshot`: the per-step grant declarations (each step's grant
 * strings plus its tool-grant `grantEffects` map, converted from the
 * walk's `Map` to a plain object) and the definition's full, unfiltered
 * `grantRequirements`. Unlike `collectDeploymentGrants`, this preserves
 * the per-step grouping and the effect data the flattened set discards.
 * A definition that declares no requirements snapshots an empty list.
 */
function buildGrantWalkSnapshot(
  walk: ReturnType<WalkCapabilities>,
  grantRequirements: readonly GrantRequirement[] | undefined,
): GrantWalkSnapshot {
  const perStep = [...walk.perStep].map(([stepId, declarations]) => ({
    stepId,
    grants: [...declarations.grants],
    grantEffects: Object.fromEntries(declarations.grantEffects),
  }));
  return {
    perStep,
    grantRequirements: [...(grantRequirements ?? [])],
  };
}

function defaultStdoutWriteLine(line: string): Promise<void> {
  return new Promise((resolve, reject) => {
    process.stdout.write(line, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

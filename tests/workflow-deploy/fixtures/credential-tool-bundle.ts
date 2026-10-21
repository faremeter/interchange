// A real, type-checked credential-consuming tool bundle for the single-step
// credential-tool e2e. `credential-tool-workflow.ts` inlines THIS module into
// the workflow's source closure via `bundleWorkflowEntry`, so the e2e drives
// the production tool loader against a genuine bundle rather than a
// hand-written string blob.
//
// The tool resolves one credential handle from the host-assembled
// `credentials` capability into an http mediated credential,
// then fetches a caller-chosen path on the credential's pinned origin. The
// bearer the origin records is the e2e's end-to-end proof that the delivery
// rail carried the secret to the tool -- through the deploy frame (or a live
// rotation push) into the child's cell, past Gate 2, and out as an authed
// request -- without ever exposing the secret on the tool's own API surface.

import fs from "node:fs";
import path from "node:path";

import { defineTool, type BaseEnv, type ToolBundle } from "@intx/agent";
import { HUB_CREDENTIAL_HANDLE } from "@intx/types/credential-delivery";
import type { RuntimeCapabilities } from "@intx/types/runtime-capabilities";

/**
 * The credential handle the tool resolves. `declaredCredentialProbe` declares
 * it, so deploy refuses that tool unless a root binding names this handle for
 * it; `credentialProbe` declares nothing, so its binding is not checked.
 */
export const CREDENTIAL_HANDLE = "api-token";

/**
 * The factory id. On the source-ref lineage it is the tool's consumer
 * identity: Gate 2 checks `toolConsumer(BUNDLE_ID)`, and a binding names it as
 * its `package`.
 */
export const BUNDLE_ID = "@intx/tools-credential-probe/sidecar-bundle";

/** The probe definition's name; a source-ref tool keeps its bare name. */
export const PROBE_DEFINITION_NAME = "probe";

/**
 * Env contract for the probe bundle: `BaseEnv` (for `workdir`) plus the
 * host-assembled `capabilities`, from which the tool resolves the
 * consumer-scoped `credentials` handle the step tool builder layered on.
 */
interface CredentialToolEnv extends BaseEnv {
  capabilities: RuntimeCapabilities;
}

function probeBundle(handle: string): (env: CredentialToolEnv) => ToolBundle {
  return (env) => ({
    definitions: [
      {
        name: "probe",
        description:
          "Fetch a path on the bound credential's pinned origin through its mediated http handle.",
        inputSchema: {
          type: "object",
          properties: {
            path: {
              type: "string",
              description:
                "Path (relative to the pinned origin) or absolute URL to fetch through the handle.",
            },
            sentinel: {
              type: "string",
              description:
                "Filename to write the fetch outcome under in the step workspace.",
            },
          },
          required: ["path", "sentinel"],
        },
      },
    ],
    run: async (call) => {
      // `path` and `sentinel` are required by the inputSchema; surface a bad
      // call rather than silently substituting a default and fetching or
      // writing the wrong thing.
      const requestPath = call.arguments.path;
      if (typeof requestPath !== "string") {
        throw new Error(
          `credential-probe: required argument "path" must be a string, got ${typeof requestPath}`,
        );
      }
      const sentinel = call.arguments.sentinel;
      if (typeof sentinel !== "string") {
        throw new Error(
          `credential-probe: required argument "sentinel" must be a string, got ${typeof sentinel}`,
        );
      }

      // Resolve the mediated credential by the declared handle. Gate 2 runs
      // here (inside `resolve`): an unauthorized consumer throws before any
      // request is shaped, which is exactly the fail-closed the negative case
      // asserts.
      const credentials = env.capabilities.resolve("credentials");
      const mediated = await credentials.resolve(handle);
      if (mediated.kind !== "http") {
        throw new Error(
          `credential-probe expected an http mediated credential, got ${mediated.kind}`,
        );
      }

      // The handle injects the bearer per request and pins to the credential's
      // origin: a relative path resolves against that origin; an absolute
      // cross-origin URL is refused by the handle (never carrying the bearer
      // off the pinned host).
      const response = await mediated.fetch(requestPath);
      const body = await response.text();

      await fs.promises.mkdir(env.workdir, { recursive: true });
      await fs.promises.writeFile(
        path.join(env.workdir, sentinel),
        JSON.stringify({ status: response.status, body }),
      );

      return {
        callId: call.id,
        content: `probe fetched ${requestPath} -> ${response.status}`,
      };
    },
  });
}

/**
 * The probe as a tool that declares no credentials. Deploy does not hold its
 * binding to a declared set, so a deployment can leave it unbound and deliver
 * its credential later over the live `credentials.update` channel.
 */
export const credentialProbe = defineTool<CredentialToolEnv>({
  id: BUNDLE_ID,
  requires: ["capabilities"],
  definitions: [{ name: "probe" }],
  factory: probeBundle(CREDENTIAL_HANDLE),
});

/**
 * The probe as a tool that declares `CREDENTIAL_HANDLE`. Deploy refuses it
 * without a root binding for that handle, and the child reconciles the
 * declaration against the delivered bindings before the tool runs.
 */
export const declaredCredentialProbe = defineTool<CredentialToolEnv>({
  id: BUNDLE_ID,
  requires: ["capabilities"],
  definitions: [{ name: "probe" }],
  credentials: [{ handle: CREDENTIAL_HANDLE }],
  factory: probeBundle(CREDENTIAL_HANDLE),
});

/**
 * The probe as a tool that declares the reserved hub handle. Deploy binds the
 * handle to the run's hub token with no authored binding, so the probe's
 * fetch goes to the hub REST API as the run.
 */
export const hubProbe = defineTool<CredentialToolEnv>({
  id: BUNDLE_ID,
  requires: ["capabilities"],
  definitions: [{ name: "probe" }],
  credentials: [{ handle: HUB_CREDENTIAL_HANDLE }],
  factory: probeBundle(HUB_CREDENTIAL_HANDLE),
});

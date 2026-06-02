/**
 * Smart-HTTP `info/refs` advertisement for upload-pack and
 * receive-pack. Produces a pkt-line stream of the shape:
 *
 *   <pkt># service=git-upload-pack\n</pkt>
 *   0000
 *   <pkt><sha> <ref>\0<caps>\n</pkt>      // first ref carries caps
 *   <pkt><sha> <ref>\n</pkt>              // subsequent refs (no NUL)
 *   ...
 *   0000
 *
 * Refs are filtered against `principal.tokenClaims.refPattern` via
 * the shared simple-glob matcher before advertisement so a token
 * cannot learn about refs outside its declared scope.
 *
 * When no refs survive filtering (or the repo is empty) the
 * advertisement emits a single zero-oid `capabilities^{}` record so
 * that stock `git clone` and `git ls-remote` accept the response.
 */

import type { RepoId } from "@intx/types/sidecar";
import { glob } from "@intx/hub-common";

import { writePktLine, writeFlush } from "./pkt-line";

/**
 * Principal contract consumed by the advertise layer. Kept narrow on
 * purpose: the advertiser only needs the ref-scope claim, so any
 * principal shape carrying `tokenClaims.refPattern` satisfies it.
 */
export type AdvertisePrincipal = {
  readonly kind: string;
  readonly tokenClaims: {
    readonly refPattern: string;
  };
};

export type RefEntry = {
  readonly name: string;
  readonly sha: string;
};

/**
 * Ref-listing capability the advertiser depends on. The substrate or
 * a repo-direct adapter implements this; the advertiser does not care
 * which.
 */
export interface RefSource {
  listRefs(principal: AdvertisePrincipal, repoId: RepoId): Promise<RefEntry[]>;
}

const INTERCHANGE_HUB_AGENT = "interchange-hub/0.0.0";

const BASELINE_CAPABILITIES = [
  "side-band-64k",
  "ofs-delta",
  "object-format=sha1",
  `agent=${INTERCHANGE_HUB_AGENT}`,
];

export const UPLOAD_PACK_CAPABILITIES = BASELINE_CAPABILITIES.join(" ");

export const RECEIVE_PACK_CAPABILITIES = [
  ...BASELINE_CAPABILITIES,
  "report-status",
].join(" ");

export const EMPTY_REPO_OID = "0".repeat(40);

type Service = "git-upload-pack" | "git-receive-pack";

function filterAndSort(
  refs: readonly RefEntry[],
  refPattern: string,
): RefEntry[] {
  const allowed = refs.filter((r) => glob.match(refPattern, r.name));
  allowed.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  return allowed;
}

async function writeAdvertisement(
  writer: WritableStreamDefaultWriter<Uint8Array>,
  service: Service,
  capabilities: string,
  refs: readonly RefEntry[],
): Promise<void> {
  await writePktLine(writer, `# service=${service}\n`);
  await writeFlush(writer);

  if (refs.length === 0) {
    await writePktLine(
      writer,
      `${EMPTY_REPO_OID} capabilities^{}\0${capabilities}\n`,
    );
  } else {
    const first = refs[0];
    if (first === undefined) {
      throw new Error("advertise-refs: unreachable empty ref list");
    }
    await writePktLine(writer, `${first.sha} ${first.name}\0${capabilities}\n`);
    for (let i = 1; i < refs.length; i++) {
      const ref = refs[i];
      if (ref === undefined) {
        throw new Error("advertise-refs: unreachable undefined ref entry");
      }
      await writePktLine(writer, `${ref.sha} ${ref.name}\n`);
    }
  }

  await writeFlush(writer);
}

function advertiseStream(
  refSource: RefSource,
  principal: AdvertisePrincipal,
  repoId: RepoId,
  service: Service,
  capabilities: string,
): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    async start(controller) {
      const sink = new WritableStream<Uint8Array>({
        write(chunk) {
          controller.enqueue(chunk);
        },
      });
      const writer = sink.getWriter();
      try {
        const allRefs = await refSource.listRefs(principal, repoId);
        const refs = filterAndSort(allRefs, principal.tokenClaims.refPattern);
        await writeAdvertisement(writer, service, capabilities, refs);
        await writer.close();
        controller.close();
      } catch (err) {
        await writer.abort(err).catch(() => undefined);
        controller.error(err);
      }
    },
  });
}

export async function advertiseUploadPack(
  refSource: RefSource,
  principal: AdvertisePrincipal,
  repoId: RepoId,
): Promise<ReadableStream<Uint8Array>> {
  return advertiseStream(
    refSource,
    principal,
    repoId,
    "git-upload-pack",
    UPLOAD_PACK_CAPABILITIES,
  );
}

export async function advertiseReceivePack(
  refSource: RefSource,
  principal: AdvertisePrincipal,
  repoId: RepoId,
): Promise<ReadableStream<Uint8Array>> {
  return advertiseStream(
    refSource,
    principal,
    repoId,
    "git-receive-pack",
    RECEIVE_PACK_CAPABILITIES,
  );
}

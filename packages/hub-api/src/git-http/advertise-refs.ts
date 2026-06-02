/**
 * Smart-HTTP `info/refs` advertisement for upload-pack and
 * receive-pack. Produces a pkt-line stream of the shape:
 *
 *   <pkt># service=git-upload-pack\n</pkt>
 *   0000
 *   <pkt><sha> HEAD\0<caps with symref=HEAD:<target>>\n</pkt>
 *   <pkt><sha> <ref>\n</pkt>
 *   ...
 *   0000
 *
 * The advertisement begins with HEAD when the repo has a born HEAD;
 * HEAD carries the capability list NUL-separated and a
 * `symref=HEAD:<target>` token so stock `git clone` lands on a real
 * branch instead of leaving the working tree unborn. The visible refs
 * follow in lexicographic order.
 *
 * Refs are filtered against `principal.tokenClaims.refPattern` via
 * the shared simple-glob matcher before advertisement so a token
 * cannot learn about refs outside its declared scope. HEAD is itself a
 * symbolic alias and not subject to refPattern filtering, but it is
 * only advertised when its target ref survives that filter — a token
 * that cannot see the target cannot learn about HEAD either.
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
 * Result of resolving HEAD on the underlying repo. `symbolicTarget` is
 * the ref HEAD symbolically points at (e.g. `refs/heads/main`); `sha`
 * is the SHA that target currently resolves to. Both fields are
 * required: a detached HEAD or an unborn HEAD is signalled by
 * returning `null` from `resolveHead`, not by populating one field and
 * leaving the other empty.
 */
export type HeadResolution = {
  readonly symbolicTarget: string;
  readonly sha: string;
};

/**
 * Ref-listing capability the advertiser depends on. The substrate or
 * a repo-direct adapter implements this; the advertiser does not care
 * which.
 */
export interface RefSource {
  listRefs(principal: AdvertisePrincipal, repoId: RepoId): Promise<RefEntry[]>;
  /**
   * Resolve HEAD into the ref it symbolically targets plus the SHA
   * that ref currently resolves to. Returns `null` when HEAD is
   * unborn (no commits yet), detached, or the on-disk repo does not
   * exist. The advertiser uses the result to emit the
   * `symref=HEAD:<target>` capability so stock `git clone` checks out
   * a real branch.
   */
  resolveHead(
    principal: AdvertisePrincipal,
    repoId: RepoId,
  ): Promise<HeadResolution | null>;
}

const INTERCHANGE_HUB_AGENT = "interchange-hub/0.0.0";

const BASELINE_CAPABILITIES = [
  "ofs-delta",
  "object-format=sha1",
  `agent=${INTERCHANGE_HUB_AGENT}`,
];

export const UPLOAD_PACK_CAPABILITIES = [
  "side-band-64k",
  ...BASELINE_CAPABILITIES,
].join(" ");

// receive-pack does not advertise `side-band-64k`: the handler returns
// the `report-status` payload as raw pkt-lines, not channel-wrapped.
// Advertising side-band-64k would invite the client to expect a
// channel-framed response, which `handleReceivePack` does not emit.
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
  head: HeadResolution | null,
): Promise<void> {
  await writePktLine(writer, `# service=${service}\n`);
  await writeFlush(writer);

  // HEAD is only advertised when its symbolic target survives ref
  // filtering: a token that cannot see the target ref has no business
  // learning about HEAD either, and stock git would otherwise resolve
  // the symref against a ref it never received and abort.
  const headTargetVisible =
    head !== null &&
    refs.some((r) => r.name === head.symbolicTarget && r.sha === head.sha);

  if (refs.length === 0) {
    await writePktLine(
      writer,
      `${EMPTY_REPO_OID} capabilities^{}\0${capabilities}\n`,
    );
  } else if (head !== null && headTargetVisible) {
    const capsWithSymref = `${capabilities} symref=HEAD:${head.symbolicTarget}`;
    await writePktLine(writer, `${head.sha} HEAD\0${capsWithSymref}\n`);
    for (const ref of refs) {
      await writePktLine(writer, `${ref.sha} ${ref.name}\n`);
    }
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
        const [allRefs, head] = await Promise.all([
          refSource.listRefs(principal, repoId),
          refSource.resolveHead(principal, repoId),
        ]);
        const refs = filterAndSort(allRefs, principal.tokenClaims.refPattern);
        await writeAdvertisement(writer, service, capabilities, refs, head);
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

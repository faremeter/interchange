/**
 * Smart-HTTP `git-receive-pack` request handler. Parses the client's
 * ref-update commands followed by a packfile, drives the substrate's
 * `receivePack` primitive for each command, and emits a
 * `report-status` pkt-line stream as the response body.
 *
 * Request body shape (see `Documentation/technical/pack-protocol.txt`
 * in the git source tree):
 *
 *   <pkt><old-sha> <new-sha> <ref>\0<caps>\n</pkt>   // first command
 *   <pkt><old-sha> <new-sha> <ref>\n</pkt>           // subsequent commands
 *   ...
 *   0000
 *   <raw packfile bytes>
 *
 * Response body shape (`report-status`):
 *
 *   <pkt>unpack ok\n</pkt>
 *   <pkt>ok <ref>\n</pkt>      // per-ref success
 *   <pkt>ng <ref> <reason>\n</pkt>  // per-ref failure
 *   ...
 *   0000
 *
 * Substrate error prefixes are translated to the report-status
 * `ng <ref> <reason>` vocabulary. The substrate's `non_fast_forward:`
 * prefix covers both stale-oldSha CAS failures and pure
 * non-fast-forward rejections, both of which surface to the client as
 * `non-fast-forward`.
 */

import type { Principal, RepoId, RepoStore } from "@intx/hub-sessions";

import { writePktLine, writeFlush } from "./pkt-line";

const RECEIVE_PACK_CONTENT_TYPE = "application/x-git-receive-pack-result";
const ZERO_OID = "0".repeat(40);

type RefCommand = {
  readonly oldSha: string;
  readonly newSha: string;
  readonly ref: string;
};

type ParsedRequest = {
  readonly commands: readonly RefCommand[];
  readonly pack: Uint8Array;
};

function parseHex4(buf: Uint8Array, off: number): number {
  let v = 0;
  for (let i = 0; i < 4; i++) {
    const c = buf[off + i];
    if (c === undefined) {
      throw new Error("truncated pkt-line: short header");
    }
    let d: number;
    if (c >= 0x30 && c <= 0x39) {
      d = c - 0x30;
    } else if (c >= 0x61 && c <= 0x66) {
      d = c - 0x61 + 10;
    } else if (c >= 0x41 && c <= 0x46) {
      d = c - 0x41 + 10;
    } else {
      throw new Error("malformed pkt-line length");
    }
    v = (v << 4) | d;
  }
  return v;
}

function parseCommandLine(line: string): RefCommand {
  // A command line is `<old-sha> <new-sha> <ref>`. The first command
  // may carry capabilities after a NUL byte; the trailing `\n` is
  // optional in some clients but is present in all stock git versions.
  // Strip the trailer and the capabilities tail before parsing.
  const nulIdx = line.indexOf("\0");
  const head = nulIdx === -1 ? line : line.substring(0, nulIdx);
  const trimmed = head.endsWith("\n") ? head.slice(0, -1) : head;
  const parts = trimmed.split(" ");
  if (parts.length < 3) {
    throw new Error(`malformed receive-pack command: ${JSON.stringify(line)}`);
  }
  const oldSha = parts[0];
  const newSha = parts[1];
  // The ref may not contain a space, but split() guarantees it does
  // not; everything after the second space is the ref.
  const ref = parts.slice(2).join(" ");
  if (oldSha === undefined || newSha === undefined || ref.length === 0) {
    throw new Error(`malformed receive-pack command: ${JSON.stringify(line)}`);
  }
  return { oldSha, newSha, ref };
}

function parseRequestBody(body: Uint8Array): ParsedRequest {
  const decoder = new TextDecoder();
  const commands: RefCommand[] = [];
  let off = 0;
  for (;;) {
    if (off + 4 > body.length) {
      throw new Error("truncated receive-pack request: incomplete header");
    }
    const length = parseHex4(body, off);
    off += 4;
    if (length === 0) {
      // Flush packet marks end of command list; rest of body is the
      // packfile (or empty when the client only deleted refs, which
      // this hub does not support yet).
      break;
    }
    if (length === 1) {
      throw new Error("unexpected delim pkt-line in receive-pack commands");
    }
    if (length < 4) {
      throw new Error(`reserved pkt-line length: ${length}`);
    }
    const bodyLen = length - 4;
    if (off + bodyLen > body.length) {
      throw new Error(
        "truncated receive-pack request: incomplete pkt-line body",
      );
    }
    const line = decoder.decode(body.subarray(off, off + bodyLen));
    off += bodyLen;
    commands.push(parseCommandLine(line));
  }
  const pack = body.subarray(off);
  return { commands, pack };
}

type RefStatus =
  | { readonly kind: "ok"; readonly ref: string }
  | { readonly kind: "ng"; readonly ref: string; readonly reason: string };

const ERROR_PREFIXES = [
  "non_fast_forward:",
  "path_violation:",
  "authorize_denied:",
  "sha_mismatch:",
] as const;

type SubstrateErrorPrefix = (typeof ERROR_PREFIXES)[number];

function classifyError(err: unknown): {
  prefix: SubstrateErrorPrefix;
  detail: string;
} | null {
  if (!(err instanceof Error)) return null;
  for (const prefix of ERROR_PREFIXES) {
    if (err.message.startsWith(prefix)) {
      const detail = err.message.substring(prefix.length).trimStart();
      return { prefix, detail };
    }
  }
  return null;
}

function translateError(err: unknown, ref: string): RefStatus {
  const classified = classifyError(err);
  if (classified === null) {
    throw err;
  }
  switch (classified.prefix) {
    case "non_fast_forward:":
      return { kind: "ng", ref, reason: "non-fast-forward" };
    case "path_violation:":
      return {
        kind: "ng",
        ref,
        reason: `path-violation: ${classified.detail}`,
      };
    case "authorize_denied:":
      return { kind: "ng", ref, reason: "forbidden" };
    case "sha_mismatch:":
      return { kind: "ng", ref, reason: "sha-mismatch" };
  }
}

async function runCommand(
  repoStore: RepoStore,
  principal: Principal,
  repoId: RepoId,
  command: RefCommand,
  pack: Uint8Array,
): Promise<RefStatus> {
  const expectedOldSha = command.oldSha === ZERO_OID ? null : command.oldSha;
  try {
    await repoStore.receivePack(
      principal,
      repoId,
      command.ref,
      pack,
      command.newSha,
      expectedOldSha,
    );
    return { kind: "ok", ref: command.ref };
  } catch (err) {
    return translateError(err, command.ref);
  }
}

async function writeReport(
  writer: WritableStreamDefaultWriter<Uint8Array>,
  unpackStatus: string,
  statuses: readonly RefStatus[],
): Promise<void> {
  await writePktLine(writer, `unpack ${unpackStatus}\n`);
  for (const status of statuses) {
    if (status.kind === "ok") {
      await writePktLine(writer, `ok ${status.ref}\n`);
    } else {
      await writePktLine(writer, `ng ${status.ref} ${status.reason}\n`);
    }
  }
  await writeFlush(writer);
}

export async function handleReceivePack(
  repoStore: RepoStore,
  principal: Principal,
  repoId: RepoId,
  request: Request,
): Promise<Response> {
  const body = new Uint8Array(await request.arrayBuffer());
  const parsed = parseRequestBody(body);

  const statuses: RefStatus[] = [];
  for (const command of parsed.commands) {
    const status = await runCommand(
      repoStore,
      principal,
      repoId,
      command,
      parsed.pack,
    );
    statuses.push(status);
  }

  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const sink = new WritableStream<Uint8Array>({
        write(chunk) {
          controller.enqueue(chunk);
        },
      });
      const writer = sink.getWriter();
      try {
        await writeReport(writer, "ok", statuses);
        await writer.close();
        controller.close();
      } catch (cause) {
        await writer.abort(cause).catch(() => undefined);
        controller.error(cause);
      }
    },
  });

  return new Response(stream, {
    status: 200,
    headers: { "content-type": RECEIVE_PACK_CONTENT_TYPE },
  });
}

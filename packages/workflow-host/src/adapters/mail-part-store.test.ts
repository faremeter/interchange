import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import type { KeyPair, MessageHeaders, MessagePart } from "@intx/types/runtime";
import {
  createRepoStore,
  workflowRunKindHandler,
  workflowRunAuthorize,
} from "@intx/hub-sessions";
import type { Principal, RepoId, RepoStore } from "@intx/hub-sessions";

import {
  commitMail,
  createMailPartReader,
  InvalidMailError,
} from "./mail-part-store";

const REF = "refs/heads/main";
const tempDirs: string[] = [];

let signingKey: KeyPair;

beforeAll(async () => {
  signingKey = await generateKeyPair();
});

afterAll(async () => {
  for (const d of tempDirs.splice(0)) {
    await fs.promises.rm(d, { recursive: true, force: true }).catch(() => {
      /* best effort */
    });
  }
});

async function makeTempDir(): Promise<string> {
  const d = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "mail-part-store-"),
  );
  tempDirs.push(d);
  return d;
}

function makeStore(dataDir: string): RepoStore {
  return createRepoStore({
    dataDir,
    signingKey,
    handlers: { "workflow-run": workflowRunKindHandler },
    authorize: workflowRunAuthorize,
  });
}

function headers(): MessageHeaders {
  return {
    from: "sender@example.com",
    to: ["run@deployment.example.com"],
    date: "2026-01-02T03:04:05Z",
    messageId: "<msg-store-1@example.com>",
  };
}

function part(
  contentType: string,
  content: string,
  extra: Partial<MessagePart> = {},
): MessagePart {
  return {
    contentType,
    content: new TextEncoder().encode(content),
    ...extra,
  };
}

async function makeStoreHandles(anchorRunId: string, runId: string) {
  const dataDir = await makeTempDir();
  const repoId: RepoId = { kind: "workflow-run", id: anchorRunId };
  const principalShape = { kind: "workflow-process" as const, anchorRunId };
  const principal: Principal = principalShape;
  const opts = {
    substrate: makeStore(dataDir),
    repoId,
    principal,
    runId,
    ref: REF,
  };
  const reader = createMailPartReader({
    substrate: makeStore(dataDir),
    repoId,
    principal,
    ref: REF,
  });
  return { opts, reader };
}

describe("mail part store", () => {
  test("commits parts and round-trips each ref back to its bytes", async () => {
    const { opts, reader } = await makeStoreHandles("dep-a", "dep-a");
    const mail = await commitMail(opts, "<msg-store-1@host>", {
      headers: headers(),
      rawHeaders: { subject: ["hello"] },
      parts: [
        part("text/plain", "the body text"),
        part("image/png", "png-bytes", {
          filename: "photo.png",
          disposition: "attachment",
        }),
      ],
    });

    expect(mail.headers.from).toBe("sender@example.com");
    expect(mail.rawHeaders["subject"]).toEqual(["hello"]);
    expect(mail.parts).toHaveLength(2);

    const [textPart, imagePart] = mail.parts;
    if (textPart === undefined || imagePart === undefined) {
      throw new Error("expected two parts");
    }
    // Text part carries inline text and a ref.
    expect(textPart.contentType).toBe("text/plain");
    expect(textPart.text).toBe("the body text");
    expect(textPart.ref.startsWith("mail-part:///")).toBe(true);
    expect(new TextDecoder().decode(await reader.read(textPart.ref))).toBe(
      "the body text",
    );

    // Image part: no inline text, metadata preserved, bytes resolvable.
    expect(imagePart.contentType).toBe("image/png");
    expect(imagePart.filename).toBe("photo.png");
    expect(imagePart.disposition).toBe("attachment");
    expect(imagePart.text).toBeUndefined();
    expect(new TextDecoder().decode(await reader.read(imagePart.ref))).toBe(
      "png-bytes",
    );
  });

  test("commits a part whose filename carries a Unicode line separator", async () => {
    const { opts, reader } = await makeStoreHandles("dep-ls", "dep-ls");
    // U+2028 slips through a naive control-char sanitizer but the kind
    // handler's `<index>-<name>` regex rejects it (JS `.` excludes it). The
    // sanitizer must strip it so the store's "satisfies the handler by
    // construction" contract holds and the commit succeeds.
    const mail = await commitMail(opts, "<msg-ls@host>", {
      headers: headers(),
      rawHeaders: {},
      parts: [part("image/png", "png-bytes", { filename: "a\u2028b.png" })],
    });
    const [imagePart] = mail.parts;
    if (imagePart === undefined) throw new Error("expected one part");
    // The descriptor keeps the original filename; only the on-disk path is
    // sanitized, and the ref still resolves to the committed bytes.
    expect(imagePart.filename).toBe("a\u2028b.png");
    expect(new TextDecoder().decode(await reader.read(imagePart.ref))).toBe(
      "png-bytes",
    );
  });

  test("resolves parts of multiple runs through one deployment-scoped reader", async () => {
    // A childWorkflow / body step resolves a PARENT run's part: the reader is
    // scoped to the deployment repo, not a single run, so a ref for any run
    // committed under that repo resolves. This is the cross-run property the
    // deployment-scoped ref exists for.
    const dataDir = await makeTempDir();
    const repoId: RepoId = { kind: "workflow-run", id: "dep-x" };
    const principalShape = {
      kind: "workflow-process" as const,
      anchorRunId: "dep-x",
    };
    const principal: Principal = principalShape;
    const commitOpts = (runId: string) => ({
      substrate: makeStore(dataDir),
      repoId,
      principal,
      runId,
      ref: REF,
    });
    const mailA = await commitMail(commitOpts("run-a"), "<msg-a@host>", {
      headers: headers(),
      rawHeaders: {},
      parts: [part("text/plain", "run A body")],
    });
    const mailB = await commitMail(commitOpts("run-b"), "<msg-b@host>", {
      headers: headers(),
      rawHeaders: {},
      parts: [part("image/png", "run-b-bytes", { filename: "b.png" })],
    });
    const refA = mailA.parts[0]?.ref;
    const refB = mailB.parts[0]?.ref;
    if (refA === undefined || refB === undefined) {
      throw new Error("expected a ref per run");
    }
    // One reader resolves BOTH runs' refs; committing run B preserved run A.
    const reader = createMailPartReader({
      substrate: makeStore(dataDir),
      repoId,
      principal,
      ref: REF,
    });
    expect(new TextDecoder().decode(await reader.read(refA))).toBe(
      "run A body",
    );
    expect(new TextDecoder().decode(await reader.read(refB))).toBe(
      "run-b-bytes",
    );
  });

  test("disambiguates two parts that share a filename via the index prefix", async () => {
    const { opts, reader } = await makeStoreHandles("dep-dup", "dep-dup");
    const mail = await commitMail(opts, "<msg-dup@host>", {
      headers: headers(),
      rawHeaders: {},
      parts: [
        part("image/png", "first", { filename: "same.png" }),
        part("image/png", "second", { filename: "same.png" }),
      ],
    });
    expect(mail.parts).toHaveLength(2);
    const [p0, p1] = mail.parts;
    if (p0 === undefined || p1 === undefined) {
      throw new Error("expected two parts");
    }
    // Same sanitized name, distinct refs (the `<index>-` prefix), distinct
    // bytes.
    expect(p0.ref).not.toBe(p1.ref);
    expect(new TextDecoder().decode(await reader.read(p0.ref))).toBe("first");
    expect(new TextDecoder().decode(await reader.read(p1.ref))).toBe("second");
  });

  test("does not inline a text part over the cap but keeps its ref resolvable", async () => {
    const { opts, reader } = await makeStoreHandles("dep-big", "dep-big");
    const big = "x".repeat(1024 * 1024 + 1); // one byte over the inline cap
    const mail = await commitMail(opts, "<msg-big@host>", {
      headers: headers(),
      rawHeaders: {},
      parts: [part("text/plain", big)],
    });
    const [textPart] = mail.parts;
    if (textPart === undefined) throw new Error("expected one part");
    // Over the cap: no inline text, but the bytes are still committed.
    expect(textPart.text).toBeUndefined();
    expect((await reader.read(textPart.ref)).byteLength).toBe(big.length);
  });

  test("reader rejects a compound-traversal ref", async () => {
    const { reader } = await makeStoreHandles("dep-trav", "dep-trav");
    // `%2e%2e%2f%2e%2e` decodes to `../..`: a compound traversal that is not
    // exactly `..`, so an equality-only guard would let it through.
    await expect(
      reader.read("mail-part:///%2e%2e%2f%2e%2e/seg/0-x"),
    ).rejects.toThrow(/malformed ref/);
    await expect(
      reader.read("mail-part:///%2e%2e%2fetc%2fpasswd/seg/0-x"),
    ).rejects.toThrow(/malformed ref/);
  });

  test("rejects a messageId that url-encodes to a traversal segment", async () => {
    const { opts } = await makeStoreHandles("dep-dot", "dep-dot");
    for (const messageId of [".", ".."]) {
      await expect(
        commitMail(opts, messageId, {
          headers: headers(),
          rawHeaders: {},
          parts: [part("text/plain", "x")],
        }),
      ).rejects.toBeInstanceOf(InvalidMailError);
    }
  });

  test("reader rejects an unrecognized or malformed ref", async () => {
    const { reader } = await makeStoreHandles("dep-bad", "dep-bad");
    await expect(reader.read("blob:deadbeef")).rejects.toThrow(
      /unrecognized ref/,
    );
    await expect(reader.read("mail-part:///../evil/0-x")).rejects.toThrow(
      /malformed ref/,
    );
  });
});

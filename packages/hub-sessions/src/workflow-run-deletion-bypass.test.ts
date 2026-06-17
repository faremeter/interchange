// Regression suite for the workflow-run kind handler's append-only,
// immutability, and atomicity invariants when the prospective tree
// DROPS entries the prior tree carried.
//
// Each invariant the kind handler enforces by iterating the
// prospective tree is also a deletion-bypass surface unless the
// handler walks the prior tree and rejects any prior entry under the
// protected prefixes that the prospective tree omits. These tests pin
// that every such omission rejects at push.

import { describe, test, expect } from "bun:test";
import { workflowRunKindHandler } from "./workflow-run-kind";
import type { Principal } from "./repo-store";

const REF = "refs/heads/events";
const HUB: Principal = { kind: "hub" };

function makeReadBlob(files: Record<string, string>) {
  return async (path: string): Promise<Uint8Array> => {
    const body = files[path];
    if (body === undefined) throw new Error(`readBlob: ${path} not found`);
    return new TextEncoder().encode(body);
  };
}

function makeListDir(files: Record<string, string>) {
  return async (path: string): Promise<string[]> => {
    const prefix = path === "" ? "" : `${path}/`;
    const names = new Set<string>();
    for (const p of Object.keys(files)) {
      if (prefix !== "" && !p.startsWith(prefix)) continue;
      const rest = p.slice(prefix.length);
      if (rest.length === 0) continue;
      const slash = rest.indexOf("/");
      names.add(slash === -1 ? rest : rest.substring(0, slash));
    }
    return Array.from(names);
  };
}

function topLevels(files: Record<string, string>): string[] {
  const names = new Set<string>();
  for (const p of Object.keys(files)) {
    const slash = p.indexOf("/");
    names.add(slash === -1 ? p : p.substring(0, slash));
  }
  return Array.from(names);
}

function makePriorReadBlob(files: Record<string, string>) {
  return async (path: string): Promise<Uint8Array | null> => {
    const body = files[path];
    if (body === undefined) return null;
    return new TextEncoder().encode(body);
  };
}

async function validate(
  prospective: Record<string, string>,
  prior: Record<string, string>,
  principal: Principal = HUB,
) {
  return workflowRunKindHandler.validatePush({
    repoId: { kind: "workflow-run", id: "dep-1" },
    ref: REF,
    principal,
    topLevelTreePaths: topLevels(prospective),
    readBlob: makeReadBlob(prospective),
    listDir: makeListDir(prospective),
    priorReadBlob: makePriorReadBlob(prior),
    priorListDir: makeListDir(prior),
  });
}

describe("workflow-run validatePush: deletion-bypass surface", () => {
  test("rejects a prospective tree that DROPS an event blob present in the prior tree", async () => {
    // Drop the tail event from the prior tree. The contiguity check
    // owned by validatePush passes (prospective is 0..0 starting at 0);
    // the deletion-walk against the prior tree is the rejection lane
    // this test pins.
    const prior = {
      "runs/r1/events/0.json": JSON.stringify({ seq: 0, type: "RunStarted" }),
      "runs/r1/events/1.json": JSON.stringify({
        seq: 1,
        type: "StepCompleted",
      }),
    };
    const prospective = {
      "runs/r1/events/0.json": JSON.stringify({ seq: 0, type: "RunStarted" }),
    };
    const r = await validate(prospective, prior);
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toContain("runs/r1/events/1.json");
    expect(r.reason).toMatch(/append-only|prior tree/i);
  });

  test("rejects a prospective tree that DROPS an immutable blob present in the prior tree", async () => {
    const blobSha = "a".repeat(64);
    const prior = {
      "runs/r1/events/0.json": JSON.stringify({ seq: 0, type: "RunStarted" }),
      [`runs/r1/blobs/${blobSha}`]: "opaque-bytes-that-should-be-immutable",
    };
    const prospective = {
      "runs/r1/events/0.json": JSON.stringify({ seq: 0, type: "RunStarted" }),
    };
    const r = await validate(prospective, prior);
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toContain(`runs/r1/blobs/${blobSha}`);
    expect(r.reason).toMatch(/immutable|prior tree/i);
  });

  test("rejects a prospective tree that DROPS a consumed/<msg>.json dedup entry", async () => {
    const ADDR = "address-a";
    const ADDR_SEG = encodeURIComponent(ADDR);
    const consumedBody = JSON.stringify({
      messageId: "msg-1",
      receivedAt: 100,
      address: ADDR,
      runId: "run-1",
      consumedAt: 200,
      mailAuditRef: { store: "audit", path: "mail/msg-1" },
    });
    const inboxBody = JSON.stringify({
      messageId: "msg-1",
      receivedAt: 300,
      address: ADDR,
      mailAuditRef: { store: "audit", path: "mail/msg-1" },
    });
    const prior = {
      [`addresses/${ADDR_SEG}/consumed/msg-1.json`]: consumedBody,
    };
    // The prospective tree drops the consumed dedup entry and writes
    // a brand-new inbox entry for the SAME messageId at a later
    // receivedAt — a re-replay path.
    const prospective = {
      [`addresses/${ADDR_SEG}/inbox/300-msg-1.json`]: inboxBody,
    };
    const r = await validate(prospective, prior);
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toContain(`addresses/${ADDR_SEG}/consumed/msg-1.json`);
    expect(r.reason).toMatch(/immutable|prior tree/i);
  });

  test("rejects a prospective tree that DROPS a processing entry without writing a consumed entry", async () => {
    const ADDR = "address-a";
    const ADDR_SEG = encodeURIComponent(ADDR);
    const body = JSON.stringify({
      messageId: "msg-1",
      receivedAt: 100,
      address: ADDR,
      mailAuditRef: { store: "audit", path: "mail/msg-1" },
    });
    const prior = {
      [`addresses/${ADDR_SEG}/processing/100-msg-1.json`]: body,
    };
    const prospective = {
      ".gitignore": "",
    };
    const r = await validate(prospective, prior);
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toContain(
      `addresses/${ADDR_SEG}/processing/100-msg-1.json`,
    );
    expect(r.reason).toMatch(/prior tree|in-flight|processing/i);
  });

  test("rejects a prospective tree that wipes addresses/ entirely while prior had consumed dedup entries", async () => {
    const ADDR = "address-a";
    const ADDR_SEG = encodeURIComponent(ADDR);
    const consumedBody = JSON.stringify({
      messageId: "msg-1",
      receivedAt: 100,
      address: ADDR,
      runId: "run-1",
      consumedAt: 200,
      mailAuditRef: { store: "audit", path: "mail/msg-1" },
    });
    const prior = {
      [`addresses/${ADDR_SEG}/consumed/msg-1.json`]: consumedBody,
    };
    const prospective = {
      ".gitignore": "",
    };
    const r = await validate(prospective, prior);
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toContain(`addresses/${ADDR_SEG}/consumed/msg-1.json`);
    expect(r.reason).toMatch(/immutable|prior tree/i);
  });

  test("rejects a prospective tree that wipes runs/ entirely while prior had a terminal RunCompleted", async () => {
    const prior = {
      "runs/r1/events/0.json": JSON.stringify({ seq: 0, type: "RunStarted" }),
      "runs/r1/events/1.json": JSON.stringify({
        seq: 1,
        type: "RunCompleted",
      }),
    };
    const prospective = {
      ".gitignore": "",
    };
    const r = await validate(prospective, prior);
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/runs\/r1\/events\/(0|1)\.json/);
    expect(r.reason).toMatch(/append-only|prior tree/i);
  });
});

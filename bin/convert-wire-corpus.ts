#!/usr/bin/env bun
/* eslint-disable no-console */

// Regenerate the session corpus for one provider from its committed wire-leaf
// captures. Walks the fixture-bearing SUPPORT_MATRIX cells for the provider and
// converts each wire capability directory into a session directory under the
// provider's discovery package.
//
// This only ever converts the real wire corpus — genuine provider bytes — so
// every session it writes is stamped `origin: "live"`. It is a regenerator: the
// committed output is a pure function of the current wire and matrix, so it
// clears the provider's session subtree before rewriting it rather than merging
// on top of a previous run.

import { promises as fs } from "node:fs";
import * as path from "node:path";

import {
  SUPPORT_MATRIX,
  baseURLForCatalogProvider,
  getFixtureDir,
  isFixtureBearing,
} from "@intx/inference-discovery/catalog";

import { convertWireCapability } from "./convert-wire-to-session";

const REPO_ROOT = path.resolve(import.meta.dirname, "..");

function resolveBaseURL(provider: string): string {
  const baseURL = baseURLForCatalogProvider(provider);
  if (baseURL === undefined) {
    throw new Error(
      `convert-wire-corpus: no base URL configured for provider ${JSON.stringify(provider)}`,
    );
  }
  return baseURL;
}

// Map a wire capability path to its session counterpart by swapping the single
// `/wire/` segment for `/sessions/`. Throws if the path carries no `/wire/`
// segment, so a getFixtureDir layout change fails loudly rather than writing a
// session tree on top of the wire tree.
function toSessionRelDir(fixtureRelDir: string): string {
  const sessionRelDir = fixtureRelDir.replace("/wire/", "/sessions/");
  if (sessionRelDir === fixtureRelDir) {
    throw new Error(
      `convert-wire-corpus: fixture path ${JSON.stringify(fixtureRelDir)} has no /wire/ segment to relocate`,
    );
  }
  return sessionRelDir;
}

export type ConvertCorpusOpts = {
  provider: string;
  /** Root the wire fixtures are read from (the repository root in normal use). */
  repoRoot: string;
  /** Root the session tree is written under (redirected to a tmp dir in tests). */
  outputRoot: string;
};

export async function convertCorpus(opts: ConvertCorpusOpts): Promise<number> {
  const entries = SUPPORT_MATRIX.filter(
    (entry) => entry.provider === opts.provider && isFixtureBearing(entry),
  );
  if (entries.length === 0) {
    throw new Error(
      `convert-wire-corpus: no fixture-bearing cells for provider ${JSON.stringify(opts.provider)}`,
    );
  }
  const baseURL = resolveBaseURL(opts.provider);

  const firstEntry = entries[0];
  if (firstEntry === undefined) {
    throw new Error("convert-wire-corpus: unreachable empty entry list");
  }
  const firstFixtureDir = getFixtureDir(firstEntry);
  if (firstFixtureDir === null) {
    throw new Error(
      `convert-wire-corpus: fixture-bearing cell has no fixture dir: ${firstEntry.provider}/${firstEntry.model}/${firstEntry.capability}`,
    );
  }
  // Clear the provider's whole session subtree first so a cell that left the
  // matrix, or whose wire shrank, leaves no orphaned session behind.
  const providerSessionRoot = path.resolve(
    opts.outputRoot,
    toSessionRelDir(path.dirname(path.dirname(firstFixtureDir))),
  );
  await fs.rm(providerSessionRoot, { recursive: true, force: true });

  let converted = 0;
  for (const entry of entries) {
    const fixtureRelDir = getFixtureDir(entry);
    if (fixtureRelDir === null) {
      throw new Error(
        `convert-wire-corpus: fixture-bearing cell has no fixture dir: ${entry.provider}/${entry.model}/${entry.capability}`,
      );
    }
    console.log(`${entry.provider}/${entry.model}/${entry.capability}`);
    await convertWireCapability({
      wireDir: path.resolve(opts.repoRoot, fixtureRelDir),
      sessionDir: path.resolve(opts.outputRoot, toSessionRelDir(fixtureRelDir)),
      baseURL,
      origin: "live",
    });
    converted++;
  }
  return converted;
}

function parseCLI(argv: string[]): { provider: string } {
  let provider: string | undefined;
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--provider") {
      provider = argv[++i];
    } else if (arg !== undefined) {
      throw new Error(`convert-wire-corpus: unexpected argument ${arg}`);
    }
  }
  if (provider === undefined) {
    throw new Error("usage: convert-wire-corpus --provider <catalogProvider>");
  }
  return { provider };
}

async function main(argv: string[]): Promise<number> {
  const { provider } = parseCLI(argv);
  const count = await convertCorpus({
    provider,
    repoRoot: REPO_ROOT,
    outputRoot: REPO_ROOT,
  });
  console.log(
    `convert-wire-corpus: converted ${String(count)} ${provider} capabilit${count === 1 ? "y" : "ies"}`,
  );
  return 0;
}

if (import.meta.main) {
  const exitCode = await main(process.argv.slice(2));
  process.exit(exitCode);
}

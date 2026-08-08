#!/usr/bin/env bun
/* eslint-disable no-console */

// Offline semantic classification of captured sessions. Given a model
// directory whose children are capability sessions (the layout bin/probe.ts
// writes and the committed sessions/ tree uses), it replays each session
// through the production decoder and reports the support-matrix outcome the
// decode implies: captured, refused, or misled.
//
// This is the read side of discovery: it makes no network call and is safe in
// CI. A capability directory that holds exchanges but no session.json was a
// non-2xx capture the probe did not manifest — an http-error the probe already
// recorded — so it is reported as such rather than replayed.

import { existsSync, readdirSync, statSync } from "node:fs";
import { resolve } from "node:path";
import { classifySession } from "@intx/inference-testing";

const ROOT = resolve(import.meta.dirname, "..");

type ParsedArgs =
  | { kind: "run"; dir: string }
  | { kind: "help" }
  | { kind: "error"; message: string };

function buildHelpText(): string {
  return `Usage: bin/classify-sessions --dir <model-directory>

Replays every capability session under <model-directory> through the
production decoder and prints the outcome each decode implies:

  captured — the decode produced real content (text or a tool call).
  refused  — the model declined (a refusal, no content).
  misled   — a capture that decodes to nothing genuine: an error, an
             invariant violation, an empty stream, or an undecodable body.

<model-directory> is a directory whose children are capability sessions,
e.g. a scratch tree from bin/probe (tmp/probe/<provider>/<model>/) or
a committed sessions/<provider>/<model>/ directory.

Options:
  --dir <path>    Required. The model directory to classify.
  --help, -h      Show this message.

This command makes no network call and is safe to run in CI.
`;
}

function parseArgs(argv: readonly string[]): ParsedArgs {
  let dir: string | undefined;
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--help" || arg === "-h") return { kind: "help" };
    if (arg === "--dir") {
      const value = argv[i + 1];
      if (value === undefined || value.startsWith("--")) {
        return { kind: "error", message: "--dir needs a value" };
      }
      i++;
      dir = value;
      continue;
    }
    return { kind: "error", message: `unknown argument '${String(arg)}'` };
  }
  if (dir === undefined) return { kind: "error", message: "--dir is required" };
  return { kind: "run", dir };
}

async function main(): Promise<number> {
  const parsed = parseArgs(process.argv.slice(2));

  if (parsed.kind === "help") {
    console.log(buildHelpText());
    return 0;
  }
  if (parsed.kind === "error") {
    console.error(`error: ${parsed.message}`);
    console.error("");
    console.error("Run with --help for usage.");
    return 1;
  }

  const modelDir = resolve(ROOT, parsed.dir);
  if (!existsSync(modelDir) || !statSync(modelDir).isDirectory()) {
    console.error(`error: not a directory: ${modelDir}`);
    return 1;
  }

  const capabilities = readdirSync(modelDir)
    .filter((entry) => statSync(resolve(modelDir, entry)).isDirectory())
    .sort();
  if (capabilities.length === 0) {
    console.error(`error: no capability sessions under ${modelDir}`);
    return 1;
  }

  console.error(
    `[classify] dir=${modelDir} capabilities=${String(capabilities.length)}`,
  );

  const counts = { captured: 0, refused: 0, misled: 0, httpError: 0 };
  for (const capability of capabilities) {
    const sessionDir = resolve(modelDir, capability);
    if (!existsSync(resolve(sessionDir, "session.json"))) {
      // No manifest means the capture was never completed as a session — a
      // non-2xx (http-error) the probe recorded but did not manifest.
      console.error(
        `[classify] ${capability.padEnd(38)} http-error (no session manifest)`,
      );
      counts.httpError++;
      continue;
    }
    const classification = await classifySession(sessionDir);
    console.error(
      `[classify] ${capability.padEnd(38)} ${classification.outcome} — ${classification.reason}`,
    );
    if (classification.outcome === "captured") counts.captured++;
    else if (classification.outcome === "refused") counts.refused++;
    else counts.misled++;
  }

  console.error(
    `[classify] done captured=${String(counts.captured)} refused=${String(counts.refused)} misled=${String(counts.misled)} http-error=${String(counts.httpError)}`,
  );
  return 0;
}

const exitCode = await main();
process.exit(exitCode);

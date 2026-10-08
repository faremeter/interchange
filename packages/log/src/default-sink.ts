import {
  configureSync,
  getConfig,
  getConsoleSink,
  ansiColorFormatter,
  getJsonLinesFormatter,
  type TextFormatter,
} from "@logtape/logtape";

/**
 * Installs a default console sink when no LogTape configuration is present,
 * so diagnostics emitted before `setup()` are not silently discarded.
 *
 * Formatter selection follows the same dev/prod heuristic as `setup()`.
 * Without a `process` global the runtime is treated as development; the
 * `dev`/`prod` options cannot influence this default because no options are
 * passed at module load.
 *
 * Invoked once at module load (bottom of this file) and exported so tests
 * can reinstall after `resetSync()`. Every package entry point must
 * side-effect import this module (`./index.ts`, `./hono.ts`). Idempotent:
 * returns immediately when a configuration is already installed.
 */
export function installDefaultConsoleSink(): void {
  if (getConfig() !== null) return;

  const isDev =
    typeof process === "undefined" || process.env["NODE_ENV"] !== "production";
  const formatter: TextFormatter = isDev
    ? ansiColorFormatter
    : getJsonLinesFormatter();

  configureSync({
    sinks: { default: getConsoleSink({ formatter }) },
    loggers: [
      {
        category: ["logtape", "meta"],
        lowestLevel: "warning",
        sinks: ["default"],
      },
      {
        category: [],
        lowestLevel: "warning",
        sinks: ["default"],
      },
    ],
  });
}

// Module-load entry.
installDefaultConsoleSink();

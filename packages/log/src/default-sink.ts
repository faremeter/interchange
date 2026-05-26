import {
  configureSync,
  getConfig,
  getConsoleSink,
  ansiColorFormatter,
  getJsonLinesFormatter,
  type TextFormatter,
} from "@logtape/logtape";

/**
 * Installs a default console sink when no LogTape configuration is present.
 *
 * Without this, a consumer that imports `getLogger` and emits diagnostics
 * before calling `setup()` will silently discard those records. The default
 * sink routes through `console.warn`/`console.error`, which lands on stderr
 * in Node and Bun and on the devtools console in browsers, until `setup()`
 * replaces the configuration.
 *
 * Formatter selection follows the same dev/prod heuristic as `setup()`,
 * but only reads `process.env["NODE_ENV"]`; the caller-facing `dev`/`prod`
 * options on `setup()` cannot influence this default because no options are
 * passed at module load.
 *
 * This is normally invoked once at module load (see the bottom of this
 * file). It is also exported so tests can reinstall the default sink after
 * a `resetSync()`. Every entry point in `package.json` must side-effect
 * import this module (see `./index.ts` and `./hono.ts`); a new entry point
 * that skips that import silently regresses to the pre-default-sink
 * behavior.
 *
 * Idempotent: returns immediately if a configuration is already installed.
 */
export function installDefaultConsoleSink(): void {
  if (getConfig() !== null) return;

  const isDev = process.env["NODE_ENV"] !== "production";
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

// Module-load entry: every side-effect import of this file installs the
// default sink. The export above is for tests that need to reinstall after
// `resetSync()`.
installDefaultConsoleSink();

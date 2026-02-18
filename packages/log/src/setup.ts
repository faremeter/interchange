import {
  configure,
  getConsoleSink,
  ansiColorFormatter,
  getJsonLinesFormatter,
  type LogLevel,
} from "@logtape/logtape";

export type SetupOptions = {
  /**
   * Override the default log level for specific categories.
   * Keys are dot-separated category paths (e.g., "hub.requests").
   * Default level is "info" for production, "debug" for development.
   */
  levels?: Record<string, LogLevel>;

  /**
   * Force development mode (pretty console output) regardless of NODE_ENV.
   */
  dev?: boolean;

  /**
   * Force production mode (JSON output) regardless of NODE_ENV.
   */
  prod?: boolean;
};

type LoggerConfigEntry = {
  category: string[];
  lowestLevel: LogLevel;
  sinks: ["console"];
};

/**
 * Configures LogTape with sensible defaults for the Interchange project.
 *
 * - Development: Pretty ANSI-colored console output, debug level
 * - Production: JSON Lines console output, info level
 *
 * Call this once at application startup (app entry point, CLI script).
 */
export async function setup(options: SetupOptions = {}): Promise<void> {
  const isDev =
    options.dev ?? (!options.prod && process.env["NODE_ENV"] !== "production");
  const defaultLevel: LogLevel = isDev ? "debug" : "info";

  const loggers: LoggerConfigEntry[] = [
    {
      category: [],
      lowestLevel: defaultLevel,
      sinks: ["console"],
    },
  ];

  if (options.levels) {
    for (const [categoryPath, level] of Object.entries(options.levels)) {
      const category = categoryPath.split(".");
      loggers.push({
        category,
        lowestLevel: level,
        sinks: ["console"],
      });
    }
  }

  await configure({
    sinks: {
      console: getConsoleSink({
        formatter: isDev ? ansiColorFormatter : getJsonLinesFormatter(),
      }),
    },
    loggers,
  });
}
